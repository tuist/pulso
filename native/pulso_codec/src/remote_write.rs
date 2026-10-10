//! Hand-rolled Prometheus remote_write v1 `WriteRequest` decoder.
//!
//! Reference: `prometheus/prometheus` repo, `prompb/types.proto` plus
//! `prompb/remote.proto`. The subset we care about:
//!
//! ```proto
//! message WriteRequest {
//!   repeated TimeSeries timeseries = 1 [(gogoproto.nullable) = false];
//!   // metadata is optional and ignored here
//! }
//!
//! message TimeSeries {
//!   repeated Label   labels    = 1 [(gogoproto.nullable) = false];
//!   repeated Sample  samples   = 2 [(gogoproto.nullable) = false];
//!   // exemplars (3), native histograms (4) explicitly counted as rejected
//! }
//!
//! message Label  { string name = 1; string value = 2; }
//! message Sample { double value = 1; int64  timestamp = 2; }
//! ```
//!
//! The wire bytes come to us Snappy-compressed (block format, same as
//! Loki push). Our contract matches `loki::decode`: we read sub-slices
//! of the decompressed buffer and never allocate per label/sample so
//! downstream code can turn them into Erlang sub-binaries. Semantic
//! errors (missing timestamp, out-of-range sample, non-UTF-8 label) are
//! counted in `rejected` rather than failing the whole batch — a
//! wire-level corruption (bad varint, truncated field) still fails
//! the whole request.
//!
//! Timestamp units: Prometheus remote_write v1 timestamps are
//! **milliseconds** since epoch. We leave that unchanged in the
//! decoded `Sample` and let the Elixir caller scale to nanoseconds
//! at the storage boundary.

use crate::wire::{Reader, Value};

pub struct Sample {
    pub value: f64,
    pub timestamp_ms: i64,
}

pub struct Series<'a> {
    pub labels: Vec<(&'a [u8], &'a [u8])>,
    pub samples: Vec<Sample>,
}

pub struct Decoded<'a> {
    pub series: Vec<Series<'a>>,
    pub rejected: u64,
}

pub fn decode(input: &[u8]) -> Option<Decoded<'_>> {
    let mut reader = Reader::new(input);
    let mut decoded = Decoded {
        series: Vec::new(),
        rejected: 0,
    };

    loop {
        match reader.next_field()? {
            None => break,
            Some((1, Value::Bytes(bytes))) => match decode_series(bytes) {
                Ok((s, rejected)) => {
                    decoded.series.push(s);
                    decoded.rejected = decoded.rejected.saturating_add(rejected);
                }
                Err(rejected) => decoded.rejected = decoded.rejected.saturating_add(rejected),
            },
            // Other fields (metadata, exemplars at the top level in
            // v2, etc.) are ignored.
            _ => {}
        }
    }

    Some(decoded)
}

fn decode_series(input: &[u8]) -> Result<(Series<'_>, u64), u64> {
    let mut reader = Reader::new(input);
    let mut labels: Vec<(&[u8], &[u8])> = Vec::new();
    let mut samples: Vec<Sample> = Vec::new();
    let mut rejected = 0u64;
    let mut total_samples = 0u64;

    loop {
        match reader.next_field().ok_or(total_samples.max(1))? {
            None => break,
            Some((1, Value::Bytes(bytes))) => {
                let label = decode_label(bytes).ok_or(total_samples.max(1))?;
                labels.push(label);
            }
            Some((2, Value::Bytes(bytes))) => {
                total_samples = total_samples.saturating_add(1);
                let sample = decode_sample(bytes).ok_or(total_samples)?;
                samples.push(sample);
            }
            Some((3 | 4, Value::Bytes(_))) => {
                // Exemplars and native histograms have no storage model yet.
                // Count them explicitly instead of silently acknowledging loss.
                total_samples = total_samples.saturating_add(1);
                rejected = rejected.saturating_add(1);
            }
            _ => {}
        }
    }

    // Prometheus requires at least `__name__` and one sample per series.
    // Treat a labels- or samples-empty series as rejected rather than
    // feeding a nameless point into storage.
    if labels.is_empty() || samples.is_empty() {
        return Err(total_samples.max(1));
    }

    // Validate after reading the whole series so rejected counts include
    // every supplied sample. UTF-8 validation also excludes the 0xff byte
    // used as the canonical label separator in storage.
    if labels.iter().any(|(name, value)| {
        simdutf8::basic::from_utf8(name).is_err() || simdutf8::basic::from_utf8(value).is_err()
    }) {
        return Err(total_samples.max(1));
    }
    labels.sort_by(|a, b| a.0.cmp(b.0));
    if labels.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(total_samples.max(1));
    }
    Ok((Series { labels, samples }, rejected))
}

fn decode_label(input: &[u8]) -> Option<(&[u8], &[u8])> {
    let mut reader = Reader::new(input);
    let mut name: &[u8] = &[];
    let mut value: &[u8] = &[];
    loop {
        match reader.next_field()? {
            None => break,
            Some((1, Value::Bytes(b))) => name = b,
            Some((2, Value::Bytes(b))) => value = b,
            _ => {}
        }
    }
    if name.is_empty() {
        None
    } else {
        Some((name, value))
    }
}

fn decode_sample(input: &[u8]) -> Option<Sample> {
    // In proto3 wire format:
    //   field 1 (double value) is wire type 1 (64-bit fixed), tag byte 0x09.
    //   field 2 (int64 timestamp) is wire type 0 (varint), tag byte 0x10.
    //
    // Our minimal `Reader` only handles varint and length-delimited;
    // fixed64 needs a tiny inline decode here. We walk the buffer by
    // hand so we don't need to extend `Reader` just for this one site.
    let mut i = 0usize;
    let mut value_bits: u64 = 0;
    let mut timestamp_ms: i64 = 0;
    while i < input.len() {
        let (field, wire, consumed) = read_tag(input, i)?;
        i += consumed;
        match (field, wire) {
            (1, 1) => {
                // fixed64 little-endian
                if i + 8 > input.len() {
                    return None;
                }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&input[i..i + 8]);
                value_bits = u64::from_le_bytes(bytes);
                i += 8;
            }
            (2, 0) => {
                let (v, consumed) = read_varint(input, i)?;
                i += consumed;
                timestamp_ms = v as i64;
            }
            // Skip unknown fields.
            (_, 0) => {
                let (_, consumed) = read_varint(input, i)?;
                i += consumed;
            }
            (_, 1) => {
                if i + 8 > input.len() {
                    return None;
                }
                i += 8;
            }
            (_, 2) => {
                let (len, consumed) = read_varint(input, i)?;
                i += consumed;
                let len = len as usize;
                if i + len > input.len() {
                    return None;
                }
                i += len;
            }
            (_, 5) => {
                if i + 4 > input.len() {
                    return None;
                }
                i += 4;
            }
            _ => return None,
        }
    }
    Some(Sample {
        value: f64::from_bits(value_bits),
        timestamp_ms,
    })
}

fn read_tag(buf: &[u8], pos: usize) -> Option<(u32, u8, usize)> {
    let (v, consumed) = read_varint(buf, pos)?;
    let field = (v >> 3) as u32;
    let wire = (v & 0x7) as u8;
    Some((field, wire, consumed))
}

fn read_varint(buf: &[u8], pos: usize) -> Option<(u64, usize)> {
    let mut out: u64 = 0;
    for (idx, shift) in (0..64).step_by(7).enumerate() {
        let byte = *buf.get(pos + idx)?;
        out |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((out, idx + 1));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // All test fields are shorter than 128 bytes, so lengths fit one varint byte.
    fn bytes_field(tag: u8, bytes: &[u8]) -> Vec<u8> {
        assert!(bytes.len() < 128);
        [vec![tag, bytes.len() as u8], bytes.to_vec()].concat()
    }

    fn series(labels: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        for (name, value) in labels {
            let label = [bytes_field(0x0a, name), bytes_field(0x12, value)].concat();
            out.extend(bytes_field(0x0a, &label));
        }
        for timestamp in 1..=3 {
            let sample = [
                vec![0x09],
                1.0f64.to_le_bytes().to_vec(),
                vec![0x10, timestamp],
            ]
            .concat();
            out.extend(bytes_field(0x12, &sample));
        }
        out
    }

    #[test]
    fn rejects_invalid_utf8_and_duplicates_before_materializing_labels() {
        let invalid: &[&[(&[u8], &[u8])]] = &[
            &[(b"job", b"a\xffb")],
            &[(b"job", b"a\xffinjected\xffx")],
            &[(b"\xc0\xaf", b"value")],
            &[(b"job", b"\xed\xa0\x80")],
            &[(b"job", b"a"), (b"job", b"b")],
            &[(b"job", b"a"), (b"job", b"a")],
        ];
        let good = series(&[(b"__name__", b"safe"), (b"job", "é😀".as_bytes())]);
        for labels in invalid {
            let request = [bytes_field(0x0a, &series(labels)), bytes_field(0x0a, &good)].concat();
            let decoded = decode(&request).expect("valid protobuf");
            assert_eq!(decoded.rejected, 3);
            assert_eq!(decoded.series.len(), 1);
            assert_eq!(decoded.series[0].labels[1].1, "é😀".as_bytes());
            assert_eq!(decoded.series[0].samples.len(), 3);
        }
    }
}
