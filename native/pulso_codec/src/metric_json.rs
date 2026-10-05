//! JSON encode of `[%Pulso.Record.MetricSample{}]` for the MCP
//! `query_metrics` response.
//!
//! Byte-for-byte parity with the Elixir fallback is **not** promised —
//! only semantic parity (same keys, same values, decodable by any JSON
//! parser). We fix the key order (`series_id`, `timestamp_ns`,
//! `value`, `labels`) because it is cheap in Rust and worth it for
//! reader stability; Elixir's `JSON.encode!` on a plain map does not.
//!
//! Fallback discipline: any sample term that is not a well-shaped
//! `%MetricSample{}` returns `:fallback` to the NIF, which the Elixir
//! caller downgrades to the pure-Elixir encoder. The caller never
//! loses the record — same contract as `Pulso.JSON`.
//!
//! Zero copies on the output: writes straight into a `Vec<u8>` that
//! becomes an Erlang binary at the NIF boundary (see `lib.rs`
//! `copy()`), and reads label name/value binaries by slice reference
//! so no intermediate `String` or `Vec<u8>` is built per field.

use rustler::{Binary, Encoder, Env, ListIterator, MapIterator, Term};

use crate::atoms;

#[derive(Debug)]
pub enum Error {
    Fallback,
}

pub fn encode<'a>(env: Env<'a>, samples: Term<'a>) -> Result<Vec<u8>, Error> {
    let items: ListIterator = samples.decode().map_err(|_| Error::Fallback)?;
    // 48 bytes per sample is a comfortable median for 4-label series —
    // see the bench fixture. The Vec grows on undershoot; overshoot is
    // cheap.
    let hint = samples.list_length().unwrap_or(0).saturating_mul(96);
    let mut buf: Vec<u8> = Vec::with_capacity(hint.max(64));
    let struct_key = atoms::struct_key().encode(env);
    let expected = atoms::metric_sample().encode(env);
    let series_id = atoms::series_id().encode(env);
    let timestamp_ns = atoms::timestamp_ns().encode(env);
    let value_key = atoms::value().encode(env);
    let labels_key = atoms::labels().encode(env);
    let nil = atoms::nil().encode(env);

    buf.push(b'[');
    let mut first = true;
    // A bounded single-entry cache of the previous label JSON range in
    // this output. Compare complete maps, not series IDs; repeated samples
    // can copy encoded bytes without decoding/escaping each label again.
    let mut previous_labels: Option<(Term<'a>, usize, usize)> = None;
    for sample in items {
        if !first {
            buf.push(b',');
        }
        first = false;

        let tag = sample.map_get(struct_key).map_err(|_| Error::Fallback)?;
        if tag.as_c_arg() != expected.as_c_arg() {
            return Err(Error::Fallback);
        }

        let sid = sample.map_get(series_id).map_err(|_| Error::Fallback)?;
        let ts = sample.map_get(timestamp_ns).map_err(|_| Error::Fallback)?;
        let val = sample.map_get(value_key).map_err(|_| Error::Fallback)?;
        let lbls = sample.map_get(labels_key).map_err(|_| Error::Fallback)?;

        buf.extend_from_slice(b"{\"series_id\":");
        write_int_or_null(&mut buf, sid, nil)?;
        buf.extend_from_slice(b",\"timestamp_ns\":");
        write_int_or_null(&mut buf, ts, nil)?;
        buf.extend_from_slice(b",\"value\":");
        write_float_or_null(&mut buf, val, nil)?;
        buf.extend_from_slice(b",\"labels\":");
        match previous_labels {
            Some((previous, start, end)) if previous == lbls => {
                buf.extend_from_within(start..end);
            }
            _ => {
                let start = buf.len();
                write_labels_map(&mut buf, lbls)?;
                previous_labels = Some((lbls, start, buf.len()));
            }
        }
        buf.push(b'}');
    }
    buf.push(b']');

    Ok(buf)
}

fn write_int_or_null<'a>(buf: &mut Vec<u8>, term: Term<'a>, nil: Term<'a>) -> Result<(), Error> {
    if term.as_c_arg() == nil.as_c_arg() {
        buf.extend_from_slice(b"null");
        return Ok(());
    }
    let v: i64 = term.decode().map_err(|_| Error::Fallback)?;
    let mut tmp = itoa::Buffer::new();
    buf.extend_from_slice(tmp.format(v).as_bytes());
    Ok(())
}

fn write_float_or_null<'a>(buf: &mut Vec<u8>, term: Term<'a>, nil: Term<'a>) -> Result<(), Error> {
    if term.as_c_arg() == nil.as_c_arg() {
        buf.extend_from_slice(b"null");
        return Ok(());
    }
    // Accept both float and integer value terms. The former is what
    // Prometheus emits; the latter is possible when a producer stored
    // an integer sample.
    if let Ok(f) = term.decode::<f64>() {
        if !f.is_finite() {
            // JSON has no NaN/inf. Fall back so the Elixir side can
            // decide (currently `JSON.encode!` will also fail; this
            // just preserves the behavior).
            return Err(Error::Fallback);
        }
        let mut tmp = ryu::Buffer::new();
        buf.extend_from_slice(tmp.format(f).as_bytes());
        return Ok(());
    }
    if let Ok(i) = term.decode::<i64>() {
        let mut tmp = itoa::Buffer::new();
        buf.extend_from_slice(tmp.format(i).as_bytes());
        return Ok(());
    }
    Err(Error::Fallback)
}

fn write_labels_map<'a>(buf: &mut Vec<u8>, term: Term<'a>) -> Result<(), Error> {
    let iter = MapIterator::new(term).ok_or(Error::Fallback)?;
    buf.push(b'{');
    let mut first = true;
    for (k, v) in iter {
        if !first {
            buf.push(b',');
        }
        first = false;
        write_json_binary(buf, k)?;
        buf.push(b':');
        write_json_binary(buf, v)?;
    }
    buf.push(b'}');
    Ok(())
}

fn write_json_binary<'a>(buf: &mut Vec<u8>, term: Term<'a>) -> Result<(), Error> {
    let bin: Binary = term.decode().map_err(|_| Error::Fallback)?;
    write_json_string(buf, bin.as_slice());
    Ok(())
}

/// Fast-path JSON string writer.
///
/// Prometheus labels are overwhelmingly plain ASCII with no `"`, `\`,
/// or sub-0x20 bytes, so we scan for the first byte that **does** need
/// escaping and bulk-copy every clean run in between. The inner scan
/// is a small `while` loop that LLVM auto-vectorises to SIMD on
/// x86_64 and aarch64 — the equivalent of a `memchr`-over-a-range
/// scan without the `memchr` crate's byte-list restriction (which
/// only supports three exact bytes, not the 34-byte "needs escape"
/// set we actually have).
///
/// Byte-identical output with the pre-fast-path implementation is
/// pinned by the `parity` test at the bottom of this file.
fn write_json_string(buf: &mut Vec<u8>, s: &[u8]) {
    buf.push(b'"');
    let mut i = 0;
    while i < s.len() {
        let start = i;
        // Clean run: anything that is not a control char, a quote, or
        // a backslash.
        while i < s.len() {
            let b = s[i];
            if b < 0x20 || b == b'"' || b == b'\\' {
                break;
            }
            i += 1;
        }
        if i > start {
            buf.extend_from_slice(&s[start..i]);
        }
        if i >= s.len() {
            break;
        }
        // Slow path: emit the escape for the single offending byte,
        // then resume the clean-run scan from i+1.
        match s[i] {
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            0x08 => buf.extend_from_slice(b"\\b"),
            0x09 => buf.extend_from_slice(b"\\t"),
            0x0a => buf.extend_from_slice(b"\\n"),
            0x0c => buf.extend_from_slice(b"\\f"),
            0x0d => buf.extend_from_slice(b"\\r"),
            b => {
                // Remaining sub-0x20 bytes: `\u00XX` hex escape.
                buf.extend_from_slice(b"\\u00");
                buf.push(hex_nibble(b >> 4));
                buf.push(hex_nibble(b & 0xf));
            }
        }
        i += 1;
    }
    buf.push(b'"');
}

fn hex_nibble(n: u8) -> u8 {
    match n {
        0..=9 => b'0' + n,
        _ => b'a' + (n - 10),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Oracle: a byte-by-byte reference implementation that matches
    /// the pre-fast-path encoder exactly. Any divergence between
    /// `write_json_string` and this function fails `parity_*`.
    fn oracle(s: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(s.len() + 2);
        buf.push(b'"');
        for &b in s {
            match b {
                b'"' => buf.extend_from_slice(b"\\\""),
                b'\\' => buf.extend_from_slice(b"\\\\"),
                0x08 => buf.extend_from_slice(b"\\b"),
                0x09 => buf.extend_from_slice(b"\\t"),
                0x0a => buf.extend_from_slice(b"\\n"),
                0x0c => buf.extend_from_slice(b"\\f"),
                0x0d => buf.extend_from_slice(b"\\r"),
                b if b < 0x20 => {
                    buf.extend_from_slice(b"\\u00");
                    buf.push(hex_nibble(b >> 4));
                    buf.push(hex_nibble(b & 0xf));
                }
                b => buf.push(b),
            }
        }
        buf.push(b'"');
        buf
    }

    fn fast(s: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(s.len() + 2);
        write_json_string(&mut buf, s);
        buf
    }

    #[test]
    fn parity_empty_and_short_ascii() {
        for s in [b"".as_ref(), b"a", b"hello", b"node-42", b"__name__"] {
            assert_eq!(fast(s), oracle(s), "input: {:?}", s);
        }
    }

    #[test]
    fn parity_quotes_and_backslashes() {
        let inputs: &[&[u8]] = &[
            b"\"",
            b"\\",
            b"a\"b\"c",
            b"\\path\\to\\file",
            b"he said \"hi\" and left",
            b"\\\\\"\\\"",
        ];
        for s in inputs {
            assert_eq!(fast(s), oracle(s), "input: {:?}", s);
        }
    }

    #[test]
    fn parity_control_chars_including_named_and_generic() {
        // Every single sub-0x20 byte on its own and in various
        // positions, plus the five named escapes.
        for b in 0u8..0x20 {
            let s = [b];
            assert_eq!(fast(&s), oracle(&s), "control byte: {:#x}", b);
        }
        // Mixed: control + plain + control.
        let s: Vec<u8> = [0x00, b'a', 0x1f, b'b', 0x08, b'c', 0x0a, b'd'].to_vec();
        assert_eq!(fast(&s), oracle(&s));
    }

    #[test]
    fn parity_long_clean_runs_cross_bulk_boundary() {
        // The fast path's clean-run bulk copy should produce the same
        // bytes as the byte-by-byte oracle regardless of run length.
        for len in [1usize, 15, 16, 17, 31, 32, 33, 63, 64, 127, 1024] {
            let s: Vec<u8> = (0..len).map(|i| b'a' + (i % 26) as u8).collect();
            assert_eq!(fast(&s), oracle(&s), "len: {}", len);
        }
    }

    #[test]
    fn parity_randomised_corpus() {
        // Deterministic PRNG (SplitMix64) over a seed so a regression
        // bisects to a specific input rather than a flake.
        let mut state: u64 = 0xdead_beef_cafe_f00d;
        let mut rand_byte = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 56) as u8
        };

        for _ in 0..2000 {
            let len = (rand_byte() as usize) % 128;
            let mut s = Vec::with_capacity(len);
            for _ in 0..len {
                s.push(rand_byte());
            }
            assert_eq!(fast(&s), oracle(&s), "input: {:?}", s);
        }
    }
}
