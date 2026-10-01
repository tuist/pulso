//! Parquet metric segment encoding and decoding.
//!
//! Fixed six-column schema (`series_id Int64`, `timestamp_ns Int64`,
//! `value Float64`, `metric_name Utf8`, `labels_canonical Utf8`,
//! `labels_json Utf8`), one row group per segment, rows sorted by
//! `(series_id, timestamp_ns)` on write. That sort is what the
//! Prometheus storage model expects on the hot query path: a
//! label-matcher query picks series, then streams samples per series in
//! time order. The dictionary + zstd encoding on `metric_name` and
//! `labels_canonical` compresses tightly because a single series
//! produces thousands of consecutive identical values; delta-binary-
//! packed `timestamp_ns` compresses because samples arrive at a fixed
//! scrape cadence.
//!
//! `labels_canonical` is the exact byte sequence hashed into
//! `series_id` (see [`crate::stable_hash`]): `name<0xff>value<0xff>…`
//! across sorted labels. Keeping it in the segment lets a decoder
//! re-verify series identity without rebuilding the hash and lets a
//! future postings-list sidecar be computed by LIST-walking segments
//! without opening the `labels_json` column.
//!
//! The matcher-pushdown surface for v1 metrics is deliberately small:
//! a decoder-side filter accepts a list of `(name, op, value)`
//! matchers and only materialises rows for which every matcher
//! holds. `op` is one of `:eq | :neq | :re | :nre`. Regex matchers
//! compile once per decode and apply per row — fine for the
//! single-row-group segments v1 produces; a sidecar postings index
//! will take over the heavy lifting later.
//!
//! No fallback to Elixir: a decode failure here is a hard error (the
//! Elixir side maps `:fallback` to `{:error, {:decode_failed, _}}`),
//! same policy as the log Parquet codec.

use crate::stable_hash;

use arrow::array::{
    Array, Float64Array, Float64Builder, Int64Array, Int64Builder, RecordBatch, StringArray,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use rustler::{Binary, Encoder, Env, ListIterator, MapIterator, NewBinary, Term};
use std::collections::HashMap;
use std::sync::Arc;

mod atoms {
    rustler::atoms! {
        struct_key = "__struct__",
        metric_sample = "Elixir.Pulso.Record.MetricSample",
        series_id,
        timestamp_ns,
        value,
        labels,
        nil,
        name_atom = "__name__",
        eq,
        neq,
        re,
        nre,
    }
}

pub struct Bounds {
    pub min_ts: i128,
    pub max_ts: i128,
    pub count: usize,
}

#[derive(Clone, Copy, Debug)]
pub enum MatcherOp {
    Eq,
    Neq,
    Re,
    Nre,
}

pub struct Matcher<'a> {
    pub name: &'a [u8],
    pub op: MatcherOp,
    pub value: &'a [u8],
}

pub struct Filter<'f> {
    pub start: Option<i128>,
    pub end: Option<i128>,
    pub matchers: Vec<Matcher<'f>>,
}

struct Row {
    series_id: i64,
    timestamp_ns: i64,
    value: f64,
    metric_name: Vec<u8>,
    labels_canonical: Vec<u8>,
    labels_json: Vec<u8>,
}

#[derive(Debug)]
pub enum EncodeError {
    BadInput,
    Writer,
}

#[derive(Debug)]
pub enum DecodeError {
    Reader,
}

pub fn encode<'a>(env: Env<'a>, samples: Term<'a>) -> Result<(Vec<u8>, Bounds), EncodeError> {
    let len = samples.list_length().map_err(|_| EncodeError::BadInput)?;
    let items: ListIterator = samples.decode().map_err(|_| EncodeError::BadInput)?;
    let struct_key = atoms::struct_key().encode(env);
    let expected = atoms::metric_sample().encode(env);

    let mut rows: Vec<Row> = Vec::with_capacity(len);
    let mut bounds = Bounds {
        min_ts: i128::MAX,
        max_ts: i128::MIN,
        count: 0,
    };

    for sample in items {
        let tag = sample.map_get(struct_key).map_err(|_| EncodeError::BadInput)?;
        if tag.as_c_arg() != expected.as_c_arg() {
            return Err(EncodeError::BadInput);
        }
        let row = extract_row(env, sample, &mut bounds)?;
        rows.push(row);
    }

    if bounds.count == 0 {
        bounds.min_ts = 0;
        bounds.max_ts = 0;
    }

    // (series_id, timestamp_ns) ascending so consecutive rows in the
    // same series sit next to each other on disk. Dictionary + zstd on
    // `metric_name` and `labels_canonical` and delta-binary-packed
    // `timestamp_ns` all do most of their work from this ordering.
    rows.sort_by(|a, b| {
        a.series_id
            .cmp(&b.series_id)
            .then(a.timestamp_ns.cmp(&b.timestamp_ns))
    });

    let batch = build_batch(&rows).map_err(|_| EncodeError::BadInput)?;
    let mut buf: Vec<u8> = Vec::with_capacity(64 + rows.len() * 32);
    let props = writer_properties();
    let mut writer =
        ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).map_err(|_| EncodeError::Writer)?;
    writer.write(&batch).map_err(|_| EncodeError::Writer)?;
    writer.close().map_err(|_| EncodeError::Writer)?;

    Ok((buf, bounds))
}

fn extract_row<'a>(
    env: Env<'a>,
    sample: Term<'a>,
    bounds: &mut Bounds,
) -> Result<Row, EncodeError> {
    let nil = atoms::nil().encode(env);

    let labels_term = sample
        .map_get(atoms::labels().encode(env))
        .map_err(|_| EncodeError::BadInput)?;

    let labels_map: HashMap<Vec<u8>, Vec<u8>> = match MapIterator::new(labels_term) {
        Some(iter) => {
            let mut m = HashMap::new();
            for (k, v) in iter {
                let kb: Binary = k.decode().map_err(|_| EncodeError::BadInput)?;
                let vb: Binary = v.decode().map_err(|_| EncodeError::BadInput)?;
                m.insert(kb.as_slice().to_vec(), vb.as_slice().to_vec());
            }
            m
        }
        None => return Err(EncodeError::BadInput),
    };

    // Sorted-by-name byte pairs: identical to what Prometheus's
    // `Labels` iterator yields. StableHash and the on-disk canonical
    // byte sequence both depend on this ordering.
    let mut pairs: Vec<(&[u8], &[u8])> =
        labels_map.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));

    let series_id_term = sample
        .map_get(atoms::series_id().encode(env))
        .map_err(|_| EncodeError::BadInput)?;

    // Caller may pre-fill series_id or leave it nil; recompute on nil.
    let series_id: i64 = if series_id_term.as_c_arg() == nil.as_c_arg() {
        stable_hash::stable_hash(pairs.iter().copied()) as i64
    } else {
        series_id_term.decode().map_err(|_| EncodeError::BadInput)?
    };

    let ts_term = sample
        .map_get(atoms::timestamp_ns().encode(env))
        .map_err(|_| EncodeError::BadInput)?;
    let timestamp_ns: i64 = if ts_term.as_c_arg() == nil.as_c_arg() {
        0
    } else {
        ts_term.decode().map_err(|_| EncodeError::BadInput)?
    };

    let value_term = sample
        .map_get(atoms::value().encode(env))
        .map_err(|_| EncodeError::BadInput)?;
    let value: f64 = if value_term.as_c_arg() == nil.as_c_arg() {
        f64::NAN
    } else {
        value_term
            .decode::<f64>()
            .or_else(|_| value_term.decode::<i64>().map(|i| i as f64))
            .map_err(|_| EncodeError::BadInput)?
    };

    let metric_name = labels_map.get(b"__name__".as_ref()).cloned().unwrap_or_default();
    let labels_canonical = canonical_bytes(&pairs);
    let labels_json = labels_as_json(&pairs);

    bounds.count += 1;
    let ts = i128::from(timestamp_ns);
    if ts < bounds.min_ts {
        bounds.min_ts = ts;
    }
    if ts > bounds.max_ts {
        bounds.max_ts = ts;
    }

    Ok(Row {
        series_id,
        timestamp_ns,
        value,
        metric_name,
        labels_canonical,
        labels_json,
    })
}

fn canonical_bytes(pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut out = Vec::with_capacity(pairs.iter().map(|(n, v)| n.len() + v.len() + 2).sum());
    for (n, v) in pairs {
        out.extend_from_slice(n);
        out.push(stable_hash::SEP);
        out.extend_from_slice(v);
        out.push(stable_hash::SEP);
    }
    out
}

fn labels_as_json(pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
    // Minimal JSON encoder for `{"name":"value",…}` — label names and
    // values can contain anything, but Prometheus's own convention
    // restricts them to UTF-8 and names to `[A-Za-z_][A-Za-z0-9_]*`, so
    // we only need to escape `"` and `\` plus the mandatory control
    // characters. If a value is not valid UTF-8 (which Prometheus
    // forbids but a malicious client could send), we lossily replace
    // with `�` — the segment stays readable rather than failing
    // the whole batch.
    let mut out = Vec::with_capacity(pairs.iter().map(|(n, v)| n.len() + v.len() + 6).sum());
    out.push(b'{');
    for (i, (n, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        encode_json_string(&mut out, n);
        out.push(b':');
        encode_json_string(&mut out, v);
    }
    out.push(b'}');
    out
}

fn encode_json_string(out: &mut Vec<u8>, s: &[u8]) {
    out.push(b'"');
    for &b in s {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x09 => out.extend_from_slice(b"\\t"),
            0x0a => out.extend_from_slice(b"\\n"),
            0x0c => out.extend_from_slice(b"\\f"),
            0x0d => out.extend_from_slice(b"\\r"),
            b if b < 0x20 => {
                out.extend_from_slice(b"\\u00");
                let hi = b >> 4;
                let lo = b & 0xf;
                out.push(hex_nibble(hi));
                out.push(hex_nibble(lo));
            }
            b => out.push(b),
        }
    }
    out.push(b'"');
}

fn hex_nibble(n: u8) -> u8 {
    match n {
        0..=9 => b'0' + n,
        _ => b'a' + (n - 10),
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("series_id", DataType::Int64, false),
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("labels_canonical", DataType::Utf8, false),
        Field::new("labels_json", DataType::Utf8, false),
    ]))
}

fn build_batch(rows: &[Row]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut series_id = Int64Builder::with_capacity(rows.len());
    let mut ts = Int64Builder::with_capacity(rows.len());
    let mut value = Float64Builder::with_capacity(rows.len());
    let mut metric_name = StringBuilder::with_capacity(rows.len(), 0);
    let mut labels_canonical = StringBuilder::with_capacity(rows.len(), 0);
    let mut labels_json = StringBuilder::with_capacity(rows.len(), 0);

    for row in rows {
        series_id.append_value(row.series_id);
        ts.append_value(row.timestamp_ns);
        value.append_value(row.value);
        // Documented contract on the Elixir side: label names and values
        // are UTF-8. The JSON encoder sanitises them lossily so this
        // string column is always valid UTF-8. The canonical column has
        // the raw bytes with 0xFF separators, which is also valid UTF-8
        // only if the names/values are UTF-8 — Prometheus rejects
        // non-UTF-8 upstream, so we accept the risk and let a malformed
        // segment fail decode loudly if it ever arrives.
        append_string(&mut metric_name, &row.metric_name);
        append_string(&mut labels_canonical, &row.labels_canonical);
        append_string(&mut labels_json, &row.labels_json);
    }

    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(series_id.finish()),
            Arc::new(ts.finish()),
            Arc::new(value.finish()),
            Arc::new(metric_name.finish()),
            Arc::new(labels_canonical.finish()),
            Arc::new(labels_json.finish()),
        ],
    )?;

    Ok(batch)
}

fn append_string(b: &mut StringBuilder, bytes: &[u8]) {
    match std::str::from_utf8(bytes) {
        Ok(s) => b.append_value(s),
        Err(_) => {
            // Lossy: convert each malformed byte to U+FFFD so the
            // segment still writes. The alternative — failing encode —
            // would drop the whole batch for one bad label and lose
            // acknowledged data.
            b.append_value(String::from_utf8_lossy(bytes));
        }
    }
}

fn writer_properties() -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_statistics_enabled(EnabledStatistics::Chunk);

    // Delta-binary-packed on the sort key: back-to-back samples for the
    // same series differ by a scrape interval, which packs down to a few
    // bits per row.
    builder = builder.set_column_encoding(
        ColumnPath::from("timestamp_ns"),
        Encoding::DELTA_BINARY_PACKED,
    );
    // Dictionary on `metric_name` and `labels_canonical`: thousands of
    // rows share a handful of distinct values each.
    builder = builder.set_column_dictionary_enabled(ColumnPath::from("metric_name"), true);
    builder = builder.set_column_dictionary_enabled(ColumnPath::from("labels_canonical"), true);

    builder.build()
}

pub fn decode<'a>(
    env: Env<'a>,
    blob: &Binary<'a>,
    filter: &Filter<'_>,
) -> Result<Term<'a>, DecodeError> {
    let bytes = Bytes::copy_from_slice(blob.as_slice());

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|_| DecodeError::Reader)?;
    let reader = builder.build().map_err(|_| DecodeError::Reader)?;

    let regex_cache: Vec<Option<regex::Regex>> = filter
        .matchers
        .iter()
        .map(|m| match m.op {
            MatcherOp::Re | MatcherOp::Nre => {
                let s = std::str::from_utf8(m.value).ok()?;
                regex::Regex::new(s).ok()
            }
            _ => None,
        })
        .collect();

    let mut records: Vec<Term<'a>> = Vec::new();

    let keys = [
        atoms::struct_key().encode(env),
        atoms::series_id().encode(env),
        atoms::timestamp_ns().encode(env),
        atoms::value().encode(env),
        atoms::labels().encode(env),
    ];
    let struct_name = atoms::metric_sample().encode(env);

    for batch_result in reader {
        let batch = batch_result.map_err(|_| DecodeError::Reader)?;
        let series_id_arr = col::<Int64Array>(&batch, 0)?;
        let ts_arr = col::<Int64Array>(&batch, 1)?;
        let value_arr = col::<Float64Array>(&batch, 2)?;
        let labels_json_arr = col::<StringArray>(&batch, 5)?;

        for row in 0..batch.num_rows() {
            let ts = ts_arr.value(row) as i128;
            if let Some(start) = filter.start {
                if ts < start {
                    continue;
                }
            }
            if let Some(end) = filter.end {
                if ts > end {
                    continue;
                }
            }

            let labels_json = labels_json_arr.value(row);
            let labels_map = parse_label_json(labels_json.as_bytes()).ok_or(DecodeError::Reader)?;

            if !labels_match(&labels_map, &filter.matchers, &regex_cache) {
                continue;
            }

            let series_id = series_id_arr.value(row);
            let value = value_arr.value(row);

            let mut names: Vec<Term<'a>> = Vec::with_capacity(labels_map.len());
            let mut values: Vec<Term<'a>> = Vec::with_capacity(labels_map.len());
            for (k, v) in &labels_map {
                names.push(copy(env, k));
                values.push(copy(env, v));
            }
            let labels_term = Term::map_from_arrays(env, &names, &values).map_err(|_| DecodeError::Reader)?;

            let fields = [
                struct_name,
                series_id.encode(env),
                ts_arr.value(row).encode(env),
                value.encode(env),
                labels_term,
            ];
            records.push(Term::map_from_arrays(env, &keys, &fields).map_err(|_| DecodeError::Reader)?);
        }
    }

    Ok(records.encode(env))
}

fn col<T: 'static>(batch: &RecordBatch, idx: usize) -> Result<&T, DecodeError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or(DecodeError::Reader)
}

fn copy<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let mut out = NewBinary::new(env, bytes.len());
    out.as_mut_slice().copy_from_slice(bytes);
    Binary::from(out).encode(env)
}

fn labels_match(
    labels: &[(Vec<u8>, Vec<u8>)],
    matchers: &[Matcher<'_>],
    regex_cache: &[Option<regex::Regex>],
) -> bool {
    for (m, re) in matchers.iter().zip(regex_cache.iter()) {
        let got: Option<&[u8]> = labels
            .iter()
            .find(|(k, _)| k.as_slice() == m.name)
            .map(|(_, v)| v.as_slice());

        let ok = match m.op {
            MatcherOp::Eq => got.map(|v| v == m.value).unwrap_or(m.value.is_empty()),
            MatcherOp::Neq => got.map(|v| v != m.value).unwrap_or(!m.value.is_empty()),
            MatcherOp::Re => match (got, re) {
                (Some(v), Some(re)) => std::str::from_utf8(v).map(|s| re.is_match(s)).unwrap_or(false),
                _ => false,
            },
            MatcherOp::Nre => match (got, re) {
                (Some(v), Some(re)) => std::str::from_utf8(v).map(|s| !re.is_match(s)).unwrap_or(true),
                _ => true,
            },
        };

        if !ok {
            return false;
        }
    }
    true
}

// Minimal JSON object parser specialised to our writer's output
// (`{"k":"v",...}` with simple backslash escapes, no numbers, no
// nested objects). This stays in-crate so the metric read path does
// not depend on `Pulso.JSON`'s term builder for a tiny, bounded shape.
fn parse_label_json(input: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut i = 0;
    skip_ws(input, &mut i);
    if input.get(i) != Some(&b'{') {
        return None;
    }
    i += 1;
    let mut out = Vec::new();

    skip_ws(input, &mut i);
    if input.get(i) == Some(&b'}') {
        return Some(out);
    }

    loop {
        skip_ws(input, &mut i);
        let k = parse_json_string(input, &mut i)?;
        skip_ws(input, &mut i);
        if input.get(i) != Some(&b':') {
            return None;
        }
        i += 1;
        skip_ws(input, &mut i);
        let v = parse_json_string(input, &mut i)?;
        out.push((k, v));
        skip_ws(input, &mut i);
        match input.get(i) {
            Some(&b',') => {
                i += 1;
                continue;
            }
            Some(&b'}') => {
                return Some(out);
            }
            _ => return None,
        }
    }
}

fn skip_ws(input: &[u8], i: &mut usize) {
    while let Some(&b) = input.get(*i) {
        if matches!(b, b' ' | b'\t' | b'\n' | b'\r') {
            *i += 1;
        } else {
            break;
        }
    }
}

fn parse_json_string(input: &[u8], i: &mut usize) -> Option<Vec<u8>> {
    if input.get(*i) != Some(&b'"') {
        return None;
    }
    *i += 1;
    let mut out = Vec::new();
    while *i < input.len() {
        let b = input[*i];
        if b == b'"' {
            *i += 1;
            return Some(out);
        }
        if b == b'\\' {
            *i += 1;
            let esc = *input.get(*i)?;
            *i += 1;
            match esc {
                b'"' => out.push(b'"'),
                b'\\' => out.push(b'\\'),
                b'/' => out.push(b'/'),
                b'b' => out.push(0x08),
                b't' => out.push(0x09),
                b'n' => out.push(0x0a),
                b'f' => out.push(0x0c),
                b'r' => out.push(0x0d),
                b'u' => {
                    let hi = hex_u16(input, i)?;
                    // Only accept a plain BMP code point; we never emit
                    // surrogates on encode. If one shows up, bail — this
                    // is a trusted-input path.
                    if (0xd800..=0xdfff).contains(&hi) {
                        return None;
                    }
                    push_utf8(&mut out, hi as u32);
                }
                _ => return None,
            }
            continue;
        }
        out.push(b);
        *i += 1;
    }
    None
}

fn hex_u16(input: &[u8], i: &mut usize) -> Option<u16> {
    if *i + 4 > input.len() {
        return None;
    }
    let mut v: u16 = 0;
    for off in 0..4 {
        let digit = input[*i + off];
        v = v.checked_mul(16)?.checked_add(hex_val(digit)? as u16)?;
    }
    *i += 4;
    Some(v)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + b - b'a'),
        b'A'..=b'F' => Some(10 + b - b'A'),
        _ => None,
    }
}

fn push_utf8(out: &mut Vec<u8>, cp: u32) {
    if cp < 0x80 {
        out.push(cp as u8);
    } else if cp < 0x800 {
        out.push(0xc0 | (cp >> 6) as u8);
        out.push(0x80 | (cp & 0x3f) as u8);
    } else {
        out.push(0xe0 | (cp >> 12) as u8);
        out.push(0x80 | ((cp >> 6) & 0x3f) as u8);
        out.push(0x80 | (cp & 0x3f) as u8);
    }
}
