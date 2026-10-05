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
    Array, BinaryArray, BinaryBuilder, Float64Array, Float64Builder, Int64Array, Int64Builder,
    RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnPath;
use rustler::{Binary, Encoder, Env, ListIterator, MapIterator, NewBinary, Term};
use std::rc::Rc;
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
    pub max_samples: Option<usize>,
}

struct Row {
    series_id: i64,
    timestamp_ns: i64,
    value: f64,
    labels: Rc<EncodedLabels>,
}

struct EncodedLabels {
    series_hash: i64,
    metric_name: Vec<u8>,
    canonical: Vec<u8>,
    json: Vec<u8>,
}

#[derive(Debug)]
pub enum EncodeError {
    BadInput,
    Writer,
}

#[derive(Debug)]
pub enum DecodeError {
    Reader,
    InvalidRegex,
    TooManySamples,
    NonFiniteValue,
}

pub fn encode<'a>(env: Env<'a>, samples: Term<'a>) -> Result<(Vec<u8>, Bounds), EncodeError> {
    let len = samples.list_length().map_err(|_| EncodeError::BadInput)?;
    let items: ListIterator = samples.decode().map_err(|_| EncodeError::BadInput)?;
    let struct_key = atoms::struct_key().encode(env);
    let expected = atoms::metric_sample().encode(env);

    let mut rows: Vec<Row> = Vec::with_capacity(len);
    let mut previous_labels: Option<(Term<'a>, Rc<EncodedLabels>)> = None;
    let mut bounds = Bounds {
        min_ts: i128::MAX,
        max_ts: i128::MIN,
        count: 0,
    };

    for sample in items {
        let tag = sample
            .map_get(struct_key)
            .map_err(|_| EncodeError::BadInput)?;
        if tag.as_c_arg() != expected.as_c_arg() {
            return Err(EncodeError::BadInput);
        }
        let row = extract_row(env, sample, &mut bounds, &mut previous_labels)?;
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
    // Arrow owns the finished columns now. Do not retain the input row
    // metadata through compression, when writer buffers are at their peak.
    drop(rows);
    drop(previous_labels);
    let mut buf: Vec<u8> = Vec::with_capacity(64 + len * 32);
    let props = writer_properties();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
        .map_err(|_| EncodeError::Writer)?;
    writer.write(&batch).map_err(|_| EncodeError::Writer)?;
    writer.close().map_err(|_| EncodeError::Writer)?;

    Ok((buf, bounds))
}

fn extract_row<'a>(
    env: Env<'a>,
    sample: Term<'a>,
    bounds: &mut Bounds,
    previous_labels: &mut Option<(Term<'a>, Rc<EncodedLabels>)>,
) -> Result<Row, EncodeError> {
    let nil = atoms::nil().encode(env);

    let labels_term = sample
        .map_get(atoms::labels().encode(env))
        .map_err(|_| EncodeError::BadInput)?;

    // Compare complete maps, never series IDs. Reuse derived bytes for a
    // consecutive run even when equivalent maps are separate Erlang terms.
    // Only one lookup entry is retained; rows share immutable metadata.
    let labels = match previous_labels.as_ref() {
        Some((previous, labels)) if *previous == labels_term => Rc::clone(labels),
        _ => {
            let labels = Rc::new(encode_labels(labels_term)?);
            *previous_labels = Some((labels_term, Rc::clone(&labels)));
            labels
        }
    };

    let series_id_term = sample
        .map_get(atoms::series_id().encode(env))
        .map_err(|_| EncodeError::BadInput)?;

    // Caller may pre-fill series_id or leave it nil; recompute on nil.
    let series_id: i64 = if series_id_term.as_c_arg() == nil.as_c_arg() {
        labels.series_hash
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
    let value: f64 = value_term
        .decode::<f64>()
        .or_else(|_| value_term.decode::<i64>().map(|i| i as f64))
        .map_err(|_| EncodeError::BadInput)?;
    if !value.is_finite() {
        return Err(EncodeError::BadInput);
    }

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
        labels,
    })
}

fn encode_labels<'a>(labels_term: Term<'a>) -> Result<EncodedLabels, EncodeError> {
    // Erlang binaries stay alive for this NIF call. A map guarantees unique
    // keys; only byte ordering is needed, with no owned per-label hash map.
    let iter = MapIterator::new(labels_term).ok_or(EncodeError::BadInput)?;
    let mut pairs: Vec<(&[u8], &[u8])> = Vec::new();
    for (k, v) in iter {
        let kb: Binary<'a> = k.decode().map_err(|_| EncodeError::BadInput)?;
        let vb: Binary<'a> = v.decode().map_err(|_| EncodeError::BadInput)?;
        pairs.push((kb.as_slice(), vb.as_slice()));
    }
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let metric_name = pairs.iter()
        .find(|(name, _)| *name == b"__name__")
        .map(|(_, value)| value.to_vec())
        .unwrap_or_default();
    Ok(EncodedLabels {
        series_hash: stable_hash::stable_hash(pairs.iter().copied()) as i64,
        metric_name,
        canonical: canonical_bytes(&pairs),
        json: labels_as_json(&pairs),
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
        // `labels_canonical` is `Binary`, not `Utf8`, specifically so
        // the 0xff byte we use as the field separator — identical to
        // Prometheus's `labels.StableHash` separator in
        // `prometheus/prometheus/model/labels/labels_common.go` — can
        // appear verbatim. Writing it to a Utf8 column would force an
        // `invalid utf-8 → U+FFFD` lossy replacement on encode and
        // break the exact-bytes contract that lets decode return
        // sub-binaries of the arena without a second parse.
        Field::new("labels_canonical", DataType::Binary, false),
        Field::new("labels_json", DataType::Utf8, false),
    ]))
}

fn build_batch(rows: &[Row]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut series_id = Int64Builder::with_capacity(rows.len());
    let mut ts = Int64Builder::with_capacity(rows.len());
    let mut value = Float64Builder::with_capacity(rows.len());
    let mut metric_name = StringBuilder::with_capacity(rows.len(), 0);
    let mut labels_canonical = BinaryBuilder::with_capacity(rows.len(), 0);
    let mut labels_json = StringBuilder::with_capacity(rows.len(), 0);

    for row in rows {
        series_id.append_value(row.series_id);
        ts.append_value(row.timestamp_ns);
        value.append_value(row.value);
        // `metric_name` and `labels_json` are UTF-8 by construction
        // (Prometheus restricts metric names, and our JSON encoder
        // escapes any sub-0x20 bytes), so the StringBuilder path is
        // the common case. `labels_canonical` is raw bytes.
        append_string(&mut metric_name, &row.labels.metric_name);
        labels_canonical.append_value(&row.labels.canonical);
        append_string(&mut labels_json, &row.labels.json);
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
        .set_statistics_enabled(EnabledStatistics::Chunk)
        // Smaller row groups than parquet-rs's default (1M rows): the
        // metric read path prunes by row-group `timestamp_ns` stats
        // before any column pages are read, so narrower groups let
        // a tighter time filter skip proportionally more of the
        // segment. 8192 rows is a decent compromise — large enough
        // that zstd on each group still gets its context, small
        // enough that a 10 000-sample segment splits into two groups.
        .set_max_row_group_size(8192);

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

// Per-batch arena for a Binary column: one Erlang `NewBinary` holds
// the entire column's underlying bytes, and `sub_binary` returns
// zero-copy slices into it. For a batch of N rows we go from N
// allocations per column to one — same contract as
// `segment_parquet::StringArena`.
struct BinaryArena<'a> {
    arena: Option<Binary<'a>>,
    offsets: Vec<i32>,
}

impl<'a> BinaryArena<'a> {
    fn from(env: Env<'a>, col: &BinaryArray) -> Self {
        let bytes = col.value_data();
        let arena = if bytes.is_empty() {
            None
        } else {
            let mut nb = NewBinary::new(env, bytes.len());
            nb.as_mut_slice().copy_from_slice(bytes);
            Some(Binary::from(nb))
        };
        BinaryArena {
            arena,
            offsets: col.value_offsets().to_vec(),
        }
    }

    fn row_bounds(&self, row: usize) -> (usize, usize) {
        (self.offsets[row] as usize, self.offsets[row + 1] as usize)
    }

    fn sub_binary(&self, start: usize, len: usize) -> Result<Binary<'a>, DecodeError> {
        match &self.arena {
            Some(a) => a
                .make_subbinary(start, len)
                .map_err(|_| DecodeError::Reader),
            None if len == 0 => Err(DecodeError::Reader),
            None => Err(DecodeError::Reader),
        }
    }

    fn arena_bytes(&self) -> Option<&[u8]> {
        self.arena.as_ref().map(|a| a.as_slice())
    }
}

pub fn decode<'a>(
    env: Env<'a>,
    blob: &Binary<'a>,
    filter: &Filter<'_>,
) -> Result<Term<'a>, DecodeError> {
    let regex_cache = compile_regexes(&filter.matchers)?;
    let bytes = Bytes::copy_from_slice(blob.as_slice());

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|_| DecodeError::Reader)?;

    // Column projection: only the four columns the decoder actually
    // reads. `metric_name` and `labels_json` stay in the file but are
    // never materialised, decompressed, or allocated into Arrow
    // buffers. After the mask is applied the returned batch has
    // these four columns in projection order:
    //
    //     0 → series_id
    //     1 → timestamp_ns
    //     2 → value
    //     3 → labels_canonical
    //
    // `metric_name` sits between `value` and `labels_canonical` in the
    // on-disk schema (position 3), so dropping it renumbers the two
    // columns after it; the indices below reflect the post-projection
    // order and must stay in sync with the mask.
    let projection =
        parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), [0usize, 1, 2, 4]);

    // Row-group pruning: when a time filter is set, consult each row
    // group's `timestamp_ns` min/max chunk stats and only read the
    // groups that could contain surviving rows. The segment writer
    // caps groups at 8192 rows, so a tight filter reads a strict
    // fraction of the segment and skips the rest without opening any
    // column pages.
    let metadata = builder.metadata().clone();
    let ts_col: Option<usize> = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.name() == "timestamp_ns");
    let time_filter = filter.start.is_some() || filter.end.is_some();
    let surviving: Vec<usize> = (0..metadata.num_row_groups())
        .filter(|&i| {
            if !time_filter {
                return true;
            }
            row_group_matches_time(metadata.row_group(i), ts_col, filter)
        })
        .collect();

    if surviving.is_empty() {
        return Ok(Vec::<Term<'a>>::new().encode(env));
    }

    let reader = builder
        .with_projection(projection)
        .with_row_groups(surviving)
        .build()
        .map_err(|_| DecodeError::Reader)?;

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
        // Post-projection column 3 is `labels_canonical` (`metric_name`
        // and `labels_json` were masked out). `labels_canonical` carries
        // the raw `name<0xff>value<0xff>...` bytes we hashed into
        // `series_id`, so every label slice is returned as a sub-binary
        // of the arena without a per-row JSON parse.
        let labels_arr = col::<BinaryArray>(&batch, 3)?;
        let labels_arena = BinaryArena::from(env, labels_arr);
        // The whole-column byte view — the per-row canonical slices we
        // scan during the row loop point into this slice.
        let labels_bytes: &[u8] = labels_arena.arena_bytes().unwrap_or(&[]);
        let base = labels_bytes.as_ptr() as usize;

        // Scratch buffers reused across rows within the batch to avoid
        // per-row Vec allocs. Scope is per-batch so the slice lifetimes
        // in `label_pairs` do not escape `labels_bytes`'s borrow.
        // Prometheus series rarely exceed a dozen labels, so 16 is a
        // comfortable upper bound to avoid reallocation.
        let mut name_terms: Vec<Term<'a>> = Vec::with_capacity(16);
        let mut value_terms: Vec<Term<'a>> = Vec::with_capacity(16);
        let mut label_pairs: Vec<(&[u8], &[u8])> = Vec::with_capacity(16);
        // Cache just the previous canonical label set, not the series hash:
        // hashes may collide and callers may supply their own series IDs.
        // Runs of samples from one series share both matcher work and the
        // immutable Erlang labels map. The cache is bounded to one entry.
        let mut previous_labels: Option<&[u8]> = None;
        let mut previous_matches = false;
        let mut previous_term: Option<Term<'a>> = None;

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

            let (row_start, row_end) = labels_arena.row_bounds(row);

            let row_bytes = &labels_bytes[row_start..row_end];
            if previous_labels != Some(row_bytes) {
                label_pairs.clear();
                parse_canonical_pairs(row_bytes, &mut label_pairs)
                    .map_err(|_| DecodeError::Reader)?;
                previous_labels = Some(row_bytes);
                previous_matches = labels_match(&label_pairs, &filter.matchers, &regex_cache);
                previous_term = None;
            }

            if !previous_matches {
                continue;
            }

            if filter.max_samples.is_some_and(|max| records.len() >= max) {
                return Err(DecodeError::TooManySamples);
            }
            let series_id = series_id_arr.value(row);
            let value = value_arr.value(row);
            if !value.is_finite() {
                return Err(DecodeError::NonFiniteValue);
            }

            // Rebuild label terms as sub-binaries of the arena. The
            // slice pointer arithmetic recovers each slice's absolute
            // offset inside the arena so the sub-binary points at the
            // right bytes — no second copy.
            let labels_term = match previous_term {
                Some(term) => term,
                None => {
                    name_terms.clear();
                    value_terms.clear();
                    for (name, value) in &label_pairs {
                        let name_off = (name.as_ptr() as usize) - base;
                        let value_off = (value.as_ptr() as usize) - base;
                        name_terms.push(labels_arena.sub_binary(name_off, name.len())?.encode(env));
                        value_terms.push(labels_arena.sub_binary(value_off, value.len())?.encode(env));
                    }
                    let term = Term::map_from_arrays(env, &name_terms, &value_terms)
                        .map_err(|_| DecodeError::Reader)?;
                    previous_term = Some(term);
                    term
                }
            };

            let fields = [
                struct_name,
                series_id.encode(env),
                ts_arr.value(row).encode(env),
                value.encode(env),
                labels_term,
            ];
            records
                .push(Term::map_from_arrays(env, &keys, &fields).map_err(|_| DecodeError::Reader)?);
        }
    }

    Ok(records.encode(env))
}

// Split a row's canonical bytes (`name<0xff>value<0xff>...`) into
// borrowed `(name, value)` pairs. Returns `Err(())` on a stray trailing
// byte or a missing separator — a well-formed segment never produces
// either.
fn parse_canonical_pairs<'r>(
    bytes: &'r [u8],
    out: &mut Vec<(&'r [u8], &'r [u8])>,
) -> Result<(), ()> {
    let mut i = 0;
    while i < bytes.len() {
        let name_start = i;
        let name_end = memchr::memchr(crate::stable_hash::SEP, &bytes[i..]).ok_or(())? + i;
        let value_start = name_end + 1;
        if value_start > bytes.len() {
            return Err(());
        }
        let value_end =
            memchr::memchr(crate::stable_hash::SEP, &bytes[value_start..]).ok_or(())? + value_start;
        out.push((&bytes[name_start..name_end], &bytes[value_start..value_end]));
        i = value_end + 1;
    }
    Ok(())
}

fn col<T: 'static>(batch: &RecordBatch, idx: usize) -> Result<&T, DecodeError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or(DecodeError::Reader)
}

/// True when the row group's `timestamp_ns` column stats overlap the
/// filter range, so the group might contain matching records. Missing
/// stats, or non-integer stats (shouldn't happen for an Int64 column
/// written by our own writer), mean we don't know — err on the side
/// of scanning. Mirrors `segment_parquet::row_group_matches_time`.
fn row_group_matches_time(
    rg: &RowGroupMetaData,
    ts_col: Option<usize>,
    filter: &Filter<'_>,
) -> bool {
    let Some(col) = ts_col else {
        return true;
    };
    let Some(stats) = rg.column(col).statistics() else {
        return true;
    };
    let Statistics::Int64(s) = stats else {
        return true;
    };
    let min = s.min_opt().map(|v| i128::from(*v));
    let max = s.max_opt().map(|v| i128::from(*v));
    if let (Some(start), Some(max)) = (filter.start, max) {
        if max < start {
            return false;
        }
    }
    if let (Some(end), Some(min)) = (filter.end, min) {
        if min > end {
            return false;
        }
    }
    true
}

/// Prometheus uses ASCII Perl classes and word boundaries. Rust's defaults
/// are Unicode, so translate these escapes before compiling for both adapters.
pub fn compile_metric_regex(pattern: &str) -> Result<regex::Regex, regex::Error> {
    reject_class_extensions(pattern)?;
    let mut translated = String::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            translated.push(ch);
            continue;
        }
        match chars.next() {
            Some('d') => translated.push_str("[0-9]"),
            Some('D') => translated.push_str("[^0-9]"),
            Some('w') => translated.push_str("[A-Za-z0-9_]"),
            Some('W') => translated.push_str("[^A-Za-z0-9_]"),
            Some('s') => translated.push_str(r"[\t\n\f\r ]"),
            Some('S') => translated.push_str(r"[^\t\n\f\r ]"),
            Some('b') => translated.push_str(r"(?-u:\b)"),
            Some('B') => translated.push_str(r"(?-u:\B)"),
            Some(next) => {
                translated.push('\\');
                translated.push(next);
            }
            None => translated.push('\\'),
        }
    }
    regex::RegexBuilder::new(&translated)
        .size_limit(1_048_576)
        .dfa_size_limit(1_048_576)
        .build()
}

// Rust class intersection/difference and nested classes have different
// meanings from Prometheus classes. Reject them instead of returning wrong
// matches. POSIX named classes remain supported.
fn reject_class_extensions(pattern: &str) -> Result<(), regex::Error> {
    let mut depth = 0usize;
    let mut escaped = false;
    let mut previous = '\0';
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        if escaped {
            escaped = false;
            previous = '\0';
            continue;
        }
        if ch == '\\' {
            escaped = true;
            previous = '\0';
            continue;
        }
        if (depth > 0 && ch == previous && matches!(ch, '&' | '-' | '~'))
            || (depth > 0 && ch == '[' && chars.peek() != Some(&':'))
            || (depth > 0 && ch == ']' && matches!(previous, '[' | '^'))
        {
            return Err(regex::Error::Syntax(
                "Unsupported character-class extension".into(),
            ));
        }
        if ch == '[' {
            depth += 1;
        }
        if ch == ']' {
            depth = depth.saturating_sub(1);
        }
        previous = ch;
    }
    Ok(())
}

fn compile_regexes(matchers: &[Matcher<'_>]) -> Result<Vec<Option<regex::Regex>>, DecodeError> {
    matchers
        .iter()
        .map(|m| match m.op {
            MatcherOp::Re | MatcherOp::Nre => {
                let pattern =
                    std::str::from_utf8(m.value).map_err(|_| DecodeError::InvalidRegex)?;
                compile_metric_regex(pattern)
                    .map(Some)
                    .map_err(|_| DecodeError::InvalidRegex)
            }
            _ => Ok(None),
        })
        .collect()
}

fn labels_match(
    labels: &[(&[u8], &[u8])],
    matchers: &[Matcher<'_>],
    regex_cache: &[Option<regex::Regex>],
) -> bool {
    for (m, re) in matchers.iter().zip(regex_cache.iter()) {
        let got: Option<&[u8]> = labels.iter().find(|(k, _)| *k == m.name).map(|(_, v)| *v);

        let ok = match m.op {
            MatcherOp::Eq => got.map(|v| v == m.value).unwrap_or(m.value.is_empty()),
            MatcherOp::Neq => got.map(|v| v != m.value).unwrap_or(!m.value.is_empty()),
            MatcherOp::Re | MatcherOp::Nre => {
                let Some(re) = re else {
                    return false;
                };
                let actual = std::str::from_utf8(got.unwrap_or(b""));
                let matched = actual.map(|value| re.is_match(value)).unwrap_or(false);
                if matches!(m.op, MatcherOp::Re) {
                    matched
                } else {
                    !matched
                }
            }
        };

        if !ok {
            return false;
        }
    }
    true
}
