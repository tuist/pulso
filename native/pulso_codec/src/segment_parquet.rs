//! Parquet log segment encoding and decoding.
//!
//! Fixed 10-column schema (Int64 / Int32 / Utf8 only), one row group per
//! segment, rows sorted by `(service, timestamp_ns)` on write so the
//! dictionary + zstd column encoding compresses tightly and the row-group
//! stats let a later query skip the whole segment. `attributes` and
//! `resource` always encode to a JSON string (empty map is `"{}"`);
//! `body` is a nullable JSON-encoded value.
//!
//! Memory contract:
//!   * Encode drops UTF-8 validation on `JsonEncoder` output — that
//!     encoder is documented to always emit valid UTF-8, and the O(n)
//!     re-scan of every JSON blob is not free.
//!   * Decode allocates one Erlang binary per string column per batch
//!     (the "arena"), and every returned string is a sub-binary of that
//!     arena. Cost: 7 fresh binaries per batch instead of 7 × row-count.
//!     A caller that retains a returned term keeps the whole column
//!     arena alive — callers past the query boundary should
//!     `:binary.copy/1` if that matters.
//!   * Row-group `timestamp_ns` min/max stats prune whole row groups
//!     before any column pages are read, so a segment outside the query
//!     range decodes to `[]` with no column-buffer allocations at all.
//!
//! Follow-ups worth doing when the query path is on the hot list:
//!   * Wrap the input Erlang binary in a `Bytes` without copying (needs
//!     ref-counted access, so either `rustler_sys` or a wrapper crate).
//!     Would remove one full-blob memcpy per read.
//!   * Arena the JSON blobs on encode too so the sort is by indices,
//!     not by owned strings.

use crate::json_read::{Fallback, Parser, Res, Stacks};
use crate::query_filter::{find_label, line_bytes_for_match, LineFilter, Matcher};
use crate::term_json::{Enc, EncodeError, JsonEncoder, TermBuilder};

use arrow::array::{
    Array, Int32Array, Int32Builder, Int64Array, Int64Builder, RecordBatch, StringArray,
    StringBuilder,
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
use rustler::types::atom;
use rustler::{Binary, Encoder, Env, ListIterator, NewBinary, Term};
use std::sync::Arc;

mod atoms {
    rustler::atoms! {
        struct_key = "__struct__",
        log = "Elixir.Pulso.Record.Log",
        timestamp_ns,
        observed_timestamp_ns,
        severity_number,
        severity_text,
        service,
        body,
        trace_id,
        span_id,
        attributes,
        resource,
    }
}

pub struct Bounds {
    pub min_ts: i128,
    pub max_ts: i128,
    pub count: usize,
}

pub struct Filter<'f> {
    pub start: Option<i128>,
    pub end: Option<i128>,
    pub service: Option<&'f [u8]>,
    pub matchers: Vec<Matcher>,
    pub line_filters: Vec<LineFilter>,
}

struct Row {
    timestamp_ns: Option<i64>,
    observed_timestamp_ns: Option<i64>,
    severity_number: Option<i32>,
    severity_text: Option<Vec<u8>>,
    service: Option<Vec<u8>>,
    // JSON-encoded body value, `None` when the caller passed nil.
    body: Option<Vec<u8>>,
    trace_id: Option<Vec<u8>>,
    span_id: Option<Vec<u8>>,
    // JSON-encoded map, always populated (empty map is `"{}"`).
    attributes: Vec<u8>,
    resource: Vec<u8>,
}

pub fn encode<'a>(env: Env<'a>, records: Term<'a>) -> Enc<(Vec<u8>, Bounds)> {
    let len = records.list_length().map_err(|_| EncodeError::Fallback)?;
    let items: ListIterator = records.decode().map_err(|_| EncodeError::Fallback)?;
    let struct_key = atoms::struct_key().encode(env);
    let log = atoms::log().encode(env);
    let enc = JsonEncoder::new(env, usize::MAX);

    let mut rows: Vec<Row> = Vec::with_capacity(len);
    let mut bounds = Bounds {
        min_ts: i128::MAX,
        max_ts: i128::MIN,
        count: 0,
    };
    // Reused scratch buffer for JSON encoding, one allocation for the
    // whole batch instead of one per row × per string column.
    let mut scratch: Vec<u8> = Vec::with_capacity(256);

    for record in items {
        let tag = record
            .map_get(struct_key)
            .map_err(|_| EncodeError::Fallback)?;
        if tag.as_c_arg() != log.as_c_arg() {
            return Err(EncodeError::Fallback);
        }
        let row = extract_row(env, record, &enc, &mut bounds, &mut scratch)?;
        rows.push(row);
    }

    if bounds.count == 0 {
        bounds.min_ts = 0;
        bounds.max_ts = 0;
    }

    // Sort by (service, timestamp_ns) so records for the same service sit
    // next to each other on disk — dictionary + zstd on `service` and
    // delta encoding on `timestamp_ns` do most of their work here.
    rows.sort_by(|a, b| {
        a.service
            .cmp(&b.service)
            .then(a.timestamp_ns.cmp(&b.timestamp_ns))
    });

    let batch = build_batch(&rows).map_err(|_| EncodeError::Fallback)?;
    // Rough starting size: 64 B header + 32 B per row. Zstd will shrink
    // it further; growing the Vec is cheap enough that under-sizing is
    // fine, but starting large avoids the first few reallocs.
    let mut buf: Vec<u8> = Vec::with_capacity(64 + rows.len() * 32);
    let props = writer_properties();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
        .map_err(|_| EncodeError::Fallback)?;
    writer.write(&batch).map_err(|_| EncodeError::Fallback)?;
    writer.close().map_err(|_| EncodeError::Fallback)?;

    Ok((buf, bounds))
}

fn extract_row<'a>(
    env: Env<'a>,
    record: Term<'a>,
    enc: &JsonEncoder<'a>,
    bounds: &mut Bounds,
    scratch: &mut Vec<u8>,
) -> Enc<Row> {
    let ts = extract_i64(record, atoms::timestamp_ns().encode(env), enc)?;
    let observed = extract_i64(record, atoms::observed_timestamp_ns().encode(env), enc)?;
    let sev_num = extract_i32(record, atoms::severity_number().encode(env), enc)?;
    let sev_text = extract_binary(record, atoms::severity_text().encode(env), enc)?;
    let service = extract_binary(record, atoms::service().encode(env), enc)?;

    let body_term = record
        .map_get(atoms::body().encode(env))
        .map_err(|_| EncodeError::Fallback)?;
    let body = if enc.is_nil_or_false(body_term) {
        None
    } else {
        scratch.clear();
        enc.value(scratch, body_term, 1)?;
        Some(scratch.clone())
    };

    let trace_id = extract_binary(record, atoms::trace_id().encode(env), enc)?;
    let span_id = extract_binary(record, atoms::span_id().encode(env), enc)?;
    let attributes = extract_map_as_json(record, atoms::attributes().encode(env), enc, scratch)?;
    let resource = extract_map_as_json(record, atoms::resource().encode(env), enc, scratch)?;

    // The caller-supplied `timestamp_ns` maps `nil` to `0`, matching the
    // NDJSON path in `Pulso.Storage.S3.caller_ts_bounds/1`.
    let ts_for_bounds: i128 = i128::from(ts.unwrap_or(0));
    bounds.min_ts = bounds.min_ts.min(ts_for_bounds);
    bounds.max_ts = bounds.max_ts.max(ts_for_bounds);
    bounds.count += 1;

    Ok(Row {
        timestamp_ns: ts,
        observed_timestamp_ns: observed,
        severity_number: sev_num,
        severity_text: sev_text,
        service,
        body,
        trace_id,
        span_id,
        attributes,
        resource,
    })
}

fn extract_i64<'a>(record: Term<'a>, key: Term<'a>, enc: &JsonEncoder<'a>) -> Enc<Option<i64>> {
    let value = record.map_get(key).map_err(|_| EncodeError::Fallback)?;
    if enc.is_nil_or_false(value) {
        return Ok(None);
    }
    if let Ok(i) = value.decode::<i64>() {
        return Ok(Some(i));
    }
    let u = value.decode::<u64>().map_err(|_| EncodeError::Fallback)?;
    i64::try_from(u)
        .map(Some)
        .map_err(|_| EncodeError::Fallback)
}

fn extract_i32<'a>(record: Term<'a>, key: Term<'a>, enc: &JsonEncoder<'a>) -> Enc<Option<i32>> {
    let value = record.map_get(key).map_err(|_| EncodeError::Fallback)?;
    if enc.is_nil_or_false(value) {
        return Ok(None);
    }
    value
        .decode::<i32>()
        .map(Some)
        .map_err(|_| EncodeError::Fallback)
}

fn extract_binary<'a>(
    record: Term<'a>,
    key: Term<'a>,
    enc: &JsonEncoder<'a>,
) -> Enc<Option<Vec<u8>>> {
    let value = record.map_get(key).map_err(|_| EncodeError::Fallback)?;
    if enc.is_nil_or_false(value) {
        return Ok(None);
    }
    let bin: Binary = value.decode().map_err(|_| EncodeError::Fallback)?;
    // Arrow's StringBuilder validates UTF-8 on `append_value(&str)`, so
    // reject non-UTF-8 here to surface a clean Fallback rather than an
    // Arrow error mid-batch.
    simdutf8::basic::from_utf8(bin.as_slice()).map_err(|_| EncodeError::Fallback)?;
    Ok(Some(bin.as_slice().to_vec()))
}

fn extract_map_as_json<'a>(
    record: Term<'a>,
    key: Term<'a>,
    enc: &JsonEncoder<'a>,
    scratch: &mut Vec<u8>,
) -> Enc<Vec<u8>> {
    let value = record.map_get(key).map_err(|_| EncodeError::Fallback)?;
    if enc.is_nil_or_false(value) {
        return Ok(b"{}".to_vec());
    }
    if value.get_type() != rustler::TermType::Map {
        return Err(EncodeError::Fallback);
    }
    scratch.clear();
    enc.map(scratch, value, 1)?;
    Ok(scratch.clone())
}

fn parquet_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, true),
        Field::new("observed_timestamp_ns", DataType::Int64, true),
        Field::new("severity_number", DataType::Int32, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("service", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource", DataType::Utf8, false),
    ]))
}

// SAFETY on the `unsafe` calls below: each `Vec<u8>` in a Row was either
// (a) written by `JsonEncoder`, which is documented to emit only valid
// UTF-8, or (b) checked at `extract_binary` via `simdutf8::from_utf8`.
// The unchecked conversion elides an O(n) revalidation per string per
// row on the encode hot path.
fn build_batch(rows: &[Row]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut ts = Int64Builder::with_capacity(rows.len());
    let mut observed = Int64Builder::with_capacity(rows.len());
    let mut sev_num = Int32Builder::with_capacity(rows.len());
    let mut sev_text = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut service = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut body = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
    let mut trace_id = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut span_id = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut attributes = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
    let mut resource = StringBuilder::with_capacity(rows.len(), rows.len() * 32);

    for row in rows {
        ts.append_option(row.timestamp_ns);
        observed.append_option(row.observed_timestamp_ns);
        sev_num.append_option(row.severity_number);
        append_opt_bytes(&mut sev_text, row.severity_text.as_deref());
        append_opt_bytes(&mut service, row.service.as_deref());
        append_opt_bytes(&mut body, row.body.as_deref());
        append_opt_bytes(&mut trace_id, row.trace_id.as_deref());
        append_opt_bytes(&mut span_id, row.span_id.as_deref());
        append_bytes(&mut attributes, &row.attributes);
        append_bytes(&mut resource, &row.resource);
    }

    RecordBatch::try_new(
        parquet_schema(),
        vec![
            Arc::new(ts.finish()),
            Arc::new(observed.finish()),
            Arc::new(sev_num.finish()),
            Arc::new(sev_text.finish()),
            Arc::new(service.finish()),
            Arc::new(body.finish()),
            Arc::new(trace_id.finish()),
            Arc::new(span_id.finish()),
            Arc::new(attributes.finish()),
            Arc::new(resource.finish()),
        ],
    )
}

fn append_opt_bytes(builder: &mut StringBuilder, bytes: Option<&[u8]>) {
    match bytes {
        Some(b) => append_bytes(builder, b),
        None => builder.append_null(),
    }
}

fn append_bytes(builder: &mut StringBuilder, bytes: &[u8]) {
    // SAFETY: see the `build_batch` header comment.
    let s = unsafe { std::str::from_utf8_unchecked(bytes) };
    builder.append_value(s);
}

fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        // Delta packing on timestamps requires dictionary encoding off
        // for those columns; the Parquet spec disallows both at once.
        .set_column_encoding(
            ColumnPath::from("timestamp_ns"),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_dictionary_enabled(ColumnPath::from("timestamp_ns"), false)
        .set_column_encoding(
            ColumnPath::from("observed_timestamp_ns"),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_dictionary_enabled(ColumnPath::from("observed_timestamp_ns"), false)
        .set_column_dictionary_enabled(ColumnPath::from("severity_number"), true)
        .set_column_dictionary_enabled(ColumnPath::from("severity_text"), true)
        .set_column_dictionary_enabled(ColumnPath::from("service"), true)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build()
}

// -- decode --------------------------------------------------------------

pub fn decode<'a>(env: Env<'a>, blob: &Binary<'a>, filter: &Filter) -> Res<Vec<Term<'a>>> {
    // One copy from the Erlang heap into a Rust-owned `Bytes` so the
    // Parquet reader owns something with `'static` lifetime.
    let bytes = Bytes::copy_from_slice(blob.as_slice());
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|_| Fallback)?;
    let metadata = builder.metadata().clone();

    let ts_col = metadata
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
            let rg = metadata.row_group(i);
            row_group_matches_time(rg, ts_col, filter)
        })
        .collect();

    if surviving.is_empty() {
        return Ok(Vec::new());
    }

    let total_rows: usize = surviving
        .iter()
        .map(|&i| metadata.row_group(i).num_rows() as usize)
        .sum();
    let reader = builder
        .with_row_groups(surviving)
        .build()
        .map_err(|_| Fallback)?;

    let mut out = Vec::with_capacity(total_rows);
    for batch in reader {
        let batch = batch.map_err(|_| Fallback)?;
        materialize(env, &batch, filter, &mut out)?;
    }
    Ok(out)
}

// True when the row group's `timestamp_ns` column stats overlap the
// filter range, so the group might contain matching records. Missing or
// non-integer stats mean we don't know — err on the side of scanning.
fn row_group_matches_time(rg: &RowGroupMetaData, ts_col: Option<usize>, filter: &Filter) -> bool {
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

// Per-column arena: one Erlang binary holding every string in the
// column, plus the Arrow offset slice. Sub-binaries slice into the arena
// without another copy — for a batch of N rows we go from N alloc/copy
// per column to one. `'a` is the NIF env's lifetime (the arena binary);
// `'b` is the batch's lifetime (the Arrow references). The two are
// deliberately independent — sub-binaries returned from `sub_binary`
// carry only `'a`, so the batch can be dropped at the end of each
// materialise loop iteration while the returned terms outlive it.
struct StringArena<'a, 'b> {
    arena: Option<Binary<'a>>,
    offsets: &'b [i32],
    array: &'b StringArray,
}

impl<'a, 'b> StringArena<'a, 'b> {
    fn from(env: Env<'a>, col: &'b StringArray) -> Self {
        let bytes = col.value_data();
        let arena = if bytes.is_empty() {
            None
        } else {
            let mut nb = NewBinary::new(env, bytes.len());
            nb.as_mut_slice().copy_from_slice(bytes);
            Some(Binary::from(nb))
        };
        StringArena {
            arena,
            offsets: col.value_offsets(),
            array: col,
        }
    }

    fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    fn is_empty(&self, i: usize) -> bool {
        self.offsets[i + 1] == self.offsets[i]
    }

    fn sub_binary(&self, i: usize) -> Res<Binary<'a>> {
        let start = self.offsets[i] as usize;
        let end = self.offsets[i + 1] as usize;
        match &self.arena {
            Some(a) => a.make_subbinary(start, end - start).map_err(|_| Fallback),
            None => Err(Fallback),
        }
    }

    fn value_bytes(&self, i: usize) -> &'b [u8] {
        self.array.value(i).as_bytes()
    }
}

fn materialize<'a>(
    env: Env<'a>,
    batch: &RecordBatch,
    filter: &Filter,
    out: &mut Vec<Term<'a>>,
) -> Res<()> {
    let ts_col: &Int64Array = downcast_int64(batch, "timestamp_ns")?;
    let observed_col: &Int64Array = downcast_int64(batch, "observed_timestamp_ns")?;
    let sev_num_col: &Int32Array = downcast_int32(batch, "severity_number")?;
    let sev_text_col: &StringArray = downcast_string(batch, "severity_text")?;
    let service_col: &StringArray = downcast_string(batch, "service")?;
    let body_col: &StringArray = downcast_string(batch, "body")?;
    let trace_id_col: &StringArray = downcast_string(batch, "trace_id")?;
    let span_id_col: &StringArray = downcast_string(batch, "span_id")?;
    let attributes_col: &StringArray = downcast_string(batch, "attributes")?;
    let resource_col: &StringArray = downcast_string(batch, "resource")?;

    let sev_text = StringArena::from(env, sev_text_col);
    let service = StringArena::from(env, service_col);
    let body = StringArena::from(env, body_col);
    let trace_id = StringArena::from(env, trace_id_col);
    let span_id = StringArena::from(env, span_id_col);
    let attributes = StringArena::from(env, attributes_col);
    let resource = StringArena::from(env, resource_col);

    let nil = atom::nil().encode(env);
    let empty_map = rustler::types::map::map_new(env);
    let struct_key = atoms::struct_key().encode(env);
    let log = atoms::log().encode(env);
    let keys = field_atoms(env);

    let mut all_keys: Vec<Term<'a>> = Vec::with_capacity(11);
    all_keys.push(struct_key);
    all_keys.extend_from_slice(&keys);

    let time_filter = filter.start.is_some() || filter.end.is_some();
    let has_matchers = !filter.matchers.is_empty();
    let has_line_filters = !filter.line_filters.is_empty();
    let mut stacks: Stacks<Term<'a>> = Stacks::new();

    for i in 0..batch.num_rows() {
        // Apply per-row filters before building any Erlang terms so the
        // rejected rows cost nothing beyond an Arrow lookup.
        if time_filter {
            let keep = if ts_col.is_null(i) {
                false
            } else {
                let ts = i128::from(ts_col.value(i));
                filter.start.is_none_or(|s| ts >= s) && filter.end.is_none_or(|e| ts <= e)
            };
            if !keep {
                continue;
            }
        }
        if let Some(wanted) = filter.service {
            let matches = !service.is_null(i) && service.value_bytes(i) == wanted;
            if !matches {
                continue;
            }
        }
        if has_matchers && !matchers_pass(&filter.matchers, &resource, &service, &sev_text, i) {
            continue;
        }
        if has_line_filters && !line_filters_pass(&filter.line_filters, &body, i) {
            continue;
        }

        let ts_term = int_term(env, ts_col, i, nil);
        let observed_term = int_term(env, observed_col, i, nil);
        let sev_num_term = int32_term(env, sev_num_col, i, nil);
        let sev_text_term = sub_term(env, &sev_text, i, nil)?;
        let service_term = sub_term(env, &service, i, nil)?;
        let body_term = json_term(env, &body, i, nil, &mut stacks)?;
        let trace_id_term = sub_term(env, &trace_id, i, nil)?;
        let span_id_term = sub_term(env, &span_id, i, nil)?;
        let attributes_term = map_json_term(env, &attributes, i, empty_map, &mut stacks)?;
        let resource_term = map_json_term(env, &resource, i, empty_map, &mut stacks)?;

        // Field order matches `field_atoms/1`.
        let values = [
            log,
            attributes_term,
            body_term,
            observed_term,
            resource_term,
            service_term,
            sev_num_term,
            sev_text_term,
            span_id_term,
            ts_term,
            trace_id_term,
        ];
        let term = Term::map_from_arrays(env, &all_keys, &values).map_err(|_| Fallback)?;
        out.push(term);
    }
    Ok(())
}

// Evaluate every matcher against the row's `resource` JSON, plus the
// promoted-field mirror for `service`/`service_name`/`level`/
// `detected_level` (so an OTLP record that never populated `resource`
// but has typed struct fields still matches). All matchers must pass
// — stream selectors are conjunctive.
//
// The lookup is inlined here rather than extracted to a helper because
// the two arenas (`service`, `severity_text`) have distinct lifetime
// parameters that Rust's variance rules don't unify through a Cow
// return without extra copies.
fn matchers_pass(
    matchers: &[Matcher],
    resource: &StringArena,
    service: &StringArena,
    severity_text: &StringArena,
    i: usize,
) -> bool {
    for m in matchers {
        let name = m.name();
        let kept = match name {
            b"service" | b"service_name" => promoted_or_resource(name, service, resource, i, m),
            b"level" | b"detected_level" => {
                promoted_or_resource(name, severity_text, resource, i, m)
            }
            _ => match resource_lookup(resource, name, i) {
                Some(v) => m.evaluate_present(&v),
                None => m.evaluate_absent(),
            },
        };

        if !kept {
            return false;
        }
    }
    true
}

// Prefer the promoted typed column when present; fall back to a
// resource-JSON scan when it's null/empty.
fn promoted_or_resource(
    name: &[u8],
    promoted: &StringArena,
    resource: &StringArena,
    i: usize,
    matcher: &Matcher,
) -> bool {
    if !promoted.is_null(i) && !promoted.is_empty(i) {
        return matcher.evaluate_present(promoted.value_bytes(i));
    }
    match resource_lookup(resource, name, i) {
        Some(v) => matcher.evaluate_present(&v),
        None => matcher.evaluate_absent(),
    }
}

fn resource_lookup<'b>(
    resource: &StringArena<'_, 'b>,
    name: &[u8],
    i: usize,
) -> Option<std::borrow::Cow<'b, [u8]>> {
    if resource.is_null(i) || resource.is_empty(i) {
        return None;
    }
    find_label(resource.value_bytes(i), name)
}

// Every line filter must match. Conjunction, same as label matchers.
// Body is stored as JSON-encoded — for a string body that is the source
// text surrounded by `"`. We peel the quotes and unescape before
// matching so `|~ "^hello"` behaves the way a user reading the log
// line would expect, and so the Rust and Memory adapters produce
// identical result sets for identical LogQL.
fn line_filters_pass(line_filters: &[LineFilter], body: &StringArena, i: usize) -> bool {
    if body.is_null(i) || body.is_empty(i) {
        return line_filters.iter().all(|f| f.matches(b""));
    }
    let raw = body.value_bytes(i);
    let matched = line_bytes_for_match(raw);
    line_filters.iter().all(|f| f.matches(&matched))
}

// Same column order + atom order as `segment.rs` uses so the returned
// maps are indistinguishable from the NDJSON path's output.
fn field_atoms<'a>(env: Env<'a>) -> [Term<'a>; 10] {
    [
        atoms::attributes(),
        atoms::body(),
        atoms::observed_timestamp_ns(),
        atoms::resource(),
        atoms::service(),
        atoms::severity_number(),
        atoms::severity_text(),
        atoms::span_id(),
        atoms::timestamp_ns(),
        atoms::trace_id(),
    ]
    .map(|a| a.encode(env))
}

fn downcast_int64<'b>(batch: &'b RecordBatch, name: &str) -> Res<&'b Int64Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or(Fallback)
}

fn downcast_int32<'b>(batch: &'b RecordBatch, name: &str) -> Res<&'b Int32Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .ok_or(Fallback)
}

fn downcast_string<'b>(batch: &'b RecordBatch, name: &str) -> Res<&'b StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or(Fallback)
}

fn int_term<'a>(env: Env<'a>, col: &Int64Array, i: usize, nil: Term<'a>) -> Term<'a> {
    if col.is_null(i) {
        nil
    } else {
        col.value(i).encode(env)
    }
}

fn int32_term<'a>(env: Env<'a>, col: &Int32Array, i: usize, nil: Term<'a>) -> Term<'a> {
    if col.is_null(i) {
        nil
    } else {
        col.value(i).encode(env)
    }
}

fn sub_term<'a>(
    env: Env<'a>,
    arena: &StringArena<'a, '_>,
    i: usize,
    nil: Term<'a>,
) -> Res<Term<'a>> {
    if arena.is_null(i) {
        return Ok(nil);
    }
    if arena.is_empty(i) {
        // Sub-binary of an empty range would need an arena; just hand
        // back a fresh empty binary. No allocation savings to protect.
        let empty = NewBinary::new(env, 0);
        return Ok(Binary::from(empty).encode(env));
    }
    Ok(arena.sub_binary(i)?.encode(env))
}

fn json_term<'a>(
    env: Env<'a>,
    arena: &StringArena<'a, '_>,
    i: usize,
    nil: Term<'a>,
    stacks: &mut Stacks<Term<'a>>,
) -> Res<Term<'a>> {
    if arena.is_null(i) {
        return Ok(nil);
    }
    if arena.is_empty(i) {
        return Ok(nil);
    }
    parse_json_sub(env, arena, i, stacks)
}

fn map_json_term<'a>(
    env: Env<'a>,
    arena: &StringArena<'a, '_>,
    i: usize,
    empty_map: Term<'a>,
    stacks: &mut Stacks<Term<'a>>,
) -> Res<Term<'a>> {
    if arena.is_null(i) || arena.is_empty(i) {
        return Ok(empty_map);
    }
    // Common fast path: `"{}"` (2 bytes) — skip the JSON parser entirely.
    let start = arena.offsets[i] as usize;
    let end = arena.offsets[i + 1] as usize;
    if end - start == 2 {
        let bytes = arena.value_bytes(i);
        if bytes == b"{}" {
            return Ok(empty_map);
        }
    }
    parse_json_sub(env, arena, i, stacks)
}

fn parse_json_sub<'a>(
    env: Env<'a>,
    arena: &StringArena<'a, '_>,
    i: usize,
    stacks: &mut Stacks<Term<'a>>,
) -> Res<Term<'a>> {
    let bin = arena.sub_binary(i)?;
    let mut builder = TermBuilder::new(env, &bin);
    Parser::new(bin.as_slice()).document_with(&mut builder, stacks)
}
