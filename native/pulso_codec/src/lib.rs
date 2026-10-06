// Pulso codec NIF: the byte-heavy encoding and decoding on Pulso's hot
// paths.
//
//   * Loki push protobuf decoding (ingest).
//   * JSON encode/decode, used as Phoenix's JSON library (request bodies,
//     responses).
//   * Log segment encode/decode for the S3 adapter and the MCP tools.
//
// Every JSON function falls back to Elixir (returns `:fallback`) on input
// it cannot guarantee to handle exactly like Elixir's `JSON` module, so
// the Rust path is an optimization, never a behavior change.
//
// Decoded strings are sub-binaries of the input where that avoids a
// copy, so a retained term can keep its whole source buffer alive.
// Callers that keep decoded data past a request should `:binary.copy/1`
// the fields they keep.

mod erlang_bytes;
mod ingest_limits;
mod json_read;
mod json_write;
mod labels;
mod loki;
mod metric_json;
mod metric_regex;
mod metric_segment_parquet;
mod metric_value;
mod out;
mod query_filter;
mod remote_write;
mod segment;
mod segment_parquet;
mod stable_hash;
mod term_json;
mod wire;

use json_read::Parser;
use memchr::memmem::Finder;
use out::BinSink;
use query_filter::{LineFilter, LineFilterOp, MatchOp, Matcher};
use regex::bytes::Regex;
use term_json::{EncodeError, JsonEncoder, TermBuilder};

use rustler::{Binary, Encoder, Env, ListIterator, NewBinary, NifResult, Term};
use std::borrow::Cow;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        invalid_snappy,
        invalid_protobuf,
        payload_too_large,
        too_many_records,
        attributes_too_large,
        fallback,
        too_big,
        query_sample_limit,
        storage,
        lines,
        eq,
        neq,
        re,
        nre,
        contains,
        not_contains,
        match_re,
        not_match_re,
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
        metric_sample = "Elixir.Pulso.Record.MetricSample",
        series_id,
        value,
        labels,
    }
}

/// Decode a Snappy-compressed (raw block format) Loki `PushRequest`.
///
/// The uncompressed size is read from the Snappy header and checked
/// against `max_decompressed` before any output buffer is allocated.
/// Runs on a dirty CPU scheduler: a 1 MiB batch takes a few ms, well
/// past the ~1 ms budget of a regular scheduler.
#[rustler::nif(schedule = "DirtyCpu")]
fn decode_loki_push<'a>(
    env: Env<'a>,
    compressed: Binary<'a>,
    max_decompressed: usize,
) -> NifResult<Term<'a>> {
    decode_loki_inner(env, compressed, max_decompressed, None)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn decode_loki_push_limited<'a>(
    env: Env<'a>,
    compressed: Binary<'a>,
    max_decompressed: usize,
    options: ingest_limits::Options,
) -> NifResult<Term<'a>> {
    decode_loki_inner(env, compressed, max_decompressed, Some(options.into()))
}

fn decode_loki_inner<'a>(
    env: Env<'a>,
    compressed: Binary<'a>,
    max_decompressed: usize,
    limits: Option<ingest_limits::Limits>,
) -> NifResult<Term<'a>> {
    let input = compressed.as_slice();
    let len = match snap::raw::decompress_len(input) {
        Ok(len) => len,
        Err(_) => return Ok(error(env, atoms::invalid_snappy())),
    };
    if len > max_decompressed {
        return Ok(error(env, atoms::payload_too_large()));
    }

    let mut buffer = NewBinary::new(env, len);
    match snap::raw::Decoder::new().decompress(input, buffer.as_mut_slice()) {
        Ok(written) if written == len => {}
        _ => return Ok(error(env, atoms::invalid_snappy())),
    }
    let buffer: Binary = buffer.into();

    if let Some(limits) = limits {
        if let Err(reason) = limits.loki(buffer.as_slice()) {
            return Ok(ingest_error(env, reason));
        }
    }

    match loki::decode(buffer.as_slice()) {
        Some(decoded) => to_terms(env, &buffer, &decoded),
        None => Ok(error(env, atoms::invalid_protobuf())),
    }
}

fn ingest_error(env: Env<'_>, reason: ingest_limits::Error) -> Term<'_> {
    error(
        env,
        match reason {
            ingest_limits::Error::InvalidProtobuf => atoms::invalid_protobuf(),
            ingest_limits::Error::TooManyRecords => atoms::too_many_records(),
            ingest_limits::Error::AttributesTooLarge => atoms::attributes_too_large(),
        },
    )
}

fn error<'a>(env: Env<'a>, reason: rustler::Atom) -> Term<'a> {
    (atoms::error(), reason).encode(env)
}

fn to_terms<'a>(env: Env<'a>, buffer: &Binary<'a>, decoded: &loki::Decoded) -> NifResult<Term<'a>> {
    let nil = atoms::nil().encode(env);
    let keys = [
        atoms::struct_key(),
        atoms::timestamp_ns(),
        atoms::observed_timestamp_ns(),
        atoms::severity_number(),
        atoms::severity_text(),
        atoms::service(),
        atoms::body(),
        atoms::trace_id(),
        atoms::span_id(),
        atoms::attributes(),
        atoms::resource(),
    ]
    .map(|atom| atom.encode(env));
    let struct_name = atoms::log().encode(env);

    let total = decoded.streams.iter().map(|s| s.records.len()).sum();
    let mut records = Vec::with_capacity(total);

    for stream in &decoded.streams {
        let mut names = Vec::with_capacity(stream.labels.len());
        let mut values = Vec::with_capacity(stream.labels.len());
        for (name, value) in &stream.labels {
            names.push(slice(env, buffer, name.as_bytes())?);
            values.push(match value {
                Cow::Borrowed(b) => slice(env, buffer, b)?,
                Cow::Owned(o) => copy(env, o),
            });
        }
        // One resource map per stream, shared by every record in it.
        let resource = Term::map_from_arrays(env, &names, &values)?;
        let service = stream.service().map_or(nil, |i| values[i]);
        let level = stream.level().map_or(nil, |i| values[i]);

        for record in &stream.records {
            let mut attr_keys = Vec::with_capacity(record.attributes.len());
            let mut attr_values = Vec::with_capacity(record.attributes.len());
            for (k, v) in &record.attributes {
                attr_keys.push(slice(env, buffer, k)?);
                attr_values.push(slice(env, buffer, v)?);
            }
            let fields = [
                struct_name,
                record.timestamp_ns.encode(env),
                nil,
                nil,
                level,
                service,
                slice(env, buffer, record.line)?,
                optional(env, buffer, record.trace_id)?,
                optional(env, buffer, record.span_id)?,
                Term::map_from_arrays(env, &attr_keys, &attr_values)?,
                resource,
            ];
            records.push(Term::map_from_arrays(env, &keys, &fields)?);
        }
    }

    Ok((atoms::ok(), records, decoded.rejected).encode(env))
}

/// A sub-binary of `buffer` when `part` lies inside it; otherwise (empty
/// proto3 defaults point at static memory) a fresh binary.
fn slice<'a>(env: Env<'a>, buffer: &Binary<'a>, part: &[u8]) -> NifResult<Term<'a>> {
    let base = buffer.as_slice();
    let start = (part.as_ptr() as usize).wrapping_sub(base.as_ptr() as usize);
    if !part.is_empty() && start <= base.len() && part.len() <= base.len() - start {
        Ok(buffer.make_subbinary(start, part.len())?.encode(env))
    } else {
        Ok(copy(env, part))
    }
}

fn optional<'a>(env: Env<'a>, buffer: &Binary<'a>, part: Option<&[u8]>) -> NifResult<Term<'a>> {
    match part {
        Some(p) => slice(env, buffer, p),
        None => Ok(atoms::nil().encode(env)),
    }
}

fn copy<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let mut out = NewBinary::new(env, bytes.len());
    out.as_mut_slice().copy_from_slice(bytes);
    Binary::from(out).encode(env)
}

// -- JSON ---------------------------------------------------------------

fn json_decode_impl<'a>(env: Env<'a>, input: Binary<'a>) -> Term<'a> {
    let mut builder = TermBuilder::new(env, &input);
    match Parser::new(input.as_slice()).document(&mut builder) {
        Ok(term) => (atoms::ok(), term).encode(env),
        Err(_) => atoms::fallback().encode(env),
    }
}

/// For inputs small enough to parse well inside a scheduler timeslice.
#[rustler::nif]
fn json_decode<'a>(env: Env<'a>, input: Binary<'a>) -> Term<'a> {
    json_decode_impl(env, input)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn json_decode_dirty<'a>(env: Env<'a>, input: Binary<'a>) -> Term<'a> {
    json_decode_impl(env, input)
}

fn json_encode_impl<'a>(env: Env<'a>, term: Term<'a>, budget: usize, capacity: usize) -> Term<'a> {
    let encoder = JsonEncoder::new(env, budget);
    let mut out = BinSink::with_capacity(capacity);
    match encoder.value(&mut out, term, 0) {
        Ok(()) => (atoms::ok(), out.finish(env)).encode(env),
        Err(EncodeError::TooBig) => atoms::too_big().encode(env),
        Err(EncodeError::Fallback) => atoms::fallback().encode(env),
    }
}

/// Encodes on the calling scheduler and gives up with `:too_big` once the
/// output passes `budget` bytes, so the caller can retry on a dirty
/// scheduler. Keeps small responses off the dirty pool.
#[rustler::nif]
fn json_encode<'a>(env: Env<'a>, term: Term<'a>, budget: usize) -> Term<'a> {
    json_encode_impl(env, term, budget, 256)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn json_encode_dirty<'a>(env: Env<'a>, term: Term<'a>) -> Term<'a> {
    json_encode_impl(env, term, usize::MAX, 64 * 1024)
}

// -- Log segments --------------------------------------------------------

fn integer<'a>(env: Env<'a>, v: i128) -> Term<'a> {
    match i64::try_from(v) {
        Ok(i) => i.encode(env),
        Err(_) => (v as u64).encode(env),
    }
}

/// Encode `[%Pulso.Record.Log{}]` as stored segment lines (`:storage`,
/// `:lines`) or as the MCP tool's JSON array (`:plain`, `:array`).
/// Returns `{:ok, binary, min_ts, max_ts, count}` or `:fallback`.
#[rustler::nif(schedule = "DirtyCpu")]
fn encode_log_segment<'a>(
    env: Env<'a>,
    records: Term<'a>,
    mode: rustler::Atom,
    framing: rustler::Atom,
) -> Term<'a> {
    let mode = if mode == atoms::storage() {
        segment::Mode::Storage
    } else {
        segment::Mode::Plain
    };
    let framing = if framing == atoms::lines() {
        segment::Framing::Lines
    } else {
        segment::Framing::Array
    };
    let estimate = records.list_length().unwrap_or(0).saturating_mul(512);
    let encoder = JsonEncoder::new(env, usize::MAX);
    let mut out = BinSink::with_capacity(estimate);
    match segment::encode(env, &mut out, records, mode, framing, &encoder) {
        Ok(b) => (
            atoms::ok(),
            out.finish(env),
            integer(env, b.min_ts),
            integer(env, b.max_ts),
            b.count,
        )
            .encode(env),
        Err(_) => atoms::fallback().encode(env),
    }
}

fn optional_int(t: Term) -> Result<Option<i128>, ()> {
    if t.decode::<rustler::Atom>().ok() == Some(atoms::nil()) {
        Ok(None)
    } else if let Ok(i) = t.decode::<i64>() {
        Ok(Some(i128::from(i)))
    } else {
        t.decode::<u64>()
            .map(|u| Some(i128::from(u)))
            .map_err(|_| ())
    }
}

/// Decode a stored segment into `[%Pulso.Record.Log{}]`, keeping only
/// records inside `[start_ts, end_ts]` (either may be nil) and with the
/// given service (nil for any). Returns `{:ok, records}` or `:fallback`.
#[rustler::nif(schedule = "DirtyCpu")]
fn decode_log_segment<'a>(
    env: Env<'a>,
    blob: Binary<'a>,
    start_ts: Term<'a>,
    end_ts: Term<'a>,
    service: Term<'a>,
) -> Term<'a> {
    let (Ok(start), Ok(end)) = (optional_int(start_ts), optional_int(end_ts)) else {
        return atoms::fallback().encode(env);
    };
    let service_bin: Option<Binary> = service.decode().ok();
    if service_bin.is_none() && service.decode::<rustler::Atom>().ok() != Some(atoms::nil()) {
        return atoms::fallback().encode(env);
    }
    let filter = segment::Filter {
        start,
        end,
        service: service_bin.as_ref().map(|b| b.as_slice()),
    };
    match segment::decode(env, &blob, &filter) {
        Ok(records) => (atoms::ok(), records).encode(env),
        Err(_) => atoms::fallback().encode(env),
    }
}

/// Encode `[%Pulso.Record.Log{}]` as an Apache Parquet segment for the
/// S3 storage adapter. Returns `{:ok, binary, min_ts, max_ts, count}` on
/// success or `:fallback` on any Rust-side failure — the Elixir side
/// treats `:fallback` here as a hard error rather than falling back to
/// pure Elixir, since there is no Elixir Parquet encoder.
#[rustler::nif(schedule = "DirtyCpu")]
fn encode_log_segment_parquet<'a>(env: Env<'a>, records: Term<'a>) -> Term<'a> {
    match segment_parquet::encode(env, records) {
        Ok((buf, b)) => (
            atoms::ok(),
            copy(env, &buf),
            integer(env, b.min_ts),
            integer(env, b.max_ts),
            b.count,
        )
            .encode(env),
        Err(_) => atoms::fallback().encode(env),
    }
}

/// Decode a Parquet segment into `[%Pulso.Record.Log{}]`, applying
/// four categories of pushdown filter before any Erlang term is built:
///
///   * `start_ts` / `end_ts` — row-group `timestamp_ns` stats prune
///     whole row groups; per-row check then rejects the rest.
///   * `service` (a binary or `nil`) — kept on its own arg because
///     `service` is a dictionary-encoded column and matching against it
///     is faster than an equivalent matcher against `resource`.
///   * `matchers` — a list of `{name, op, value}` tuples evaluated
///     against the row's `resource` JSON. `op` is one of `:eq | :neq |
///     :re | :nre`. Regex ops compile the pattern once here.
///   * `line_filters` — a list of `{op, value}` tuples evaluated against
///     the row's raw `body` bytes. `op` is one of `:contains |
///     :not_contains | :match_re | :not_match_re`.
///
/// Any element the NIF cannot decode returns `:fallback`; the Elixir
/// side treats that as a hard error and does not silently drop the
/// pushdown — a Parquet segment lacks the Elixir reference
/// implementation the JSON codecs have.
#[rustler::nif(schedule = "DirtyCpu")]
fn decode_log_segment_parquet<'a>(
    env: Env<'a>,
    blob: Binary<'a>,
    start_ts: Term<'a>,
    end_ts: Term<'a>,
    service: Term<'a>,
    matchers: Term<'a>,
    line_filters: Term<'a>,
) -> Term<'a> {
    let (Ok(start), Ok(end)) = (optional_int(start_ts), optional_int(end_ts)) else {
        return atoms::fallback().encode(env);
    };
    let service_bin: Option<Binary> = service.decode().ok();
    if service_bin.is_none() && service.decode::<rustler::Atom>().ok() != Some(atoms::nil()) {
        return atoms::fallback().encode(env);
    }
    let matchers_vec = match decode_matchers(matchers) {
        Ok(v) => v,
        Err(_) => return atoms::fallback().encode(env),
    };
    let line_filters_vec = match decode_line_filters(line_filters) {
        Ok(v) => v,
        Err(_) => return atoms::fallback().encode(env),
    };
    let filter = segment_parquet::Filter {
        start,
        end,
        service: service_bin.as_ref().map(|b| b.as_slice()),
        matchers: matchers_vec,
        line_filters: line_filters_vec,
    };
    match segment_parquet::decode(env, &blob, &filter) {
        Ok(records) => (atoms::ok(), records).encode(env),
        Err(_) => atoms::fallback().encode(env),
    }
}

/// Encode `[%Pulso.Record.MetricSample{}]` as an Apache Parquet segment
/// for the metrics S3 storage path. Returns
/// `{:ok, binary, min_ts, max_ts, count}` on success or `:fallback`.
#[rustler::nif(schedule = "DirtyCpu")]
fn encode_metric_segment_parquet<'a>(env: Env<'a>, samples: Term<'a>) -> Term<'a> {
    match metric_segment_parquet::encode(env, samples) {
        Ok((buf, b)) => (
            atoms::ok(),
            copy(env, &buf),
            integer(env, b.min_ts),
            integer(env, b.max_ts),
            b.count,
        )
            .encode(env),
        Err(_) => atoms::fallback().encode(env),
    }
}

/// Decode a metric Parquet segment into
/// `[%Pulso.Record.MetricSample{}]`, keeping only samples inside
/// `[start_ts, end_ts]` and matching every supplied label matcher.
/// Matchers are a list of `{name :: binary, op :: :eq | :neq | :re | :nre, value :: binary}`.
#[rustler::nif(schedule = "DirtyCpu")]
fn decode_metric_segment_parquet<'a>(
    env: Env<'a>,
    blob: Binary<'a>,
    start_ts: Term<'a>,
    end_ts: Term<'a>,
    matchers: Term<'a>,
) -> Term<'a> {
    decode_metrics(env, blob, start_ts, end_ts, matchers, None)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn decode_metric_segment_parquet_bounded<'a>(
    env: Env<'a>,
    blob: Binary<'a>,
    start_ts: Term<'a>,
    end_ts: Term<'a>,
    matchers: Term<'a>,
    max_samples: usize,
) -> Term<'a> {
    decode_metrics(env, blob, start_ts, end_ts, matchers, Some(max_samples))
}

fn decode_metrics<'a>(
    env: Env<'a>,
    blob: Binary<'a>,
    start_ts: Term<'a>,
    end_ts: Term<'a>,
    matchers: Term<'a>,
    max_samples: Option<usize>,
) -> Term<'a> {
    let (Ok(start), Ok(end)) = (optional_int(start_ts), optional_int(end_ts)) else {
        return atoms::fallback().encode(env);
    };
    let Ok(matcher_iter) = matchers.decode::<ListIterator>() else {
        return atoms::fallback().encode(env);
    };
    // Borrow the raw bytes from the matcher value binaries through the
    // owning `Binary` list we keep alive via `_owners`.
    let mut owners: Vec<(Binary, Binary, metric_segment_parquet::MatcherOp)> = Vec::new();
    for term in matcher_iter {
        let Ok((name, op_atom, value)): Result<(Binary, rustler::Atom, Binary), _> = term.decode()
        else {
            return atoms::fallback().encode(env);
        };
        let op = if op_atom == atoms::eq() {
            metric_segment_parquet::MatcherOp::Eq
        } else if op_atom == atoms::neq() {
            metric_segment_parquet::MatcherOp::Neq
        } else if op_atom == atoms::re() {
            metric_segment_parquet::MatcherOp::Re
        } else if op_atom == atoms::nre() {
            metric_segment_parquet::MatcherOp::Nre
        } else {
            return atoms::fallback().encode(env);
        };
        owners.push((name, value, op));
    }
    let matcher_views: Vec<metric_segment_parquet::Matcher> = owners
        .iter()
        .map(|(n, v, op)| metric_segment_parquet::Matcher {
            name: n.as_slice(),
            value: v.as_slice(),
            op: *op,
        })
        .collect();
    let filter = metric_segment_parquet::Filter {
        start,
        end,
        matchers: matcher_views,
        max_samples,
    };
    match metric_segment_parquet::decode(env, &blob, &filter) {
        Ok(records) => (atoms::ok(), records).encode(env),
        Err(metric_segment_parquet::DecodeError::TooManySamples) => {
            (atoms::error(), atoms::query_sample_limit()).encode(env)
        }
        Err(_) => atoms::fallback().encode(env),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn validate_metric_regex(pattern: &str) -> rustler::Atom {
    if metric_segment_parquet::compile_metric_regex(pattern).is_ok() {
        atoms::ok()
    } else {
        atoms::error()
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn validate_log_regex(pattern: &str) -> rustler::Atom {
    if Regex::new(pattern).is_ok() {
        atoms::ok()
    } else {
        atoms::error()
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn match_metric_regex(pattern: &str, value: &str) -> NifResult<bool> {
    let regex = metric_segment_parquet::compile_metric_regex(pattern)
        .map_err(|_| rustler::Error::BadArg)?;
    Ok(regex.is_match(value))
}

/// Decode a Snappy-compressed Prometheus remote_write v1
/// `WriteRequest`. Returns `{:ok, series, rejected}` where each entry
/// of `series` is `{labels_map, [{ts_ms, value}, …], series_id}`.
///
/// `labels_map` is `%{binary => binary}`, `ts_ms` is milliseconds (the
/// Prometheus unit, not nanoseconds — the Elixir caller rescales),
/// `series_id` is the Prometheus-compatible `labels.StableHash`
/// digest (see `src/stable_hash.rs`).
#[rustler::nif(schedule = "DirtyCpu")]
fn decode_remote_write<'a>(
    env: Env<'a>,
    compressed: Binary<'a>,
    max_decompressed: usize,
) -> NifResult<Term<'a>> {
    decode_remote_write_inner(env, compressed, max_decompressed, None)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn decode_remote_write_limited<'a>(
    env: Env<'a>,
    compressed: Binary<'a>,
    max_decompressed: usize,
    options: ingest_limits::Options,
) -> NifResult<Term<'a>> {
    decode_remote_write_inner(env, compressed, max_decompressed, Some(options.into()))
}

fn decode_remote_write_inner<'a>(
    env: Env<'a>,
    compressed: Binary<'a>,
    max_decompressed: usize,
    limits: Option<ingest_limits::Limits>,
) -> NifResult<Term<'a>> {
    let input = compressed.as_slice();
    let len = match snap::raw::decompress_len(input) {
        Ok(len) => len,
        Err(_) => return Ok(error(env, atoms::invalid_snappy())),
    };
    if len > max_decompressed {
        return Ok(error(env, atoms::payload_too_large()));
    }

    let mut buffer = NewBinary::new(env, len);
    match snap::raw::Decoder::new().decompress(input, buffer.as_mut_slice()) {
        Ok(written) if written == len => {}
        _ => return Ok(error(env, atoms::invalid_snappy())),
    }
    let buffer: Binary = buffer.into();

    if let Some(limits) = limits {
        if let Err(reason) = limits.remote_write(buffer.as_slice()) {
            return Ok(ingest_error(env, reason));
        }
    }

    let decoded = match remote_write::decode(buffer.as_slice()) {
        Some(d) => d,
        None => return Ok(error(env, atoms::invalid_protobuf())),
    };

    let mut series_terms: Vec<Term<'a>> = Vec::with_capacity(decoded.series.len());
    for series in &decoded.series {
        let mut names: Vec<Term<'a>> = Vec::with_capacity(series.labels.len());
        let mut values: Vec<Term<'a>> = Vec::with_capacity(series.labels.len());
        for (name, value) in &series.labels {
            names.push(slice(env, &buffer, name)?);
            values.push(slice(env, &buffer, value)?);
        }
        let labels_map = Term::map_from_arrays(env, &names, &values)?;
        let samples: Vec<Term<'a>> = series
            .samples
            .iter()
            .map(|s| {
                (
                    s.timestamp_ms.encode(env),
                    metric_value::encode(env, s.value),
                )
                    .encode(env)
            })
            .collect();
        let sorted_pairs: Vec<(&[u8], &[u8])> =
            series.labels.iter().map(|(n, v)| (*n, *v)).collect();
        let series_id = stable_hash::stable_hash(sorted_pairs) as i64;
        series_terms.push((labels_map, samples, series_id).encode(env));
    }

    Ok((atoms::ok(), series_terms, decoded.rejected).encode(env))
}

// -- LogQL pushdown helpers for `decode_log_segment_parquet` ---------------
//
// These share the `MatchOp` / `LineFilterOp` enums with `query_filter`
// so the log Parquet decoder can push label matchers and line filters
// down to Rust. The metric decoder uses its own `MatcherOp` because its
// row shape and column set are different — a near-duplicate that is a
// documented follow-up when the two filter shapes converge.

fn decode_matchers(term: Term<'_>) -> Result<Vec<Matcher>, ()> {
    let list: Vec<Term> = term.decode().map_err(|_| ())?;
    let mut out = Vec::with_capacity(list.len());
    for item in list {
        let (name, op_atom, value): (Binary, rustler::Atom, Binary) =
            item.decode().map_err(|_| ())?;
        let op = decode_match_op(op_atom)?;
        let name_vec = name.as_slice().to_vec();
        let value_vec = value.as_slice().to_vec();
        match op {
            MatchOp::Re | MatchOp::Nre => {
                let pat = std::str::from_utf8(&value_vec).map_err(|_| ())?;
                let re = Regex::new(pat).map_err(|_| ())?;
                out.push(Matcher::Regex {
                    name: name_vec,
                    op,
                    regex: re,
                });
            }
            MatchOp::Eq | MatchOp::Neq => out.push(Matcher::Literal {
                name: name_vec,
                op,
                value: value_vec,
            }),
        }
    }
    Ok(out)
}

fn decode_match_op(atom: rustler::Atom) -> Result<MatchOp, ()> {
    if atom == atoms::eq() {
        Ok(MatchOp::Eq)
    } else if atom == atoms::neq() {
        Ok(MatchOp::Neq)
    } else if atom == atoms::re() {
        Ok(MatchOp::Re)
    } else if atom == atoms::nre() {
        Ok(MatchOp::Nre)
    } else {
        Err(())
    }
}

fn decode_line_filters(term: Term<'_>) -> Result<Vec<LineFilter>, ()> {
    let list: Vec<Term> = term.decode().map_err(|_| ())?;
    let mut out = Vec::with_capacity(list.len());
    for item in list {
        let (op_atom, value): (rustler::Atom, Binary) = item.decode().map_err(|_| ())?;
        let op = decode_line_filter_op(op_atom)?;
        match op {
            LineFilterOp::Contains | LineFilterOp::NotContains => {
                let owned = value.as_slice().to_vec();
                out.push(LineFilter::Substring {
                    op,
                    finder: Box::new(Finder::new(&owned).into_owned()),
                });
            }
            LineFilterOp::MatchRe | LineFilterOp::NotMatchRe => {
                let pat = std::str::from_utf8(value.as_slice()).map_err(|_| ())?;
                let re = Regex::new(pat).map_err(|_| ())?;
                out.push(LineFilter::Regex { op, regex: re });
            }
        }
    }
    Ok(out)
}

fn decode_line_filter_op(atom: rustler::Atom) -> Result<LineFilterOp, ()> {
    if atom == atoms::contains() {
        Ok(LineFilterOp::Contains)
    } else if atom == atoms::not_contains() {
        Ok(LineFilterOp::NotContains)
    } else if atom == atoms::match_re() {
        Ok(LineFilterOp::MatchRe)
    } else if atom == atoms::not_match_re() {
        Ok(LineFilterOp::NotMatchRe)
    } else {
        Err(())
    }
}

/// JSON-encode `[%Pulso.Record.MetricSample{}]` for the MCP
/// `query_metrics` response. Returns `{:ok, binary}` on success or
/// `:fallback` on any input the Rust encoder cannot guarantee to emit
/// identically — the Elixir caller downgrades to `JSON.encode!`.
///
/// Fast-path numbers (10 000 samples × 4 labels): see commit body.
#[rustler::nif(schedule = "DirtyCpu")]
fn encode_metric_samples<'a>(env: Env<'a>, samples: Term<'a>) -> Term<'a> {
    match metric_json::encode(env, samples) {
        Ok(buf) => (atoms::ok(), copy(env, &buf)).encode(env),
        Err(_) => atoms::fallback().encode(env),
    }
}

rustler::init!("Elixir.Pulso.Codec.NIF");
