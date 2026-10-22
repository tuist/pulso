//! Query-local compiled capture regexes share selector RE2-subset semantics.
use rustler::{Encoder, Env, ResourceArc, Term};

pub struct MetricRegex(regex::Regex);
#[rustler::resource_impl]
impl rustler::Resource for MetricRegex {}

mod atoms {
    rustler::atoms! { ok, error, nil }
}

// Live tails compile once, then filter whole batches with the exact byte-regex
// engine used by Parquet log queries. Input and output records stay BEAM terms;
// only borrowed line/label slices cross into the matcher.
pub struct LogRegex(regex::bytes::Regex);
#[rustler::resource_impl]
impl rustler::Resource for LogRegex {}

#[rustler::nif(schedule = "DirtyCpu")]
fn compile_log_regex<'a>(env: Env<'a>, pattern: &str) -> Term<'a> {
    match crate::query_regex::bytes(pattern) {
        Ok(regex) => (atoms::ok(), ResourceArc::new(LogRegex(regex))).encode(env),
        Err(_) => atoms::error().encode(env),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn filter_log_regex<'a>(
    env: Env<'a>,
    regex: ResourceArc<LogRegex>,
    rows: rustler::ListIterator<'a>,
    negate: bool,
) -> rustler::NifResult<Term<'a>> {
    let mut kept = Vec::new();
    for row in rows {
        let (record, value): (Term<'a>, rustler::Binary<'a>) = row.decode()?;
        if regex.0.is_match(value.as_slice()) != negate {
            kept.push(record);
        }
    }
    Ok(kept.encode(env))
}

// Remaining LogQL stages use the same non-backtracking engine as pushdown.
// Named values borrow the input binary; retained query/tail results detach at
// their existing lifetime boundary rather than copying every parsed line.
#[rustler::nif(schedule = "DirtyCpu")]
fn match_log_regex(regex: ResourceArc<LogRegex>, input: rustler::Binary<'_>) -> bool {
    regex.0.is_match(input.as_slice())
}

#[rustler::nif(schedule = "DirtyCpu")]
fn log_regex_named_captures<'a>(
    env: Env<'a>,
    regex: ResourceArc<LogRegex>,
    input: rustler::Binary<'a>,
) -> rustler::NifResult<Term<'a>> {
    let Some(captures) = regex.0.captures(input.as_slice()) else {
        return Ok(atoms::nil().encode(env));
    };
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for (index, name) in regex.0.capture_names().enumerate() {
        if let Some(name) = name {
            let (start, len) = captures
                .get(index)
                .map_or((0, 0), |capture| (capture.start(), capture.len()));
            keys.push(name);
            values.push(input.make_subbinary(start, len)?);
        }
    }
    Term::map_from_arrays(env, &keys, &values)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn compile_metric_capture_regex<'a>(env: Env<'a>, pattern: &str) -> Term<'a> {
    match crate::metric_segment_parquet::compile_metric_regex(&format!("(?s:\\A(?:{pattern})\\z)"))
    {
        Ok(regex) => (atoms::ok(), ResourceArc::new(MetricRegex(regex))).encode(env),
        Err(_) => atoms::error().encode(env),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn metric_regex_captures<'a>(
    env: Env<'a>,
    regex: ResourceArc<MetricRegex>,
    input: &str,
) -> Term<'a> {
    let Some(captures) = regex.0.captures(input) else {
        return atoms::nil().encode(env);
    };
    let mut pairs = Vec::with_capacity(captures.len() * 2);
    for (index, name) in regex.0.capture_names().enumerate() {
        let value = captures.get(index).map_or("", |capture| capture.as_str());
        pairs.push((index.to_string(), value.to_string()));
        if let Some(name) = name {
            pairs.push((name.to_string(), value.to_string()));
        }
    }
    let (keys, values): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
    Term::map_from_arrays(env, &keys, &values).unwrap()
}
