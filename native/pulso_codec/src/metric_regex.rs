//! Query-local compiled capture regexes share selector RE2-subset semantics.
use rustler::{Encoder, Env, ResourceArc, Term};

pub struct MetricRegex(regex::Regex);
#[rustler::resource_impl]
impl rustler::Resource for MetricRegex {}

mod atoms {
    rustler::atoms! { ok, error, nil }
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
