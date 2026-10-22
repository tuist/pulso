//! Native LogQL range-aggregation input: selects log rows, runs a supported
//! pipeline subset and reduces each kept row to one sample of its series,
//! without building Erlang terms for log records.
//!
//! This is a fast path for `Pulso.LogQL.MetricEval`'s streamed evaluation and
//! must reproduce it exactly. Labels are those of `Pulso.LogQL.Entry`
//! (resource, then attributes, then the promoted service/level mirrors when
//! absent), stages follow `Pulso.LogQL.Pipeline`, unwrap values follow
//! `Pulso.PromQL.FloatParser` and label-filter numbers `Integer.parse` then
//! `Float.parse`. Whenever a row needs behavior this module cannot guarantee
//! (non-string label values, non-string bodies, number spellings outside the
//! plain decimal grammar), the whole scan answers `:fallback` and the caller
//! evaluates the query in Elixir instead.
use crate::query_filter::{self, LineFilter, Matcher};
use arrow::array::{Array, Int64Array, StringArray};
use bytes::Bytes;
use memchr::memmem::Finder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use regex::bytes::Regex;
use rustler::{Binary, Encoder, Env, ListIterator, NewBinary, ResourceArc, Term};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::Mutex;

mod atoms {
    rustler::atoms! { ok, error, fallback, nil, contains, not_contains, match_re, not_match_re,
    logfmt, regexp, label_filter, and, or, eq, neq, re, nre, num, lt, lte, gt, gte, count, bytes,
    unwrap, by, without, pipeline_error, query_resource_limit }
}

const COLUMNS: [&str; 6] = [
    "timestamp_ns",
    "service",
    "severity_text",
    "body",
    "attributes",
    "resource",
];

#[derive(Debug)]
enum Error {
    Fallback,
    Pipeline,
    Limit,
}

type Res<T> = Result<T, Error>;

#[derive(Clone, Copy, PartialEq)]
enum CmpOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Copy, PartialEq, PartialOrd)]
enum Number {
    Int(i64),
    Float(f64),
}

impl Number {
    fn float(self) -> f64 {
        match self {
            Number::Int(i) => i as f64,
            Number::Float(f) => f,
        }
    }
}

enum Filter {
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
    Str {
        name: String,
        negate: bool,
        value: Vec<u8>,
    },
    Re {
        name: String,
        negate: bool,
        regex: Regex,
    },
    Num {
        name: String,
        op: CmpOp,
        value: Number,
    },
}

enum Stage {
    Line {
        negate: bool,
        finder: Box<Finder<'static>>,
    },
    LineRe {
        negate: bool,
        regex: Regex,
    },
    Logfmt,
    Regexp {
        regex: Regex,
        names: Vec<(String, usize)>,
    },
    Labels(Filter),
}

enum Value {
    Count,
    Bytes,
    Unwrap(String),
}

enum Grouping {
    All,
    By(Vec<String>),
    Without(Vec<String>),
}

type SeriesKey = Vec<(String, String)>;

#[derive(Clone, Copy)]
enum Sample {
    Float(f64),
    Int(i64),
    Error,
}

struct State {
    stages: Vec<Stage>,
    value: Value,
    drop: Option<String>,
    grouping: Grouping,
    series: HashMap<SeriesKey, Vec<(i64, Sample)>>,
    samples: usize,
    // Retained samples allowed: the query's remaining log sample budget.
    max_samples: u64,
    // Pattern scans train this miner on every selected line instead.
    patterns: Option<crate::patterns::Miner>,
}

pub struct Scan(Mutex<Option<State>>);

#[rustler::resource_impl]
impl rustler::Resource for Scan {}

fn result<'a, T: Encoder>(env: Env<'a>, value: Res<T>) -> Term<'a> {
    match value {
        Ok(value) => (atoms::ok(), value).encode(env),
        Err(Error::Fallback) => atoms::fallback().encode(env),
        Err(Error::Pipeline) => (atoms::error(), atoms::pipeline_error()).encode(env),
        Err(Error::Limit) => (atoms::error(), atoms::query_resource_limit()).encode(env),
    }
}

// The reference uses UTF-8 PCRE with strict end-of-input `$`; the shared
// native subset additionally enforces Go's ASCII Perl classes/boundaries.
fn pcre_compatible(pattern: &str) -> Res<Regex> {
    crate::query_regex::bytes(pattern).map_err(|_| Error::Fallback)
}

fn text(term: Term<'_>) -> Res<String> {
    let binary: Binary = term.decode().map_err(|_| Error::Fallback)?;
    String::from_utf8(binary.as_slice().to_vec()).map_err(|_| Error::Fallback)
}

fn number(term: Term<'_>) -> Res<Number> {
    if let Ok(i) = term.decode::<i64>() {
        return Ok(Number::Int(i));
    }
    term.decode::<f64>()
        .map(Number::Float)
        .map_err(|_| Error::Fallback)
}

fn cmp_op(atom: rustler::Atom) -> Res<CmpOp> {
    Ok(if atom == atoms::eq() {
        CmpOp::Eq
    } else if atom == atoms::neq() {
        CmpOp::Neq
    } else if atom == atoms::lt() {
        CmpOp::Lt
    } else if atom == atoms::lte() {
        CmpOp::Lte
    } else if atom == atoms::gt() {
        CmpOp::Gt
    } else if atom == atoms::gte() {
        CmpOp::Gte
    } else {
        return Err(Error::Fallback);
    })
}

fn filter(term: Term<'_>, depth: usize) -> Res<Filter> {
    if depth > 64 {
        return Err(Error::Fallback);
    }
    let parts: Vec<Term> = rustler::types::tuple::get_tuple(term).map_err(|_| Error::Fallback)?;
    let tag: rustler::Atom = parts
        .first()
        .ok_or(Error::Fallback)?
        .decode()
        .map_err(|_| Error::Fallback)?;
    match parts.len() {
        3 if tag == atoms::and() || tag == atoms::or() => {
            let left = Box::new(filter(parts[1], depth + 1)?);
            let right = Box::new(filter(parts[2], depth + 1)?);
            Ok(if tag == atoms::and() {
                Filter::And(left, right)
            } else {
                Filter::Or(left, right)
            })
        }
        3 if tag == atoms::eq() || tag == atoms::neq() => Ok(Filter::Str {
            name: text(parts[1])?,
            negate: tag == atoms::neq(),
            value: text(parts[2])?.into_bytes(),
        }),
        3 if tag == atoms::re() || tag == atoms::nre() => Ok(Filter::Re {
            name: text(parts[1])?,
            negate: tag == atoms::nre(),
            regex: pcre_compatible(&text(parts[2])?)?,
        }),
        4 if tag == atoms::num() => Ok(Filter::Num {
            op: cmp_op(parts[1].decode().map_err(|_| Error::Fallback)?)?,
            name: text(parts[2])?,
            value: number(parts[3])?,
        }),
        _ => Err(Error::Fallback),
    }
}

fn stage(term: Term<'_>) -> Res<Stage> {
    if let Ok(atom) = term.decode::<rustler::Atom>() {
        return if atom == atoms::logfmt() {
            Ok(Stage::Logfmt)
        } else {
            Err(Error::Fallback)
        };
    }
    let parts: Vec<Term> = rustler::types::tuple::get_tuple(term).map_err(|_| Error::Fallback)?;
    if parts.len() != 2 {
        return Err(Error::Fallback);
    }
    let tag: rustler::Atom = parts[0].decode().map_err(|_| Error::Fallback)?;
    if tag == atoms::contains() || tag == atoms::not_contains() {
        let needle = text(parts[1])?.into_bytes();
        Ok(Stage::Line {
            negate: tag == atoms::not_contains(),
            finder: Box::new(Finder::new(&needle).into_owned()),
        })
    } else if tag == atoms::match_re() || tag == atoms::not_match_re() {
        Ok(Stage::LineRe {
            negate: tag == atoms::not_match_re(),
            regex: pcre_compatible(&text(parts[1])?)?,
        })
    } else if tag == atoms::regexp() {
        let regex = pcre_compatible(&text(parts[1])?)?;
        let names = regex
            .capture_names()
            .enumerate()
            .filter_map(|(i, name)| name.map(|n| (n.to_owned(), i)))
            .collect();
        Ok(Stage::Regexp { regex, names })
    } else if tag == atoms::label_filter() {
        Ok(Stage::Labels(filter(parts[1], 0)?))
    } else {
        Err(Error::Fallback)
    }
}

fn names(term: Term<'_>) -> Res<Vec<String>> {
    term.decode::<ListIterator>()
        .map_err(|_| Error::Fallback)?
        .map(text)
        .collect()
}

#[rustler::nif]
fn log_metric_api_version() -> u32 {
    3
}

/// `stages` is the pipeline after pushdown; `value` is `:count`, `:bytes` or
/// `{:unwrap, label}`; `drop` the unwrapped label removed from series keys or
/// nil; `grouping` nil, `{:by, names}` or `{:without, names}`; `max_samples`
/// the retained samples allowed before the scan fails with a resource limit.
#[rustler::nif(schedule = "DirtyCpu")]
fn log_metric_scan_new<'a>(
    env: Env<'a>,
    stages: Term<'a>,
    value: Term<'a>,
    drop: Term<'a>,
    grouping: Term<'a>,
    max_samples: u64,
) -> Term<'a> {
    let build = || {
        let stages = stages
            .decode::<ListIterator>()
            .map_err(|_| Error::Fallback)?
            .map(stage)
            .collect::<Res<Vec<_>>>()?;
        let value = if let Ok(atom) = value.decode::<rustler::Atom>() {
            if atom == atoms::count() {
                Value::Count
            } else if atom == atoms::bytes() {
                Value::Bytes
            } else {
                return Err(Error::Fallback);
            }
        } else {
            let (tag, label): (rustler::Atom, Term) =
                value.decode().map_err(|_| Error::Fallback)?;
            if tag != atoms::unwrap() {
                return Err(Error::Fallback);
            }
            Value::Unwrap(text(label)?)
        };
        let drop = if drop.decode::<rustler::Atom>().ok() == Some(atoms::nil()) {
            None
        } else {
            Some(text(drop)?)
        };
        let grouping = if grouping.decode::<rustler::Atom>().ok() == Some(atoms::nil()) {
            Grouping::All
        } else {
            let (mode, list): (rustler::Atom, Term) =
                grouping.decode().map_err(|_| Error::Fallback)?;
            if mode == atoms::by() {
                Grouping::By(names(list)?)
            } else if mode == atoms::without() {
                Grouping::Without(names(list)?)
            } else {
                return Err(Error::Fallback);
            }
        };
        Ok(ResourceArc::new(Scan(Mutex::new(Some(State {
            stages,
            value,
            drop,
            grouping,
            series: HashMap::new(),
            samples: 0,
            max_samples,
            patterns: None,
        })))))
    };
    result(env, build())
}

/// Fold one segment: `{:ok, {rows_selected, rows_kept}}`: rows the storage
/// filters select, then rows the pipeline keeps. Selection arguments are
/// those of `decode_log_segment_parquet_v2`.
#[allow(clippy::too_many_arguments)]
#[rustler::nif(schedule = "DirtyCpu")]
fn log_metric_scan_add<'a>(
    env: Env<'a>,
    scan: ResourceArc<Scan>,
    blob: Binary<'a>,
    start: i64,
    end: i64,
    service: Term<'a>,
    matchers: Term<'a>,
    line_filters: Term<'a>,
) -> Term<'a> {
    let add = || {
        let service = match service.decode::<Binary>() {
            Ok(service) => Some(service.as_slice().to_vec()),
            Err(_) if service.decode::<rustler::Atom>().ok() == Some(atoms::nil()) => None,
            Err(_) => return Err(Error::Fallback),
        };
        let matchers = crate::decode_matchers(matchers).map_err(|_| Error::Fallback)?;
        let line_filters = crate::decode_line_filters(line_filters).map_err(|_| Error::Fallback)?;
        let mut guard = scan.0.lock().map_err(|_| Error::Fallback)?;
        let state = guard.as_mut().ok_or(Error::Fallback)?;
        let selection = Selection {
            start,
            end,
            service,
            matchers,
            line_filters,
        };
        let outcome = state.scan(crate::erlang_bytes::from_binary(blob), &selection);
        if outcome.is_err() {
            guard.take();
        }
        outcome
    };
    result(env, add())
}

/// `[{labels, samples}]` where `samples` packs `<<ts::64, tag::8, value::64>>`
/// in descending order (tag 0 float bits, 1 unwrap error, 2 integer).
#[rustler::nif(schedule = "DirtyCpu")]
fn log_metric_scan_finish<'a>(env: Env<'a>, scan: ResourceArc<Scan>) -> Term<'a> {
    let finish = || {
        let state = scan
            .0
            .lock()
            .map_err(|_| Error::Fallback)?
            .take()
            .ok_or(Error::Fallback)?;
        let mut out = Vec::with_capacity(state.series.len());
        for (labels, mut samples) in state.series {
            samples.sort_by(|a, b| descending(*a, *b));
            let mut packed = NewBinary::new(env, samples.len() * 17);
            for (i, (ts, sample)) in samples.iter().enumerate() {
                let slot = &mut packed.as_mut_slice()[i * 17..(i + 1) * 17];
                slot[..8].copy_from_slice(&ts.to_be_bytes());
                let (tag, bits) = match sample {
                    Sample::Float(f) => (0u8, f.to_bits()),
                    Sample::Error => (1, 0),
                    Sample::Int(n) => (2, *n as u64),
                };
                slot[8] = tag;
                slot[9..].copy_from_slice(&bits.to_be_bytes());
            }
            let keys: Vec<Term> = labels.iter().map(|(k, _)| k.encode(env)).collect();
            let values: Vec<Term> = labels.iter().map(|(_, v)| v.encode(env)).collect();
            let map = Term::map_from_arrays(env, &keys, &values).map_err(|_| Error::Fallback)?;
            out.push((map, Binary::from(packed)));
        }
        Ok(out)
    };
    result(env, finish())
}

#[rustler::nif]
fn log_metric_scan_abort(scan: ResourceArc<Scan>) -> rustler::Atom {
    if let Ok(mut guard) = scan.0.lock() {
        guard.take();
    }
    atoms::ok()
}

/// A scan that mines log patterns (`crate::patterns`) from every selected
/// line instead of running a pipeline. `log_metric_scan_add` feeds it from
/// stored segments, `log_pattern_observe` from lines already in the BEAM.
#[rustler::nif]
fn log_pattern_scan_new(start: i64, step: i64) -> ResourceArc<Scan> {
    ResourceArc::new(Scan(Mutex::new(Some(State {
        stages: Vec::new(),
        value: Value::Count,
        drop: None,
        grouping: Grouping::All,
        series: HashMap::new(),
        samples: 0,
        max_samples: u64::MAX,
        patterns: Some(crate::patterns::Miner::new(start, step)),
    }))))
}

/// Train on `[{timestamp_ns, line}]`.
#[rustler::nif(schedule = "DirtyCpu")]
fn log_pattern_observe<'a>(env: Env<'a>, scan: ResourceArc<Scan>, lines: Term<'a>) -> Term<'a> {
    let observe = || {
        let mut guard = scan.0.lock().map_err(|_| Error::Fallback)?;
        let miner = guard
            .as_mut()
            .and_then(|state| state.patterns.as_mut())
            .ok_or(Error::Fallback)?;
        for item in lines
            .decode::<ListIterator>()
            .map_err(|_| Error::Fallback)?
        {
            let (ts, line): (i64, Binary) = item.decode().map_err(|_| Error::Fallback)?;
            miner.train(ts, line.as_slice());
        }
        Ok(atoms::ok())
    };
    match observe() {
        Ok(ok) => ok.encode(env),
        Err(error) => result::<rustler::Atom>(env, Err(error)),
    }
}

/// `{:ok, [{pattern, [{bucket_start_ns, count}]}]}`, largest total first.
#[rustler::nif(schedule = "DirtyCpu")]
fn log_pattern_finish<'a>(env: Env<'a>, scan: ResourceArc<Scan>) -> Term<'a> {
    let finish = || {
        let miner = scan
            .0
            .lock()
            .map_err(|_| Error::Fallback)?
            .take()
            .and_then(|state| state.patterns)
            .ok_or(Error::Fallback)?;
        Ok(miner
            .finish()
            .into_iter()
            .map(|(pattern, counts)| {
                let mut binary = NewBinary::new(env, pattern.len());
                binary.as_mut_slice().copy_from_slice(&pattern);
                (Binary::from(binary), counts)
            })
            .collect::<Vec<_>>())
    };
    result(env, finish())
}

// Erlang term order, descending: time, then `:error` (an atom) above every
// number, then numeric value. Series values are homogeneous in kind.
fn descending(a: (i64, Sample), b: (i64, Sample)) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    b.0.cmp(&a.0).then_with(|| match (a.1, b.1) {
        (Sample::Error, Sample::Error) => Ordering::Equal,
        (Sample::Error, _) => Ordering::Less,
        (_, Sample::Error) => Ordering::Greater,
        (Sample::Int(x), Sample::Int(y)) => y.cmp(&x),
        (x, y) => sample_float(y)
            .partial_cmp(&sample_float(x))
            .unwrap_or(Ordering::Equal),
    })
}

fn sample_float(s: Sample) -> f64 {
    match s {
        Sample::Float(f) => f,
        Sample::Int(i) => i as f64,
        Sample::Error => f64::NAN,
    }
}

struct Selection {
    start: i64,
    end: i64,
    service: Option<Vec<u8>>,
    matchers: Vec<Matcher>,
    line_filters: Vec<LineFilter>,
}

type Labels = BTreeMap<String, String>;

struct EntryLabels {
    labels: Labels,
    stream_keys: HashSet<String>,
}
// A row's stream identity within one batch: resource, attributes, service, level.
type StreamKey<'b> = (&'b str, &'b str, Option<&'b str>, Option<&'b str>);

impl State {
    fn scan(&mut self, bytes: Bytes, selection: &Selection) -> Res<(usize, usize)> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|_| Error::Fallback)?;
        let schema = builder.schema().clone();
        let mut indexes = Vec::with_capacity(COLUMNS.len());
        for name in COLUMNS {
            indexes.push(schema.index_of(name).map_err(|_| Error::Fallback)?);
        }
        let projection = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), indexes);
        let reader = builder
            .with_projection(projection)
            .with_batch_size(4096)
            .build()
            .map_err(|_| Error::Fallback)?;
        let (mut chosen, mut kept) = (0, 0);
        for batch in reader {
            let batch = batch.map_err(|_| Error::Fallback)?;
            let column = |name| batch.column_by_name(name).ok_or(Error::Fallback);
            let strings = |name| -> Res<&StringArray> {
                column(name)?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or(Error::Fallback)
            };
            let times = column("timestamp_ns")?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(Error::Fallback)?;
            let (service, level, body) = (
                strings("service")?,
                strings("severity_text")?,
                strings("body")?,
            );
            let (attributes, resource) = (strings("attributes")?, strings("resource")?);
            // Stream labels repeat for runs of rows: build each set once.
            let mut bases: HashMap<StreamKey, Rc<EntryLabels>> = HashMap::new();
            for row in 0..batch.num_rows() {
                if times.is_null(row) {
                    continue;
                }
                let ts = times.value(row);
                if ts < selection.start || ts > selection.end {
                    continue;
                }
                let svc = (!service.is_null(row)).then(|| service.value(row));
                let lvl = (!level.is_null(row)).then(|| level.value(row));
                let res = if resource.is_null(row) {
                    ""
                } else {
                    resource.value(row)
                };
                if !selected(selection, res.as_bytes(), svc, lvl) {
                    continue;
                }
                let raw_body = (!body.is_null(row) && !body.value(row).is_empty())
                    .then(|| body.value(row).as_bytes());
                let matched = match raw_body {
                    Some(raw) => query_filter::line_bytes_for_match(raw),
                    None => std::borrow::Cow::Borrowed(&b""[..]),
                };
                if !selection.line_filters.iter().all(|f| f.matches(&matched)) {
                    continue;
                }
                chosen += 1;
                if let Some(miner) = self.patterns.as_mut() {
                    miner.train(ts, &matched);
                    kept += 1;
                    continue;
                }
                // Entry.line re-encodes non-string bodies; only strings match.
                if raw_body.is_some_and(|raw| {
                    raw.len() < 2 || raw[0] != b'"' || raw[raw.len() - 1] != b'"'
                }) {
                    return Err(Error::Fallback);
                }
                let attrs = if attributes.is_null(row) {
                    ""
                } else {
                    attributes.value(row)
                };
                let key = (res, attrs, svc, lvl);
                let base = match bases.get(&key) {
                    Some(base) => Rc::clone(base),
                    None => {
                        let base = Rc::new(entry_labels(res, attrs, svc, lvl)?);
                        bases.insert(key, Rc::clone(&base));
                        base
                    }
                };
                if let Some(labels) = self.pipeline(&matched, &base)? {
                    kept += 1;
                    self.observe(ts, &matched, &labels)?;
                }
            }
        }
        Ok((chosen, kept))
    }

    // Returns the entry's labels after every stage, or None when dropped.
    fn pipeline<'l>(
        &self,
        line: &[u8],
        base: &'l EntryLabels,
    ) -> Res<Option<std::borrow::Cow<'l, Labels>>> {
        let mut labels = std::borrow::Cow::Borrowed(&base.labels);
        for stage in &self.stages {
            match stage {
                Stage::Line { negate, finder } => {
                    if finder.find(line).is_some() == *negate {
                        return Ok(None);
                    }
                }
                Stage::LineRe { negate, regex } => {
                    if regex.is_match(line) == *negate {
                        return Ok(None);
                    }
                }
                Stage::Logfmt => {
                    let pairs = logfmt(line)?;
                    let owned = labels.to_mut();
                    for (k, v) in pairs {
                        let target = parsed_name(&base.stream_keys, k);
                        owned.insert(target, v);
                    }
                }
                Stage::Regexp { regex, names } => {
                    if let Some(captures) = regex.captures(line) {
                        let owned = labels.to_mut();
                        for (name, index) in names {
                            let value = captures.get(*index).map_or(&b""[..], |m| m.as_bytes());
                            let value =
                                String::from_utf8(value.to_vec()).map_err(|_| Error::Fallback)?;
                            owned.insert(parsed_name(&base.stream_keys, name.clone()), value);
                        }
                    }
                }
                Stage::Labels(filter) => {
                    if !evaluate(filter, &labels)? {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(Some(labels))
    }

    fn observe(&mut self, ts: i64, line: &[u8], labels: &Labels) -> Res<()> {
        if labels.get("__error__").is_some_and(|e| !e.is_empty()) {
            return Err(Error::Pipeline);
        }
        let sample = match &self.value {
            Value::Count => Sample::Float(1.0),
            Value::Bytes => Sample::Int(i64::try_from(line.len()).map_err(|_| Error::Fallback)?),
            Value::Unwrap(name) => unwrap_value(labels.get(name))?,
        };
        let key: Vec<(String, String)> = labels
            .iter()
            .filter(|(k, _)| self.drop.as_deref() != Some(k.as_str()))
            .filter(|(k, _)| match &self.grouping {
                Grouping::All => true,
                Grouping::By(names) => names.iter().any(|n| n == *k),
                Grouping::Without(names) => !names.iter().any(|n| n == *k),
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.samples += 1;
        if self.samples as u64 > self.max_samples {
            return Err(Error::Limit);
        }
        self.series.entry(key).or_default().push((ts, sample));
        Ok(())
    }
}

// Parser collisions use the immutable entry labels, not previously extracted
// labels. Exactly one suffix matches the Elixir pipeline's provenance rule.
fn parsed_name(base: &HashSet<String>, mut name: String) -> String {
    if base.contains(&name) {
        name.push_str("_extracted");
    }
    name
}

// The log decoder's stream selection, with promoted service/level mirrors.
fn present(value: Option<&str>) -> Option<&[u8]> {
    value.filter(|v| !v.is_empty()).map(str::as_bytes)
}

fn selected(
    selection: &Selection,
    resource: &[u8],
    service: Option<&str>,
    level: Option<&str>,
) -> bool {
    if let Some(wanted) = &selection.service {
        if service.map(str::as_bytes) != Some(wanted.as_slice()) {
            return false;
        }
    }
    selection.matchers.iter().all(|m| {
        let promoted = match m.name() {
            b"service" | b"service_name" => present(service),
            b"level" | b"detected_level" => present(level),
            _ => None,
        };
        if let Some(value) = promoted {
            return m.evaluate_present(value);
        }
        if resource.is_empty() {
            return m.evaluate_absent();
        }
        match query_filter::find_label(resource, m.name()) {
            Some(value) => m.evaluate_present(&value),
            None => m.evaluate_absent(),
        }
    })
}

// `Entry.from_record/1`: resource, attributes over it, then promoted fields
// only where absent. Only flat string values reproduce its stringification.
fn entry_labels(
    resource: &str,
    attributes: &str,
    service: Option<&str>,
    level: Option<&str>,
) -> Res<EntryLabels> {
    let mut labels = Labels::new();
    let mut stream_keys = HashSet::new();
    for (index, json) in [resource, attributes].into_iter().enumerate() {
        if json.is_empty() {
            continue;
        }
        for (k, v) in query_filter::strict_string_pairs(json.as_bytes()).ok_or(Error::Fallback)? {
            let k = String::from_utf8(k.into_owned()).map_err(|_| Error::Fallback)?;
            let v = String::from_utf8(v.into_owned()).map_err(|_| Error::Fallback)?;
            if index == 0 {
                stream_keys.insert(k.clone());
            }
            labels.insert(k, v);
        }
    }
    for (name, value) in [
        ("service", service),
        ("service_name", service),
        ("level", level),
        ("detected_level", level),
    ] {
        if let Some(value) = value {
            stream_keys.insert(name.to_owned());
            labels
                .entry(name.to_owned())
                .or_insert_with(|| value.to_owned());
        }
    }
    Ok(EntryLabels {
        labels,
        stream_keys,
    })
}

// `Pulso.LogQL.Pipeline.Logfmt.parse(line, false)`.
fn logfmt(line: &[u8]) -> Res<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    let n = line.len();
    let mut i = 0;
    loop {
        while i < n && matches!(line[i], b' ' | b'\t') {
            i += 1;
        }
        if i >= n {
            break;
        }
        let key_start = i;
        while i < n && !matches!(line[i], b'=' | b' ' | b'\t') {
            i += 1;
        }
        if i == key_start {
            break;
        }
        let key = &line[key_start..i];
        if i < n && line[i] == b'=' {
            i += 1;
            let value = if i < n && line[i] == b'"' {
                i += 1;
                let mut value = Vec::new();
                while i < n && line[i] != b'"' {
                    if line[i] == b'\\' && i + 1 < n {
                        value.push(line[i + 1]);
                        i += 2;
                    } else {
                        value.push(line[i]);
                        i += 1;
                    }
                }
                if i < n {
                    i += 1;
                }
                value
            } else {
                let start = i;
                while i < n && !matches!(line[i], b' ' | b'\t') {
                    i += 1;
                }
                line[start..i].to_vec()
            };
            pairs.push((
                String::from_utf8(key.to_vec()).map_err(|_| Error::Fallback)?,
                String::from_utf8(value).map_err(|_| Error::Fallback)?,
            ));
        }
    }
    Ok(pairs)
}

fn evaluate(filter: &Filter, labels: &Labels) -> Res<bool> {
    Ok(match filter {
        Filter::And(l, r) => evaluate(l, labels)? && evaluate(r, labels)?,
        Filter::Or(l, r) => evaluate(l, labels)? || evaluate(r, labels)?,
        Filter::Str {
            name,
            negate,
            value,
        } => (labels.get(name).map_or(&b""[..], |v| v.as_bytes()) == value.as_slice()) != *negate,
        Filter::Re {
            name,
            negate,
            regex,
        } => regex.is_match(labels.get(name).map_or(&b""[..], |v| v.as_bytes())) != *negate,
        Filter::Num { name, op, value } => {
            match label_number(labels.get(name).map_or("", String::as_str))? {
                None => false,
                Some(actual) => compare(*op, actual, *value)?,
            }
        }
    })
}

// `Integer.parse` then `Float.parse`, each requiring the whole string.
fn label_number(text: &str) -> Res<Option<Number>> {
    let bytes = text.as_bytes();
    let digits = |s: &[u8]| !s.is_empty() && s.iter().all(u8::is_ascii_digit);
    let unsigned = bytes
        .strip_prefix(b"-")
        .or_else(|| bytes.strip_prefix(b"+"))
        .unwrap_or(bytes);
    if digits(unsigned) {
        return text
            .parse::<i64>()
            .map(|i| Some(Number::Int(i)))
            .map_err(|_| Error::Fallback);
    }
    let (mantissa, exponent) = match unsigned.iter().position(|b| matches!(b, b'e' | b'E')) {
        Some(i) => (&unsigned[..i], Some(&unsigned[i + 1..])),
        None => (unsigned, None),
    };
    let (whole, fraction) = match mantissa.iter().position(|&b| b == b'.') {
        Some(i) => (&mantissa[..i], Some(&mantissa[i + 1..])),
        None => (mantissa, None),
    };
    let exponent_ok = exponent.is_none_or(|e| {
        digits(
            e.strip_prefix(b"-")
                .or_else(|| e.strip_prefix(b"+"))
                .unwrap_or(e),
        )
    });
    if !digits(whole) || !fraction.is_none_or(digits) || !exponent_ok {
        return Ok(None);
    }
    let value: f64 = text.parse().map_err(|_| Error::Fallback)?;
    if value.is_finite() {
        Ok(Some(Number::Float(value)))
    } else {
        Err(Error::Fallback)
    }
}

// Erlang numeric comparison; mixed int/float only where floats are exact.
fn compare(op: CmpOp, actual: Number, expected: Number) -> Res<bool> {
    let exact = |n: Number| {
        matches!(n, Number::Float(_)) || matches!(n, Number::Int(i) if i.unsigned_abs() <= 1 << 53)
    };
    let ordering = match (actual, expected) {
        (Number::Int(a), Number::Int(b)) => a.cmp(&b),
        (a, b) if exact(a) && exact(b) => {
            a.float().partial_cmp(&b.float()).ok_or(Error::Fallback)?
        }
        _ => return Err(Error::Fallback),
    };
    use std::cmp::Ordering::*;
    Ok(match op {
        CmpOp::Eq => ordering == Equal,
        CmpOp::Neq => ordering != Equal,
        CmpOp::Lt => ordering == Less,
        CmpOp::Lte => ordering != Greater,
        CmpOp::Gt => ordering == Greater,
        CmpOp::Gte => ordering != Less,
    })
}

// `FloatParser.parse/1` for unwrap: plain decimals parse here; a missing
// label or text without digits (other than NaN/Inf spellings) is an
// extraction error; every other spelling falls back.
fn unwrap_value(value: Option<&String>) -> Res<Sample> {
    let Some(text) = value else {
        return Ok(Sample::Error);
    };
    let bytes = text.as_bytes();
    let unsigned = bytes
        .strip_prefix(b"-")
        .or_else(|| bytes.strip_prefix(b"+"))
        .unwrap_or(bytes);
    let (mantissa, exponent) = match unsigned.iter().position(|b| matches!(b, b'e' | b'E')) {
        Some(i) => (&unsigned[..i], Some(&unsigned[i + 1..])),
        None => (unsigned, None),
    };
    let digits = |s: &[u8]| !s.is_empty() && s.iter().all(u8::is_ascii_digit);
    let (whole, fraction) = match mantissa.iter().position(|&b| b == b'.') {
        Some(i) => (&mantissa[..i], Some(&mantissa[i + 1..])),
        None => (mantissa, None),
    };
    let mantissa_ok = match fraction {
        None => digits(whole),
        Some(f) => {
            (digits(whole) && (f.is_empty() || digits(f))) || (whole.is_empty() && digits(f))
        }
    };
    let exponent_ok = exponent.is_none_or(|e| {
        digits(
            e.strip_prefix(b"-")
                .or_else(|| e.strip_prefix(b"+"))
                .unwrap_or(e),
        )
    });
    if mantissa_ok && exponent_ok {
        let normalized = text.replace(".e", ".0e").replace(".E", ".0E");
        let normalized = if normalized.ends_with('.') {
            format!("{normalized}0")
        } else {
            normalized
        };
        let value: f64 = normalized.parse().map_err(|_| Error::Fallback)?;
        return if value.is_finite() {
            Ok(Sample::Float(value))
        } else {
            Err(Error::Fallback)
        };
    }
    let lower = text.to_ascii_lowercase();
    let special = [
        "nan",
        "inf",
        "+inf",
        "-inf",
        "infinity",
        "+infinity",
        "-infinity",
    ]
    .contains(&lower.as_str());
    if !special && !bytes.iter().any(u8::is_ascii_digit) {
        Ok(Sample::Error)
    } else {
        Err(Error::Fallback)
    }
}
