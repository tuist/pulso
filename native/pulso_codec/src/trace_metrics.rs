//! TraceQL metrics (Tempo's `/api/metrics/query_range` and `/api/metrics/query`)
//! over stored trace segments: spans are selected by the bounded selector
//! program of `tempo_expression` (or all spans), optionally only roots, then
//! reduced per series and time interval without building Erlang terms.
//!
//! Semantics follow Tempo's first-stage aggregation: spans are placed by
//! start time into right-closed intervals `(start + i*step, start + (i+1)*step]`
//! (instant queries use one closed interval), series are keyed by the
//! typed `by(...)` values with missing values dropped from the labels, and
//! `count_over_time`/`rate`/`histogram_over_time` report every interval of a
//! series while `min`/`max`/`sum`/`avg_over_time` report intervals with
//! values only. Durations bucket as powers of two of nanoseconds reported in
//! seconds, and quantiles interpolate exponentially between those buckets.
use crate::json_read::RawFields;
use crate::out::{BinSink, Counter, Sink};
use crate::tempo_expression::Expression;
use crate::trace_search::{field, object, result, string, Error, Res};
use arrow::array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rustler::{Binary, Env, ListIterator, ResourceArc, Term};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

const MAX_SERIES: usize = 10_000;
const MAX_POINTS: usize = 2_000_000;
const MAX_OUTPUT: usize = 64 * 1024 * 1024;
const KINDS: [&str; 6] = [
    "unspecified",
    "internal",
    "server",
    "client",
    "producer",
    "consumer",
];
const STATUSES: [&str; 3] = ["unset", "ok", "error"];

#[derive(Clone)]
enum Lookup {
    Span(String),
    Resource(String),
    // Span first, then resource, as Tempo resolves unscoped attributes.
    Unscoped(String),
    Name,
    Status,
    Kind,
    Duration,
}

impl Lookup {
    fn original(&self) -> bool {
        matches!(
            self,
            Lookup::Span(_) | Lookup::Resource(_) | Lookup::Unscoped(_)
        )
    }
}

#[derive(Clone, PartialEq)]
enum Op {
    Count,
    Rate,
    Min,
    Max,
    Sum,
    Avg,
    Histogram,
    Quantile(Vec<f64>),
}

impl Op {
    fn name(&self) -> &'static str {
        match self {
            Op::Count => "count_over_time",
            Op::Rate => "rate",
            Op::Min => "min_over_time",
            Op::Max => "max_over_time",
            Op::Sum => "sum_over_time",
            Op::Avg => "avg_over_time",
            Op::Histogram => "histogram_over_time",
            Op::Quantile(_) => "quantile_over_time",
        }
    }

    fn bucketed(&self) -> bool {
        matches!(self, Op::Histogram | Op::Quantile(_))
    }

    fn zero_filled(&self) -> bool {
        matches!(self, Op::Count | Op::Rate | Op::Histogram | Op::Quantile(_))
    }
}

struct Mapper {
    start: u64,
    end: u64,
    step: u64,
    instant: bool,
    count: usize,
}

impl Mapper {
    fn interval(&self, ts: u64) -> Option<usize> {
        if self.instant {
            return (ts >= self.start && ts <= self.end && self.end != self.start).then_some(0);
        }
        if ts <= self.start || ts > self.end || self.step == 0 {
            return None;
        }
        let offset = ts - self.start;
        let mut i = offset / self.step;
        if i * self.step == offset {
            i -= 1;
        }
        usize::try_from(i).ok().filter(|i| *i < self.count)
    }

    fn timestamp_ms(&self, i: usize) -> i64 {
        let ns = if self.instant {
            self.end
        } else {
            self.start + (i as u64 + 1) * self.step
        };
        (ns / 1_000_000) as i64
    }
}

// Raw typed OTLP AnyValue JSON per `by` entry (None when missing), plus the
// power-of-two bucket of bucketed operations.
type Labels = Vec<Option<Vec<u8>>>;
type Key = (Labels, Option<u64>);
// A finished series: output labels and one value per interval (NaN: none).
type Final = (Vec<(String, Vec<u8>)>, Vec<f64>);

struct Series {
    values: Vec<f64>,
    counts: Vec<u64>,
}

struct State {
    program: Option<Expression>,
    root_only: bool,
    needs_attributes: bool,
    requires_originals: bool,
    op: Op,
    value: Option<Lookup>,
    by: Vec<(Lookup, String)>,
    mapper: Mapper,
    // Second stage: `topk(k)` (true) or `bottomk(k)` (false).
    second: Option<(bool, usize)>,
    series: HashMap<Key, Series>,
    work: usize,
}

pub struct Metrics(Mutex<Option<State>>);

#[rustler::resource_impl]
impl rustler::Resource for Metrics {}

fn lookup(scope: &str, key: &str) -> Res<Lookup> {
    Ok(match (scope, key) {
        ("span", k) if !k.is_empty() => Lookup::Span(k.to_owned()),
        ("resource", k) if !k.is_empty() => Lookup::Resource(k.to_owned()),
        ("unscoped", k) if !k.is_empty() => Lookup::Unscoped(k.to_owned()),
        ("intrinsic", "name") => Lookup::Name,
        ("intrinsic", "status") => Lookup::Status,
        ("intrinsic", "kind") => Lookup::Kind,
        ("intrinsic", "duration") => Lookup::Duration,
        _ => return Err(Error::Invalid),
    })
}

#[rustler::nif]
fn trace_metrics_api_version() -> u32 {
    1
}

/// `program` is a `tempo_expression` program or nil; `op` an operation name;
/// `value` nil or `{scope, key}`; `by` `[{scope, key, label}]`; times in
/// nanoseconds, already aligned by the caller; `second` nil or
/// `{"topk" | "bottomk", k}`.
#[allow(clippy::too_many_arguments)]
#[rustler::nif(schedule = "DirtyCpu")]
fn trace_metrics_new<'a>(
    env: Env<'a>,
    program: Term<'a>,
    root_only: bool,
    op: String,
    quantiles: Vec<f64>,
    value: Term<'a>,
    by: Vec<(String, String, String)>,
    window: (u64, u64, u64, bool),
    second: Term<'a>,
) -> Term<'a> {
    let build = || {
        let program = match program.decode::<Vec<(String, String, String, String)>>() {
            Ok(program) => Some(Expression::compile(program).ok_or(Error::Invalid)?),
            Err(_) if program.decode::<ListIterator>().is_err() => None,
            Err(_) => return Err(Error::Invalid),
        };
        let op = match op.as_str() {
            "count_over_time" => Op::Count,
            "rate" => Op::Rate,
            "min_over_time" => Op::Min,
            "max_over_time" => Op::Max,
            "sum_over_time" => Op::Sum,
            "avg_over_time" => Op::Avg,
            "histogram_over_time" => Op::Histogram,
            "quantile_over_time"
                if !quantiles.is_empty() && quantiles.iter().all(|q| (0.0..=1.0).contains(q)) =>
            {
                Op::Quantile(quantiles)
            }
            _ => return Err(Error::Invalid),
        };
        let value = match value.decode::<(String, String)>() {
            Ok((scope, key)) => Some(lookup(&scope, &key)?),
            Err(_) => None,
        };
        if matches!(op, Op::Count | Op::Rate) != value.is_none() || by.len() > 5 {
            return Err(Error::Invalid);
        }
        let by = by
            .into_iter()
            .map(|(scope, key, label)| Ok((lookup(&scope, &key)?, label)))
            .collect::<Res<Vec<_>>>()?;
        let second = match second.decode::<(String, usize)>() {
            Ok((op, k)) if k > 0 && (op == "topk" || op == "bottomk") => Some((op == "topk", k)),
            Ok(_) => return Err(Error::Invalid),
            Err(_) => None,
        };
        let (start, end, step, instant) = window;
        if end <= start || step == 0 || (instant && step != end - start) {
            return Err(Error::Invalid);
        }
        let count = if instant {
            1
        } else {
            usize::try_from((end - start) / step).map_err(|_| Error::Invalid)?
        };
        if count == 0 || count > 11_000 {
            return Err(Error::Invalid);
        }
        let needs_attributes = value.as_ref().is_some_and(Lookup::original)
            || by.iter().any(|(lookup, _)| lookup.original());
        let requires_originals = needs_attributes || program.is_some();
        Ok(ResourceArc::new(Metrics(Mutex::new(Some(State {
            program,
            root_only,
            needs_attributes,
            requires_originals,
            op,
            value,
            by,
            mapper: Mapper {
                start,
                end,
                step,
                instant,
                count,
            },
            second,
            series: HashMap::new(),
            work: 0,
        })))))
    };
    result(env, build())
}

/// Fold one trace segment, ignoring spans that start before `floor` (the
/// retention floor). `{:ok, rows}`.
#[rustler::nif(schedule = "DirtyCpu")]
fn trace_metrics_add<'a>(
    env: Env<'a>,
    metrics: ResourceArc<Metrics>,
    blob: Binary<'a>,
    floor: i64,
) -> Term<'a> {
    let add = || {
        let mut lock = metrics.0.try_lock().map_err(|_| Error::Budget)?;
        let state = lock.as_mut().ok_or(Error::Invalid)?;
        let outcome = state.scan(crate::erlang_bytes::from_binary(blob), floor);
        if outcome.is_err() {
            *lock = None;
        }
        outcome
    };
    result(env, add())
}

/// The response JSON: Tempo's `QueryRangeResponse`, or `QueryInstantResponse`
/// for instant queries.
#[rustler::nif(schedule = "DirtyCpu")]
fn trace_metrics_finish<'a>(env: Env<'a>, metrics: ResourceArc<Metrics>) -> Term<'a> {
    let finish = || {
        let state = metrics
            .0
            .lock()
            .map_err(|_| Error::Invalid)?
            .take()
            .ok_or(Error::Invalid)?;
        state.finish(env)
    };
    result(env, finish())
}

impl State {
    fn scan(&mut self, blob: bytes::Bytes, floor: i64) -> Res<usize> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(blob)
            .map_err(|_| Error::Invalid)?
            .with_batch_size(1024)
            .build()
            .map_err(|_| Error::Invalid)?;
        let mut rows = 0;
        for batch in reader {
            let batch = batch.map_err(|_| Error::Invalid)?;
            let ints = |name| {
                batch
                    .column_by_name(name)
                    .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                    .ok_or(Error::Invalid)
            };
            let strings = |name| {
                batch
                    .column_by_name(name)
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .ok_or(Error::Invalid)
            };
            let (starts, ends) = (ints("timestamp_ns")?, ints("end_timestamp_ns")?);
            let (ids, payloads) = (strings("trace_id")?, strings("payload")?);
            for i in 0..batch.num_rows() {
                if starts.is_null(i) || ends.is_null(i) || ids.is_null(i) || payloads.is_null(i) {
                    return Err(Error::Invalid);
                }
                rows += 1;
                let start = starts.value(i);
                if start < floor || start < 0 {
                    continue;
                }
                let Some(interval) = self.mapper.interval(start as u64) else {
                    continue;
                };
                let payload = payloads.value(i).as_bytes();
                let fields =
                    crate::tempo_search::validate_row(ids.value(i), start, ends.value(i), payload)?;
                self.observe(&fields, interval, start, ends.value(i))?;
            }
        }
        Ok(rows)
    }

    fn observe(
        &mut self,
        fields: &RawFields<'_>,
        interval: usize,
        start: i64,
        end: i64,
    ) -> Res<()> {
        if self.requires_originals && field(fields, b"otlp").unwrap_or(b"null") == b"null" {
            return Err(Error::Unknown);
        }
        if self.root_only && field(fields, b"parent_span_id")? != b"null" {
            return Ok(());
        }
        if let Some(program) = &self.program {
            if !program.evaluate(fields, &mut self.work, false)?.matched {
                return Ok(());
            }
        }
        let owners = if self.needs_attributes {
            Owners::read(fields)?
        } else {
            Owners::default()
        };
        let duration = (end - start) as u64;
        let mut labels = Vec::with_capacity(self.by.len());
        for (lookup, _) in &self.by {
            labels.push(owners.value(fields, lookup, duration)?);
        }
        let value = match &self.value {
            None => 1.0,
            Some(Lookup::Duration) => duration as f64,
            Some(lookup) => match owners.value(fields, lookup, duration)? {
                Some(raw) => number(&raw).unwrap_or(f64::NAN),
                None => f64::NAN,
            },
        };
        let bucket = if self.op.bucketed() {
            match bucket(&self.value, value) {
                Some(bucket) => Some(bucket.to_bits()),
                None => return Ok(()),
            }
        } else {
            None
        };
        // Seconds for duration values of non-bucketed operations.
        let value = if matches!(self.value, Some(Lookup::Duration)) {
            value / 1e9
        } else {
            value
        };
        let key = (labels, bucket);
        let full = self.series.len() >= MAX_SERIES
            || (self.series.len() + 1) * self.mapper.count > MAX_POINTS;
        let series = match self.series.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                if full {
                    return Err(Error::Groups);
                }
                let fill = if self.op.zero_filled() { 0.0 } else { f64::NAN };
                entry.insert(Series {
                    values: vec![fill; self.mapper.count],
                    counts: vec![0; self.mapper.count],
                })
            }
        };
        let slot = &mut series.values[interval];
        match self.op {
            Op::Count | Op::Rate | Op::Histogram | Op::Quantile(_) => *slot += 1.0,
            Op::Min if !value.is_nan() => {
                *slot = if slot.is_nan() {
                    value
                } else {
                    slot.min(value)
                }
            }
            Op::Max if !value.is_nan() => {
                *slot = if slot.is_nan() {
                    value
                } else {
                    slot.max(value)
                }
            }
            Op::Sum | Op::Avg if !value.is_nan() => {
                *slot = if slot.is_nan() { value } else { *slot + value };
                series.counts[interval] += 1;
            }
            _ => {}
        }
        Ok(())
    }

    fn finish(mut self, env: Env<'_>) -> Res<Binary<'_>> {
        let mut finals = self.final_series()?;
        // Finals own their values. Release accumulators before top-k/output
        // allocation instead of retaining both representations through finish.
        self.series = HashMap::new();
        if let Some((top, k)) = self.second {
            finals = select_k(finals, top, k, self.mapper.count);
        }
        let mut counter = Counter::new(MAX_OUTPUT);
        self.write(&finals, &mut counter)?;
        let mut out = BinSink::with_capacity(counter.len());
        self.write(&finals, &mut out)?;
        Ok(out.finish(env))
    }

    fn write<S: Sink>(&self, finals: &[Final], out: &mut S) -> Res<()> {
        let mut written = 0;
        out.extend(b"{\"series\":[");
        for (names, values) in finals {
            self.write_series(out, &mut written, names, values);
            if out.len() > MAX_OUTPUT {
                return Err(Error::Budget);
            }
        }
        out.extend(b"]}");
        if out.len() > MAX_OUTPUT {
            return Err(Error::Budget);
        }
        Ok(())
    }

    fn final_series(&self) -> Res<Vec<Final>> {
        let mut finals = Vec::new();
        let rate = 1e9 / self.mapper.step as f64;
        if let Op::Quantile(qs) = &self.op {
            // Group the power-of-two buckets of each series per interval.
            let mut groups: BTreeMap<Labels, Vec<BTreeMap<u64, f64>>> = BTreeMap::new();
            for ((labels, bucket), series) in &self.series {
                let bucket = bucket.ok_or(Error::Invalid)?;
                let group = groups
                    .entry(labels.clone())
                    .or_insert_with(|| vec![BTreeMap::new(); self.mapper.count]);
                for (i, count) in series.values.iter().enumerate() {
                    if *count > 0.0 {
                        group[i].insert(bucket, *count);
                    }
                }
            }
            for (labels, intervals) in groups {
                for q in qs {
                    let values: Vec<f64> = intervals
                        .iter()
                        .map(|buckets| {
                            let mut sorted: Vec<(f64, f64)> = buckets
                                .iter()
                                .map(|(bits, n)| (f64::from_bits(*bits), *n))
                                .collect();
                            sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
                            log2_quantile(*q, &sorted)
                        })
                        .collect();
                    let mut names = self.label_list(&labels);
                    names.push(("p".to_owned(), double_value(*q)));
                    finals.push((names, values));
                }
            }
        } else {
            let mut ordered: Vec<_> = self.series.iter().collect();
            ordered.sort_by(|a, b| {
                a.0 .0.cmp(&b.0 .0).then_with(|| {
                    let (x, y) = (a.0 .1.map(f64::from_bits), b.0 .1.map(f64::from_bits));
                    x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal)
                })
            });
            for ((labels, bucket), series) in ordered {
                let mut names = if self.by.is_empty() && bucket.is_none() {
                    vec![(
                        "__name__".to_owned(),
                        string_value(self.op.name().as_bytes()),
                    )]
                } else {
                    self.label_list(labels)
                };
                if let Some(bits) = bucket {
                    names.push(("__bucket".to_owned(), double_value(f64::from_bits(*bits))));
                }
                let values: Vec<f64> = match self.op {
                    Op::Rate => series.values.iter().map(|v| v * rate).collect(),
                    Op::Avg => series
                        .values
                        .iter()
                        .zip(&series.counts)
                        .map(|(sum, n)| if *n == 0 { f64::NAN } else { sum / *n as f64 })
                        .collect(),
                    _ => series.values.clone(),
                };
                finals.push((names, values));
            }
        }
        Ok(finals)
    }

    // Labels in `by` order without missing values; when all are missing,
    // the first is kept as the string "nil", as Tempo does.
    fn label_list(&self, values: &[Option<Vec<u8>>]) -> Vec<(String, Vec<u8>)> {
        let mut labels: Vec<(String, Vec<u8>)> = self
            .by
            .iter()
            .zip(values)
            .filter_map(|((_, name), value)| value.clone().map(|v| (name.clone(), v)))
            .collect();
        if labels.is_empty() && !self.by.is_empty() && !self.op.bucketed() {
            labels.push((self.by[0].1.clone(), string_value(b"nil")));
        }
        labels
    }

    fn write_series<S: Sink>(
        &self,
        out: &mut S,
        written: &mut usize,
        labels: &[(String, Vec<u8>)],
        values: &[f64],
    ) {
        let mut samples = values
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, v)| !v.is_nan())
            .peekable();
        if samples.peek().is_none() {
            return;
        }
        if *written > 0 {
            out.push(b',');
        }
        *written += 1;
        out.extend(b"{\"labels\":[");
        for (i, (name, value)) in labels.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend(b"{\"key\":");
            crate::json_write::write_str(out, name.as_bytes());
            out.extend(b",\"value\":");
            out.extend(value);
            out.push(b'}');
        }
        if self.mapper.instant {
            out.extend(b"],\"value\":");
            write_number(out, samples.next().expect("nonempty samples").1);
            out.push(b'}');
            return;
        }
        out.extend(b"],\"samples\":[");
        for (j, (i, v)) in samples.enumerate() {
            if j > 0 {
                out.push(b',');
            }
            out.extend(b"{\"timestampMs\":\"");
            out.extend(
                itoa::Buffer::new()
                    .format(self.mapper.timestamp_ms(i))
                    .as_bytes(),
            );
            out.extend(b"\",\"value\":");
            write_number(out, v);
            out.push(b'}');
        }
        out.extend(b"]}");
    }
}

// Tempo's second stage: per interval, the `k` series with the largest
// (`topk`) or smallest values keep that interval's value and every other
// value becomes empty. With at most `k` series the input is unchanged. Ties go
// to the series that comes first in label order.
fn select_k(finals: Vec<Final>, top: bool, k: usize, intervals: usize) -> Vec<Final> {
    if finals.len() <= k {
        return finals;
    }
    let mut winners = Vec::new();
    for i in 0..intervals {
        let mut candidates: Vec<(usize, f64)> = finals
            .iter()
            .enumerate()
            .filter_map(|(s, (_, values))| {
                values
                    .get(i)
                    .copied()
                    .filter(|v| !v.is_nan())
                    .map(|v| (s, v))
            })
            .collect();
        candidates.sort_by(|a, b| {
            let order = a.1.total_cmp(&b.1);
            (if top { order.reverse() } else { order }).then(a.0.cmp(&b.0))
        });
        Extend::extend(
            &mut winners,
            candidates.into_iter().take(k).map(|(s, v)| (s, i, v)),
        );
    }
    let mut kept = vec![vec![f64::NAN; intervals]; finals.len()];
    for (s, i, v) in winners {
        kept[s][i] = v;
    }
    finals
        .into_iter()
        .zip(kept)
        .map(|((names, _), values)| (names, values))
        .filter(|(_, values)| values.iter().any(|v| !v.is_nan()))
        .collect()
}

// The typed attribute owners of a span, read once per observed span.
#[derive(Default)]
struct Owners<'a> {
    resource: BTreeMap<String, &'a [u8]>,
    span: BTreeMap<String, &'a [u8]>,
}

impl<'a> Owners<'a> {
    fn read(fields: &RawFields<'a>) -> Res<Self> {
        match field(fields, b"otlp") {
            Ok(raw) if raw != b"null" => {
                let meta = object(raw)?;
                Ok(Owners {
                    resource: crate::tempo_expression::attributes(field(&meta, b"resource")?)?,
                    span: crate::tempo_expression::attributes(field(&meta, b"attributes")?)?,
                })
            }
            _ => Ok(Owners {
                resource: BTreeMap::new(),
                span: BTreeMap::new(),
            }),
        }
    }

    // The value as typed OTLP AnyValue JSON, or None when missing or of a
    // type Tempo cannot group by (arrays, maps, bytes).
    fn value(
        &self,
        fields: &RawFields<'_>,
        lookup: &Lookup,
        duration: u64,
    ) -> Res<Option<Vec<u8>>> {
        let attribute = |raw: Option<&&[u8]>| -> Res<Option<Vec<u8>>> {
            match raw {
                None => Ok(None),
                Some(raw) => scalar(raw),
            }
        };
        Ok(match lookup {
            Lookup::Span(key) => attribute(self.span.get(key))?,
            Lookup::Resource(key) => attribute(self.resource.get(key))?,
            Lookup::Unscoped(key) => match attribute(self.span.get(key))? {
                Some(v) => Some(v),
                None => attribute(self.resource.get(key))?,
            },
            Lookup::Name => Some(string_value(string(field(fields, b"name")?)?.as_bytes())),
            Lookup::Status => {
                let code = crate::tempo_search::integer(field(
                    &object(field(fields, b"status")?)?,
                    b"code",
                )?)?;
                Some(string_value(
                    STATUSES
                        .get(code as usize)
                        .ok_or(Error::Invalid)?
                        .as_bytes(),
                ))
            }
            Lookup::Kind => {
                let kind = crate::tempo_search::integer(field(fields, b"kind")?)?;
                Some(string_value(
                    KINDS.get(kind as usize).ok_or(Error::Invalid)?.as_bytes(),
                ))
            }
            Lookup::Duration => Some(format!("{{\"intValue\":\"{duration}\"}}").into_bytes()),
        })
    }
}

fn scalar(raw: &[u8]) -> Res<Option<Vec<u8>>> {
    let value = object(raw)?;
    for name in [
        b"stringValue".as_slice(),
        b"intValue",
        b"boolValue",
        b"doubleValue",
    ] {
        if field(&value, name).is_ok() {
            return Ok(Some(raw.to_vec()));
        }
    }
    Ok(None)
}

// Floats of numeric AnyValues; strings and other types are not numbers.
fn number(raw: &[u8]) -> Option<f64> {
    let value = object(raw).ok()?;
    if let Ok(n) = field(&value, b"intValue") {
        let text = string(n)
            .ok()
            .or_else(|| std::str::from_utf8(n).ok().map(str::to_owned))?;
        return text.parse::<i64>().ok().map(|n| n as f64);
    }
    let n = field(&value, b"doubleValue").ok()?;
    match string(n) {
        Ok(text) => match text.as_str() {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            other => other.parse().ok(),
        },
        Err(_) => std::str::from_utf8(n).ok()?.parse().ok(),
    }
}

// Power-of-two buckets: durations in nanoseconds reported as seconds,
// integer attributes as themselves. Values below 2 are dropped.
fn bucket(value: &Option<Lookup>, v: f64) -> Option<f64> {
    if v.is_nan() || v < 2.0 {
        return None;
    }
    let b = 2f64.powf((v as u64 as f64).log2().ceil());
    Some(if matches!(value, Some(Lookup::Duration)) {
        b / 1e9
    } else {
        b
    })
}

fn log2_quantile(p: f64, buckets: &[(f64, f64)]) -> f64 {
    let total: f64 = buckets.iter().map(|b| b.1).sum();
    if p.is_nan() || !(0.0..=1.0).contains(&p) || buckets.is_empty() || total == 0.0 {
        return 0.0;
    }
    let max = ((p * total).ceil() as u64).max(1) as f64;
    let (mut seen, mut index) = (0.0, 0);
    for (i, (bound, count)) in buckets.iter().enumerate() {
        index = i;
        if seen + count > max {
            break;
        }
        seen += count;
        if seen == max {
            return *bound;
        }
    }
    let interp = (max - seen) / buckets[index].1;
    let hi = buckets[index].0.log2();
    let lo = if index > 0 {
        buckets[index - 1].0.log2()
    } else {
        hi - 1.0
    };
    2f64.powf(lo + (hi - lo) * interp)
}

fn string_value(text: &[u8]) -> Vec<u8> {
    let mut out = b"{\"stringValue\":".to_vec();
    crate::json_write::write_str(&mut out, text);
    out.push(b'}');
    out
}

fn double_value(v: f64) -> Vec<u8> {
    let mut out = b"{\"doubleValue\":".to_vec();
    write_number(&mut out, v);
    out.push(b'}');
    out
}

// Protobuf JSON numbers; non-finite values as their string names.
fn write_number<S: Sink>(out: &mut S, v: f64) {
    if v.is_nan() {
        out.extend(b"\"NaN\"");
    } else if v.is_infinite() {
        out.extend(if v > 0.0 {
            b"\"Infinity\""
        } else {
            b"\"-Infinity\""
        });
    } else {
        out.extend(
            ryu::Buffer::new()
                .format_finite(v)
                .trim_end_matches(".0")
                .as_bytes(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_right_closed_intervals_like_tempo() {
        let m = Mapper {
            start: 100,
            end: 130,
            step: 10,
            instant: false,
            count: 3,
        };
        assert_eq!(m.interval(100), None);
        assert_eq!(m.interval(101), Some(0));
        assert_eq!(m.interval(110), Some(0));
        assert_eq!(m.interval(111), Some(1));
        assert_eq!(m.interval(130), Some(2));
        assert_eq!(m.interval(131), None);
    }

    #[test]
    fn log2_quantiles_interpolate_between_buckets() {
        // Buckets of 1s and 2s with two spans each.
        let buckets = [(1.0, 2.0), (2.0, 2.0)];
        assert_eq!(log2_quantile(0.5, &buckets), 1.0);
        assert_eq!(log2_quantile(1.0, &buckets), 2.0);
        assert!((log2_quantile(0.75, &buckets) - 2f64.powf(0.5)).abs() < 1e-12);
        assert_eq!(log2_quantile(0.5, &[]), 0.0);
    }

    #[test]
    fn buckets_durations_in_seconds() {
        assert_eq!(bucket(&Some(Lookup::Duration), 1.0), None);
        assert_eq!(
            bucket(&Some(Lookup::Duration), 3e9),
            Some(4_294_967_296.0 / 1e9)
        );
        assert_eq!(bucket(&Some(Lookup::Span("n".into())), 5.0), Some(8.0));
    }

    #[test]
    fn selects_k_series_per_interval() {
        let nan = f64::NAN;
        let series = |name: &str, values: Vec<f64>| -> Final {
            (
                vec![(name.to_owned(), string_value(name.as_bytes()))],
                values,
            )
        };
        let input = vec![
            series("a", vec![1.0, 5.0, nan]),
            series("b", vec![3.0, 2.0, nan]),
            series("c", vec![2.0, 4.0, 7.0]),
        ];
        let top = select_k(input.clone(), true, 1, 3);
        let values: Vec<(String, Vec<String>)> = top
            .iter()
            .map(|(n, v)| (n[0].0.clone(), v.iter().map(|x| format!("{x}")).collect()))
            .collect();
        assert_eq!(
            values,
            vec![
                ("a".into(), vec!["NaN".into(), "5".into(), "NaN".into()]),
                ("b".into(), vec!["3".into(), "NaN".into(), "NaN".into()]),
                ("c".into(), vec!["NaN".into(), "NaN".into(), "7".into()]),
            ]
        );
        let bottom = select_k(input.clone(), false, 2, 3);
        assert_eq!(bottom[0].1[0], 1.0);
        assert!(bottom[1].1[0].is_nan());
        assert_eq!(bottom[2].1[0], 2.0);
        assert_eq!(select_k(input, true, 3, 3).len(), 3);
    }

    #[test]
    fn writes_protobuf_json_numbers() {
        let mut out = Vec::new();
        for v in [1.0, 0.5, 1e21, f64::NAN] {
            write_number(&mut out, v);
            out.push(b' ');
        }
        assert_eq!(String::from_utf8(out).unwrap(), "1 0.5 1e21 \"NaN\" ");
    }
}
