//! Hour finalization and hourly rollups of metric segments.
//!
//! A finalization reads every segment overlapping one UTC hour and rewrites
//! the rows inside the hour as segments grouped by metric name, so the
//! manifest's metric-name summaries let a query skip other metrics' rows.
//! Every row is kept, duplicates and typed metadata included; rows outside the
//! hour (from segments straddling its edges) are written back as ordinary
//! segments before and after it. Alongside, each series' samples of the hour
//! are summarized into one rollup row whose fields compose exactly into the
//! float PromQL range functions the evaluator serves from rollups.
//!
//! Rollup rows carry flags instead of guessing: a series-hour with native
//! histograms, conflicting samples at one timestamp, or a counter reset from
//! a negative zero (where the evaluator's sequence of additions would depend
//! on sample order) is marked so the evaluator reads raw samples instead.
//!
//! Rollup objects deliberately start with a string column, so metric segment
//! readers that predate them reject them rather than misread them.

use crate::metric_segment_parquet::{self as mp, EncodedLabels, Row};
use crate::metric_value::{EXEMPLAR_ONLY_BITS, STALE_BITS};

use arrow::array::{
    Array, BinaryArray, BinaryBuilder, DictionaryArray, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use rustler::{Binary, Env};
use std::collections::HashMap;
use std::sync::Arc;

pub const ROLLUP_KEY: &str = "pulso.metric.rollup";
pub const ROLLUP_VERSION: &str = "1";

pub const FLAG_HISTOGRAM: i32 = 1;
pub const FLAG_CONFLICT: i32 = 2;
pub const FLAG_NEGATIVE_ZERO_RESET: i32 = 4;

const NO_EXTRA: u32 = u32::MAX;
const MAX_SERIES: usize = 4_000_000;
const NEGATIVE_ZERO: u64 = 0x8000_0000_0000_0000;

#[derive(Debug)]
pub enum TierError {
    Invalid,
    TooLarge,
    Encode,
}

impl From<mp::EncodeError> for TierError {
    fn from(_: mp::EncodeError) -> Self {
        TierError::Encode
    }
}

// 32 bytes per stored row; optional metadata lives in `extras`.
#[derive(Clone, Copy)]
struct CompactRow {
    series_id: i64,
    ts: i64,
    value: u64,
    series: u32,
    extra: u32,
}

struct Extra {
    start: Option<i64>,
    otlp: Option<Vec<u8>>,
    histogram: Option<Vec<u8>>,
    exemplars: Option<Vec<u8>>,
}

/// Interned label sets, one per distinct canonical byte sequence.
#[derive(Default)]
pub struct Series {
    labels: Vec<Arc<EncodedLabels>>,
    index: HashMap<Vec<u8>, u32>,
}

impl Series {
    fn intern(&mut self, canonical: &[u8]) -> Result<u32, TierError> {
        if let Some(index) = self.index.get(canonical) {
            return Ok(*index);
        }
        if self.labels.len() >= MAX_SERIES {
            return Err(TierError::TooLarge);
        }
        let labels = EncodedLabels::from_canonical(canonical).ok_or(TierError::Invalid)?;
        let index = self.labels.len() as u32;
        self.labels.push(Arc::new(labels));
        self.index.insert(canonical.to_vec(), index);
        Ok(index)
    }

    pub fn canonical(&self, index: u32) -> &[u8] {
        &self.labels[index as usize].canonical
    }

    // Rank of each series ordered by (metric name, canonical labels).
    fn ranks(&self) -> Vec<u32> {
        let mut order: Vec<u32> = (0..self.labels.len() as u32).collect();
        order.sort_by(|a, b| {
            let (a, b) = (&self.labels[*a as usize], &self.labels[*b as usize]);
            (&a.metric_name, &a.canonical).cmp(&(&b.metric_name, &b.canonical))
        });
        let mut ranks = vec![0u32; order.len()];
        for (rank, index) in order.into_iter().enumerate() {
            ranks[index as usize] = rank as u32;
        }
        ranks
    }

    fn name(&self, index: u32) -> &[u8] {
        &self.labels[index as usize].metric_name
    }
}

/// One encoded output: payload, time bounds, rows and its distinct series.
pub struct Output<'a> {
    pub payload: Binary<'a>,
    pub min_ts: i64,
    pub max_ts: i64,
    pub rows: usize,
    pub series: Vec<u32>,
}

pub struct Finished<'a> {
    pub finals: Vec<Output<'a>>,
    pub before: Option<Output<'a>>,
    pub after: Option<Output<'a>>,
    pub rollups: Vec<Output<'a>>,
}

/// Accumulates every row of the segments overlapping `[start, end)`.
pub struct Builder {
    start: i64,
    end: i64,
    max_rows: usize,
    pub series: Series,
    rows: Vec<CompactRow>,
    extras: Vec<Extra>,
}

impl Builder {
    pub fn new(start: i64, end: i64, max_rows: usize, expected_rows: usize) -> Self {
        Builder {
            start,
            end,
            max_rows,
            series: Series::default(),
            rows: Vec::with_capacity(expected_rows.min(max_rows)),
            extras: Vec::new(),
        }
    }

    /// Append every row of one metric segment; returns the rows read.
    pub fn add(&mut self, bytes: Bytes) -> Result<usize, TierError> {
        let metadata = ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::default())
            .map_err(|_| TierError::Invalid)?;
        let [otlp_column, histogram_column, exemplars_column, start_column] =
            mp::optional_columns(metadata.schema()).map_err(|_| TierError::Invalid)?;
        let read_metadata = dictionary_labels(&metadata, 4)?;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, read_metadata);
        let mut projected = vec![0usize, 1, 2, 4];
        let mut place = |column: Option<usize>| {
            column.map(|column| {
                projected.push(column);
                projected.len() - 1
            })
        };
        let otlp_index = place(otlp_column);
        let histogram_index = place(histogram_column);
        let exemplars_index = place(exemplars_column);
        let start_index = place(start_column);
        let mask = parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), projected);
        let reader = builder
            .with_projection(mask)
            .build()
            .map_err(|_| TierError::Invalid)?;
        let mut added = 0usize;
        for batch in reader {
            let batch = batch.map_err(|_| TierError::Invalid)?;
            let series_ids = column::<Int64Array>(&batch, 0)?;
            let timestamps = column::<Int64Array>(&batch, 1)?;
            let values = column::<Float64Array>(&batch, 2)?;
            let labels = column::<DictionaryArray<Int32Type>>(&batch, 3)?;
            let dictionary = labels
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or(TierError::Invalid)?;
            let otlp = otlp_index
                .map(|i| column::<StringArray>(&batch, i))
                .transpose()?;
            let histograms = histogram_index
                .map(|i| column::<BinaryArray>(&batch, i))
                .transpose()?;
            let exemplars = exemplars_index
                .map(|i| column::<BinaryArray>(&batch, i))
                .transpose()?;
            let starts = start_index
                .map(|i| column::<Int64Array>(&batch, i))
                .transpose()?;
            let mut interned = vec![u32::MAX; dictionary.len()];
            for row in 0..batch.num_rows() {
                let key = labels.key(row).ok_or(TierError::Invalid)?;
                let series = match interned.get(key).copied() {
                    Some(u32::MAX) => {
                        let index = self.series.intern(dictionary.value(key))?;
                        interned[key] = index;
                        index
                    }
                    Some(index) => index,
                    None => return Err(TierError::Invalid),
                };
                if self.rows.len() >= self.max_rows {
                    return Err(TierError::TooLarge);
                }
                let extra = Extra {
                    start: starts.filter(|a| !a.is_null(row)).map(|a| a.value(row)),
                    otlp: otlp
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row).as_bytes().to_vec()),
                    histogram: histograms
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row).to_vec()),
                    exemplars: exemplars
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row).to_vec()),
                };
                if extra.start == Some(0) {
                    return Err(TierError::Invalid);
                }
                let extra = if extra.start.is_none()
                    && extra.otlp.is_none()
                    && extra.histogram.is_none()
                    && extra.exemplars.is_none()
                {
                    NO_EXTRA
                } else {
                    if self.extras.len() >= u32::MAX as usize - 1 {
                        return Err(TierError::TooLarge);
                    }
                    self.extras.push(extra);
                    (self.extras.len() - 1) as u32
                };
                self.rows.push(CompactRow {
                    series_id: series_ids.value(row),
                    ts: timestamps.value(row),
                    value: values.value(row).to_bits(),
                    series,
                    extra,
                });
                added += 1;
            }
        }
        Ok(added)
    }

    /// Encode the hour's rows grouped by metric, the rows outside the hour,
    /// and the hour's rollup rows.
    pub fn finish<'a>(
        mut self,
        env: Env<'a>,
        target_rows: usize,
        rollup_target_rows: usize,
        max_names: usize,
    ) -> Result<(Finished<'a>, Series), TierError> {
        let ranks = self.series.ranks();
        let (start, end) = (self.start, self.end);
        let class = |ts: i64| -> u8 {
            if ts < start {
                1
            } else if ts >= end {
                2
            } else {
                0
            }
        };
        self.rows.sort_unstable_by_key(|r| {
            (
                class(r.ts),
                ranks[r.series as usize],
                r.ts,
                r.series_id,
                r.value,
                r.extra,
            )
        });
        let inside = self.rows.partition_point(|r| class(r.ts) == 0);
        let before = inside + self.rows[inside..].partition_point(|r| class(r.ts) == 1);
        let mut finals = Vec::new();
        for range in groups(
            &self.rows[..inside],
            &self.series,
            |r| r.series,
            target_rows,
            max_names,
        ) {
            finals.push(self.encode_range(env, range)?);
        }
        let earlier = (before > inside)
            .then(|| self.encode_range(env, inside..before))
            .transpose()?;
        let later = (self.rows.len() > before)
            .then(|| self.encode_range(env, before..self.rows.len()))
            .transpose()?;
        let aggregates = self.rollup_rows(&self.rows[..inside]);
        drop(std::mem::take(&mut self.rows));
        let mut rollups = Vec::new();
        for range in groups(
            &aggregates,
            &self.series,
            |a| a.series,
            rollup_target_rows,
            max_names,
        ) {
            rollups.push(encode_rollups(env, &aggregates[range], &self.series)?);
        }
        let finished = Finished {
            finals,
            before: earlier,
            after: later,
            rollups,
        };
        Ok((finished, self.series))
    }

    fn encode_range<'a>(
        &self,
        env: Env<'a>,
        range: std::ops::Range<usize>,
    ) -> Result<Output<'a>, TierError> {
        let rows = &self.rows[range];
        let mut series = Vec::new();
        let mut encoded = Vec::with_capacity(rows.len());
        for row in rows {
            if series.last() != Some(&row.series) {
                series.push(row.series);
            }
            let extra = (row.extra != NO_EXTRA).then(|| &self.extras[row.extra as usize]);
            encoded.push(Row {
                series_id: row.series_id,
                timestamp_ns: row.ts,
                start_timestamp_ms: extra.and_then(|e| e.start),
                value: f64::from_bits(row.value),
                labels: Arc::clone(&self.series.labels[row.series as usize]),
                otlp: extra.and_then(|e| e.otlp.clone()),
                histogram: extra.and_then(|e| e.histogram.clone()),
                exemplars: extra.and_then(|e| e.exemplars.clone()),
            });
        }
        series.sort_unstable();
        series.dedup();
        let min_ts = rows.iter().map(|r| r.ts).min().unwrap_or(0);
        let max_ts = rows.iter().map(|r| r.ts).max().unwrap_or(0);
        let count = encoded.len();
        let payload = mp::write_rows(env, encoded, true)?;
        Ok(Output {
            payload,
            min_ts,
            max_ts,
            rows: count,
            series,
        })
    }

    // One aggregate per series with samples in the hour, in rank order.
    fn rollup_rows(&self, rows: &[CompactRow]) -> Vec<Aggregate> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < rows.len() {
            let series = rows[i].series;
            let mut aggregate = Aggregate::new(series, rows[i].series_id, self.start);
            let mut j = i;
            while j < rows.len() && rows[j].series == series {
                let ts = rows[j].ts;
                let mut k = j;
                let mut chosen: Option<u64> = None;
                let mut classes = 0usize;
                let mut first_class: Option<u64> = None;
                while k < rows.len() && rows[k].series == series && rows[k].ts == ts {
                    let row = rows[k];
                    k += 1;
                    let extra = (row.extra != NO_EXTRA).then(|| &self.extras[row.extra as usize]);
                    if extra.is_some_and(|e| e.histogram.is_some()) {
                        aggregate.flags |= FLAG_HISTOGRAM;
                        continue;
                    }
                    // Exemplar-only rows hold no sample (queries skip them).
                    if extra.is_some_and(|e| e.exemplars.is_some())
                        && row.value == EXEMPLAR_ONLY_BITS
                    {
                        continue;
                    }
                    let class = value_class(row.value);
                    if first_class != Some(class) {
                        if first_class.is_none() {
                            first_class = Some(class);
                            classes = 1;
                            chosen = Some(row.value);
                        } else {
                            classes += 1;
                        }
                    }
                }
                if classes > 1 {
                    aggregate.flags |= FLAG_CONFLICT;
                }
                if let Some(bits) = chosen {
                    if bits != STALE_BITS {
                        aggregate.push(ts, f64::from_bits(bits), bits);
                    }
                }
                j = k;
            }
            if aggregate.count > 0 || aggregate.flags != 0 {
                out.push(aggregate);
            }
            i = j;
        }
        out
    }
}

// Rows with these bits are the same Erlang sample value: every NaN other than
// the stale marker decodes to `:nan`.
fn value_class(bits: u64) -> u64 {
    let value = f64::from_bits(bits);
    if bits != STALE_BITS && value.is_nan() {
        f64::NAN.to_bits()
    } else {
        bits
    }
}

/// The summary of one series' float samples in one bucket.
#[derive(Clone)]
pub struct Aggregate {
    pub series: u32,
    pub series_id: i64,
    pub bucket: i64,
    pub count: i64,
    pub first_ts: i64,
    pub first: f64,
    pub last_ts: i64,
    pub last: f64,
    pub min: f64,
    pub max: f64,
    pub changes: i64,
    pub resets: i64,
    pub reset_values: Vec<f64>,
    pub flags: i32,
    last_bits: u64,
}

impl Aggregate {
    fn new(series: u32, series_id: i64, bucket: i64) -> Self {
        Aggregate {
            series,
            series_id,
            bucket,
            count: 0,
            first_ts: 0,
            first: 0.0,
            last_ts: 0,
            last: 0.0,
            min: f64::NAN,
            max: f64::NAN,
            changes: 0,
            resets: 0,
            reset_values: Vec::new(),
            flags: 0,
            last_bits: 0,
        }
    }

    // Mirrors the evaluator: Number.min/max ignore NaN; min keeps the earliest
    // of equal values and max the latest; changes treat two NaNs as equal; resets compare with
    // IEEE `<`.
    fn push(&mut self, ts: i64, value: f64, bits: u64) {
        if self.count == 0 {
            self.first_ts = ts;
            self.first = value;
        } else {
            let previous = self.last;
            if !(previous == value || (previous.is_nan() && value.is_nan())) {
                self.changes += 1;
            }
            if value < previous {
                self.resets += 1;
                self.reset_values.push(previous);
                if self.last_bits == NEGATIVE_ZERO {
                    self.flags |= FLAG_NEGATIVE_ZERO_RESET;
                }
            }
        }
        if !value.is_nan() && (self.min.is_nan() || value < self.min) {
            self.min = value;
        }
        // Number.max keeps the newest of equal values (signed zeros differ).
        if !value.is_nan() && (self.max.is_nan() || value >= self.max) {
            self.max = value;
        }
        self.count += 1;
        self.last_ts = ts;
        self.last = value;
        self.last_bits = bits;
    }
}

/// Ranges of `items` (sorted by series rank) for outputs of at most
/// `target_rows` rows and `max_names` metric names. A metric is kept in one
/// output when it fits; a larger metric is split between series.
fn groups<T>(
    items: &[T],
    series: &Series,
    series_of: impl Fn(&T) -> u32,
    target_rows: usize,
    max_names: usize,
) -> Vec<std::ops::Range<usize>> {
    let target = target_rows.max(1);
    let mut out = Vec::new();
    let mut group_start = 0;
    let mut group_rows = 0;
    let mut group_names = 0;
    let mut i = 0;
    while i < items.len() {
        // One metric: [i, metric_end).
        let name = series.name(series_of(&items[i]));
        let mut metric_end = i;
        while metric_end < items.len() && series.name(series_of(&items[metric_end])) == name {
            metric_end += 1;
        }
        if group_rows > 0 && (group_rows + (metric_end - i) > target || group_names + 1 > max_names)
        {
            out.push(group_start..i);
            group_start = i;
            group_rows = 0;
            group_names = 0;
        }
        group_names += 1;
        let mut j = i;
        while j < metric_end {
            let id = series_of(&items[j]);
            let mut series_end = j;
            while series_end < metric_end && series_of(&items[series_end]) == id {
                series_end += 1;
            }
            // Split a large metric between series before a group overflows.
            if group_rows > 0 && group_rows + (series_end - j) > target && j > group_start {
                out.push(group_start..j);
                group_start = j;
                group_rows = 0;
                group_names = 1;
            }
            group_rows += series_end - j;
            j = series_end;
        }
        i = metric_end;
    }
    if group_start < items.len() {
        out.push(group_start..items.len());
    }
    out
}

fn column<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T, TierError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or(TierError::Invalid)
}

// Read the label column at `index` as a dictionary, sharing its values.
fn dictionary_labels(
    metadata: &ArrowReaderMetadata,
    index: usize,
) -> Result<ArrowReaderMetadata, TierError> {
    let mut fields = metadata.schema().fields().to_vec();
    let field = fields.get(index).ok_or(TierError::Invalid)?;
    if field.data_type() != &DataType::Binary {
        return Err(TierError::Invalid);
    }
    fields[index] = Arc::new(field.as_ref().clone().with_data_type(DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Binary),
    )));
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        metadata.schema().metadata().clone(),
    ));
    ArrowReaderMetadata::try_new(
        Arc::clone(metadata.metadata()),
        ArrowReaderOptions::new().with_schema(schema),
    )
    .map_err(|_| TierError::Invalid)
}

// ---- rollup objects --------------------------------------------------------

fn rollup_schema() -> Arc<Schema> {
    let fields = vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("labels_canonical", DataType::Binary, false),
        Field::new("series_id", DataType::Int64, false),
        Field::new("bucket_start", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
        Field::new("first_ts", DataType::Int64, false),
        Field::new("first_value", DataType::Float64, false),
        Field::new("last_ts", DataType::Int64, false),
        Field::new("last_value", DataType::Float64, false),
        Field::new("min_value", DataType::Float64, false),
        Field::new("max_value", DataType::Float64, false),
        Field::new("changes", DataType::Int64, false),
        Field::new("resets", DataType::Int64, false),
        Field::new("reset_values", DataType::Binary, true),
        Field::new("flags", DataType::Int32, false),
    ];
    let mut metadata = HashMap::new();
    metadata.insert(ROLLUP_KEY.to_owned(), ROLLUP_VERSION.to_owned());
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

pub fn encode_rollups<'a>(
    env: Env<'a>,
    aggregates: &[Aggregate],
    series: &Series,
) -> Result<Output<'a>, TierError> {
    let n = aggregates.len();
    let mut names = StringBuilder::with_capacity(n, n * 16);
    let mut canonical = BinaryBuilder::with_capacity(n, n * 64);
    let mut resets = BinaryBuilder::with_capacity(n, 0);
    let mut distinct = Vec::new();
    for a in aggregates {
        let labels = &series.labels[a.series as usize];
        names.append_value(String::from_utf8_lossy(&labels.metric_name));
        canonical.append_value(&labels.canonical);
        if a.reset_values.is_empty() {
            resets.append_null();
        } else {
            let mut bytes = Vec::with_capacity(a.reset_values.len() * 8);
            for value in &a.reset_values {
                bytes.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            resets.append_value(bytes);
        }
        if distinct.last() != Some(&a.series) {
            distinct.push(a.series);
        }
    }
    distinct.sort_unstable();
    distinct.dedup();
    let i64s = |f: fn(&Aggregate) -> i64| -> Arc<dyn Array> {
        Arc::new(Int64Array::from_iter_values(aggregates.iter().map(f)))
    };
    let f64s = |f: fn(&Aggregate) -> f64| -> Arc<dyn Array> {
        Arc::new(Float64Array::from_iter_values(aggregates.iter().map(f)))
    };
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(names.finish()),
        Arc::new(canonical.finish()),
        i64s(|a| a.series_id),
        i64s(|a| a.bucket),
        i64s(|a| a.count),
        i64s(|a| a.first_ts),
        f64s(|a| a.first),
        i64s(|a| a.last_ts),
        f64s(|a| a.last),
        f64s(|a| a.min),
        f64s(|a| a.max),
        i64s(|a| a.changes),
        i64s(|a| a.resets),
        Arc::new(resets.finish()),
        Arc::new(Int32Array::from_iter_values(
            aggregates.iter().map(|a| a.flags),
        )),
    ];
    let batch = RecordBatch::try_new(rollup_schema(), columns).map_err(|_| TierError::Encode)?;
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(6).unwrap()))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_offset_index_disabled(true)
        .set_statistics_truncate_length(Some(64))
        .set_max_row_group_size(65_536)
        .set_column_dictionary_enabled(ColumnPath::from("metric_name"), true)
        .set_column_dictionary_enabled(ColumnPath::from("labels_canonical"), true)
        .build();
    let mut buf = crate::out::BinaryWriter(crate::out::BinSink::with_capacity(
        256 + aggregates.len() * 48,
    ));
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
        .map_err(|_| TierError::Encode)?;
    writer.write(&batch).map_err(|_| TierError::Encode)?;
    writer.close().map_err(|_| TierError::Encode)?;
    let min_ts = aggregates.iter().map(|a| a.bucket).min().unwrap_or(0);
    let max_ts = aggregates.iter().map(|a| a.bucket).max().unwrap_or(0);
    Ok(Output {
        payload: buf.0.finish(env),
        min_ts,
        max_ts,
        rows: n,
        series: distinct,
    })
}

/// Rollup rows of one object, read into `series`.
fn read_rollups(
    bytes: Bytes,
    series: &mut Series,
    mut keep: impl FnMut(&[u8], i64) -> bool,
) -> Result<Vec<Aggregate>, TierError> {
    let metadata = ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::default())
        .map_err(|_| TierError::Invalid)?;
    let schema = metadata.schema();
    let expected = rollup_schema();
    if schema.metadata().get(ROLLUP_KEY).map(String::as_str) != Some(ROLLUP_VERSION)
        || schema.fields().len() != expected.fields().len()
        || schema
            .fields()
            .iter()
            .zip(expected.fields().iter())
            .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
    {
        return Err(TierError::Invalid);
    }
    let read_metadata = dictionary_labels(&metadata, 1)?;
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, read_metadata)
        .build()
        .map_err(|_| TierError::Invalid)?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|_| TierError::Invalid)?;
        let labels = column::<DictionaryArray<Int32Type>>(&batch, 1)?;
        let dictionary = labels
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or(TierError::Invalid)?;
        let int = |i| column::<Int64Array>(&batch, i);
        let float = |i| column::<Float64Array>(&batch, i);
        let (series_id, bucket, count, first_ts) = (int(2)?, int(3)?, int(4)?, int(5)?);
        let (first, last_ts, last, min, max) =
            (float(6)?, int(7)?, float(8)?, float(9)?, float(10)?);
        let (changes, resets) = (int(11)?, int(12)?);
        let reset_values = column::<BinaryArray>(&batch, 13)?;
        let flags = column::<Int32Array>(&batch, 14)?;
        // Per dictionary entry: series index, or u32::MAX - 1 when filtered.
        let mut interned = vec![u32::MAX; dictionary.len()];
        for row in 0..batch.num_rows() {
            let key = labels.key(row).ok_or(TierError::Invalid)?;
            let canonical = dictionary.value(key);
            if !keep(canonical, bucket.value(row)) {
                continue;
            }
            let index = match interned[key] {
                u32::MAX => {
                    let index = series.intern(canonical)?;
                    interned[key] = index;
                    index
                }
                index => index,
            };
            let values = if reset_values.is_null(row) {
                Vec::new()
            } else {
                let bytes = reset_values.value(row);
                if bytes.len() % 8 != 0 {
                    return Err(TierError::Invalid);
                }
                bytes
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|c| f64::from_bits(u64::from_le_bytes(*c)))
                    .collect()
            };
            if count.value(row) < 0 || values.len() as i64 != resets.value(row) {
                return Err(TierError::Invalid);
            }
            out.push(Aggregate {
                series: index,
                series_id: series_id.value(row),
                bucket: bucket.value(row),
                count: count.value(row),
                first_ts: first_ts.value(row),
                first: first.value(row),
                last_ts: last_ts.value(row),
                last: last.value(row),
                min: min.value(row),
                max: max.value(row),
                changes: changes.value(row),
                resets: resets.value(row),
                reset_values: values,
                flags: flags.value(row),
                last_bits: 0,
            });
        }
    }
    Ok(out)
}

/// Rollup rows of `bytes` with buckets in `[start, end]` whose labels match.
pub fn decode_rollups(
    bytes: Bytes,
    start: Option<i128>,
    end: Option<i128>,
    matchers: &[mp::Matcher<'_>],
) -> Result<(Series, Vec<Aggregate>), mp::DecodeError> {
    let regexes = mp::compile_regexes(matchers)?;
    let mut series = Series::default();
    let mut previous: Option<(Vec<u8>, bool)> = None;
    let rows = read_rollups(bytes, &mut series, |canonical, bucket| {
        let bucket = bucket as i128;
        if start.is_some_and(|s| bucket < s) || end.is_some_and(|e| bucket > e) {
            return false;
        }
        match &previous {
            Some((labels, matched)) if labels.as_slice() == canonical => *matched,
            _ => {
                let mut pairs = Vec::new();
                let matched = mp::parse_canonical_pairs(canonical, &mut pairs).is_ok()
                    && mp::labels_match(&pairs, matchers, &regexes);
                previous = Some((canonical.to_vec(), matched));
                matched
            }
        }
    })
    .map_err(|_| mp::DecodeError::Reader)?;
    Ok((series, rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(name: &str, series: &str) -> Vec<u8> {
        let mut out = Vec::new();
        for (n, v) in [("__name__", name), ("s", series)] {
            out.extend_from_slice(n.as_bytes());
            out.push(0xff);
            out.extend_from_slice(v.as_bytes());
            out.push(0xff);
        }
        out
    }

    #[test]
    fn groups_keep_metrics_together_and_split_large_ones() {
        let mut series = Series::default();
        let a1 = series.intern(&labels("a", "1")).unwrap();
        let b1 = series.intern(&labels("b", "1")).unwrap();
        let b2 = series.intern(&labels("b", "2")).unwrap();
        let c1 = series.intern(&labels("c", "1")).unwrap();
        // a: 2 rows, b: 3 + 3 rows, c: 1 row.
        let items = [a1, a1, b1, b1, b1, b2, b2, b2, c1];
        let ranges = groups(&items, &series, |s| *s, 4, 128);
        // b splits between its series; c fits beside the tail of b.
        assert_eq!(ranges, vec![0..2, 2..5, 5..9]);
        let ranges = groups(&items, &series, |s| *s, 100, 2);
        assert_eq!(ranges, vec![0..8, 8..9]);
    }

    #[test]
    fn aggregates_follow_the_evaluator_rules() {
        let mut a = Aggregate::new(0, 0, 0);
        for (ts, v) in [
            (1, 5.0),
            (2, 7.0),
            (3, 2.0),
            (4, 2.0),
            (5, f64::NAN),
            (6, 1.0),
        ] {
            a.push(ts, v, f64::to_bits(v));
        }
        assert_eq!(a.count, 6);
        assert_eq!((a.first_ts, a.first, a.last_ts, a.last), (1, 5.0, 6, 1.0));
        assert_eq!((a.min, a.max), (1.0, 7.0));
        // 5->7, 7->2, 2->NaN, NaN->1 change; 2->2 does not.
        assert_eq!(a.changes, 4);
        // 7->2 resets; comparisons with NaN never do.
        assert_eq!((a.resets, a.reset_values.clone()), (1, vec![7.0]));
        assert_eq!(a.flags, 0);
        let mut z = Aggregate::new(0, 0, 0);
        z.push(1, -0.0, (-0.0f64).to_bits());
        z.push(2, -1.0, (-1.0f64).to_bits());
        assert_eq!(z.flags, FLAG_NEGATIVE_ZERO_RESET);
        let mut m = Aggregate::new(0, 0, 0);
        m.push(1, 0.0, 0f64.to_bits());
        m.push(2, -0.0, (-0.0f64).to_bits());
        assert_eq!(
            (m.min.to_bits(), m.max.to_bits()),
            (0f64.to_bits(), (-0.0f64).to_bits())
        );
    }
}
