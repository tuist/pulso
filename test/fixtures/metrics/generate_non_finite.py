"""Regenerate the legacy metric segment fixture with `uv run --with pyarrow python generate_non_finite.py`.

This is a development-only fixture generator. The application and tests do not
need PyArrow. The second value uses Prometheus's stale-marker bit pattern.
"""

from pathlib import Path
import struct

import pyarrow as arrow
import pyarrow.parquet as parquet


stale = struct.unpack("<d", struct.pack("<Q", 0x7FF0000000000002))[0]
schema = arrow.schema(
    [
        arrow.field("series_id", arrow.int64(), nullable=False),
        arrow.field("timestamp_ns", arrow.int64(), nullable=False),
        arrow.field("value", arrow.float64(), nullable=False),
        arrow.field("metric_name", arrow.string(), nullable=False),
        arrow.field("labels_canonical", arrow.binary(), nullable=False),
        arrow.field("labels_json", arrow.string(), nullable=False),
    ]
)
table = arrow.Table.from_arrays(
    [
        arrow.array([1, 2], type=arrow.int64()),
        arrow.array([1, 2], type=arrow.int64()),
        arrow.array([1.0, stale], type=arrow.float64()),
        arrow.array(["safe", "unsafe"]),
        arrow.array([b"__name__\xffsafe\xff", b"__name__\xffunsafe\xff"]),
        arrow.array(['{"__name__":"safe"}', '{"__name__":"unsafe"}']),
    ],
    schema=schema,
)
parquet.write_table(table, Path(__file__).with_name("non_finite.parquet"), compression="zstd")
