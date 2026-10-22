# Log Parquet compatibility fixtures

These synthetic Arrow/Parquet files were generated independently of the NIF by
`native/pulso_codec/examples/log_compatibility_fixtures.rs`, using the project's
pinned Arrow/Parquet 54 dependencies. They test reader compatibility, not SDK
traffic, provider durability or migration acceptance.

- `legacy_log.parquet`: ten historical log columns, no `otlp` column. New readers
  return `otlp: nil` while preserving the historical projected fields.
- `legacy_expanded_log.parquet`: 32 historical ten-column rows with one repeated
  JSON-escaped body larger than 3 MB. Dictionary compression keeps the file below
  3 KiB, but decoding its body column expands beyond 96 MB. Bounded readers must
  charge that expansion even when filters reject every row. This historical
  fixture does not relax the current 16 MiB normalized write ceiling.
- `invalid_otlp_type.parquet`: a present `otlp` column with an integer Arrow type.
  Readers must fail rather than treating it as absent legacy metadata.
- `invalid_otlp_value.parquet`: a UTF-8 `otlp` column containing an array rather
  than an envelope map. Readers must fail rather than discarding the value.

Reproduce from the repository root:

```sh
cargo run --manifest-path native/pulso_codec/Cargo.toml \
  --example log_compatibility_fixtures -- test/fixtures/logs
```

SHA-256:

| Fixture | SHA-256 |
| --- | --- |
| Legacy | `98e5d3689b0f83f38dc2bd1e20f9bfbff586113241baa3db0f8495c7392bfa06` |
| Legacy expanded | `1f8cdad5d5234195dad62e01d7a1dd02c246474a7645f12cd6b8bf918d2f4c1d` |
| Invalid type | `797f38630eb076e418849441f9f10a932ba69c4f04f011921dc401b2fd7e9326` |
| Invalid value | `fe74b7b1d42e704f1e98345d82ee6a2c056e8815bb1638f9cb7d9693b7f0b626` |
