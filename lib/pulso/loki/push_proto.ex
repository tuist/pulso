defmodule Pulso.Loki.PushProto do
  @moduledoc """
  Protobuf schema for the Loki `POST /loki/api/v1/push` wire format.

  Field numbers and wire types mirror the upstream Loki definition
  (`pkg/push/push.proto` in grafana/loki). Message names are our own —
  the wire format only cares about field tags, so `Stream` / `Entry` /
  `LabelPair` here decode bytes produced by Loki's `StreamAdapter` /
  `EntryAdapter` / `LabelPairAdapter` unchanged.

  We inline `Timestamp` rather than importing `google/protobuf/timestamp.proto`
  because the encoding is identical (int64 `seconds` = 1, int32 `nanos` = 2)
  and skipping the well-known-types import keeps the compile-time
  dependency surface small.

  `Entry.parsed` (field 4 in Loki's schema) is deliberately omitted:
  it carries labels the ingester pipeline extracts, which Pulso does
  not persist. Unknown fields on the wire are silently skipped by the
  decoder, so ignoring `parsed` costs nothing.
  """

  use Protox,
    schema: """
    syntax = "proto3";

    message PushRequest {
      repeated Stream streams = 1;
    }

    message Stream {
      string labels = 1;
      repeated Entry entries = 2;
      string hash = 3;
    }

    message Entry {
      Timestamp timestamp = 1;
      string line = 2;
      repeated LabelPair structured_metadata = 3;
    }

    message Timestamp {
      int64 seconds = 1;
      int32 nanos = 2;
    }

    message LabelPair {
      string name = 1;
      string value = 2;
    }
    """,
    namespace: Pulso.Loki.PushProto
end
