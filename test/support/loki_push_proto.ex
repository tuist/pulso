defmodule Pulso.Loki.PushProto do
  @moduledoc """
  Test-only encoder for Loki `POST /loki/api/v1/push` protobuf fixtures.

  Production decoding lives in Rust (`native/pulso_ingest`). Building
  fixtures with an independent protobuf implementation means the tests
  check the Rust decoder against a second reading of the schema rather
  than against itself.

  Field numbers and wire types mirror the upstream Loki definition
  (`pkg/push/push.proto` in grafana/loki); only field tags matter on the
  wire, so the message names are our own. `Timestamp` is inlined because
  its encoding is identical to `google.protobuf.Timestamp`.
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
