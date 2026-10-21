defmodule PulsoWeb.RemoteWriteV2Test do
  # Remote Write 2.0 (`io.prometheus.write.v2.Request`), encoded by hand as
  # Prometheus and Alloy send it: labels and metadata are references into a
  # request-wide symbol table.
  use PulsoWeb.ConnCase, async: true

  import Bitwise

  alias Pulso.Record.MetricSample
  alias Pulso.Storage

  @v2 "application/x-protobuf;proto=io.prometheus.write.v2.Request"

  defp varint(v) when v < 128, do: <<v>>
  defp varint(v), do: <<(v &&& 0x7F) ||| 0x80>> <> varint(v >>> 7)
  defp field(n, bytes), do: varint(n <<< 3 ||| 2) <> varint(byte_size(bytes)) <> bytes
  defp varint_field(n, v), do: varint(n <<< 3) <> varint(v)

  defp sample(value, ts_ms), do: field(2, <<0x09, value::little-float-64>> <> varint_field(2, ts_ms))

  # `symbols` is the table; `series` entries name symbols, not indexes.
  defp request(symbols, series) do
    index = symbols |> Enum.with_index() |> Map.new()
    ref = &Map.fetch!(index, &1)

    timeseries =
      Enum.map_join(series, fn s ->
        refs = s.labels |> Enum.flat_map(fn {n, v} -> [ref.(n), ref.(v)] end) |> Enum.map_join(&varint/1)
        samples = Enum.map_join(s[:samples] || [], fn {v, ts} -> sample(v, ts) end)
        # A float histogram whose span claims a bucket it never sends.
        invalid = <<0x11, 1.0::little-float-64>> <> field(11, <<0x08, 0x00, 0x10, 0x01>>)
        histograms = String.duplicate(field(3, invalid), s[:invalid_histograms] || 0)

        metadata =
          case s[:metadata] do
            nil ->
              ""

            {type, help, unit} ->
              field(5, varint_field(1, type) <> varint_field(3, ref.(help)) <> varint_field(4, ref.(unit)))
          end

        field(5, field(1, refs) <> samples <> histograms <> metadata)
      end)

    timeseries <> Enum.map_join(symbols, &field(4, &1))
  end

  defp post_v2(tenant, body, content_type \\ @v2) do
    {:ok, compressed} = :snappyer.compress(body)

    build_conn()
    |> put_req_header("x-scope-orgid", tenant)
    |> put_req_header("content-type", content_type)
    |> put_req_header("content-encoding", "snappy")
    |> put_req_header("x-prometheus-remote-write-version", "2.0.0")
    |> post("/api/v1/write", compressed)
  end

  defp written(conn) do
    for kind <- ~w(samples histograms exemplars) do
      conn |> get_resp_header("x-prometheus-remote-write-#{kind}-written") |> List.first()
    end
  end

  defp tenant, do: "rw2-#{System.unique_integer([:positive])}"

  @symbols [
    "",
    "__name__",
    "http_requests_total",
    "job",
    "api",
    "Requests served.",
    "latency_seconds_bucket",
    "le",
    "0.5",
    "Latency.",
    "seconds"
  ]

  test "stores samples, reports written counts and retains per-series metadata" do
    tenant = tenant()

    body =
      request(@symbols, [
        %{
          labels: [{"__name__", "http_requests_total"}, {"job", "api"}],
          samples: [{1.0, 1_700_000_000_000}, {2.0, 1_700_000_015_000}],
          metadata: {1, "Requests served.", ""}
        },
        %{
          labels: [{"__name__", "latency_seconds_bucket"}, {"le", "0.5"}],
          samples: [{3.0, 1_700_000_000_000}],
          metadata: {3, "Latency.", "seconds"}
        }
      ])

    conn = post_v2(tenant, body)
    assert response(conn, 204) == ""
    assert written(conn) == ["3", "0", "0"]

    assert {:ok, stored} = Storage.query(:metrics, tenant)

    assert stored |> Enum.map(&{&1.labels["__name__"], &1.value}) |> Enum.sort() == [
             {"http_requests_total", 1.0},
             {"http_requests_total", 2.0},
             {"latency_seconds_bucket", 3.0}
           ]

    assert [%MetricSample{labels: %{"job" => "api"}} | _] = Enum.sort_by(stored, & &1.labels["__name__"])

    assert build_conn() |> put_req_header("x-scope-orgid", tenant) |> get("/api/v1/metadata") |> json_response(200) ==
             %{
               "status" => "success",
               "data" => %{
                 "http_requests_total" => [%{"type" => "counter", "help" => "Requests served.", "unit" => ""}],
                 "latency_seconds" => [%{"type" => "histogram", "help" => "Latency.", "unit" => "seconds"}]
               }
             }
  end

  test "stores valid series of a partly invalid request and answers 400 with the written count" do
    tenant = tenant()

    body =
      request(@symbols, [
        %{labels: [{"__name__", "http_requests_total"}], samples: [{1.0, 1_700_000_000_000}], invalid_histograms: 1},
        %{labels: [{"__name__", "latency_seconds_bucket"}], invalid_histograms: 2},
        %{labels: [{"job", "api"}, {"job", "api"}], samples: [{1.0, 1_700_000_000_000}]}
      ])

    conn = post_v2(tenant, body)
    assert json_response(conn, 400) == %{"error" => "rejected_records", "rejected" => 4}
    assert written(conn) == ["1", "0", "0"]
    assert get_resp_header(conn, "x-pulso-rejected-records") == ["4"]
    assert {:ok, [%MetricSample{value: 1.0}]} = Storage.query(:metrics, tenant)
  end

  test "selects the message by the Content-Type proto parameter" do
    tenant = tenant()
    body = request(@symbols, [%{labels: [{"__name__", "http_requests_total"}], samples: [{1.0, 1_700_000_000_000}]}])

    for content_type <- [
          "application/x-protobuf; proto=\"io.prometheus.write.v2.Request\"",
          "Application/X-Protobuf;PROTO=io.prometheus.write.v2.Request"
        ] do
      assert tenant |> post_v2(body, content_type) |> response(204)
    end

    assert {:ok, [_, _]} = Storage.query(:metrics, tenant)

    assert tenant |> post_v2(body, "application/x-protobuf;proto=io.prometheus.write.v3.Request") |> response(415)
    assert tenant |> post_v2(body, "text/plain") |> response(415)

    # Read as 1.0, a 2.0 body has no series: nothing is stored and no
    # written counts are claimed.
    conn = post_v2(tenant, body, "application/x-protobuf;proto=prometheus.WriteRequest")
    assert response(conn, 204)
    assert written(conn) == [nil, nil, nil]
    assert {:ok, [_, _]} = Storage.query(:metrics, tenant)
  end

  test "malformed requests store nothing" do
    tenant = tenant()
    assert tenant |> post_v2(<<0x2A, 0x10, 0x01>>) |> json_response(400) == %{"error" => "invalid_protobuf"}
    assert {:ok, []} = Storage.query(:metrics, tenant)
  end
end
