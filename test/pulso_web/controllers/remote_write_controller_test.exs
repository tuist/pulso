defmodule PulsoWeb.RemoteWriteControllerTest do
  # Controller tests for Prometheus remote_write v1. The protobuf
  # payload is encoded by hand here (no `prost` on either side) so the
  # fixture stays legible and the on-wire bytes we assert against are
  # exactly what Alloy / Grafana Agent / Prometheus will send.
  use PulsoWeb.ConnCase, async: false

  import Bitwise

  alias Pulso.Record.MetricSample
  alias Pulso.Storage
  alias Pulso.Storage.Memory

  setup do
    Memory.reset()
    :ok
  end

  # ------- protobuf encoder (minimal) --------------------------------------

  defp varint(v) when is_integer(v) and v >= 0 do
    if v < 128 do
      <<v>>
    else
      <<(v &&& 0x7F) ||| 0x80>> <> varint(v >>> 7)
    end
  end

  defp tag(field, wire), do: varint(field <<< 3 ||| wire)
  defp length_delim(field, bytes), do: tag(field, 2) <> varint(byte_size(bytes)) <> bytes
  defp varint_field(field, v), do: tag(field, 0) <> varint(v)
  defp fixed64_field(field, bytes), do: tag(field, 1) <> bytes

  defp encode_label(name, value) do
    length_delim(1, name) <> length_delim(2, value)
  end

  defp encode_sample({:bits, bits}, ts_ms) do
    fixed64_field(1, <<bits::little-unsigned-64>>) <> varint_field(2, ts_ms)
  end

  defp encode_sample(value, ts_ms) do
    fixed64_field(1, <<value::little-float-64>>) <> varint_field(2, ts_ms)
  end

  defp encode_series(labels, samples) do
    labels_bytes = Enum.map_join(labels, "", fn {n, v} -> length_delim(1, encode_label(n, v)) end)
    samples_bytes = Enum.map_join(samples, "", fn {v, ts} -> length_delim(2, encode_sample(v, ts)) end)
    labels_bytes <> samples_bytes
  end

  defp encode_write_request(series_list) do
    Enum.map_join(series_list, "", fn series -> length_delim(1, encode_series(series.labels, series.samples)) end)
  end

  defp snappy(bytes) do
    {:ok, compressed} = :snappyer.compress(bytes)
    compressed
  end

  defp body(series_list) do
    series_list |> encode_write_request() |> snappy()
  end

  defp post_write(conn, body, headers \\ %{}) do
    defaults = %{
      "content-type" => "application/x-protobuf",
      "content-encoding" => "snappy",
      "x-prometheus-remote-write-version" => "0.1.0"
    }

    merged = Map.merge(defaults, headers)

    merged
    |> Enum.reduce(conn, fn {h, v}, c -> put_req_header(c, h, v) end)
    |> post(~p"/api/v1/write", body)
  end

  # ------- happy path -------------------------------------------------------

  test "POST /api/v1/write returns 204 and stores metric samples", %{conn: conn} do
    series = [
      %{
        labels: [{"__name__", "up"}, {"instance", "node-1"}, {"job", "pulso"}],
        samples: [{1.0, 1_700_000_000_000}, {1.0, 1_700_000_015_000}]
      }
    ]

    conn =
      conn
      |> put_req_header("x-scope-orgid", "acme")
      |> post_write(body(series))

    assert conn.status == 204
    assert conn.resp_body == ""

    {:ok, samples} = Storage.query(:metrics, "acme")
    assert length(samples) == 2
    assert Enum.all?(samples, fn s -> s.labels["__name__"] == "up" end)
    # Prometheus ships timestamps in milliseconds; the storage layer
    # keeps them in nanoseconds so the controller rescales.
    assert Enum.map(samples, & &1.timestamp_ns) |> Enum.sort() ==
             [1_700_000_000_000_000_000, 1_700_000_015_000_000_000]
  end

  test "stale and non-finite samples are rejected without losing finite samples", %{conn: conn} do
    unsupported = [
      {:stale, 0x7FF0000000000002},
      {:other_nan, 0x7FF8000000000001},
      {:positive_infinity, 0x7FF0000000000000},
      {:negative_infinity, 0xFFF0000000000000}
    ]

    for {_name, bits} <- unsupported do
      Memory.reset()

      request =
        body([
          %{labels: [{"__name__", "safe"}], samples: [{1.0, 1}, {{:bits, bits}, 2}, {2.0, 3}]},
          %{labels: [{"__name__", "only_unsupported"}], samples: [{{:bits, bits}, 4}]}
        ])

      response = post_write(conn, request)
      assert response.status == 204
      assert get_resp_header(response, "x-pulso-rejected-records") == ["2"]
      assert {:ok, stored} = Storage.query(:metrics, "default")

      assert Enum.sort_by(stored, & &1.timestamp_ns) |> Enum.map(&{&1.timestamp_ns, &1.value}) ==
               [{1_000_000, 1.0}, {3_000_000, 2.0}]
    end
  end

  test "falls back to the default tenant when X-Scope-OrgID is absent", %{conn: conn} do
    series = [%{labels: [{"__name__", "x"}], samples: [{1.0, 1}]}]
    conn = conn |> post_write(body(series))
    assert conn.status == 204
    assert {:ok, [%MetricSample{}]} = Storage.query(:metrics, "default")
  end

  # ------- header validation ------------------------------------------------

  test "415 when Content-Type is not application/x-protobuf", %{conn: conn} do
    series = [%{labels: [{"__name__", "x"}], samples: [{1.0, 1}]}]

    # Use `application/octet-stream` so `Plug.Parsers` leaves the body
    # alone — the test is guarding the controller's content-type gate,
    # not the parser pipeline above it.
    conn = conn |> post_write(body(series), %{"content-type" => "application/octet-stream"})
    assert json_response(conn, 415) == %{"error" => "unsupported_content_type"}
  end

  test "415 when Content-Encoding is not snappy (unlike Loki, absence is also rejected)",
       %{conn: conn} do
    series = [%{labels: [{"__name__", "x"}], samples: [{1.0, 1}]}]
    raw = encode_write_request(series)

    conn =
      conn
      |> put_req_header("content-type", "application/x-protobuf")
      |> put_req_header("x-prometheus-remote-write-version", "0.1.0")
      |> post(~p"/api/v1/write", raw)

    assert json_response(conn, 415) == %{"error" => "unsupported_content_encoding"}
  end

  test "400 when the remote_write version is a non-zero major", %{conn: conn} do
    series = [%{labels: [{"__name__", "x"}], samples: [{1.0, 1}]}]

    conn =
      conn
      |> post_write(body(series), %{"x-prometheus-remote-write-version" => "2.0.0"})

    assert json_response(conn, 400) == %{"error" => "unsupported_remote_write_version"}
  end

  test "an absent version header is accepted", %{conn: conn} do
    series = [%{labels: [{"__name__", "x"}], samples: [{1.0, 1}]}]
    raw = body(series)

    conn =
      conn
      |> put_req_header("content-type", "application/x-protobuf")
      |> put_req_header("content-encoding", "snappy")
      |> post(~p"/api/v1/write", raw)

    assert conn.status == 204
  end

  # ------- malformed inputs -------------------------------------------------

  test "400 when the body is not valid Snappy", %{conn: conn} do
    conn = conn |> post_write("not snappy data")
    assert json_response(conn, 400) == %{"error" => "invalid_snappy"}
  end

  test "rejects a series missing __name__ as a decode reject (counted, not fatal)",
       %{conn: conn} do
    # A series with no labels is counted in the rejected header but
    # does not fail the request.
    series = [
      %{labels: [], samples: [{1.0, 1}]},
      %{labels: [{"__name__", "ok"}], samples: [{1.0, 1}]}
    ]

    conn = conn |> post_write(body(series))

    assert conn.status == 204
    assert get_resp_header(conn, "x-pulso-rejected-records") == ["1"]
    assert {:ok, [%MetricSample{}]} = Storage.query(:metrics, "default")
  end

  test "counts every sample in a structurally rejected series", %{conn: conn} do
    request =
      body([
        %{labels: [], samples: [{1.0, 1}, {{:bits, 0x7FF0000000000002}, 2}, {2.0, 3}]},
        %{labels: [{"__name__", "safe"}], samples: [{3.0, 4}]}
      ])

    response = post_write(conn, request)
    assert response.status == 204
    assert get_resp_header(response, "x-pulso-rejected-records") == ["3"]
    assert {:ok, [stored]} = Storage.query(:metrics, "default")
    assert stored.value == 3.0
  end
end
