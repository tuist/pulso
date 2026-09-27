defmodule Pulso.Storage.S3CodecTest do
  # The S3 adapter encodes and decodes segments in Rust and keeps the
  # Elixir implementation as the reference and fallback. These tests hold
  # the two to the same results on random batches and on every case where
  # Rust defers. No object store needed.
  use ExUnit.Case, async: true

  alias Pulso.Codec.NIF
  alias Pulso.Record.Log
  alias Pulso.Storage.S3
  alias Pulso.Test.RandomTerms

  defp lines(payload), do: payload |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)

  defp same_encode(records) do
    native = S3.encode_segment(records, :native)
    elixir = S3.encode_segment(records, :elixir)

    case {native, elixir} do
      {{:ok, n_payload, n_min, n_max}, {:ok, e_payload, e_min, e_max}} ->
        assert {n_min, n_max} == {e_min, e_max}
        assert lines(n_payload) == lines(e_payload)

      {{:error, n_reason}, {:error, e_reason}} ->
        assert error_shape(n_reason) == error_shape(e_reason)
    end

    native
  end

  defp error_shape({:encode_failed, e}), do: {:encode_failed, e.__struct__}
  defp error_shape(other), do: other

  describe "encode_segment/2" do
    test "matches the Elixir encoder on random batches, via Rust" do
      for _ <- 1..200 do
        records = RandomTerms.logs(:rand.uniform(40))
        assert {:ok, _, _, _, _} = NIF.encode_log_segment(records, :storage, :lines)
        assert {:ok, _, _, _} = same_encode(records)
      end
    end

    test "encodes shared resources once but writes them on every line" do
      resource = %{"service_name" => "api", "pod" => String.duplicate("p", 100)}
      records = for i <- 1..50, do: %Log{timestamp_ns: i, resource: resource, attributes: %{"i" => i}}
      {:ok, payload, 1, 50} = same_encode(records)
      assert Enum.all?(lines(payload), &(&1["resource"] == resource))
    end

    test "nil or false attributes and resource are stored as empty maps" do
      records = [%Log{timestamp_ns: 1, attributes: nil, resource: false}]
      {:ok, payload, _, _} = same_encode(records)
      assert [%{"attributes" => %{}, "resource" => %{}}] = lines(payload)
    end

    test "defers to Elixir and returns the same errors" do
      cases = [
        [%Log{timestamp_ns: 1, attributes: %{:a => 1, "a" => 2}}],
        [%Log{timestamp_ns: 1, resource: %{1 => "x", "1" => "y"}}],
        [%Log{timestamp_ns: 1, attributes: %{"k" => {:tuple}}}],
        [%Log{timestamp_ns: 1, body: <<0xFF>>}],
        [%Log{timestamp_ns: 1.5}],
        [%Log{timestamp_ns: 123_456_789_012_345_678_901_234_567_890}],
        [%Log{timestamp_ns: 1, body: %{2.5 => "float key"}}]
      ]

      for records <- cases do
        assert NIF.encode_log_segment(records, :storage, :lines) == :fallback
        same_encode(records)
      end
    end

    test "raises what the Elixir encoder raises" do
      records = [%Log{timestamp_ns: 1, attributes: %{"at" => ~U[2026-01-01 00:00:00Z]}}]
      expected = catch_error(S3.encode_segment(records, :elixir))
      assert catch_error(S3.encode_segment(records, :native)).__struct__ == expected.__struct__
    end

    test "the plain array encoding matches the MCP tool's field maps" do
      for _ <- 1..50 do
        records = RandomTerms.logs(:rand.uniform(20))
        assert {:ok, json, _, _, _} = NIF.encode_log_segment(records, :plain, :array)
        assert JSON.decode!(json) == records |> Enum.map(&Map.from_struct/1) |> JSON.encode!() |> JSON.decode!()
      end
    end
  end

  describe "decode_segment/5" do
    test "matches the Elixir decoder and filters on random segments, via Rust" do
      for _ <- 1..100 do
        records = RandomTerms.logs(:rand.uniform(40))
        {:ok, payload, _, _} = S3.encode_segment(records, :elixir)
        timestamps = records |> Enum.map(& &1.timestamp_ns) |> Enum.reject(&is_nil/1)
        pick = fn -> if !(timestamps == [] or :rand.uniform(3) == 1), do: Enum.random(timestamps) end

        filter = {pick.(), pick.(), Enum.random([nil, "svc1", "svc2", "missing"])}
        {start_ts, end_ts, service} = filter

        assert {:ok, native} = NIF.decode_log_segment(payload, start_ts, end_ts, service)
        assert native == S3.decode_segment(payload, start_ts, end_ts, service, :elixir), inspect(filter)
      end
    end

    test "reads segments written by the Rust encoder" do
      records = RandomTerms.logs(30)
      {:ok, payload, _, _} = S3.encode_segment(records, :native)
      assert S3.decode_segment(payload, nil, nil, nil) == S3.decode_segment(payload, nil, nil, nil, :elixir)
    end

    test "keeps nil-timestamp records only when there is no time filter" do
      {:ok, payload, _, _} = S3.encode_segment([%Log{timestamp_ns: nil, body: "x"}, %Log{timestamp_ns: 5}], :elixir)
      assert [_, _] = S3.decode_segment(payload, nil, nil, nil)
      assert [%Log{timestamp_ns: 5}] = S3.decode_segment(payload, 0, nil, nil)
    end

    test "defers to Elixir on duplicate keys and non-integer filters" do
      duplicate = ~s({"timestamp_ns":1,"body":"first","body":"second"}\n)
      assert NIF.decode_log_segment(duplicate, nil, nil, nil) == :fallback
      assert S3.decode_segment(duplicate, nil, nil, nil) == S3.decode_segment(duplicate, nil, nil, nil, :elixir)

      {:ok, payload, _, _} = S3.encode_segment([%Log{timestamp_ns: 10}, %Log{timestamp_ns: 20}], :elixir)
      assert S3.decode_segment(payload, 10.5, nil, nil) == [%Log{timestamp_ns: 20, attributes: %{}, resource: %{}}]
    end

    test "raises what the Elixir decoder raises on malformed segments" do
      for blob <- [~s({"body":"no timestamp"}\n), "{not json\n", ~s(1\n)] do
        expected = catch_error(S3.decode_segment(blob, nil, nil, nil, :elixir))
        actual = catch_error(S3.decode_segment(blob, nil, nil, nil))
        assert error_kind(actual) == error_kind(expected)
      end
    end

    test "an empty segment decodes to no records" do
      assert S3.decode_segment("", nil, nil, nil) == []
      assert S3.decode_segment("\n\n", 0, 10, "svc") == []
    end
  end

  defp error_kind(error) when is_exception(error), do: error.__struct__
  defp error_kind(error), do: error
end
