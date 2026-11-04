defmodule Pulso.QueryLimit do
  @moduledoc """
  Which query budget a `:query_scan_limit` failure hit, and its value.

  Storage keeps returning the bare `:query_scan_limit` reason that every
  caller matches on; the detail travels beside it in the process dictionary.
  `Pulso.QueryRunner` carries it from the query worker back to the caller,
  where error rendering reads it with `take/0`.
  """

  @key {__MODULE__, :exceeded}

  @env %{
    max_scan_segments: "PULSO_QUERY_MAX_SCAN_SEGMENTS",
    max_scan_bytes: "PULSO_QUERY_MAX_SCAN_BYTES",
    max_scan_rows: "PULSO_QUERY_MAX_SCAN_ROWS"
  }

  @hint "narrow the time range or stream selector, or raise"

  @type detail :: %{limit: atom(), maximum: integer() | nil, observed: integer() | nil, stage: atom()}

  @doc """
  Record that `limit` (with configured `maximum`) was exceeded at `stage`,
  having seen at least `observed`, and return `{:error, :query_scan_limit}`.
  """
  def exceeded(limit, maximum, observed, stage) do
    Process.put(@key, %{limit: limit, maximum: maximum, observed: observed, stage: stage})
    {:error, :query_scan_limit}
  end

  @doc "The latest recorded detail, removing it."
  @spec take() :: detail() | nil
  def take, do: Process.delete(@key)

  @doc "Re-record a detail taken from another process."
  def put(nil), do: :ok
  def put(detail), do: Process.put(@key, detail) && :ok

  @doc "A one-line, human-readable description of a detail."
  @spec describe(detail() | nil) :: String.t() | nil
  def describe(nil), do: nil

  def describe(%{limit: :metadata}),
    do: "manifest pages read while planning candidate segments exceeded their metadata budget; narrow the time range"

  def describe(%{limit: :max_scan_bytes, stage: :object, maximum: remaining}),
    do: "a segment object exceeded the #{remaining} bytes left of max_scan_bytes; #{@hint} PULSO_QUERY_MAX_SCAN_BYTES"

  def describe(%{limit: limit, maximum: maximum, observed: observed, stage: stage}) do
    env = Map.get(@env, limit)

    [
      "#{limit}=#{maximum} exceeded",
      if(is_integer(observed), do: ": #{subject(stage)} at least #{observed} #{unit(limit)}", else: ""),
      if(env, do: "; #{@hint} #{env}", else: "")
    ]
    |> IO.iodata_to_binary()
  end

  defp subject(:candidates), do: "candidates in the time range total"
  defp subject(:visited), do: "the scan needed"
  defp subject(_stage), do: "the query reached"

  defp unit(:max_scan_bytes), do: "bytes"
  defp unit(:max_scan_rows), do: "rows"
  defp unit(_limit), do: "segments"
end
