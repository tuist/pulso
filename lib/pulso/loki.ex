defmodule Pulso.Loki do
  @moduledoc """
  Read-only client for a Grafana Loki backend.

  Wraps a subset of Loki's HTTP query API. The backend URL is read from
  `config :pulso, Pulso.Loki, base_url: "http://..."` and can be overridden
  per call with the `:base_url` option.
  """

  @default_limit 100

  @type range_opt ::
          {:start, DateTime.t() | integer()}
          | {:end, DateTime.t() | integer()}
          | {:limit, pos_integer()}
          | {:direction, :backward | :forward}
          | {:base_url, String.t()}

  @spec query_range(String.t(), [range_opt()]) :: {:ok, map()} | {:error, term()}
  def query_range(logql, opts \\ []) when is_binary(logql) do
    params =
      %{"query" => logql, "limit" => Keyword.get(opts, :limit, @default_limit)}
      |> put_time("start", Keyword.get(opts, :start))
      |> put_time("end", Keyword.get(opts, :end))
      |> put_direction(Keyword.get(opts, :direction))

    req()
    |> Req.merge(base_url: Keyword.get(opts, :base_url) || configured_base_url())
    |> Req.get(url: "/loki/api/v1/query_range", params: params)
    |> handle()
  end

  defp put_time(params, _key, nil), do: params

  defp put_time(params, key, %DateTime{} = dt), do: Map.put(params, key, DateTime.to_unix(dt, :nanosecond))

  defp put_time(params, key, ns) when is_integer(ns), do: Map.put(params, key, ns)

  defp put_direction(params, nil), do: params

  defp put_direction(params, dir) when dir in [:backward, :forward],
    do: Map.put(params, "direction", Atom.to_string(dir))

  defp handle({:ok, %Req.Response{status: 200, body: body}}), do: {:ok, body}

  defp handle({:ok, %Req.Response{status: status, body: body}}), do: {:error, {:http, status, body}}

  defp handle({:error, reason}), do: {:error, reason}

  defp req do
    Req.new(receive_timeout: 15_000)
  end

  defp configured_base_url do
    Application.get_env(:pulso, __MODULE__, [])
    |> Keyword.get(:base_url) ||
      raise "Pulso.Loki base_url not configured. Set config :pulso, Pulso.Loki, base_url: \"...\""
  end
end
