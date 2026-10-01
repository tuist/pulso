defmodule PulsoWeb.LokiQueryController do
  @moduledoc """
  Loki-compatible query endpoints backed by `Pulso.LogQL.Evaluator`.

  Routes served here are the read-side complement of
  `PulsoWeb.LokiController`'s push endpoint. They accept the same
  `X-Scope-OrgID` tenant header and pass every request through
  `Pulso.Auth.verify/2` so a shared-secret deployment applies uniformly
  across ingest and query.

  Endpoints:

    * `GET|POST /loki/api/v1/query_range` — range query over `[start,
      end]` at `step`. Returns `streams` for log queries and `matrix`
      for metric queries.
    * `GET|POST /loki/api/v1/query` — instant query at `time`. Returns
      `streams` for log queries and `vector` for metric queries.
    * `GET /loki/api/v1/labels` — list of every label name seen across
      the tenant's segments within `[start, end]`.
    * `GET /loki/api/v1/label/:name/values` — list of every value seen
      for one label within `[start, end]`.
  """

  use PulsoWeb, :controller

  alias Pulso.Auth
  alias Pulso.LogQL.AST
  alias Pulso.LogQL.Envelope
  alias Pulso.LogQL.Evaluator
  alias Pulso.LogQL.Parser
  alias Pulso.Storage

  @default_tenant "default"
  @default_step_ms 15_000
  @default_direction :backward
  @default_limit 100
  # Hard cap on records the `labels` / `label_values` endpoints load in
  # a single call. Real label discovery will land as a sidecar posting
  # list (see PR 4 in the plan); until then, cap the scan so a
  # Grafana-style dashboard poll on a large tenant does not OOM the
  # node. The value is deliberately conservative — 5k records already
  # covers the label surface of a well-configured tenant.
  @label_scan_cap 5_000

  # ---------------------------------------------------------------------------
  # query_range
  # ---------------------------------------------------------------------------

  def query_range(conn, params) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         {:ok, query_str} <- require_query(params),
         {:ok, ast} <- parse_query(query_str) do
      opts = build_opts(params, :range)

      case ast do
        %AST.LogQuery{} ->
          case Evaluator.evaluate_log(ast, tenant, opts) do
            {:ok, streams} -> json(conn, Envelope.streams(streams))
            {:error, reason} -> error_json(conn, :bad_request, "evaluation_failed", inspect(reason))
          end

        _ ->
          case Evaluator.evaluate_metric(ast, tenant, opts) do
            {:ok, {:matrix, series}} -> json(conn, Envelope.matrix(series))
            {:ok, {:vector, series}} -> json(conn, Envelope.vector(series))
            {:error, reason} -> error_json(conn, :bad_request, "evaluation_failed", inspect(reason))
          end
      end
    else
      err -> render_error(conn, err)
    end
  end

  # ---------------------------------------------------------------------------
  # query (instant)
  # ---------------------------------------------------------------------------

  def query(conn, params) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         {:ok, query_str} <- require_query(params),
         {:ok, ast} <- parse_query(query_str) do
      opts = build_opts(params, :instant)

      case ast do
        %AST.LogQuery{} ->
          case Evaluator.evaluate_log(ast, tenant, opts) do
            {:ok, streams} -> json(conn, Envelope.streams(streams))
            {:error, reason} -> error_json(conn, :bad_request, "evaluation_failed", inspect(reason))
          end

        _ ->
          case Evaluator.evaluate_metric(ast, tenant, opts) do
            {:ok, {:vector, series}} -> json(conn, Envelope.vector(series))
            {:ok, {:matrix, series}} -> json(conn, Envelope.matrix(series))
            {:error, reason} -> error_json(conn, :bad_request, "evaluation_failed", inspect(reason))
          end
      end
    else
      err -> render_error(conn, err)
    end
  end

  # ---------------------------------------------------------------------------
  # labels / label values — cheap path: scan the manifest and use the first
  # segment's resource JSON as the sample. Real implementation will
  # augment with a per-tenant sidecar label index; that lands with the
  # posting-list follow-up.
  # ---------------------------------------------------------------------------

  def labels(conn, params) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         {:ok, records} <- Storage.query(tenant, build_storage_time_opts(params)) do
      names =
        records
        |> Enum.flat_map(fn r ->
          resource = Map.keys(r.resource || %{})
          attributes = Map.keys(r.attributes || %{})
          resource ++ attributes
        end)
        |> Enum.uniq()
        |> Enum.sort()

      json(conn, %{"status" => "success", "data" => names})
    else
      err -> render_error(conn, err)
    end
  end

  def label_values(conn, %{"name" => name} = params) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         {:ok, records} <- Storage.query(tenant, build_storage_time_opts(params)) do
      values =
        records
        |> Enum.map(fn r ->
          Map.get(r.resource || %{}, name) || Map.get(r.attributes || %{}, name)
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()
        |> Enum.sort()

      json(conn, %{"status" => "success", "data" => values})
    else
      err -> render_error(conn, err)
    end
  end

  defp build_storage_time_opts(params) do
    [limit: @label_scan_cap]
    |> maybe_kw(:start_ts, parse_ts(params["start"]))
    |> maybe_kw(:end_ts, parse_ts(params["end"]))
  end

  defp maybe_kw(kw, _key, nil), do: kw
  defp maybe_kw(kw, key, value), do: Keyword.put(kw, key, value)

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp require_query(%{"query" => q}) when is_binary(q) and byte_size(q) > 0, do: {:ok, q}
  defp require_query(_), do: {:error, {:missing_param, "query"}}

  defp parse_query(str) do
    case Parser.parse(str) do
      {:ok, ast} -> {:ok, ast}
      {:error, reason} -> {:error, {:parse_error, reason}}
    end
  end

  # Loki accepts start/end as either Unix nanoseconds (integer or
  # integer-in-a-string) or RFC3339. For phase 1 we accept only integer
  # nanoseconds; the RFC3339 form can be added if a real client needs it.
  # `step` is a duration string (`15s`, `1m`) or an integer seconds value.
  defp build_opts(params, mode) do
    params
    |> build_time_opts()
    |> maybe_put(:step_ns, step_for(mode, params))
    |> maybe_put(:limit, parse_limit(params["limit"]))
    |> maybe_put(:direction, parse_direction(params["direction"]))
  end

  defp step_for(:range, params), do: parse_step(params["step"]) || default_step_ns()
  defp step_for(:instant, _params), do: nil

  defp parse_limit(nil), do: @default_limit
  defp parse_limit(v) when is_integer(v) and v > 0, do: v

  defp parse_limit(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, ""} when n > 0 -> n
      _ -> @default_limit
    end
  end

  defp parse_limit(_), do: @default_limit

  defp parse_direction("forward"), do: :forward
  defp parse_direction("backward"), do: :backward
  defp parse_direction(_), do: @default_direction

  defp build_time_opts(params) do
    %{}
    |> maybe_put(:start_ts_ns, parse_ts(params["start"]))
    |> maybe_put(:end_ts_ns, parse_ts(params["end"]))
  end

  defp default_step_ns, do: @default_step_ms * 1_000_000

  defp parse_ts(nil), do: nil
  defp parse_ts(v) when is_integer(v), do: v

  # Loki accepts Unix nanoseconds (integer or integer-string) or an
  # RFC3339 timestamp. Grafana Explore uses RFC3339 by default; without
  # it the endpoint would silently drop the bound and scan the whole
  # tenant.
  defp parse_ts(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, ""} ->
        n

      _ ->
        case DateTime.from_iso8601(v) do
          {:ok, dt, _offset} -> DateTime.to_unix(dt, :nanosecond)
          _ -> nil
        end
    end
  end

  defp parse_step(nil), do: nil
  defp parse_step(v) when is_integer(v), do: v * 1_000_000_000

  defp parse_step(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, ""} -> n * 1_000_000_000
      {n, "s"} -> n * 1_000_000_000
      {n, "ms"} -> n * 1_000_000
      {n, "m"} -> n * 60 * 1_000_000_000
      {n, "h"} -> n * 3600 * 1_000_000_000
      _ -> nil
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp tenant_from(conn) do
    case Plug.Conn.get_req_header(conn, "x-scope-orgid") do
      [t | _] when is_binary(t) and t != "" -> t
      _ -> @default_tenant
    end
  end

  # -- Error rendering -------------------------------------------------------

  defp render_error(conn, {:error, {:missing_param, name}}) do
    error_json(conn, :bad_request, "missing_param", name)
  end

  defp render_error(conn, {:error, {:parse_error, reason}}) do
    error_json(conn, :bad_request, "parse_error", inspect(reason))
  end

  defp render_error(conn, {:error, reason}) when reason in [:missing_token, :invalid_token, :unknown_tenant] do
    conn |> put_status(:unauthorized) |> json(%{"status" => "error", "error" => to_string(reason)})
  end

  defp render_error(conn, err) do
    error_json(conn, :internal_server_error, "unexpected", inspect(err))
  end

  defp error_json(conn, status, code, message) do
    conn
    |> put_status(status)
    |> json(%{"status" => "error", "error" => code, "message" => message})
  end
end
