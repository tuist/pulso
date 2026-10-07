defmodule PulsoWeb.AlertingController do
  use PulsoWeb, :controller

  alias Pulso.Alerting
  alias Pulso.Alerting.{Evaluator, Principal}
  alias Pulso.Alerting.Importer

  def index(conn, _params), do: run(conn, fn actor -> Alerting.list(actor) end)

  def import_preview(conn, params) do
    run(conn, fn actor ->
      with :ok <- Principal.authorize(actor, "alert:import"),
           {:ok, reports} <- Importer.preview(params["rules"]),
           do: {:ok, %{"rules" => reports}}
    end)
  end

  def show(conn, %{"id" => id}), do: run(conn, fn actor -> Alerting.get(actor, id) end)
  def state(conn, %{"id" => id}), do: run(conn, fn actor -> Alerting.state(actor, id) end)
  def preview(conn, %{"id" => id}), do: run(conn, fn actor -> Evaluator.preview(actor, id) end)
  def evaluate(conn, %{"id" => id}), do: run(conn, fn actor -> Evaluator.evaluate(actor, id) end)

  def create(conn, %{"id" => id} = params), do: mutation(conn, id, params, &Alerting.create/3)
  def update(conn, %{"id" => id} = params), do: mutation(conn, id, params, &Alerting.update/3)
  def delete(conn, %{"id" => id} = params), do: mutation(conn, id, params, &Alerting.delete/3)
  def restore(conn, %{"id" => id} = params), do: mutation(conn, id, params, &Alerting.restore/3)

  def changes(conn, %{"id" => id} = params) do
    run(conn, fn actor ->
      if conn.method == "GET" and Map.has_key?(params, "cursor") do
        {:error, :invalid_arguments}
      else
        Alerting.changes(actor, id, pagination(params))
      end
    end)
  end

  def events(conn, %{"id" => id} = params) do
    run(conn, fn actor -> Alerting.events(actor, id, pagination(params)) end)
  end

  def revision(conn, %{"id" => id, "revision" => revision} = params) do
    run(conn, fn actor ->
      if conn.method == "GET" and Map.has_key?(params, "revision_cursor"),
        do: {:error, :invalid_arguments},
        else:
          Alerting.historical(
            actor,
            id,
            Map.take(Map.put(params, "revision", revision), ["revision", "revision_cursor"])
          )
    end)
  end

  defp mutation(conn, id, params, fun) do
    run(conn, fn actor ->
      params = Map.delete(params, "id")

      with {:ok, params} <- precondition(get_req_header(conn, "if-match"), params),
           do: fun.(actor, id, params)
    end)
  end

  defp precondition([], params), do: {:ok, params}

  defp precondition([value], params) do
    expected = String.trim(value, "\"")

    if params["expected_revision"] in [nil, expected],
      do: {:ok, Map.put(params, "expected_revision", expected)},
      else: {:error, :conflict}
  end

  defp precondition(_, _), do: {:error, :invalid_arguments}

  defp pagination(params) do
    params = Map.take(params, ["cursor", "limit"])

    case params["limit"] do
      value when is_binary(value) ->
        case Integer.parse(value) do
          {number, ""} -> Map.put(params, "limit", number)
          _ -> params
        end

      _ ->
        params
    end
  end

  defp run(conn, fun) do
    tenant =
      case get_req_header(conn, "x-scope-orgid") do
        [] -> "default"
        [tenant] -> tenant
        _ -> nil
      end

    with {:ok, actor} <- Principal.authenticate(conn, tenant), {:ok, data} <- fun.(actor) do
      conn =
        if is_map(data) and is_binary(data["revision"]),
          do: put_resp_header(conn, "etag", "\"#{data["revision"]}\""),
          else: conn

      json(conn, data)
    else
      {:error, reason} -> error(conn, reason)
    end
  end

  defp error(conn, reason) do
    {status, code} =
      case reason do
        :unauthorized ->
          {401, "unauthorized"}

        :forbidden ->
          {403, "forbidden"}

        :not_found ->
          {404, "not_found"}

        reason when reason in [:conflict, :operation_reused, :not_due, :stale_evaluation] ->
          {409, Atom.to_string(reason)}

        reason when reason in [:invalid_arguments, :invalid_rule, :invalid_id, :invalid_import, :duplicate_rule_id] ->
          {400, Atom.to_string(reason)}

        reason when reason in [:reset_required, :invalid_or_expired_position] ->
          {409, Atom.to_string(reason)}

        reason when reason in [:rule_disabled, :unsupported_rule] ->
          {422, Atom.to_string(reason)}

        _ ->
          {503, "alerting_unavailable"}
      end

    conn |> put_status(status) |> json(%{"error" => code})
  end
end
