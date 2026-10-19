defmodule PulsoWeb.SentryController do
  use PulsoWeb, :controller

  alias Pulso.Errors
  alias Pulso.Sentry
  alias Pulso.Sentry.Envelope

  def envelope(conn, %{"project_id" => id}) do
    with {:ok, headers, event, discarded} <- Envelope.decode(conn.private.sentry_body),
         {:ok, project} <- Sentry.authorize(conn, id, headers) do
      ingest(conn, id, project, headers, event, discarded)
    else
      error -> failure(conn, error)
    end
  end

  def store(conn, %{"project_id" => id}) do
    with {:ok, project} <- Sentry.authorize(conn, id, %{}),
         true <- byte_size(conn.private.sentry_body) <= 1_048_576,
         {:ok, event} when is_map(event) <- Pulso.JSON.decode(conn.private.sentry_body) do
      ingest(conn, id, project, %{}, event, %{})
    else
      false -> failure(conn, {:error, :event_too_large})
      {:error, :invalid_sentry_key} = error -> failure(conn, error)
      _ -> failure(conn, {:error, :invalid_event})
    end
  end

  defp ingest(conn, _id, _project, _headers, nil, discarded), do: success(conn, nil, discarded)

  defp ingest(conn, id, project, headers, event, discarded) do
    with {:ok, record} <- Errors.record(event, headers, id, project),
         :ok <- Pulso.Metrics.append(:logs, project["tenant"], [record], 0, []) do
      success(conn, record.resource["error_event_id"], discarded)
    else
      error -> failure(conn, error)
    end
  end

  defp success(conn, id, discarded) do
    count = Enum.sum(Map.values(discarded))
    conn = if count > 0, do: put_resp_header(conn, "x-pulso-discarded-items", to_string(count)), else: conn
    json(conn, %{id: id, discarded_items: discarded})
  end

  defp failure(conn, {:error, :invalid_sentry_key}), do: conn |> put_status(403) |> json(%{error: "invalid_sentry_key"})

  defp failure(conn, {:error, reason})
       when reason in [:event_too_large, :too_many_items, :attributes_too_large, :too_many_records],
       do: conn |> put_status(413) |> json(%{error: to_string(reason)})

  defp failure(conn, {:error, reason})
       when reason in [
              :invalid_envelope,
              :multiple_events,
              :invalid_event,
              :invalid_event_id,
              :invalid_timestamp,
              :invalid_fingerprint,
              :retention_expired,
              :timestamp_too_new
            ], do: conn |> put_status(400) |> json(%{error: to_string(reason)})

  # Storage details can contain URLs/credentials. Do not expose them to SDKs.
  defp failure(conn, _error),
    do: conn |> put_resp_header("retry-after", "5") |> put_status(503) |> json(%{error: "storage_unavailable"})
end
