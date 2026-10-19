defmodule Pulso.Errors do
  @moduledoc "Sentry errors as structured Parquet logs; issue summaries are bounded read-time projections."

  alias Pulso.Alerting.Canonical
  alias Pulso.QueryRunner
  alias Pulso.Record.Log
  alias Pulso.Runtime
  alias Pulso.Storage

  @max_events 5000
  @max_decoded_bytes 33_554_432
  @max_response_bytes 2_097_152
  @cursor_salt "pulso-error-pages-v1"
  @max_ts 9_223_372_036_854_775_807
  @sensitive ~w(authorization proxy-authorization cookie set-cookie password passwd secret token api_key apikey access_token refresh_token sentry_key dsn)

  def record(event, headers, project_id, project) when is_map(event) do
    with :ok <- Pulso.IngestLimits.validate(:sentry, event),
         {:ok, id} <- event_id(headers["event_id"] || event["event_id"]),
         {:ok, timestamp} <- timestamp(event["timestamp"]),
         :ok <- validate_fingerprint(event["fingerprint"]) do
      event = event |> scrub() |> Map.put("event_id", id)
      issue = issue_id(event, project_id)
      level = text(event["level"], "error")
      trace = object(object(event["contexts"])["trace"])

      {:ok,
       %Log{
         timestamp_ns: timestamp,
         observed_timestamp_ns: System.system_time(:nanosecond),
         service: project["service"],
         severity_text: level,
         severity_number:
           Map.get(%{"fatal" => 21, "error" => 17, "warning" => 13, "info" => 9, "debug" => 5}, level, 17),
         trace_id: hex_id(trace["trace_id"], 32),
         span_id: hex_id(trace["span_id"], 16),
         body: event,
         resource: %{
           "error_source" => "sentry",
           "error_project" => project_id,
           "error_issue_id" => issue,
           "error_event_id" => id,
           "environment" => text(event["environment"], ""),
           "release" => text(event["release"], "")
         }
       }}
    end
  end

  def record(_, _, _, _), do: {:error, :invalid_event}

  @doc "Return byte-bounded JSON, including serialization inside the supervised query task."
  def query(kind, tenant, args) when kind in [:events, :issues] do
    start = args["start_ts_ns"]
    finish = args["end_ts_ns"]

    if is_integer(start) and is_integer(finish) and start >= 0 and finish >= start and finish <= @max_ts do
      QueryRunner.run(tenant, :raw, &query_window(kind, tenant, args, &1))
    else
      {:error, {:invalid_arguments, "require 0 <= start_ts_ns <= end_ts_ns <= signed 64-bit maximum"}}
    end
  end

  defp query_window(kind, tenant, args, deadline) do
    opts =
      [
        start_ts: args["start_ts_ns"],
        end_ts: args["end_ts_ns"],
        matchers: matchers(args),
        fresh: true,
        max_decoded_bytes: @max_decoded_bytes
      ] ++
        QueryRunner.storage_opts(deadline, @max_events)

    opts = if args["service"], do: Keyword.put(opts, :service, args["service"]), else: opts

    scope =
      digest([
        Atom.to_string(kind),
        tenant,
        Map.take(args, ~w(start_ts_ns end_ts_ns project service environment release issue_id event_id))
      ])

    with {:ok, position} <- page_position(args["cursor"], scope),
         {:ok, records} <- Storage.query(:logs, tenant, opts) do
      events =
        records
        |> Enum.sort_by(
          &{&1.timestamp_ns, &1.resource["error_event_id"], event_digest(&1), &1.resource["error_project"],
           &1.observed_timestamp_ns},
          :desc
        )
        |> Enum.uniq_by(&{&1.resource["error_project"], &1.resource["error_event_id"]})

      # Hash each event separately so fingerprinting does not allocate one
      # unbounded encoded copy of the complete result window.
      snapshot =
        events
        |> Enum.reduce(:crypto.hash_init(:sha256), fn event, hash ->
          :crypto.hash_update(hash, Pulso.JSON.encode!(event_result(event)))
        end)
        |> :crypto.hash_final()
        |> Base.encode16(case: :lower)

      results = if kind == :events, do: Enum.map(events, &event_result/1), else: issues(events)

      if position.snapshot in [nil, snapshot] do
        encode_page(results, length(events), args["limit"] || 100, scope, snapshot, position.offset)
      else
        {:error, :error_query_reset_required}
      end
    end
  end

  defp page_position(nil, _scope), do: {:ok, %{offset: 0, snapshot: nil}}

  defp page_position(token, scope) when is_binary(token) and byte_size(token) <= 4096 do
    case Phoenix.Token.decrypt(cursor_secret(), @cursor_salt, token, max_age: 600) do
      {:ok, %{scope: ^scope, offset: offset, snapshot: snapshot}}
      when is_integer(offset) and offset in 1..@max_events and is_binary(snapshot) ->
        {:ok, %{offset: offset, snapshot: snapshot}}

      _ ->
        {:error, :error_query_reset_required}
    end
  end

  defp page_position(_, _scope), do: {:error, :error_query_reset_required}

  defp cursor_secret, do: Runtime.fetch_env!(:pulso, PulsoWeb.Endpoint)[:secret_key_base]
  defp digest(value), do: :crypto.hash(:sha256, Pulso.JSON.encode!(value)) |> Base.encode16(case: :lower)

  defp encode_page(results, matched_events, limit, scope, snapshot, offset) do
    {page, _bytes} =
      results
      |> Enum.drop(offset)
      |> Enum.take(limit)
      |> Enum.reduce_while({[], 0}, fn result, {page, bytes} ->
        size = byte_size(Pulso.JSON.encode!(result)) + 1

        # Reserve space for the bounded cursor and metadata as well as rows.
        if bytes + size > @max_response_bytes - 4096,
          do: {:halt, {page, bytes}},
          else: {:cont, {[result | page], bytes + size}}
      end)

    next = offset + length(page)

    cursor =
      if next < length(results) and page != [] do
        Phoenix.Token.encrypt(cursor_secret(), @cursor_salt, %{scope: scope, snapshot: snapshot, offset: next})
      end

    json =
      Pulso.JSON.encode!(%{
        results: Enum.reverse(page),
        matched_events: matched_events,
        matched_results: length(results),
        snapshot_id: snapshot,
        next_cursor: cursor
      })

    if byte_size(json) <= @max_response_bytes and (page != [] or results == []),
      do: {:ok, json},
      else: {:error, :query_result_limit}
  end

  defp matchers(args) do
    fields = ~w(project issue_id event_id environment release)
    labels = ~w(error_project error_issue_id error_event_id environment release)

    Enum.zip(fields, labels)
    |> Enum.reduce([{"error_source", :eq, "sentry"}], fn {field, label}, acc ->
      if args[field], do: [{label, :eq, args[field]} | acc], else: acc
    end)
  end

  defp event_result(record) do
    %{
      event_id: record.resource["error_event_id"],
      issue_id: record.resource["error_issue_id"],
      project: record.resource["error_project"],
      service: record.service,
      timestamp_ns: record.timestamp_ns,
      received_at_ns: record.observed_timestamp_ns,
      trace_id: record.trace_id,
      span_id: record.span_id,
      event: record.body
    }
  end

  # The derived index can retain this compact conflict key instead of event
  # bodies, while choosing the same retry winner as raw diagnosis.
  @doc false
  def event_digest(record) do
    Canonical.hash([record.body, record.service, record.resource["error_issue_id"]])
  end

  defp issues(events) do
    events
    |> Enum.group_by(&{&1.resource["error_project"], &1.resource["error_issue_id"]})
    |> Enum.map(fn {{project, id}, [latest | _] = occurrences} ->
      %{
        issue_id: id,
        project: project,
        service: latest.service,
        title: title(latest.body),
        count: length(occurrences),
        first_seen_ns: Enum.min_by(occurrences, & &1.timestamp_ns).timestamp_ns,
        last_seen_ns: latest.timestamp_ns,
        latest_event_id: latest.resource["error_event_id"]
      }
    end)
    |> Enum.sort_by(&{-&1.last_seen_ns, &1.project, &1.issue_id})
  end

  # JSON framing preserves component boundaries (unlike joining with a delimiter).
  # v1 is part of the identity so future grouping algorithms can coexist.
  defp issue_id(event, project) do
    exception = exception(event)
    frames = object(exception["stacktrace"])["frames"]
    frames = if is_list(frames), do: Enum.filter(frames, &is_map/1), else: []
    in_app = Enum.filter(frames, &(&1["in_app"] == true))
    frame = List.last(if in_app == [], do: frames, else: in_app) || %{}
    location = [text(frame["module"], text(frame["filename"], "")), text(frame["function"], "")]

    default =
      if location == ["", ""] do
        [text(exception["type"], ""), text(event["logger"], ""), normalize_message(title(event))]
      else
        [text(exception["type"], ""), location]
      end

    fingerprint = if event["fingerprint"] in [nil, []], do: ["{{default}}"], else: event["fingerprint"]

    components =
      Enum.map(fingerprint, fn value ->
        if Regex.match?(~r/\A\{\{\s*default\s*\}\}\z/, value), do: default, else: value
      end)

    digest = :crypto.hash(:sha256, Pulso.JSON.encode!(["sentry-group-v1", project, components]))
    "v1_" <> Base.encode16(digest, case: :lower)
  end

  defp normalize_message(message) do
    message |> String.replace(~r/0x[0-9a-f]+|\d+/i, "#") |> String.replace(~r/\s+/, " ") |> String.slice(0, 200)
  end

  @doc false
  def title(event) do
    ex = exception(event)
    logentry = object(event["logentry"])
    message = text(event["message"], text(object(event["message"])["formatted"], "Error"))
    message = text(ex["value"], text(logentry["formatted"], text(logentry["message"], message)))
    type = text(ex["type"], "")
    String.slice(if(type == "", do: message, else: type <> ": " <> message), 0, 500)
  end

  defp exception(event) do
    case object(event["exception"])["values"] do
      values when is_list(values) -> values |> Enum.filter(&is_map/1) |> List.last() || %{}
      _ -> %{}
    end
  end

  defp validate_fingerprint(nil), do: :ok

  defp validate_fingerprint(values) when is_list(values) and length(values) <= 32 do
    if Enum.all?(values, &(is_binary(&1) and byte_size(&1) <= 256)), do: :ok, else: {:error, :invalid_fingerprint}
  end

  defp validate_fingerprint(_), do: {:error, :invalid_fingerprint}

  defp event_id(nil), do: {:ok, Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)}

  defp event_id(value) when is_binary(value) do
    if Regex.match?(
         ~r/\A(?:[0-9a-fA-F]{32}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\z/,
         value
       ),
       do: {:ok, value |> String.replace("-", "") |> String.downcase()},
       else: {:error, :invalid_event_id}
  end

  defp event_id(_), do: {:error, :invalid_event_id}

  defp timestamp(nil), do: {:ok, System.system_time(:nanosecond)}

  defp timestamp(value) when is_number(value) and value >= 0 and value <= 9_223_372_036 do
    validate_timestamp(round(value * 1_000_000_000))
  end

  defp timestamp(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _} -> validate_timestamp(DateTime.to_unix(dt, :nanosecond))
      _ -> {:error, :invalid_timestamp}
    end
  end

  defp timestamp(_), do: {:error, :invalid_timestamp}

  defp validate_timestamp(ns) when ns >= 0 and ns <= @max_ts do
    if ns <= System.system_time(:nanosecond) + 600_000_000_000, do: {:ok, ns}, else: {:error, :invalid_timestamp}
  end

  defp validate_timestamp(_), do: {:error, :invalid_timestamp}

  # A conservative baseline, not a promise to recognize secrets in free text.
  defp scrub(map) when is_map(map) do
    Map.new(map, fn {key, value} ->
      normalized = String.downcase(key)

      value =
        cond do
          normalized in @sensitive -> "[Filtered]"
          normalized in ~w(user request) -> "[Filtered]"
          normalized == "headers" -> "[Filtered]"
          true -> scrub(value)
        end

      {key, value}
    end)
  end

  defp scrub([key, value]) when is_binary(key) do
    if String.downcase(key) in @sensitive, do: [key, "[Filtered]"], else: [key, scrub(value)]
  end

  defp scrub(list) when is_list(list), do: Enum.map(list, &scrub/1)
  defp scrub(value), do: value

  defp text(value, _) when is_binary(value), do: value
  defp text(_, default), do: default
  defp object(value) when is_map(value), do: value
  defp object(_), do: %{}

  defp hex_id(value, size) when is_binary(value) and byte_size(value) == size do
    case Base.decode16(value, case: :mixed) do
      {:ok, _} -> String.downcase(value)
      _ -> nil
    end
  end

  defp hex_id(_, _), do: nil
end
