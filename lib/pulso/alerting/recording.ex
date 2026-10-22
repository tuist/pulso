defmodule Pulso.Alerting.Recording do
  @moduledoc "Bounded native recording results and replay of committed, immutable publication intents."

  alias Pulso.Alerting.{Canonical, Repository, Rule}
  alias Pulso.PromQL.FloatParser
  alias Pulso.Record.MetricSample
  alias Pulso.Storage
  alias Pulso.Storage.S3

  @max_samples 128
  @max_bytes 65_536
  # Both a fresh set and retirement of that set must fit half the joint
  # budget. A future tick can replace every series without getting stuck
  # retrying an individually valid set plus oversized prior stale markers.
  @set_bytes div(@max_bytes - 2048, 2)

  def result(rule, samples, timestamp) do
    with true <- is_integer(timestamp) and timestamp in 0..9_223_372_036_854_775_807,
         true <- is_list(samples) and length(samples) <= @max_samples,
         {:ok, points} <- points(rule, samples),
         result = %{"timestamp_ns" => Integer.to_string(timestamp), "samples" => points},
         true <- byte_size(Canonical.encode(result)) <= @set_bytes,
         markers = retirement(Enum.map(points, & &1["labels"]), timestamp) |> Map.delete("next_series"),
         true <- byte_size(Canonical.encode(markers)) <= @set_bytes do
      {:ok, result}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_recording_result}
    end
  end

  def transition(rule, samples, timestamp, previous) do
    with {:ok, result} <- result(rule, samples, timestamp),
         true <- is_list(previous) and length(previous) <= @max_samples and Enum.all?(previous, &Rule.labels?/1) do
      next = Enum.map(result["samples"], & &1["labels"])
      missing = Enum.reject(previous, &(&1 in next))
      markers = Enum.map(missing, &%{"labels" => &1, "value" => "NaN", "stale" => true})
      result = result |> Map.put("samples", result["samples"] ++ markers) |> Map.put("next_series", next)

      if byte_size(Canonical.encode(Map.delete(result, "next_series"))) <= @max_bytes - 2048,
        do: {:ok, result},
        else: {:error, :invalid_recording_result}
    else
      {:error, _} = error -> error
      _ -> {:error, :integrity_error}
    end
  end

  def retirement(previous, timestamp) do
    %{
      "timestamp_ns" => Integer.to_string(timestamp),
      "samples" => Enum.map(previous, &%{"labels" => &1, "value" => "NaN", "stale" => true}),
      "next_series" => []
    }
  end

  defp points(rule, samples) do
    Enum.reduce_while(samples, {:ok, %{}}, fn sample, {:ok, acc} ->
      with %{"metric" => labels, "value" => [_time, value]} <- sample,
           true <- Rule.labels?(labels) and Enum.all?(Map.keys(labels), &Rule.label_name?/1),
           {:ok, _} <- FloatParser.parse(value),
           labels = labels(labels, rule),
           true <- Rule.labels?(labels),
           key = Canonical.encode(labels),
           false <- Map.has_key?(acc, key) do
        {:cont, {:ok, Map.put(acc, key, %{"labels" => labels, "value" => value})}}
      else
        true -> {:halt, {:error, :duplicate_recording_label_sets}}
        _ -> {:halt, {:error, :invalid_recording_result}}
      end
    end)
    |> case do
      {:ok, points} -> {:ok, points |> Enum.sort() |> Enum.map(&elem(&1, 1))}
      error -> error
    end
  end

  defp labels(labels, rule) do
    labels
    |> Map.merge(rule["labels"])
    |> Map.reject(fn {_key, item} -> item == "" end)
    |> Map.put("__name__", rule["name"])
  end

  @doc "Publish at most one previously committed intent. Disabled/deleted rules still drain acknowledged work."
  def publish_one(tenant, id, opts \\ []) do
    with {:ok, head, _} <- Repository.load(tenant, id, opts) do
      case Map.get(head, "recording_queue", []) do
        [] ->
          {:ok, :idle}

        [ref | _] ->
          publish_checked(tenant, id, ref, opts)
      end
    end
  end

  defp publish_checked(tenant, id, ref, opts) do
    case publish(tenant, id, ref, opts) do
      {:error, reason} = error ->
        Repository.recording_error(tenant, id, ref, error_code(reason), opts)
        error

      result ->
        result
    end
  end

  @doc """
  Drain at most eight results or two seconds (`:drain_ms`) of new work; each
  publication is independently durable.
  """
  def drain(tenant, id, opts \\ []) do
    drain(tenant, id, opts, 8, System.monotonic_time(:millisecond) + Keyword.get(opts, :drain_ms, 2000), 0)
  end

  defp drain(_tenant, _id, _opts, 0, _deadline, count), do: {:ok, count}

  defp drain(tenant, id, opts, remaining, deadline, count) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:ok, count}
    else
      case publish_one(tenant, id, opts) do
        {:ok, :idle} -> {:ok, count}
        {:ok, _} -> drain(tenant, id, opts, remaining - 1, deadline, count + 1)
        error -> error
      end
    end
  end

  defp error_code(reason)
       when reason in [
              :integrity_error,
              :committed_object_unavailable,
              :unsupported_recording_storage,
              :retention_migration_required,
              :retention_capacity,
              :timestamp_too_new,
              :recording_conflict,
              :keyed_content_mismatch
            ], do: Atom.to_string(reason)

  defp error_code(_), do: "storage_unavailable"

  defp publish(tenant, id, ref, opts) do
    with {:ok, object} <- Repository.revision(tenant, id, ref, opts),
         {:ok, records} <- records(object) do
      case append(tenant, records, "pulso-recording-v1-" <> ref["digest"], opts) do
        :ok -> acknowledge(tenant, id, ref, object, "published", opts)
        {:error, :retention_expired} -> acknowledge(tenant, id, ref, object, "expired", opts)
        error -> error
      end
    end
  end

  @doc false
  def state_classification(head) do
    retained? =
      Map.get(head, "recording_queue", []) != [] or Map.get(head, "recording_series", []) != [] or
        not is_nil(head["recording_last"]) or not is_nil(head["recording_error"]) or
        Map.get(head, "recording_fence_ns", "-1") != "-1"

    if retained?, do: Enum.uniq(["alert:record" | head["classification"]]), else: head["classification"]
  end

  @doc false
  def event_classification(%{"type" => "recording_expired"} = event),
    do: Enum.uniq(["alert:record" | event["classification"]])

  def event_classification(event), do: event["classification"]

  defp acknowledge(tenant, id, ref, object, status, opts) do
    progress = %{"status" => status, "timestamp_ns" => object["timestamp_ns"], "digest" => ref["digest"]}

    event =
      if status == "expired" do
        %{
          "type" => "recording_expired",
          "recording" => ref,
          "revision" => object["revision"],
          "classification" => ["alert:record"],
          "timestamp_ns" => Integer.to_string(System.system_time(:nanosecond)),
          "actor" => %{"id" => "pulso-recording-publisher", "type" => "service"}
        }
      end

    with :ok <-
           Repository.ack_recording(
             tenant,
             id,
             ref,
             Keyword.merge(opts, recording_progress: progress, recording_event: event)
           ),
         do: {:ok, if(status == "expired", do: :expired, else: :published)}
  end

  defp records(%{"schema_version" => 1, "timestamp_ns" => timestamp, "samples" => samples})
       when is_binary(timestamp) and is_list(samples) and (is_list(samples) and samples != []) and
              length(samples) <= @max_samples * 2 do
    with {ns, ""} <- Integer.parse(timestamp),
         true <- ns in 0..9_223_372_036_854_775_807 do
      Enum.reduce_while(samples, {:ok, []}, &record(&1, &2, ns))
      |> case do
        {:ok, records} -> {:ok, Enum.reverse(records)}
        error -> error
      end
    else
      _ -> {:error, :integrity_error}
    end
  end

  defp records(_), do: {:error, :integrity_error}

  defp record(point, {:ok, acc}, ns) do
    with %{"labels" => labels, "value" => value} <- point,
         true <- Rule.labels?(labels),
         {:ok, number} <- number(point, value) do
      {:cont, {:ok, [%MetricSample{labels: labels, timestamp_ns: ns, value: number} | acc]}}
    else
      _ -> {:halt, {:error, :integrity_error}}
    end
  end

  defp number(%{"stale" => true}, "NaN"), do: {:ok, :stale}
  defp number(%{"stale" => _}, _), do: :error
  defp number(_point, value), do: FloatParser.parse(value)

  defp append(tenant, records, key, opts) do
    # Only the durable, keyed adapter is supported in production. Test injection
    # is private to callers, never a rule field, binding, API argument or URL.
    case Keyword.get(opts, :recording_append) do
      fun when is_function(fun, 3) ->
        fun.(tenant, records, idempotency_key: key)

      nil ->
        if Storage.adapter() == S3,
          do: Storage.append(:metrics, tenant, records, idempotency_key: key),
          else: {:error, :unsupported_recording_storage}
    end
  catch
    _, _ -> {:error, :storage_unavailable}
  end
end
