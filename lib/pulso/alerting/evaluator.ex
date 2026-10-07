defmodule Pulso.Alerting.Evaluator do
  @moduledoc "Bounded native Prometheus threshold evaluation with rule-head fencing."
  alias Pulso.Alerting.{Lifecycle, Principal, Repository}
  alias Pulso.PromQL.Evaluator

  def evaluate(principal, id, opts \\ []) do
    with :ok <- Principal.authorize(principal, "alert:evaluate"),
         :ok <- Principal.authorize(principal, "alert:read"),
         {:ok, head, etag} <- Repository.load(principal.tenant, id, opts),
         :ok <- Principal.authorize(principal, "alert:evaluate", head["classification"]),
         "active" <- head["state"],
         {:ok, snapshot} <- Repository.revision(principal.tenant, id, head["revision"], opts),
         %{"kind" => "promql_threshold"} = rule <- snapshot["rule"] do
      now =
        Keyword.get_lazy(opts, :timestamp_ns, fn ->
          period = rule["cadence_ms"] * 1_000_000
          div(System.system_time(:nanosecond), period) * period
        end)

      previous = String.to_integer(head["completed_at_ns"])

      if now <= previous or (previous >= 0 and now - previous < rule["cadence_ms"] * 1_000_000) do
        {:error, :not_due}
      else
        evaluate_due(principal, id, rule, {head, etag}, now, opts)
      end
    else
      "disabled" -> {:error, :rule_disabled}
      "deleted" -> {:error, :rule_disabled}
      {:error, _} = error -> error
      _ -> {:error, :unsupported_rule}
    end
  end

  defp evaluate_due(principal, id, rule, {head, etag}, now, opts) do
    started = System.monotonic_time(:millisecond)
    result = query(rule, principal.tenant, now, opts)

    if System.monotonic_time(:millisecond) - started >= rule["cadence_ms"],
      do: {:error, :evaluation_skipped},
      else: commit(result, principal, id, rule, head, etag, now, opts)
  end

  def preview(principal, id, opts \\ []) do
    with :ok <- Principal.authorize(principal, "alert:preview"),
         {:ok, current} <- Pulso.Alerting.get(principal, id, opts),
         %{"kind" => "promql_threshold"} = rule <- current["rule"],
         {:ok, samples} <-
           query(rule, principal.tenant, System.system_time(:nanosecond), Keyword.put(opts, :query_class, :promql)),
         {:ok, instances, events, health} <- Lifecycle.step(rule, %{}, samples, System.system_time(:nanosecond)) do
      {:ok, %{"instances" => instances, "transitions" => events, "health" => health}}
    else
      {:error, _} = error -> error
      _ -> {:error, :unsupported_rule}
    end
  end

  defp query(rule, tenant, now, opts) do
    fun = Keyword.get(opts, :query, &Evaluator.query/3)

    case fun.(rule["query"], tenant, %{end_ts_ns: now, query_class: Keyword.get(opts, :query_class, :alerting)}) do
      {:ok, %{"data" => %{"resultType" => "vector", "result" => samples}}} -> {:ok, samples}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_query_result}
    end
  end

  defp commit({:error, reason}, _principal, _id, _rule, _head, _etag, _now, _opts)
       when reason in [
              :query_overloaded,
              :query_timeout,
              :query_resource_limit,
              :query_scan_limit,
              :query_sample_limit,
              :query_work_limit,
              :query_result_limit
            ], do: {:error, :evaluation_skipped}

  defp commit(result, principal, id, rule, head, etag, now, opts) do
    {instances, events, health} =
      case result do
        {:ok, samples} ->
          case Lifecycle.step(rule, head["instances"], samples, now) do
            {:ok, instances, events, health} -> {instances, events, health}
            {:error, _} -> {head["instances"], [], "error"}
          end

        {:error, _} ->
          {head["instances"], [], "error"}
      end

    events =
      Enum.map(
        events,
        &Map.merge(&1, %{
          "revision" => head["revision"],
          "classification" => head["classification"],
          "actor" => %{"id" => principal.id, "type" => principal.type}
        })
      )

    events =
      if health == head["health"] do
        events
      else
        events ++
          [
            %{
              "type" => "health_changed",
              "health" => health,
              "revision" => head["revision"],
              "classification" => head["classification"],
              "timestamp_ns" => Integer.to_string(now),
              "actor" => %{"id" => principal.id, "type" => principal.type}
            }
          ]
      end

    with {:ok, next} <-
           Repository.checkpoint(
             principal.tenant,
             id,
             {head, etag},
             %{
               revision: head["revision"]["digest"],
               timestamp: now,
               instances: instances,
               health: health,
               events: events
             },
             opts
           ),
         do: {:ok, Map.take(next, ~w(id generation state completed_at_ns instances health))}
  end
end
