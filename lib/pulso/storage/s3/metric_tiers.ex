defmodule Pulso.Storage.S3.MetricTiers do
  @moduledoc """
  Hour finalization of metric segments.

  Once a UTC hour has settled, `finalize/3` reads every segment overlapping it
  and replaces them, in one conditional manifest transition, with:

    * the hour's rows grouped by metric name into `-final-` segments of about
      `:target_rows` rows, so a selector's manifest name summary skips every
      other metric's rows;
    * the rows outside the hour from segments straddling its edges, written
      back as raw `-edge-` segments (distinct from ingested keys, so manifest
      recovery never adopts them);
    * hourly rollup objects (`-rollup-`), one row per series with float
      samples in the hour, that PromQL composes into long range functions.

  Every row is kept, including duplicates and typed metadata; nothing is
  aggregated away. Late writes into a finalized hour make it raw again for
  queries (they ignore its rollups while any raw segment overlaps it) until
  the next finalization rewrites it. Prefixes with enforced retention (format
  3 manifests) are not finalized yet.

  Options: `:settle_ms` (600_000), `:target_rows` (32_768),
  `:rollup_target_rows` (65_536), `:max_rows` (4_000_000), `:max_sources`
  (8192), `:grace_ms` (3_600_000), `:concurrency` (8).
  """

  alias Pulso.Codec.NIF
  alias Pulso.ObjectStore
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.JournalManifest
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.ManifestOwner
  alias Pulso.Storage.S3.Retention

  @hour_ns 3_600_000_000_000

  @doc "Start of the UTC hour holding `ts`."
  def hour(ts), do: Integer.floor_div(ts, @hour_ns) * @hour_ns
  def hour_ns, do: @hour_ns

  @doc """
  Finalize the oldest settled hour that still has raw segments among the
  bounded compaction candidates, except `:skip_hours`. Returns
  `{:ok, %{hour: start | nil, ...}}`, or `{:error, {hour, reason}}`.
  """
  def finalize(tenant, config, opts \\ []) do
    Pulso.Metrics.measure(:compaction, "finalize", fn -> do_finalize(tenant, config, opts) end, fn
      {:ok, %{sources: count}} -> %{segments: count}
      _ -> %{}
    end)
  end

  defp do_finalize(tenant, config, opts) do
    with :ok <- S3.validate_tenant(tenant),
         {:ok, root, _etag} <- Retention.load(tenant, "metrics", config) do
      case root.version do
        3 -> {:ok, %{hour: nil, reason: :retention_managed}}
        _ -> finalize_root(root, tenant, config, opts)
      end
    end
  end

  defp finalize_root(root, tenant, config, opts) do
    settle = Keyword.get(opts, :settle_ms, 600_000) * 1_000_000
    settled_before = System.os_time(:nanosecond) - settle
    # Hours that failed recently, so one oversized hour cannot block the rest.
    skip = Keyword.get(opts, :skip_hours, MapSet.new())
    # Hours wholly below the retention floor are expiring, not worth rewriting.
    floor = JournalManifest.retention_floor(root) || 0

    with {:ok, candidates} <- candidates(root, tenant, config) do
      candidates.segments
      |> Enum.filter(&(Segment.metric_tier(&1) == :raw and is_integer(&1.min_ts) and is_integer(&1.max_ts)))
      |> Enum.flat_map(fn s -> Enum.to_list(hour(s.min_ts)..hour(s.max_ts)//@hour_ns) end)
      |> Enum.filter(&(&1 + @hour_ns <= settled_before and &1 + @hour_ns > floor and not MapSet.member?(skip, &1)))
      |> Enum.min(fn -> nil end)
      |> finalize_oldest(root, tenant, config, opts)
    end
  end

  defp finalize_oldest(nil, _root, _tenant, _config, _opts), do: {:ok, %{hour: nil}}

  defp finalize_oldest(start, root, tenant, config, opts) do
    case finalize_hour(root, start, tenant, config, opts) do
      {:error, reason} -> {:error, {start, reason}}
      result -> result
    end
  end

  defp candidates(%{version: 4} = root, tenant, config), do: JournalManifest.candidates(root, tenant, "metrics", config)
  defp candidates(root, _tenant, _config), do: {:ok, root}

  @doc false
  # Finalize one hour now, whatever its age (tests and maintenance).
  def finalize_hour(tenant, start, config, opts) do
    with {:ok, root, _etag} <- Retention.load(tenant, "metrics", config) do
      if root.version == 3,
        do: {:ok, %{hour: nil, reason: :retention_managed}},
        else: finalize_hour(root, hour(start), tenant, config, opts)
    end
  end

  defp finalize_hour(root, start, tenant, config, opts) do
    finish = start + @hour_ns - 1

    with {:ok, samples} <- segments(root, start, finish, :samples, tenant, config),
         {:ok, rollups} <- segments(root, start, finish, :rollup, tenant, config),
         sources = samples ++ rollups,
         :ok <- check_sources(sources, opts),
         :ok <- if(samples == [], do: {:empty, start}, else: :ok),
         {:ok, finished} <- build(config, start, samples, opts),
         {:ok, outputs} <- upload(tenant, config, finished) do
      grace = Keyword.get(opts, :grace_ms, 3_600_000)

      case publish(tenant, config, Enum.map(sources, & &1.key), outputs, grace) do
        :ok ->
          {:ok,
           %{
             hour: start,
             sources: length(sources),
             finals: Enum.count(outputs, &(Segment.metric_tier(&1) == :final)),
             rollups: Enum.count(outputs, &(Segment.metric_tier(&1) == :rollup))
           }}

        {:error, reason} = error when reason in [:compaction_conflict, :cas_retries_exhausted] ->
          discard(config, outputs, error)

        {:error, {:unpublished, reason}} ->
          discard(config, outputs, {:error, reason})

        error ->
          error
      end
    else
      {:empty, start} -> {:ok, %{hour: start, sources: 0, finals: 0, rollups: 0}}
      error -> error
    end
  end

  defp check_sources(sources, opts) do
    if length(sources) > Keyword.get(opts, :max_sources, 8192), do: {:error, :too_many_sources}, else: :ok
  end

  # Every active segment of `tier` overlapping the hour, from manifest metadata only.
  defp segments(%{version: 4} = root, start, finish, tier, tenant, config) do
    opts = [start_ts: start, end_ts: finish, metrics_filter: Segment.metrics_filter([], tier)]
    JournalManifest.query(root, tenant, "metrics", opts, config)
  end

  defp segments(root, start, finish, tier, _tenant, _config) do
    filter = Segment.metrics_filter([], tier)
    {:ok, root |> Manifest.prune_by_time(start, finish) |> Enum.filter(&Segment.matches_metrics?(&1, filter))}
  end

  defp build(config, start, sources, opts) do
    expected = Enum.sum_by(sources, &(&1.row_count || 0))
    max_rows = Keyword.get(opts, :max_rows, 4_000_000)

    with :ok <- NIF.ensure_metric_tier_api!(),
         {:ok, finalizer} <- NIF.metric_finalize_new(start, start + @hour_ns, max_rows, expected),
         :ok <- add_sources(finalizer, config, sources, opts) do
      finish_build(finalizer, opts)
    end
  end

  defp add_sources(finalizer, config, sources, opts) do
    sources
    |> Pulso.Runtime.Task.async_stream(&ObjectStore.get(config, &1.key),
      max_concurrency: Keyword.get(opts, :concurrency, 8),
      ordered: false,
      timeout: :infinity
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, {:ok, blob}}, :ok -> blob |> add_source(finalizer) |> then(&{if(&1 == :ok, do: :cont, else: :halt), &1})
      {:ok, {:error, _} = error}, :ok -> {:halt, error}
    end)
  end

  defp add_source(blob, finalizer) do
    case NIF.metric_finalize_add(finalizer, blob) do
      {:ok, _rows} -> :ok
      {:error, _} = error -> error
      :fallback -> {:error, :invalid_segment}
    end
  end

  defp finish_build(finalizer, opts) do
    target = Keyword.get(opts, :target_rows, 32_768)

    case NIF.metric_finalize_finish(finalizer, target, Keyword.get(opts, :rollup_target_rows, 65_536), 128) do
      {:ok, finals, before, later, rollups} ->
        {:ok, Enum.map(finals, &{:final, &1}) ++ edge(before) ++ edge(later) ++ Enum.map(rollups, &{:rollup, &1})}

      {:error, _} = error ->
        error

      :fallback ->
        {:error, {:encode_failed, :parquet_encoder_rejected_input}}
    end
  end

  defp edge(nil), do: []
  defp edge(output), do: [{:raw, output}]

  defp upload(tenant, config, finished) do
    Enum.reduce_while(finished, {:ok, []}, fn {tier, {payload, min_ts, max_ts, rows, labels}}, {:ok, acc} ->
      segment = output_segment(tenant, tier, payload, min_ts, max_ts, rows, labels)

      case ObjectStore.put(config, segment.key, payload) do
        {:ok, _etag} -> {:cont, {:ok, [segment | acc]}}
        {:error, _} = error -> {:halt, discard(config, acc, error)}
      end
    end)
  end

  defp output_segment(tenant, tier, payload, min_ts, max_ts, rows, labels) do
    suffix = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
    key = S3.object_key(tenant, "metrics", min_ts, max_ts, suffix, nil)
    marker = %{final: "-final-", rollup: "-rollup-", raw: "-edge-"}[tier]
    key = Path.join(Path.dirname(key), String.replace(Path.basename(key), "-rand-", marker))

    key
    |> Segment.build(min_ts, max_ts, rows, byte_size(payload))
    |> Segment.summarize_metrics(Enum.map(labels, &%{labels: &1}))
  end

  # Ambiguous failures keep their uploads: the manifest may reference them.
  defp discard(config, outputs, error) do
    Enum.each(outputs, &ObjectStore.delete(config, &1.key))
    error
  end

  defp publish(tenant, config, sources, outputs, grace) do
    marker = List.first(outputs)

    transaction = fn current ->
      case published?(current, marker, tenant, config) do
        {:ok, true} -> {:done, :ok}
        {:ok, false} -> current |> prepare(sources, outputs, grace, tenant, config) |> write()
        error -> error
      end
    end

    case ManifestOwner.transact(tenant, "metrics", config, transaction) do
      {:ok, :ok} -> :ok
      {:error, :cas_retries_exhausted} = error -> resolve_exhausted(tenant, config, marker, error)
      error -> error
    end
  end

  # The last conditional response may have been lost after success.
  defp resolve_exhausted(tenant, config, marker, error) do
    with {:ok, current, _etag} <- Retention.load(tenant, "metrics", config),
         {:ok, published} <- published?(current, marker, tenant, config),
         do: if(published, do: :ok, else: error)
  end

  defp write({:ok, updated}), do: {:write, updated, updated |> Manifest.encode() |> IO.iodata_to_binary(), :ok}
  defp write({:ok, sealed, payload}), do: {:write, sealed, payload, :ok}
  defp write({:error, reason}), do: {:error, {:unpublished, reason}}

  defp prepare(%{version: 4} = root, sources, outputs, grace, tenant, config) do
    deadline = System.system_time(:millisecond) + grace

    with {:ok, updated} <- JournalManifest.replace_all(root, sources, outputs, deadline, tenant, "metrics", config),
         {:ok, {sealed, payload, _}} <- JournalManifest.seal(updated, tenant, "metrics", config),
         do: {:ok, sealed, payload}
  end

  defp prepare(%{version: 3}, _sources, _outputs, _grace, _tenant, _config), do: {:error, :retention_managed}

  defp prepare(root, sources, outputs, grace, _tenant, _config),
    do: Manifest.replace_all(root, sources, outputs, System.system_time(:millisecond) + grace)

  defp published?(_root, nil, _tenant, _config), do: {:ok, false}

  defp published?(%{version: 4} = root, segment, tenant, config) do
    with {:ok, found} <- JournalManifest.lookup(root, segment, tenant, "metrics", config), do: {:ok, found != :absent}
  end

  defp published?(%{version: 3}, _segment, _tenant, _config), do: {:ok, false}

  defp published?(root, segment, _tenant, _config),
    do: {:ok, Enum.any?(root.segments, &(&1.key == segment.key)) or Map.has_key?(root.retired, segment.key)}
end
