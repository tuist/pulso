defmodule Pulso.Storage.S3.ManifestOwner do
  @moduledoc """
  Per-`(tenant, signal)` manifest writer.

  Every S3 CAS on a manifest funnels through one owner process — so
  there is no local write contention on that manifest object, and the
  S3 conditional PUT only ever fights genuine cross-node races (not
  in-process ones).

  ## Write coalescing

  Multiple ingester calls that arrive close together are batched into
  one CAS. Each caller enqueues its segment(s) and blocks in
  `GenServer.call/3`; the owner accumulates a pending list, starts a
  short flush timer (default 10ms), and on flush issues **one** CAS
  that folds every pending segment into the manifest. Every waiter is
  replied to once the CAS lands.

  Under steady write load this pins the S3 CAS rate to
  `1 / flush_interval_ms` per tenant regardless of how many concurrent
  ingesters push. Under bursty load the batch cap (default 256) triggers
  an immediate flush without waiting for the timer, so tail latency
  stays bounded.

  ## Conflict recovery

  On a `:precondition_failed` from S3 (another node landed a CAS on the
  same manifest between our GET and PUT), the owner reloads the
  manifest, re-applies its pending segments on top, and retries. Retries
  are bounded and back off before giving up.

  ## First-write migration

  When the owner boots and no manifest exists yet at that key, it
  rebuilds one by LIST-ing the tenant's segment prefix — every
  historical write from before the manifest existed still ends up in
  the manifest on first read.

  ## Backpressure

  Every `register_segments` call checks the owner's mailbox length
  (`Process.info(pid, :message_queue_len)`) before it hands off and
  short-circuits with `{:error, :owner_overloaded}` when we're already
  at or above `max_mailbox` (default 512). This is the explicit
  backpressure signal to the ingester layer above — callers see a
  typed error and can shed load rather than sitting on a 15s
  `GenServer.call` timeout while the mailbox keeps growing. And on
  the timeout path, `call_owner/3` translates
  `exit({:timeout, {GenServer, :call, _}})` into
  `{:error, :timeout}`, so `register_segments/5` honors the typed
  return in its `@spec` in every failure mode. The mailbox-length
  probe is inherently racy (the length may rise between check and
  enqueue), but the race is bounded by the number of concurrent
  callers — not unbounded as the earlier design allowed.
  """

  use GenServer, restart: :transient

  alias Pulso.ObjectStore
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.ManifestRegistry
  alias Pulso.Storage.S3.ManifestSupervisor

  require Logger

  @registry ManifestRegistry
  @default_flush_interval_ms 10
  @default_flush_batch_max 256
  @cas_max_retries 5
  # A cached manifest is served without I/O for this long after its
  # last refresh; beyond it, `ensure_loaded/3` issues a conditional GET
  # to pick up cross-node writes. This is the bound on cross-node
  # freshness in the absence of UDP gossip.
  @default_refresh_stale_ms 1_000
  # Ceiling on the owner's mailbox length. Every `register_segments`
  # call checks the owner's `:message_queue_len` before it hands off
  # and short-circuits with `{:error, :owner_overloaded}` when we're
  # already at or above the ceiling. This is the explicit backpressure
  # signal — callers see a typed error and can shed load rather than
  # sitting on a 15s timeout while the mailbox keeps growing. The
  # check is racy (mailbox length can rise between check and call),
  # but the race is bounded: at worst we overshoot by the number of
  # racing callers, which is `O(ingester concurrency)`, not unbounded.
  @default_max_mailbox 512

  # Idle owners hibernate after this many milliseconds. `:hibernate` GCs
  # the heap and drops any large stack — the owner will still respond to
  # the next call, just after a small wakeup cost. This keeps the
  # per-tenant memory footprint bounded when a tenant goes quiet.
  @idle_hibernate_ms 30_000

  @type opts :: [
          tenant: String.t(),
          signal: String.t(),
          config: map(),
          flush_interval_ms: pos_integer(),
          flush_batch_max: pos_integer()
        ]

  # ---- public API -----------------------------------------------------------

  @doc """
  Register segments in the manifest and wait until the CAS lands.

  Returns `:ok` once the manifest that references every supplied
  segment is durably in S3, `{:error, reason}` otherwise.

  ## Typed error contract

  This function only ever returns `:ok | {:error, term()}`; the
  underlying `GenServer.call/3`'s `exit({:timeout, _})` on call
  timeout is translated to `{:error, :timeout}`, and a mailbox
  saturation to `{:error, :owner_overloaded}`, so the caller can
  shed load without a `try/catch`.
  """
  @spec register_segments(String.t(), String.t(), [Segment.t()], map(), timeout()) ::
          :ok | {:error, term()}
  def register_segments(tenant, signal, segments, config, timeout \\ 15_000)
      when is_binary(tenant) and is_binary(signal) and is_list(segments) and is_map(config) do
    with {:ok, pid} <- ensure_started(tenant, signal, config),
         :ok <- guard_mailbox(pid, config) do
      call_owner(pid, {:register_segments, segments}, timeout)
    end
  end

  # `Process.info(pid, :message_queue_len)` is cheap (one BIF, no
  # message crossing). We use it as an in-flight cap so an ingester
  # burst that has overwhelmed the owner shows up as a typed error
  # to the caller, not as unbounded mailbox growth followed by a 15s
  # `GenServer.call` timeout. The check is racy (the length may rise
  # between check and enqueue), but the race is bounded by the
  # number of concurrent callers — far short of the "unbounded" the
  # earlier design allowed.
  defp guard_mailbox(pid, config) do
    ceiling = Map.get(config, :max_mailbox, @default_max_mailbox)

    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, len} when len >= ceiling ->
        {:error, :owner_overloaded}

      _ ->
        :ok
    end
  end

  # Translate the `GenServer.call/3` timeout `exit` into a typed
  # error tuple so callers can pattern-match on it — a `catch` on
  # every call site would be worse. Any other exit is re-raised
  # because it signals a genuine bug (owner crashed, unknown
  # message shape, etc.) and callers should not swallow it.
  defp call_owner(pid, request, timeout) do
    GenServer.call(pid, request, timeout)
  catch
    :exit, {:timeout, {GenServer, :call, _}} -> {:error, :timeout}
  end

  @doc """
  Look up the manifest, loading it from S3 (or rebuilding from a LIST)
  if the local cache is empty. Never triggers a CAS — pure read.

  Refreshes stale cache entries with a **conditional GET**: when the
  cached entry is older than `refresh_stale_ms` (default 1000ms), we
  issue `get_if_none_match` with the cached ETag. A 304 is a single
  small S3 round-trip and touches the entry's freshness timestamp; a
  200 returns the fresh manifest and updates the cache. This is what
  closes the cross-node freshness gap: a node whose only local writer
  is idle still learns about segments other nodes have committed.

  A stale-refresh failure (network hiccup, S3 throttle) returns the
  stale cached entry rather than failing the query. The next query
  will try again.

  This is also the query-path entry point when a query arrives before
  the owner has been booted (a cold cache after a node restart) —
  that path routes through the owner, which does the initial load.
  """
  @spec ensure_loaded(String.t(), String.t(), map()) ::
          {:ok, ManifestCache.entry()} | {:error, term()}
  def ensure_loaded(tenant, signal, config) when is_binary(tenant) and is_binary(signal) and is_map(config) do
    case ManifestCache.get(tenant, signal) do
      %{} = entry ->
        {:ok, maybe_refresh(entry, tenant, signal, config)}

      nil ->
        with {:ok, pid} <- ensure_started(tenant, signal, config) do
          call_owner(pid, :ensure_loaded, 15_000)
        end
    end
  end

  # A cached entry is considered fresh for `refresh_stale_ms`
  # milliseconds after its last refresh. Beyond that we issue a
  # conditional GET so a manifest another node committed becomes
  # visible without waiting for a local write. The refresh happens on
  # the caller's process (the query path) — not through the owner —
  # because it does not need serialization; `:ets.insert/2` is atomic
  # per key, so two racing refreshers converge to the same fresh
  # value.
  defp maybe_refresh(entry, tenant, signal, config) do
    if stale?(entry, config) do
      refresh(entry, tenant, signal, config)
    else
      entry
    end
  end

  defp stale?(entry, config) do
    interval_ms = Map.get(config, :refresh_stale_ms, @default_refresh_stale_ms)
    System.monotonic_time(:millisecond) - entry.refreshed_at_mono >= interval_ms
  end

  defp refresh(entry, tenant, signal, config) do
    key = Manifest.manifest_key(tenant, signal)

    case Pulso.ObjectStore.get_if_none_match(config, key, entry.etag) do
      :not_modified ->
        # No new writes; just touch the freshness timestamp so we don't
        # keep issuing 304s on every query.
        touched = %{entry | refreshed_at_mono: System.monotonic_time(:millisecond)}
        ManifestCache.put(tenant, signal, touched.manifest, touched.etag)
        touched

      {:ok, new_etag, body} ->
        case Manifest.decode(body) do
          {:ok, manifest} ->
            ManifestCache.put(tenant, signal, manifest, new_etag)

            %{
              manifest: manifest,
              etag: new_etag,
              refreshed_at_mono: System.monotonic_time(:millisecond)
            }

          {:error, _} ->
            # Decoded manifest is garbage — the safe move is to serve
            # the stale entry we already trust rather than start
            # returning empty results because a manifest was corrupted
            # server-side.
            entry
        end

      {:error, _} ->
        # Transient failure (network, throttling, 404 mid-migration).
        # Serve the stale entry; the next query will retry.
        entry
    end
  end

  # ---- supervision helpers --------------------------------------------------

  @doc """
  Look up (or start) the owner process for a given `(tenant, signal)`.
  Starting is idempotent via `Registry`-based `:via` naming — two
  racing starts collapse to one process.
  """
  @spec ensure_started(String.t(), String.t(), map()) ::
          {:ok, pid()} | {:error, term()}
  def ensure_started(tenant, signal, config) do
    case Registry.lookup(@registry, {tenant, signal}) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        opts = [tenant: tenant, signal: signal, config: config]
        spec = %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :transient}

        case DynamicSupervisor.start_child(ManifestSupervisor, spec) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, _} = err -> err
        end
    end
  end

  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts) do
    tenant = Keyword.fetch!(opts, :tenant)
    signal = Keyword.fetch!(opts, :signal)
    name = {:via, Registry, {@registry, {tenant, signal}}}
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # ---- GenServer callbacks --------------------------------------------------

  @impl GenServer
  def init(opts) do
    state = %{
      tenant: Keyword.fetch!(opts, :tenant),
      signal: Keyword.fetch!(opts, :signal),
      config: Keyword.fetch!(opts, :config),
      flush_interval_ms: Keyword.get(opts, :flush_interval_ms, @default_flush_interval_ms),
      flush_batch_max: Keyword.get(opts, :flush_batch_max, @default_flush_batch_max),
      manifest: nil,
      etag: nil,
      loaded?: false,
      pending: [],
      waiters: [],
      timer_ref: nil
    }

    {:ok, state, {:continue, :load}}
  end

  @impl GenServer
  def handle_continue(:load, state) do
    case load_manifest(state) do
      {:ok, manifest, etag} ->
        ManifestCache.put(state.tenant, state.signal, manifest, etag)

        {:noreply, %{state | manifest: manifest, etag: etag, loaded?: true}, @idle_hibernate_ms}

      {:error, reason} ->
        # We do NOT terminate the process — a load failure is transient
        # (network blip, S3 throttle). Callers will see `{:error, reason}`
        # on the first `register_segments`/`ensure_loaded` after this,
        # and retry.
        Logger.warning(
          "manifest load failed tenant=#{state.tenant} signal=#{state.signal} reason=#{inspect(reason)}"
        )

        {:noreply, state, @idle_hibernate_ms}
    end
  end

  @impl GenServer
  def handle_call(:ensure_loaded, _from, state) do
    if state.loaded? do
      entry = %{
        manifest: state.manifest,
        etag: state.etag || "",
        refreshed_at_mono: System.monotonic_time(:millisecond)
      }

      {:reply, {:ok, entry}, state, @idle_hibernate_ms}
    else
      case load_manifest(state) do
        {:ok, manifest, etag} ->
          ManifestCache.put(state.tenant, state.signal, manifest, etag)
          state = %{state | manifest: manifest, etag: etag, loaded?: true}

          entry = %{
            manifest: manifest,
            etag: etag,
            refreshed_at_mono: System.monotonic_time(:millisecond)
          }

          {:reply, {:ok, entry}, state, @idle_hibernate_ms}

        {:error, _} = err ->
          {:reply, err, state, @idle_hibernate_ms}
      end
    end
  end

  def handle_call({:register_segments, segments}, from, state) do
    if state.loaded? do
      enqueue_and_maybe_flush(state, from, segments)
    else
      case load_manifest(state) do
        {:ok, manifest, etag} ->
          ManifestCache.put(state.tenant, state.signal, manifest, etag)

          state
          |> Map.merge(%{manifest: manifest, etag: etag, loaded?: true})
          |> enqueue_and_maybe_flush(from, segments)

        {:error, _} = err ->
          {:reply, err, state, @idle_hibernate_ms}
      end
    end
  end

  @impl GenServer
  def handle_info(:flush, state) do
    do_flush(state)
  end

  def handle_info(:timeout, state) do
    # `:hibernate` compacts the process heap and drops the stack; a
    # manual `garbage_collect/1` beforehand would be redundant.
    {:noreply, state, :hibernate}
  end

  # ---- flush pipeline -------------------------------------------------------

  # Enqueue the caller's segments, register them as a waiter, and — if
  # the batch cap trips — flush immediately instead of waiting for the
  # timer. Under bursty load this bounds tail latency.
  defp enqueue_and_maybe_flush(state, from, segments) do
    state = %{state | pending: state.pending ++ segments, waiters: [from | state.waiters]}

    cond do
      length(state.pending) >= state.flush_batch_max ->
        cancel_timer(state) |> do_flush()

      state.timer_ref == nil ->
        ref = Process.send_after(self(), :flush, state.flush_interval_ms)
        {:noreply, %{state | timer_ref: ref}}

      true ->
        {:noreply, state}
    end
  end

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(%{timer_ref: ref} = state) do
    # If `cancel_timer/1` returns `false`, the timer already fired and
    # the `:flush` message is already in our mailbox. Drain it so we
    # don't fire two flushes back-to-back (a wasted CAS on an empty
    # pending queue, but also a subtle re-entry through `do_flush/1`).
    case Process.cancel_timer(ref) do
      false ->
        receive do
          :flush -> :ok
        after
          0 -> :ok
        end

      _ ->
        :ok
    end

    %{state | timer_ref: nil}
  end

  defp do_flush(%{pending: []} = state) do
    {:noreply, %{state | timer_ref: nil}, @idle_hibernate_ms}
  end

  defp do_flush(state) do
    state = %{state | timer_ref: nil}

    case cas_with_retry(state, state.pending, @cas_max_retries) do
      {:ok, manifest, etag} ->
        ManifestCache.put(state.tenant, state.signal, manifest, etag)
        Enum.each(state.waiters, &GenServer.reply(&1, :ok))

        {:noreply,
         %{
           state
           | manifest: manifest,
             etag: etag,
             loaded?: true,
             pending: [],
             waiters: []
         }, @idle_hibernate_ms}

      {:error, reason} = err ->
        Enum.each(state.waiters, &GenServer.reply(&1, err))

        Logger.warning(
          "manifest CAS gave up tenant=#{state.tenant} signal=#{state.signal} reason=#{inspect(reason)}"
        )

        # Keep the process alive: the next register_segments will start
        # from a clean slate (and if the underlying failure was
        # transient, the next flush will succeed).
        {:noreply, %{state | pending: [], waiters: []}, @idle_hibernate_ms}
    end
  end

  # One CAS attempt, with recovery via reload-and-retry on
  # `:precondition_failed` or `:already_exists` (a first-create race).
  # Every retry rebuilds the merged manifest against the freshest known
  # ETag, so we never overwrite a concurrent writer's segments.
  defp cas_with_retry(_state, _segments, 0), do: {:error, :cas_retries_exhausted}

  defp cas_with_retry(state, segments, retries_left) do
    merged = Manifest.merge(state.manifest, segments)
    payload = merged |> Manifest.encode() |> IO.iodata_to_binary()

    case attempt_cas(state, payload) do
      {:ok, new_etag} -> {:ok, merged, new_etag}
      {:error, :precondition_failed} -> reload_and_retry(state, segments, retries_left)
      {:error, :already_exists} -> reload_and_retry(state, segments, retries_left)
      {:error, _} = err -> err
    end
  end

  # `nil` etag means the owner has never seen this manifest; use
  # `put_if_none_match` so a first create is atomic. Any subsequent
  # update rides `put_if_match` against the ETag we remember.
  defp attempt_cas(state, payload) do
    key = Manifest.manifest_key(state.tenant, state.signal)

    case state.etag do
      nil -> ObjectStore.put_if_none_match(state.config, key, payload)
      etag -> ObjectStore.put_if_match(state.config, key, payload, etag)
    end
  end

  defp reload_and_retry(state, segments, retries_left) do
    case load_manifest(state) do
      {:ok, reloaded_manifest, reloaded_etag} ->
        cas_with_retry(
          %{state | manifest: reloaded_manifest, etag: reloaded_etag},
          segments,
          retries_left - 1
        )

      {:error, _} = err ->
        err
    end
  end

  # ---- initial load / migration ---------------------------------------------

  defp load_manifest(state) do
    key = Manifest.manifest_key(state.tenant, state.signal)

    case ObjectStore.get_if_none_match(state.config, key, nil) do
      {:ok, etag, body} ->
        case Manifest.decode(body) do
          {:ok, manifest} -> {:ok, manifest, etag}
          {:error, _} = err -> err
        end

      {:error, :not_found} ->
        rebuild_from_prefix(state)

      {:error, _} = err ->
        err
    end
  end

  # First-write migration: no manifest exists yet, so LIST the v2 prefix
  # and reconstitute one from the segments that are already in S3. Then
  # PUT it with `put_if_none_match`. If another node beats us to the
  # create, we lose gracefully and reload their version.
  defp rebuild_from_prefix(state) do
    prefix = "tenants/#{state.tenant}/v2/#{state.signal}/"

    with {:ok, keys} <- ObjectStore.list(state.config, prefix) do
      publish_rebuilt_manifest(state, keys)
    end
  end

  defp publish_rebuilt_manifest(state, keys) do
    manifest = Manifest.merge(Manifest.new(), rebuild_segments(keys))
    payload = manifest |> Manifest.encode() |> IO.iodata_to_binary()
    manifest_key = Manifest.manifest_key(state.tenant, state.signal)

    case ObjectStore.put_if_none_match(state.config, manifest_key, payload) do
      {:ok, etag} -> {:ok, manifest, etag}
      {:error, :already_exists} -> reload_after_create_race(state, manifest_key)
      {:error, _} = err -> err
    end
  end

  # Only keys that parse cleanly to `<min_ts>-<max_ts>-…ndjson` get into
  # the rebuilt manifest. Everything else — the manifest itself, future
  # sidecar files (`.bloom`, `.postings`, `.stats`), stray uploads —
  # resolves to `:skip`. Codex flagged the earlier "keep with nil bounds"
  # fallback: the rebuilt manifest would then fail to decode
  # (`Segment.from_wire/1` requires integer bounds), leaving the tenant
  # unrecoverable.
  defp rebuild_segments(keys) do
    Enum.flat_map(keys, fn key ->
      case segment_from_key(key) do
        {:ok, segment} -> [segment]
        :skip -> []
      end
    end)
  end

  defp reload_after_create_race(state, manifest_key) do
    with {:ok, etag, body} <- ObjectStore.get_if_none_match(state.config, manifest_key, nil),
         {:ok, manifest} <- Manifest.decode(body) do
      {:ok, manifest, etag}
    end
  end

  # A rebuild-derived segment gets its bounds from the key format
  # (`<min_ts>-<max_ts>-<suffix>.ndjson`). Anything the writer would
  # never produce — the manifest itself, sidecar indexes, a stray
  # upload — resolves to `:skip` and stays out of the manifest. Nothing
  # else has integer bounds, and the manifest requires them.
  @sort_key_width 20

  @spec segment_from_key(String.t()) :: {:ok, Segment.t()} | :skip
  defp segment_from_key(key) do
    with true <- String.ends_with?(key, ".ndjson"),
         [_, rest] <- String.split(key, "/logs/", parts: 2) do
      parse_bounds(key, rest)
    else
      _ -> :skip
    end
  end

  defp parse_bounds(key, rest) do
    case rest do
      <<min_str::binary-size(@sort_key_width), "-", max_str::binary-size(@sort_key_width), "-", _::binary>> ->
        with {min_ts, ""} <- Integer.parse(min_str),
             {max_ts, ""} <- Integer.parse(max_str) do
          {:ok, Segment.build(key, min_ts, max_ts, 0)}
        else
          _ -> :skip
        end

      _ ->
        :skip
    end
  end
end
