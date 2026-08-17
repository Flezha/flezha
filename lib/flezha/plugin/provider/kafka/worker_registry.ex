defmodule Flezha.Plugin.Provider.Kafka.WorkerRegistry do
  @moduledoc """
  Refcounted, per-cluster shared-worker registry for the hardened Kafka producer.

  Two plugin instances configured against the same connection identity (hosts +
  ssl_options + auth) acquire the SAME `KafkaEx.Client` worker pid; the
  worker is stopped after a 5-second linger once the last owner releases it.

  Design:

    * `acquire/2` is synchronous, returns `{:ok, worker_pid, ref}`. The `ref`
      is opaque — pass it back to `release/2` from the same plugin lifecycle.
    * The caller process is monitored. If the owner dies without calling
      `release/2`, the registry treats the DOWN as an implicit release.
    * Workers are monitored too. If a worker dies (broker partition restart,
      kafka_ex internal crash, OS suspend leaving the socket half-dead), the
      registry sends `{:kafka_worker_dead, ref, reason}` to every caller that
      still holds a live token BEFORE clearing the entry, then snapshots the
      entry's `refs` count into `state.pending_refs` so the upcoming swap can
      restore the same number of seats.
    * `swap/2` is the recovery API consumed by `ProducerWorker`. It drops
      the dead token, then either re-attaches to an entry another swap
      already recreated (no refs change — the seat is inherited) or starts
      a fresh worker and inherits `entry.refs` from the snapshot. The
      snapshot is dropped after `:linger_ms` (`:linger_expired_pending_refs`)
      to bound memory across genuinely-down brokers; callers whose swap
      arrives after expiry receive `{:error, :no_pending_swap}` and SHOULD
      fall back to `acquire/2` with their stored identity.
    * Linger guards against churn during plugin restart (acquire -> release ->
      acquire within milliseconds reuses the same pid).
  """

  use GenServer

  require Logger

  @default_linger_ms 5_000

  @type host :: {binary(), pos_integer()}
  @type identity :: %{
          required(:hosts) => [host()],
          optional(:ssl_options) => term(),
          optional(:use_ssl) => boolean(),
          optional(:auth) => term(),
          optional(:metadata_update_interval) => pos_integer()
        }
  @type ref :: {term(), reference()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @spec acquire(identity(), GenServer.server()) :: {:ok, pid(), ref()} | {:error, term()}
  def acquire(identity, server \\ __MODULE__) do
    GenServer.call(server, {:acquire, identity, self()}, 30_000)
  end

  @spec release(ref(), GenServer.server()) :: :ok
  def release(ref, server \\ __MODULE__) do
    GenServer.call(server, {:release, ref})
  end

  @doc """
  In-place re-acquire after a `{:kafka_worker_dead, ref, reason}` notification.

  Drops the dead `old_ref`'s token bookkeeping and either attaches to the
  entry another caller's swap already recreated (refcount unchanged — the
  caller is filling a seat inherited from the snapshot) or starts a fresh
  worker for the same canonical identity and seeds its `refs` from
  `state.pending_refs[key]`.

  Returns `{:error, :no_pending_swap}` if the snapshot has already expired
  (the caller SHOULD then fall back to `acquire/2` with its stored identity).
  """
  @spec swap(ref(), GenServer.server()) :: {:ok, pid(), ref()} | {:error, term()}
  def swap(old_ref, server \\ __MODULE__) do
    GenServer.call(server, {:swap, old_ref, self()}, 30_000)
  end

  @doc """
  Force a fresh broker connection for the identity behind `ref`.

  A clean broker restart leaves the shared `KafkaEx.Client` alive but
  permanently returning retriable produce errors, so the worker-DOWN
  recovery path never fires. `refresh/1` stops that client deliberately,
  which trips the registry's own worker monitor and drives the SAME
  `{:kafka_worker_dead, ref, reason}` fan-out + `pending_refs` snapshot
  every holder already swaps on — a brand-new client process reconnects
  fresh, refcount inherited unchanged.

  Returns `:ok` once the stop is issued, or `{:error, :no_entry}` if the
  token's entry is already gone (a concurrent DOWN won the race — recovery
  is already in flight, so the caller needs no further action).
  """
  @spec refresh(ref(), GenServer.server()) :: {:ok, pid()} | {:error, term()}
  def refresh(ref, server \\ __MODULE__) do
    GenServer.call(server, {:refresh, ref}, 30_000)
  end

  @doc """
  Unconditionally replace the shared client for `identity` with a freshly
  started one and return its pid. Recreates the entry if it was already torn
  down (a dead client's DOWN clears the entry, and the swap notification may
  never reach a stalled caller). This is the reliable recovery path — it does
  not depend on caller tokens or the monitor-DOWN swap fan-out.
  """
  @spec force_replace(identity(), GenServer.server()) :: {:ok, pid()} | {:error, term()}
  def force_replace(identity, server \\ __MODULE__) do
    GenServer.call(server, {:force_replace, identity}, 30_000)
  end

  @doc false
  @spec inspect_state(GenServer.server()) :: map()
  def inspect_state(server \\ __MODULE__) do
    GenServer.call(server, :inspect_state)
  end

  @impl true
  def init(opts) do
    # Clients are start_link'd into this process; trap exits so a client that
    # dies (e.g. fails to connect mid broker-restart) becomes a message handled
    # via the monitor DOWN path instead of crashing the whole registry.
    Process.flag(:trap_exit, true)

    {:ok,
     %{
       starter: Keyword.get(opts, :starter, &default_start/1),
       stopper: Keyword.get(opts, :stopper, &default_stop/1),
       linger_ms: Keyword.get(opts, :linger_ms, @default_linger_ms),
       entries: %{},
       tokens: %{},
       caller_monitors: %{},
       worker_monitors: %{},
       pending_refs: %{}
     }}
  end

  @impl true
  def handle_call({:acquire, identity, caller}, _from, state) do
    key = canonical_key(identity)

    case Map.get(state.entries, key) do
      nil ->
        case state.starter.(identity) do
          {:ok, pid} when is_pid(pid) ->
            state = install_worker(state, key, identity, pid, 1, MapSet.new())
            {token, state} = register_owner(state, key, caller)
            {:reply, {:ok, pid, {key, token}}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}

          other ->
            {:reply, {:error, {:unexpected_start_result, other}}, state}
        end

      %{linger: linger_timer} = entry ->
        if linger_timer, do: Process.cancel_timer(linger_timer)
        entry = %{entry | refs: entry.refs + 1, linger: nil}
        state = %{state | entries: Map.put(state.entries, key, entry)}
        {token, state} = register_owner(state, key, caller)
        {:reply, {:ok, entry.pid, {key, token}}, state}
    end
  end

  def handle_call({:release, {_key, token}}, _from, state) do
    {state, key} = drop_token(state, token, nil)
    state = if key, do: decrement_refs(state, key), else: state
    {:reply, :ok, state}
  end

  def handle_call({:swap, {key, old_token}, caller}, _from, state) do
    {state, _} = drop_token(state, old_token, key)

    case Map.get(state.entries, key) do
      %{pid: pid} ->
        {token, state} = register_owner(state, key, caller)
        {:reply, {:ok, pid, {key, token}}, state}

      nil ->
        case Map.pop(state.pending_refs, key) do
          {{identity, count}, pending_refs} ->
            state = %{state | pending_refs: pending_refs}

            case state.starter.(identity) do
              {:ok, pid} when is_pid(pid) ->
                state = install_worker(state, key, identity, pid, count, MapSet.new())
                {token, state} = register_owner(state, key, caller)
                {:reply, {:ok, pid, {key, token}}, state}

              {:error, reason} ->
                state = restore_pending(state, key, identity, count)
                {:reply, {:error, reason}, state}

              other ->
                state = restore_pending(state, key, identity, count)
                {:reply, {:error, {:unexpected_start_result, other}}, state}
            end

          {nil, _} ->
            {:reply, {:error, :no_pending_swap}, state}
        end
    end
  end

  def handle_call({:refresh, {key, req_token}}, _from, state) do
    case Map.get(state.entries, key) do
      %{pid: old_pid, monitor: old_mon, callers: callers, identity: identity} = entry ->
        # A clean broker restart never kills the shared client, so the
        # monitor-DOWN swap path never fires. Replace the client in place and
        # explicitly notify every holder to re-acquire the fresh pid. Relying on
        # a self-inflicted DOWN loses caller tokens under refresh churn and
        # silently strands producers on a dead client.
        Process.demonitor(old_mon, [:flush])
        _ = state.stopper.(old_pid)

        case state.starter.(identity) do
          {:ok, new_pid} when is_pid(new_pid) ->
            wmref = Process.monitor(new_pid)

            state = %{
              state
              | entries: Map.put(state.entries, key, %{entry | pid: new_pid, monitor: wmref}),
                worker_monitors:
                  state.worker_monitors |> Map.delete(old_mon) |> Map.put(wmref, key)
            }

            # The calling worker rebinds directly from the returned pid; the
            # OTHER holders still cache the dead pid, so notify them to swap.
            Enum.each(callers, fn {caller_pid, ctoken} ->
              if ctoken != req_token do
                send(caller_pid, {:kafka_worker_dead, {key, ctoken}, :broker_refresh})
              end
            end)

            {:reply, {:ok, new_pid}, state}

          other ->
            # Could not start a replacement (rare — start is lazy). Drop the
            # entry and snapshot refs so the notified callers rebuild on swap.
            Logger.warning(
              "[Kafka.WorkerRegistry] refresh restart failed for #{inspect(key)}: " <>
                "#{inspect(other)}; tearing down so callers rebuild"
            )

            if MapSet.size(entry.callers) > 0 do
              Process.send_after(self(), {:linger_expired_pending_refs, key}, state.linger_ms)
            end

            state = %{
              state
              | entries: Map.delete(state.entries, key),
                worker_monitors: Map.delete(state.worker_monitors, old_mon),
                pending_refs: Map.put(state.pending_refs, key, {entry.identity, entry.refs})
            }

            Enum.each(callers, fn {caller_pid, ctoken} ->
              send(caller_pid, {:kafka_worker_dead, {key, ctoken}, :broker_refresh})
            end)

            {:reply, {:error, :restart_failed}, state}
        end

      nil ->
        {:reply, {:error, :no_entry}, state}
    end
  end

  def handle_call({:force_replace, identity}, _from, state) do
    key = canonical_key(identity)
    prev = Map.get(state.entries, key)

    state =
      case prev do
        %{monitor: mon, pid: old_pid} ->
          Process.demonitor(mon, [:flush])
          _ = state.stopper.(old_pid)
          %{state | worker_monitors: Map.delete(state.worker_monitors, mon)}

        nil ->
          state
      end

    # The broker DNS entry disappears while its container is down, so a start
    # here can crash (`:nxdomain`). Keep the registry alive — the caller stays
    # on its old client and retries until the broker (and its DNS) is back.
    started =
      try do
        state.starter.(identity)
      catch
        kind, reason -> {:error, {:start_crashed, kind, reason}}
      end

    case started do
      {:ok, new_pid} when is_pid(new_pid) ->
        refs = if prev, do: prev.refs, else: 1
        callers = if prev, do: prev.callers, else: MapSet.new()
        state = install_worker(state, key, identity, new_pid, refs, callers)
        {:reply, {:ok, new_pid}, state}

      other ->
        {:reply, {:error, {:start_failed, other}}, state}
    end
  end

  def handle_call(:inspect_state, _from, state) do
    summary =
      Map.new(state.entries, fn {key, entry} ->
        {key,
         %{
           pid: entry.pid,
           refs: entry.refs,
           linger?: not is_nil(entry.linger),
           callers: MapSet.size(entry.callers)
         }}
      end)

    {:reply, summary, state}
  end

  @impl true
  def handle_info({:linger_expired, key, pid}, state) do
    case Map.get(state.entries, key) do
      %{pid: ^pid, refs: 0, monitor: wmref} ->
        Process.demonitor(wmref, [:flush])
        _ = state.stopper.(pid)

        {:noreply,
         %{
           state
           | entries: Map.delete(state.entries, key),
             worker_monitors: Map.delete(state.worker_monitors, wmref)
         }}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:linger_expired_pending_refs, key}, state) do
    {:noreply, %{state | pending_refs: Map.delete(state.pending_refs, key)}}
  end

  # Trapped client EXITs are handled via the monitor DOWN path; ignore here.
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    cond do
      Map.has_key?(state.caller_monitors, ref) ->
        token = Map.fetch!(state.caller_monitors, ref)
        {state, key} = drop_token(state, token, nil)
        state = if key, do: decrement_refs(state, key), else: state
        {:noreply, state}

      Map.has_key?(state.worker_monitors, ref) ->
        key = Map.fetch!(state.worker_monitors, ref)
        entry = Map.fetch!(state.entries, key)

        Logger.warning(
          "[Kafka.WorkerRegistry] worker for #{inspect(key)} went DOWN " <>
            "(reason=#{inspect(reason)}); notifying #{MapSet.size(entry.callers)} " <>
            "active token holder(s) and clearing entry"
        )

        Enum.each(entry.callers, fn {caller_pid, token} ->
          send(caller_pid, {:kafka_worker_dead, {key, token}, reason})
        end)

        if is_reference(entry.linger), do: Process.cancel_timer(entry.linger)

        pending_refs =
          if MapSet.size(entry.callers) > 0 do
            Process.send_after(self(), {:linger_expired_pending_refs, key}, state.linger_ms)
            Map.put(state.pending_refs, key, {entry.identity, entry.refs})
          else
            state.pending_refs
          end

        {:noreply,
         %{
           state
           | entries: Map.delete(state.entries, key),
             worker_monitors: Map.delete(state.worker_monitors, ref),
             pending_refs: pending_refs
         }}

      true ->
        {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.entries, fn {_key, %{pid: pid}} ->
      _ = state.stopper.(pid)
    end)

    :ok
  end

  # Monitor a freshly started worker and register its entry under `key`.
  defp install_worker(state, key, identity, pid, refs, callers) do
    wmref = Process.monitor(pid)

    entry = %{
      pid: pid,
      refs: refs,
      linger: nil,
      monitor: wmref,
      callers: callers,
      identity: identity
    }

    %{
      state
      | entries: Map.put(state.entries, key, entry),
        worker_monitors: Map.put(state.worker_monitors, wmref, key)
    }
  end

  defp register_owner(state, key, caller) do
    token = make_ref()
    cmref = Process.monitor(caller)

    state = %{
      state
      | tokens: Map.put(state.tokens, token, {key, cmref}),
        caller_monitors: Map.put(state.caller_monitors, cmref, token),
        entries:
          Map.update!(state.entries, key, fn entry ->
            %{entry | callers: MapSet.put(entry.callers, {caller, token})}
          end)
    }

    {token, state}
  end

  defp drop_token(state, token, expected_key) do
    case Map.pop(state.tokens, token) do
      {nil, _} ->
        {state, expected_key}

      {{key, cmref}, tokens} ->
        Process.demonitor(cmref, [:flush])

        state = %{
          state
          | tokens: tokens,
            caller_monitors: Map.delete(state.caller_monitors, cmref)
        }

        state =
          case Map.get(state.entries, key) do
            nil ->
              state

            entry ->
              new_callers =
                entry.callers
                |> Enum.reject(fn {_pid, tok} -> tok == token end)
                |> MapSet.new()

              %{state | entries: Map.put(state.entries, key, %{entry | callers: new_callers})}
          end

        {state, key}
    end
  end

  defp decrement_refs(state, key) do
    case Map.get(state.entries, key) do
      nil ->
        state

      entry ->
        new_refs = max(entry.refs - 1, 0)

        entry =
          if new_refs == 0 do
            timer_ref =
              Process.send_after(
                self(),
                {:linger_expired, key, entry.pid},
                state.linger_ms
              )

            %{entry | refs: 0, linger: timer_ref}
          else
            %{entry | refs: new_refs}
          end

        %{state | entries: Map.put(state.entries, key, entry)}
    end
  end

  defp restore_pending(state, key, identity, count) do
    Process.send_after(self(), {:linger_expired_pending_refs, key}, state.linger_ms)
    %{state | pending_refs: Map.put(state.pending_refs, key, {identity, count})}
  end

  defp canonical_key(identity) do
    hosts =
      identity
      |> Map.fetch!(:hosts)
      |> Enum.map(fn {h, p} -> {to_string(h), p} end)
      |> Enum.sort()

    # `auth_key` is the provider-computed fingerprint of the resolved SASL
    # config — the registry groups on it without ever holding the secret.
    auth_key = Map.get(identity, :auth_key) || :erlang.phash2(Map.get(identity, :auth))

    {hosts, :erlang.phash2(Map.get(identity, :ssl_options)), auth_key}
  end

  defp default_start(identity) do
    worker_opts =
      [uris: Map.fetch!(identity, :hosts)]
      |> maybe_put(:ssl_options, Map.get(identity, :ssl_options))
      |> maybe_put(:use_ssl, Map.get(identity, :use_ssl))
      |> maybe_put(:auth, resolve_identity_auth(identity))
      |> maybe_put(:metadata_update_interval, Map.get(identity, :metadata_update_interval))

    KafkaEx.start_link_worker(:no_name, worker_opts)
  end

  # Credentials ride `auth_fun` (a closure the provider built) and are
  # resolved only here, at client start — never stored in registry state
  # beyond the closure itself.
  defp resolve_identity_auth(identity) do
    case Map.get(identity, :auth_fun) do
      fun when is_function(fun, 0) -> fun.()
      _ -> Map.get(identity, :auth)
    end
  end

  defp default_stop(pid) when is_pid(pid) do
    KafkaEx.stop_worker(pid)
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
