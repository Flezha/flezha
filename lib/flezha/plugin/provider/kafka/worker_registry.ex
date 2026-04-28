defmodule Flezha.Plugin.Provider.Kafka.WorkerRegistry do
  @moduledoc """
  Refcounted, per-cluster shared-worker registry for the hardened Kafka producer.

  Two plugin instances configured against the same connection identity (hosts +
  ssl_options + auth) acquire the SAME `KafkaEx.New.Client` worker pid; the
  worker is stopped after a 5-second linger once the last owner releases it.

  Design:

    * `acquire/2` is synchronous, returns `{:ok, worker_pid, ref}`. The `ref`
      is opaque — pass it back to `release/2` from the same plugin lifecycle.
    * The caller process is monitored. If the owner dies without calling
      `release/2`, the registry treats the DOWN as an implicit release.
    * Workers are monitored too. If a worker dies (broker partition restart,
      kafka_ex internal crash), the entry is dropped; the next `acquire/2`
      starts a fresh worker. Outstanding tokens for the dead worker no-op on
      release.
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

  @doc false
  @spec inspect_state(GenServer.server()) :: map()
  def inspect_state(server \\ __MODULE__) do
    GenServer.call(server, :inspect_state)
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       starter: Keyword.get(opts, :starter, &default_start/1),
       stopper: Keyword.get(opts, :stopper, &default_stop/1),
       linger_ms: Keyword.get(opts, :linger_ms, @default_linger_ms),
       entries: %{},
       tokens: %{},
       caller_monitors: %{},
       worker_monitors: %{}
     }}
  end

  @impl true
  def handle_call({:acquire, identity, caller}, _from, state) do
    key = canonical_key(identity)

    case Map.get(state.entries, key) do
      nil ->
        case state.starter.(identity) do
          {:ok, pid} when is_pid(pid) ->
            wmref = Process.monitor(pid)

            entry = %{
              pid: pid,
              refs: 1,
              linger: nil,
              monitor: wmref
            }

            {token, state} = register_owner(state, key, caller)

            state = %{
              state
              | entries: Map.put(state.entries, key, entry),
                worker_monitors: Map.put(state.worker_monitors, wmref, key)
            }

            {:reply, {:ok, pid, {key, token}}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}

          other ->
            {:reply, {:error, {:unexpected_start_result, other}}, state}
        end

      %{linger: linger_timer} = entry ->
        if linger_timer, do: Process.cancel_timer(linger_timer)
        entry = %{entry | refs: entry.refs + 1, linger: nil}
        {token, state} = register_owner(state, key, caller)
        state = %{state | entries: Map.put(state.entries, key, entry)}
        {:reply, {:ok, entry.pid, {key, token}}, state}
    end
  end

  def handle_call({:release, {key, token}}, _from, state) do
    {state, _key} = drop_token(state, token, key)
    state = decrement_refs(state, key)
    {:reply, :ok, state}
  end

  def handle_call(:inspect_state, _from, state) do
    summary =
      Map.new(state.entries, fn {key, entry} ->
        {key,
         %{pid: entry.pid, refs: entry.refs, linger?: not is_nil(entry.linger)}}
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

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    cond do
      Map.has_key?(state.caller_monitors, ref) ->
        token = Map.fetch!(state.caller_monitors, ref)
        {state, key} = drop_token(state, token, nil)
        state = if key, do: decrement_refs(state, key), else: state
        {:noreply, state}

      Map.has_key?(state.worker_monitors, ref) ->
        key = Map.fetch!(state.worker_monitors, ref)

        Logger.warning(
          "[Kafka.WorkerRegistry] worker for #{inspect(key)} went DOWN; clearing entry"
        )

        {:noreply,
         %{
           state
           | entries: Map.delete(state.entries, key),
             worker_monitors: Map.delete(state.worker_monitors, ref)
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

  defp register_owner(state, key, caller) do
    token = make_ref()
    cmref = Process.monitor(caller)

    {token,
     %{
       state
       | tokens: Map.put(state.tokens, token, {key, cmref}),
         caller_monitors: Map.put(state.caller_monitors, cmref, token)
     }}
  end

  defp drop_token(state, token, expected_key) do
    case Map.pop(state.tokens, token) do
      {nil, _} ->
        {state, expected_key}

      {{key, cmref}, tokens} ->
        Process.demonitor(cmref, [:flush])

        {%{
           state
           | tokens: tokens,
             caller_monitors: Map.delete(state.caller_monitors, cmref)
         }, key}
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

  defp canonical_key(identity) do
    hosts =
      identity
      |> Map.fetch!(:hosts)
      |> Enum.map(fn {h, p} -> {to_string(h), p} end)
      |> Enum.sort()

    {hosts, :erlang.phash2(Map.get(identity, :ssl_options)),
     :erlang.phash2(Map.get(identity, :auth))}
  end

  defp default_start(identity) do
    worker_opts =
      [uris: Map.fetch!(identity, :hosts)]
      |> maybe_put(:ssl_options, Map.get(identity, :ssl_options))
      |> maybe_put(:use_ssl, Map.get(identity, :use_ssl))
      |> maybe_put(:auth, Map.get(identity, :auth))
      |> maybe_put(:metadata_update_interval, Map.get(identity, :metadata_update_interval))

    KafkaEx.start_link_worker(:no_name, worker_opts)
  end

  defp default_stop(pid) when is_pid(pid) do
    KafkaEx.stop_worker(pid)
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
