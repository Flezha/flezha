defmodule Flezha.Relay.Handover do
  @moduledoc """
  Moving a live relay between controller nodes: what this node ships, what an
  arriving one applies, and when either may happen at all.

  The SDK's serialized state travels, and it comes from the **runner**, so a
  migration caused by that runner's death ships nothing. The source
  controller's disk then holds the only surviving state, which the destination
  already pulled at its start (`Replay.adopt_moved_state/4`), before it bound.

  Consent is the other half: a migration mid-correction, mid-iteration, or with
  an outbox still owing output would strand work on the departing node, so those
  states defer rather than move.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Relay.Calls.Inbound
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Lifecycle
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Plugins
  alias Flezha.Relay.Replay
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.ControlUnit
  alias Flezha.Service.HubManager
  alias Flezha.State.TaskJournal.Records
  alias Flezha.State.WalWriter
  alias Flezha.State.WriterRegistry

  require Logger

  @doc """
  The handover payload this node ships: identity, plugins, the callers waiting
  on the relay with each one's deadline, and the SDK's serialized state.
  When the source runner is already gone, `serialize_data` is the final state
  it handed over if no runner has taken the relay over since, and nil
  otherwise, and then the disk is the only surviving state.

  The waiting callers leave with the payload: this process drops its rows
  (`:calls_handed_over`), so its terminate owes them nothing. The move starts
  here, so the lineage is released here too: this process lives on until the
  new one registers, and a bid from this node in that gap could only fence it.
  """
  @spec prepare(Overwatch.t()) :: {map(), binary() | nil}
  def prepare(%Overwatch{} = state) do
    # The hub takes this answer only within its state query timeout, so the
    # serialize and the wait for its cut share that one budget.
    budget = HubManager.migration_strategy().state_query_timeout
    deadline = System.monotonic_time(:millisecond) + budget

    serialize_data =
      case serialize_relay_sdk(state.runner_id, state.id) do
        nil -> state.snapshot
        data -> await_cut(state, data, deadline)
      end

    handover = %{
      id: state.id,
      namespace: state.namespace,
      factory_id: state.factory_id,
      init_values: state.init_values,
      calls: Inbound.export(state.calls)
    }

    Lifecycle.release_lineage(%Overwatch{state | cause: :move})
    send(self(), :calls_handed_over)
    {handover, serialize_data}
  end

  @doc """
  Apply an arriving handover: ship the SDK its deserialization command, rebuild
  the plugin table, and decide whether the WAL restore is suppressed.
  """
  @spec absorb(Overwatch.t(), {map(), binary() | nil}) :: Overwatch.t()
  def absorb(%Overwatch{} = state, {handover_state, pipe_serialized}) do
    # `serialized_state` is a protobuf bytes field. When the SDK has no state
    # to hand over (no prior snapshot, or the source runner is already gone)
    # `pipe_serialized` is nil, and Protox.encode/1 raises on a non-binary —
    # which crashed the send before the deserialization command was ever
    # sent, leaving the runner stuck "awaiting controller control-plane
    # traffic". An empty binary means "no prior state": the relay starts fresh.
    serialized_state = if is_binary(pipe_serialized), do: pipe_serialized, else: <<>>

    # A relay can arrive with nothing bound: this controller served its
    # group when the move started and no longer does, or its last runner for
    # the factory went while the move was in flight. There is no runner to ship
    # the state to, and the bind that follows restores it.
    bound? = is_binary(state.runner_id)
    if bound?, do: deserialize(state, Map.get(handover_state, :factory_id), serialized_state)

    # The bindings come up from the relay's declared entry, resolved on this
    # controller: what it can run here may differ from what the old one could.
    # A provider that fails to reconnect stays `:prepared`; the convergence loop
    # diffs it as missing and heals it under backoff.
    fresh = %Overwatch{state | extensions: Table.teardown_all(state.extensions)}

    prepared =
      case Plugins.prepare(fresh, Lifecycle.declared_bindings(fresh, state.builder)) do
        {:ok, prepared} ->
          prepared

        {:error, reason} ->
          Logger.warning("[handover] #{state.id} arrived without its plugins: #{inspect(reason)}")
          fresh
      end

    {brought_up, _failed} = Plugins.bring_up(prepared)
    arrived? = bound? and serialized_state != <<>>

    %Overwatch{
      state
      | extensions: brought_up.extensions,
        init_values: handover_state.init_values,
        calls: Inbound.absorb(Map.get(handover_state, :calls, [])),
        # Only a handover that actually reached a runner suppresses the WAL
        # restore. One that carried nothing, or that found nothing bound to
        # carry it to, falls through to the on-disk state the bind already
        # restored from — otherwise the explicit `kick_restore` repair would
        # be locked out too.
        arrived_via_hotswap?: arrived?,
        cause: if(arrived?, do: :arrive, else: state.cause)
    }
    |> arm_calls()
  end

  defp deserialize(%Overwatch{} = state, factory_id, serialized_state) do
    command = %RunnerCommands.MigrationDeserialization{
      relay_id: state.id,
      factory_id: factory_id,
      serialized_state: serialized_state
    }

    result =
      Overwatch.with_task_span(:migration_deserialize, state.id, fn ->
        ControlUnit.send_control_await(state.runner_id, {:migration_deserialization, command})
      end)

    case result do
      {:error, reason} -> Logger.error("Failed to deserialize relay: #{inspect(reason)}")
      _else -> :ok
    end
  end

  # Each carried row keeps the wall-clock deadline it had on the source node.
  defp arm_calls(%Overwatch{calls: calls} = state) do
    deadlines =
      for({token, row} <- calls.rows, do: {{:call_deadline, token}, row.deadline_at}) ++
        for({id, row} <- calls.out, do: {{:call_timeout, id}, row.deadline_at})

    Enum.reduce(deadlines, state, fn {message, at}, state ->
      Inbound.arm_call(state, message, at)
    end)
  end

  @doc """
  Busy classification for ProcessHub's migration consent: defer while a
  correction is pending, while an instance that journals its progress stage by
  stage is awake (a pass in flight is not a state document a handover can
  ship, so it migrates only from its sleep), or while the effectively-once
  outbox still owes output that would strand on this node — queued behind the
  flush, or dispatched and not yet confirmed by a sink. A sleeping instance of
  either kind consents, and so does an awake stream, whether or not it sleeps.

  The synchronous correction transaction needs no clause — mid-run this process
  cannot answer, and consent's no-reply timeout defers it.
  """
  @spec consent(Overwatch.t()) :: :ready | :defer
  def consent(%Overwatch{} = state) do
    cond do
      state.corrections != %{} -> :defer
      state.durable_steps and state.phase != :suspended -> :defer
      state.effectively_once and outbox_pending?(state) -> :defer
      true -> :ready
    end
  end

  # Steps-time and fail-open: a query failure yields `:ready` — worst case
  # is pre-consent behaviour, never a wedged migration.
  defp outbox_pending?(%Overwatch{} = state) do
    Overwatch.relay_class(state)
    |> WriterRegistry.live_writers()
    |> Enum.any?(fn pid ->
      match?(
        %{pending: p, awaiting: a} when p + a > 0,
        WalWriter.outbox_status(pid, 1_000)
      )
    end)
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  @doc """
  Read the SDK's whole-relay state off its live runner. The read is not
  kept for a handover: the runner's next input would make it stale.
  """
  @spec create_snapshot(binary(), binary() | nil) :: binary() | nil
  def create_snapshot(relay_id, runner_id) do
    command = %RunnerCommands.CellState{relay_id: relay_id}

    case ControlUnit.send_control_await(runner_id, {:cell_state, command}) do
      {:ok, snapshot} -> snapshot
      {:error, _reason} -> nil
    end
  end

  # The destination pulls this node's copy of the state only once the handover
  # state is out, so for a class whose bind re-forwards above its Cells' floor
  # the state leaves only when every record the runner shipped up to its cut
  # (an early-closed checkpoint interval's among them) is durable here. Past
  # the grace nothing is handed over: the destination's bind restores from the
  # disk it pulled and re-forwards the tail above that floor, which is exact.
  # The wait ends by the hub's deadline too, if that comes first.
  defp await_cut(%Overwatch{} = state, data, deadline) do
    class = Overwatch.relay_class(state)
    left = max(deadline - System.monotonic_time(:millisecond), 0)
    grace = min(Durations.get(:forced_handover_grace_ms), left)

    if Replay.awaited_restore?(state) and ClassRegistry.persistent?(class) and
         not durable_cut?(data, grace) do
      Logger.warning("[handover] #{state.id}: the runner's cut is not durable within the grace")
      nil
    else
      data
    end
  end

  @doc """
  Whether every Cell a moved state names (its `$cells`, each with the `next_seq`
  after the last record its runner shipped) has that record durable on this
  node within `grace_ms`. A writer that refuses the wait, or cannot be reached,
  answers no.
  """
  @spec durable_cut?(binary(), non_neg_integer()) :: boolean()
  def durable_cut?(data, grace_ms) do
    deadline = System.monotonic_time(:millisecond) + grace_ms

    data
    |> moved_cells()
    |> Enum.reduce_while({:ok, :gen_server.reqids_new()}, fn {cell_id, seq}, {:ok, reqids} ->
      case Records.await_record_async(cell_id, seq, reqids, cell_id) do
        {:ok, reqids} -> {:cont, {:ok, reqids}}
        {:error, _unreachable} -> {:halt, {:abandon, reqids}}
      end
    end)
    |> case do
      {:ok, reqids} -> all_durable?(reqids, deadline)
      {:abandon, reqids} -> abandon(reqids)
    end
  end

  # `{cell_id, last_seq}` for each moved Cell that shipped a record.
  defp moved_cells(data) do
    case Jason.decode(data) do
      {:ok, %{"$cells" => cells}} when is_list(cells) ->
        for %{"identity" => identity, "next_seq" => next} <- cells,
            is_list(identity) and is_integer(next) and next > 0,
            do: {:erlang.list_to_binary(identity), next - 1}

      _no_moved_cells ->
        []
    end
  end

  defp all_durable?(reqids, deadline) do
    case :gen_server.receive_response(reqids, {:abs, deadline}, true) do
      :no_request -> true
      {{:reply, {:ok, _seq}}, _cell_id, reqids} -> all_durable?(reqids, deadline)
      {_refused_or_down, _cell_id, reqids} -> abandon(reqids)
      :timeout -> abandon(reqids)
    end
  end

  # A wait given up is demonitored, so a late answer never reaches this process.
  defp abandon(reqids) do
    for {request, _cell_id} <- :gen_server.reqids_to_list(reqids),
        do: Process.demonitor(request, [:flush])

    false
  end

  defp serialize_relay_sdk(runner_id, relay_id) do
    command = %RunnerCommands.MigrationSerialization{relay_id: relay_id}

    case ControlUnit.send_control_await(runner_id, {:migration_serialization, command}) do
      {:ok, serialized_data} ->
        serialized_data

      {:error, reason} ->
        if Config.get(:env) !== :test do
          Logger.error("Failed to serialize relay: #{inspect(reason)}")
        end

        nil
    end
  end
end
