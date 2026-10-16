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
  alias Flezha.Relay.Calls
  alias Flezha.Relay.Lifecycle
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Plugins
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Process.Timer
  alias Flezha.Runner.ControlUnit
  alias Flezha.State.WalWriter
  alias Flezha.State.WriterRegistry

  require Logger

  @doc """
  The handover payload this node ships: identity, plugins, the callers waiting
  on the relay with the time each has left, and the SDK's serialized state.
  When the source runner is already gone, `serialize_data` is the final state
  it handed over if no runner has taken the relay over since, and nil
  otherwise, and then the disk is the only surviving state.

  The waiting callers leave with the payload: this process drops its rows
  (`:calls_handed_over`), so its terminate owes them nothing.
  """
  @spec prepare(Overwatch.t()) :: {map(), binary() | nil}
  def prepare(%Overwatch{} = state) do
    serialize_data =
      case serialize_relay_sdk(state.runner_id, state.id) do
        nil -> state.snapshot
        data -> data
      end

    handover = %{
      id: state.id,
      namespace: state.namespace,
      factory_id: state.factory_id,
      init_values: state.init_values,
      calls: Calls.export(state.calls)
    }

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

    command = %RunnerCommands.MigrationDeserialization{
      relay_id: state.id,
      factory_id: Map.get(handover_state, :factory_id),
      serialized_state: serialized_state
    }

    # A relay can arrive with nothing bound: this controller served its
    # group when the move started and no longer does, or its last runner for
    # the factory went while the move was in flight. There is no runner to ship
    # the state to, and the bind that follows restores it.
    bound? = is_binary(state.runner_id)

    if bound? do
      result =
        Overwatch.with_task_span(:migration_deserialize, state.id, fn ->
          ControlUnit.send_control_await(state.runner_id, {:migration_deserialization, command})
        end)

      case result do
        {:error, reason} -> Logger.error("Failed to deserialize relay: #{inspect(reason)}")
        _else -> :ok
      end
    end

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

    %Overwatch{
      state
      | extensions: brought_up.extensions,
        init_values: handover_state.init_values,
        calls: Calls.absorb(Map.get(handover_state, :calls, [])),
        # Only a handover that actually reached a runner suppresses the WAL
        # restore. One that carried nothing, or that found nothing bound to
        # carry it to, falls through to the on-disk state the bind already
        # restored from — otherwise the explicit `kick_restore` repair would
        # be locked out too.
        arrived_via_hotswap?: bound? and serialized_state != <<>>
    }
    |> arm_call_deadlines()
  end

  # Each carried row keeps the deadline it had on the source node; an
  # relay's own call keeps its wall-clock one.
  defp arm_call_deadlines(%Overwatch{calls: calls} = state) do
    timers =
      Enum.reduce(Map.keys(calls.rows), state.timers, fn token, timers ->
        Timer.arm(
          timers,
          {:call, token},
          Calls.remaining_ms(calls, token),
          {:call_deadline, token}
        )
      end)

    timers =
      Enum.reduce(calls.out, timers, fn {id, row}, timers ->
        left = max(row.deadline_at - System.system_time(:millisecond), 0)
        Timer.arm(timers, {:call, id}, left, {:call_timeout, id})
      end)

    %Overwatch{state | timers: timers}
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
      state.durable_steps and not state.suspended -> :defer
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
