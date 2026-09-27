defmodule Flezha.Operator.Handover do
  @moduledoc """
  Moving a live operator between controller nodes: what this node ships, what an
  arriving one applies, and when either may happen at all.

  The SDK's serialized state travels, and it comes from the **runner**, so a
  migration caused by that runner's death ships nothing. The source
  controller's disk then holds the only surviving state, which the destination
  already pulled at its start (`Replay.adopt_moved_state/2`), before it bound.

  Consent is the other half: a migration mid-correction, mid-iteration, or with
  an outbox still owing output would strand work on the departing node, so those
  states defer rather than move.
  """

  alias Flezha.Config
  alias Flezha.Operator.Overwatch
  alias Flezha.Operator.Plugins
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.ControlUnit
  alias Flezha.State.WalWriter
  alias Flezha.State.WriterRegistry

  require Logger

  @doc """
  The handover payload this node ships: identity, plugins and the SDK's
  serialized state. When the source runner is already gone, `serialize_data`
  is the final state it handed over if no runner has taken the operator over
  since, and nil otherwise, and then the disk is the only surviving state.
  """
  @spec prepare(Overwatch.t()) :: {map(), binary() | nil}
  def prepare(%Overwatch{} = state) do
    serialize_data =
      case serialize_operator_sdk(state.runner_id, state.id) do
        nil -> state.snapshot
        data -> data
      end

    handover = %{
      id: state.id,
      namespace: state.namespace,
      factory_id: state.factory_id,
      init_values: state.init_values,
      plugins: Table.serializable_plugins(state.extensions)
    }

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
    # traffic". An empty binary means "no prior state": the operator starts fresh.
    serialized_state = if is_binary(pipe_serialized), do: pipe_serialized, else: <<>>

    command = %RunnerCommands.MigrationDeserialization{
      operator_id: state.id,
      factory_id: Map.get(handover_state, :factory_id),
      serialized_state: serialized_state
    }

    # An operator can arrive with nothing bound: this controller served its
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
        {:error, reason} -> Logger.error("Failed to deserialize operator: #{inspect(reason)}")
        _else -> :ok
      end
    end

    # Tear down existing, then rehydrate; bring up so providers reconnect.
    table = Plugins.unregister(Table.teardown_all(state.extensions))
    rehydrated = Table.rehydrate(table, Map.get(handover_state, :plugins, []))

    # A provider that fails to reconnect on the migrated side stays `:prepared`;
    # the convergence loop diffs it as missing and heals it under backoff.
    {brought_up, _failed} = Plugins.bring_up(%Overwatch{state | extensions: rehydrated})

    %Overwatch{
      state
      | extensions: brought_up.extensions,
        init_values: handover_state.init_values,
        # Only a handover that actually reached a runner suppresses the WAL
        # restore. One that carried nothing, or that found nothing bound to
        # carry it to, falls through to the on-disk state the bind already
        # restored from — otherwise the explicit `kick_restore` repair would
        # be locked out too.
        arrived_via_hotswap?: bound? and serialized_state != <<>>
    }
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
      state.durable_bounded and not state.suspended -> :defer
      state.effectively_once and outbox_pending?(state) -> :defer
      true -> :ready
    end
  end

  # Bounded-time and fail-open: a query failure yields `:ready` — worst case
  # is pre-consent behaviour, never a wedged migration.
  defp outbox_pending?(%Overwatch{} = state) do
    Overwatch.operator_class(state)
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
  Read the SDK's whole-operator state off its live runner. The read is not
  kept for a handover: the runner's next input would make it stale.
  """
  @spec create_snapshot(binary(), binary() | nil) :: binary() | nil
  def create_snapshot(operator_id, runner_id) do
    command = %RunnerCommands.CellState{operator_id: operator_id}

    case ControlUnit.send_control_await(runner_id, {:cell_state, command}) do
      {:ok, snapshot} -> snapshot
      {:error, _reason} -> nil
    end
  end

  defp serialize_operator_sdk(runner_id, operator_id) do
    command = %RunnerCommands.MigrationSerialization{operator_id: operator_id}

    case ControlUnit.send_control_await(runner_id, {:migration_serialization, command}) do
      {:ok, serialized_data} ->
        serialized_data

      {:error, reason} ->
        if Config.get(:env) !== :test do
          Logger.error("Failed to serialize operator: #{inspect(reason)}")
        end

        nil
    end
  end
end
