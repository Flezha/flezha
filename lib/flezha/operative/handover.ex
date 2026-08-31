defmodule Flezha.Operative.Handover do
  @moduledoc """
  Moving a live operative between controller nodes: what this node ships, what an
  arriving one applies, and when either may happen at all.

  Two things travel, and the second exists because the first can be empty. The
  SDK's serialized state comes from the **runner**, so a migration caused by that
  runner's death ships nothing — and then the source controller's disk holds the
  only surviving state, which the destination pulls by node name.

  Consent is the other half: a migration mid-correction, mid-iteration, or with
  an outbox still owing output would strand work on the departing node, so those
  states defer rather than move.
  """

  alias Flezha.Operative.Plugins
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Replay
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.ControlUnit
  alias Flezha.State.WalTransfer
  alias Flezha.State.WalWriter

  require Logger

  @doc """
  The handover payload this node ships: identity, plugins and the SDK's
  serialized state — plus the node whose disk holds the durable files, because
  `serialize_data` is nil whenever the source runner is already gone and then
  the disk is the only surviving state.
  """
  @spec prepare(Overwatch.t()) :: {map(), binary() | nil}
  def prepare(%Overwatch{} = state) do
    serialize_data =
      case serialize_operative_sdk(state.runner_id, state.id) do
        nil -> state.snapshot
        data -> data
      end

    handover = %{
      id: state.id,
      namespace: state.namespace,
      factory_id: state.factory_id,
      init_values: state.init_values,
      plugins: Table.serializable_plugins(state.extensions),
      source_node: node()
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
    # traffic". An empty binary means "no prior state": the operative starts fresh.
    serialized_state = if is_binary(pipe_serialized), do: pipe_serialized, else: <<>>

    command = %RunnerCommands.MigrationDeserialization{
      operative_id: state.id,
      factory_id: Map.get(handover_state, :factory_id),
      serialized_state: serialized_state
    }

    result =
      Overwatch.with_proc_span(:migration_deserialize, state.id, fn ->
        ControlUnit.send_control_await(state.runner_id, {:migration_deserialization, command})
      end)

    case result do
      {:error, reason} -> Logger.error("Failed to deserialize operative: #{inspect(reason)}")
      _else -> :ok
    end

    # Tear down existing, then rehydrate; activate so providers reconnect.
    table = Plugins.unregister(Table.teardown_all(state.extensions))
    rehydrated = Table.rehydrate(table, Map.get(handover_state, :plugins, []))

    # A provider that fails to reconnect on the migrated side stays `:prepared`;
    # the convergence loop diffs it as missing and heals it under backoff.
    {reactivated, _failed} = Table.bulk_activate(rehydrated, Plugins.env(state))

    migrated =
      %Overwatch{
        state
        | extensions: reactivated,
          init_values: handover_state.init_values,
          # Only a handover that actually carried state suppresses the WAL
          # restore. An empty one carries nothing to double-apply, so the
          # operative must fall through to its on-disk state — otherwise a
          # migration away from a dead runner starts fresh AND locks the
          # replay out, including the explicit `kick_restore` repair.
          arrived_via_hotswap?: serialized_state != <<>>
      }

    if migrated.arrived_via_hotswap?,
      do: migrated,
      else: adopt_source_state(migrated, Map.get(handover_state, :source_node))
  end

  # An empty handover means the source runner was already gone, so the source
  # controller's DISK holds the only surviving state — and `state_root` is
  # node-local, never assumed shared. Pull it before replaying, off-process:
  # this runs inside ProcessHub's migration call and the copy is bounded by
  # file size, not by anything this process should block on.
  defp adopt_source_state(%Overwatch{runner_id: runner_id} = state, source_node)
       when is_atom(source_node) and source_node != nil and is_binary(runner_id) do
    operative_class = Overwatch.operative_class(state)
    overwatch = self()

    spawn(fn ->
      if WalTransfer.pull_operative(source_node, operative_class) > 0 do
        # The sanctioned restore trigger — the same one the convergence heal
        # reaches through `State.Recovery.resync_for_overwatch/2`. Its
        # `arrived_via_hotswap?` gate is false here precisely because the
        # handover was empty.
        Replay.kick_restore_for_bound_runner(overwatch, runner_id)
      end
    end)

    state
  end

  # No source to pull from, or nothing bound yet to replay onto — the ordinary
  # bind-time restore covers the latter once a runner is resolved.
  defp adopt_source_state(%Overwatch{} = state, _source_node), do: state

  @doc """
  Busy classification for ProcessHub's migration consent: defer while a
  correction is pending, while a durable workflow is off an iteration
  boundary (suspension IS the safe barrier), or while the effectively-once
  outbox still owes output that would strand on this node — queued behind the
  flush, or dispatched and not yet confirmed by a sink.

  The synchronous correction transaction needs no clause — mid-run this process
  cannot answer, and consent's no-reply timeout defers it.
  """
  @spec consent(Overwatch.t()) :: :ready | :defer
  def consent(%Overwatch{} = state) do
    cond do
      state.corrections != %{} -> :defer
      state.durable_workflow and not state.suspended -> :defer
      state.effectively_once and outbox_pending?(state) -> :defer
      true -> :ready
    end
  end

  # Bounded-time and fail-open: a query failure yields `:ready` — worst case
  # is pre-consent behaviour, never a wedged migration.
  defp outbox_pending?(%Overwatch{} = state) do
    Overwatch.operative_class(state)
    |> Flezha.State.Supervisor.live_writers()
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
  Pull the SDK's whole-operative state as the handover snapshot.
  """
  @spec create_snapshot(binary(), binary() | nil) :: binary() | nil
  def create_snapshot(operative_id, runner_id) do
    command = %RunnerCommands.EntityState{operative_id: operative_id}

    case ControlUnit.send_control_await(runner_id, {:entity_state, command}) do
      {:ok, snapshot} -> snapshot
      {:error, _reason} -> nil
    end
  end

  defp serialize_operative_sdk(runner_id, operative_id) do
    command = %RunnerCommands.MigrationSerialization{operative_id: operative_id}

    case ControlUnit.send_control_await(runner_id, {:migration_serialization, command}) do
      {:ok, serialized_data} ->
        serialized_data

      {:error, reason} ->
        if Application.get_env(:flezha, :env) !== :test do
          Logger.error("Failed to serialize operative: #{inspect(reason)}")
        end

        nil
    end
  end
end
