defmodule Flezha.Relay.Rebind do
  @moduledoc """
  The bind task every move runs (a rebind, a wake, a restarted runner): it runs
  `Flezha.Relay.Lifecycle.complete_init/5` off the Overwatch and reports, and
  the result is folded back here. And a reconnect's session swap.
  """

  alias Flezha.Identificator
  alias Flezha.Payloader
  alias Flezha.Relay.Builder
  alias Flezha.Relay.Ingest
  alias Flezha.Relay.Lifecycle
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Replay
  alias Flezha.Relay.Suspend
  alias Flezha.Runner.ControlUnit
  alias ProcessHub.Service.Migration, as: HubMigration

  require Logger

  @doc """
  The relay a bind onto another runner starts: the builder, seeded from
  the final state a graceful stop shipped when one is stored, else from the
  init state this Overwatch already resolved, so a move never runs the
  `:on_relay_init` chain again. The kind and the partition identity ride
  the builder, so a moved member keeps its mis-route guard.
  """
  @spec bind_relay(Overwatch.t()) :: Builder.t()
  def bind_relay(%Overwatch{builder: builder, init_values: resolved} = state) do
    case Replay.carrier(state) do
      [_, :shutdown_snapshot] -> %{builder | init_values: Payloader.json_to_map(state.snapshot)}
      [_, :init] -> if resolved == %{}, do: builder, else: %{builder | init_values: resolved}
    end
  end

  @doc """
  The convergence loop's `{:rebind, target}`, answered as the reply
  tuple the Overwatch's call returns.

  Every move is the full bind a rebind runs, off the Overwatch so a slow
  bind never blocks it: the resume state rides `AddRelay`, the durable
  Cells are restored and the input log's undelivered tail is re-forwarded,
  whether the relay was halted, suspended or running on a runner that
  died. A failed bind halts the relay on the target.
  """
  @spec rebind(Overwatch.t(), binary()) :: {:reply, :ok | {:error, term()}, Overwatch.t()}
  def rebind(%Overwatch{bind_task: task} = state, _new_runner_id) when task != nil,
    do: {:reply, {:error, :busy}, state}

  def rebind(%Overwatch{} = state, new_runner_id),
    do: {:reply, :ok, start_bind(state, new_runner_id, nil)}

  @doc """
  The runner this relay is bound to connected again as `new_session` with
  the same stable identity. A reconnect of the same process still runs the
  relay, so the binding is only re-pointed: the Overwatch, the registry
  tag and the route move together. A restarted process runs nothing, so the
  relay is bound onto it afresh. A binding that has already moved away
  from `old_session` is left alone.
  """
  @spec session_replaced(Overwatch.t(), binary(), binary(), :reconnected | :restarted) ::
          Overwatch.t()
  def session_replaced(%Overwatch{runner_id: old} = state, old, new_session, continuity) do
    case continuity do
      :reconnected -> Lifecycle.commit_binding(state, new_session)
      :restarted when state.bind_task == nil -> start_bind(state, new_session, nil)
      :restarted -> state
    end
  end

  def session_replaced(%Overwatch{} = state, _old_session, _new_session, _continuity), do: state

  @doc """
  Tell a deferred migration it may retry now. It re-queries consent on its own
  tick; this only makes the move prompt. Any failure — hub down mid-shutdown,
  nothing deferred — is a no-op.
  """
  @spec signal_migration_ready(Overwatch.t()) :: :ok
  def signal_migration_ready(%Overwatch{} = state) do
    _ = HubMigration.migration_ready(Identificator.hub(state.namespace), state.id)

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  # A graceful stop ends the Overwatch with a reason its linked bind task
  # would ignore, so teardown kills the task rather than wait on its bind.
  @spec terminate_bind_task(Overwatch.t()) :: Overwatch.t()
  def terminate_bind_task(%Overwatch{bind_task: nil} = state), do: state

  def terminate_bind_task(%Overwatch{bind_task: %{pid: pid}} = state) do
    Process.unlink(pid)
    Process.exit(pid, :kill)
    %{state | bind_task: nil}
  end

  @doc """
  Run `complete_init/5` on `runner_id` in a task so a slow bind never blocks
  the Overwatch. The task is linked to the Overwatch, so a killed Overwatch
  takes its bind with it and no second bind outlives it. It reports its
  outcome and decides nothing: a failure is a halt the Overwatch declares.
  `then` is what a wake carries, `from` the phase the bind started from
  (`apply_bind_result/2`) and `journaled` the input log's last offset then.
  `reforward` is set when the bind's own re-forward is owed again.
  """
  @spec start_bind(Overwatch.t(), binary(), :stop | nil) :: Overwatch.t()
  def start_bind(%Overwatch{} = state, runner_id, then) do
    relay = bind_relay(state)
    parent = self()
    last = Ingest.journaled(state)

    pid =
      spawn_link(fn ->
        result =
          case Lifecycle.complete_init(state, runner_id, relay, parent, :pinned) do
            {:ok, recovered} -> {:bound, recovered}
            {:error, err, halted} -> {:halt, halted, err}
          end

        send(parent, {:bind_task_result, self(), result})
      end)

    %Overwatch{state | bind_task: %{pid: pid, then: then, from: state.phase, journaled: last}}
  end

  @doc """
  The bind task's report, folded back by `apply_bind_result/2`; the outputs
  that arrived while it bound are delivered after it.
  """
  @spec bind_task_reported(Overwatch.t(), tuple()) :: Overwatch.t()
  def bind_task_reported(%Overwatch{} = state, result) do
    # The task reports a copy of this state taken before the outputs it held.
    %Overwatch{} = adopted = apply_bind_result(state, result)
    release_outputs(%Overwatch{adopted | held_outputs: state.held_outputs})
  end

  @doc """
  The bind task ended before it reported: its bind failed with `reason`. A
  task that reported is no longer in flight, so its exit reaches here only
  when it died first. The outputs held meanwhile are delivered after it.
  """
  @spec bind_task_exited(Overwatch.t(), term()) :: Overwatch.t()
  def bind_task_exited(%Overwatch{} = state, reason),
    do: state |> apply_bind_result({:halt, state, {:bind_task_exit, reason}}) |> release_outputs()

  @doc """
  Void the start phase of a bind in flight: a halt or a park landed while it
  was binding, so its result is stale. A wake's or a halted relay's bind keeps
  the phase it started from, so the phase alone cannot tell.
  """
  @spec void_bind(Overwatch.t()) :: Overwatch.t()
  def void_bind(%Overwatch{bind_task: %{} = task} = state),
    do: %Overwatch{state | bind_task: %{task | from: nil}}

  def void_bind(%Overwatch{} = state), do: state

  @doc """
  Fold a bind task's result back into the state.

  `complete_init/5` ran inside the task, whose `self()` is not the Overwatch
  process, so the binding is published here — the one point every move
  funnels through: a halted relay's rebind, a wake, a `rebind` and a
  restarted runner. A phase that changed since the bind started, or a start
  phase `void_bind/1` voided, is a halt or a sleep that landed meanwhile: the
  bound state is stale and is not adopted.
  """
  @spec apply_bind_result(Overwatch.t(), tuple()) :: Overwatch.t()
  def apply_bind_result(%Overwatch{phase: phase, bind_task: %{from: from}} = state, {:bound, _})
      when phase != from do
    # The tail journaled meanwhile, or a stop the wake carried, wakes it again.
    then = carried(state)
    state = %Overwatch{state | bind_task: nil}

    case then do
      :stop -> state |> Suspend.wake(:stop) |> elem(1)
      nil -> Suspend.rehydrate_if_pending(state)
    end
  end

  def apply_bind_result(state, {:bound, recovered}) do
    Logger.info("[Overwatch] relay #{state.id} bound onto runner #{recovered.runner_id}")

    Lifecycle.publish_binding(recovered, state.runner_id)

    if carried(state) == :stop, do: ControlUnit.steps_stop(recovered.runner_id, state.id)

    # The bound runner's state supersedes a snapshot stored before the bind.
    adopted = %{recovered | bind_task: nil, snapshot: nil}

    case state.bind_task do
      %{reforward: true} ->
        Replay.reforward_undelivered_tail(adopted)

      # Input journaled while the task bound went to the channel this relay
      # left. Only that input: an edge value the task already re-delivered
      # would count twice, since edges move no Cell's floor.
      %{journaled: before} ->
        Replay.replay_slice(adopted, Ingest.journaled_since(adopted, before))
        adopted

      _no_log_read ->
        adopted
    end
  end

  # A failed bind with input journaled meanwhile tries again at once, as any
  # bind that ends with work owed does.
  def apply_bind_result(_state, {:halt, halted, err}) do
    %{Lifecycle.halt(halted, err) | bind_task: nil}
    |> Suspend.rehydrate_if_pending()
  end

  defp carried(%Overwatch{bind_task: %{then: then}}), do: then
  defp carried(%Overwatch{}), do: nil

  # Deliver what arrived while a bind task was binding, in arrival order, on the
  # Overwatch's own output path; a bind spawned meanwhile holds them again.
  defp release_outputs(%Overwatch{held_outputs: []} = state), do: state

  defp release_outputs(%Overwatch{held_outputs: held} = state) do
    held
    |> Enum.reverse()
    |> Enum.reduce(%Overwatch{state | held_outputs: []}, fn frame, acc ->
      {:noreply, acc} = Overwatch.handle_cast(frame, acc)
      acc
    end)
  end
end
