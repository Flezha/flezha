defmodule Flezha.Runner.ControlChannel.Requests do
  @moduledoc """
  The requests a runner's SDK initiates on its control stream: the ones that
  act on one operative (a graceful-shutdown notification, a workflow
  completion, an iteration park, a durability barrier) and the ones scoped to
  the runner or its namespace (a factory update, a tasklet-requested spawn,
  an admin call).

  An operative request is judged first by the operative's ownership record
  (`Flezha.Runner.StreamChannel.Records.admit/3`), so a runner acts only on the
  operatives bound to its stable identity, and a refused request does nothing
  to the operative. A runner or namespace request is judged as the identity
  the connection proved.

  Every function runs in the control channel's process and takes its state.
  The channel stays the one writer of control frames: a request that owes an
  answer hands it back for the channel to write, at once or, when it is
  resolved off-process, as a `{:sdk_reply, ...}` message, and a barrier is
  answered by the commit worker through the channel.
  """

  alias Flezha.Config.Durations
  alias Flezha.Operative.Migration
  alias Flezha.Runner.ControlChannel.Commits
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Session
  alias Flezha.Runner.StreamChannel.Records
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.Admin
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.TaskJournal

  require Logger

  @doc """
  Apply the SDK's final state to the operative's `Overwatch`, and answer the
  `RunnerShutdownAck` for the channel to write back.

  The cast-not-call ordering is deliberate: the snapshot is enqueued on the
  Overwatch's mailbox before the ack flows back, so even if the controller
  crashes between the cast and the write, the snapshot is in the GenServer's
  state.

  The ack is answered unconditionally, for a refused notification and for an
  Overwatch already migrated to a peer controller alike, so the SDK never
  blocks its container's shutdown grace period on an ack that will not arrive.
  """
  @spec runner_shutdown(map(), RunnerCommands.RunnerShutdownNotification.t()) ::
          {tuple(), binary()}
  def runner_shutdown(state, %RunnerCommands.RunnerShutdownNotification{} = notif) do
    %{operative_id: operative_id, serialized_state: serialized_state} = notif

    if Records.admit(state, operative_id, :runner_shutdown_notification) do
      case HubManager.get_pid(state.namespace, operative_id) do
        {:ok, pid} when is_pid(pid) ->
          GenServer.cast(pid, {:apply_external_snapshot, serialized_state})

        _ ->
          Logger.warning(
            "[control_channel] Overwatch missing for shutdown notification — assumed migrated. " <>
              "namespace=#{inspect(state.namespace)} operative_id=#{inspect(operative_id)}"
          )
      end
    end

    {{:runner_shutdown_ack, %RunnerCommands.RunnerShutdownAck{operative_id: operative_id}},
     "RunnerShutdownAck operative=#{inspect(operative_id)}"}
  end

  @doc """
  Converged teardown for every workflow exit trigger (quiescence, ctx.complete,
  loop terminal, max_duration, cooperative stop): surface the outcome and the
  result payload, then deregister on completion so the operative never replays
  on boot. A `status = "error"` tears down identically, with no retry. A
  completion from a runner that does not own the instance is not an exit
  trigger.
  """
  @spec workflow_completed(map(), RunnerCommands.WorkflowCompleted.t()) :: :ok
  def workflow_completed(state, %RunnerCommands.WorkflowCompleted{} = completed) do
    if Records.admit(state, completed.operative_id, :workflow_completed),
      do: complete(state, completed)

    :ok
  end

  defp complete(state, completed) do
    %RunnerCommands.WorkflowCompleted{
      operative_id: operative_id,
      status: status,
      error: error,
      result: result
    } = completed

    :telemetry.execute(
      [:flezha, :workflow, :completed],
      %{count: 1},
      %{
        namespace: state.namespace,
        operative_id: operative_id,
        status: status,
        result: result
      }
    )

    if status == "error", do: report_workflow_failure(state, operative_id, error)

    # Finished-job retention: record the terminal outcome (durably, before the
    # teardown) so `flz operative info` can surface it and `stop_operative`
    # retains the state instead of deleting it. `finished_at` is stamped
    # controller-side (the wire carries no timestamp).
    _ =
      Completion.write(operative_id, %{
        namespace: state.namespace,
        status: status,
        error: error,
        result: result,
        finished_at: System.system_time(:second)
      })

    deregister_on_completion(state, operative_id)
  end

  defp report_workflow_failure(state, operative_id, error) do
    Logger.warning(
      "[control_channel] workflow operative #{inspect(operative_id)} failed in namespace " <>
        "#{inspect(state.namespace)}: #{inspect(error)}"
    )

    :telemetry.execute(
      [:flezha, :workflow, :failed],
      %{count: 1},
      %{namespace: state.namespace, operative_id: operative_id, error: error}
    )
  end

  # Tear down off the control-channel process: `stop_operative` drives a
  # RemOperative round-trip whose CommandResponse returns on THIS control
  # stream, so calling it inline would deadlock the reader.
  defp deregister_on_completion(%{namespace: namespace, runner_id: runner_id}, operative_id) do
    Task.start(fn ->
      drain_owed_outputs(runner_id, operative_id)

      case OperativeManager.stop_operative(namespace, operative_id) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.warning(
            "[control_channel] workflow operative #{inspect(operative_id)} teardown failed: #{inspect(reason)}"
          )
      end
    end)
  end

  # A completion must not tear the Overwatch down under output still on its
  # way: a durable class's last outbox is flushed after its commit ack on this
  # channel, and an opted-out class's direct outputs travel the data stream
  # while the completion travels this one. Two debts, settled differently:
  #
  #   * the data stream is FLUSHED, once, by a barrier across the runner's
  #     stream channels. Nothing is counted — counting what the runner wrote
  #     against what the controller ran is decidable only while nothing is
  #     ever lost, and a frame routed to an Overwatch that has since been
  #     replaced is exactly that loss.
  #   * the journal is WAITED for, unchanged: `pending_entries/1` is an exact,
  #     durable, re-drivable debt, and `Durations.get(:drain_owed_ms)` keeps a sink that never
  #     confirms from holding the teardown — the retained journal keeps the
  #     debt. Under a start storm the flush itself queues behind the channel
  #     for tens of seconds, and the instance is retained either way.

  defp drain_owed_outputs(runner_id, operative_id) do
    StreamPool.flush_channels(runner_id)

    await_owed_journal(
      operative_id,
      System.monotonic_time(:millisecond) + Durations.get(:drain_owed_ms)
    )
  end

  defp await_owed_journal(operative_id, deadline) do
    pending = TaskJournal.pending_entries(operative_id)

    cond do
      pending == [] ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        Logger.warning(
          "[control_channel] workflow operative #{inspect(operative_id)} completed with " <>
            "#{length(pending)} outbox entries still owed"
        )

      true ->
        TaskJournal.flush_pending(operative_id)
        Process.sleep(50)
        await_owed_journal(operative_id, deadline)
    end
  end

  @doc """
  The SDK's loop found its inbox empty at a committed iteration boundary and
  tore its run down without a completion (`workflow-durable-suspension`).
  Journal the park record, durable before the eviction so a controller crash in
  between still rediscovers the suspension, then hand the instance to its
  Overwatch, which evicts it from this runner and keeps it resumable.

  Off this process: the eviction drives a `RemOperative` whose response returns
  on this control stream, so doing it inline would deadlock the reader, the
  same reason as the workflow-completed teardown.
  """
  @spec iter_park(map(), RunnerCommands.IterPark.t()) :: :ok
  def iter_park(state, %RunnerCommands.IterPark{} = park) do
    if Records.admit(state, park.operative_id, :iter_park), do: park_instance(state, park)
    :ok
  end

  defp park_instance(state, park) do
    %RunnerCommands.IterPark{
      operative_id: operative_id,
      iter: iter,
      wake_deadline_ms: wake_deadline_ms,
      consumed_offset: consumed_offset
    } = park

    deadline = if wake_deadline_ms == 0, do: nil, else: wake_deadline_ms

    :telemetry.execute(
      [:flezha, :workflow, :parked],
      %{count: 1},
      %{namespace: state.namespace, operative_id: operative_id, iter: iter}
    )

    namespace = state.namespace

    Task.start(fn ->
      case TaskJournal.append_parked(namespace, operative_id, iter, deadline, consumed_offset) do
        :ok ->
          # Before the suspension asks the log whether input is still owed: a
          # message the run parked unread is consumed, not owed.
          TaskJournal.release_input(operative_id, consumed_offset)
          suspend_parked(namespace, operative_id, iter, deadline)

        {:error, reason} ->
          # The park is not durable, so the instance must NOT be evicted: leave
          # it bound. Its driver has already exited, so it stays idle until the
          # runner reconnects or convergence rebinds it — at which point it
          # resumes from the last committed iteration and parks again.
          Logger.warning(
            "[control_channel] park record failed for #{inspect(operative_id)} " <>
              "iter=#{iter}: #{inspect(reason)} — instance left bound"
          )
      end
    end)
  end

  # No Overwatch to suspend (already migrated / torn down) is not a failure: the
  # park record is durable, so the instance is rediscovered as suspended
  # whenever it is next started.
  defp suspend_parked(namespace, operative_id, iter, deadline) do
    case HubManager.get_pid(namespace, operative_id) do
      {:ok, pid} when is_pid(pid) -> Migration.suspend(pid, iter, deadline)
      _ -> :ok
    end
  end

  @doc """
  A durability barrier (`TaskCommit` or `IterCommit`), handed to the runner's
  commit worker so this process does no disk I/O. A barrier from a runner that
  does not own the instance is answered with an error and journals nothing,
  which ends the sending driver's run.
  """
  @spec barrier(map(), :task_commit | :iter_commit, struct(), binary()) :: :ok
  def barrier(state, kind, commit, reference_id) do
    if Records.admit(state, commit.operative_id, kind),
      do: Commits.commit(state.runner_id, reference_id, commit),
      else: Commits.refuse(state.runner_id, reference_id, commit)
  end

  @doc """
  Full-set replacement of this runner's declared factory list after a
  post-start define or remove. Fire-and-forget by design, with no ack, and a
  cast so it never blocks the control channel; the reconnect init frame
  remains the resync of record.
  """
  @spec update_factories(map(), RunnerCommands.UpdateFactories.t()) :: :ok
  def update_factories(state, %RunnerCommands.UpdateFactories{factories: factories}) do
    factories =
      Enum.map(factories, fn %RunnerCommands.FactoryDecl{
                               id: id,
                               description: description,
                               sdk_lang: sdk_lang,
                               kind: kind
                             } ->
        %{
          "id" => id,
          "description" => if(description == "", do: nil, else: description),
          "sdk_lang" => sdk_lang,
          "kind" => kind
        }
      end)

    GenServer.cast(
      FleetManager,
      {:sync_factories, state.namespace, state.runner_id, factories}
    )
  end

  @doc """
  A tasklet asked for a workflow instance (`ctx.spawn_workflow`), judged as
  the identity this connection proved. The namespace is this channel's, never
  the requester's claim, so a request cannot reach another namespace.
  """
  @spec spawn_workflow(map(), RunnerCommands.SpawnWorkflowRequest.t(), binary()) :: :ok
  def spawn_workflow(state, request, reference_id) do
    %RunnerCommands.SpawnWorkflowRequest{
      instance_id: instance_id,
      factory_id: factory_id,
      payload: payload,
      if_running: if_running
    } = request

    namespace = state.namespace

    answer_off_process(reference_id, fn ->
      judged =
        once_admitted(state, fn ->
          OperativeManager.spawn_or_deliver(
            namespace,
            instance_id,
            factory_id,
            payload,
            if_running(if_running),
            Session.runner_caller_context(namespace)
          )
        end)

      # A session refused before it was admitted judged nothing, so it answers
      # the id the request carried.
      {outcome, resolved_id} =
        case judged do
          {:refused, _reason} -> {judged, instance_id}
          {_outcome, _resolved_id} = answered -> answered
        end

      {name, error} = spawn_reply(outcome)

      :telemetry.execute(
        [:flezha, :operative, :requested_spawn],
        %{count: 1},
        %{
          namespace: namespace,
          instance_id: resolved_id,
          factory_id: factory_id,
          outcome: name
        }
      )

      {{:spawn_workflow_response,
        %RunnerCommands.SpawnWorkflowResponse{
          outcome: Atom.to_string(name),
          error: error,
          instance_id: resolved_id
        }}, "spawn-workflow response #{name}"}
    end)
  end

  @doc """
  Application code administering the namespace this channel proved. The door
  judges it as the connection's identity against the published surface, so
  nothing about the request names a namespace, a principal or a role.
  """
  @spec admin(map(), RunnerCommands.AdminRequest.t(), binary()) :: :ok
  def admin(state, request, reference_id) do
    %RunnerCommands.AdminRequest{verb: verb, args: args} = request
    namespace = state.namespace

    answer_off_process(reference_id, fn ->
      {result, error} =
        case once_admitted(state, fn -> Admin.call(namespace, verb, args) end) do
          {:ok, json} -> {json, ""}
          {:refused, reason} -> {"", reason}
          {:error, reason} -> {"", reason}
        end

      :telemetry.execute(
        [:flezha, :runner, :admin_request],
        %{count: 1},
        %{namespace: namespace, verb: verb, refused: error != ""}
      )

      {{:admin_response, %RunnerCommands.AdminResponse{result: result, error: error}},
       "admin response"}
    end)
  end

  # A request whose answer cannot be worked out on the channel's process:
  # resolving it drives an `AddOperative` or a `RemOperative` whose response
  # returns on this control stream, so doing it inline would deadlock the
  # reader. `answer` runs off-process and hands back the reply command and its
  # log label, which the channel writes.
  defp answer_off_process(reference_id, answer) when is_function(answer, 0) do
    channel = self()

    Task.start(fn ->
      {command, log_label} = answer.()
      send(channel, {:sdk_reply, reference_id, command, log_label})
    end)

    :ok
  end

  # An unrecognised policy reads as the default: a requester on a newer SDK
  # must never have its payload silently refused by an older controller.
  defp if_running("error"), do: :error
  defp if_running(_), do: :deliver

  # `{:refused, reason}` is the only outcome carrying detail; the rest are their
  # own name with nothing attached.
  # A runner can send a request before its handshake completes, and the roster
  # holds neither the runner nor its factories until then, so a request judged
  # against it waits for the session to be admitted.
  defp once_admitted(state, judge) do
    case FleetManager.await_admitted(state.runner_id) do
      :ok -> judge.()
      {:error, :not_admitted} -> {:refused, "the runner's session ended before it was admitted"}
    end
  end

  defp spawn_reply({:refused, reason}), do: {:refused, reason}
  defp spawn_reply(outcome), do: {outcome, ""}
end
