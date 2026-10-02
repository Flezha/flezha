defmodule Flezha.Runner.ControlChannel.Requests do
  @moduledoc """
  The requests a runner's SDK initiates on its control stream: the ones that
  act on one operator (a graceful-shutdown notification, a steps operator
  completion, an iteration park, an exit report, a durability barrier) and the ones scoped to
  the runner or its namespace (a factory update, a tasklet-requested spawn,
  an admin call).

  An operator request is judged first by the operator's ownership record
  (`Flezha.Runner.StreamChannel.Records.admit/3`), so a runner acts only on the
  operators bound to its stable identity, and a refused request does nothing
  to the operator. A runner or namespace request is judged as the identity
  the connection proved.

  Every function runs in the control channel's process and takes its state.
  The channel stays the one writer of control frames: a request that owes an
  answer hands it back for the channel to write, at once or, when it is
  resolved off-process, as a `{:sdk_reply, ...}` message, and a barrier is
  answered by the commit worker through the channel.
  """

  alias Flezha.Config.Durations
  alias Flezha.Operator.Migration
  alias Flezha.Runner.ControlChannel.Commits
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Session
  alias Flezha.Runner.StreamChannel.Records
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.Admin
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.{Completion, TaskJournal}

  require Logger

  @doc """
  Apply the SDK's final state to the operator's `Overwatch`, and answer the
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
    %{operator_id: operator_id, serialized_state: serialized_state} = notif

    if Records.admit(state, operator_id, :runner_shutdown_notification) do
      case HubManager.get_pid(state.namespace, operator_id) do
        {:ok, pid} when is_pid(pid) ->
          GenServer.cast(pid, {:apply_external_snapshot, serialized_state})

        _ ->
          Logger.warning(
            "[control_channel] Overwatch missing for shutdown notification — assumed migrated. " <>
              "namespace=#{inspect(state.namespace)} operator_id=#{inspect(operator_id)}"
          )
      end
    end

    {{:runner_shutdown_ack, %RunnerCommands.RunnerShutdownAck{operator_id: operator_id}},
     "RunnerShutdownAck operator=#{inspect(operator_id)}"}
  end

  @doc """
  Converged teardown for every steps operator exit trigger (quiescence, ctx.complete,
  loop terminal, max_duration, cooperative stop): surface the outcome and the
  result payload, then deregister on completion so the operator never replays
  on boot. A `status = "error"` tears down identically, with no retry. A
  completion from a runner that does not own the instance is not an exit
  trigger.
  """
  @spec operator_completed(map(), RunnerCommands.OperatorCompleted.t()) :: :ok
  def operator_completed(state, %RunnerCommands.OperatorCompleted{} = completed) do
    if Records.admit(state, completed.operator_id, :operator_completed),
      do: complete(state, completed)

    :ok
  end

  defp complete(state, completed) do
    %RunnerCommands.OperatorCompleted{
      operator_id: operator_id,
      status: status,
      error: error,
      result: result
    } = completed

    :telemetry.execute(
      [:flezha, :steps, :completed],
      %{count: 1},
      %{
        namespace: state.namespace,
        operator_id: operator_id,
        status: status,
        result: result
      }
    )

    if status == "error", do: report_steps_failure(state, operator_id, error)

    # Finished-job retention: record the terminal outcome (durably, before the
    # teardown) so `flz operator info` can surface it and `stop_operator`
    # retains the state instead of deleting it. `finished_at` is stamped
    # controller-side (the wire carries no timestamp).
    _ =
      Completion.write(operator_id, %{
        namespace: state.namespace,
        run: Atom.to_string(Records.run_of(operator_id)),
        status: status,
        error: error,
        result: result,
        finished_at: System.system_time(:second)
      })

    deregister_on_completion(state, operator_id)
  end

  defp report_steps_failure(state, operator_id, error) do
    Logger.warning(
      "[control_channel] steps operator #{inspect(operator_id)} failed in namespace " <>
        "#{inspect(state.namespace)}: #{inspect(error)}"
    )

    :telemetry.execute(
      [:flezha, :steps, :failed],
      %{count: 1},
      %{namespace: state.namespace, operator_id: operator_id, error: error}
    )
  end

  # Tear down off the control-channel process: `stop_operator` drives a
  # RemOperator round-trip whose CommandResponse returns on THIS control
  # stream, so calling it inline would deadlock the reader.
  defp deregister_on_completion(%{namespace: namespace, runner_id: runner_id}, operator_id) do
    Task.start(fn ->
      drain_owed_outputs(runner_id, operator_id)

      case OperatorManager.stop_operator(namespace, operator_id, reason: :completed) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.warning(
            "[control_channel] steps operator #{inspect(operator_id)} teardown failed: #{inspect(reason)}"
          )
      end
    end)
  end

  # A completion must not tear the Overwatch down under output still on its
  # way: a durable class's last stage output leaves its Cells' outboxes after
  # the commit ack on this channel, and an opted-out class's direct outputs
  # travel the data stream while the completion travels this one. Two debts,
  # settled differently:
  #
  #   * the data stream is FLUSHED, once, by a barrier across the runner's
  #     stream channels. Nothing is counted — counting what the runner wrote
  #     against what the controller ran is decidable only while nothing is
  #     ever lost, and a frame routed to an Overwatch that has since been
  #     replaced is exactly that loss.
  #   * the Cells' outboxes are WAITED for: each writer's committed entries
  #     are an exact, durable, re-drivable debt, and
  #     `Durations.get(:drain_owed_ms)` keeps a sink that never confirms from
  #     holding the teardown; the WAL keeps the debt. An entry still held for
  #     a stage marker belongs to a stage that never committed, and is owed
  #     nothing.

  defp drain_owed_outputs(runner_id, operator_id) do
    StreamPool.flush_channels(runner_id)

    await_owed_outboxes(
      operator_id,
      System.monotonic_time(:millisecond) + Durations.get(:drain_owed_ms)
    )
  end

  defp await_owed_outboxes(operator_id, deadline) do
    owed = TaskJournal.owed_outputs(operator_id)

    cond do
      owed == 0 ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        Logger.warning(
          "[control_channel] steps operator #{inspect(operator_id)} completed with " <>
            "#{owed} outbox entries still owed"
        )

      true ->
        Process.sleep(50)
        await_owed_outboxes(operator_id, deadline)
    end
  end

  @doc """
  An instance has been idle for its class's `sleep_after` and stopped
  without a completion (`durable-suspension`). Hand it to its
  Overwatch, which writes the park record durably before it evicts the instance
  from this runner and keeps it resumable; with no Overwatch here, the record
  is written under the instance's own key so the sleep is rediscovered when the
  instance is next started.

  Off this process: the eviction drives a `RemOperator` whose response returns
  on this control stream, so doing it inline would deadlock the reader, the
  same reason as the steps-completed teardown.
  """
  @spec iter_park(map(), RunnerCommands.IterPark.t()) :: :ok
  def iter_park(state, %RunnerCommands.IterPark{} = park) do
    if Records.admit(state, park.operator_id, :iter_park), do: park_instance(state, park)
    :ok
  end

  defp park_instance(state, park) do
    %RunnerCommands.IterPark{
      operator_id: operator_id,
      iter: iter,
      wake_deadline_ms: wake_deadline_ms,
      consumed_offset: consumed_offset
    } = park

    deadline = if wake_deadline_ms == 0, do: nil, else: wake_deadline_ms

    :telemetry.execute(
      [:flezha, :steps, :parked],
      %{count: 1},
      %{namespace: state.namespace, operator_id: operator_id, iter: iter}
    )

    namespace = state.namespace
    park = %{wake_deadline_ms: deadline}

    Task.start(fn ->
      # Journaled before the park is recorded, and before the suspension asks
      # the log whether input is still owed: a message the run parked unread
      # is consumed, not owed, and the journal keeps that across a restart.
      with :ok <- TaskJournal.release_unread(operator_id, consumed_offset),
           do: Migration.suspend(namespace, operator_id, iter, park)
    end)
  end

  @doc """
  The runner reports that an instance's process exited abnormally and that it
  no longer hosts it. Tell the instance's Overwatch, which parks it for the
  convergence loop's rebind when the report is current.

  Off this process, behind a flush of the runner's stream channels: every
  frame the dead incarnation wrote is read before the rebind restores its
  Cells, and the flush waits on channels this process does not own.
  """
  @spec operator_exited(map(), RunnerCommands.OperatorExited.t()) :: :ok
  def operator_exited(state, %RunnerCommands.OperatorExited{} = exited) do
    %RunnerCommands.OperatorExited{operator_id: operator_id, reason: reason} = exited
    %{namespace: namespace, runner_id: runner_id} = state

    if Records.admit(state, operator_id, :operator_exited) do
      Task.start(fn ->
        StreamPool.flush_channels(runner_id)
        Migration.operator_exited(namespace, operator_id, runner_id, reason)
      end)
    end

    :ok
  end

  @doc """
  A durability barrier (`TaskCommit` or `IterCommit`), handed to the runner's
  commit worker so this process does no disk I/O. A barrier from a runner that
  does not own the instance is answered with an error and journals nothing,
  which ends the sending driver's run.
  """
  @spec barrier(map(), :task_commit | :iter_commit, struct(), binary()) :: :ok
  def barrier(state, kind, commit, reference_id) do
    if Records.admit(state, commit.operator_id, kind),
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
                               run: run,
                               each: each,
                               on_failure: on_failure
                             } ->
        %{
          "id" => id,
          "description" => if(description == "", do: nil, else: description),
          "sdk_lang" => sdk_lang,
          "run" => run,
          "each" => each,
          "on_failure" => on_failure
        }
      end)

    GenServer.cast(
      FleetManager,
      {:sync_factories, state.namespace, state.runner_id, factories}
    )
  end

  @doc """
  A tasklet asked for a steps instance (`ctx.spawn_steps`), judged as
  the identity this connection proved. The namespace is this channel's, never
  the requester's claim, so a request cannot reach another namespace.
  """
  @spec spawn_steps(map(), RunnerCommands.SpawnStepsRequest.t(), binary()) :: :ok
  def spawn_steps(state, request, reference_id) do
    %RunnerCommands.SpawnStepsRequest{
      instance_id: instance_id,
      factory_id: factory_id,
      payload: payload,
      if_running: if_running,
      identity: identity
    } = request

    namespace = state.namespace

    answer_off_process(reference_id, fn ->
      judged =
        once_admitted(state, fn ->
          OperatorManager.spawn_or_deliver(
            namespace,
            instance_id,
            factory_id,
            payload,
            if_running(if_running),
            Session.runner_caller_context(namespace),
            identity
          )
        end)

      # A session refused before it was admitted judged nothing, so it answers
      # the id the request carried.
      {outcome, resolved_id} =
        case judged do
          {:refused, %Flezha.Error{}} -> {judged, instance_id}
          {_outcome, _resolved_id} = answered -> answered
        end

      :telemetry.execute(
        [:flezha, :operator, :requested_spawn],
        %{count: 1},
        %{
          namespace: namespace,
          instance_id: resolved_id,
          factory_id: factory_id,
          outcome: outcome_name(outcome)
        }
      )

      {{:spawn_steps_response, spawn_response(outcome, resolved_id)},
       "spawn-steps response #{outcome_name(outcome)}"}
    end)
  end

  defp spawn_response({:refused, error}, instance_id),
    do: %RunnerCommands.SpawnStepsResponse{
      error: Flezha.Error.to_wire(error),
      instance_id: instance_id
    }

  defp spawn_response(outcome, instance_id),
    do: %RunnerCommands.SpawnStepsResponse{
      outcome: Atom.to_string(outcome),
      instance_id: instance_id
    }

  defp outcome_name({:refused, error}), do: error.code
  defp outcome_name(outcome), do: outcome

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
      response =
        case once_admitted(state, fn -> Admin.call(namespace, verb, args) end) do
          {:ok, json} ->
            %RunnerCommands.AdminResponse{result: json}

          {tag, error} when tag in [:refused, :error] ->
            %RunnerCommands.AdminResponse{error: Flezha.Error.to_wire(error)}
        end

      :telemetry.execute(
        [:flezha, :runner, :admin_request],
        %{count: 1},
        %{namespace: namespace, verb: verb, refused: response.error != nil}
      )

      {{:admin_response, response}, "admin response"}
    end)
  end

  # A request whose answer cannot be worked out on the channel's process:
  # resolving it drives an `AddOperator` or a `RemOperator` whose response
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

  # A runner can send a request before its handshake completes, and the roster
  # holds neither the runner nor its factories until then, so a request judged
  # against it waits for the session to be admitted.
  defp once_admitted(state, judge) do
    case FleetManager.await_admitted(state.runner_id) do
      :ok ->
        judge.()

      {:error, :not_admitted} ->
        {:refused, Flezha.Error.from_reason(:not_admitted, :"wire.spawn")}
    end
  end
end
