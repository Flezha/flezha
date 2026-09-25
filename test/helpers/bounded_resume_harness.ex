defmodule FlezhaTest.Helper.BoundedResumeHarness do
  @moduledoc """
  What a durable-bounded end-to-end scenario drives: a runner on its
  transport, a bounded operator on it, an output sink that collects into a
  process, and the full SIGKILL-mid-run to respawn to resume flow.

  Boots on `ctx.transport` rather than the UDS the tier harness pins, and waits
  on the terminal completion record rather than on a replacement Overwatch — a
  resumed chain can run to completion and deregister between two polls.
  """

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [cleanup: 3, kill_overwatch: 2, os_pid: 1, sigkill: 1, unique: 1, unique_id_path: 0]

  alias Flezha.Operator.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias Flezha.State.TaskJournal
  alias Flezha.SDK.RecordFrame
  alias FlezhaTest.Helper.NodeTestHelper

  def collect_loop(acc) do
    receive do
      {:message_forward, msg, md} -> collect_loop([{to_string(msg), md} | acc])
      {:collected?, from} -> send(from, {:collected, acc}) && collect_loop(acc)
    end
  end

  def collected(collector), do: collector |> collected_with_meta() |> Enum.map(&elem(&1, 0))

  # Everything the sink received, oldest first, with the frame metadata (the
  # `flz.*` identity stamps a flushed outbox entry carries).
  def collected_with_meta(collector) do
    send(collector, {:collected?, self()})

    receive do
      {:collected, acc} -> Enum.reverse(acc)
    after
      1_000 -> []
    end
  end

  def forward_sink(id, collector) do
    %Flezha.Plugin{
      id: id,
      namespace: nil,
      operator_id: nil,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: collector, include_metadata: true},
      hooks: [{:on_output, []}]
    }
  end

  # Wait for the sink to receive at least `count` frames and for the instance's
  # Cell writers to owe nothing more, then take everything: an unexpected EXTRA
  # delivery still fails the exact-count assertions, because it lands before
  # the outboxes are settled.
  def await_outputs(collector, count, operator_id) do
    NodeTestHelper.wait_until(fn -> length(collected(collector)) >= count end, 30_000)
    assert outboxes_settled?(operator_id), "a Cell writer still owes output"
    collected_with_meta(collector)
  end

  def outboxes_settled?(operator_id),
    do:
      NodeTestHelper.wait_until(
        fn -> TaskJournal.owed_outputs(operator_id) == 0 end,
        30_000
      )

  # --- durable-loop helpers ---------------------------------------------------

  def send_turn(namespace, operator_id, text, channel \\ "in") do
    :ok = OperatorManager.send_io(operator_id, namespace, text, %{"channel" => channel})
  end

  def committed_iters(operator_id) do
    {:ok, entries} = TaskJournal.read(operator_id)
    for %{type: :iter_committed, iter: iter} <- entries, do: iter
  end

  def committed_tasks(operator_id) do
    {:ok, entries} = TaskJournal.read(operator_id)
    for %{type: :task_committed, task_idx: idx} <- entries, do: idx
  end

  # Compensation-done markers the controller wrote, one per compensated Cell,
  # across the operator or under one tasklet.
  def comp_done_count(operator_id, tasklet \\ "**") do
    [Paths.class_dir(operator_id), tasklet, "*.comp_done"]
    |> Path.join()
    |> Path.wildcard()
    |> length()
  end

  def consumed_offsets(operator_id) do
    {:ok, entries} = TaskJournal.read(operator_id)
    for %{type: :iter_committed, payload: %{consumed_offset: o}} <- entries, do: o
  end

  # A pass in flight: its root committed, the iteration not yet.
  def root_committed_without_iteration?(operator_id) do
    {:ok, entries} = TaskJournal.read(operator_id)

    Enum.any?(entries, &(&1.type == :task_committed and &1.task_idx == 0)) and
      not Enum.any?(entries, &(&1.type == :iter_committed))
  end

  def boot_runner(namespace, ctx, id_path, opts) do
    env =
      [{"FLEZHA_RUNNER_ID_PATH", id_path}] ++
        Keyword.get(opts, :extra_env, []) ++
        if Keyword.fetch!(opts, :release?), do: [{"FLEZHA_BOUNDED_E2E_RELEASE", "1"}], else: []

    {port, runner_id} =
      NodeTestHelper.start_runner_port(namespace,
        transport: ctx.transport,
        env: env
      )

    # Close the port at teardown so the runner process exits (it blocks on
    # stdin until then); the SIGKILLed runner is already dead, so this is a
    # no-op for it.
    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)
    {port, runner_id}
  end

  # Start one bounded operator of `factory_id` on `namespace`, returning the
  # start info. Every once chain here takes the same first message,
  # `{"seed":"go"}`, delivered as `flz operator start --text` delivers one; a
  # loop passes `first_message: nil` and sends its turns itself.
  def start_bounded(namespace, operator_id, factory_id, opts) do
    {first_message, opts} = Keyword.pop(opts, :first_message, ~s({"seed":"go"}))
    opts = if first_message, do: [{:seed_input, {first_message, %{}}} | opts], else: opts

    builder = %Builder{
      id: operator_id,
      factory_id: factory_id,
      kind: :bounded,
      init_values: %{},
      opts: opts
    }

    assert {:ok, info} = OperatorManager.start_operator(namespace, builder)
    info
  end

  # Wait for the run's terminal completion record to carry `status`.
  def await_status(operator_id, status, timeout_ms) do
    assert NodeTestHelper.wait_until(
             fn -> match?({:ok, %{status: ^status}}, Completion.read(operator_id)) end,
             timeout_ms
           ),
           "run never completed with status #{inspect(status)}"
  end

  # Asleep: evicted from its runner, its Overwatch kept.
  def suspended?(namespace, operator_id) do
    match?(
      {:ok, %{suspended: true}},
      OperatorManager.get_overwatch_state(namespace, operator_id)
    )
  end

  def registered?(namespace, operator_id) do
    match?({:ok, pid} when is_pid(pid), HubManager.get_pid(namespace, operator_id))
  end

  # Drive one operator through the full SIGKILL-mid-run → respawn → resume flow
  # and assert the terminal outcome. `mid_run?` is a predicate on the operator
  # id that becomes true once the durable mid-run point is reached on the first
  # runner (which never completes it — the release env is unset there).
  # Returns the operator id for scenario-specific assertions.
  def drive_crash_resume(ctx, factory_id, terminal_status, mid_run?, opts \\ []) do
    {before_rebind, builder_opts} = Keyword.pop(opts, :before_rebind)
    namespace = unique("bounded_crash")
    operator_id = unique(factory_id)
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operator_id, id_path) end)

    # Runner A (no release env): the operator commits, then blocks mid-chain.
    {port_a, exec_a} = boot_runner(namespace, ctx, id_path, release?: false)

    info = start_bounded(namespace, operator_id, factory_id, builder_opts)
    assert info.runner_id == exec_a

    assert NodeTestHelper.wait_until(fn -> mid_run?.(operator_id) end, 30_000),
           "operator never reached its durable mid-run point on the first runner"

    refute Completion.exists?(operator_id),
           "operator terminated before the crash — it did not block mid-chain"

    assert registered?(namespace, operator_id)

    # SIGKILL A; the replacement (release env set) re-presents A's stable id, so
    # its connect swaps the bindings + evicts the dead session on both transports.
    sigkill(os_pid(port_a))
    {_port_b, exec_b} = boot_runner(namespace, ctx, id_path, release?: true)
    assert exec_b != exec_a, "replacement must mint a fresh session token"

    assert NodeTestHelper.wait_until(
             fn -> exec_a not in FleetManager.active_runners(namespace) end,
             15_000
           ),
           "dead runner was never evicted after the replacement connected"

    # Scenario hook on the durable state the crash left, before the rebind
    # (which is what triggers the restore).
    if before_rebind, do: before_rebind.(operator_id)

    kill_overwatch(namespace, operator_id)

    # The resumed operator completes with the expected terminal status.
    await_status(operator_id, terminal_status, 60_000)

    assert NodeTestHelper.wait_until(fn -> not registered?(namespace, operator_id) end, 15_000),
           "completed operator was never deregistered"

    operator_id
  end

  @doc """
  Every record seq in a WAL file's bytes, newest first, through the one walk
  that reads the framing.
  """
  def wal_seqs(bytes, acc) do
    {:ok, seqs} =
      RecordFrame.fold_bytes(bytes, {:log, "wal_seqs"}, acc, fn {seq, _h, _b, _raw}, acc ->
        {:cont, [seq | acc]}
      end)

    seqs
  end
end
