defmodule FlezhaTest.Helper.StepsResumeHarness do
  @moduledoc """
  What a durable-steps end-to-end scenario drives: a runner on its
  transport, a steps relay on it, an output sink that collects into a
  process, and the full SIGKILL-mid-run to respawn to resume flow.

  Boots on `ctx.transport` rather than the UDS the tier harness pins, and waits
  on the run's outcome (`Flezha.Service.Steps.outcome/2`) rather than on a
  replacement Overwatch — a resumed chain can run to completion and deregister
  between two polls.
  """

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [cleanup: 3, os_pid: 1, sigkill: 1, unique: 1, unique_id_path: 0]

  alias Flezha.Relay.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.RelayTestHelper
  alias Flezha.Service.RelayManager.Init
  alias Flezha.Service.Steps
  alias Flezha.State.Paths
  alias Flezha.State.TaskJournal
  alias Flezha.State.TaskJournal.Records
  alias Flezha.State.Frame
  alias FlezhaTest.Fixtures.RelayFixture
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
      relay_id: nil,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: collector, include_metadata: true},
      hooks: [{:on_output, []}]
    }
  end

  # Wait for the sink to receive at least `count` frames and for the instance's
  # Cell writers to owe nothing more, then take everything: an unexpected EXTRA
  # delivery still fails the exact-count assertions, because it lands before
  # the outboxes are settled.
  def await_outputs(collector, count, relay_id) do
    NodeTestHelper.wait_until(fn -> length(collected(collector)) >= count end, 30_000)
    assert outboxes_settled?(relay_id), "a Cell writer still owes output"
    collected_with_meta(collector)
  end

  def outboxes_settled?(relay_id),
    do:
      NodeTestHelper.wait_until(
        fn -> Records.owed_outputs(relay_id) == 0 end,
        30_000
      )

  # --- durable-loop helpers ---------------------------------------------------

  def send_turn(namespace, relay_id, text, channel \\ "in") do
    :ok = RelayTestHelper.cast_io(relay_id, namespace, text, %{"channel" => channel})
  end

  # The committed iterations as `node`'s journal holds them; `[]` while it
  # holds none, as during a move.
  def committed_iters(relay_id, node \\ node()) do
    case :erpc.call(node, TaskJournal, :read, [relay_id]) do
      {:ok, entries} -> for %{type: :iter_committed, iter: iter} <- entries, do: iter
      _ -> []
    end
  end

  # --- two-controller helpers -------------------------------------------------

  # A steps relay the hub placed on `peer`, its outputs forwarded to `collector`.
  def start_steps_on_peer(peer, namespace, factory_id, collector) do
    NodeTestHelper.await_serving(peer, namespace, factory_id)

    Enum.find_value(1..20, fn i ->
      id = "#{factory_id}_#{System.unique_integer([:positive])}_#{i}"

      builder = %Builder{
        id: id,
        factory_id: factory_id,
        run: :steps,
        init_values: %{},
        opts: [bindings: RelayFixture.bindings(namespace, id, [forward_sink("out", collector)])]
      }

      {:ok, %{node: placed}} = RelayManager.start_relay(namespace, builder)

      if placed == peer,
        do: id,
        else: RelayManager.request_stop(namespace, id) && nil
    end) || flunk("no steps relay landed on #{inspect(peer)} in 20 attempts")
  end

  def committed_tasks(relay_id) do
    {:ok, entries} = TaskJournal.read(relay_id)
    for %{type: :task_committed, task_idx: idx} <- entries, do: idx
  end

  # Compensation-done markers the controller wrote, one per compensated Cell,
  # across the relay or under one tasklet.
  def comp_done_count(relay_id, tasklet \\ "**") do
    [Paths.class_dir(relay_id), tasklet, "*.comp_done"]
    |> Path.join()
    |> Path.wildcard()
    |> length()
  end

  def consumed_offsets(relay_id) do
    {:ok, entries} = TaskJournal.read(relay_id)
    for %{type: :iter_committed, payload: %{consumed_offset: o}} <- entries, do: o
  end

  # A pass in flight: its root committed, the iteration not yet.
  def root_committed_without_iteration?(relay_id) do
    {:ok, entries} = TaskJournal.read(relay_id)

    Enum.any?(entries, &(&1.type == :task_committed and &1.task_idx == 0)) and
      not Enum.any?(entries, &(&1.type == :iter_committed))
  end

  def boot_runner(namespace, ctx, id_path, opts) do
    env =
      [{"FLEZHA_RUNNER_ID_PATH", id_path}] ++
        Keyword.get(opts, :extra_env, []) ++
        if Keyword.fetch!(opts, :release?), do: [{"FLEZHA_STEPS_E2E_RELEASE", "1"}], else: []

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

  # Start one steps relay of `factory_id` on `namespace`, returning the
  # start info. Every once chain here takes the same first message,
  # `{"seed":"go"}`, delivered as `flz relay start --text` delivers one; a
  # loop passes `first_message: nil` and sends its turns itself.
  #
  # `plugins:` are enabled for this relay and bound
  # (`FlezhaTest.Fixtures.RelayFixture.bindings/3`).
  def start_steps(namespace, relay_id, factory_id, opts) do
    {first_message, opts} = Keyword.pop(opts, :first_message, ~s({"seed":"go"}))
    opts = if first_message, do: [{:seed_input, {first_message, %{}}} | opts], else: opts
    opts = bound(namespace, relay_id, opts)

    builder = %Builder{
      id: relay_id,
      factory_id: factory_id,
      run: :steps,
      init_values: %{},
      opts: opts
    }

    assert {:ok, info} = RelayManager.start_relay(namespace, builder)
    info
  end

  # Start one flow relay of `factory_id` on `namespace`.
  def start_stream(namespace, relay_id, factory_id, opts) do
    opts = bound(namespace, relay_id, opts)
    builder = %Builder{id: relay_id, factory_id: factory_id, init_values: %{}, opts: opts}
    assert {:ok, info} = RelayManager.start_relay(namespace, builder)
    info
  end

  # One input on the relay's first entry channel, sent until the relay accepts
  # it: a cast drops an input a flow relay is not yet bound to deliver.
  def send_input(namespace, relay_id, text) do
    deadline = System.monotonic_time(:millisecond) + 30_000
    RelayTestHelper.send_confirmed(relay_id, namespace, text, %{}, deadline)
  end

  # Wait for the run's outcome to carry `status`, or every field of a map
  # (`state_kept`, `result`, ...), and return it.
  def await_status(namespace, relay_id, status, timeout_ms) when is_binary(status),
    do: await_status(namespace, relay_id, %{status: status}, timeout_ms)

  def await_status(namespace, relay_id, fields, timeout_ms) do
    reached? = fn ->
      case Steps.outcome(namespace, relay_id) do
        {:ok, outcome} -> Map.take(outcome, Map.keys(fields)) == fields
        :error -> false
      end
    end

    assert NodeTestHelper.wait_until(reached?, timeout_ms),
           "run never ended with #{inspect(fields)}: #{inspect(Steps.outcome(namespace, relay_id))}"

    {:ok, outcome} = Steps.outcome(namespace, relay_id)
    outcome
  end

  # Keep every finished run's state for the calling test, for the scenarios
  # that read a run's journal, Cells or result after it ended.
  def keep_finished do
    on_exit(Flezha.State.TestHelpers.override_env(:keep_finished_by_default, true))
    :ok
  end

  # Move the run window's recorded end into the past, as if the window ran out
  # while nothing ran it: the next bind carries 0 and the run ends at once. The
  # transform is built here so a peer's registry can resolve it.
  def spend_window(namespace, relay_id) do
    spent = System.os_time(:millisecond) - 1
    put = &Keyword.put(&1, :max_duration_ends_at, spent)

    assert :ok =
             Init.write_plugins(namespace, relay_id, fn cspec, node_pids, metadata ->
               {HubManager.rewrite_cspec_opts(cspec, put), node_pids, metadata}
             end)
  end

  # Asleep: evicted from its runner, its Overwatch kept.
  def suspended?(namespace, relay_id) do
    match?(
      {:ok, %{phase: :suspended}},
      RelayManager.get_overwatch_state(namespace, relay_id)
    )
  end

  def registered?(namespace, relay_id) do
    match?({:ok, pid} when is_pid(pid), HubManager.get_pid(namespace, relay_id))
  end

  # Drive one relay through the full SIGKILL-mid-run → respawn → resume flow
  # and assert the terminal outcome. `mid_run?` is a predicate on the relay
  # id that becomes true once the durable mid-run point is reached on the first
  # runner (which never completes it — the release env is unset there).
  # `rebind:` is how the instance reaches the replacement: `:restart` (its
  # connect on the same identity binds it) or `:heal` (a replacement on a new
  # identity, which the running convergence loop moves it onto).
  # Returns the relay id for scenario-specific assertions.
  def drive_crash_resume(ctx, factory_id, terminal_status, mid_run?, opts \\ []) do
    {before_rebind, opts} = Keyword.pop(opts, :before_rebind)
    {rebind, builder_opts} = Keyword.pop(opts, :rebind, :restart)
    namespace = unique("steps_crash")
    relay_id = unique(factory_id)
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    # Runner A (no release env): the relay commits, then blocks mid-chain.
    {port_a, exec_a} = boot_runner(namespace, ctx, id_path, release?: false)

    info = start_steps(namespace, relay_id, factory_id, builder_opts)
    assert info.runner_id == exec_a

    assert NodeTestHelper.wait_until(fn -> mid_run?.(relay_id) end, 30_000),
           "relay never reached its durable mid-run point on the first runner"

    assert Steps.outcome(namespace, relay_id) == :error,
           "relay terminated before the crash — it did not block mid-chain"

    assert registered?(namespace, relay_id)

    # `before_rebind` reads the durable state the crash left, before the rebind
    # (which is what triggers the restore); `:heal` gives the replacement an
    # identity of its own.
    id_path_b = if rebind == :heal, do: unique_id_path(), else: id_path

    replace_runner(namespace, ctx, {port_a, exec_a}, id_path_b,
      before_boot: fn -> if before_rebind, do: before_rebind.(relay_id) end
    )

    # The resumed relay completes with the expected terminal status.
    await_status(namespace, relay_id, terminal_status, 60_000)

    assert NodeTestHelper.wait_until(fn -> not registered?(namespace, relay_id) end, 15_000),
           "completed relay was never deregistered"

    relay_id
  end

  # SIGKILL the runner; a released replacement booted on `id_path` re-presents
  # its stable id with a new incarnation (or, on an id of its own, is healed
  # onto), so its connect evicts the dead session and the instance is bound
  # onto it. `before_boot` runs between the kill and the boot. Returns the
  # replacement's `{port, runner_id}`.
  def replace_runner(namespace, ctx, {port, dead}, id_path, opts \\ []) do
    sigkill(os_pid(port))
    if before_boot = Keyword.get(opts, :before_boot), do: before_boot.()
    {_port, runner_id} = replacement = boot_runner(namespace, ctx, id_path, release?: true)
    assert runner_id != dead, "replacement must mint a fresh session token"

    assert NodeTestHelper.wait_until(
             fn -> dead not in FleetManager.active_runners(namespace) end,
             15_000
           ),
           "dead runner was never evicted after the replacement connected"

    replacement
  end

  @doc """
  Every record seq in a WAL file's bytes, newest first, through the one walk
  that reads the framing.
  """
  def wal_seqs(bytes, acc) do
    {:ok, seqs} =
      Frame.fold_bytes(bytes, {:log, "wal_seqs"}, acc, fn {seq, _h, _b, _raw}, acc ->
        {:cont, [seq | acc]}
      end)

    seqs
  end

  defp bound(namespace, relay_id, opts) do
    case Keyword.pop(opts, :plugins) do
      {nil, opts} ->
        opts

      {plugins, opts} ->
        [{:bindings, RelayFixture.bindings(namespace, relay_id, plugins)} | opts]
    end
  end
end
