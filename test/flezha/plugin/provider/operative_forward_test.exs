defmodule FlezhaTest.Plugin.Provider.OperativeForwardTest do
  @moduledoc """
  `operative-to-operative-messaging`: the forward provider, its enable-time
  refusals, and the delivery worker's retry / terminal / back-pressure
  behaviour.

  The target operative is a stand-in process answering `{:ingest, _, _}` with a
  scripted verdict, resolved through the real
  `OperativeManager.send_io_confirming/5` path — so the worker exercises the
  production call, not a test double of it.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Plugin.Extension.{Ctx, Table}
  alias Flezha.Plugin.Provider.OperativeForward
  alias Flezha.Plugin.Provider.OperativeForward.{ForwardSup, ForwardWorker}
  alias Flezha.Service.HubManager

  setup :set_mimic_global

  setup do
    unless Process.whereis(ForwardSup), do: start_supervised!(ForwardSup)

    # One namespace unless a test says otherwise: only the cross-namespace
    # refusal cares which namespaces are running.
    stub(HubManager, :get_running_namespaces, fn -> [:default] end)
    :ok
  end

  defp uniq(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  # A stand-in target Overwatch: answers `{:ingest, _, _}` with whatever
  # `script.(message)` returns and reports every attempt to the test.
  defp fake_target(script) do
    test_pid = self()

    spawn_link(fn -> target_loop(test_pid, script) end)
  end

  defp target_loop(test_pid, script) do
    receive do
      {:"$gen_call", {from, ref}, {:ingest, message, metadata}} ->
        send(test_pid, {:target_got, message, metadata})
        send(from, {ref, script.(message)})
        target_loop(test_pid, script)
    end
  end

  defp resolve_to(%{} = targets) do
    stub(HubManager, :get_pid, fn _ns, id ->
      case Map.fetch(targets, id) do
        {:ok, pid} -> {:ok, pid}
        :error -> {:error, "no pid found"}
      end
    end)
  end

  defp prepare(source, target, extra \\ %{}) do
    opts =
      Map.merge(
        %{
          target_operative_id: target,
          source_namespace: "default",
          source_operative_id: source,
          plugin_id: uniq("plug")
        },
        extra
      )

    OperativeForward.init_provider(opts)
  end

  defp emit(opts, message, metadata \\ %{}) do
    ctx = %Ctx.IO{
      operative_id: opts.source_operative_id,
      namespace: "default",
      message: message,
      metadata: metadata
    }

    OperativeForward.on_output(ctx, opts)
  end

  ## ---------- enable-time refusals (tasks 3.1 – 3.5) ----------

  test "the provider is reachable by name, so the CLI can attach it" do
    # The only registry the CLI resolves a `--plugins` / `flz plugin enable`
    # provider id through; absent here, every non-Elixir caller is locked out.
    assert Flezha.Service.ConfigManager.get_enabled_plugins()["operative_forward"] ==
             OperativeForward
  end

  test "a missing target is refused, naming the option" do
    assert {:error, message} =
             OperativeForward.init_provider(%{
               source_namespace: "default",
               source_operative_id: "a"
             })

    assert message =~ ":target_operative_id"
  end

  test "a self-target is refused, identifying the self-target" do
    assert {:error, message} = prepare("solo", "solo")
    assert message =~ "solo"
    assert message =~ "the operative itself"
  end

  test "a target resolving in another namespace is refused at the boundary" do
    target = fake_target(fn _ -> :ok end)

    stub(HubManager, :get_running_namespaces, fn -> [:default, :other] end)

    stub(HubManager, :get_pid, fn
      "other", "elsewhere" -> {:ok, target}
      _ns, _id -> {:error, "no pid found"}
    end)

    assert {:error, message} = prepare("a", "elsewhere")
    assert message =~ "namespace"
    assert message =~ "other"
  end

  test "a two-operative cycle is accepted by both enable calls" do
    a = fake_target(fn _ -> :ok end)
    b = fake_target(fn _ -> :ok end)
    resolve_to(%{"a" => a, "b" => b})

    assert {:ok, _opts, _pid} = prepare("a", "b")
    assert {:ok, _opts, _pid} = prepare("b", "a")
  end

  test "an unknown option is rejected by build/1 rather than ignored" do
    assert {:error, message} =
             OperativeForward.build(%{
               "target_operative_id" => "b",
               "delivery_mode" => "exactly_once"
             })

    assert message =~ "delivery_mode"

    assert {:ok, %OperativeForward{}} =
             OperativeForward.build(%{"target_operative_id" => "b", "queue_max" => "5"})
  end

  ## ---------- delivery (tasks 5.1 – 5.6) ----------

  test "the callback enqueues and returns without waiting for a slow target" do
    slow = spawn_link(fn -> Process.sleep(:infinity) end)
    resolve_to(%{"slow" => slow})

    {:ok, opts, _pid} = prepare("src", "slow")

    # A blocking design would sit here for the full call timeout.
    task = Task.async(fn -> emit(opts, "payload") end)
    assert {:ok, %Ctx.IO{}} = Task.await(task, 500)
  end

  test "identity stamps survive the hop unmodified" do
    target = fake_target(fn _ -> :ok end)
    resolve_to(%{"sink" => target})

    {:ok, opts, _pid} = prepare("src", "sink")

    stamps = %{"flz.seq" => "12.0", "flz.key" => "BTC-USD", "flz.epoch" => "3"}
    emit(opts, "candle", stamps)

    assert_receive {:target_got, "candle", ^stamps}, 1_000
  end

  test "terminal verdicts are not retried" do
    for verdict <- [:ok, {:ok, :absorbed}, {:ok, :suspended}] do
      target = fake_target(fn _ -> verdict end)
      id = uniq("term")
      resolve_to(%{id => target})

      {:ok, opts, worker} = prepare(uniq("src"), id)
      emit(opts, "once")

      assert_receive {:target_got, "once", _}, 1_000
      refute_receive {:target_got, "once", _}, 200

      assert %{buffer_len: 0, attempts: 0} = ForwardWorker.stats(worker)
    end
  end

  test "a parked target is retried until it recovers, and nothing is dropped" do
    id = uniq("parked")
    counter = :counters.new(1, [])

    target =
      fake_target(fn _ ->
        :counters.add(counter, 1, 1)
        if :counters.get(counter, 1) < 3, do: {:error, :not_ready}, else: :ok
      end)

    resolve_to(%{id => target})

    {:ok, opts, worker} = prepare(uniq("src"), id, %{retry_backoff_ms: 10})
    emit(opts, "held")

    assert_receive {:target_got, "held", _}, 1_000
    assert_receive {:target_got, "held", _}, 1_000
    assert_receive {:target_got, "held", _}, 1_000

    wait_until(fn -> ForwardWorker.stats(worker).buffer_len == 0 end)
    assert %{attempts: 0} = ForwardWorker.stats(worker)
  end

  test "an unresolvable target is retried, then delivered once it resolves" do
    id = uniq("late")
    resolve_to(%{})

    {:ok, opts, worker} = prepare(uniq("src"), id, %{retry_backoff_ms: 10})
    emit(opts, "waiting")

    # Held, not dropped, while the target does not exist.
    Process.sleep(50)
    assert ForwardWorker.stats(worker).buffer_len == 1

    resolve_to(%{id => fake_target(fn _ -> :ok end)})

    assert_receive {:target_got, "waiting", _}, 2_000
    wait_until(fn -> ForwardWorker.stats(worker).buffer_len == 0 end)
  end

  test "a saturated queue parks the enqueueing caller" do
    id = uniq("stuck")
    resolve_to(%{id => spawn_link(fn -> Process.sleep(:infinity) end)})

    {:ok, opts, worker} = prepare(uniq("src"), id, %{queue_max: 2, call_timeout_ms: 50})

    emit(opts, "one")
    parked = Task.async(fn -> emit(opts, "two") end)

    # The second enqueue fills the queue to `:queue_max` and parks its caller —
    # back-pressure into the source Overwatch, not a drop.
    assert catch_exit(Task.await(parked, 200))
    assert ForwardWorker.stats(worker).buffer_len >= 2
  end

  test "a re-prepared plugin reuses its live worker and keeps queued items" do
    id = uniq("reuse")
    resolve_to(%{id => spawn_link(fn -> Process.sleep(:infinity) end)})

    plugin_id = uniq("plug")
    source = uniq("src")

    {:ok, opts, worker} = prepare(source, id, %{plugin_id: plugin_id, call_timeout_ms: 50})
    emit(opts, "queued")

    {:ok, _opts2, worker2} = prepare(source, id, %{plugin_id: plugin_id, call_timeout_ms: 50})

    assert worker2 == worker
    assert ForwardWorker.stats(worker).buffer_len >= 1
  end

  test "two operatives forwarding to each other neither deadlock nor lose a message" do
    a_id = uniq("a")
    b_id = uniq("b")
    test_pid = self()

    # Each stand-in Overwatch does what a real one does on `:on_output`: it
    # enqueues into its own forward worker from inside its own callback. If the
    # worker blocked on delivery, this is the four-way cycle that would wedge.
    {:ok, a_opts, _} = prepare(a_id, b_id, %{plugin_id: uniq("pa")})
    {:ok, b_opts, _} = prepare(b_id, a_id, %{plugin_id: uniq("pb")})

    a = spawn_link(fn -> peer_loop(test_pid, a_id, a_opts, 1) end)
    b = spawn_link(fn -> peer_loop(test_pid, b_id, b_opts, 1) end)

    resolve_to(%{a_id => a, b_id => b})

    Task.async(fn -> emit(a_opts, "ping-from-a") end) |> Task.await(1_000)
    Task.async(fn -> emit(b_opts, "ping-from-b") end) |> Task.await(1_000)

    assert_receive {:peer_got, ^b_id, "ping-from-a"}, 2_000
    assert_receive {:peer_got, ^a_id, "ping-from-b"}, 2_000

    # Each peer echoed once from inside its own ingest handler, so both workers
    # were mid-delivery while both peers were mid-enqueue — the four-way cycle.
    assert_receive {:peer_got, ^a_id, "echo-" <> _}, 2_000
    assert_receive {:peer_got, ^b_id, "echo-" <> _}, 2_000
  end

  # A stand-in Overwatch that echoes back through its OWN forward plugin from
  # inside its ingest handler, the way a real one would from `:on_output`. The
  # echo budget keeps the cycle finite.
  defp peer_loop(test_pid, id, forward_opts, echo_budget) do
    receive do
      {:"$gen_call", {from, ref}, {:ingest, message, _metadata}} ->
        send(test_pid, {:peer_got, id, message})

        budget =
          if echo_budget > 0 do
            emit(forward_opts, "echo-#{id}")
            echo_budget - 1
          else
            echo_budget
          end

        send(from, {ref, :ok})
        peer_loop(test_pid, id, forward_opts, budget)
    end
  end

  ## ---------- plugin lifecycle (task 2.5) ----------

  test "disabling the plugin stops its worker for good" do
    id = uniq("bye")
    resolve_to(%{id => fake_target(fn _ -> :ok end)})

    source = uniq("src")

    plugin =
      Flezha.Plugin.new(
        id: "fwd",
        provider: OperativeForward,
        opts: %{target_operative_id: id},
        namespace: "default",
        operative_id: source,
        subscriptions: [{:on_output, []}]
      )

    table = Table.new(Flezha.Operative.Overwatch.__extensions_catalog__())
    {:ok, table} = Table.subscribe(table, "fwd", plugin)

    worker = table.plugins["fwd"].opts.worker_pid
    assert Process.alive?(worker)

    _table = Table.unsubscribe(table, "fwd")

    wait_until(fn -> not Process.alive?(worker) end)
    # A permanent child would be back by now; a transient one stays down.
    Process.sleep(50)

    assert ForwardSup.stop_worker(ForwardSup.key(plugin_id: "fwd", target_operative_id: id)) ==
             :ok
  end

  ## ---------- telemetry (task 4.2) ----------

  test "delivery, retry and queue-depth events reach the observability bus" do
    id = uniq("obs")
    counter = :counters.new(1, [])

    target =
      fake_target(fn _ ->
        :counters.add(counter, 1, 1)
        if :counters.get(counter, 1) < 2, do: {:error, :not_ready}, else: :ok
      end)

    resolve_to(%{id => target})

    test_pid = self()
    handler = {__MODULE__, System.unique_integer([:positive])}

    :telemetry.attach_many(
      handler,
      [
        [:flezha, :plugin, :operative_forward, :delivered],
        [:flezha, :plugin, :operative_forward, :retry],
        [:flezha, :plugin, :operative_forward, :queue_depth]
      ],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, List.last(event), measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    source = uniq("src")
    {:ok, opts, _worker} = prepare(source, id, %{retry_backoff_ms: 10})
    emit(opts, "observed")

    assert_receive {:telemetry, :queue_depth, %{depth: 1}, %{source_operative_id: ^source}}, 1_000
    assert_receive {:telemetry, :retry, %{attempt: 1}, %{target_operative_id: ^id}}, 1_000
    assert_receive {:telemetry, :delivered, %{count: 1}, %{target_operative_id: ^id}}, 2_000
  end

  ## ---------- effectively-once across the hop (task 5.4) ----------

  test "a retry after the target journaled the payload is absorbed, not duplicated" do
    id = uniq("eo")
    journaled = :counters.new(1, [])
    attempts = :counters.new(1, [])

    # Stands in for `journal_input/2`'s duplicate absorption: the first delivery
    # journals and then fails to answer (the sender never learns it landed); the
    # retry carries the same identity and is absorbed.
    target =
      fake_target(fn _ ->
        :counters.add(attempts, 1, 1)

        case :counters.get(attempts, 1) do
          1 ->
            :counters.add(journaled, 1, 1)
            {:error, :refused}

          _ ->
            {:ok, :absorbed}
        end
      end)

    resolve_to(%{id => target})

    {:ok, opts, worker} = prepare(uniq("src"), id, %{retry_backoff_ms: 10})

    stamps = %{"flz.seq" => "4.0", "flz.key" => "k", "flz.epoch" => "0"}
    emit(opts, "once-only", stamps)

    # Both attempts carry the same identity — that is what makes the second one
    # recognisable as a duplicate.
    assert_receive {:target_got, "once-only", ^stamps}, 1_000
    assert_receive {:target_got, "once-only", ^stamps}, 1_000

    wait_until(fn -> ForwardWorker.stats(worker).buffer_len == 0 end)
    assert :counters.get(journaled, 1) == 1
  end

  ## ---------- partitioned targets ----------

  describe "a partitioned target" do
    # ranges(2) = [{0, 127}, {128, 255}]; SOL-USD hashes to group 60 (p0),
    # BTC-USD to 129 and ETH-USD to 213 (both p1).
    defp group(base, count) do
      count
      |> Flezha.Partition.ranges()
      |> Enum.with_index()
      |> Enum.map(fn {range, index} ->
        %{
          id: Flezha.Partition.member_id(base, index),
          partition: %{group: base, index: index, count: count, range: range}
        }
      end)
    end

    # A target per member that names itself on every ingest, so a routing
    # assertion is about WHICH member received a payload, not merely that one did.
    defp labelled_target(member_id) do
      test_pid = self()
      spawn_link(fn -> labelled_loop(test_pid, member_id) end)
    end

    defp labelled_loop(test_pid, member_id) do
      receive do
        {:"$gen_call", {from, ref}, {:ingest, message, _metadata}} ->
          send(test_pid, {:member_got, member_id, message})
          send(from, {ref, :ok})
          labelled_loop(test_pid, member_id)
      end
    end

    defp partitioned_target(base, count) do
      members = group(base, count)
      targets = Map.new(members, &{&1.id, labelled_target(&1.id)})

      stub(Flezha.Operative.PartitionGroup, :members, fn _ns, id ->
        if id == base, do: members, else: []
      end)

      resolve_to(targets)
      {members, targets}
    end

    test "one worker per member, and the same key always takes the same one" do
      base = uniq("grp")
      {members, _targets} = partitioned_target(base, 2)

      {:ok, opts, _worker} = prepare(uniq("src"), base)
      assert length(opts.routes) == 2
      assert Enum.map(opts.routes, & &1.id) == Enum.map(members, & &1.id)

      owner = Flezha.Partition.member_id(base, 1)

      emit(opts, "a", %{"flz.key" => "BTC-USD"})
      emit(opts, "b", %{"flz.key" => "BTC-USD"})

      assert_receive {:member_got, ^owner, "a"}, 1_000
      assert_receive {:member_got, ^owner, "b"}, 1_000
    end

    test "keys land on the member whose range owns them" do
      base = uniq("grp")
      {_members, _targets} = partitioned_target(base, 2)
      {:ok, opts, _worker} = prepare(uniq("src"), base)

      p0 = Flezha.Partition.member_id(base, 0)
      p1 = Flezha.Partition.member_id(base, 1)

      emit(opts, "sol", %{"flz.key" => "SOL-USD"})
      emit(opts, "eth", %{"flz.key" => "ETH-USD"})

      assert_receive {:member_got, ^p0, "sol"}, 1_000
      assert_receive {:member_got, ^p1, "eth"}, 1_000
    end

    test "one unreachable member does not hold up its siblings" do
      base = uniq("grp")
      members = group(base, 2)
      reachable = Flezha.Partition.member_id(base, 0)
      target = labelled_target(reachable)

      stub(Flezha.Operative.PartitionGroup, :members, fn _ns, id ->
        if id == base, do: members, else: []
      end)

      # Only p0 resolves; p1's worker retries against a target that is not there.
      resolve_to(%{reachable => target})

      {:ok, opts, _worker} = prepare(uniq("src"), base, %{retry_backoff_ms: 10})

      emit(opts, "stuck", %{"flz.key" => "BTC-USD"})
      emit(opts, "flows", %{"flz.key" => "SOL-USD"})

      assert_receive {:member_got, ^reachable, "flows"}, 1_000
    end

    test "an unkeyed message is parked with the contract it violated, never guessed" do
      base = uniq("grp")
      {_members, _targets} = partitioned_target(base, 2)
      source = uniq("src")
      {:ok, opts, _worker} = prepare(source, base)

      parent = self()

      expect(Flezha.Dlq.Store, :append, fn entry ->
        send(parent, {:parked, entry})
        {:ok, "ref"}
      end)

      emit(opts, "no-key", %{})

      assert_receive {:parked, entry}, 1_000
      assert entry.operative_class == source
      assert entry.entity_key == base
      assert entry.payload == "no-key"
      assert entry.error =~ "partitioned target requires keyed emit"
      assert entry.error =~ source
      assert entry.error =~ base

      refute_receive {:member_got, _, "no-key"}, 200
    end

    test "an unkeyed message to a single-member target still flows" do
      id = uniq("plain")
      target = fake_target(fn _ -> :ok end)
      stub(Flezha.Operative.PartitionGroup, :members, fn _ns, _id -> [] end)
      resolve_to(%{id => target})

      {:ok, opts, _worker} = prepare(uniq("src"), id)
      assert length(opts.routes) == 1

      emit(opts, "unkeyed", %{})
      assert_receive {:target_got, "unkeyed", _}, 1_000
    end

    test "disabling the plugin stops every member's worker" do
      base = uniq("grp")
      {_members, _targets} = partitioned_target(base, 3)
      source = uniq("src")

      plugin =
        Flezha.Plugin.new(
          id: "fwd_grp",
          provider: OperativeForward,
          opts: %{target_operative_id: base},
          namespace: "default",
          operative_id: source,
          subscriptions: [{:on_output, []}]
        )

      table = Table.new(Flezha.Operative.Overwatch.__extensions_catalog__())
      {:ok, table} = Table.subscribe(table, "fwd_grp", plugin)

      workers = Enum.map(table.plugins["fwd_grp"].opts.routes, & &1.pid)
      assert length(workers) == 3
      assert Enum.all?(workers, &Process.alive?/1)

      _table = Table.unsubscribe(table, "fwd_grp")

      wait_until(fn -> Enum.all?(workers, &(not Process.alive?(&1))) end)
    end

    test "a re-resolve re-points the routes and hands the vanished member's queue over" do
      base = uniq("grp")
      source = uniq("src")

      # Three members, but only p0 and p1 answer: p2's target is unreachable, so
      # its worker accumulates exactly the way it does while a rescale has the
      # group stopped.
      three = group(base, 3)
      reachable = Map.new(Enum.take(three, 2), &{&1.id, labelled_target(&1.id)})

      stub(Flezha.Operative.PartitionGroup, :members, fn _ns, id ->
        if id == base, do: three, else: []
      end)

      resolve_to(reachable)

      {:ok, opts, _worker} = prepare(source, base, %{retry_backoff_ms: 10_000})
      assert length(opts.routes) == 3
      [_p0, _p1, p2] = opts.routes

      # ETH-USD (key group 213) belongs to p2 of three. Its worker cannot
      # deliver, so the payload sits in the queue.
      emit(opts, "eth-1", %{"flz.key" => "ETH-USD"})
      emit(opts, "eth-2", %{"flz.key" => "ETH-USD"})
      wait_until(fn -> ForwardWorker.stats(p2.pid).buffer_len == 2 end)

      # The group rescales to two: p2 is gone and 213 now belongs to p1.
      two = group(base, 2)
      new_owner = Flezha.Partition.member_id(base, 1)

      stub(Flezha.Operative.PartitionGroup, :members, fn _ns, id ->
        if id == base, do: two, else: []
      end)

      assert {:ok, fresh, _pid} = OperativeForward.init_provider(opts)
      assert Enum.map(fresh.routes, & &1.id) == Enum.map(two, & &1.id)

      # The orphaned worker is gone, and what it owed arrived at the new owner —
      # in the order it was queued, before anything emitted afterwards.
      wait_until(fn -> not Process.alive?(p2.pid) end)
      assert_receive {:member_got, ^new_owner, "eth-1"}, 1_000
      assert_receive {:member_got, ^new_owner, "eth-2"}, 1_000

      emit(fresh, "eth-3", %{"flz.key" => "ETH-USD"})
      assert_receive {:member_got, ^new_owner, "eth-3"}, 1_000
    end

    test "a surviving member keeps its own worker, and its queue with it" do
      base = uniq("grp")
      three = group(base, 3)

      stub(Flezha.Operative.PartitionGroup, :members, fn _ns, id ->
        if id == base, do: three, else: []
      end)

      # Nothing answers, so every worker holds its payload.
      resolve_to(%{})

      {:ok, opts, _worker} = prepare(uniq("src"), base, %{retry_backoff_ms: 10_000})
      [p0, _p1, _p2] = opts.routes

      emit(opts, "sol-1", %{"flz.key" => "SOL-USD"})
      wait_until(fn -> ForwardWorker.stats(p0.pid).buffer_len == 1 end)

      two = group(base, 2)

      stub(Flezha.Operative.PartitionGroup, :members, fn _ns, id ->
        if id == base, do: two, else: []
      end)

      assert {:ok, fresh, _pid} = OperativeForward.init_provider(opts)

      # SOL-USD (group 60) is p0's under both shapes, so its worker is the same
      # process and still owes the payload — a re-resolve must not cost a
      # member that did not move anything.
      surviving = Enum.find(fresh.routes, &(&1.id == p0.id))
      assert surviving.pid == p0.pid
      assert ForwardWorker.stats(p0.pid).buffer_len == 1
    end

    test "a drained payload that belongs nowhere is parked, never dropped" do
      base = uniq("grp")
      source = uniq("src")
      three = group(base, 3)

      stub(Flezha.Operative.PartitionGroup, :members, fn _ns, id ->
        if id == base, do: three, else: []
      end)

      resolve_to(%{})

      {:ok, opts, _worker} = prepare(source, base, %{retry_backoff_ms: 10_000})
      [_p0, _p1, p2] = opts.routes

      emit(opts, "stranded", %{"flz.key" => "ETH-USD"})
      wait_until(fn -> ForwardWorker.stats(p2.pid).buffer_len == 1 end)

      # The target collapses to a single member owning only the low groups, so
      # ETH-USD's group has no owner at all.
      orphaned = [
        %{
          id: Flezha.Partition.member_id(base, 0),
          partition: %{group: base, index: 0, count: 2, range: {0, 10}}
        },
        %{
          id: Flezha.Partition.member_id(base, 1),
          partition: %{group: base, index: 1, count: 2, range: {11, 20}}
        }
      ]

      stub(Flezha.Operative.PartitionGroup, :members, fn _ns, id ->
        if id == base, do: orphaned, else: []
      end)

      parent = self()

      expect(Flezha.Dlq.Store, :append, fn entry ->
        send(parent, {:parked, entry})
        {:ok, "ref"}
      end)

      assert {:ok, _fresh, _pid} = OperativeForward.init_provider(opts)

      assert_receive {:parked, entry}, 1_000
      assert entry.payload == "stranded"
      assert entry.operative_class == source
    end

    test "health reports the sickest worker, not the healthy majority" do
      base = uniq("grp")
      {_members, _targets} = partitioned_target(base, 3)
      {:ok, opts, _worker} = prepare(uniq("src"), base)

      assert OperativeForward.health(opts) == :healthy

      [_p0, p1, _p2] = opts.routes

      # The kill is the point of the test; its supervisor report is not.
      ExUnit.CaptureLog.capture_log(fn ->
        Process.exit(p1.pid, :kill)
        wait_until(fn -> not Process.alive?(p1.pid) end)
      end)

      assert OperativeForward.health(opts) == {:unhealthy, :worker_dead}
    end
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() -> :ok
      attempts <= 0 -> flunk("condition never held")
      true -> Process.sleep(10) && wait_until(fun, attempts - 1)
    end
  end
end
