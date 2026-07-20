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

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() -> :ok
      attempts <= 0 -> flunk("condition never held")
      true -> Process.sleep(10) && wait_until(fun, attempts - 1)
    end
  end
end
