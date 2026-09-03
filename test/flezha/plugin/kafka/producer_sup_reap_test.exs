defmodule Flezha.Plugin.Provider.Kafka.ProducerSupReapTest do
  @moduledoc """
  `ProducerSup.ensure_worker/1` dead/parked-pid reaping. A recovered operative
  must start a fresh, healthy producer rather than re-attach to a worker that
  cannot drain.
  """
  # async: false: registered singletons.
  use ExUnit.Case, async: false

  alias Flezha.Plugin.Provider.Kafka.ProducerSup
  alias Flezha.Plugin.Provider.WorkerSup
  alias FlezhaTest.Helper.NodeTestHelper

  # A stub supervised by the producer dyn-sup and registered under the worker's
  # via-name; it answers :stats with a permanently-parked verdict so
  # ensure_worker treats it as non-reusable.
  defmodule ParkedStub do
    use GenServer

    def start_link(opts) do
      key = {opts[:plugin_id], opts[:topic], opts[:partition]}
      GenServer.start_link(__MODULE__, opts, name: WorkerSup.via(ProducerSup, key))
    end

    def child_spec(opts) do
      %{
        id: {__MODULE__, make_ref()},
        start: {__MODULE__, :start_link, [opts]},
        restart: :temporary
      }
    end

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def handle_call(:stats, _from, state) do
      {:reply, %{last_error: {:swap_exhausted, :test}, queue_depth: 0, in_flight: 0}, state}
    end
  end

  defp forward_sleeps(pid) do
    receive do
      {:trace, _, :call, {Process, :sleep, [ms]}} -> send(pid, {:slept, ms})
      _other -> :ok
    end

    forward_sleeps(pid)
  end

  defp fake_worker do
    pid = spawn(&NodeTestHelper.park/0)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    pid
  end

  # idempotent: false so the fresh ProducerWorker skips InitProducerId against
  # the fake (non-Kafka) worker pid and starts cleanly with no broker.
  defp opts(plugin_id) do
    [plugin_id: plugin_id, topic: "t", partition: 0, worker_pid: fake_worker(), idempotent: false]
  end

  test "reuses a healthy worker" do
    o = opts("ps_reuse")
    {:ok, p1} = ProducerSup.ensure_worker(o)
    on_exit(fn -> ProducerSup.stop_worker("ps_reuse", "t", 0) end)

    {:ok, p2} = ProducerSup.ensure_worker(o)
    assert p1 == p2
    assert Process.alive?(p1)
  end

  test "reaps a parked worker and starts a fresh one" do
    o = opts("ps_parked")

    {:ok, parked} =
      DynamicSupervisor.start_child(ProducerSup.dyn_sup_name(), {ParkedStub, o})

    on_exit(fn -> ProducerSup.stop_worker("ps_parked", "t", 0) end)

    {:ok, fresh} = ProducerSup.ensure_worker(o)

    assert fresh != parked
    refute Process.alive?(parked)
    assert Process.alive?(fresh)
  end

  test "a registry entry still naming the dead worker does not delay the swap" do
    o = opts("ps_stale")
    key = {"ps_stale", "t", 0}
    on_exit(fn -> ProducerSup.stop_worker("ps_stale", "t", 0) end)

    {:ok, dead} = DynamicSupervisor.start_child(ProducerSup.dyn_sup_name(), {ParkedStub, o})

    # The family's Registry partition clears an entry from the `:EXIT` it is
    # linked by; a suspended process processes none, so the entry provably
    # still names the worker after it dies. Without this the partition has
    # usually cleared it before the swap looks, and a busy-wait that never
    # spins is a busy-wait no test can see.
    partition = Process.whereis(Module.concat(ProducerSup.registry_name(), "PIDPartition0"))
    :sys.suspend(partition)
    on_exit(fn -> :sys.resume(partition) end)

    ref = Process.monitor(dead)
    Process.exit(dead, :kill)
    assert_receive {:DOWN, ^ref, :process, ^dead, _}, 1_000
    assert [{^dead, _}] = Registry.lookup(ProducerSup.registry_name(), key)

    # A process cannot be its own call tracer, so the sleeps are collected by a
    # forwarder that dies with the test.
    test_pid = self()
    tracer = spawn_link(fn -> forward_sleeps(test_pid) end)
    :erlang.trace_pattern({Process, :sleep, 1}, true, [:local])
    :erlang.trace(self(), true, [:call, {:tracer, tracer}])
    on_exit(fn -> :erlang.trace_pattern({Process, :sleep, 1}, false, [:local]) end)

    {elapsed_us, {:ok, fresh}} = :timer.tc(fn -> ProducerSup.ensure_worker(o) end)
    :erlang.trace(self(), false, [:call])

    refute_receive {:slept, _}, 100
    assert elapsed_us < 50_000, "the swap took #{elapsed_us} us"

    assert fresh != dead
    assert Process.alive?(fresh)
    assert [{^fresh, _}] = Registry.lookup(ProducerSup.registry_name(), key)
  end

  test "a forced worker death is answered with a fresh, registered worker" do
    o = opts("ps_killed")
    key = {"ps_killed", "t", 0}
    on_exit(fn -> ProducerSup.stop_worker("ps_killed", "t", 0) end)

    fresh_pids =
      for _ <- 1..10 do
        {:ok, victim} =
          DynamicSupervisor.start_child(ProducerSup.dyn_sup_name(), {ParkedStub, o})

        ref = Process.monitor(victim)
        Process.exit(victim, :kill)
        assert_receive {:DOWN, ^ref, :process, ^victim, _}, 1_000

        {:ok, fresh} = ProducerSup.ensure_worker(o)
        assert fresh != victim
        assert Process.alive?(fresh)
        assert [{^fresh, _}] = Registry.lookup(ProducerSup.registry_name(), key)
        :ok = ProducerSup.stop_worker("ps_killed", "t", 0)
        fresh
      end

    assert length(Enum.uniq(fresh_pids)) == 10
  end
end
