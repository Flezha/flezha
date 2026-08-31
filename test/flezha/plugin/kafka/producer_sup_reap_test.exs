defmodule Flezha.Plugin.Provider.Kafka.ProducerSupReapTest do
  @moduledoc """
  `ProducerSup.ensure_worker/1` dead/parked-pid reaping (self-healing-control-plane,
  Phase 7.3). A recovered operative must start a fresh, healthy producer rather than
  re-attach to a worker that can no longer drain.
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
end
