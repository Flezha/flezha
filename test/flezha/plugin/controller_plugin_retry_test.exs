defmodule FlezhaTest.RefusingWorker do
  @moduledoc false
  # A worker that refuses to start, as a listener on a port in use does, until
  # `allow/0` is called; it tells `recv_pid` each time it is tried.
  use GenServer

  @key {__MODULE__, :allowed}

  def allow, do: :persistent_term.put(@key, true)
  def refuse, do: :persistent_term.put(@key, false)

  def start_link(%{name: name} = opts), do: GenServer.start_link(__MODULE__, opts, name: name)

  @impl true
  def init(opts) do
    if :persistent_term.get(@key, false) do
      send(opts.recv_pid, {:worker_up, self()})
      {:ok, nil}
    else
      send(opts.recv_pid, :worker_refused)
      {:stop, {:http_listen_failed, %{plugin: "retry", reason: :eaddrinuse}}}
    end
  end
end

defmodule FlezhaTest.RefusingProvider do
  @moduledoc false
  use Flezha.Plugin.ProviderBehaviour,
    type: :controller,
    namespace: "controller_plugin_retry",
    options: [recv_pid: [type: :pid, required: true, doc: "told each time the worker is tried"]],
    worker: [module: FlezhaTest.RefusingWorker, restart: :transient]
end

defmodule FlezhaTest.Plugin.ControllerPluginRetryTest do
  @moduledoc """
  A controller plugin's copy whose worker cannot start runs once the cause
  goes away, without a disable and enable.
  """

  use FlezhaTest.Case, fixtures: [:env], async: false

  import ExUnit.CaptureLog

  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.Shared
  alias Flezha.Service.HubManager
  alias Flezha.Service.PluginManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.RefusingProvider
  alias FlezhaTest.RefusingWorker

  @namespace "controller_plugin_retry"

  # Started once for the module, so it outlives each test's on_exit disable.
  setup_all do
    start_supervised!({WorkerSup, name: WorkerSup.family(RefusingProvider)})
    :ok
  end

  setup do
    RefusingWorker.refuse()

    on_exit(fn ->
      PluginManager.disable_plugin(@namespace, "retry")
      HubManager.stop_hub(@namespace)
      RefusingWorker.refuse()
    end)
  end

  for single <- [false, true] do
    test "a copy whose worker failed at start runs once the cause is gone, single: #{single}" do
      on_exit(NodeTestHelper.override_log_level(:warning))

      log =
        capture_log(fn ->
          assert {:ok, _} = enable(unquote(single))
          assert_receive :worker_refused, 5_000

          copy = waiting_copy()

          RefusingWorker.allow()

          # A few retry periods, so a late tick on a loaded machine still lands.
          assert_receive {:worker_up, _worker},
                         3 * Durations.get(:convergence_tick_ms),
                         "the copy was never placed again after its worker could start"

          assert Directory.plugin_hosts(@namespace, "retry") == [copy]
          assert Shared.health(copy) == :healthy
          assert [%{controllers: [here]}] = PluginManager.plugins(@namespace)
          assert here == node()
        end)

      assert log =~ "FlezhaTest.RefusingWorker.init/1"
      assert log =~ "[Plugin.Shared] retry did not start: {:worker_start_failed"
      assert log =~ ":eaddrinuse"
      refute log =~ "Flezha.Plugin.Shared.init/1"
    end
  end

  test "an update that starts a waiting copy leaves no retry due" do
    on_exit(NodeTestHelper.override_log_level(:warning))

    log =
      capture_log(fn ->
        assert {:ok, _} = enable(false)
        assert_receive :worker_refused, 5_000
        copy = waiting_copy()
        assert Map.has_key?(:sys.get_state(copy).timers, :restart)

        RefusingWorker.allow()
        {:ok, entry} = HubManager.plugin_entry(@namespace, "retry")
        Shared.update(copy, entry, :restart)
        assert_receive {:worker_up, _worker}, 5_000

        # The update's restart ran before this call is answered: with no retry
        # armed, nothing starts the copy a second time.
        refute Map.has_key?(:sys.get_state(copy).timers, :restart)
        assert Shared.health(copy) == :healthy
      end)

    assert log =~ "[Plugin.Shared] retry did not start: {:worker_start_failed"
    assert log =~ "FlezhaTest.RefusingWorker.init/1"
  end

  # Listed where it waits, as a copy whose worker went down is, with the
  # reason it waits, and running nowhere.
  defp waiting_copy do
    assert [copy] = Directory.plugin_hosts(@namespace, "retry")
    assert node(copy) == node()

    assert {:unhealthy,
            {:plugin_not_started,
             {:worker_start_failed, _key, {:http_listen_failed, %{reason: :eaddrinuse}}}}} =
             Shared.health(copy)

    assert [%{name: "retry", controllers: []}] = PluginManager.plugins(@namespace)
    copy
  end

  defp enable(single) do
    PluginManager.enable_plugin("retry", %Plugin{
      id: "retry",
      provider: RefusingProvider,
      namespace: @namespace,
      single: single,
      opts: %{recv_pid: self()}
    })
  end
end
