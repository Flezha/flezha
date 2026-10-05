defmodule FlezhaTest.Plugin.Extension.PhaseTest do
  @moduledoc """
  The two-phase plugin lifecycle (capability `relay-extensions`,
  "Two-phase plugin lifecycle gated on SDK readiness"). A plugin enters the
  `:prepared` phase on `Table.bulk_subscribe/2`; `bulk_activate/2` starts its
  declared worker and calls `activate/1`, moving it to `:running`. A provider
  that overrides neither takes the declaration's defaults and transitions
  silently.
  """

  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Plugins
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.ProviderBehaviour
  alias FlezhaTest.Helper.MessageTestHelper

  use ExUnit.Case, async: true

  @catalog Host.build_catalog(
             [{:on_input, :transform, Ctx.IO}],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  defmodule QuiescentProvider do
    @moduledoc "Provider with no activate/1 — transitions silently."
    use ProviderBehaviour,
      options: [
        marker: [type: :any, internal: true],
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule TrackingProvider do
    @moduledoc """
    Provider whose `init_provider/1` is fast/local and whose `activate/1`
    sends a marker to the registered reporter pid — the mirror of the
    `Plugin.Provider.Websocket` shape after the lifecycle split.
    """
    use ProviderBehaviour,
      options: [
        marker: [type: :any, internal: true],
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}

    @impl true
    def activate(%{report_to: pid, marker: m}) do
      send(pid, {:activated, m})
      :ok
    end

    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule RaisingProvider do
    use ProviderBehaviour,
      options: [
        marker: [type: :any, internal: true],
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}
    @impl true
    def activate(_), do: raise("upstream went away")
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule FailingActivateProvider do
    use ProviderBehaviour,
      options: [
        marker: [type: :any, internal: true],
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}
    @impl true
    def activate(_), do: {:error, :upstream_down}
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule InvalidActivateProvider do
    use ProviderBehaviour,
      options: [
        marker: [type: :any, internal: true],
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}
    @impl true
    def activate(_), do: :nonsense
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule RaisingHealthProvider do
    use ProviderBehaviour,
      options: [
        marker: [type: :any, internal: true],
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}
    @impl true
    def activate(_), do: :ok
    @impl true
    def health(_opts), do: raise("probe blew up")
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule GarbageHealthProvider do
    use ProviderBehaviour,
      options: [
        marker: [type: :any, internal: true],
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}
    @impl true
    def activate(_), do: :ok
    @impl true
    def health(_opts), do: :probably_fine
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule UncertainHealthProvider do
    use ProviderBehaviour,
      options: [
        marker: [type: :any, internal: true],
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}
    @impl true
    def activate(_), do: :ok
    @impl true
    def health(_opts), do: {:fail_open, :probe_timeout}
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule DefiniteHealthProvider do
    use ProviderBehaviour,
      options: [
        marker: [type: :any, internal: true],
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}
    @impl true
    def activate(_), do: :ok
    @impl true
    def health(_opts), do: {:unhealthy, :upstream_down}
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule ReportingInitProvider do
    @moduledoc "Reports from `init_provider/1` — evidence that phase 1 reached it."
    use ProviderBehaviour,
      options: [
        marker: [type: :any, internal: true],
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(%{report_to: pid, marker: m} = opts) do
      send(pid, {:init_provider, m})
      {:ok, opts}
    end

    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defp subscribed(provider, id \\ "p1") do
    plugin = %Plugin{id: id, provider: provider, hooks: [{:on_input, [position: 0]}]}
    {:ok, table} = Table.bulk_subscribe(Table.new(@catalog), [{id, plugin}])
    table
  end

  # A failed activation logs at :error by design; swallow it so a green run
  # stays quiet while the assertions still read the returned outcome.
  defp activate_quietly(table) do
    {result, _log} = ExUnit.CaptureLog.with_log(fn -> Table.bulk_activate(table) end)
    result
  end

  test "plugins enter :prepared on subscribe" do
    plugin = %Plugin{
      id: "p1",
      provider: QuiescentProvider,
      hooks: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.bulk_subscribe(table, [{"p1", plugin}])

    assert Table.phase(table, "p1") == :prepared
  end

  test "bulk_activate transitions prepared plugins to :running exactly once" do
    plugin = %Plugin{
      id: "p1",
      provider: TrackingProvider,
      opts: %{report_to: self(), marker: :first},
      hooks: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.bulk_subscribe(table, [{"p1", plugin}])

    refute_received {:activated, _}

    {table, _} = Table.bulk_activate(table)
    assert_receive {:activated, :first}, 200
    assert Table.phase(table, "p1") == :running

    {table, _} = Table.bulk_activate(table)
    refute_received {:activated, _}
    assert Table.phase(table, "p1") == :running
  end

  test "plugins without activate/1 transition silently to :running" do
    plugin = %Plugin{
      id: "p1",
      provider: QuiescentProvider,
      hooks: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.bulk_subscribe(table, [{"p1", plugin}])
    {table, _} = Table.bulk_activate(table)
    assert Table.phase(table, "p1") == :running
  end

  test "teardown handles prepared and running plugins uniformly" do
    quiescent = %Plugin{
      id: "qpro",
      provider: QuiescentProvider,
      hooks: [{:on_input, [position: 0]}]
    }

    tracking = %Plugin{
      id: "tpro",
      provider: TrackingProvider,
      opts: %{report_to: self(), marker: :td},
      hooks: [{:on_input, [position: 1]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.bulk_subscribe(table, [{"qpro", quiescent}])
    {:ok, table} = Table.bulk_subscribe(table, [{"tpro", tracking}])
    {table, _} = Table.bulk_activate(table)

    assert Table.phase(table, "qpro") == :running
    assert Table.phase(table, "tpro") == :running

    # Now unsubscribe one and partially tear down. Teardown should not
    # raise regardless of phase mix.
    teardown_table = Table.teardown_all(table)
    assert teardown_table.plugins == %{}
    assert teardown_table.entries == %{}
  end

  test "activate that raises leaves plugin in :prepared and logs" do
    plugin = %Plugin{
      id: "p1",
      provider: RaisingProvider,
      hooks: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.bulk_subscribe(table, [{"p1", plugin}])

    table =
      ExUnit.CaptureLog.with_log(fn -> elem(Table.bulk_activate(table), 0) end)
      |> elem(0)

    assert Table.phase(table, "p1") == :prepared
  end

  describe "a failed activation is an outcome, not only a log line" do
    test "activate returning an error is reported with its reason" do
      {table, failed} = activate_quietly(subscribed(FailingActivateProvider))

      assert Table.phase(table, "p1") == :prepared
      assert failed == [{"p1", :upstream_down}]
    end

    test "activate raising is reported with its reason" do
      {table, failed} = activate_quietly(subscribed(RaisingProvider))

      assert Table.phase(table, "p1") == :prepared
      assert [{"p1", {:error, %RuntimeError{message: "upstream went away"}}}] = failed
    end

    test "activate returning nonsense is reported, not treated as success" do
      {table, failed} = activate_quietly(subscribed(InvalidActivateProvider))

      assert Table.phase(table, "p1") == :prepared
      assert failed == [{"p1", {:invalid_activate_return, :nonsense}}]
    end

    test "a successful activation reports nothing failed" do
      {table, failed} = activate_quietly(subscribed(QuiescentProvider))

      assert Table.phase(table, "p1") == :running
      assert failed == []
    end

    test "a plugin that failed to activate is subscribed but NOT running" do
      {table, _failed} = activate_quietly(subscribed(FailingActivateProvider))

      assert Table.subscribed_ids(table) == ["p1"]
      assert Table.running_ids(table) == []

      # `:prepared` reads healthy on purpose — the plugin holds no upstream
      # resources, so it has nothing to be unhealthy about. That verdict is not
      # read as "it is running": the phase decides that.
      assert Table.health(table) == %{"p1" => :healthy}
    end
  end

  describe "an uncertain health probe is observable" do
    # `fail_open/3` is a VM-global `:telemetry` event, and this module is
    # `async: true`: an unscoped forward delivers a concurrently running
    # emitter's event here too, so `assert_receive` matches a signal this test
    # never caused and `refute_received` is falsified outright. Each test gets
    # its own plugin id and forwards only events carrying it.
    setup do
      id = "fail_open_#{System.unique_integer([:positive])}"

      MessageTestHelper.forward_telemetry(
        ProviderBehaviour.fail_open_event(),
        :fail_open,
        &(Map.get(&1, :plugin_id) == id)
      )

      %{plugin_id: id}
    end

    test "a health/1 that raises answers fail-open AND emits", %{plugin_id: id} do
      {table, []} = activate_quietly(subscribed(RaisingHealthProvider, id))

      assert %{^id => {:fail_open, {:raised, _}}} = Table.health(table)

      assert_receive {:fail_open, %{count: 1}, meta}
      assert meta.plugin_id == id
      assert meta.provider == RaisingHealthProvider
      assert {:raised, %RuntimeError{message: "probe blew up"}} = meta.reason
    end

    test "a health/1 returning an unrecognised value answers fail-open AND emits", %{
      plugin_id: id
    } do
      {table, []} = activate_quietly(subscribed(GarbageHealthProvider, id))

      assert Table.health(table) == %{id => {:fail_open, {:invalid_return, :probably_fine}}}

      assert_receive {:fail_open, %{count: 1}, meta}
      assert meta.reason == {:invalid_return, :probably_fine}
    end

    test "a provider that reports its own uncertainty is carried AND emits", %{plugin_id: id} do
      {table, []} = activate_quietly(subscribed(UncertainHealthProvider, id))

      # This is the wedged-worker path every worker-backed provider takes: the
      # provider cannot complete its probe, says so, and the table — which knows
      # the plugin id — turns it into the one fail-open event. The uncertainty
      # travels unjudged; the convergence judge is what folds it to healthy.
      assert Table.health(table) == %{id => {:fail_open, :probe_timeout}}

      assert_receive {:fail_open, %{count: 1}, meta}
      assert meta.plugin_id == id
      assert meta.provider == UncertainHealthProvider
      assert meta.reason == :probe_timeout
    end

    test "a definite verdict emits nothing", %{plugin_id: id} do
      {table, []} = activate_quietly(subscribed(DefiniteHealthProvider, id))

      assert Table.health(table) == %{id => {:unhealthy, :upstream_down}}
      refute_received {:fail_open, _, _}
    end

    test "another plugin's fail-open is never delivered here", %{plugin_id: id} do
      foreign =
        Task.async(fn ->
          ProviderBehaviour.fail_open("foreign_#{id}", GarbageHealthProvider, :not_this_test)
        end)

      # `:telemetry.execute/3` runs every attached handler in the emitting
      # process, so the task returning means this test's handler has already
      # decided on that event — the refute below is a decision, not a race.
      assert Task.await(foreign) == {:fail_open, :not_this_test}
      refute_received {:fail_open, _, _}

      # ... and the filter is not one that simply matches nothing: this test's
      # own emit still arrives, on the same handler, straight after.
      {table, []} = activate_quietly(subscribed(UncertainHealthProvider, id))
      assert Table.health(table) == %{id => {:fail_open, :probe_timeout}}
      assert_receive {:fail_open, %{count: 1}, %{plugin_id: ^id, reason: :probe_timeout}}
    end
  end

  describe "a plugin phase 1 cannot prepare" do
    setup do
      orphan = %Plugin{
        id: "orphan",
        provider: MyOrg.UninstalledProvider,
        hooks: [{:on_input, [position: 0]}]
      }

      healthy = %Plugin{
        id: "healthy",
        provider: ReportingInitProvider,
        opts: %{report_to: self(), marker: :healthy},
        hooks: [{:on_input, [position: 1]}]
      }

      %{entries: [{"orphan", orphan}, {"healthy", healthy}]}
    end

    test "bulk_subscribe names the plugin that failed and the rows it left unattempted",
         %{entries: entries} do
      {result, log} =
        ExUnit.CaptureLog.with_log(fn ->
          Table.bulk_subscribe(Table.new(@catalog), entries)
        end)

      assert {:error, {"orphan", {:module_not_loaded, MyOrg.UninstalledProvider, _node}}} = result

      assert log =~ "plugin_id=orphan"
      assert log =~ "MyOrg.UninstalledProvider"
      assert log =~ "healthy"
    end

    # A binding resolves against its plugin's entry in the relay's
    # namespace; with no hub there is none, so nothing is subscribed and the
    # bind goes on (`FlezhaTest.Relay.PackageProviderTest` is the live one).
    test "prepare/2 records a binding it cannot bring up as not started and binds on",
         %{entries: entries} do
      state = %Overwatch{id: "op1", namespace: "default", extensions: Table.new(@catalog)}

      {result, _log} = ExUnit.CaptureLog.with_log(fn -> Plugins.prepare(state, entries) end)

      assert {:ok, %Overwatch{extensions: table}} = result
      assert Table.phase(table, "orphan") == {:not_started, {:plugin_not_found, "orphan"}}
      assert Table.phase(table, "healthy") == {:not_started, {:plugin_not_found, "healthy"}}
      refute_received {:init_provider, _}
    end

    test "the fold still halts: the row after the failure is never attempted",
         %{entries: entries} do
      {_result, _log} =
        ExUnit.CaptureLog.with_log(fn ->
          Table.bulk_subscribe(Table.new(@catalog), entries)
        end)

      refute_received {:init_provider, :healthy}
    end
  end
end
