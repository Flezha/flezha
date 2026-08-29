defmodule FlezhaTest.Plugin.Extension.PhaseTest do
  @moduledoc """
  Covers the two-phase plugin lifecycle introduced by
  `fix-overwatch-restart-storm` (capability `operative-extensions` —
  "Two-phase plugin lifecycle gated on SDK readiness"). Plugins enter the
  `:prepared` phase on `Table.subscribe/3`; `bulk_activate/1` transitions
  every prepared plugin to `:running`, invoking the optional `activate/1`
  callback. Plugins without an `activate/1` callback transition silently.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host

  use ExUnit.Case, async: true

  @catalog Host.build_catalog(
             [{:on_input, :transform, Ctx.IO}],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  defmodule QuiescentProvider do
    @moduledoc "Provider with no activate/1 — transitions silently."
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule TrackingProvider do
    @moduledoc """
    Provider whose `init_provider/1` is fast/local and whose `activate/1`
    sends a marker to the registered reporter pid — the mirror of the
    `Plugin.Provider.Websocket` shape after the lifecycle split.
    """
    def init_provider(opts), do: {:ok, opts}

    def activate(%{report_to: pid, marker: m}) do
      send(pid, {:activated, m})
      :ok
    end

    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule RaisingProvider do
    def init_provider(opts), do: {:ok, opts}
    def activate(_), do: raise("upstream went away")
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule FailingActivateProvider do
    def init_provider(opts), do: {:ok, opts}
    def activate(_), do: {:error, :upstream_down}
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule InvalidActivateProvider do
    def init_provider(opts), do: {:ok, opts}
    def activate(_), do: :nonsense
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule RaisingHealthProvider do
    def init_provider(opts), do: {:ok, opts}
    def activate(_), do: :ok
    def health(_opts), do: raise("probe blew up")
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule GarbageHealthProvider do
    def init_provider(opts), do: {:ok, opts}
    def activate(_), do: :ok
    def health(_opts), do: :probably_fine
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule UncertainHealthProvider do
    def init_provider(opts), do: {:ok, opts}
    def activate(_), do: :ok
    def health(_opts), do: {:fail_open, :probe_timeout}
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule DefiniteHealthProvider do
    def init_provider(opts), do: {:ok, opts}
    def activate(_), do: :ok
    def health(_opts), do: {:unhealthy, :upstream_down}
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defp subscribed(provider, id \\ "p1") do
    plugin = %Plugin{id: id, provider: provider, subscriptions: [{:on_input, [position: 0]}]}
    {:ok, table} = Table.subscribe(Table.new(@catalog), id, plugin)
    table
  end

  # A failed activation logs at :error by design; swallow it so a green run
  # stays quiet while the assertions still read the returned outcome.
  defp activate_quietly(table) do
    {result, _log} = ExUnit.CaptureLog.with_log(fn -> Table.bulk_activate(table) end)
    result
  end

  test "5.5 plugins enter :prepared on subscribe" do
    plugin = %Plugin{
      id: "p1",
      provider: QuiescentProvider,
      subscriptions: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.subscribe(table, "p1", plugin)

    assert Table.phase(table, "p1") == :prepared
  end

  test "5.6 bulk_activate transitions prepared plugins to :running exactly once" do
    plugin = %Plugin{
      id: "p1",
      provider: TrackingProvider,
      opts: %{report_to: self(), marker: :first},
      subscriptions: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.subscribe(table, "p1", plugin)

    refute_received {:activated, _}

    {table, _} = Table.bulk_activate(table)
    assert_receive {:activated, :first}, 200
    assert Table.phase(table, "p1") == :running

    {table, _} = Table.bulk_activate(table)
    refute_received {:activated, _}
    assert Table.phase(table, "p1") == :running
  end

  test "5.6 plugins without activate/1 transition silently to :running" do
    plugin = %Plugin{
      id: "p1",
      provider: QuiescentProvider,
      subscriptions: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.subscribe(table, "p1", plugin)
    {table, _} = Table.bulk_activate(table)
    assert Table.phase(table, "p1") == :running
  end

  test "5.6 teardown handles prepared and running plugins uniformly" do
    quiescent = %Plugin{
      id: "qpro",
      provider: QuiescentProvider,
      subscriptions: [{:on_input, [position: 0]}]
    }

    tracking = %Plugin{
      id: "tpro",
      provider: TrackingProvider,
      opts: %{report_to: self(), marker: :td},
      subscriptions: [{:on_input, [position: 1]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.subscribe(table, "qpro", quiescent)
    {:ok, table} = Table.subscribe(table, "tpro", tracking)
    {table, _} = Table.bulk_activate(table)

    assert Table.phase(table, "qpro") == :running
    assert Table.phase(table, "tpro") == :running

    # Now unsubscribe one and partially tear down. Teardown should not
    # raise regardless of phase mix.
    teardown_table = Table.teardown_all(table)
    assert teardown_table.plugins == %{}
    assert teardown_table.phases == %{}
  end

  test "5.6 activate that raises leaves plugin in :prepared and logs" do
    plugin = %Plugin{
      id: "p1",
      provider: RaisingProvider,
      subscriptions: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.subscribe(table, "p1", plugin)

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

      # The `:prepared`-is-healthy rule is unchanged on purpose — the plugin
      # holds no upstream resources, so it has nothing to be unhealthy about.
      # What changed is that this verdict is no longer read as "it is running".
      assert Table.health(table) == %{"p1" => :healthy}
    end
  end

  describe "an uncertain health probe is observable" do
    setup do
      handler = "fail-open-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler,
        Flezha.Plugin.ProviderBehaviour.fail_open_event(),
        fn _event, measurements, meta, _cfg ->
          send(test_pid, {:fail_open, measurements, meta})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
      :ok
    end

    test "a health/1 that raises reports healthy AND emits" do
      {table, []} = activate_quietly(subscribed(RaisingHealthProvider))

      assert Table.health(table) == %{"p1" => :healthy}

      assert_receive {:fail_open, %{count: 1}, meta}
      assert meta.plugin_id == "p1"
      assert meta.provider == RaisingHealthProvider
      assert {:raised, %RuntimeError{message: "probe blew up"}} = meta.reason
    end

    test "a health/1 returning an unrecognised value reports healthy AND emits" do
      {table, []} = activate_quietly(subscribed(GarbageHealthProvider))

      assert Table.health(table) == %{"p1" => :healthy}

      assert_receive {:fail_open, %{count: 1}, meta}
      assert meta.reason == {:invalid_return, :probably_fine}
    end

    test "a provider that reports its own uncertainty reads healthy AND emits" do
      {table, []} = activate_quietly(subscribed(UncertainHealthProvider))

      # This is the wedged-worker path every worker-backed provider takes: the
      # provider cannot complete its probe, says so, and the table — which knows
      # the plugin id — turns it into the one fail-open event.
      assert Table.health(table) == %{"p1" => :healthy}

      assert_receive {:fail_open, %{count: 1}, meta}
      assert meta.plugin_id == "p1"
      assert meta.provider == UncertainHealthProvider
      assert meta.reason == :probe_timeout
    end

    test "a definite verdict emits nothing" do
      {table, []} = activate_quietly(subscribed(DefiniteHealthProvider))

      assert Table.health(table) == %{"p1" => {:unhealthy, :upstream_down}}
      refute_received {:fail_open, _, _}
    end
  end
end
