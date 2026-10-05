defmodule FlezhaTest.Plugin.NamedPluginTest do
  @moduledoc """
  A plugin is enabled by name in a namespace, without a relay, and relays
  are bound to it: the plugin is its own entry in the namespace hub's declared
  set, a binding lives on the relay's entry, and every refusal names why.
  """

  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.File.Sink, as: FileSink
  alias Flezha.Plugin.Provider.File.Source, as: FileSource
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Cli
  alias Flezha.Service.HubManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.WebsocketServer

  @namespace "named_plugin_ns"
  @factory "store_input"

  # One runner for the module: every test's relays and plugins have their
  # own names.
  setup_all do
    TestHelpers.reset_all_cells()
    runner_id = NodeTestHelper.start_runner(@namespace)

    on_exit(fn ->
      NodeTestHelper.stop_runner(@namespace, runner_id)
      FleetManager.stop_namespace(@namespace)
    end)

    :ok
  end

  defp websocket(name), do: plugin(name, Websocket, %{uri: "ws://127.0.0.1:1/feed"})

  defp plugin(name, provider, opts),
    do: %Plugin{id: name, provider: provider, namespace: @namespace, opts: opts}

  defp start_relay(id) do
    {:ok, _} = id |> RelayFixture.get(@factory) |> RelayFixture.start(@namespace)
    id
  end

  # The namespace's plugin entries; with auto-restore off, as the suite runs,
  # the hub's rows hold them (the declared set is
  # `Flezha.Service.HubManagerRecoveryTest`'s).
  defp enabled, do: Enum.map(HubManager.plugin_entries(@namespace), & &1.id)

  defp code({:error, reason}), do: Flezha.Error.from_reason(reason, "plugin.enable").code

  describe "enable by name" do
    test "records the plugin as its own declared entry and starts nothing" do
      assert {:ok, "plugin enabled"} =
               PluginManager.enable_plugin("ws_feed", websocket("ws_feed"))

      assert "ws_feed" in enabled()

      refute Enum.any?(
               HubManager.get_all_relays(@namespace),
               &(elem(&1, 0) == {:plugin, "ws_feed"})
             )

      assert WorkerSup.keys(WorkerSup.family(Websocket)) == []
    end

    test "a re-run is already_enabled and another provider is plugin.id_taken" do
      {:ok, _} = PluginManager.enable_plugin("ws_twice", websocket("ws_twice"))

      assert {:ok, :already_enabled} =
               PluginManager.enable_plugin("ws_twice", websocket("ws_twice"))

      taken = PluginManager.enable_plugin("ws_twice", plugin("ws_twice", FileSink, %{path: "/x"}))
      assert code(taken) == "plugin.id_taken"
      assert Flezha.Error.from_reason(elem(taken, 1), nil).message =~ "namespace '#{@namespace}'"
    end

    # The bound copy connects, so it is given a peer that answers: one that
    # refuses would log a failed connect at a moment the test does not choose.
    test "disable removes the entry and leaves bindings on their relays" do
      op = start_relay("named_disable_op")
      uri = "ws://127.0.0.1:#{WebsocketServer.start().port}/feed"
      {:ok, _} = PluginManager.enable_plugin("ws_gone", plugin("ws_gone", Websocket, %{uri: uri}))
      :ok = PluginManager.bind_plugin(@namespace, "ws_gone", op, [])

      assert {:ok, _} = PluginManager.disable_plugin(@namespace, "ws_gone")

      refute "ws_gone" in enabled()
      assert {:ok, [{"ws_gone", _binding}]} = HubManager.relay_bindings(@namespace, op)
    end
  end

  describe "bind" do
    test "a name that is not enabled is plugin.not_found and the entry is unchanged" do
      op = start_relay("named_nope_op")

      assert code(PluginManager.bind_plugin(@namespace, "nope", op, [])) == "plugin.not_found"
      assert {:ok, []} = HubManager.relay_bindings(@namespace, op)
    end

    test "a relay with no entry is refused and no entry is created" do
      {:ok, _} = PluginManager.enable_plugin("ws_orphan", websocket("ws_orphan"))

      assert {:error, _} = PluginManager.bind_plugin(@namespace, "ws_orphan", "ghost_op", [])
      refute Enum.any?(HubManager.get_all_relays(@namespace), &(elem(&1, 0) == "ghost_op"))
    end

    test "a second relay on a plugin that cannot be shared is plugin.not_shareable" do
      a = start_relay("named_share_a")
      b = start_relay("named_share_b")

      {:ok, _} =
        PluginManager.enable_plugin(
          "in",
          plugin("in", FileSource, %{path: "named_in.txt", channel: "in"})
        )

      :ok = PluginManager.bind_plugin(@namespace, "in", a, [])

      refused = PluginManager.bind_plugin(@namespace, "in", b, [])
      assert code(refused) == "plugin.not_shareable"
      assert Flezha.Error.from_reason(elem(refused, 1), nil).message =~ a
    end

    test "two binds racing for a plugin that cannot be shared bind one relay" do
      a = start_relay("named_race_a")
      b = start_relay("named_race_b")

      {:ok, _} =
        PluginManager.enable_plugin(
          "race_in",
          plugin("race_in", FileSource, %{path: "named_race.txt", channel: "in"})
        )

      # Each bind reads its relay's bindings after its check: held open
      # here, both checks run before either write lands.
      stub(HubManager, :relay_bindings, fn namespace, relay_id ->
        Process.sleep(300)
        Mimic.call_original(HubManager, :relay_bindings, [namespace, relay_id])
      end)

      outcomes =
        [a, b]
        |> Enum.map(fn op ->
          Task.async(fn -> PluginManager.bind_plugin(@namespace, "race_in", op, []) end)
        end)
        |> Task.await_many(10_000)

      assert [:ok] = Enum.filter(outcomes, &(&1 == :ok))
      assert [refused] = Enum.reject(outcomes, &(&1 == :ok))
      assert code(refused) == "plugin.not_shareable"
    end

    test "unbind of a binding the relay does not have is plugin.not_bound" do
      op = start_relay("named_unbound_op")
      {:ok, _} = PluginManager.enable_plugin("ws_free", websocket("ws_free"))

      assert code(PluginManager.unbind_plugin(@namespace, "ws_free", op)) == "plugin.not_bound"
    end

    test "unbind leaves the plugin enabled" do
      op = start_relay("named_unbind_op")
      server = WebsocketServer.start()
      uri = "ws://127.0.0.1:#{server.port}/feed"
      {:ok, _} = PluginManager.enable_plugin("ws_keep", plugin("ws_keep", Websocket, %{uri: uri}))
      :ok = PluginManager.bind_plugin(@namespace, "ws_keep", op, [])

      assert :ok = PluginManager.unbind_plugin(@namespace, "ws_keep", op)

      assert {:ok, []} = HubManager.relay_bindings(@namespace, op)
      assert "ws_keep" in enabled()
    end
  end

  describe "a relay entry with old plugin rows" do
    test "starts without them and one warning names the relay and the ids" do
      on_exit(NodeTestHelper.override_log_level(:warning))
      old = %Plugin{id: "w", provider: Flezha.Plugin.Provider.StdOut}

      log =
        capture_log([level: :warning], fn ->
          {:ok, _} =
            %{RelayFixture.get("named_old_op", @factory) | opts: [plugins: [old]]}
            |> RelayFixture.start(@namespace)
        end)

      {:ok, ow} = HubManager.get_pid(@namespace, "named_old_op")
      assert %{extensions: %{plugins: plugins}} = :sys.get_state(ow)
      assert plugins == %{}
      assert [_one] = Regex.scan(~r/named_old_op.*old plugin rows.*\["w"\]/, log)
    end
  end

  describe "a refusal reaches the CLI as its rendered error" do
    defp cli(fun) do
      {outcome, _out, err} = FlezhaTest.Helper.CliCapture.run(fun)
      assert {:exit, {:shutdown, _code}} = outcome
      err
    end

    defp door(name, plugin, fields),
      do: Cli.call(name, Jason.encode!([plugin, Map.put(fields, "namespace", @namespace)]), [])

    test "plugin.not_found" do
      op = start_relay("cli_nope_op")

      err = cli(fn -> door("PluginManager.bind", "cli_nope", %{"relay_id" => op}) end)

      assert err =~ "error: plugin 'cli_nope' is not enabled (plugin.not_found)"
    end

    test "plugin.not_bound" do
      op = start_relay("cli_unbound_op")
      {:ok, _} = PluginManager.enable_plugin("cli_free", websocket("cli_free"))

      err = cli(fn -> door("PluginManager.unbind", "cli_free", %{"relay_id" => op}) end)

      assert err =~ "relay '#{op}' is not bound to plugin 'cli_free' (plugin.not_bound)"
    end

    test "plugin.not_shareable" do
      a = start_relay("cli_share_a")
      b = start_relay("cli_share_b")

      {:ok, _} =
        PluginManager.enable_plugin(
          "cli_in",
          plugin("cli_in", FileSource, %{path: "cli_in.txt", channel: "in"})
        )

      :ok = PluginManager.bind_plugin(@namespace, "cli_in", a)

      err = cli(fn -> door("PluginManager.bind", "cli_in", %{"relay_id" => b}) end)

      assert err =~ "plugin 'cli_in' cannot be shared, and relay '#{a}' is already bound to it"
    end

    test "plugin.single_output" do
      op = start_relay("cli_single_op")
      one = %{plugin("cli_one", Flezha.Plugin.Provider.StdOut, %{}) | single: true}
      {:ok, _} = PluginManager.enable_plugin("cli_one", one)
      hooks = [%{"name" => "on_output", "opts" => %{}}]

      err =
        cli(fn ->
          door("PluginManager.bind", "cli_one", %{"relay_id" => op, "hooks" => hooks})
        end)

      assert err =~
               "runs as one copy in the cluster, so it takes no on_output hook (plugin.single_output)"
    end

    test "plugin.id_taken" do
      {:ok, _} = PluginManager.enable_plugin("cli_taken", websocket("cli_taken"))

      err =
        cli(fn ->
          door("PluginManager.enable", "cli_taken", %{"provider" => "relay_stdout"})
        end)

      assert err =~
               "plugin 'cli_taken' is already enabled in namespace '#{@namespace}' (plugin.id_taken)"
    end
  end
end
