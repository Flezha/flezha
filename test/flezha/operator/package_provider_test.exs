defmodule FlezhaTest.Operator.PackageProviderTest do
  @moduledoc """
  A plugin package's provider on a live operator: a binding whose provider no
  package here loaded leaves the operator running, the binding not started and
  reported, logged once; a package provider's own refusal reaches the caller
  with a core code, wrapped only when the catalogue does not map it, on the
  bind path and in the park reason.

  A worker that fails to start does not park an operator: activation failures
  leave the plugin prepared for the convergence loop to heal, so `provider_failed`
  is asserted on the bind path only.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  @moduletag :capture_log

  import ExUnit.CaptureLog

  alias Flezha.Operator.Builder
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager
  alias Flezha.TestHelpers.PluginPackage
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "default"
  @factory "store_input"
  @stamp FlezhaExamplePlugin.Stamp

  setup %{tmp: tmp} do
    root = Path.join(tmp, "plugins")
    on_exit(fn -> PluginPackage.unload!(root) end)
    on_exit(fn -> OperatorManager.stop_all_operators(@namespace) end)
    {:ok, root: root, example: PluginPackage.example!()}
  end

  defp stamp_plugin(name, provider \\ @stamp, path) do
    %Plugin{
      id: name,
      provider: provider,
      namespace: @namespace,
      opts: %{path: path},
      hooks: [{:on_output, []}]
    }
  end

  # An operator started with a binding to `name`, as a restart or a move
  # rebuilds it from its declared entry.
  defp start_bound(id, name) do
    binding = %Plugin{
      id: name,
      provider: @stamp,
      namespace: @namespace,
      hooks: [{:on_output, []}]
    }

    OperatorManager.start_operator(@namespace, %Builder{
      id: id,
      factory_id: @factory,
      opts: [bindings: [{name, binding}]]
    })
  end

  defp enable(%Plugin{id: name} = plugin) do
    on_exit(fn -> PluginManager.disable_plugin(@namespace, name) end)
    PluginManager.enable_plugin(name, plugin)
  end

  # The reason the operator parked with, wherever the start's answer nests it.
  defp prepare_failure({:plugin_prepare_failed, _, _} = reason), do: reason

  defp prepare_failure(term) when is_tuple(term),
    do: term |> Tuple.to_list() |> Enum.find_value(&prepare_failure/1)

  defp prepare_failure(_term), do: nil

  defp enable_on_live_operator(id, plugin, family? \\ true) do
    if family?, do: start_supervised!({WorkerSup, name: WorkerSup.family(plugin.provider)})
    {:ok, _} = OperatorManager.start_operator(@namespace, %Builder{id: id, factory_id: @factory})
    name = "s-#{id}"
    enable(%{plugin | id: name, operator_id: id})
  end

  # The package leaves this VM and its archive the plugin root: what a
  # controller that never installed it holds.
  defp uninstall!(root) do
    PluginPackage.unload!(root)
    File.rm_rf!(root)
  end

  defp table(id) do
    {:ok, %{extensions: table, init_error: init_error}} =
      OperatorManager.get_overwatch_state(@namespace, id)

    {table, init_error}
  end

  describe "a binding whose provider no package here loaded" do
    test "leaves the operator running, the binding not started, warned once",
         %{root: root, example: example, tmp: tmp} do
      [%{status: :loaded}] = PluginPackage.load!(root, [example])
      {:ok, _} = enable(stamp_plugin("s", Path.join(tmp, "x")))
      uninstall!(root)
      on_exit(NodeTestHelper.override_log_level(:warning))

      log =
        capture_log([level: :warning], fn ->
          assert {:ok, _} = start_bound("orphan_op", "s")
        end)

      {table, init_error} = table("orphan_op")
      assert init_error == nil
      controller = node()

      assert {:not_started, {:module_not_loaded, @stamp, ^controller} = why} =
               Table.phase(table, "s")

      assert Table.health(table) == %{"s" => {:unhealthy, why}}

      error = Flezha.Error.from_reason(why, nil)
      assert error.code == "plugin.provider_not_loaded"
      assert error.details["controller"] == to_string(controller)
      assert error.message =~ "no plugin package installed there provides it"
      assert length(String.split(log, "operator orphan_op runs without plugin s:")) == 2
    end

    test "names the package that declared it and its refusal",
         %{root: root, example: example, tmp: tmp} do
      [%{status: :loaded}] = PluginPackage.load!(root, [example])
      {:ok, _} = enable(stamp_plugin("s", Path.join(tmp, "x")))
      uninstall!(root)

      refused =
        PluginPackage.rewrite(example, Path.join(tmp, "old_otp.fpz"), [
          {:manifest, &put_in(&1.built_with.otp, "0")}
        ])

      [%{status: :refused}] = PluginPackage.load!(root, [refused])
      assert {:ok, _} = start_bound("refused_op", "s")

      {table, nil} = table("refused_op")

      assert {:not_started,
              {:provider_unavailable, "FlezhaExamplePlugin.Stamp", "flezha_example_plugin",
               "refused", "plugin.runtime_mismatch"} = why} = Table.phase(table, "s")

      assert Flezha.Error.from_reason(why, nil).code == "plugin.provider_unavailable"
    end
  end

  describe "a loaded package provider's own refusal" do
    test "an uncatalogued refusal of its options is plugin.provider_refused, on bind and in a park",
         %{root: root, example: example} do
      [%{status: :loaded}] = PluginPackage.load!(root, [example])

      assert {:error, reason} = enable_on_live_operator("refuse_op", stamp_plugin("s", ""))
      error = Flezha.Error.from_reason(reason, "plugin.enable")
      assert error.code == "plugin.provider_refused"

      assert error.details == %{
               "plugin" => "flezha_example_plugin",
               "provider" => "example_stamp",
               "reason" => ":stamp_path_empty"
             }

      {:ok, _} = enable(stamp_plugin("p", ""))

      capture_log(fn ->
        assert {:error, answer} = start_bound("refuse_park_op", "p")
        send(self(), {:answer, answer})
      end)

      assert_received {:answer, answer}

      assert {:plugin_prepare_failed, "p",
              {:provider_refused, "flezha_example_plugin", "example_stamp", _}} =
               prepare_failure(answer)
    end

    test "a worker that fails to start is plugin.provider_failed on bind",
         %{root: root, example: example, tmp: tmp} do
      [{writer, beam}] =
        PluginPackage.compile_unloaded("""
        defmodule FlezhaExamplePlugin.Stamp.Writer do
          def child_spec(opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
          def start_link(_opts), do: {:error, :disk_full}
        end
        """)

      failing =
        PluginPackage.rewrite(example, Path.join(tmp, "failing.fpz"), [
          {:put, "lib/flezha_example_plugin-0.1.0/ebin/#{writer}.beam", beam}
        ])

      [%{status: :loaded}] = PluginPackage.load!(root, [failing])

      assert {:error, reason} =
               enable_on_live_operator("fail_op", stamp_plugin("s", Path.join(tmp, "x")))

      error = Flezha.Error.from_reason(reason, "plugin.enable")
      assert error.code == "plugin.provider_failed"
      assert error.details["plugin"] == "flezha_example_plugin"
      assert error.details["reason"] =~ "disk_full"
    end

    test "a refusal the catalogue maps by its tag keeps its code, on bind and in a park",
         %{root: root, tmp: tmp} do
      catalogued =
        PluginPackage.sibling(Path.join(tmp, "flz_tagged.fpz"), "flz_tagged", [], """
        @impl true
        def init_provider(_opts), do: {:error, {:unknown_provider, "tagged"}}

        @impl true
        def on_output(ctx, _opts), do: {:ok, ctx}
        """)

      [%{status: :loaded}] = PluginPackage.load!(root, [catalogued])

      plugin = %Plugin{
        id: "s",
        provider: FlzTagged.Provider,
        namespace: @namespace,
        opts: %{},
        hooks: [{:on_output, []}]
      }

      assert {:error, reason} = enable_on_live_operator("tag_op", plugin, false)
      assert Flezha.Error.from_reason(reason, "plugin.enable").code == "plugin.unknown_provider"

      {:ok, _} = enable(%{plugin | id: "t", hooks: []})

      capture_log(fn ->
        assert {:error, answer} = start_bound("tag_park_op", "t")
        send(self(), {:answer, answer})
      end)

      assert_received {:answer, answer}
      assert {:plugin_prepare_failed, "t", why} = prepare_failure(answer)
      assert Flezha.Error.from_reason(why, nil).code == "plugin.unknown_provider"
    end
  end
end
