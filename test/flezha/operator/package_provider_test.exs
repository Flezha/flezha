defmodule FlezhaTest.Operator.PackageProviderTest do
  @moduledoc """
  A plugin package's provider on a live operator: a persisted plugin whose
  provider no package here loaded parks naming why, logged once; a package
  provider's own refusal reaches the caller with a core code, wrapped only when
  the catalogue does not map it, on the enable path and in the park reason.

  A worker that fails to start does not park an operator: activation failures
  leave the plugin prepared for the convergence loop to heal, so `provider_failed`
  is asserted on the enable path only.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  @moduletag :capture_log

  import ExUnit.CaptureLog

  alias Flezha.Operator.Builder
  alias Flezha.Operator.Lifecycle
  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager
  alias Flezha.TestHelpers.PluginPackage

  @namespace "default"
  @factory "store_input"
  @stamp FlezhaExamplePlugin.Stamp

  setup %{tmp: tmp} do
    root = Path.join(tmp, "plugins")
    on_exit(fn -> PluginPackage.unload!(root) end)
    on_exit(fn -> OperatorManager.stop_all_operators(@namespace) end)
    {:ok, root: root, example: PluginPackage.example!()}
  end

  defp stamp_plugin(provider \\ @stamp, path) do
    %Plugin{id: "s", provider: provider, opts: %{path: path}, hooks: [{:on_output, []}]}
  end

  defp start(id, plugin) do
    OperatorManager.start_operator(@namespace, %Builder{
      id: id,
      factory_id: @factory,
      opts: [plugins: [plugin]]
    })
  end

  # The reason the operator parked with, wherever the start's answer nests it.
  defp prepare_failure({:plugin_prepare_failed, _, _} = reason), do: reason

  defp prepare_failure(term) when is_tuple(term),
    do: term |> Tuple.to_list() |> Enum.find_value(&prepare_failure/1)

  defp prepare_failure(_term), do: nil

  defp enable_on_live_operator(id, plugin, family? \\ true) do
    if family?, do: start_supervised!({WorkerSup, name: WorkerSup.family(plugin.provider)})
    {:ok, _} = OperatorManager.start_operator(@namespace, %Builder{id: id, factory_id: @factory})

    PluginManager.enable_plugin("s-#{id}", %{
      plugin
      | id: "s-#{id}",
        namespace: @namespace,
        operator_id: id
    })
  end

  describe "a persisted plugin whose provider no package here loaded" do
    test "parks naming that no installed package provides it, logged once", %{tmp: tmp} do
      log =
        capture_log(fn ->
          assert {:error, answer} = start("orphan_op", stamp_plugin(Path.join(tmp, "x")))
          send(self(), {:answer, answer})
        end)

      assert_received {:answer, answer}

      assert {:plugin_prepare_failed, "s", {:module_not_loaded, @stamp} = why} =
               prepare_failure(answer)

      error = Flezha.Error.from_reason(why, nil)
      assert error.code == "plugin.provider_not_loaded"
      assert error.message =~ "no plugin package installed on this controller provides it"
      assert length(String.split(log, "operator orphan_op parked: plugin s:")) == 2
    end

    test "parks naming the package that declared it and its refusal",
         %{root: root, tmp: tmp} = ctx do
      refused =
        PluginPackage.rewrite(ctx.example, Path.join(tmp, "old_otp.fpz"), [
          {:manifest, &put_in(&1.built_with.otp, "0")}
        ])

      [%{status: :refused}] = PluginPackage.load!(root, [refused])

      capture_log(fn ->
        assert {:error, answer} = start("refused_op", stamp_plugin(Path.join(tmp, "x")))
        send(self(), {:answer, answer})
      end)

      assert_received {:answer, answer}

      assert {:plugin_prepare_failed, "s",
              {:provider_unavailable, "FlezhaExamplePlugin.Stamp", "flezha_example_plugin",
               "refused", "plugin.runtime_mismatch"} = why} = prepare_failure(answer)

      assert Flezha.Error.from_reason(why, nil).code == "plugin.provider_unavailable"
    end

    test "a park repeated for the same reason is not logged again" do
      reason = {:plugin_prepare_failed, "s", {:module_not_loaded, @stamp}}
      state = %Overwatch{id: "twice", namespace: @namespace}

      log =
        capture_log(fn ->
          parked = Lifecycle.park(state, reason)
          Lifecycle.park(parked, reason)
        end)

      assert length(String.split(log, "operator twice parked")) == 2
    end
  end

  describe "a loaded package provider's own refusal" do
    test "an uncatalogued refusal of its options is plugin.provider_refused, on enable and in a park",
         %{root: root, example: example} do
      [%{status: :loaded}] = PluginPackage.load!(root, [example])

      assert {:error, reason} = enable_on_live_operator("refuse_op", stamp_plugin(""))
      error = Flezha.Error.from_reason(reason, "plugin.enable")
      assert error.code == "plugin.provider_refused"

      assert error.details == %{
               "plugin" => "flezha_example_plugin",
               "provider" => "example_stamp",
               "reason" => ":stamp_path_empty"
             }

      capture_log(fn ->
        assert {:error, answer} = start("refuse_park_op", stamp_plugin(""))
        send(self(), {:answer, answer})
      end)

      assert_received {:answer, answer}

      assert {:plugin_prepare_failed, "s",
              {:provider_refused, "flezha_example_plugin", "example_stamp", _}} =
               prepare_failure(answer)
    end

    test "a worker that fails to start is plugin.provider_failed on enable",
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
               enable_on_live_operator("fail_op", stamp_plugin(Path.join(tmp, "x")))

      error = Flezha.Error.from_reason(reason, "plugin.enable")
      assert error.code == "plugin.provider_failed"
      assert error.details["plugin"] == "flezha_example_plugin"
      assert error.details["reason"] =~ "disk_full"
    end

    test "a refusal the catalogue maps by its tag keeps its code, on enable and in a park",
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
        opts: %{},
        hooks: [{:on_output, []}]
      }

      assert {:error, reason} = enable_on_live_operator("tag_op", plugin, false)
      assert Flezha.Error.from_reason(reason, "plugin.enable").code == "plugin.unknown_provider"

      capture_log(fn ->
        assert {:error, answer} = start("tag_park_op", plugin)
        send(self(), {:answer, answer})
      end)

      assert_received {:answer, answer}
      assert {:plugin_prepare_failed, "s", why} = prepare_failure(answer)
      assert Flezha.Error.from_reason(why, nil).code == "plugin.unknown_provider"
    end
  end
end
