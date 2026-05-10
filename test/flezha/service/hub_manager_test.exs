defmodule Flezha.Service.HubManagerTest do
  @moduledoc """
  Asserts the cspec-mutation contract for
  `Flezha.Service.HubManager.rewrite_cspec_plugins/2` and
  `Flezha.Service.HubManager.relay_plugins/2`.

  After the `auto-restore-on-boot` change, `update_relay_plugins/3` was
  collapsed into `Flezha.Service.PluginManager` (which writes through
  `ProcessHub.Service.ProcessRegistry.update/3` directly). The pure
  transformer `rewrite_cspec_plugins/2` survived in `HubManager` so
  PluginManager and any future caller share the cspec-encoding logic.

  These tests exercise the transformer in isolation; the durable-write
  path is covered in `Flezha.Service.PluginManagerTest`.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin
  alias Flezha.Relay.Builder
  alias Flezha.Service.HubManager

  @namespace "default"

  defp relay_builder(opts) do
    %Builder{
      id: "ignored",
      factory_id: "do_nothing",
      init_values: %{},
      opts: opts
    }
  end

  defp cspec(start_arg) do
    %{
      id: "ignored",
      start: {Flezha.Relay.Overwatch, :start_link, [start_arg]},
      restart: :transient
    }
  end

  defp plugin(id, opts \\ %{}) do
    Plugin.new(
      id: id,
      provider: __MODULE__.NoopProvider,
      opts: opts,
      subscriptions: [{:on_output, []}],
      namespace: @namespace,
      relay_id: "ignored"
    )
  end

  defp extract_plugins(rewritten) do
    {Flezha.Relay.Overwatch, :start_link, [arg]} = rewritten.start

    builder =
      case arg do
        {_ns, b} -> b
        {_ns, _ce, b} -> b
      end

    Keyword.get(builder.opts, :plugins, [])
  end

  describe "registry_backend_config/1" do
    @tag :tmp_dir
    test "regression_ensures_registry_backend_atom_routes_to_real_backend", %{tmp_dir: tmp_dir} do
      # A regression test for the `:durable_ets` typo: ProcessHub's
      # `resolve_registry_backend/1` only recognises `:dets` and `:ets`
      # as backend identifiers — anything else is treated as a module
      # name and crashes `setup_storage/2` on first use. Pinning the
      # atom here means the next typo fails fast in CI instead of at
      # the first executor connect.
      previous_enabled = Application.get_env(:flezha, :auto_restore_enabled)
      previous_root = Application.get_env(:flezha, :process_hub_registry_root)
      Application.put_env(:flezha, :auto_restore_enabled, true)
      Application.put_env(:flezha, :process_hub_registry_root, tmp_dir)

      try do
        assert {:dets, opts} = HubManager.registry_backend_config(:any_hub_id_for_test)
        assert is_list(opts)
        assert Keyword.has_key?(opts, :path)
      after
        Application.put_env(:flezha, :auto_restore_enabled, previous_enabled)

        if previous_root do
          Application.put_env(:flezha, :process_hub_registry_root, previous_root)
        else
          Application.delete_env(:flezha, :process_hub_registry_root)
        end
      end
    end

    test "returns :ets when auto_restore is disabled" do
      previous = Application.get_env(:flezha, :auto_restore_enabled)
      Application.put_env(:flezha, :auto_restore_enabled, false)

      try do
        assert :ets = HubManager.registry_backend_config(:any_hub_id_for_test)
      after
        Application.put_env(:flezha, :auto_restore_enabled, previous)
      end
    end
  end

  describe "rewrite_cspec_plugins/2" do
    test "appends a plugin to a cspec with empty opts[:plugins]" do
      builder = relay_builder(plugins: [])
      cs = cspec({@namespace, "ce_1", builder})
      new_plugin = plugin("plug_x", %{a: 1})

      rewritten =
        HubManager.rewrite_cspec_plugins(cs, fn current ->
          current ++ [{"plug_x", new_plugin}]
        end)

      assert [{"plug_x", got}] = extract_plugins(rewritten)
      assert got.id == "plug_x"
      assert got.opts == %{a: 1}
    end

    test "removes a plugin from a cspec with multiple opts[:plugins]" do
      a = plugin("a")
      b = plugin("b")
      c = plugin("c")

      builder = relay_builder(plugins: [{"a", a}, {"b", b}, {"c", c}])
      cs = cspec({@namespace, "ce_1", builder})

      rewritten =
        HubManager.rewrite_cspec_plugins(cs, fn current ->
          Enum.reject(current, fn {id, _} -> id == "b" end)
        end)

      assert Enum.map(extract_plugins(rewritten), fn {id, _} -> id end) == ["a", "c"]
    end

    test "handles the {namespace, relay} start-args shape" do
      builder = relay_builder(plugins: [])
      cs = cspec({@namespace, builder})

      rewritten =
        HubManager.rewrite_cspec_plugins(cs, fn _ ->
          [{"only", plugin("only")}]
        end)

      assert [{"only", _}] = extract_plugins(rewritten)
      # The 2-tuple shape is preserved.
      assert {Flezha.Relay.Overwatch, :start_link, [{@namespace, %Builder{}}]} =
               rewritten.start
    end

    test "handles the {namespace, executor_id, relay} start-args shape" do
      builder = relay_builder(plugins: [])
      cs = cspec({@namespace, "ce_42", builder})

      rewritten =
        HubManager.rewrite_cspec_plugins(cs, fn _ ->
          [{"only", plugin("only")}]
        end)

      assert [{"only", _}] = extract_plugins(rewritten)

      # The executor_id position must survive the rewrite. Losing it
      # would break alter_child_data's invariants on the next restart.
      assert {Flezha.Relay.Overwatch, :start_link, [{@namespace, "ce_42", %Builder{}}]} =
               rewritten.start
    end

    @tag :bench
    test "1000 sequential cspec rewrites stay under the documented latency budget" do
      builder = relay_builder(plugins: [])
      cs = cspec({@namespace, "ce_1", builder})

      times =
        for _ <- 1..1000 do
          {us, _} =
            :timer.tc(fn ->
              HubManager.rewrite_cspec_plugins(cs, fn p -> p end)
            end)

          us
        end

      sorted = Enum.sort(times)
      median_us = Enum.at(sorted, div(length(sorted), 2))
      # Pure in-memory rewrite should be sub-millisecond; 5 ms is a
      # generous CI ceiling.
      assert median_us < 5_000
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end
end
