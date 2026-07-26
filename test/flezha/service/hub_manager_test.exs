defmodule Flezha.Service.HubManagerTest do
  @moduledoc """
  Asserts the cspec-mutation contract for
  `Flezha.Service.HubManager.rewrite_cspec_plugins/2` and
  `Flezha.Service.HubManager.operative_plugins/2`.

  After the `auto-restore-on-boot` change, `update_operative_plugins/3` was
  collapsed into `Flezha.Service.PluginManager` (which writes through
  `ProcessHub.Service.ProcessRegistry.update/3` directly). The pure
  transformer `rewrite_cspec_plugins/2` survived in `HubManager` so
  PluginManager and any future caller share the cspec-encoding logic.

  These tests exercise the transformer in isolation; the durable-write
  path is covered in `Flezha.Service.PluginManagerTest`.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin
  alias Flezha.Operative.Builder
  alias Flezha.Service.HubManager

  @namespace "default"

  defp operative_builder(opts) do
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
      start: {Flezha.Operative.Overwatch, :start_link, [start_arg]},
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
      operative_id: "ignored"
    )
  end

  defp extract_plugins(rewritten) do
    {Flezha.Operative.Overwatch, :start_link, [arg]} = rewritten.start

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
      # the first runner connect.
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

  describe "consent_settings/0" do
    test "defaults derive max_defer_time from the graceful-shutdown budget" do
      previous_consent = Application.get_env(:flezha, :migration_consent)
      previous_timeout = Application.get_env(:flezha, :graceful_shutdown_timeout_ms)
      Application.delete_env(:flezha, :migration_consent)
      Application.put_env(:flezha, :graceful_shutdown_timeout_ms, 60_000)

      try do
        settings = HubManager.consent_settings()
        assert %ProcessHub.Strategy.Migration.MigrationConsent{} = settings
        assert settings.consent_timeout == 5_000
        assert settings.retry_interval == 2_000
        assert settings.max_defer_time == 60_000
      after
        restore_env(:migration_consent, previous_consent)
        restore_env(:graceful_shutdown_timeout_ms, previous_timeout)
      end
    end

    test "explicit max_defer_ms overrides the derived default" do
      previous_consent = Application.get_env(:flezha, :migration_consent)

      Application.put_env(:flezha, :migration_consent,
        consent_timeout_ms: 1_000,
        retry_interval_ms: 500,
        max_defer_ms: 10_000
      )

      try do
        settings = HubManager.consent_settings()
        assert settings.consent_timeout == 1_000
        assert settings.retry_interval == 500
        assert settings.max_defer_time == 10_000
      after
        restore_env(:migration_consent, previous_consent)
      end
    end

    test "the hub strategy carries the resolved consent settings" do
      strategy = HubManager.migration_strategy()
      assert %ProcessHub.Strategy.Migration.HotSwap{handover: true} = strategy
      assert strategy.consent_settings == HubManager.consent_settings()
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:flezha, key)
  defp restore_env(key, value), do: Application.put_env(:flezha, key, value)

  describe "rewrite_cspec_plugins/2" do
    test "appends a plugin to a cspec with empty opts[:plugins]" do
      builder = operative_builder(plugins: [])
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

      builder = operative_builder(plugins: [{"a", a}, {"b", b}, {"c", c}])
      cs = cspec({@namespace, "ce_1", builder})

      rewritten =
        HubManager.rewrite_cspec_plugins(cs, fn current ->
          Enum.reject(current, fn {id, _} -> id == "b" end)
        end)

      assert Enum.map(extract_plugins(rewritten), fn {id, _} -> id end) == ["a", "c"]
    end

    test "handles the {namespace, operative} start-args shape" do
      builder = operative_builder(plugins: [])
      cs = cspec({@namespace, builder})

      rewritten =
        HubManager.rewrite_cspec_plugins(cs, fn _ ->
          [{"only", plugin("only")}]
        end)

      assert [{"only", _}] = extract_plugins(rewritten)
      # The 2-tuple shape is preserved.
      assert {Flezha.Operative.Overwatch, :start_link, [{@namespace, %Builder{}}]} =
               rewritten.start
    end

    test "handles the {namespace, runner_id, operative} start-args shape" do
      builder = operative_builder(plugins: [])
      cs = cspec({@namespace, "ce_42", builder})

      rewritten =
        HubManager.rewrite_cspec_plugins(cs, fn _ ->
          [{"only", plugin("only")}]
        end)

      assert [{"only", _}] = extract_plugins(rewritten)

      # The runner_id position must survive the rewrite. Losing it
      # would break alter_child_data's invariants on the next restart.
      assert {Flezha.Operative.Overwatch, :start_link, [{@namespace, "ce_42", %Builder{}}]} =
               rewritten.start
    end

    @tag :bench
    test "1000 sequential cspec rewrites stay under the documented latency budget" do
      builder = operative_builder(plugins: [])
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

  describe "recovery-marker resolution" do
    @tag :tmp_dir
    test "default config derives marker path under process_hub_registry_root", %{tmp_dir: tmp_dir} do
      previous_enabled = Application.get_env(:flezha, :auto_restore_enabled)
      previous_root = Application.get_env(:flezha, :process_hub_registry_root)
      previous_override = Application.get_env(:flezha, :process_hub_recovery_marker_path)
      Application.put_env(:flezha, :auto_restore_enabled, true)
      Application.put_env(:flezha, :process_hub_registry_root, tmp_dir)
      Application.delete_env(:flezha, :process_hub_recovery_marker_path)

      try do
        path = HubManager.resolved_marker_path(:my_test_hub)
        assert path == Path.join([tmp_dir, "my_test_hub", "cluster.healthy"])
      after
        Application.put_env(:flezha, :auto_restore_enabled, previous_enabled)

        if previous_root,
          do: Application.put_env(:flezha, :process_hub_registry_root, previous_root),
          else: Application.delete_env(:flezha, :process_hub_registry_root)

        if previous_override do
          Application.put_env(:flezha, :process_hub_recovery_marker_path, previous_override)
        end
      end
    end

    test "marker-path env override is honoured for any hub_id" do
      previous_override = Application.get_env(:flezha, :process_hub_recovery_marker_path)

      Application.put_env(
        :flezha,
        :process_hub_recovery_marker_path,
        "/var/lib/process_hub/whatever/cluster.healthy"
      )

      try do
        assert HubManager.resolved_marker_path(:default_hub) ==
                 "/var/lib/process_hub/whatever/cluster.healthy"

        assert HubManager.resolved_marker_path(:market_hub) ==
                 "/var/lib/process_hub/whatever/cluster.healthy"
      after
        if previous_override do
          Application.put_env(:flezha, :process_hub_recovery_marker_path, previous_override)
        else
          Application.delete_env(:flezha, :process_hub_recovery_marker_path)
        end
      end
    end
  end

  describe "hook_insert_pid/2 routing" do
    test "an unknown runner's missing stream supervisors do not crash the caller" do
      # This runs inside the `child_registered` hook, where an exit aborts the
      # registry write that dispatched it.
      hook_data = %{child_id: "r1", node_pids: [{node(), self()}]}

      assert HubManager.hook_insert_pid("no_such_runner", hook_data) == :ok
    end

    test "no local pid means nothing to route" do
      assert HubManager.hook_insert_pid("e1", %{child_id: "r1", node_pids: []}) == nil

      remote = %{child_id: "r1", node_pids: [{:other@node, self()}]}
      assert HubManager.hook_insert_pid("e1", remote) == nil
    end

    test "a tag left on the hook data does not gate the route" do
      # ProcessHub never passes `:metadata` to `child_registered`, so a tag gate
      # here would be dead code pretending to protect the data plane. The route
      # is operative -> local pid, which any runner's channel resolves alike.
      hook_data = %{child_id: "r1", node_pids: [{node(), self()}], metadata: %{tag: "e2"}}

      assert HubManager.hook_insert_pid("no_such_runner", hook_data) == :ok
    end
  end

  describe "recovery state (Snapshot + RecoveryGate)" do
    alias Flezha.Convergence.{RecoveryGate, Snapshot}

    test "await returns :ok immediately when target is already :normal" do
      hub_id = :"hm_rt_normal_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :normal)
      assert :ok = RecoveryGate.await_recovery_complete([hub_id], 200)
    end

    test "await returns :ok immediately when target is already :skipped" do
      hub_id = :"hm_rt_skipped_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :skipped)
      assert :ok = RecoveryGate.await_recovery_complete([hub_id], 200)
    end

    test "await blocks until :complete telemetry fires" do
      hub_id = :"hm_rt_blocking_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :recovering)
      test_pid = self()

      task =
        Task.async(fn ->
          send(test_pid, :await_started)
          RecoveryGate.await_recovery_complete([hub_id], 2_000)
        end)

      assert_receive :await_started, 1_000

      # Simulate the upstream recovery_state_changed hook reaching :normal.
      Snapshot.handle_recovery_transition(hub_id, %{to: :normal, reason: :replay_complete})

      assert :ok = Task.await(task, 2_000)
    end

    test "await times out and reports outstanding hub_ids" do
      hub_id = :"hm_rt_timeout_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :recovering)

      assert {:error, {:recovery_timeout, [^hub_id]}} =
               RecoveryGate.await_recovery_complete([hub_id], 100)
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end
end
