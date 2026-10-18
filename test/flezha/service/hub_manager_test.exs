defmodule Flezha.Service.HubManagerTest do
  @moduledoc """
  Asserts the cspec-mutation contract for
  `Flezha.Service.HubManager.rewrite_cspec_plugins/2` (a relay's
  bindings, or a plugin child's own entry) and
  `Flezha.Service.HubManager.relay_bindings/2`.

  After the `auto-restore-on-boot` change, `update_relay_plugins/3` was
  collapsed into `Flezha.Service.PluginManager` (which writes through
  `ProcessHub.Service.ProcessRegistry.update/3` directly). The pure
  transformer `rewrite_cspec_plugins/2` survived in `HubManager` so
  PluginManager and any future caller share the cspec-encoding logic.

  These tests exercise the transformer in isolation; the durable-write
  path is covered in `Flezha.Service.PluginManagerTest`.
  """

  # async: false: writes Application env; a shared on-disk root; the live controller and its fleet.

  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.Relay.Builder
  alias Flezha.Plugin
  alias Flezha.Service.HubManager
  alias ProcessHub.Service.Recovery, as: HubRecovery
  alias ProcessHub.Storage.RemoteManifest

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
      hooks: [{:on_output, []}],
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

    Keyword.get(builder.opts, :bindings, [])
  end

  describe "registry_backend_config/1" do
    @tag :tmp_dir
    test "regression_ensures_registry_backend_atom_routes_to_real_backend", %{tmp_dir: tmp_dir} do
      # Fails when a controller cannot boot at all: ProcessHub treats an
      # unrecognised backend atom as a module name and crashes
      # `setup_storage/2` on the first runner connect. Pin both the atom
      # and the module it must resolve to.
      previous_enabled = Application.get_env(:flezha, :auto_restore_enabled)
      previous_root = Application.get_env(:flezha, :process_hub_registry_root)
      Application.put_env(:flezha, :auto_restore_enabled, true)
      Application.put_env(:flezha, :process_hub_registry_root, tmp_dir)

      try do
        assert {:durable_ets, opts} = HubManager.registry_backend_config(:any_hub_id_for_test)
        assert is_list(opts)
        assert Keyword.has_key?(opts, :path)
        assert Code.ensure_loaded?(ProcessHub.Service.Storage.DurableEts)
        assert function_exported?(ProcessHub.Service.Storage.DurableEts, :open, 2)
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

    @tag :tmp_dir
    test "a boot that skips replay leaves the on-disk registry intact", %{tmp_dir: tmp_dir} do
      # Fails when a whole-cluster cold boot comes up with zero relays and
      # no way back: the boot that declines to replay must not erase the only
      # on-disk record a later recovery boot reads.
      hub_id = :registry_survives_skipped_replay_hub
      previous_enabled = Application.get_env(:flezha, :auto_restore_enabled)
      previous_root = Application.get_env(:flezha, :process_hub_registry_root)
      Application.put_env(:flezha, :auto_restore_enabled, true)
      Application.put_env(:flezha, :process_hub_registry_root, tmp_dir)

      try do
        {atom, opts} = HubManager.registry_backend_config(hub_id)

        assert atom == :durable_ets,
               "the configured backend must keep the file across a skipped replay"

        backend = ProcessHub.Service.Storage.DurableEts

        {:ok, ref} = backend.open(hub_id, opts)
        :ok = backend.insert(ref, "op1", {:cspec, "op1"})
        :ok = backend.close(ref)

        # A marker-present boot: replay is skipped because a live peer is
        # expected to supply the registry.
        {:ok, skipped} = backend.open(hub_id, Keyword.put(opts, :recovery_replay, false))
        assert backend.get(skipped, "op1") == nil, "the live registry must start empty"
        :ok = backend.close(skipped)

        # A later recovery boot must still find the row on disk.
        {:ok, replayed} = backend.open(hub_id, Keyword.put(opts, :recovery_replay, true))
        assert backend.get(replayed, "op1") == {:cspec, "op1"}
        :ok = backend.close(replayed)
      after
        Application.put_env(:flezha, :auto_restore_enabled, previous_enabled)

        if previous_root do
          Application.put_env(:flezha, :process_hub_registry_root, previous_root)
        else
          Application.delete_env(:flezha, :process_hub_registry_root)
        end
      end
    end
  end

  describe "consent_settings/0" do
    test "defaults derive max_defer_time from the graceful-shutdown budget" do
      Application.delete_env(:flezha, :migration_consent)
      Application.put_env(:flezha, :graceful_shutdown_timeout_ms, 60_000)

      settings = HubManager.consent_settings()
      assert %ProcessHub.Strategy.Migration.MigrationConsent{} = settings
      assert settings.consent_timeout == 5_000
      assert settings.retry_interval == 2_000
      assert settings.max_defer_time == 60_000
    end

    test "explicit max_defer_ms overrides the derived default" do
      Application.put_env(:flezha, :migration_consent,
        consent_timeout_ms: 1_000,
        retry_interval_ms: 500,
        max_defer_ms: 10_000
      )

      settings = HubManager.consent_settings()
      assert settings.consent_timeout == 1_000
      assert settings.retry_interval == 500
      assert settings.max_defer_time == 10_000
    end

    test "the hub strategy carries the resolved consent settings" do
      strategy = HubManager.migration_strategy()
      assert %ProcessHub.Strategy.Migration.HotSwap{handover: true} = strategy
      assert strategy.consent_settings == HubManager.consent_settings()
    end
  end

  describe "rewrite_cspec_plugins/2" do
    test "appends a binding to a cspec with empty opts[:bindings]" do
      builder = relay_builder(bindings: [])
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

    test "removes a binding from a cspec with multiple opts[:bindings]" do
      a = plugin("a")
      b = plugin("b")
      c = plugin("c")

      builder = relay_builder(bindings: [{"a", a}, {"b", b}, {"c", c}])
      cs = cspec({@namespace, "ce_1", builder})

      rewritten =
        HubManager.rewrite_cspec_plugins(cs, fn current ->
          Enum.reject(current, fn {id, _} -> id == "b" end)
        end)

      assert Enum.map(extract_plugins(rewritten), fn {id, _} -> id end) == ["a", "c"]
    end

    test "handles the {namespace, relay} start-args shape" do
      builder = relay_builder(bindings: [])
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

    test "handles the {namespace, runner_id, relay} start-args shape" do
      builder = relay_builder(bindings: [])
      cs = cspec({@namespace, "ce_42", builder})

      rewritten =
        HubManager.rewrite_cspec_plugins(cs, fn _ ->
          [{"only", plugin("only")}]
        end)

      assert [{"only", _}] = extract_plugins(rewritten)

      # The runner_id position must survive the rewrite. Losing it
      # would break alter_child_data's invariants on the next restart.
      assert {Flezha.Relay.Overwatch, :start_link, [{@namespace, "ce_42", %Builder{}}]} =
               rewritten.start
    end

    test "rewrites a plugin child's own entry, a list of one" do
      entry = %Plugin{id: "trades", provider: __MODULE__.NoopProvider, opts: %{uri: "a"}}

      cs = %{
        id: {:plugin, "trades"},
        start: {Flezha.Plugin.Home, :start_link, [{@namespace, entry}]}
      }

      rewritten =
        HubManager.rewrite_cspec_plugins(cs, fn [{"trades", %Plugin{} = plugin}] ->
          [{"trades", %Plugin{plugin | opts: %{uri: "b"}}}]
        end)

      assert {Flezha.Plugin.Home, :start_link, [{@namespace, %Plugin{opts: %{uri: "b"}}}]} =
               rewritten.start
    end

    @tag :bench
    test "1000 sequential cspec rewrites stay under the documented latency budget" do
      builder = relay_builder(bindings: [])
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

  describe "auto_recovery_config/0" do
    test "every key we advertise is one upstream reads, and every value is in range" do
      # Fails when a knob drifts out of `parse_config/1`'s accepted set or
      # bounds: upstream then ignores the key, warning only for one it
      # deprecated, or refuses the whole hub spec. Round-tripping through the
      # real parser is the only check that catches both.
      previous = Application.get_env(:flezha, :auto_restore_enabled)
      Application.put_env(:flezha, :auto_restore_enabled, true)

      try do
        config = HubManager.auto_recovery_config()

        # The shipped defaults, and `nil` because no durability tier is
        # configured here.
        assert config == [
                 reconcile_grace_ms: 45_000,
                 reconcile_interval_ms: 30_000,
                 cluster_settle_ms: 2_000,
                 remote_manifest: nil
               ]

        assert {:ok, parsed} = HubRecovery.parse_config(config)
        assert parsed.enabled?
        assert Map.take(parsed, Keyword.keys(config)) == Map.new(config)
      after
        Application.put_env(:flezha, :auto_restore_enabled, previous)
      end
    end

    test "with the durability tier enabled the declared set ships through it" do
      previous_restore = Application.get_env(:flezha, :auto_restore_enabled)
      previous_tier = Application.get_env(:flezha, :durability_tier)
      Application.put_env(:flezha, :auto_restore_enabled, true)

      Application.put_env(:flezha, :durability_tier,
        backend: :local_path,
        backend_opts: [path: System.tmp_dir!()]
      )

      try do
        config = HubManager.auto_recovery_config()

        assert {:ok, parsed} = HubRecovery.parse_config(config)

        assert parsed.remote_manifest ==
                 {Flezha.State.DurabilityTier.DeclaredManifest, []}

        # The adapter must also pass the hub's start-time validation, or the
        # tier being on would fail every namespace hub start.
        assert :ok = RemoteManifest.validate(parsed.remote_manifest)
      after
        Application.put_env(:flezha, :auto_restore_enabled, previous_restore)

        if previous_tier,
          do: Application.put_env(:flezha, :durability_tier, previous_tier),
          else: Application.delete_env(:flezha, :durability_tier)
      end
    end

    test "returns false when auto_restore is disabled" do
      previous = Application.get_env(:flezha, :auto_restore_enabled)
      Application.put_env(:flezha, :auto_restore_enabled, false)

      try do
        assert false == HubManager.auto_recovery_config()
      after
        Application.put_env(:flezha, :auto_restore_enabled, previous)
      end
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
