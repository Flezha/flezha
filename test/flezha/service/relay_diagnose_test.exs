defmodule FlezhaTest.Service.RelayDiagnoseTest do
  @moduledoc """
  `Relay.diagnose` through the CLI door: one relay's verdict and findings,
  read from the facts the controller already keeps. Each fact is faked at its
  own seam: the Overwatch state at `RelayManager`, the verdict records in the
  published snapshot, the runners at `FleetManager`, the dead letters at
  `Dlq.Store` and the input log's tail at `InputLog`.
  """

  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  import ExUnit.CaptureIO

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Intent
  alias Flezha.Convergence.Snapshot
  alias Flezha.Convergence.Verdict
  alias Flezha.Dlq.Store
  alias Flezha.Relay.Calls
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Cli
  alias Flezha.AuthZ
  alias Flezha.AuthZ.{RoleStore, Scope}
  alias Flezha.Service.Relay
  alias Flezha.Service.Relay.Diagnose
  alias Flezha.Service.RelayManager
  alias Flezha.State.{Completion, InputLog, Paths}
  alias FlezhaTest.Helper.AuthTestHelper

  @namespace "relay-diagnose-test"
  @subject {:relay, {@namespace, "op-1"}}

  setup :set_mimic_global_until_exit

  setup do
    published = Snapshot.current()
    on_exit(fn -> Snapshot.publish(published) end)
    stub(RelayManager, :get_relay_pid, fn _ns, "op-1" -> {:ok, self()} end)
    :ok
  end

  defp running_state(plugins \\ %{}) do
    %{
      builder: %{run: :flow, factory_id: "orders", opts: []},
      settings: %{},
      runner_id: "exec1",
      namespace: @namespace,
      started_at: System.system_time(:millisecond),
      default_input_type: "2",
      extensions: %{plugins: plugins},
      calls: %Calls{}
    }
  end

  defp stub_one(state \\ running_state()) do
    stub(RelayManager, :get_overwatch_state, fn _ns, "op-1" -> {:ok, state} end)
  end

  # The records one convergence tick mints for a relay observed as `observed`.
  defp publish_judged(observed, roster \\ ["exec1"]) do
    intent = %Intent{
      relays: [%{namespace: @namespace, relay_id: "op-1", factory_id: "orders", plugins: []}]
    }

    reality = %{
      relays: %{{@namespace, "op-1"} => Map.merge(%{parked?: false, suspended?: false}, observed)},
      active_runners: %{@namespace => MapSet.new(roster)}
    }

    publish(FlezhaTest.Helper.ConvergenceFixtures.judge(intent, reality))
  end

  defp publish(records, unmet_ms \\ nil) do
    Snapshot.current()
    |> Map.put(:relay_health, records)
    |> Map.put(:unmet_intent_ms, unmet_ms)
    |> Snapshot.publish()
  end

  defp publish_reason(state, reason, extra \\ %{}) do
    record =
      Verdict.new(@subject, state, reason: reason, now: System.monotonic_time(:millisecond))

    publish(Map.put(extra, @subject, record))
  end

  defp grown_log(class) do
    threshold = Application.get_env(:flezha, :wal_high_watermark_bytes)
    Application.put_env(:flezha, :wal_high_watermark_bytes, 1)
    on_exit(fn -> Application.put_env(:flezha, :wal_high_watermark_bytes, threshold) end)

    path = Paths.input_log_path(class)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "grown")
  end

  # What the input log's last sweep remembered: the watermark and the floor.
  defp sweep_memo(class, watermark, floor) do
    InputLog.ensure_watermark_table()
    keys = [{class, :last_sweep_watermark}, {class, :last_sweep_floor}]
    :ets.insert(InputLog.watermark_table(), Enum.zip(keys, [watermark, floor]))
    on_exit(fn -> Enum.each(keys, &:ets.delete(InputLog.watermark_table(), &1)) end)
  end

  defp dead_letters_for(principal) do
    {:ok, answer} = Relay.diagnose(@namespace, "op-1", %{source: :cli, principal: principal})
    Enum.find(answer.findings, &(&1.code == "diagnose.dead_letters"))
  end

  defp diagnose(format \\ "json") do
    capture_io(fn ->
      Cli.call("Relay.diagnose", Jason.encode!([@namespace, "op-1"]), output: format)
    end)
  end

  defp answer(), do: Jason.decode!(diagnose())

  defp codes(%{"findings" => findings}), do: Enum.map(findings, & &1["code"])

  defp finding(answer, code), do: Enum.find(answer["findings"], &(&1["code"] == code))

  test "a healthy running relay answers ok with one healthy finding naming its runner" do
    stub_one()
    publish_judged(%{bound_runner: "exec1"})

    assert %{"relay" => "op-1", "verdict" => "ok", "findings" => [finding]} = answer()

    assert finding["code"] == "diagnose.healthy"
    assert finding["level"] == "note"
    assert finding["details"]["runner"] == "exec1"
    assert finding["details"]["node"] == Atom.to_string(node())
  end

  describe "the binding" do
    setup do
      stub_one()
      :ok
    end

    test "a relay whose runner is gone and whose factory nobody announces has no runner" do
      stub(FleetManager, :placement_refusal, fn @namespace, "orders" -> :no_such_factory end)
      publish_judged(%{bound_runner: "gone"})

      answer = answer()
      assert answer["verdict"] == "problem"
      assert codes(answer) == ["diagnose.no_runner"]

      assert finding(answer, "diagnose.no_runner")["hint"] =~
               "start a runner that defines 'orders'"
    end

    test "runners of another language are a mismatch, in the placement refusal's words" do
      stub(FleetManager, :placement_refusal, fn _ns, _factory -> {:language, 3} end)
      publish_judged(%{bound_runner: nil}, [])

      answer = answer()
      assert codes(answer) == ["diagnose.runner_mismatch"]

      assert finding(answer, "diagnose.runner_mismatch")["message"] =~
               "established in SDK language 3"
    end

    test "an orphaned relay a runner can take is rebinding inside the grace, lost past it" do
      stub(FleetManager, :placement_refusal, fn _ns, _factory -> :none end)
      record = Verdict.new(@subject, :failed, reason: :dead_runner, now: 0)

      publish(%{@subject => record}, 0)
      assert %{"verdict" => "ok", "findings" => [%{"code" => "diagnose.rebinding"}]} = answer()

      publish(%{@subject => record}, Durations.get(:convergence_unmet_intent_grace_ms) + 1)
      answer = answer()
      assert codes(answer) == ["diagnose.runner_disconnected"]
      assert finding(answer, "diagnose.runner_disconnected")["details"]["runner"] == "exec1"
    end

    test "an unbound relay past the grace is unbound when a runner can take it" do
      stub(FleetManager, :placement_refusal, fn _ns, _factory -> :none end)
      record = Verdict.new(@subject, :unbound, reason: :no_binding, now: 0)
      publish(%{@subject => record}, Durations.get(:convergence_unmet_intent_grace_ms) + 1)

      assert codes(answer()) == ["diagnose.unbound"]
    end
  end

  describe "the verdict's other reasons" do
    setup do
      stub_one()
      :ok
    end

    test "a suspended relay a runner can take is not a problem" do
      stub(FleetManager, :placement_refusal, fn @namespace, "orders" -> :none end)
      publish_judged(%{bound_runner: "exec1", suspended?: true})

      assert %{"verdict" => "ok", "findings" => [asleep]} = answer()
      assert asleep["code"] == "diagnose.asleep"
      assert asleep["message"] =~ "is suspended at a committed boundary"
      assert asleep["message"] =~ "the next message wakes it on a runner"
    end

    test "a suspended relay no runner serves is a problem, not asleep" do
      stub(FleetManager, :placement_refusal, fn @namespace, "orders" -> :no_such_factory end)
      publish_judged(%{bound_runner: "exec1", suspended?: true})

      answer = answer()
      assert answer["verdict"] == "problem"
      assert codes(answer) == ["diagnose.no_runner"]
      assert finding(answer, "diagnose.no_runner")["details"]["factory"] == "orders"
    end

    test "a suspended relay another node's runners serve is asleep, not a problem" do
      stub(FleetManager, :placement_refusal, fn @namespace, "orders" -> :no_such_factory end)
      stub(Flezha.Cluster.Directory, :factory_nodes, fn @namespace, "orders" -> [:other@host] end)
      publish_judged(%{bound_runner: "exec1", suspended?: true})

      assert %{"verdict" => "ok", "findings" => [%{"code" => "diagnose.asleep"}]} = answer()
    end

    test "a suspended relay the serving runners cannot take is a mismatch" do
      stub(FleetManager, :placement_refusal, fn _ns, _factory -> {:language, 3} end)
      publish_judged(%{bound_runner: "exec1", suspended?: true})

      assert codes(answer()) == ["diagnose.runner_mismatch"]
    end

    test "a parked relay is a warning naming why" do
      publish_reason(:parked, {:parked, {:no_runner, "exec1"}})

      answer = answer()
      assert answer["verdict"] == "warning"
      assert finding(answer, "diagnose.parked")["details"]["reason"] =~ "exec1"
    end

    test "a wedge, a stall and a stuck relay are each a problem of their own" do
      for {reason, code} <- [
            {{:wedged, :relay}, "diagnose.data_plane_wedge"},
            {{:stalled, :output_stall}, "diagnose.output_stall"},
            {{:stuck, :watermark_frozen}, "diagnose.stuck"}
          ] do
        publish_reason(:degraded, reason)
        answer = answer()
        assert answer["verdict"] == "problem"
        assert codes(answer) == [code]
      end
    end

    test "a relay on a runner the loop judged wedged is wedged too" do
      runner = {:runner, "exec1"}
      wedged = Verdict.new(runner, :degraded, reason: {:wedged, :runner_pool_wedged}, now: 0)

      publish_reason(:healthy, :producing, %{runner => wedged})

      answer = answer()
      assert codes(answer) == ["diagnose.data_plane_wedge"]

      assert finding(answer, "diagnose.data_plane_wedge")["details"]["layer"] ==
               "runner_pool_wedged"
    end

    test "a relay the loop has not judged yet is unknown" do
      publish(%{})
      assert %{"verdict" => "ok", "findings" => [%{"code" => "diagnose.unknown"}]} = answer()
    end
  end

  describe "the plugins" do
    test "an unhealthy plugin is named once, with its provider and its reason" do
      stub_one(running_state(%{"src" => %Flezha.Plugin{id: "src", provider: Flezha.Plugin}}))
      plugin = {:plugin, {@namespace, "op-1", "src"}}
      why = {:plugin_unhealthy, :broker_down}

      publish_reason(:degraded, {:plugin_unhealthy, "src", why}, %{
        plugin => Verdict.new(plugin, :degraded, reason: why)
      })

      answer = answer()
      assert codes(answer) == ["diagnose.plugin_unhealthy"]

      assert %{"plugin" => "src", "provider" => "Flezha.Plugin", "reason" => ":broker_down"} =
               finding(answer, "diagnose.plugin_unhealthy")["details"]
    end

    test "a provider this controller does not have is missing" do
      stub_one(running_state(%{"src" => %Flezha.Plugin{id: "src", provider: :"Elixir.Nope"}}))
      publish_judged(%{bound_runner: "exec1"})

      answer = answer()
      assert answer["verdict"] == "problem"
      assert finding(answer, "diagnose.plugin_missing")["details"]["provider"] == "Nope"
    end
  end

  describe "dead letters and inputs" do
    setup do
      stub_one()
      publish_judged(%{bound_runner: "exec1"})
      :ok
    end

    test "dead letters are summarised by count, most common errors and the newest age" do
      now = System.system_time(:millisecond)

      stub(Store, :list, fn "op-1" ->
        for ts <- [now - 9_000, now - 5_000, now - 2_000],
            do: %{"ts_ms" => ts, "error" => "call.timeout"}
      end)

      answer = answer()
      assert answer["verdict"] == "warning"
      refute finding(answer, "diagnose.healthy")

      dead = finding(answer, "diagnose.dead_letters")
      assert %{"count" => 3, "errors" => ["call.timeout"], "newest_s" => age} = dead["details"]
      assert age in 2..3
      assert dead["hint"] == "flz dlq list op-1"
    end

    test "inputs above the floor are a note beside the healthy line" do
      stub(InputLog, :tail, fn "op-1" -> {:ok, -1, 4} end)

      assert %{"verdict" => "ok", "findings" => [healthy, waiting]} = answer()
      assert healthy["code"] == "diagnose.healthy"
      assert waiting["code"] == "diagnose.inputs_waiting"
      assert waiting["details"]["count"] == 5
    end

    test "waiting inputs stick only under the sweep's own frozen condition" do
      stub(InputLog, :tail, fn "op-1" -> {:ok, -1, 4} end)
      grown_log("op-1")

      # The watermark stands where the last sweep left it, and nothing became
      # reclaimable since: the sweep would warn, and so does diagnose.
      sweep_memo("op-1", nil, nil)
      answer = answer()
      assert answer["verdict"] == "warning"

      assert %{"count" => 5, "size_bytes" => 5} =
               finding(answer, "diagnose.inputs_stuck")["details"]

      # The watermark has not moved since a sweep a moment ago, but the floor
      # did: the sweep would truncate, not warn.
      cell = {"op-1", "t", ""}
      :ok = InputLog.record_consumed("op-1", cell, 3)
      on_exit(fn -> :ets.delete(InputLog.watermark_table(), {"op-1", cell}) end)
      sweep_memo("op-1", 3, 1)

      assert codes(answer()) == ["diagnose.healthy", "diagnose.inputs_waiting"]
    end

    test "the input log and the dead letters are read only where the relay runs" do
      stub(RelayManager, :get_relay_pid, fn _ns, "op-1" -> {:error, :elsewhere} end)
      stub(InputLog, :tail, fn "op-1" -> {:ok, -1, 4} end)
      stub(Store, :list, fn "op-1" -> [%{"ts_ms" => 0, "error" => "e"}] end)
      grown_log("op-1")
      sweep_memo("op-1", nil, nil)

      assert codes(answer()) == ["diagnose.healthy"]
    end
  end

  describe "under enforcement" do
    setup do
      FlezhaTest.Case.install([{:auth_stores, enabled: true, claim: true, register_source: true}])
      :ok = RoleStore.bind("eve", @namespace, :viewer)
      :ok = RoleStore.bind("olga", @namespace, :ops)
      stub_one()
      publish_judged(%{bound_runner: "exec1"})
      stub(Store, :list, fn "op-1" -> [%{"ts_ms" => 0, "error" => "card 4111 declined"}] end)
      :ok
    end

    test "a viewer is told how many dead letters, never what they say" do
      dead = dead_letters_for("eve")
      assert dead.details["count"] == 1
      refute Map.has_key?(dead.details, "errors")
      refute dead.message =~ "4111"
    end

    test "a caller that may list dead letters is shown their errors" do
      assert dead_letters_for("olga").details["errors"] == ["card 4111 declined"]
    end

    test "a viewer's diagnose records no denial for dlq.list, which it never asked for" do
      boundary = System.monotonic_time(:microsecond)
      assert dead_letters_for("eve").details["count"] == 1

      denials =
        :authorization_denied
        |> AuthTestHelper.platform_authz_events(boundary)
        |> Enum.filter(&(&1.metadata.action == :"dlq.list"))

      assert denials == [],
             "diagnose wrote #{length(denials)} authorization_denied event(s) for dlq.list: " <>
               inspect(Enum.map(denials, & &1.metadata))

      # The ring keeps denials: the same check asked as a refusal records one.
      control = System.monotonic_time(:microsecond)
      ctx = %{source: :cli, principal: "eve"}

      assert {:error, {:forbidden, :role_insufficient}} =
               AuthZ.authorize(ctx, :"dlq.list", Scope.build(@namespace))

      assert [%{metadata: %{action: :"dlq.list", principal: "eve"}}] =
               AuthTestHelper.platform_authz_events(:authorization_denied, control)
    end
  end

  describe "a relay that is not live" do
    setup do
      stub(RelayManager, :get_overwatch_state, fn ns, id ->
        {:error, {:relay_not_found, ns, id}}
      end)

      :ok
    end

    test "a finished relay inside its retention window is a note saying how it ended" do
      Completion.write("op-1", %{
        namespace: @namespace,
        status: "error",
        error: "boom",
        result: "",
        finished_at: System.system_time(:second)
      })

      assert %{"verdict" => "ok", "findings" => [done]} = answer()
      assert done["code"] == "diagnose.finished"

      assert done["details"] == %{
               "error" => "boom",
               "status" => "error",
               "namespace" => @namespace,
               "relay" => "op-1"
             }
    end

    test "an id that never ran is relay.not_found" do
      err = capture_io(:stderr, fn -> assert catch_exit(diagnose()) == {:shutdown, 5} end)
      assert err =~ ~s("code":"relay.not_found")
    end
  end

  describe "the text rendering" do
    setup do
      stub_one()
      :ok
    end

    test "ok is one line naming the runner and node" do
      publish_judged(%{bound_runner: "exec1"})

      assert diagnose("text") ==
               "OK       relay 'op-1' is healthy, bound to runner 'exec1' on controller #{node()}\n"
    end

    test "a problem is a block with its code, message and hint" do
      stub(FleetManager, :placement_refusal, fn _ns, _factory -> :no_such_factory end)
      publish_judged(%{bound_runner: "gone"})

      assert diagnose("text") ==
               "PROBLEM  diagnose.no_runner  no connected runner announces factory 'orders', " <>
                 "so relay 'op-1' has nowhere to run\n" <>
                 "         hint: start a runner that defines 'orders', then check it with " <>
                 "flz runner list --namespace #{@namespace}\n"
    end

    test "a warning lists after the problems" do
      stub(FleetManager, :placement_refusal, fn _ns, _factory -> :no_such_factory end)
      publish_judged(%{bound_runner: "gone"})
      stub(Store, :list, fn "op-1" -> [%{"ts_ms" => 0, "error" => "e"}] end)

      [problem, _hint, warning, _dlq_hint] = String.split(diagnose("text"), "\n", trim: true)
      assert problem =~ ~r/^PROBLEM  diagnose.no_runner/
      assert warning =~ ~r/^WARNING  diagnose.dead_letters  relay 'op-1' has dead letters: 1,/
    end
  end

  describe "every verdict maps to a finding" do
    # The relay rules of the judge: its generic unobserved clause, then every
    # `decide(:relay, ...)` arm and the healthy-or-degraded fold.
    @judge "lib/flezha/convergence/probe/judge.ex"

    test "every reason tag the judge mints for a relay names a finding" do
      source = File.read!(@judge)
      [_, rules] = String.split(source, "defp decide(_kind, _facts, {id, why})", parts: 2)
      [relay_rules, _plugin] = String.split(rules, "defp decide(:plugin", parts: 2)
      [_, fold] = String.split(rules, "defp degraded_or_healthy(facts)", parts: 2)
      [fold, _] = String.split(fold, "defp live?", parts: 2)

      states = Enum.map_join(Verdict.reachable(:relay), "|", &Atom.to_string/1)

      tags =
        ~r/\{:(?:#{states}),\s*\{?:([a-z_]+)/
        |> Regex.scan(relay_rules <> fold, capture: :all_but_first)
        |> List.flatten()
        |> Enum.uniq()

      minted =
        ~r/\{:(#{states}),/
        |> Regex.scan(relay_rules <> fold, capture: :all_but_first)
        |> List.flatten()
        |> MapSet.new(&String.to_existing_atom/1)

      assert MapSet.equal?(minted, MapSet.new(Verdict.reachable(:relay))),
             "the scan no longer sees every relay state the judge mints: #{inspect(minted)}"

      # The plugin issue the judge rolls up into its relay rides a variable.
      unmapped =
        for tag <- ["plugin_unhealthy" | tags],
            is_nil(Diagnose.reason_code({String.to_existing_atom(tag), nil})),
            do: tag

      assert unmapped == [], "verdict reasons no finding maps: #{inspect(unmapped)}"
    end

    test "every finding code is catalogued with a message" do
      for code <- Diagnose.codes() do
        assert %{"message" => _} = Flezha.Error.catalogue()["codes"][code],
               "#{code} is not catalogued"
      end
    end
  end
end
