defmodule FlezhaTest.Service.RelayInfoWedgedTest do
  @moduledoc """
  A relay its hub still holds but whose Overwatch does not answer is described
  by the verdict the convergence loop published for it: `Relay.diagnose` and
  `Relay.info` answer from that record, marked `live_read` with the read's
  error code and without the fields only the live state knows.
  """

  # async: false: Mimic global mode; publishes the shared snapshot.

  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  import ExUnit.CaptureIO

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Snapshot
  alias Flezha.Convergence.Verdict
  alias Flezha.Plugin
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Cli
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager

  @namespace "relay-info-wedged-test"
  @subject {:relay, {@namespace, "op-1"}}

  setup :set_mimic_global_until_exit

  setup do
    published = Snapshot.current()
    on_exit(fn -> Snapshot.publish(published) end)
    publish(state: :degraded, reason: {:wedged, :relay}, phase: :running)

    reads = :counters.new(1, [])
    stub(RelayManager, :get_relay_pid, fn _ns, "op-1" -> {:ok, self()} end)
    stub(RelayManager, :get_relay_ids, fn @namespace -> {:ok, ["op-1"]} end)
    %{reads: reads}
  end

  defp publish(nil), do: Snapshot.current() |> Map.put(:relay_health, %{}) |> Snapshot.publish()

  defp publish(opts) do
    {state, opts} = Keyword.pop!(opts, :state)
    record = Verdict.new(@subject, state, [now: System.monotonic_time(:millisecond)] ++ opts)
    Snapshot.current() |> Map.put(:relay_health, %{@subject => record}) |> Snapshot.publish()
  end

  defp overwatch_answers(reads, reply) do
    stub(RelayManager, :get_overwatch_state, fn _ns, "op-1" ->
      :counters.add(reads, 1, 1)
      reply
    end)
  end

  defp out(fun, args, opts \\ [output: "json"]),
    do: capture_io(fn -> Cli.call(fun, Jason.encode!(args), opts) end)

  # Every message and fact reads whole: none left as a placeholder or `null`.
  defp readable_report!(answer) do
    for finding <- answer["findings"] do
      readable!(finding["message"])
      Enum.each(finding["details"], fn {_key, value} -> readable!(value) end)
    end

    answer
  end

  defp readable!(text) when is_binary(text), do: refute(text =~ ~r/\{[a-z0-9_]+\}|\bnull\b/)
  defp readable!(value), do: refute(is_nil(value))

  defp diagnose(),
    do: readable_report!(Jason.decode!(out("Relay.diagnose", [@namespace, "op-1"])))

  defp registry_records(runner, bindings) do
    stub(HubManager, :relay_spec, fn @namespace, "op-1" ->
      {:ok, %{factory_id: "echo", opts: []}, %{runner: runner, node: node()}}
    end)

    stub(HubManager, :relay_bindings, fn @namespace, "op-1" -> {:ok, bindings} end)
  end

  defp refusal(fun, args),
    do: capture_io(:stderr, fn -> assert {:shutdown, _code} = catch_exit(out(fun, args)) end)

  describe "Relay.diagnose" do
    test "a relay whose Overwatch read times out answers its published wedge", %{reads: reads} do
      overwatch_answers(reads, {:error, :timeout})

      answer = diagnose()
      assert answer["verdict"] == "problem"
      assert answer["live_read"] == "controller.timeout"

      assert [%{"code" => "diagnose.data_plane_wedge", "details" => details}, unread] =
               answer["findings"]

      assert details["layer"] == "relay"
      assert unread["code"] == "diagnose.unread"
      assert unread["details"]["live_read"] == "controller.timeout"
      assert :counters.get(reads, 1) == 1
    end

    test "a registered relay whose process is gone answers its published verdict",
         %{reads: reads} do
      overwatch_answers(reads, {:error, :noproc})

      answer = diagnose()
      assert answer["verdict"] == "problem"
      assert answer["live_read"] == Flezha.Error.from_reason(:noproc, :"relay.list").code
    end

    test "a runner nobody recorded is absent from the details and unknown in the message",
         %{reads: reads} do
      publish(state: :failed, reason: :dead_runner)
      past_grace = Durations.get(:convergence_unmet_intent_grace_ms) + 1
      Snapshot.current() |> Map.put(:unmet_intent_ms, past_grace) |> Snapshot.publish()
      overwatch_answers(reads, {:error, :timeout})

      answer = diagnose()

      assert [
               %{"code" => "diagnose.runner_disconnected", "details" => details} = finding,
               _unread
             ] =
               answer["findings"]

      message = finding["message"]

      assert Map.keys(details) == ["namespace", "relay", "seconds"]
      assert message =~ "bound to runner 'unknown'"
    end

    test "a healthy verdict without the live state is a warning, never ok", %{reads: reads} do
      publish(state: :healthy, reason: :producing)
      overwatch_answers(reads, {:error, :timeout})

      answer = diagnose()
      assert answer["verdict"] == "warning"
      assert [%{"code" => "diagnose.unread", "details" => details}] = answer["findings"]
      assert details["live_read"] == "controller.timeout"
    end

    test "an unreadable relay's runner is the one its hub recorded", %{reads: reads} do
      publish(state: :failed, reason: :dead_runner)
      past_grace = Durations.get(:convergence_unmet_intent_grace_ms) + 1
      Snapshot.current() |> Map.put(:unmet_intent_ms, past_grace) |> Snapshot.publish()
      stub(FleetManager, :placement_refusal, fn @namespace, "echo" -> :none end)
      registry_records("exec1", [])
      overwatch_answers(reads, {:error, :timeout})

      assert [%{"code" => "diagnose.runner_disconnected", "details" => details}, _unread] =
               diagnose()["findings"]

      assert details["runner"] == "exec1"
    end

    test "an unreadable relay's plugin provider is the one its hub recorded", %{reads: reads} do
      publish(state: :degraded, reason: {:plugin_unhealthy, "feed", "broker down"})

      registry_records("exec1", [
        {"feed", %Plugin{id: "feed", provider: Flezha.Plugin.Provider.File.Source}}
      ])

      overwatch_answers(reads, {:error, :timeout})

      assert [%{"code" => "diagnose.plugin_unhealthy", "details" => details}, _unread] =
               diagnose()["findings"]

      assert details["provider"] == "Flezha.Plugin.Provider.File.Source"
    end

    test "a live relay's plugin verdict for a plugin it has not bound names no provider",
         %{reads: reads} do
      publish(state: :degraded, reason: {:plugin_unhealthy, "feed", "broker down"})
      state = %{runner_id: "exec1", builder: %{opts: []}, extensions: %{plugins: %{}}}
      overwatch_answers(reads, {:ok, state})

      assert [%{"code" => "diagnose.plugin_unhealthy", "details" => details} = finding] =
               diagnose()["findings"]

      refute Map.has_key?(details, "provider")
      assert finding["message"] =~ "plugin 'feed' (unknown)"
    end

    test "an unreadable relay with no published verdict answers the read's error",
         %{reads: reads} do
      publish(nil)
      overwatch_answers(reads, {:error, :timeout})

      assert refusal("Relay.diagnose", [@namespace, "op-1"]) =~ ~s("code":"controller.timeout")
    end

    test "a relay its hub does not hold answers relay.not_found, verdict or not",
         %{reads: reads} do
      overwatch_answers(reads, {:error, {:relay_not_found, @namespace, "op-1"}})

      assert refusal("Relay.diagnose", [@namespace, "op-1"]) =~ ~s("code":"relay.not_found")
    end
  end

  describe "Relay.info" do
    test "a row whose read times out carries the published phase and health", %{reads: reads} do
      overwatch_answers(reads, {:error, :timeout})

      [row] = Jason.decode!(out("Relay.info", [@namespace]))["relays"]
      assert :counters.get(reads, 1) == 1
      Enum.each(row, fn {_key, value} -> readable!(value) end)

      assert row == %{
               "id" => "op-1",
               "phase" => "running",
               "health" => "degraded",
               "live_read" => "controller.timeout"
             }
    end

    test "the text form says the live state was not read", %{reads: reads} do
      overwatch_answers(reads, {:error, :timeout})

      stdout = out("Relay.info", [@namespace], [])
      assert stdout =~ "Relay: op-1\n  Phase: running\n  Health: degraded\n"
      assert stdout =~ "  Live state: not read (controller.timeout)\n"
      readable!(stdout)
    end

    test "a published verdict with no phase reads as phase unknown", %{reads: reads} do
      publish(state: :degraded, reason: {:wedged, :relay})
      overwatch_answers(reads, {:error, :timeout})

      [row] = Jason.decode!(out("Relay.info", [@namespace]))["relays"]
      assert row["phase"] == "unknown"
      assert out("Relay.info", [@namespace], []) =~ "  Phase: unknown\n"
    end

    test "a row with no published verdict carries the structured error", %{reads: reads} do
      publish(nil)
      overwatch_answers(reads, {:error, :timeout})

      [row] = Jason.decode!(out("Relay.info", [@namespace]))["relays"]
      assert %{"id" => "op-1", "error" => %{"code" => "controller.timeout"} = error} = row
      assert is_binary(error["message"])

      assert out("Relay.info", [@namespace], []) =~
               "  Error getting overwatch state: #{error["message"]}\n"
    end
  end
end
