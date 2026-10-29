defmodule Flezha.Logs.RelayHistoryTest do
  @moduledoc """
  How a run's end is told: the outcome an `ended` record carries for each
  combination of the facts its stop carried, and the record's own shape.
  """

  use FlezhaTest.Case, async: true

  alias Flezha.Logs.{Codec, Kinds, RelayHistory}
  alias Flezha.Relay.{Builder, Overwatch}

  defp ending(fields) do
    struct!(
      %Overwatch{id: "r", namespace: "ns", builder: %Builder{factory_id: "f", run: :steps}},
      fields
    )
  end

  defp terminal(status, undo \\ "", error \\ "boom"),
    do: %{status: status, error: error, undo: undo}

  defp no_window(), do: nil

  test "a run's own account of its end decides the outcome" do
    assert RelayHistory.outcome(ending(terminal: terminal("ok", "", "")), &no_window/0) ==
             {"completed", ""}

    assert {"completed", _} =
             RelayHistory.outcome(ending(terminal: terminal("completed")), &no_window/0)

    assert RelayHistory.outcome(ending(terminal: terminal("error", "complete")), &no_window/0) ==
             {"undone", "boom"}

    assert RelayHistory.outcome(ending(terminal: terminal("error")), &no_window/0) ==
             {"failed", "boom"}

    assert RelayHistory.outcome(ending(terminal: terminal("error", "partial")), &no_window/0) ==
             {"failed", "boom; rollback incomplete"}
  end

  test "a cancelled run ended by a stop, by its time limit, or by itself" do
    cancelled = ending(terminal: terminal("cancelled", "", ""))
    passed = fn -> System.os_time(:millisecond) - 1 end
    asked = %{cancelled | stop_requested_by: %{source: :cli}}

    assert {"stopped", _} = RelayHistory.outcome(asked, passed)
    assert {"timed_out", _} = RelayHistory.outcome(cancelled, passed)
    assert {"cancelled", _} = RelayHistory.outcome(cancelled, &no_window/0)
  end

  test "a stop with no account, a halted one, and a rescale's departure" do
    assert RelayHistory.outcome(ending(cause: :stop), &no_window/0) == {"stopped", nil}
    assert RelayHistory.outcome(ending(halt_reason: :drill), &no_window/0) == {"stopped", :drill}

    assert RelayHistory.outcome(ending(cause: :rescale_leave), &no_window/0) ==
             {"rescaled_away", nil}
  end

  test "a record carries its fields in order, with its controller last" do
    kind = Kinds.fetch!(:relay_history)

    line =
      Codec.encode(kind, %{
        "ts_us" => 1_000_000,
        "event" => "ended",
        "namespace" => "ns",
        "relay" => "r",
        "factory" => "f",
        "run" => "steps",
        "origin" => %{kind: "operator"},
        "first_started_at" => 1,
        "outcome" => "completed",
        "halted" => false,
        "duration_ms" => 5,
        "error" => ""
      })

    assert line =~ ~r/^\{"ts_us":1000000,"ts_iso":"1970-01-01T00:00:01.000Z","event":"ended"/
    assert String.ends_with?(line, ~s("error":"","node":"#{node()}"}\n))
  end

  test "an origin names who asked and leaves out what nobody gave" do
    assert RelayHistory.origin("operator", %{source: :ui, principal: "alice", request_id: nil}) ==
             %{kind: "operator", source: "ui", principal: "alice"}

    assert RelayHistory.origin("spawn", %{}, %{parent_relay: nil, spawn_identity: ""}) ==
             %{kind: "spawn"}
  end
end
