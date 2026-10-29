defmodule FlezhaTest.Service.LivenessFilterTest do
  @moduledoc """
  `Controller.liveness_counters/2` with a relay filter: only the named relays'
  counters and their runners' channel counters, read by key.
  """
  # async: false: Mimic global mode; the node-wide liveness table.
  use ExUnit.Case, async: false

  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Service.Controller
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Transport.LivenessCounters

  @caller %{source: :cli}

  setup :set_mimic_global_until_exit

  setup do
    LivenessCounters.ensure_table()
    test = self()

    owners =
      for n <- 1..30 do
        spawn_link(fn ->
          LivenessCounters.register_relay(relay(n))
          if n <= 2, do: LivenessCounters.register_channel(runner(n), :control)
          send(test, :registered)
          Process.sleep(:infinity)
        end)
      end

    for _ <- owners, do: assert_receive(:registered)
    Mimic.stub(RelayManager, :on_hub_member, fn _ns, _key, {m, f, a} -> apply(m, f, a) end)

    Mimic.stub(HubManager, :relay_rows_of, fn _ns, ids ->
      for id <- ids,
          do: %{
            id: id,
            runner: runner(rem(String.to_integer(String.trim_leading(id, "lv-")), 2) + 1)
          }
    end)

    on_exit(fn -> Enum.each(owners, &Process.exit(&1, :kill)) end)
  end

  defp relay(n), do: "lv-#{n}"
  defp runner(n), do: "lv-runner-#{n}"

  test "a filter naming 3 relays answers those 3 and their runners' channels" do
    named = [{"default", relay(1)}, {"default", relay(3)}, ["default", relay(5)]]
    {:ok, answer} = Controller.liveness_counters([filter: [relays: named]], @caller)

    assert answer.relays |> Enum.map(& &1.relay_id) |> Enum.sort() == ["lv-1", "lv-3", "lv-5"]
    assert answer.channels |> Enum.map(& &1.runner_id) |> Enum.uniq() == [runner(2)]
  end

  test "the unfiltered answer still holds every relay" do
    {:ok, answer} = Controller.liveness_counters(@caller)
    ids = MapSet.new(answer.relays, & &1.relay_id)
    assert Enum.all?(1..30, &MapSet.member?(ids, relay(&1)))
  end

  test "501 relays, or a paging key, is refused" do
    many = for n <- 1..501, do: {"default", "x#{n}"}

    assert {:error, %Flezha.Error{code: "usage.invalid_number"}} =
             Controller.liveness_counters([filter: [relays: many]], @caller)

    assert {:error, %Flezha.Error{code: "control.unknown_parameter"}} =
             Controller.liveness_counters([limit: 5], @caller)
  end
end
