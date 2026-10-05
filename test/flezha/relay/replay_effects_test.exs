defmodule FlezhaTest.Relay.ReplayEffectsTest do
  @moduledoc """
  A bind's effect results (`recorded-effects`): the kept results for inputs
  above the instance's floor are counted onto `AddRelay.effect_result_count`
  and shipped to the bound runner as `EffectResults` batches, awaited; the
  ones at or below the floor stay behind.
  """
  # async: false: a shared on-disk state root; Mimic global mode.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Relay.{Builder, Overwatch, Replay}
  alias Flezha.Runner.ControlUnit
  alias Flezha.State.{InputLog, TestHelpers}

  setup :set_mimic_global_until_exit

  setup do
    class = "replay_eff_#{System.unique_integer([:positive])}"
    on_exit(fn -> TestHelpers.close_input_log(class) end)
    %{class: class}
  end

  test "only the results above the floor are counted and shipped, awaited", %{class: class} do
    test_pid = self()

    ControlUnit
    |> stub(:send_control_await, fn "runner-1", command, _opts ->
      send(test_pid, {:shipped, command})
      {:ok, <<>>}
    end)

    for i <- 0..6, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})

    for offset <- [2, 5, 6],
        do: :kept = InputLog.keep_effect(class, offset, "id-#{offset}", "cell", "r#{offset}")

    InputLog.record_consumed(class, {class, "task", "a"}, 2)

    results = Replay.effect_results(class)
    assert length(results) == 2

    state = %Overwatch{
      id: class,
      runner_id: "runner-1",
      builder: %Builder{id: class, factory_id: "f", opts: []}
    }

    assert Replay.ship_effect_results(state, results) == :ok

    assert_receive {:shipped,
                    {:effect_results, %RunnerCommands.EffectResults{relay_id: ^class} = batch}}

    assert Enum.map(batch.results, &{&1.offset, &1.identity, &1.payload}) == [
             {5, "id-5", "r5"},
             {6, "id-6", "r6"}
           ]

    refute_receive {:shipped, _}
  end

  test "an instance that keeps none ships nothing", %{class: class} do
    ControlUnit |> reject(:send_control_await, 3)

    assert Replay.effect_results(class) == []

    assert Replay.ship_effect_results(
             %Overwatch{
               id: class,
               runner_id: "runner-1",
               builder: %Builder{id: class, factory_id: "f", opts: []}
             },
             []
           ) == :ok
  end
end
