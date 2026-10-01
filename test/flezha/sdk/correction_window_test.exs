defmodule Flezha.SDK.CorrectionWindowTest do
  @moduledoc """
  Late-input correction in the SDK's own core: the lateness classification
  against the Cell's event-time high-water, the correction request it sends
  instead of dispatching, the beyond-window policy, and the replayed slice it
  holds to its end marker and folds in event-time order. No controller and no
  real transport — frames are read off a native `LocalStream` the test owns.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Cell, Hold, Record, Spawn}
  alias Flezha.SDK.Cell.{State, Tier}
  alias Flezha.SDK.Operator.Inbox
  alias Flezha.SDK.Wire
  alias FlezhaTest.Helper.SdkTierFixtures.{CorrectionDropOp, CorrectionOp, CorrectionTask}

  import FlezhaTest.Helper.SdkOperatorHarness

  @window 600_000
  @task "correction_tasklet"

  # The correction probe, spawning once per input and folding what the handle
  # answered into the output it records.
  defmodule SpawningCorrectionTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "spawning_correction_tasklet"
    @impl true
    defdelegate init_state(key), to: CorrectionTask
    @impl true
    defdelegate event_time(input), to: CorrectionTask

    defchange fold(s, at) do
      %{s | order: [at | s.order]}
    end

    @impl true
    def run(input, s, ctx) do
      handle = spawn_steps(ctx, "worker", input, id: "w-" <> input)
      emit_keyed(ctx, "fold", inspect(Spawn.try_result(handle)))
      {:ok, fold(s, ctx, now(ctx))}
    end
  end

  defmodule SpawningCorrectionOp do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "spawning_correction_probe"
    def run, do: {:flow, exactly: {:effectively_once, correction_ms: 600_000}}

    def channels(c),
      do:
        c
        |> Channels.input("input")
        |> Channels.into(SpawningCorrectionTask)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
  end

  defp push(op, input, offset),
    do: Inbox.input(op, "input", input, %{"flz.offset" => Integer.to_string(offset)})

  # A member of a replayed slice, as `Replay.replay_slice/4` stamps it.
  defp replay(op, input, offset),
    do:
      Inbox.input(op, "input", input, %{
        "flz.offset" => Integer.to_string(offset),
        "flz.replay" => "1"
      })

  # The controller's payload-less end-of-slice broadcast: no channel, no body.
  defp end_slice(op), do: Inbox.input(op, nil, "", %{"flz.replay_end" => "1"})

  defp folds({"cell_effect_record", _, _} = frame) do
    {seq, body} = effect(frame)

    {seq,
     Enum.map(body.changes, fn c ->
       {:ok, {:fold, at}} = Record.decode_body(c)
       at
     end)}
  end

  defp target({"correction_request", frame, _}) do
    {:ok, %{seq: seq, body: body}} = Wire.decode_frame(frame)
    {:ok, target} = Wire.decode_correction_request(body)
    {seq, target}
  end

  # A state carrying only what `Tier.admit/3` reads.
  defp state(high_water, policy, secondary \\ nil) do
    s = %State{}

    %{
      s
      | timers: %{s.timers | high_water: high_water},
        tier: %{s.tier | correction: policy},
        stream: Hold.new(streams: List.wrap(secondary))
    }
  end

  describe "the classification, against the reference's own table" do
    test "no high-water yet is never late" do
      assert Tier.classify(1_000, nil, @window) == :on_time
    end

    test "at or ahead of the high-water is on time" do
      assert Tier.classify(5_000, 5_000, @window) == :on_time
      assert Tier.classify(5_001, 5_000, @window) == :on_time
    end

    test "behind the high-water and inside the window is correctable" do
      assert Tier.classify(4_999, 5_000, @window) == :correctable
      assert Tier.classify(1, 600_001, @window) == :correctable
    end

    test "behind by more than the window is beyond it" do
      assert Tier.classify(1, 600_002, @window) == :beyond_window
    end
  end

  describe "admission" do
    test "a class declaring no window always advances" do
      assert Tier.admit(state(5_000, nil), 1, nil) == :advance
    end

    test "an ordered slice member advances without being classified" do
      policy = %{window_ms: @window, beyond_window: :drop}
      assert Tier.admit(state(5_000, policy), 1, :ordered) == :advance
    end

    test "a correctable input with no state stream falls back to processing as an arrival" do
      policy = %{window_ms: @window, beyond_window: :process_as_arrival}
      assert Tier.admit(state(5_000, policy), 4_999, nil) == :advance
    end

    test "a beyond-window input dispatches without advancing, or is withheld" do
      arrival = %{window_ms: @window, beyond_window: :process_as_arrival}
      drop = %{window_ms: @window, beyond_window: :drop}
      assert Tier.admit(state(600_002, arrival), 1, nil) == :dispatch_only
      assert Tier.admit(state(600_002, drop), 1, nil) == :withhold
    end
  end

  test "the Cell state stays inside credo's struct-field cap" do
    assert %State{} |> Map.from_struct() |> map_size() == 30
  end

  test "a late input inside the window is withheld, and one correction request carries its event time" do
    %{op: op} = start_operator(CorrectionOp)

    push(op, "300000:a", 0)
    assert [{0, [300_000]}] == frames(1) |> Enum.map(&folds/1)

    push(op, "299000:late", 1)
    assert [{0, 299_000}] == frames(1) |> Enum.map(&target/1)

    # The withheld input shipped no record: the next input's record is seq 1.
    push(op, "400000:b", 2)
    assert [{1, [400_000]}] == frames(1) |> Enum.map(&folds/1)
  end

  test "a beyond-window arrival is processed and does NOT drag the high-water backwards" do
    %{op: op} = start_operator(CorrectionOp)

    push(op, "1000000:a", 0)
    assert [{0, [1_000_000]}] == frames(1) |> Enum.map(&folds/1)

    # 999 s late against a 600 s window: processed as an arrival, the default.
    push(op, "1000:straggler", 1)
    assert [{1, [1_000]}] == frames(1) |> Enum.map(&folds/1)

    # If the straggler had moved the high-water to 1 000, this would be ahead of
    # it and dispatch. It asks for a correction, so the high-water is still
    # 1 000 000 — which is the whole point of not advancing on a straggler.
    push(op, "999000:late", 2)
    assert [{0, 999_000}] == frames(1) |> Enum.map(&target/1)
  end

  test "a beyond-window input under the drop policy is neither processed nor recorded" do
    %{op: op} = start_operator(CorrectionDropOp)

    push(op, "1000000:a", 0)
    assert [{0, [1_000_000]}] == frames(1) |> Enum.map(&folds/1)

    push(op, "1000:straggler", 1)
    push(op, "1000001:b", 2)
    assert [{1, [1_000_001]}] == frames(1) |> Enum.map(&folds/1)
  end

  describe "the replayed slice" do
    test "is held to its end marker and folded in event-time order, not arrival order" do
      %{op: op} = start_operator(CorrectionOp)

      push(op, "300000:seed", 0)
      assert [{0, [300_000]}] == frames(1) |> Enum.map(&folds/1)

      # Arrival order 300, 100, 200 — every one of them behind the high-water,
      # so dispatching any on arrival would recompute the wrong answer.
      replay(op, "300:c", 3)
      replay(op, "100:a", 1)
      replay(op, "200:b", 2)
      end_slice(op)

      assert [{1, [100]}, {2, [200]}, {3, [300]}] == frames(3) |> Enum.map(&folds/1)
    end

    test "orders ties by ascending input-log offset" do
      %{op: op} = start_operator(CorrectionOp)

      push(op, "300000:seed", 0)
      assert [{0, [300_000]}] == frames(1) |> Enum.map(&folds/1)

      replay(op, "100:second", 9)
      replay(op, "100:first", 4)
      end_slice(op)

      assert [{"cell_effect_record", _, _} = f1, f2] = frames(2)
      assert {1, [100]} = folds(f1)
      assert {2, [100]} = folds(f2)
      assert [%{payload: "100:first"}] = elem(effect(f1), 1).outputs
      assert [%{payload: "100:second"}] = elem(effect(f2), 1).outputs
    end

    test "an input enqueued behind the end marker folds after every member" do
      %{op: op} = start_operator(CorrectionOp)

      push(op, "300000:seed", 0)
      assert [{0, [300_000]}] == frames(1) |> Enum.map(&folds/1)

      replay(op, "300:c", 3)
      replay(op, "100:a", 1)
      end_slice(op)
      push(op, "400000:live", 9)

      assert [{1, [100]}, {2, [300]}, {3, [400_000]}] == frames(3) |> Enum.map(&folds/1)
    end

    test "a replayed member does not lower the event-time high-water" do
      %{op: op} = start_operator(CorrectionOp)

      push(op, "300000:seed", 0)
      assert [{0, [300_000]}] == frames(1) |> Enum.map(&folds/1)

      replay(op, "100:a", 1)
      end_slice(op)
      assert [{1, [100]}] == frames(1) |> Enum.map(&folds/1)

      # Still 300 000: an input at 299 000 is inside the window behind it and
      # asks for a correction. Had the member lowered the high-water to 100,
      # 299 000 would be ahead of it and dispatch instead.
      push(op, "299000:late", 5)
      assert [{0, 299_000}] == frames(1) |> Enum.map(&target/1)
    end

    test "the broadcast reaches a Cell that buffered nothing without stalling it" do
      %{op: op} = start_operator(CorrectionOp)

      push(op, "300000:seed", 0)
      assert [{0, [300_000]}] == frames(1) |> Enum.map(&folds/1)

      end_slice(op)
      push(op, "400000:b", 1)
      assert [{1, [400_000]}] == frames(1) |> Enum.map(&folds/1)
    end

    test "a restore discards a buffered slice" do
      %{op: op} = start_operator(CorrectionOp)

      push(op, "300000:seed", 0)
      assert [{0, [300_000]}] == frames(1) |> Enum.map(&folds/1)

      replay(op, "100:a", 1)
      pid = cell!(op, @task)
      assert [{100, %{}}] = :sys.get_state(pid).tier.slice

      snap = Record.encode_body(%{order: [300_000]})
      assert {:ok, 0, false} = Cell.restore(pid, Record.header(snap, 1), snap, <<>>, 0)
      assert [] == :sys.get_state(pid).tier.slice

      end_slice(op)
      push(op, "400000:b", 9)
      assert [{1, [400_000]}] == frames(1) |> Enum.map(&folds/1)
    end

    test "a spawn issues nothing from a replayed member, and once from the same input live" do
      %{op: op} = start_operator(SpawningCorrectionOp)

      push(op, "100:a", 0)
      assert [live] = frames(1)
      assert outputs(live) == [inspect(:pending)]

      assert_received {:"$gen_cast",
                       {:spawn, _, %Spawn{instance_id: "w-100:a"}, "100:a", :deliver}}

      refute_received {:"$gen_cast", {:spawn, _, _, _, _}}

      replay(op, "100:a", 5)
      end_slice(op)
      assert [replayed] = frames(1)
      assert outputs(replayed) == [inspect({:ok, %{id: "w-100:a", outcome: :skipped_on_replay}})]
      refute_received {:"$gen_cast", {:spawn, _, _, _, _}}
    end
  end

  defp outputs(frame),
    do: frame |> effect() |> elem(1) |> Map.fetch!(:outputs) |> Enum.map(& &1.payload)

  test "a Cell of a correction class still reports its tasklet name" do
    %{op: op} = start_operator(CorrectionOp)
    push(op, "1:a", 0)
    assert [_] = frames(1)
    assert is_pid(cell!(op, @task))
  end
end
