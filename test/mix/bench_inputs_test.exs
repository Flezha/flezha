defmodule Mix.Tasks.Bench.InputsTest do
  @moduledoc """
  What `mix bench` sends: the size an input is padded to, the stamp it carries
  back, the range a run reports, and the combinations refused before boot.
  """

  use ExUnit.Case, async: true

  alias Mix.Tasks.Bench.Inputs
  alias Mix.Tasks.Bench.Tally

  defp parse(opts, shape \\ :regression, via \\ :cast),
    do: Inputs.parse(opts, shape, via, {100, 1000}, 10)

  describe "build/5" do
    test "without options an input is the historical body" do
      inputs = parse([])
      assert Inputs.build(inputs, :regression, "flow_test_3", 7, 0) == "test_msg_flow_test_3_7"
      assert Inputs.build(inputs, :dynamic, "flow_test_3", 9, 0) == "k1:test_msg_flow_test_3_9"

      assert Inputs.build(inputs, :send, "flow_test_3", 7, 0) ==
               "flow_test_3_sink:test_msg_flow_test_3_7"

      assert Inputs.build(inputs, :sleeping, "flow_test_3", 9, 0) == "k1:1"
    end

    test "a stamped input carries its seq back through every wire form" do
      inputs = parse(latency: true)

      for shape <- [:regression, :dynamic, :tier, :timer] do
        assert {42, _sent_ns} =
                 inputs |> Inputs.build(shape, "flow_test_1", 1, 42) |> Tally.parse_stamp()
      end
    end

    test "padding is exact, stamped or not, and the stamp survives it" do
      for latency <- [false, true], shape <- [:regression, :dynamic, :send] do
        inputs = parse(latency: latency, payload_bytes: 256)
        input = Inputs.build(inputs, shape, "flow_test_100", 1000, 100_999)
        assert byte_size(input) == 256
        if latency and shape != :send, do: assert({100_999, _} = Tally.parse_stamp(input))
      end
    end
  end

  describe "parse/5" do
    test "the payload range comes from the builder" do
      # `test_msg_flow_test_1_1` to `test_msg_flow_test_100_1000`
      assert parse([]).payload == {22, 27}
      assert parse(payload_bytes: 64).payload == {64, 64}
    end

    test "every combination that cannot run is refused by name" do
      for {opts, shape, via, message} <- [
            {[rate: 0], :regression, :cast, "--rate must be a positive"},
            {[rate: 10], :regression, :host_inject, "--rate needs --via cast"},
            {[rate: 10], :cell, :cast, "--shape cell"},
            {[payload_bytes: 64], :sleeping, :cast, "--shape sleeping"},
            {[payload_bytes: 0], :regression, :cast, "--payload-bytes must be a positive"},
            {[payload_bytes: 8, latency: true], :regression, :cast, "use at least"}
          ] do
        error = assert_raise Mix.Error, fn -> parse(opts, shape, via) end
        assert error.message =~ message
      end
    end

    test "the header names the payload, the send mode and latency" do
      inputs = parse(rate: 2000, latency: true, payload_bytes: 64)

      assert Inputs.header_rows(inputs) == [
               {"payload", "64–64 B"},
               {"send", "paced, 2,000/s"},
               {"latency", "on"}
             ]
    end
  end

  describe "check_pace/3" do
    import ExUnit.CaptureIO

    defp late_warnings(rate, end_to_end_us) do
      Process.delete(:bench_report)
      inputs = %{parse([]) | rate: rate}
      capture_io(fn -> assert Inputs.check_pace(inputs, 10_000, end_to_end_us) == :ok end)
      (Process.get(:bench_report) || %{}) |> get_in([:header, :warnings]) |> List.wrap()
    end

    test "a burst, and a run on its schedule, are not warned about" do
      assert late_warnings(nil, 10_000_000) == []
      assert late_warnings(10_000, 1_040_000) == []
      # A short schedule still gets the 5 ms slack.
      assert late_warnings(1_000_000, 14_000) == []
    end

    test "a run past its schedule is warned about with its end-to-end rate" do
      assert [text] = late_warnings(10_000, 2_000_000)
      assert text =~ "reached 5,000/s end to end of the 10,000/s asked"
    end
  end
end
