defmodule Flezha.Executor.StreamChannelLivenessTest do
  @moduledoc """
  The StreamChannel bumps liveness counters on the data path. An unbound
  channel (stream_ref nil) must count both the io_in arrival and the drop, so a
  stall classifier can tell "fed but unbound" from "fed but not sending".
  """

  use ExUnit.Case, async: false

  alias Flezha.Executor.StreamChannel
  alias Flezha.Transport.LivenessCounters

  test "unbound channel counts io_in_received and nil_drops" do
    exec = "exec_sc_#{System.unique_integer([:positive])}"
    name = :"sc_#{System.unique_integer([:positive])}"

    {:ok, _pid} = StreamChannel.start_link({exec, name, :primary})

    GenServer.cast(name, {:io_in, "relay1", {"msg", %{}}})
    GenServer.cast(name, {:io_in, "relay1", {"msg", %{}}})
    # Force the casts to drain before reading the counters.
    _ = :sys.get_state(name)

    row =
      LivenessCounters.snapshot_all().channels
      |> Enum.find(fn c -> c.executor_id == exec and c.name == name end)

    assert row.io_in_received == 2
    assert row.nil_drops == 2
    assert row.send_frame_calls == 0
  end
end
