defmodule FlezhaTest.Operative.OverwatchIoDropTest do
  @moduledoc """
  6.3: the Overwatch `io_in` *call* path counts an unbound-stream drop instead of
  silently discarding the channel's `{:error, :no_stream}` reply. Exercised by
  calling the handler directly (the data channel is a real nil-ref StreamChannel).
  """
  use ExUnit.Case, async: false

  alias Flezha.Executor.StreamChannel
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Operative.Overwatch

  test "io_in call on an unbound channel emits a drop telemetry event" do
    test_pid = self()
    id = "ow-io-drop-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      id,
      [:flezha, :stream, :drop],
      fn _e, m, md, _ -> send(test_pid, {:drop, m, md}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    chan = :"ow_drop_chan_#{System.unique_integer([:positive])}"
    {:ok, channel} = StreamChannel.start_link({"ce", chan, :primary})
    on_exit(fn -> if Process.alive?(channel), do: GenServer.stop(channel) end)
    assert GenServer.call(chan, :stream_ref_status) == nil

    # An empty catalog makes run_io fall through to the message-passthrough
    # branch, so a non-nil payload reaches the (unbound) channel.
    state = %Overwatch{
      id: "r_drop",
      namespace: "ns",
      default_input_type: "2",
      channel: chan,
      extensions: Table.new(%{})
    }

    assert {:reply, :ok, ^state} =
             Overwatch.handle_call({:io_in, "msg", %{}}, {self(), make_ref()}, state)

    assert_receive {:drop, %{count: 1}, %{operative_id: "r_drop"}}
  end
end
