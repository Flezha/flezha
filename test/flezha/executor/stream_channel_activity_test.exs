defmodule FlezhaTest.Executor.StreamChannelActivityTest do
  @moduledoc """
  Egress-activity hot-path write (self-healing-control-plane, task 6.1).

  `Native` is a NIF and cannot be mocked, but the activity record runs *before*
  the NIF hand-off, so a fake `stream_ref` lets us assert the table write while
  tolerating the NIF rejecting the bogus ref.
  """
  use ExUnit.Case, async: false

  alias Flezha.Executor.StreamChannel
  alias Flezha.Reconciler.Activity

  defp state(activity_enabled) do
    %{
      executor_id: "ce",
      stream_ref: make_ref(),
      relay_mapping: %{},
      name: :test_channel,
      activity_enabled: activity_enabled
    }
  end

  defp send_frame(handler, relay_id, state) do
    # The NIF will reject the fake ref; the activity record already ran by then.
    try do
      case handler do
        :cast -> StreamChannel.handle_cast({:io_in, relay_id, {"msg", %{}}}, state)
        :call -> StreamChannel.handle_call({:io_in, relay_id, {"msg", %{}}}, self(), state)
      end
    catch
      _, _ -> :ok
    end
  end

  test "init/1 derives activity_enabled from :reconciler_enabled (false in test)" do
    {:ok, state} = StreamChannel.init({"ce", :primary, :chan})
    assert state.activity_enabled == false
  end

  describe "with activity enabled (table started)" do
    setup do
      start_supervised!(Activity)
      :ok
    end

    test "cast send path records egress activity" do
      send_frame(:cast, "relay_cast", state(true))
      assert is_integer(Activity.last_output_ms("relay_cast"))
    end

    test "call send path records egress activity" do
      send_frame(:call, "relay_call", state(true))
      assert is_integer(Activity.last_output_ms("relay_call"))
    end

    test "disabled channel records nothing" do
      send_frame(:cast, "relay_off", state(false))
      assert Activity.last_output_ms("relay_off") == nil
    end
  end
end
