defmodule FlezhaTest.Helper.MessageTestHelper do
  @doc """
  Forward every occurrence of `event` to the calling process as
  `{tag, measurements, metadata}` for the duration of the test, so a telemetry
  signal can be awaited with `assert_receive` like any other message. The
  handler is detached on exit.

      forward_telemetry([:flezha, :transport, :connect_rejected], :rejected)
      assert_receive {:rejected, %{count: 1}, %{stage: :handshake}}, 500
  """
  def forward_telemetry(event, tag) do
    test_pid = self()
    handler_id = {__MODULE__, tag, System.unique_integer([:positive])}

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        fn _event, measurements, metadata, _ ->
          send(test_pid, {tag, measurements, metadata})
        end,
        nil
      )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  def recv_num(msg, count) do
    for _i <- 1..count do
      receive do
        ^msg -> :ok
      after
        2000 -> raise "Timeout waiting for message"
      end
    end
  end

  def recv_drain(msg, timeout \\ 2000) do
    receive do
      ^msg -> 1 + recv_drain(msg, timeout)
    after
      timeout -> 0
    end
  end

  def flush(msg) do
    receive do
      ^msg -> flush(msg)
    after
      0 -> :ok
    end
  end

  def recv_until(msg, condition, timeout \\ 5000) do
    if condition.() do
      :ok
    else
      receive do
        ^msg -> recv_until(msg, condition, timeout)
      after
        timeout -> raise "Condition not met within #{timeout}ms"
      end
    end
  end
end
