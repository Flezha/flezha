defmodule FlezhaTest.Helper.MessageTestHelper do
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
