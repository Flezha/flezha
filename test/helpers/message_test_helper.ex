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
end
