defmodule Flezha.Service.CallerContextTest do
  use ExUnit.Case, async: true

  alias Flezha.Service.CallerContext

  describe "receive_for/3 on a window that has already closed" do
    test "hands on a message queued before the window closed" do
      send(self(), {:signal, :queued})

      assert CallerContext.receive_for(0, fn {:signal, :queued} -> :done end) == :done
    end

    test "closes after one message when the handler keeps answering :continue" do
      send(self(), {:signal, 1})
      send(self(), {:signal, 2})

      handler = fn {:signal, n} ->
        send(self(), {:seen, n})
        :continue
      end

      assert CallerContext.receive_for(0, handler) == :window_closed

      assert_received {:seen, 1}
      refute_received {:seen, 2}
      assert_received {:signal, 2}
    end

    test "answers :window_closed with nothing queued" do
      assert CallerContext.receive_for(0, fn _ -> :done end) == :window_closed
    end
  end
end
