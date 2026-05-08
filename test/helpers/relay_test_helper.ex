defmodule FlezhaTest.Helper.RelayTestHelper do
  alias Flezha.Service.RelayManager

  def send_sync_io(relay_id, namespace, msg, metadata \\ %{}) do
    RelayManager.send_io(relay_id, namespace, msg, metadata)

    receive do
      {:message_forward, msg} -> msg
    after
      2000 -> raise "Timeout waiting for message"
    end
  end
end
