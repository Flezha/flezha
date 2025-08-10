defmodule FlezhaTest.Helper.ComputationTestHelper do
  alias Flezha.Service.ComputationManager

  def send_sync_io(computation_id, namespace, msg, metadata \\ %{}) do
    ComputationManager.send_io(computation_id, namespace, msg, metadata)

    receive do
      {:message_forward, msg} -> msg
    after
      2000 -> raise "Timeout waiting for message"
    end
  end
end
