defmodule FlezhaTest.Helper.ComputationTestHelper do
  alias Flezha.Service.ComputationManager

  def send_sync_io(pipeline_id, group_id, msg) do
    ComputationManager.send_io(pipeline_id, group_id, msg, 1)

    receive do
      {:message_forward, msg} -> msg
    after
      2000 -> raise "Timeout waiting for message"
    end
  end
end
