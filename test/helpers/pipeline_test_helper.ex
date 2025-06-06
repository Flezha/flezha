defmodule FlezhaTest.Helper.PipelineTestHelper do
  alias Flezha.Service.PipelineManager

  def send_sync_io(pipeline_id, group_id, msg) do
    PipelineManager.send_io(pipeline_id, group_id, msg)

    receive do
      {:message_forward, msg} -> msg
    after
      2000 -> raise "Timeout waiting for message"
    end
  end
end
