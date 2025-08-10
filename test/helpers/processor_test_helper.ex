defmodule FlezhaTest.Helper.ProcessorTestHelper do
  alias Flezha.Service.ProcessorManager

  def send_sync_io(processor_id, namespace, msg, metadata \\ %{}) do
    ProcessorManager.send_io(processor_id, namespace, msg, metadata)

    receive do
      {:message_forward, msg} -> msg
    after
      2000 -> raise "Timeout waiting for message"
    end
  end
end
