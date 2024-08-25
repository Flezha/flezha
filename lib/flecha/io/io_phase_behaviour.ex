defmodule Flecha.IO.IOBehaviour do
  @callback send_internal_payload(server :: pid, payload :: struct()) :: :ok | {:error, binary()}
end
