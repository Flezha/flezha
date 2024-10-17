defmodule Flecha.IO.IOBaseBehaviour do
  @callback handle_internal_payload(server :: pid, payload :: struct()) ::
              :ok | {:error, binary()}
end
