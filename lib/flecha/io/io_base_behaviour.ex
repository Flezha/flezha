defmodule Flecha.IO.BaseBehaviour do
  @callback handle_internal_payload(server :: pid, payload :: struct()) ::
              :ok | {:error, binary()}
end
