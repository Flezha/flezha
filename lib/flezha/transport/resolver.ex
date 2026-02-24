defmodule Flezha.Transport.Resolver do
  @moduledoc """
  Resolves the transport module.

  With the NIF-based transport, all transports use the same module.
  The transport type selection (UDS vs QUIC) is handled within the NIF.
  """

  def transport_module do
    Flezha.Transport.Native
  end

  def transport_type do
    Application.get_env(:flezha, :transport, :uds)
  end
end
