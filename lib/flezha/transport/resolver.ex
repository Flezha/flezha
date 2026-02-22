defmodule Flezha.Transport.Resolver do
  @moduledoc """
  Resolves the transport module based on the configured transport type.
  """

  def transport_module do
    case Application.get_env(:flezha, :transport, :uds) do
      :quic -> Flezha.Transport.Quic
      _ -> Flezha.Transport.Uds
    end
  end

  def transport_type do
    Application.get_env(:flezha, :transport, :uds)
  end
end
