defmodule Flezha.Transport.Behaviour do
  @moduledoc """
  Behaviour for transport implementations.

  Defines the interface that transport modules (UDS, QUIC) must implement
  for sending/receiving data over connections.
  """

  @type conn :: any()

  @callback send_data(conn, binary()) :: :ok | {:error, term()}
  @callback set_active(conn) :: :ok | {:error, term()}
  @callback close(conn) :: :ok | {:error, term()}
  @callback controlling_process(conn, pid()) :: :ok | {:error, term()}
end
