defmodule Flezha.Transport.Uds do
  @moduledoc """
  Unix Domain Socket transport implementation.
  """

  @behaviour Flezha.Transport.Behaviour

  @impl true
  def send_data(socket, data) do
    :gen_tcp.send(socket, data)
  end

  @impl true
  def set_active(socket) do
    :inet.setopts(socket, [{:active, true}])
  end

  @impl true
  def close(socket) do
    :gen_tcp.close(socket)
  end

  @impl true
  def controlling_process(socket, pid) do
    :gen_tcp.controlling_process(socket, pid)
  end
end
