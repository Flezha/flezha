defmodule Flezha.Transport.Quic do
  @moduledoc """
  QUIC transport implementation using quicer (msquic wrapper).
  """

  @behaviour Flezha.Transport.Behaviour

  @impl true
  def send_data(stream, data) do
    :quicer.send(stream, data)
  end

  @impl true
  def set_active(stream) do
    :quicer.setopt(stream, :active, true)
  end

  @impl true
  def close(stream) do
    :quicer.close_stream(stream)
  end

  @impl true
  def controlling_process(stream, pid) do
    :quicer.controlling_process(stream, pid)
  end
end
