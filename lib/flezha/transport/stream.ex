defmodule Flezha.Transport.Stream do
  @moduledoc """
  The per-stream transport operations, over a NIF stream resource of the UDS
  or QUIC server. The runner and transport call sites go through this seam, so
  the invariant it protects stays grep-simple: no per-stream `Native.` call
  outside `native.ex` and here.

  There is one send: `send_frame/4`, which frames in Rust. A second send path
  that must track the send gate forever is a trap for the next reader.
  """

  alias Flezha.Transport.Native

  @type handle :: reference()

  @spec send_frame(handle(), binary(), term(), map()) :: :ok | {:error, term()}
  def send_frame(ref, payload_id, message, metadata),
    do: Native.send_frame(ref, payload_id, message, metadata)

  @spec set_active(handle()) :: :ok | {:error, term()}
  def set_active(ref), do: Native.set_active(ref)

  @doc "Deliver the next frame, then buffer until the stream is activated again."
  @spec set_active_once(handle()) :: :ok | {:error, term()}
  def set_active_once(ref), do: Native.set_active_once(ref)

  @spec close(handle()) :: :ok | {:error, term()}
  def close(ref), do: Native.close(ref)

  @spec controlling_process(handle(), pid()) :: :ok | {:error, term()}
  def controlling_process(ref, pid), do: Native.controlling_process(ref, pid)

  @spec stream_id(handle()) :: non_neg_integer()
  def stream_id(ref), do: Native.stream_id(ref)

  @doc """
  The DER certificate the peer presented on this stream's connection, or `nil`
  when it presented none. Only a QUIC control stream can answer one: UDS has no
  handshake.
  """
  @spec peer_certificate(handle()) :: binary() | nil
  def peer_certificate(ref), do: Native.peer_certificate(ref)

  @doc "The process id of the runner on this stream's socket, or `nil`: only a UDS stream can say."
  @spec peer_pid(handle()) :: non_neg_integer() | nil
  def peer_pid(ref), do: Native.peer_pid(ref)
end
