defmodule Flezha.Transport.Stream do
  @moduledoc """
  Handle-polymorphic dispatch for per-stream transport operations. A handle is
  either a native `%Flezha.Transport.Local.Stream{}` (in-node transport) or a
  NIF stream resource (UDS/QUIC servers, and the standalone client). The five
  runner/transport call sites go through this seam so a native and a socket
  runner can coexist on one Controller; the NIF arm delegates to
  `Flezha.Transport.Native` unchanged.

  There is one send: `send_frame/4`, which frames in Rust. A raw-bytes send
  existed beside it, reached nothing, and is deleted — a second send path that
  must track the send gate forever is a trap for the next reader.
  `set_passive/1` is absent too (zero callers). The invariant this seam protects
  is grep-simple — no per-stream `Native.` call outside `native.ex` and here.
  """

  alias Flezha.Transport.Local
  alias Flezha.Transport.Native

  @type handle :: Local.Stream.t() | reference()

  @spec send_frame(handle(), binary(), term(), map()) :: :ok | {:error, term()}
  def send_frame(%Local.Stream{} = handle, payload_id, message, metadata),
    do: Local.Stream.send_frame(handle, payload_id, message, metadata)

  def send_frame(ref, payload_id, message, metadata),
    do: Native.send_frame(ref, payload_id, message, metadata)

  @spec set_active(handle()) :: :ok | {:error, term()}
  def set_active(%Local.Stream{} = handle), do: Local.Stream.set_active(handle)
  def set_active(ref), do: Native.set_active(ref)

  @doc """
  Deliver the next frame, then buffer until the stream is activated again. There
  is no native arm: the one caller is the socket server reading a connection's
  first frame, and an in-node connect is a function call with no frame to read.
  """
  @spec set_active_once(reference()) :: :ok | {:error, term()}
  def set_active_once(ref) when is_reference(ref), do: Native.set_active_once(ref)

  @spec close(handle()) :: :ok | {:error, term()}
  def close(%Local.Stream{} = handle), do: Local.Stream.close(handle)
  def close(ref), do: Native.close(ref)

  @spec controlling_process(handle(), pid()) :: :ok | {:error, term()}
  def controlling_process(%Local.Stream{} = handle, pid),
    do: Local.Stream.controlling_process(handle, pid)

  def controlling_process(ref, pid), do: Native.controlling_process(ref, pid)

  @spec stream_id(handle()) :: non_neg_integer()
  def stream_id(%Local.Stream{} = handle), do: Local.Stream.stream_id(handle)
  def stream_id(ref), do: Native.stream_id(ref)

  @doc """
  The DER certificate the peer presented on this stream's connection, or `nil`
  when it presented none. Only a QUIC control stream can answer one: UDS has no
  handshake and an in-node stream has no peer.
  """
  @spec peer_certificate(handle()) :: binary() | nil
  def peer_certificate(%Local.Stream{}), do: nil
  def peer_certificate(ref), do: Native.peer_certificate(ref)
end
