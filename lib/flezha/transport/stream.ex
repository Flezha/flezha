defmodule Flezha.Transport.Stream do
  @moduledoc """
  Handle-polymorphic dispatch for per-stream transport operations. A handle is
  either a native `%Flezha.Transport.Local.Stream{}` (in-node transport) or a
  NIF stream resource (UDS/QUIC servers, and the standalone client). The five
  runner/transport call sites go through this seam so a native and a socket
  runner can coexist on one Controller; the NIF arm delegates to
  `Flezha.Transport.Native` unchanged.

  `send_data/2` is deliberately absent: it carries pre-framed bytes and only the
  control channel used it, which now frames once in Rust via `send_frame/4`.
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
end
