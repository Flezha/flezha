defmodule Flezha.Transport.Protocol do
  @moduledoc """
  Constants for the QUIC stream type identification protocol.

  When the SDK opens QUIC streams for data (after ConnectSocket command),
  the first byte identifies the stream type.
  """

  # Primary stream: controller writes input to SDK
  @stream_type_primary 0x01

  # Secondary stream: SDK writes output to controller
  @stream_type_secondary 0x02

  def stream_type_primary, do: @stream_type_primary
  def stream_type_secondary, do: @stream_type_secondary
end
