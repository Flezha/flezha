defmodule Flezha.Service.Payloader do
  @moduledoc """
  Binary frame building and utility functions.

  Frame parsing (parse_frame) has been moved to the Rust NIF (FrameBuffer).
  The NIF delivers complete `{payload_id, message, metadata}` tuples to Elixir.

  This module retains frame building for Elixir-side frame construction
  (e.g., control commands) and JSON utility functions.
  """

  alias Flezha.Operative.Builder, as: OperativeBuilder

  def json_to_map(""), do: %{}

  def json_to_map(json) do
    case Jason.decode(json) do
      {:ok, map} -> map
      {:error, reason} -> raise reason
    end
  end

  def construct_operative!(input, type) do
    result =
      case type do
        :json -> OperativeBuilder.from_json(input)
        :map -> OperativeBuilder.from_map(input)
      end

    case result do
      {:ok, operative} -> operative
      {:error, reason} -> raise reason
    end
  end
end
