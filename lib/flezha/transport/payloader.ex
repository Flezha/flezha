defmodule Flezha.Payloader do
  @moduledoc """
  JSON decoding for payloads that cross the wire as text.

  It raises rather than returning an error tuple: every caller holds a payload
  the sender already committed to, so an undecodable one is a defect and not a
  refusal the caller can act on.
  """

  @spec json_to_map(binary()) :: map()
  def json_to_map(""), do: %{}

  def json_to_map(json) do
    case Jason.decode(json) do
      {:ok, map} -> map
      {:error, reason} -> raise reason
    end
  end
end
