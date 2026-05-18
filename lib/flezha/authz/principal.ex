defmodule Flezha.AuthZ.Principal do
  @moduledoc """
  A Principal is a verified identity string.

  Examples:

    * SPIFFE URI SAN: `"spiffe://flezha.local/ns/team-orders/sa/exec-1"`
    * RFC 4514 subject DN: `"CN=executor-7,O=team-orders"`
    * Token-bound principal: `"alice@example.com"` (or whatever
      string was supplied at token-issue time).
    * `nil` — no principal bound. Permitted only while
      `:authn_authz, :enabled` is `false`.
  """

  @type t :: binary() | nil

  @doc """
  True when `value` is a usable Principal binding (a non-empty binary).
  """
  @spec valid?(t()) :: boolean()
  def valid?(p) when is_binary(p) and byte_size(p) > 0, do: true
  def valid?(_), do: false
end
