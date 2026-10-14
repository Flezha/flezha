defmodule Flezha.Relay.KeyGroup do
  @moduledoc """
  Key groups: the hash that puts a routing key in one of #{256} groups, the
  same hash every runner's partition guard reads, so a key the controller
  routes to a partition is a key that partition admits.

  The hash is FNV-1a-128 with the constants frozen by every SDK's `StableId`.
  """

  import Bitwise

  @key_groups 256

  @offset_basis 0x6C62272E07BB014262B821756295C58D
  @prime 0x0000000001000000000000000000013B
  @mask 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF

  @doc "How many key groups the space is divided into."
  @spec key_groups() :: pos_integer()
  def key_groups, do: @key_groups

  @doc """
  The key group a routing key belongs to: FNV-1a-128 of the key, mod 256.
  """
  @spec group_of(binary()) :: non_neg_integer()
  def group_of(key) when is_binary(key), do: band(fnv1a128(@offset_basis, key), @key_groups - 1)

  defp fnv1a128(hash, <<byte, rest::binary>>),
    do: fnv1a128(band(bxor(hash, byte) * @prime, @mask), rest)

  defp fnv1a128(hash, <<>>), do: hash
end
