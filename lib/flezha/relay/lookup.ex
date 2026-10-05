defmodule Flezha.Relay.Lookup do
  @moduledoc """
  The one answer to "which process serves this relay".

  Four places used to answer it — a class registry read, a hub row read, a
  routed wrapper over the hub read, and a stream's own pid cache — in three
  spellings of the same failure (`"no pid found"`, `"relay not found"`,
  `"hub not alive"`) and with each caller writing its own liveness arm. A
  kind of key now names its resolver, and the failure is three atoms.

  Adding a way to name a relay is a module implementing
  `Flezha.Relay.Lookup.Resolver` and a line in `@resolvers`; it is not a
  fifth function somebody has to find.
  """

  alias Flezha.Relay.Lookup.Class
  alias Flezha.Relay.Lookup.Hub
  alias Flezha.Relay.Lookup.Resolver

  @resolvers [Class, Hub]

  @by_kind Map.new(@resolvers, fn resolver -> {resolver.key_kind(), resolver} end)

  @doc "The declared resolvers, in declaration order."
  @spec resolvers() :: [module()]
  def resolvers(), do: @resolvers

  @doc "The key kinds `resolve/2` answers for."
  @spec kinds() :: [atom()]
  def kinds(), do: Map.keys(@by_kind)

  @doc "Resolve `key` through the resolver declared for `kind`."
  @spec resolve(atom(), Resolver.key()) :: {:ok, pid()} | {:error, Resolver.reason()}
  def resolve(kind, key) when is_map_key(@by_kind, kind) do
    @by_kind |> Map.fetch!(kind) |> apply(:resolve, [key])
  end

  @doc """
  The live Overwatch pid for `relay_class`, from this node's class
  registry.
  """
  @spec overwatch(binary()) :: {:ok, pid()} | {:error, Resolver.reason()}
  def overwatch(relay_class), do: resolve(:class, relay_class)

  @doc "The pid serving `relay_id` in `namespace`, from that hub's registry."
  @spec relay(binary(), binary()) :: {:ok, pid()} | {:error, Resolver.reason()}
  def relay(namespace, relay_id), do: resolve(:namespaced, {namespace, relay_id})
end
