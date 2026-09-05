defmodule Flezha.Operative.Lookup do
  @moduledoc """
  The one answer to "which process serves this operative".

  Four places used to answer it — a class registry read, a hub row read, a
  routed wrapper over the hub read, and a stream's own pid cache — in three
  spellings of the same failure (`"no pid found"`, `"operative not found"`,
  `"hub not alive"`) and with each caller writing its own liveness arm. A
  kind of key now names its resolver, and the failure is three atoms.

  Adding a way to name an operative is a module implementing
  `Flezha.Operative.Lookup.Resolver` and a line in `@resolvers`; it is not a
  fifth function somebody has to find.
  """

  alias Flezha.Operative.Lookup.Class
  alias Flezha.Operative.Lookup.Hub
  alias Flezha.Operative.Lookup.Resolver

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
  The live Overwatch pid for `operative_class`, from this node's class
  registry.
  """
  @spec overwatch(binary()) :: {:ok, pid()} | {:error, Resolver.reason()}
  def overwatch(operative_class), do: resolve(:class, operative_class)

  @doc "The pid serving `operative_id` in `namespace`, from that hub's registry."
  @spec operative(binary(), binary()) :: {:ok, pid()} | {:error, Resolver.reason()}
  def operative(namespace, operative_id), do: resolve(:namespaced, {namespace, operative_id})
end
