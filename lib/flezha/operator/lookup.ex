defmodule Flezha.Operator.Lookup do
  @moduledoc """
  The one answer to "which process serves this operator".

  Four places used to answer it — a class registry read, a hub row read, a
  routed wrapper over the hub read, and a stream's own pid cache — in three
  spellings of the same failure (`"no pid found"`, `"operator not found"`,
  `"hub not alive"`) and with each caller writing its own liveness arm. A
  kind of key now names its resolver, and the failure is three atoms.

  Adding a way to name an operator is a module implementing
  `Flezha.Operator.Lookup.Resolver` and a line in `@resolvers`; it is not a
  fifth function somebody has to find.
  """

  alias Flezha.Operator.Lookup.Class
  alias Flezha.Operator.Lookup.Hub
  alias Flezha.Operator.Lookup.Resolver

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
  The live Overwatch pid for `operator_class`, from this node's class
  registry.
  """
  @spec overwatch(binary()) :: {:ok, pid()} | {:error, Resolver.reason()}
  def overwatch(operator_class), do: resolve(:class, operator_class)

  @doc "The pid serving `operator_id` in `namespace`, from that hub's registry."
  @spec operator(binary(), binary()) :: {:ok, pid()} | {:error, Resolver.reason()}
  def operator(namespace, operator_id), do: resolve(:namespaced, {namespace, operator_id})
end
