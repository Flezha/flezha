defmodule Flezha.Service.Namespace do
  @moduledoc """
  Published service-API surface for namespaces as objects: their records, their
  quotas and the role bindings that reach them.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, authorizes through it with the action `flz namespace` and `flz role`
  use for the same verb, and answers data. `Flezha.Service.Cli.Authz` keeps the
  rendering.

  `roles/2` filters `Flezha.AuthZ.RoleStore.list/0` by namespace here rather
  than asking the store for a per-namespace list, because the store has no such
  read and adding one is a question about what a namespace screen shows.
  """

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.QuotaCounters
  alias Flezha.AuthZ.RoleStore
  alias Flezha.Service.CallerContext

  @audit_source :namespace_service

  @type caller_context :: CallerContext.t()

  # ---------- Reads ----------

  @doc "Every namespace record this controller holds."
  @spec list(caller_context()) :: {:ok, [map()]} | {:error, term()}
  def list(caller_context) do
    namespace_list_read(caller_context, fn ->
      {:ok, NamespaceStore.list()}
    end)
  end

  @doc "One namespace record."
  @spec get(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  def get(name, caller_context) when is_binary(name) do
    CallerContext.scoped_read(caller_context, :"authz.namespace_show", name, fn ->
      NamespaceStore.get(name)
    end)
  end

  @doc "The quota kinds a namespace can be given a ceiling for."
  @spec quota_kinds(caller_context()) :: {:ok, [atom()]} | {:error, term()}
  def quota_kinds(caller_context) do
    namespace_list_read(caller_context, fn ->
      {:ok, QuotaCounters.kinds()}
    end)
  end

  @doc "One namespace's current count and ceiling for `kind`."
  @spec quota(String.t(), atom(), caller_context()) :: {:ok, term()} | {:error, term()}
  def quota(name, kind, caller_context) when is_binary(name) and is_atom(kind) do
    CallerContext.scoped_read(caller_context, :"authz.namespace_show", name, fn ->
      {:ok, QuotaCounters.get(name, kind)}
    end)
  end

  @doc """
  The role bindings that reach `name`, including the ones bound cluster-wide.

  `nil` answers every binding, which is what a cluster-wide role screen shows.
  """
  @spec roles(String.t() | nil, caller_context()) :: {:ok, [map()]} | {:error, term()}
  def roles(name, caller_context) do
    CallerContext.scoped_read(
      caller_context,
      :"authz.role_list",
      name || CallerContext.system_scope(),
      fn ->
        {:ok, filter_by_namespace(RoleStore.list(), name)}
      end
    )
  end

  # ---------- Mutations ----------

  @doc "Create the namespace `name`."
  @spec create(String.t(), keyword(), caller_context()) :: term() | {:error, term()}
  def create(name, opts, caller_context) when is_binary(name) do
    CallerContext.gated(
      caller_context,
      :"authz.namespace_create",
      name,
      @audit_source,
      %{},
      fn -> NamespaceStore.create(name, opts) end
    )
  end

  @doc "Delete the namespace `name`."
  @spec delete(String.t(), caller_context()) :: term() | {:error, term()}
  def delete(name, caller_context) when is_binary(name) do
    CallerContext.gated(
      caller_context,
      :"authz.namespace_delete",
      name,
      @audit_source,
      %{},
      fn -> NamespaceStore.delete(name) end
    )
  end

  @doc "Replace the namespace's quota overrides."
  @spec set_quotas(String.t(), map(), caller_context()) :: term() | {:error, term()}
  def set_quotas(name, overrides, caller_context) when is_binary(name) and is_map(overrides) do
    CallerContext.gated(
      caller_context,
      :"authz.namespace_set_quota",
      name,
      @audit_source,
      %{kinds: Map.keys(overrides)},
      fn -> NamespaceStore.set_quotas(name, overrides) end
    )
  end

  defp namespace_list_read(caller_context, fun) do
    CallerContext.scoped_read(
      caller_context,
      :"authz.namespace_list",
      CallerContext.system_scope(),
      fun
    )
  end

  # A binding with no namespace is cluster-wide and reaches every namespace, so
  # it belongs in the answer for each of them.
  defp filter_by_namespace(bindings, nil), do: bindings

  defp filter_by_namespace(bindings, name) do
    Enum.filter(bindings, fn binding ->
      case Map.get(binding, :namespace) do
        nil -> true
        bound -> to_string(bound) == name
      end
    end)
  end
end
