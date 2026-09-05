defmodule Flezha.Service.Namespace do
  @moduledoc """
  Published service-API surface for namespaces as objects: their records, their
  quotas and the role bindings that reach them.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, binds the row `flz namespace` and `flz role` bind for the same
  verb, and answers data. `Flezha.Service.Cli.Authz` keeps the rendering.

  `roles/2` filters `Flezha.AuthZ.RoleStore.list/0` by namespace here rather
  than asking the store for a per-namespace list, because the store has no such
  read and adding one is a question about what a namespace screen shows.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.QuotaCounters
  alias Flezha.AuthZ.RoleStore
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  # ---------- Reads ----------

  @doc "Every namespace record this controller holds."
  @spec list(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list(caller_context), verb: :namespace_list do
    {:ok, NamespaceStore.list()}
  end

  @doc "One namespace record."
  @spec get(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb get(name, caller_context) when is_binary(name), verb: :namespace_show do
    NamespaceStore.get(name)
  end

  @doc "The quota kinds a namespace can be given a ceiling for."
  @spec quota_kinds(caller_context()) :: {:ok, [atom()]} | {:error, term()}
  verb quota_kinds(caller_context), verb: :namespace_list do
    {:ok, QuotaCounters.kinds()}
  end

  @doc "One namespace's current count and ceiling for `kind`."
  @spec quota(String.t(), atom(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb quota(name, kind, caller_context) when is_binary(name) and is_atom(kind),
    verb: :namespace_show do
    {:ok, QuotaCounters.get(name, kind)}
  end

  @doc """
  The role bindings that reach `name`, including the ones bound cluster-wide.

  `nil` answers every binding, which is what a cluster-wide role screen shows.
  """
  @spec roles(String.t() | nil, caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb roles(name, caller_context), verb: :namespace_roles do
    {:ok, filter_by_namespace(RoleStore.list(), name)}
  end

  # ---------- Mutations ----------

  @doc "Create the namespace `name`."
  @spec create(String.t(), keyword(), caller_context()) :: term() | {:error, term()}
  verb create(name, opts, caller_context) when is_binary(name), verb: :namespace_create do
    NamespaceStore.create(name, opts)
  end

  @doc "Delete the namespace `name`."
  @spec delete(String.t(), caller_context()) :: term() | {:error, term()}
  verb delete(name, caller_context) when is_binary(name), verb: :namespace_delete do
    remove(name)
  end

  @doc "Replace the namespace's quota overrides."
  @spec set_quotas(String.t(), map(), caller_context()) :: term() | {:error, term()}
  verb set_quotas(name, overrides, caller_context) when is_binary(name) and is_map(overrides),
    verb: :namespace_set_quota do
    NamespaceStore.set_quotas(name, overrides)
  end

  @doc false
  # The one effect of a namespace delete, shared with the terminal: the record
  # goes, then the quota counters that were keyed on it.
  @spec remove(String.t()) :: :ok | {:error, term()}
  def remove(name) do
    with :ok <- NamespaceStore.delete(name) do
      _ = QuotaCounters.drop_namespace(name)
      :ok
    end
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
