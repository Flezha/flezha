defmodule Flezha.AuthZ.NamespaceStore do
  @moduledoc """
  Durable namespace catalog backed by `<auth_dir>/namespaces.dets`.

  Each row stores `{name, quotas, created_at, created_by_principal}`.
  An ETS read cache fronts the DETS table for hot-path quota
  lookups.

  The system namespace `_system` is seeded on first open when the
  catalog is empty.
  """

  use GenServer
  use Flezha.State.Store

  alias Flezha.AuthZ.{Config, Namespace}
  alias Flezha.DetsStore

  @table :flz_authz_namespaces
  @cache :flz_authz_namespaces_cache
  @filename "namespaces.dets"

  @type entry :: %{
          required(:name) => binary(),
          required(:quotas) => Namespace.quotas(),
          required(:created_at) => integer(),
          required(:created_by_principal) => binary() | nil
        }

  ## ----- Public API -----

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec create(binary(), keyword()) :: {:ok, entry()} | {:error, term()}
  def create(name, opts \\ []) when is_binary(name) do
    with :ok <- Namespace.validate_name(name),
         {:ok, quotas} <- build_full_quotas(name, Keyword.get(opts, :quotas, %{})) do
      by = Keyword.get(opts, :created_by_principal)
      GenServer.call(__MODULE__, {:create, name, quotas, by})
    end
  end

  @spec delete(binary()) :: :ok | {:error, term()}
  def delete(name) when is_binary(name) do
    if Namespace.system?(name) do
      {:error, :cannot_delete_system_namespace}
    else
      GenServer.call(__MODULE__, {:delete, name})
    end
  end

  @spec get(binary()) :: {:ok, entry()} | :error
  def get(name) when is_binary(name), do: DetsStore.cache_lookup(@cache, name)

  @spec list() :: [entry()]
  def list(), do: DetsStore.cache_values(@cache)

  @spec set_quotas(binary(), map()) :: {:ok, entry()} | {:error, term()}
  def set_quotas(name, overrides) when is_binary(name) and is_map(overrides) do
    with {:ok, quotas} <- build_full_quotas(name, overrides) do
      GenServer.call(__MODULE__, {:set_quotas, name, quotas})
    end
  end

  ## ----- store declaration -----

  @impl Flezha.State.Store
  def subtree(), do: :auth

  @impl Flezha.State.Store
  def filename(), do: @filename

  @impl Flezha.State.Store
  def table(), do: @table

  @impl Flezha.State.Store
  def cache(), do: @cache

  @impl Flezha.State.Store
  def after_open(_state), do: seed_system_namespace_if_absent()

  ## ----- GenServer -----

  @impl true
  def handle_call({:create, name, quotas, by}, _from, state) do
    entry = %{
      name: name,
      quotas: quotas,
      created_at: System.system_time(:second),
      created_by_principal: by
    }

    case DetsStore.put_new(@table, @cache, name, entry) do
      :ok -> {:reply, {:ok, entry}, state}
      {:error, :already_exists} -> {:reply, {:error, {:namespace_exists, name}}, state}
    end
  end

  # A delete says whether it removed anything: `create` refuses
  # `:already_exists`, so absence has to be an answer on this side too, or a
  # typo in a teardown script reports success.
  def handle_call({:delete, name}, _from, state) do
    case DetsStore.fetch(@table, name) do
      {:ok, _entry} ->
        :ok = DetsStore.delete(@table, @cache, name)
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, {:not_found, name}}, state}
    end
  end

  def handle_call({:set_quotas, name, quotas}, _from, state) do
    case DetsStore.update(@table, @cache, name, &%{&1 | quotas: quotas}) do
      {:ok, updated} -> {:reply, {:ok, updated}, state}
      {:error, :not_found} -> {:reply, {:error, {:not_found, name}}, state}
    end
  end

  ## ----- private -----

  defp seed_system_namespace_if_absent() do
    system = Namespace.system_name()

    case DetsStore.fetch(@table, system) do
      :error ->
        {:ok, quotas} = build_full_quotas(system, %{})

        entry = %{
          name: system,
          quotas: quotas,
          created_at: System.system_time(:second),
          created_by_principal: nil
        }

        :ok = DetsStore.put(@table, @cache, system, entry)

      _ ->
        :ok
    end
  end

  # Quota precedence: defaults < config-file overrides for this
  # namespace < user-supplied overrides at create / set-quota time.
  defp build_full_quotas(name, user_overrides) do
    config_overrides = Config.namespace_quota_overrides() |> Map.get(name, %{})
    Namespace.build_quotas(Map.merge(config_overrides, user_overrides))
  end
end
