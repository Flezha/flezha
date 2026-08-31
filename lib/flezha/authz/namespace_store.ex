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

  @doc "Absolute path of the backing DETS file."
  @spec path() :: String.t()
  def path(), do: Path.join(Config.auth_dir(), @filename)

  ## ----- GenServer -----

  @impl true
  def init(_opts) do
    with {:ok, state} <- DetsStore.init_cached(@table, @cache, path()) do
      :ok = seed_system_namespace_if_absent()
      {:ok, state}
    end
  end

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
      {:error, :already_exists} = err -> {:reply, err, state}
    end
  end

  def handle_call({:delete, name}, _from, state) do
    :ok = DetsStore.delete(@table, @cache, name)
    {:reply, :ok, state}
  end

  def handle_call({:set_quotas, name, quotas}, _from, state) do
    case DetsStore.update(@table, @cache, name, &%{&1 | quotas: quotas}) do
      {:ok, updated} -> {:reply, {:ok, updated}, state}
      {:error, :not_found} = err -> {:reply, err, state}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :dets.close(@table)
    :ok
  end

  ## ----- private -----

  defp seed_system_namespace_if_absent() do
    system = Namespace.system_name()

    case :dets.lookup(@table, system) do
      [] ->
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
