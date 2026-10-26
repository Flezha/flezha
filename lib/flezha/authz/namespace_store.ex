defmodule Flezha.AuthZ.NamespaceStore do
  @moduledoc """
  Durable namespace catalog backed by `<auth_dir>/namespaces.dets`.

  Each row stores `{name, quotas, migration, created_at, created_by_principal}`.
  An ETS read cache fronts the DETS table for hot-path quota
  lookups. `migration` names how the namespace's hub moves its relays, `:cold`
  or `:hot`; a row written before the field existed reads `:cold`.

  The system namespace `_system` is seeded on first open when the
  catalog is empty.
  """

  use GenServer
  use Flezha.State.Store

  @behaviour Flezha.AuthZ.Replica

  alias Flezha.AuthZ.{Config, Namespace, Replica}
  alias Flezha.Config.Durations
  alias Flezha.DetsStore

  @table :flz_authz_namespaces
  @cache :flz_authz_namespaces_cache
  @filename "namespaces.dets"

  @type entry :: %{
          required(:name) => binary(),
          required(:quotas) => Namespace.quotas(),
          required(:migration) => migration(),
          required(:created_at) => integer(),
          required(:created_by_principal) => binary() | nil
        }

  @type migration :: :cold | :hot

  @migrations [:cold, :hot]

  ## ----- Public API -----

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec create(binary(), keyword()) :: {:ok, entry()} | {:error, term()}
  def create(name, opts \\ []) when is_binary(name) do
    with :ok <- Namespace.validate_name(name),
         {:ok, quotas} <- build_full_quotas(name, Keyword.get(opts, :quotas, %{})) do
      by = Keyword.get(opts, :created_by_principal)
      Replica.published(call({:create, name, quotas, by}), __MODULE__, name)
    end
  end

  @spec delete(binary()) :: :ok | {:error, term()}
  def delete(name) when is_binary(name) do
    if Namespace.system?(name) do
      {:error, :cannot_delete_system_namespace}
    else
      Replica.published(call({:delete, name}), __MODULE__, name)
    end
  end

  @spec get(binary()) :: {:ok, entry()} | :error
  def get(name) when is_binary(name) do
    with {:ok, entry} <- DetsStore.cache_lookup(@cache, name), do: {:ok, with_defaults(entry)}
  end

  @spec list() :: [entry()]
  def list(), do: Enum.map(DetsStore.cache_values(@cache), &with_defaults/1)

  @spec set_quotas(binary(), map()) :: {:ok, entry()} | {:error, term()}
  def set_quotas(name, overrides) when is_binary(name) and is_map(overrides) do
    with {:ok, quotas} <- build_full_quotas(name, overrides) do
      Replica.published(call({:set, name, :quotas, quotas}), __MODULE__, name)
    end
  end

  @doc "The ways a namespace's hub can move its relays."
  @spec migrations() :: [migration()]
  def migrations(), do: @migrations

  @spec set_migration(binary(), migration()) :: {:ok, entry()} | {:error, term()}
  def set_migration(name, migration) when is_binary(name) and migration in @migrations,
    do: Replica.published(call({:set, name, :migration, migration}), __MODULE__, name)

  @doc "How `name`'s hub moves its relays; `:cold` for a namespace with no record."
  @spec migration(binary()) :: migration()
  def migration(name) when is_binary(name) do
    case get(name) do
      {:ok, entry} -> entry.migration
      :error -> :cold
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
    entry = new_entry(name, quotas, by)

    case DetsStore.fetch(@table, name) do
      :error ->
        :ok = Replica.put(__MODULE__, name, entry)
        {:reply, {:ok, entry}, state}

      {:ok, _held} ->
        {:reply, {:error, {:namespace_exists, name}}, state}
    end
  end

  # A delete says whether it removed anything: `create` refuses
  # `:already_exists`, so absence has to be an answer on this side too, or a
  # typo in a teardown script reports success.
  def handle_call({:delete, name}, _from, state) do
    case DetsStore.fetch(@table, name) do
      {:ok, _entry} ->
        :ok = Replica.delete(__MODULE__, name)
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, {:not_found, name}}, state}
    end
  end

  def handle_call({:set, name, field, value}, _from, state) do
    case DetsStore.fetch(@table, name) do
      {:ok, entry} ->
        updated = entry |> with_defaults() |> Map.put(field, value)
        :ok = Replica.put(__MODULE__, name, updated)
        {:reply, {:ok, updated}, state}

      :error ->
        {:reply, {:error, {:not_found, name}}, state}
    end
  end

  def handle_call({:replica_accept, rows}, _from, state),
    do: {:reply, Replica.merge(__MODULE__, rows), state}

  ## ----- private -----

  defp seed_system_namespace_if_absent() do
    system = Namespace.system_name()

    case DetsStore.fetch(@table, system) do
      :error ->
        {:ok, quotas} = build_full_quotas(system, %{})
        :ok = DetsStore.put(@table, @cache, system, new_entry(system, quotas, nil))

      _ ->
        :ok
    end
  end

  defp new_entry(name, quotas, by) do
    %{
      name: name,
      quotas: quotas,
      migration: :cold,
      created_at: System.system_time(:second),
      created_by_principal: by
    }
  end

  defp with_defaults(entry), do: Map.put_new(entry, :migration, :cold)

  # Quota precedence: defaults < config-file overrides for this
  # namespace < user-supplied overrides at create / set-quota time.
  defp build_full_quotas(name, user_overrides) do
    config_overrides = Config.namespace_quota_overrides() |> Map.get(name, %{})
    Namespace.build_quotas(Map.merge(config_overrides, user_overrides))
  end

  defp call(request), do: GenServer.call(__MODULE__, request, Durations.get(:authz_store_call_ms))
end
