defmodule Flezha.AuthZ.RoleStore do
  @moduledoc """
  Durable principal → {namespace, role} bindings backed by
  `<auth_dir>/roles.dets`.

  An ETS read cache fronts the DETS table for hot-path lookups.
  Every mutation writes DETS first with `:dets.sync/1`, then updates
  the ETS cache, so a crash mid-write loses the cache update but
  never an unsynced mutation.

  A single principal may hold at most one binding (one (namespace,
  role) pair). Operators wanting cross-namespace presence must hold
  multiple principal identities (token-per-namespace pattern).
  """

  use GenServer

  alias Flezha.AuthZ.{Namespace, Role, Store}

  @table :flz_authz_roles
  @cache :flz_authz_roles_cache
  @filename "roles.dets"

  @type binding :: %{
          required(:principal) => binary(),
          required(:namespace) => binary(),
          required(:role) => Role.t(),
          required(:bound_at) => integer(),
          required(:bound_by) => binary() | nil
        }

  ## ----- Public API -----

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Lookup a principal's binding. Returns `{:ok, binding}` or
  `:error`. Reads from the ETS cache — no GenServer hop.
  """
  @spec lookup(binary() | nil) :: {:ok, binding()} | :error
  def lookup(nil), do: :error

  def lookup(principal) when is_binary(principal) do
    case :ets.whereis(@cache) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(@cache, principal) do
          [{^principal, binding}] -> {:ok, binding}
          _ -> :error
        end
    end
  rescue
    _ -> :error
  end

  @doc """
  Bind `principal` to `(namespace, role)`. Overwrites any existing
  binding. Synchronous: `:dets.sync/1` completes before return.
  """
  @spec bind(binary(), binary(), Role.t(), keyword()) :: :ok | {:error, term()}
  def bind(principal, namespace, role, opts \\ [])
      when is_binary(principal) and is_binary(namespace) and is_atom(role) do
    with :ok <- Role.validate(role),
         :ok <- Namespace.validate_name(namespace) do
      bound_by = Keyword.get(opts, :bound_by)
      GenServer.call(__MODULE__, {:bind, principal, namespace, role, bound_by})
    end
  end

  @doc "Remove a principal's binding. Idempotent."
  @spec unbind(binary()) :: :ok
  def unbind(principal) when is_binary(principal) do
    GenServer.call(__MODULE__, {:unbind, principal})
  end

  @doc "List every binding currently in the store."
  @spec list() :: [binding()]
  def list() do
    case :ets.whereis(@cache) do
      :undefined ->
        []

      _ ->
        :ets.tab2list(@cache)
        |> Enum.map(fn {_p, binding} -> binding end)
    end
  end

  @doc "Bindings belonging to `principal` (zero or one)."
  @spec list_for_principal(binary()) :: [binding()]
  def list_for_principal(principal) when is_binary(principal) do
    case lookup(principal) do
      {:ok, b} -> [b]
      :error -> []
    end
  end

  @doc "Every binding scoped to `namespace`."
  @spec list_for_namespace(binary()) :: [binding()]
  def list_for_namespace(namespace) when is_binary(namespace) do
    list() |> Enum.filter(&(&1.namespace == namespace))
  end

  @doc "True when at least one `admin@_system` binding exists."
  @spec admin_system_exists?() :: boolean()
  def admin_system_exists?() do
    Enum.any?(list(), fn b ->
      b.role == :admin and b.namespace == Namespace.system_name()
    end)
  end

  @doc "Absolute path of the backing DETS file."
  @spec path() :: String.t()
  def path(), do: Path.join(Flezha.AuthZ.Config.auth_dir(), @filename)

  ## ----- GenServer -----

  @impl true
  def init(_opts) do
    file_path = path()
    :ok = Store.ensure_auth_dir!(Path.dirname(file_path))

    case Store.open(@table, file_path) do
      {:ok, _} ->
        :ok = ensure_cache!()
        :ok = warm_cache_from_dets()
        {:ok, %{path: file_path}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, file_path, reason}}
    end
  end

  @impl true
  def handle_call({:bind, principal, namespace, role, bound_by}, _from, state) do
    binding = %{
      principal: principal,
      namespace: namespace,
      role: role,
      bound_at: System.system_time(:second),
      bound_by: bound_by
    }

    :ok = :dets.insert(@table, {principal, binding})
    :ok = :dets.sync(@table)
    :ets.insert(@cache, {principal, binding})
    {:reply, :ok, state}
  end

  def handle_call({:unbind, principal}, _from, state) do
    :ok = :dets.delete(@table, principal)
    :ok = :dets.sync(@table)
    :ets.delete(@cache, principal)
    {:reply, :ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :dets.close(@table)
    :ok
  end

  ## ----- private -----

  defp ensure_cache!() do
    case :ets.whereis(@cache) do
      :undefined ->
        :ets.new(@cache, [:set, :public, :named_table, read_concurrency: true])

      _ ->
        :ets.delete_all_objects(@cache)
    end

    :ok
  end

  defp warm_cache_from_dets() do
    :dets.match_object(@table, {:_, :_})
    |> Enum.each(fn {principal, binding} ->
      :ets.insert(@cache, {principal, binding})
    end)

    :ok
  end
end
