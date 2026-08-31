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

  alias Flezha.AuthZ.Config
  alias Flezha.AuthZ.{Namespace, Role}
  alias Flezha.DetsStore

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

  def lookup(principal) when is_binary(principal), do: DetsStore.cache_lookup(@cache, principal)

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
  def list(), do: DetsStore.cache_values(@cache)

  @doc "True when at least one `admin@_system` binding exists."
  @spec admin_system_exists?() :: boolean()
  def admin_system_exists?() do
    Enum.any?(list(), fn b ->
      b.role == :admin and b.namespace == Namespace.system_name()
    end)
  end

  @doc "Absolute path of the backing DETS file."
  @spec path() :: String.t()
  def path(), do: Path.join(Config.auth_dir(), @filename)

  ## ----- GenServer -----

  @impl true
  def init(_opts) do
    DetsStore.init_cached(@table, @cache, path())
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

    :ok = DetsStore.put(@table, @cache, principal, binding)
    {:reply, :ok, state}
  end

  def handle_call({:unbind, principal}, _from, state) do
    :ok = DetsStore.delete(@table, @cache, principal)
    {:reply, :ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :dets.close(@table)
    :ok
  end
end
