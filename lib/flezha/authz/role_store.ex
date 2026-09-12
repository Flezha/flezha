defmodule Flezha.AuthZ.RoleStore do
  @moduledoc """
  Durable principal → {namespace, roles} bindings backed by
  `<auth_dir>/roles.dets`.

  An ETS read cache fronts the DETS table for hot-path lookups.
  Every mutation writes DETS first with `:dets.sync/1`, then updates
  the ETS cache, so a crash mid-write loses the cache update but
  never an unsynced mutation.

  A principal holds a SET of roles in ONE namespace: `bind/4` adds a role and
  refuses a namespace other than the one the principal already holds roles in,
  `unbind/2` removes one, and `unbind/1` removes the identity. Operators wanting
  cross-namespace presence must hold multiple principal identities
  (token-per-namespace pattern).

  A stored row carries both the set and a one-role field, the role itself when
  the set has exactly one member and `nil` when it has several. This release
  never reads that field from a row carrying `roles`; it is written so that a
  release predating sets reads a one-role row as it always did and grants
  nothing from a row holding several, instead of raising on a missing key. A row
  written before sets is read here as a set of one, and rewritten in the new
  shape only when its identity's roles next change.
  """

  use GenServer
  use Flezha.State.Store

  alias Flezha.AuthZ.{Namespace, Policy, Role}
  alias Flezha.DetsStore

  @table :flz_authz_roles
  @cache :flz_authz_roles_cache
  @filename "roles.dets"

  @type binding :: %{
          required(:principal) => binary(),
          required(:namespace) => binary(),
          required(:roles) => [Role.t(), ...],
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
    with {:ok, row} <- DetsStore.cache_lookup(@cache, principal), do: from_row(row)
  end

  @doc """
  Add `role` to `principal`'s roles in `namespace`. Idempotent: a role already
  held writes nothing. Refuses `{:error, {:bound_elsewhere, held}}` when the
  principal holds roles in another namespace, naming that one. Synchronous:
  `:dets.sync/1` completes before return.
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

  @doc "Remove a principal's whole binding, whatever it holds. Idempotent."
  @spec unbind(binary()) :: :ok
  def unbind(principal) when is_binary(principal) do
    GenServer.call(__MODULE__, {:unbind, principal})
  end

  @doc """
  Remove one role, deleting the binding when it was the last. Idempotent, and a
  role the principal does not hold changes nothing.
  """
  @spec unbind(binary(), Role.t()) :: :ok | {:error, :unknown_role}
  def unbind(principal, role) when is_binary(principal) and is_atom(role) do
    with :ok <- Role.validate(role) do
      GenServer.call(__MODULE__, {:unbind, principal, role})
    end
  end

  @doc "List every binding currently in the store. A row it cannot read is skipped."
  @spec list() :: [binding()]
  def list() do
    @cache
    |> DetsStore.cache_values()
    |> Enum.flat_map(fn row ->
      case from_row(row) do
        {:ok, binding} -> [binding]
        :error -> []
      end
    end)
  end

  @doc "True when at least one identity holds `admin` in `_system`."
  @spec admin_system_exists?() :: boolean()
  def admin_system_exists?() do
    Enum.any?(list(), fn b ->
      :admin in b.roles and b.namespace == Namespace.system_name()
    end)
  end

  @doc """
  The roles a runner connection for `namespace` holds, read per call.

  The roles bound to `principal` in the namespace the connection PROVED that
  grant `runner.connect`, or `[:runner]` when the principal is unbound, bound
  elsewhere, or holds no such role. A role an operator's credential carries
  (`viewer`, `operator`, `admin`) is never held by a connection: keeping it
  would open the administration surface's namespace and quota mutations to
  runner code, which `sdk-admin-api` forbids.

  It lives here rather than beside the session so that the connect path keeps
  naming only the `AuthZ` modules the layer baseline already records.
  """
  @spec runner_roles(binary(), binary()) :: [Role.t(), ...]
  def runner_roles(principal, namespace) when is_binary(principal) and is_binary(namespace) do
    with {:ok, %{namespace: ^namespace, roles: roles}} <- lookup(principal),
         [_ | _] = connecting <- Enum.filter(roles, &Policy.runner_principal_role?/1) do
      connecting
    else
      _unbound_foreign_or_operator_only -> [:runner]
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

  ## ----- GenServer -----

  @impl true
  def handle_call({:bind, principal, namespace, role, bound_by}, _from, state) do
    {:reply, do_bind(principal, namespace, role, bound_by), state}
  end

  def handle_call({:unbind, principal}, _from, state) do
    {:reply, DetsStore.delete(@table, @cache, principal), state}
  end

  def handle_call({:unbind, principal, role}, _from, state) do
    {:reply, do_unbind(principal, role), state}
  end

  ## ----- writes (serialized by the GenServer) -----

  # Each write reads DETS rather than the cache, so the set it adds to is the
  # one on disk. `DetsStore.update/4` is not used: its mapping function cannot
  # refuse, and a bind must refuse another namespace.
  defp do_bind(principal, namespace, role, bound_by) do
    case stored(principal) do
      {:ok, %{namespace: held_ns}} when held_ns != namespace ->
        {:error, {:bound_elsewhere, held_ns}}

      {:ok, held} ->
        if role in held.roles do
          :ok
        else
          write(%{held | roles: order([role | held.roles]), bound_at: now(), bound_by: bound_by})
        end

      :error ->
        write(%{
          principal: principal,
          namespace: namespace,
          roles: [role],
          bound_at: now(),
          bound_by: bound_by
        })
    end
  end

  defp do_unbind(principal, role) do
    case stored(principal) do
      {:ok, %{roles: roles} = held} ->
        case roles -- [role] do
          ^roles -> :ok
          [] -> DetsStore.delete(@table, @cache, principal)
          remaining -> write(%{held | roles: remaining})
        end

      :error ->
        :ok
    end
  end

  defp stored(principal) do
    with {:ok, row} <- DetsStore.fetch(@table, principal), do: from_row(row)
  end

  defp write(binding), do: DetsStore.put(@table, @cache, binding.principal, to_row(binding))

  defp now(), do: System.system_time(:second)

  ## ----- the row shape -----

  # The one reader of a stored value, so no caller in this tree can mistake the
  # one-role field for the set. A row matching neither shape is no binding: a
  # row this release cannot read grants nothing, which is the rule it asks of
  # older releases in return.
  defp from_row(%{principal: principal, namespace: namespace} = row)
       when is_binary(principal) and is_binary(namespace) do
    case row_roles(row) do
      [] ->
        :error

      roles ->
        {:ok,
         %{
           principal: principal,
           namespace: namespace,
           roles: roles,
           bound_at: Map.get(row, :bound_at),
           bound_by: Map.get(row, :bound_by)
         }}
    end
  end

  defp from_row(_row), do: :error

  defp row_roles(%{roles: roles}) when is_list(roles), do: order(roles)
  defp row_roles(%{role: role}) when is_atom(role) and not is_nil(role), do: order([role])
  defp row_roles(_row), do: []

  defp to_row(%{roles: roles} = binding) do
    ordered = order(roles)
    binding |> Map.put(:roles, ordered) |> Map.put(:role, one_role(ordered))
  end

  # Written for a release that predates sets, and read back by nothing here.
  defp one_role([role]), do: role
  defp one_role(_several), do: nil

  # `Role.all/0` order, deduplicated, and anything the enumeration does not name
  # dropped: equality, idempotence and every rendering are then deterministic.
  defp order(roles), do: Enum.filter(Role.all(), &(&1 in roles))
end
