defmodule Flezha.Service.Namespace do
  @moduledoc """
  Published service-API surface for namespaces and access to them: their
  records and quotas, the role bindings that reach them, the tokens that prove
  an identity, and the one-time claim of the first admin.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data. The actor a
  store records is the principal the call was judged as, read from the
  context, never from an argument.

  `roles/2` filters `Flezha.AuthZ.RoleStore.list/0` by namespace here rather
  than asking the store for a per-namespace list, because the store has no such
  read and adding one is a question about what a namespace screen shows.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ.Bootstrap
  alias Flezha.AuthZ.Config
  alias Flezha.AuthZ.Namespace
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.Policy
  alias Flezha.AuthZ.QuotaCounters
  alias Flezha.AuthZ.Role
  alias Flezha.AuthZ.RoleStore
  alias Flezha.AuthZ.TokenStore
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Admin
  alias Flezha.Service.CallerContext
  alias Flezha.Service.HubManager

  @type caller_context :: CallerContext.t()

  @name [type: :string, doc: "The namespace."]
  @principal [type: :string, doc: "The principal."]
  @quotas [
    type: :map,
    default: %{},
    doc: "The ceilings that differ from the defaults, by quota kind."
  ]

  # ---------- Reads ----------

  @doc "Every namespace record this controller holds."
  @spec list(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list(caller_context), verb: :namespace_list do
    {:ok, NamespaceStore.list()}
  end

  @doc "One namespace record."
  @spec get(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb get(name, caller_context) when is_binary(name),
    verb: :namespace_show,
    args: [name: @name] do
    entry(name)
  end

  @doc "The quota kinds a namespace can be given a ceiling for."
  @spec quota_kinds(caller_context()) :: {:ok, [atom()]} | {:error, term()}
  verb quota_kinds(caller_context), verb: :namespace_list do
    {:ok, QuotaCounters.kinds()}
  end

  @doc "One namespace's current count and ceiling for `kind`."
  @spec quota(String.t(), atom(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb quota(name, kind, caller_context) when is_binary(name) and is_atom(kind),
    verb: :namespace_show,
    args: [
      name: @name,
      kind: [type: :enum, values: QuotaCounters.kinds(), doc: "The quota kind."]
    ] do
    {:ok, QuotaCounters.get(name, kind)}
  end

  @doc """
  The role bindings that reach `name`, including the ones bound cluster-wide.

  `nil` answers every binding, which is what a cluster-wide role screen shows.
  """
  @spec roles(String.t() | nil, caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb roles(name, caller_context),
    verb: :namespace_roles,
    args: [name: [type: :string, default: nil, doc: "The namespace; every binding when absent."]] do
    {:ok, filter_by_namespace(RoleStore.list(), name)}
  end

  @doc """
  The role bindings the caller may see, one per identity: every binding for
  `admin@_system`, otherwise those of the caller's own namespace.
  """
  @spec list_roles(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list_roles(caller_context), verb: :role_list do
    case Namespace.listing_scope(caller_context, :all) do
      :all -> {:ok, RoleStore.list()}
      namespace -> {:ok, filter_by_namespace(RoleStore.list(), namespace)}
    end
  end

  @doc """
  The identity the caller's credential resolves to.

  `operations` names the operations its role grants, as
  `Flezha.Service.Admin.catalogue/1` answers them for the same context, so a
  surface can offer only what the caller may use. The namespace is still judged
  at each call.
  """
  @spec whoami(caller_context()) :: {:ok, map()} | {:error, term()}
  verb whoami(caller_context), verb: :authz_whoami do
    identity = Map.new([:principal, :namespace, :role], &{&1, Map.get(caller_context, &1)})
    {:ok, Map.put(identity, :operations, Enum.map(Admin.catalogue(caller_context), & &1.name))}
  end

  @doc """
  The tokens the caller may see, one row each: its id, principal, role,
  namespace, label and whether it is active, revoked or expired, with
  `created_at`, `expires_at` (unix seconds, or `:never`), `revoked_at` (unix
  seconds, or nil) and `created_by`, the principal that issued it. Never the
  token itself.
  """
  @spec tokens(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb tokens(caller_context), verb: :token_list do
    {:ok,
     caller_context |> Namespace.listing_scope() |> TokenStore.list() |> Enum.map(&token_row/1)}
  end

  # ---------- Mutations ----------

  @doc "Create the namespace `name`, with `opts[:quotas]` its ceilings that differ from the defaults."
  @spec create(String.t(), keyword(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb create(name, opts, caller_context) when is_binary(name),
    verb: :namespace_create,
    args: [
      name: @name,
      opts: [
        type: :keyword,
        default: [],
        doc: "`quotas`, the ceilings that differ from the defaults."
      ]
    ] do
    with {:ok, quotas} <- quota_overrides(Keyword.get(opts, :quotas, %{})) do
      NamespaceStore.create(name, quotas: quotas, created_by_principal: actor(caller_context))
    end
  end

  @doc """
  Delete the namespace `name` and the quota counters keyed on it, refused while
  any relay or runner is still attached to it, naming them.
  """
  @spec delete(String.t(), caller_context()) :: :ok | {:error, term()}
  verb delete(name, caller_context) when is_binary(name),
    verb: :namespace_delete,
    args: [name: @name] do
    with {:ok, _entry} <- entry(name),
         :ok <- vacant(name),
         :ok <- NamespaceStore.delete(name) do
      _ = QuotaCounters.drop_namespace(name)
      :ok
    end
  end

  @doc "Replace the namespace's quota overrides."
  @spec set_quotas(String.t(), map(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb set_quotas(name, overrides, caller_context) when is_binary(name) and is_map(overrides),
    verb: :namespace_set_quota,
    args: [name: @name, overrides: @quotas] do
    with {:ok, overrides} <- quota_overrides(overrides),
         do: NamespaceStore.set_quotas(name, overrides)
  end

  @doc """
  Claim the first admin with the one-time bootstrap token, answering the
  principal bound as `admin@_system` and, when one was minted for it, its
  token, shown this once.
  """
  @spec bootstrap(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb bootstrap(token, caller_context) when is_binary(token),
    verb: :authz_bootstrap,
    args: [token: [type: :string, doc: "The one-time bootstrap token."]] do
    Bootstrap.claim(token)
  end

  @doc """
  Issue a token for `principal` holding `role` in `namespace`, answering its id
  and the token itself, shown this once. `opts` carries `ttl`, in seconds or
  `"never"`, and `label`.
  """
  @spec issue_token(String.t(), String.t(), String.t(), keyword(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb issue_token(principal, namespace, role, opts, caller_context),
    verb: :token_issue,
    args: [
      principal: @principal,
      namespace: @name,
      role: [type: :string, doc: "The role the token holds."],
      opts: [type: :keyword, default: [], doc: "`ttl`, in seconds or `\"never\"`, and `label`."]
    ] do
    with {:ok, role} <- parse_role(role),
         :ok <- issuable(role),
         {:ok, ttl} <- ttl(Keyword.get(opts, :ttl)),
         {:ok, raw, entry} <-
           TokenStore.issue(principal, namespace, role, ttl,
             created_by_principal: actor(caller_context),
             label: Keyword.get(opts, :label)
           ) do
      {:ok, entry |> token_row() |> Map.put(:token, raw)}
    end
  end

  @doc "Revoke the token whose id starts with `token_id`."
  @spec revoke_token(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb revoke_token(token_id, caller_context),
    verb: :token_revoke,
    row: &token_scope/1,
    args: [token_id: [type: :string, doc: "The token's id, or a prefix of it."]] do
    with {:ok, entry} <- TokenStore.revoke(token_id, revoked_by_principal: actor(caller_context)),
         do: {:ok, token_row(entry)}
  end

  @doc "Bind `principal` to `role` in `namespace`, answering the identity's binding after it."
  @spec bind_role(String.t(), String.t(), String.t(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb bind_role(principal, role, namespace, caller_context),
    verb: :role_bind,
    args: [principal: @principal, role: [type: :string, doc: "The role."], namespace: @name] do
    with {:ok, role} <- parse_role(role),
         :ok <- RoleStore.bind(principal, namespace, role, bound_by: actor(caller_context)) do
      {:ok,
       %{
         principal: principal,
         role: role,
         namespace: namespace,
         binding: bound_identity(principal)
       }}
    end
  end

  @doc """
  Unbind `role` from `principal`, or the principal's whole binding when `role`
  is absent or empty: the one-command revocation for an identity that is
  compromised or retired, which can only take rights away.
  """
  @spec unbind_role(String.t(), String.t() | nil, caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb unbind_role(principal, role, caller_context),
    verb: :role_unbind,
    row: &binding_scope/1,
    args: [
      principal: @principal,
      role: [type: :string, default: nil, doc: "The role; the whole binding when absent."]
    ] do
    if role in [nil, ""] do
      with :ok <- RoleStore.unbind(principal),
           do: {:ok, %{principal: principal, role: nil, binding: nil}}
    else
      with {:ok, role} <- parse_role(role),
           :ok <- RoleStore.unbind(principal, role) do
        {:ok, %{principal: principal, role: role, binding: bound_identity(principal)}}
      end
    end
  end

  # ---------- The rows ----------

  # The token a revoke names: its own namespace is the scope.
  defp token_scope([token_id | _]) do
    case Enum.find(TokenStore.list(:all), &String.starts_with?(&1.token_hash, token_id)) do
      nil -> {:error, {:token_not_found, token_id}}
      entry -> {:ok, %{namespace: entry.namespace}}
    end
  end

  # The binding an unbind names; a principal bound nowhere is judged in `_system`.
  defp binding_scope([principal | _]) do
    case RoleStore.lookup(principal) do
      {:ok, %{namespace: namespace}} -> {:ok, %{namespace: namespace}}
      :error -> {:ok, %{namespace: Namespace.system_name()}}
    end
  end

  # ---------- Helpers ----------

  defp actor(caller_context), do: Map.get(caller_context, :principal)

  defp entry(name), do: with(:error <- NamespaceStore.get(name), do: {:error, {:not_found, name}})

  # The relays `flz relay list` shows, and the runners `flz runner list` shows
  # plus those still in their handshake, which already run in the namespace.
  defp vacant(name) do
    relays = name |> HubManager.relay_rows() |> Enum.map(& &1.id)
    {:ok, runners} = FleetManager.global_occupants(name)

    case {relays, runners} do
      {[], []} -> :ok
      {relays, runners} -> {:error, {:namespace_in_use, name, relays, runners}}
    end
  end

  defp bound_identity(principal) do
    case RoleStore.lookup(principal) do
      {:ok, binding} -> binding
      :error -> nil
    end
  end

  defp token_row(entry) do
    %{
      id: String.slice(entry.token_hash, 0, 12),
      principal: entry.principal,
      namespace: entry.namespace,
      role: entry.role,
      label: Map.get(entry, :label),
      status: token_status(entry),
      created_at: entry.created_at,
      expires_at: entry.expires_at,
      revoked_at: entry.revoked_at,
      created_by: Map.get(entry, :created_by_principal)
    }
  end

  defp token_status(%{revoked_at: revoked}) when is_integer(revoked), do: :revoked

  defp token_status(%{expires_at: expires}) when is_integer(expires) do
    if expires <= System.system_time(:second), do: :expired, else: :active
  end

  defp token_status(_entry), do: :active

  defp ttl(nil), do: {:ok, Config.default_token_ttl_seconds()}
  defp ttl("never"), do: {:ok, :never}
  defp ttl(seconds) when is_integer(seconds) and seconds > 0, do: {:ok, seconds}
  defp ttl(written), do: {:error, {:invalid_ttl, written}}

  # Resolved against the one enumeration, so a name nobody declared never
  # becomes an atom and a declared role is never missing here.
  defp parse_role(role) do
    case Enum.find(Role.all(), &(Atom.to_string(&1) == to_string(role))) do
      nil -> {:error, {:unknown_role, role}}
      role -> {:ok, role}
    end
  end

  # A role carrying `runner.connect` is what a runner principal is bound to,
  # and a runner proves itself with a `runner` credential, so no credential is
  # issued for any of the others. Worked out from the grant, so a role added to
  # the enumeration with the connect needs no list edited here.
  defp issuable(role) do
    if role != :runner and Policy.runner_principal_role?(role),
      do: {:error, {:role_not_issuable, role}},
      else: :ok
  end

  # Quota overrides name documented quota fields and give each a whole number,
  # validated again when the store builds the record.
  defp quota_overrides(overrides) do
    Enum.reduce_while(overrides, {:ok, %{}}, fn {key, value}, {:ok, done} ->
      with {:ok, field} <- quota_field(key),
           {:ok, ceiling} <- quota_value(field, value) do
        {:cont, {:ok, Map.put(done, field, ceiling)}}
      else
        refused -> {:halt, refused}
      end
    end)
  end

  defp quota_field(key) when is_atom(key), do: {:ok, key}

  defp quota_field(key) do
    {:ok, String.to_existing_atom(key)}
  rescue
    ArgumentError -> {:error, {:unknown_quota_field, key}}
  end

  defp quota_value(_field, value) when is_integer(value), do: {:ok, value}

  defp quota_value(field, value) when is_binary(value) do
    case Integer.parse(value) do
      {ceiling, ""} -> {:ok, ceiling}
      _ -> {:error, Flezha.Error.new("usage.invalid_number", option: "--X#{field}", value: value)}
    end
  end

  defp quota_value(field, value),
    do:
      {:error,
       Flezha.Error.new("usage.invalid_number", option: "--X#{field}", value: inspect(value))}

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
