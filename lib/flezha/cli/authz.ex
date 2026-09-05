defmodule Flezha.Service.Cli.Authz do
  @moduledoc """
  `flz auth | token | role | namespace` — bootstrap, token issue/revoke/list,
  role bind/unbind/list, namespace create/delete/show/set-quota, and the
  renderers `flz` calls for each. Each body receives the bound call, so the
  actor a store records is the principal the verb was judged as.
  """

  alias Flezha.AuthZ.Bootstrap
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.RoleStore
  alias Flezha.AuthZ.TokenStore
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.Namespace

  @doc "The token a `token revoke` names: its own namespace is the scope."
  def token_row([token_id | _]) do
    case find_token_by_id(token_id) do
      {:ok, entry} -> {:ok, %{namespace: entry.namespace, entry: entry}}
      :error -> Context.fail!("token revoke: :not_found")
    end
  end

  @doc "The binding a `role unbind` names; a principal bound nowhere is judged in `_system`."
  def binding_row([principal | _]) do
    case RoleStore.lookup(principal) do
      {:ok, %{namespace: namespace}} -> {:ok, %{namespace: namespace}}
      :error -> {:ok, %{namespace: Flezha.AuthZ.Namespace.system_name()}}
    end
  end

  @doc "CLI entry: `flz auth bootstrap <token>`. The one-time token is its own credential."
  def authz_render_bootstrap(token, opts \\ []) when is_binary(token) do
    case Bootstrap.claim(token, opts) do
      {:ok, %{principal: principal, minted_token: nil}} ->
        IO.puts("bootstrap claimed; admin@_system bound to #{principal}")

      {:ok, %{principal: principal, minted_token: minted}} ->
        IO.puts("bootstrap claimed; admin@_system bound to #{principal}")
        IO.puts("admin token (shown once, store it now): #{minted}")

      {:error, reason} ->
        Context.fail!("auth bootstrap: #{Context.format_reason(reason)}")
    end
  end

  @doc "The identity a verified token resolves to."
  def authz_whoami(%{context: context}) do
    {:ok,
     %{
       principal: Map.get(context, :principal),
       namespace: Map.get(context, :namespace),
       role: Map.get(context, :role)
     }}
  end

  @doc "CLI entry: `flz auth whoami`."
  def authz_render_whoami(call) do
    {:ok, identity} = authz_whoami(call)
    IO.puts("principal: #{identity.principal || "(unbound — supply --token / FLEZHA_TOKEN)"}")
    IO.puts("namespace: #{identity.namespace || "-"}")
    IO.puts("role:      #{identity.role || "-"}")
  end

  @doc "CLI entry: `flz token issue <principal> <namespace> <role> [--ttl <seconds>]`."
  def authz_render_token_issue(principal, namespace, role_string, call) do
    with {:ok, role} <- parse_role(role_string),
         {:ok, raw, entry} <-
           TokenStore.issue(principal, namespace, role, Keyword.fetch!(call.opts, :ttl),
             created_by_principal: actor(call),
             label: Keyword.get(call.opts, :label)
           ) do
      IO.puts("token issued for #{principal} (#{role}@#{namespace})")
      IO.puts("id:    #{String.slice(entry.token_hash, 0, 12)}")
      IO.puts("token (shown once, store it now): #{raw}")
    else
      {:error, reason} -> Context.fail!("token issue: #{Context.format_reason(reason)}")
    end
  end

  @doc "CLI entry: `flz token revoke <id>`."
  def authz_render_token_revoke(token_id, call) do
    case TokenStore.revoke(token_id, revoked_by_principal: actor(call)) do
      {:ok, entry} -> IO.puts("revoked token #{String.slice(entry.token_hash, 0, 12)}")
      {:error, reason} -> Context.fail!("token revoke: #{Context.format_reason(reason)}")
    end
  end

  @doc "CLI entry: `flz token list`."
  def authz_render_token_list(%{context: context}) do
    case TokenStore.list(filter_namespace_for_list(context)) do
      [] ->
        IO.puts("no tokens")

      entries ->
        Enum.each(entries, fn entry ->
          status =
            cond do
              is_integer(entry.revoked_at) ->
                "revoked"

              entry.expires_at != :never and entry.expires_at <= System.system_time(:second) ->
                "expired"

              true ->
                "active"
            end

          IO.puts(
            "#{String.slice(entry.token_hash, 0, 12)}  #{entry.principal}  " <>
              "#{entry.role}@#{entry.namespace}  #{status}#{token_label_suffix(entry)}"
          )
        end)
    end
  end

  defp token_label_suffix(%{label: label}) when is_binary(label) and label != "",
    do: "  (#{label})"

  defp token_label_suffix(_), do: ""

  @doc "CLI entry: `flz role bind <principal> <role> <namespace>`."
  def authz_render_role_bind(principal, role_string, namespace, call) do
    with {:ok, role} <- parse_role(role_string),
         :ok <- RoleStore.bind(principal, namespace, role, bound_by: actor(call)) do
      IO.puts("bound #{principal} as #{role}@#{namespace}")
    else
      {:error, reason} -> Context.fail!("role bind: #{Context.format_reason(reason)}")
    end
  end

  @doc "CLI entry: `flz role unbind <principal>`."
  def authz_render_role_unbind(principal, _call) do
    :ok = RoleStore.unbind(principal)
    IO.puts("unbound #{principal}")
  end

  @doc "CLI entry: `flz role list`."
  def authz_render_role_list(_call) do
    case RoleStore.list() do
      [] ->
        IO.puts("no role bindings")

      bindings ->
        Enum.each(bindings, fn binding ->
          IO.puts("#{binding.principal}  #{binding.role}@#{binding.namespace}")
        end)
    end
  end

  @doc "CLI entry: `flz namespace create <name> [--Xquota=value ...]`."
  def authz_render_namespace_create(name, quotas_json, call) do
    with {:ok, quotas} <- parse_quota_overrides(quotas_json),
         {:ok, entry} <-
           NamespaceStore.create(name, quotas: quotas, created_by_principal: actor(call)) do
      IO.puts("namespace #{name} created")
      print_namespace_entry(entry)
    else
      {:error, reason} -> Context.fail!("namespace create: #{Context.format_reason(reason)}")
    end
  end

  @doc "CLI entry: `flz namespace delete <name>`."
  def authz_render_namespace_delete(name, _call) do
    case Namespace.remove(name) do
      :ok -> IO.puts("namespace #{name} deleted")
      {:error, reason} -> Context.fail!("namespace delete: #{Context.format_reason(reason)}")
    end
  end

  @doc "CLI entry: `flz namespace show <name>`."
  def authz_render_namespace_show(name, _call) do
    case NamespaceStore.get(name) do
      {:ok, entry} -> print_namespace_entry(entry)
      :error -> Context.fail!("namespace show: :not_found")
    end
  end

  @doc "CLI entry: `flz namespace set-quota <name> --X<quota>=<value> ...`."
  def authz_render_namespace_set_quota(name, quotas_json, _call) do
    with {:ok, overrides} <- parse_quota_overrides(quotas_json),
         {:ok, entry} <- NamespaceStore.set_quotas(name, overrides) do
      IO.puts("namespace #{name} quotas updated")
      print_namespace_entry(entry)
    else
      {:error, reason} -> Context.fail!("namespace set-quota: #{Context.format_reason(reason)}")
    end
  end

  defp actor(%{context: context}), do: Map.get(context, :principal)

  defp print_namespace_entry(entry) do
    IO.puts("namespace: #{entry.name}")
    IO.puts("quotas:")

    entry.quotas
    |> Enum.sort()
    |> Enum.each(fn {k, v} -> IO.puts("  #{k}: #{v}") end)
  end

  defp parse_role(role_string) when is_binary(role_string) do
    case role_string do
      "viewer" -> {:ok, :viewer}
      "operator" -> {:ok, :operator}
      "admin" -> {:ok, :admin}
      "runner" -> {:ok, :runner}
      other -> {:error, {:unknown_role, other}}
    end
  end

  defp parse_role(role) when is_atom(role), do: {:ok, role}

  # Quota overrides arrive as the CLI's `--X` JSON object; keys must name
  # documented quota fields, values must be non-negative integers (validated
  # again by `Namespace.build_quotas/1`).
  defp parse_quota_overrides(quotas_json) when is_binary(quotas_json) do
    {:ok,
     quotas_json
     |> Context.json!("quota overrides")
     |> Map.new(fn {k, v} -> {String.to_existing_atom(k), parse_quota_value(v)} end)}
  rescue
    ArgumentError -> {:error, :unknown_quota_field}
  end

  defp parse_quota_overrides(%{} = quotas), do: {:ok, quotas}

  defp parse_quota_value(v) when is_integer(v), do: v
  defp parse_quota_value(v) when is_binary(v), do: Context.integer!(v, "quota value")

  defp filter_namespace_for_list(%{role: :admin, namespace: "_system"}), do: :all
  defp filter_namespace_for_list(context), do: Map.get(context, :namespace) || "_system"

  defp find_token_by_id(id) do
    TokenStore.list(:all)
    |> Enum.find(fn entry -> String.starts_with?(entry.token_hash, id) end)
    |> case do
      nil -> :error
      entry -> {:ok, entry}
    end
  end
end
