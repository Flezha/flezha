defmodule Flezha.Service.Cli.Authz do
  @moduledoc """
  `flz auth | token | role | namespace` — bootstrap, token issue/revoke/list,
  role bind/unbind/list, namespace create/delete/show/set-quota, and the
  renderers `flz` calls for each.
  """

  alias Flezha.AuthZ
  alias Flezha.AuthZ.Bootstrap
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.QuotaCounters
  alias Flezha.AuthZ.RoleStore
  alias Flezha.AuthZ.Scope
  alias Flezha.AuthZ.TokenStore
  alias Flezha.Service.Cli.Context

  defp authz_bootstrap(token, opts) when is_binary(token) do
    case Bootstrap.claim(token, opts) do
      {:ok, %{principal: principal, minted_token: minted}} ->
        Context.emit_cli_audit(:bootstrap_completed, principal, %{
          namespace: "_system",
          minted_token?: not is_nil(minted)
        })

        {:ok, %{principal: principal, minted_token: minted}}

      {:error, _} = err ->
        err
    end
  end

  defp authz_issue(principal, namespace, role, ttl_seconds, opts) do
    cli_ctx = Context.cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.token_issue", Scope.build(namespace)) do
      TokenStore.issue(principal, namespace, role, ttl_seconds,
        created_by_principal: Map.get(cli_ctx, :principal),
        label: Keyword.get(opts, :label)
      )
    end
  end

  defp authz_revoke(token_id, opts) when is_binary(token_id) do
    cli_ctx = Context.cli_context(opts)

    # We must check the token's namespace to authorize cross-namespace
    # revocations. For simplicity we look it up first.
    case find_token_by_id(token_id) do
      {:ok, entry} ->
        with :ok <- AuthZ.require_bound_principal(cli_ctx),
             :ok <-
               AuthZ.authorize(
                 cli_ctx,
                 :"authz.token_revoke",
                 Scope.build(entry.namespace)
               ) do
          TokenStore.revoke(token_id, revoked_by_principal: Map.get(cli_ctx, :principal))
        end

      :error ->
        {:error, :not_found}
    end
  end

  defp authz_list(opts) do
    cli_ctx = Context.cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <-
           AuthZ.authorize(cli_ctx, :"authz.token_list", Scope.build(scope_namespace(cli_ctx))) do
      ns = filter_namespace_for_list(cli_ctx)
      {:ok, TokenStore.list(ns)}
    end
  end

  def authz_whoami(opts \\ []) do
    cli_ctx = Context.cli_context(opts)

    {:ok,
     %{
       principal: Map.get(cli_ctx, :principal),
       namespace: Map.get(cli_ctx, :namespace),
       role: Map.get(cli_ctx, :role)
     }}
  end

  def namespace_create(name, quotas, opts \\ []) when is_binary(name) and is_map(quotas) do
    cli_ctx = Context.cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.namespace_create", Scope.build(name)) do
      result =
        NamespaceStore.create(name,
          quotas: quotas,
          created_by_principal: Map.get(cli_ctx, :principal)
        )

      case result do
        {:ok, entry} ->
          Context.emit_cli_audit(:namespace_created, Map.get(cli_ctx, :principal), %{
            namespace: name
          })

          {:ok, entry}

        other ->
          other
      end
    end
  end

  defp namespace_delete(name, opts) when is_binary(name) do
    cli_ctx = Context.cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.namespace_delete", Scope.build(name)) do
      case NamespaceStore.delete(name) do
        :ok ->
          _ = QuotaCounters.drop_namespace(name)

          Context.emit_cli_audit(:namespace_deleted, Map.get(cli_ctx, :principal), %{
            namespace: name
          })

          :ok

        other ->
          other
      end
    end
  end

  defp namespace_show(name, opts) when is_binary(name) do
    cli_ctx = Context.cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.namespace_show", Scope.build(name)) do
      case NamespaceStore.get(name) do
        {:ok, entry} -> {:ok, entry}
        :error -> {:error, :not_found}
      end
    end
  end

  defp namespace_set_quota(name, overrides, opts)
       when is_binary(name) and is_map(overrides) do
    cli_ctx = Context.cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.namespace_set_quota", Scope.build(name)) do
      case NamespaceStore.set_quotas(name, overrides) do
        {:ok, entry} ->
          Context.emit_cli_audit(:namespace_quota_updated, Map.get(cli_ctx, :principal), %{
            namespace: name,
            quotas: overrides
          })

          {:ok, entry}

        other ->
          other
      end
    end
  end

  defp role_bind(principal, namespace, role, opts)
       when is_binary(principal) and is_binary(namespace) and is_atom(role) do
    cli_ctx = Context.cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.role_bind", Scope.build(namespace)) do
      case RoleStore.bind(principal, namespace, role, bound_by: Map.get(cli_ctx, :principal)) do
        :ok ->
          Context.emit_cli_audit(:role_bound, Map.get(cli_ctx, :principal), %{
            principal: principal,
            namespace: namespace,
            role: role
          })

          :ok

        other ->
          other
      end
    end
  end

  defp role_unbind(principal, opts) when is_binary(principal) do
    cli_ctx = Context.cli_context(opts)

    bound_ns =
      case RoleStore.lookup(principal) do
        {:ok, %{namespace: ns}} -> ns
        :error -> "_system"
      end

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.role_unbind", Scope.build(bound_ns)) do
      :ok = RoleStore.unbind(principal)
      Context.emit_cli_audit(:role_unbound, Map.get(cli_ctx, :principal), %{principal: principal})
      :ok
    end
  end

  defp role_list(opts) do
    cli_ctx = Context.cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <-
           AuthZ.authorize(
             cli_ctx,
             :"authz.role_list",
             Scope.build(scope_namespace(cli_ctx))
           ) do
      {:ok, RoleStore.list()}
    end
  end

  @doc "CLI entry: `flz auth bootstrap <token>`."
  def authz_render_bootstrap(token, opts \\ []) do
    case authz_bootstrap(token, opts) do
      {:ok, %{principal: principal, minted_token: nil}} ->
        IO.puts("bootstrap claimed; admin@_system bound to #{principal}")

      {:ok, %{principal: principal, minted_token: minted}} ->
        IO.puts("bootstrap claimed; admin@_system bound to #{principal}")
        IO.puts("admin token (shown once, store it now): #{minted}")

      {:error, reason} ->
        Context.cli_error!("auth bootstrap: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz auth whoami`."
  def authz_render_whoami(opts \\ []) do
    {:ok, identity} = authz_whoami(opts)
    IO.puts("principal: #{identity.principal || "(unbound — supply --token / FLEZHA_TOKEN)"}")
    IO.puts("namespace: #{identity.namespace || "-"}")
    IO.puts("role:      #{identity.role || "-"}")
  end

  @doc "CLI entry: `flz token issue <principal> <namespace> <role> [--ttl <seconds>]`."
  def authz_render_token_issue(principal, namespace, role_string, ttl_seconds, opts \\ []) do
    with {:ok, role} <- parse_role(role_string),
         {:ok, raw, entry} <- authz_issue(principal, namespace, role, ttl_seconds, opts) do
      IO.puts("token issued for #{principal} (#{role}@#{namespace})")
      IO.puts("id:    #{String.slice(entry.token_hash, 0, 12)}")
      IO.puts("token (shown once, store it now): #{raw}")
    else
      {:error, reason} ->
        Context.cli_error!("token issue: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz token revoke <id>`."
  def authz_render_token_revoke(token_id, opts \\ []) do
    case authz_revoke(token_id, opts) do
      {:ok, entry} ->
        IO.puts("revoked token #{String.slice(entry.token_hash, 0, 12)}")

      {:error, reason} ->
        Context.cli_error!("token revoke: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz token list`."
  def authz_render_token_list(opts \\ []) do
    case authz_list(opts) do
      {:ok, []} ->
        IO.puts("no tokens")

      {:ok, entries} ->
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

      {:error, reason} ->
        Context.cli_error!("token list: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  defp token_label_suffix(%{label: label}) when is_binary(label) and label != "",
    do: "  (#{label})"

  defp token_label_suffix(_), do: ""

  @doc "CLI entry: `flz role bind <principal> <role> <namespace>`."
  def authz_render_role_bind(principal, role_string, namespace, opts \\ []) do
    with {:ok, role} <- parse_role(role_string),
         :ok <- role_bind(principal, namespace, role, opts) do
      IO.puts("bound #{principal} as #{role}@#{namespace}")
    else
      {:error, reason} ->
        Context.cli_error!("role bind: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz role unbind <principal>`."
  def authz_render_role_unbind(principal, opts \\ []) do
    case role_unbind(principal, opts) do
      :ok ->
        IO.puts("unbound #{principal}")

      {:error, reason} ->
        Context.cli_error!("role unbind: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz role list`."
  def authz_render_role_list(opts \\ []) do
    case role_list(opts) do
      {:ok, []} ->
        IO.puts("no role bindings")

      {:ok, bindings} ->
        Enum.each(bindings, fn binding ->
          IO.puts("#{binding.principal}  #{binding.role}@#{binding.namespace}")
        end)

      {:error, reason} ->
        Context.cli_error!("role list: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz namespace create <name> [--Xquota=value ...]`."
  def authz_render_namespace_create(name, quotas_json, opts \\ []) do
    with {:ok, quotas} <- parse_quota_overrides(quotas_json),
         {:ok, entry} <- namespace_create(name, quotas, opts) do
      IO.puts("namespace #{name} created")
      print_namespace_entry(entry)
    else
      {:error, reason} ->
        Context.cli_error!("namespace create: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz namespace delete <name>`."
  def authz_render_namespace_delete(name, opts \\ []) do
    case namespace_delete(name, opts) do
      :ok ->
        IO.puts("namespace #{name} deleted")

      {:error, reason} ->
        Context.cli_error!("namespace delete: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz namespace show <name>`."
  def authz_render_namespace_show(name, opts \\ []) do
    case namespace_show(name, opts) do
      {:ok, entry} ->
        print_namespace_entry(entry)

      {:error, reason} ->
        Context.cli_error!("namespace show: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz namespace set-quota <name> --X<quota>=<value> ...`."
  def authz_render_namespace_set_quota(name, quotas_json, opts \\ []) do
    with {:ok, overrides} <- parse_quota_overrides(quotas_json),
         {:ok, entry} <- namespace_set_quota(name, overrides, opts) do
      IO.puts("namespace #{name} quotas updated")
      print_namespace_entry(entry)
    else
      {:error, reason} ->
        Context.cli_error!("namespace set-quota: #{render_authz_verdict(reason)}", :stderr)
    end
  end

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
    with {:ok, decoded} <- Jason.decode(quotas_json) do
      {:ok,
       Map.new(decoded, fn {k, v} ->
         {String.to_existing_atom(k), parse_quota_value(v)}
       end)}
    end
  rescue
    ArgumentError -> {:error, :unknown_quota_field}
  end

  defp parse_quota_overrides(%{} = quotas), do: {:ok, quotas}

  defp parse_quota_value(v) when is_integer(v), do: v
  defp parse_quota_value(v) when is_binary(v), do: String.to_integer(v)

  defp render_authz_verdict({:forbidden, reason}), do: Context.render_forbidden(reason)
  defp render_authz_verdict(reason), do: inspect(reason)

  defp scope_namespace(ctx), do: Map.get(ctx, :namespace) || "_system"

  defp filter_namespace_for_list(%{role: :admin, namespace: "_system"}), do: :all
  defp filter_namespace_for_list(ctx), do: scope_namespace(ctx)

  defp find_token_by_id(id) do
    TokenStore.list(:all)
    |> Enum.find(fn entry -> String.starts_with?(entry.token_hash, id) end)
    |> case do
      nil -> :error
      entry -> {:ok, entry}
    end
  end
end
