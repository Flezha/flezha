defmodule Flezha.Service.Cli.Authz do
  @moduledoc """
  `flz auth | token | role | namespace` — the text forms of the answers
  `Flezha.Service.Namespace` gives for identity, tokens, role bindings and
  namespaces.
  """

  @doc "`flz auth whoami`."
  def whoami(identity, _args, _opts) do
    IO.puts("principal: #{identity.principal || "(unbound — supply --token / FLEZHA_TOKEN)"}")
    IO.puts("namespace: #{identity.namespace || "-"}")
    IO.puts("role:      #{identity.role || "-"}")
  end

  @doc "`flz auth bootstrap`."
  def bootstrap(%{principal: principal, minted_token: minted}, _args, _opts) do
    IO.puts("bootstrap claimed; admin@_system bound to #{principal}")
    if minted, do: IO.puts("admin token (shown once, store it now): #{minted}")
  end

  @doc "`flz token issue`."
  def issue_token(token, _args, _opts) do
    IO.puts("token issued for #{token.principal} (#{token.role}@#{token.namespace})")
    IO.puts("id:    #{token.id}")
    IO.puts("token (shown once, store it now): #{token.token}")
  end

  @doc "`flz token list`."
  def tokens([], _args, _opts), do: IO.puts("no tokens")

  def tokens(tokens, _args, _opts) do
    Enum.each(tokens, fn token ->
      IO.puts(
        "#{token.id}  #{token.principal}  #{token.role}@#{token.namespace}  " <>
          "#{token.status}#{label_suffix(token.label)}"
      )
    end)
  end

  @doc "`flz token revoke <id>`."
  def revoke_token(token, _args, _opts), do: IO.puts("revoked token #{token.id}")

  @doc "`flz role bind`."
  def bind_role(bound, _args, _opts) do
    IO.puts("bound #{bound.principal} as #{bound.role}@#{bound.namespace}")
    print_identity(bound.binding)
  end

  @doc "`flz role unbind`."
  def unbind_role(%{principal: principal, role: nil}, _args, _opts),
    do: IO.puts("unbound #{principal}")

  def unbind_role(unbound, _args, _opts) do
    IO.puts("unbound #{unbound.role} from #{unbound.principal}")
    print_identity(unbound.binding)
  end

  @doc "`flz role list`."
  def list_roles([], _args, _opts), do: IO.puts("no role bindings")
  def list_roles(bindings, _args, _opts), do: Enum.each(bindings, &print_identity/1)

  @doc "`flz namespace create`."
  def create(entry, _args, _opts) do
    IO.puts("namespace #{entry.name} created")
    print_namespace(entry)
  end

  @doc "`flz namespace delete`."
  def delete(:ok, [name | _], _opts), do: IO.puts("namespace #{name} deleted")

  @doc "`flz namespace show`."
  def get(entry, _args, _opts), do: print_namespace(entry)

  @doc "`flz namespace set-quota`."
  def set_quotas(entry, _args, _opts) do
    IO.puts("namespace #{entry.name} quotas updated")
    print_namespace(entry)
  end

  defp label_suffix(label) when is_binary(label) and label != "", do: "  (#{label})"
  defp label_suffix(_none), do: ""

  # One line per identity, shared by the three role verbs. An identity holding
  # one role prints the line `role list` has always printed.
  defp print_identity(nil), do: :ok

  defp print_identity(%{principal: principal, roles: roles, namespace: namespace}),
    do: IO.puts("#{principal}  #{Enum.join(roles, ",")}@#{namespace}")

  defp print_namespace(entry) do
    IO.puts("namespace: #{entry.name}")
    IO.puts("quotas:")

    entry.quotas
    |> Enum.sort()
    |> Enum.each(fn {k, v} -> IO.puts("  #{k}: #{v}") end)
  end
end
