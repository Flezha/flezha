defmodule Flezha.Service.Cli.Plugin do
  @moduledoc """
  `flz plugin`: the text forms of enable, bind, unbind, update and disable,
  of the enabled plugins, the providers, a provider's help and the installed
  packages.
  """

  alias Flezha.Service.Cli.Context

  @doc "`flz plugin enable`."
  def enable(enabled, _args, _opts) do
    case enabled.outcome do
      :already_enabled ->
        IO.puts(
          "Plugin #{enabled.name} is already enabled; the existing plugin was left " <>
            "untouched (options were not compared)."
        )

      :enabled ->
        IO.puts("Plugin #{enabled.name} enabled.")
    end

    if enabled.lacking != [] do
      IO.puts(
        "warning: #{Enum.join(enabled.lacking, ", ")} hold the namespace and lack the " <>
          "provider or did not answer; the plugin will not run there, and a relay " <>
          "bound to it there runs without it"
      )
    end

    if enabled.bound, do: IO.puts("Plugin #{enabled.name} bound to #{enabled.bound}.")
  end

  @doc "`flz plugin bind`."
  def bind(:already_bound, [name, fields | _], _opts),
    do: IO.puts("Plugin #{name} is already bound to #{fields["relay_id"]}.")

  def bind(_bound, [name, fields | _], _opts),
    do: IO.puts("Plugin #{name} bound to #{fields["relay_id"]}.")

  @doc "`flz plugin unbind`."
  def unbind(_unbound, [name, fields | _], _opts),
    do: IO.puts("Plugin #{name} unbound from #{fields["relay_id"]}.")

  @doc "`flz plugin update`."
  def update(%{applied: applied, relays: relays}, [name | _], _opts) do
    case {applied, relays} do
      {:unchanged, _} ->
        IO.puts("Plugin #{name} is unchanged.")

      {:live, _} ->
        IO.puts("Plugin #{name} updated in place.")

      {:restarted, []} ->
        IO.puts("Plugin #{name} updated; its running copies restarted.")

      {:restarted, relays} ->
        IO.puts("Plugin #{name} updated; its running copies restarted.")
        IO.puts("warning: restarted under #{Enum.join(relays, ", ")}")
    end
  end

  @doc "`flz plugin disable`."
  def disable(_disabled, [name | _], _opts), do: IO.puts("Plugin #{name} disabled.")

  @doc "`flz plugin list`: each enabled plugin, where it runs and what it is bound to."
  def enabled(%{rows: _} = page, args, opts),
    do: Context.rows(page, &enabled(&1, args, opts))

  def enabled([], _args, _opts), do: IO.puts("No plugins enabled.")
  def enabled(rows, _args, _opts), do: Enum.each(rows, &IO.puts(enabled_line(&1)))

  defp enabled_line(row) do
    single = if row.single, do: " single", else: ""
    where = if row.controllers == [], do: "idle", else: Enum.join(row.controllers, ", ")
    bound = if row.relays == [], do: "none", else: Enum.join(row.relays, ", ") <> more(row)
    "#{row.name}  #{row.provider}#{single}  bound: #{bound}  runs on: #{where}" <> hooks(row)
  end

  # A paged row names a sample of its relays.
  defp more(%{relay_count: count, relays: relays}) when count > length(relays),
    do: " (#{count} in all)"

  defp more(_row), do: ""

  # A controller plugin's own hooks, and where its copy falls behind.
  defp hooks(%{hooks: []}), do: ""

  defp hooks(%{hooks: hooks, lagging: lagging}) do
    behind = if lagging == [], do: "", else: "  lagging on: #{Enum.join(lagging, ", ")}"
    "  hooks: #{Enum.join(hooks, ", ")}" <> behind
  end

  @doc "`flz plugin list --installed`."
  def installed(answer, _args, _opts), do: Context.rows(answer, &installed_lines/1)

  defp installed_lines([]), do: IO.puts("No plugin packages installed.")
  defp installed_lines(rows), do: Enum.each(rows, &IO.puts("#{&1.node}  #{installed_line(&1)}"))

  defp installed_line(%{status: :loaded} = row),
    do: "#{row.id} #{row.version}  loaded  providers: #{Enum.join(row.providers, ", ")}"

  defp installed_line(%{status: :refused, error: error} = row) do
    hint = if error.hint, do: "\n    hint: #{error.hint}", else: ""
    "#{row.id} #{row.version}  refused  #{error.message} (#{error.code})" <> hint
  end

  defp installed_line(%{status: :pending, loaded_version: nil} = row),
    do: "#{row.id} #{row.version}  pending  loads at next restart"

  defp installed_line(%{status: :pending} = row),
    do: "#{row.id} #{row.version}  pending  loads at next restart (running #{row.loaded_version})"

  defp installed_line(%{status: :removed} = row),
    do: "#{row.id} #{row.version}  removed  unloads at next restart"

  @doc "`flz plugin list --providers`."
  def providers(%{rows: providers, refused: refused, unreachable: unreachable}, _args, _opts) do
    IO.puts("Available Plugin Providers:")
    IO.puts("==========================")

    Enum.each(providers, fn provider ->
      IO.puts("  #{provider.id}")
      IO.puts("    Module: #{provider.module}")
      if provider.type, do: IO.puts("    Type: #{provider.type}")
      IO.puts("    Controllers: #{Enum.join(provider.nodes, ", ")}")
      IO.puts("")
    end)

    IO.puts("Use 'flz plugin help <provider_id>' for detailed help on a specific plugin.")

    for {node, count} <- refused, count > 0 do
      IO.puts(
        "#{count} plugin package(s) were refused at boot on #{node}: flz plugin list --installed"
      )
    end

    Context.unreachable(unreachable)
  end

  @doc "`flz plugin help <provider_id>`."
  def provider(%{help: nil, provider: provider_id}, _args, _opts) do
    IO.puts(
      "Help not available for plugin '#{provider_id}'. " <>
        "The plugin provider does not implement help/1."
    )
  end

  def provider(help, _args, _opts) do
    IO.puts(help.help)
    IO.puts("Type: #{help.type}")
    IO.puts("")
    IO.puts("SUPPORTED HOOKS:")
    Enum.each(help.hooks, &IO.puts("    --hook=#{&1}"))
  end
end
