defmodule Flezha.Service.Cli.Plugin do
  @moduledoc """
  `flz plugin` — enable, disable and help, plus the installer bridges
  (install, upgrade, uninstall, list, info).
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Providers
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.PluginManager

  # The plain options an enable reads. The verb is open, so every other plain
  # word a caller types arrives in the same document.
  @read_options ["namespace", "operator_id"]

  @doc """
  The row a `plugin enable` is judged by: the namespace in the caller's
  options, with the decoded provider, flags and options the body needs. A plain
  option the enable does not read is refused before anything is written.
  """
  def enable_row([_plugin_id, provider_id, plugin_flags_json, opts_json | _]) do
    with {:ok, provider_mod} <- get_plugin_provider_mod(provider_id),
         opts = Context.json!(opts_json, "plugin options"),
         :ok <- refuse_unread_options(opts) do
      plugin_flags = Context.json!(plugin_flags_json, "plugin settings")

      {:ok,
       %{
         namespace: Map.get(opts, "namespace", "default"),
         provider_mod: provider_mod,
         plugin_flags: plugin_flags,
         opts: opts
       }}
    end
  end

  @doc "The row a `plugin disable` is judged by: the namespace the plugin is enabled in."
  def plugin_row([plugin_id | _]),
    do: {:ok, %{namespace: PluginManager.target_namespace(plugin_id)}}

  @doc "Enables plugin."
  def enable_plugin(plugin_id, _provider_id, _plugin_flags_json, _opts_json, %{row: row}) do
    with {:ok, plugin} <- plugin(plugin_id, row),
         {:ok, outcome} <- PluginManager.enable_plugin(plugin_id, plugin) do
      IO.puts(enabled_message(plugin_id, outcome))
    end
  end

  defp plugin(plugin_id, row) do
    with {:ok, hooks, provider_opts} <- read_settings(row.plugin_flags) do
      {:ok,
       %Plugin{
         id: plugin_id,
         provider: row.provider_mod,
         namespace: row.namespace,
         operator_id:
           Map.get(row.plugin_flags, "operator_id") || Map.get(row.opts, "operator_id"),
         opts: provider_opts,
         hooks: hooks
       }}
    end
  end

  defp enabled_message(plugin_id, :already_enabled) do
    "Plugin #{plugin_id} is already enabled; the existing instance was left " <>
      "untouched (options were not compared)."
  end

  defp enabled_message(plugin_id, _msg), do: "Plugin #{plugin_id} enabled successfully."

  @doc """
  The settings document read as the plugin's hooks and its provider options.

  The hooks are the boundary shapes `Flezha.Plugin.hooks/1` reads, and a
  document naming none has no hooks. A hook says what a plugin SENDS; what it
  receives is its provider's own declared worker, which the extension table
  starts either way. The hooks leave the options, because a provider with a
  strict allow-list would refuse them as a key it does not own.
  """
  @spec read_settings(map()) :: {:ok, [Plugin.hook()], map()} | {:error, term()}
  def read_settings(plugin_flags) do
    {written, provider_flags} = Map.pop(plugin_flags, "hooks")

    with {:ok, hooks} <- read_hooks(List.wrap(written)) do
      {:ok, hooks, SpecDecoder.atomize_keys(provider_flags)}
    end
  end

  defp read_hooks([]), do: {:ok, []}
  defp read_hooks(written), do: Plugin.hooks(written)

  # Dropping a word the caller typed would enable a plugin with less than was
  # written and still report success.
  defp refuse_unread_options(opts) do
    case opts |> Map.drop(@read_options) |> Map.keys() |> Enum.sort() do
      [] ->
        :ok

      unread ->
        {:error, {:unread_plugin_options, Enum.map(unread, &"--#{&1}")}}
    end
  end

  defp get_plugin_provider_mod(provider_id) do
    with :error <- Providers.fetch(provider_id), do: {:error, {:unknown_provider, provider_id}}
  end

  @doc """
  Disables a plugin — or, when the id was fanned out over a partition group,
  every `#pK` instance of it.
  """
  def disable_plugin(plugin_id, _call) do
    with {:ok, msg} <- PluginManager.disable_plugin_group(plugin_id),
         do: IO.puts("Plugin #{plugin_id} disabled successfully (#{msg}).")
  end

  @doc "Install a plugin from a `.fpz` archive."
  def install_plugin(path, _call) do
    with {:ok, installed} <- Installer.install(path) do
      IO.puts(
        "Installed plugin #{installed.id}@#{installed.version} (status: #{installed.status})"
      )
    end
  end

  @doc "Upgrade an installed plugin to a new version in `path`."
  def upgrade_plugin(plugin_id, path, _call) do
    with {:ok, installed} <- Installer.upgrade(plugin_id, path),
         do: IO.puts("Upgraded plugin #{installed.id} to #{installed.version}")
  end

  @doc "Uninstall an installed plugin by id."
  def uninstall_plugin(plugin_id, _call) do
    with :ok <- Installer.uninstall(plugin_id), do: IO.puts("Uninstalled plugin #{plugin_id}.")
  end

  @doc "Enable an installed plugin (routes through the Installer)."
  def installer_enable_plugin(plugin_id, _call) do
    with :ok <- Installer.enable(plugin_id), do: IO.puts(enabled_message(plugin_id, :ok))
  end

  @doc "Disable an installed plugin (routes through the Installer)."
  def installer_disable_plugin(plugin_id, _call) do
    with :ok <- Installer.disable(plugin_id),
         do: IO.puts("Plugin #{plugin_id} disabled successfully.")
  end

  @doc """
  List every installed plugin, as text or as the JSON the caller's `output`
  asks for.
  """
  def list_installed_plugins(%{opts: opts}) do
    plugins = Installer.list()

    case Context.format(opts) do
      "json" ->
        plugins
        |> Enum.map(&installed_plugin_to_json/1)
        |> Jason.encode!()
        |> IO.puts()

      _ ->
        if plugins == [] do
          IO.puts("No plugins installed.")
        else
          IO.puts("Installed plugins:")

          Enum.each(plugins, fn p ->
            IO.puts("  #{p.id}@#{p.version} (#{p.status}) — #{p.description || ""}")
          end)
        end
    end
  end

  @doc "Return detailed information about an installed plugin."
  def installed_plugin_info(plugin_id, _call) do
    case Installer.info(plugin_id) do
      {:ok, p} ->
        IO.puts("#{p.id}@#{p.version}")
        IO.puts("  Status: #{p.status}")
        IO.puts("  Description: #{p.description || "(none)"}")
        IO.puts("  Author: #{p.author || "(none)"}")
        IO.puts("  Modules: #{inspect(p.modules)}")
        IO.puts("  Provides:")

        Enum.each(p.provides, fn prov ->
          IO.puts("    - #{Map.get(prov, :id)} -> #{inspect(Map.get(prov, :provider))}")
        end)

      {:error, :not_found} ->
        {:error, {:plugin_not_installed, plugin_id}}
    end
  end

  @doc """
  Returns `true` when `plugin_id` is tracked by `Flezha.Plugin.Installer`,
  so the CLI's `enable`/`disable` commands know which code path to use.
  """
  def installed_plugin?(plugin_id, _call) do
    case Installer.info(plugin_id) do
      {:ok, _} -> true
      _ -> false
    end
  end

  defp installed_plugin_to_json(%Flezha.Plugin.Installer.InstalledPlugin{} = p) do
    %{
      id: p.id,
      version: p.version,
      status: p.status,
      description: p.description,
      author: p.author,
      modules: Enum.map(p.modules, &inspect/1),
      provides:
        Enum.map(p.provides, fn prov ->
          %{
            id: Map.get(prov, :id),
            provider: inspect(Map.get(prov, :provider)),
            scope: Map.get(prov, :scope)
          }
        end),
      installed_at: p.installed_at
    }
  end

  @doc "Show help for a specific plugin provider."
  def plugin_help(provider_id, _call) do
    case get_plugin_provider_mod(provider_id) do
      {:ok, provider_mod} ->
        try do
          help_text = provider_mod.help(provider_id)
          IO.puts(help_text)
          IO.puts("")
          print_supported_hooks(provider_mod)
        rescue
          UndefinedFunctionError ->
            IO.puts(
              "Help not available for plugin '#{provider_id}'. The plugin provider does not implement help/1."
            )

          e ->
            {:error, {:plugin_help_failed, provider_id, Exception.message(e)}}
        end

      {:error, _reason} = refused ->
        refused
    end
  end

  defp print_supported_hooks(provider_mod) do
    IO.puts("SUPPORTED HOOKS:")
    Enum.each(provider_mod.__provider__().extensions, &IO.puts("    --hook=#{&1}"))
  end

  @doc "List all available plugin providers."
  def list_plugins(_call) do
    enabled_plugins = Providers.all()

    IO.puts("Available Plugin Providers:")
    IO.puts("==========================")

    Enum.each(enabled_plugins, fn {provider_id, module} ->
      IO.puts("  #{provider_id}")
      IO.puts("    Module: #{inspect(module)}")

      try do
        plugin_type = apply(module, :plugin_type, [])
        IO.puts("    Type: #{plugin_type}")
      rescue
        _ -> nil
      end

      IO.puts("")
    end)

    IO.puts("Use 'flz plugin help <provider_id>' for detailed help on a specific plugin.")
  end
end
