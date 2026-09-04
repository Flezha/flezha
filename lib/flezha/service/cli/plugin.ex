defmodule Flezha.Service.Cli.Plugin do
  @moduledoc """
  `flz plugin` — enable, disable and help, plus the installer bridges
  (install, upgrade, uninstall, list, info).
  """

  alias Flezha.Config
  alias Flezha.Plugin
  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.PluginManager

  # Subscription directives, not provider options: they are consumed by
  # `extract_subscriptions/2` and must not reach `init_provider/1`, which for a
  # provider with a strict allow-list refuses every key it does not own.
  @subscription_keys ["extensions", "trigger"]

  @doc "Enables plugin."
  def enable_plugin(plugin_id, provider_id, plugin_flags_json, opts_json) do
    with {:ok, provider_mod} <- get_plugin_provider_mod(provider_id),
         {:ok, plugin_flags} <- Jason.decode(plugin_flags_json),
         {:ok, opts} <- Jason.decode(opts_json) do
      subscriptions = extract_subscriptions(plugin_flags, opts)
      namespace = Map.get(opts, "namespace", "default")
      operative_id = Map.get(plugin_flags, "operative_id") || Map.get(opts, "operative_id")

      Context.with_cli_authorization(
        Context.json_token_opts(opts_json),
        :"plugin.enable",
        namespace,
        %{plugin_id: plugin_id, provider: provider_id},
        fn ->
          plugin = %Plugin{
            id: plugin_id,
            provider: provider_mod,
            namespace: namespace,
            operative_id: operative_id,
            opts: provider_opts(plugin_flags),
            subscriptions: subscriptions
          }

          case PluginManager.enable_plugin(plugin_id, plugin) do
            {:ok, :already_enabled} ->
              IO.puts(
                "Plugin #{plugin_id} is already enabled; the existing instance was left " <>
                  "untouched (options were not compared)."
              )

            {:ok, _msg} ->
              IO.puts("Plugin #{plugin_id} enabled successfully.")

            {:error, reason} ->
              Context.cli_error!("Error enabling plugin: #{inspect(reason)}.")
          end
        end
      )
    else
      {:error, reason} -> Context.cli_error!("Error enabling plugin: #{inspect(reason)}.")
    end
  end

  @doc """
  Build subscriptions from the legacy `--trigger=on_input` shape (single
  trigger) OR the `--extension=on_input` repeatable shape. The legacy trigger
  values `proactive` and `interval` both map onto `:on_interval`.
  """
  def extract_subscriptions(plugin_flags, opts) do
    extensions =
      plugin_flags
      |> Map.get("extensions")
      |> List.wrap()

    extensions =
      if extensions == [],
        do: Map.get(opts, "extensions") |> List.wrap(),
        else: extensions

    if extensions != [] do
      # Raw text, both halves: `Extension.Table.canonicalise/2` resolves the
      # name and types every value against the declaration for its kind.
      Enum.map(extensions, fn e ->
        {Map.get(e, "name"), Map.get(e, "opts", %{})}
      end)
    else
      trigger = Map.get(opts, "trigger") || Map.get(plugin_flags, "trigger")
      trigger_to_subscription(trigger)
    end
  end

  @doc """
  The plugin flags a provider actually receives: everything the operator wrote
  as `--X…` minus the subscription directives, which `extract_subscriptions/2`
  has already consumed.
  """
  def provider_opts(plugin_flags) do
    plugin_flags
    |> Map.drop(@subscription_keys)
    |> SpecDecoder.atomize_keys()
  end

  @doc false
  def trigger_to_subscription("on_input"), do: [{:on_input, []}]
  def trigger_to_subscription("on_output"), do: [{:on_output, []}]
  def trigger_to_subscription("interval"), do: [{:on_interval, [interval_ms: 5_000]}]
  def trigger_to_subscription("proactive"), do: [{:on_interval, [interval_ms: 5_000]}]
  def trigger_to_subscription(_), do: [{:on_input, []}]

  defp get_plugin_provider_mod(provider_id) do
    enabled_plugins = Config.get_enabled_plugins()

    case Map.get(enabled_plugins, provider_id, nil) do
      nil -> {:error, "plugin provider not found, make sure the plugin is enabled."}
      module -> {:ok, module}
    end
  end

  @doc """
  Disables a plugin — or, when the id was fanned out over a partition group,
  every `#pK` instance of it.
  """
  def disable_plugin(plugin_id, opts \\ []) do
    Context.with_cli_authorization(
      opts,
      :"plugin.disable",
      PluginManager.target_namespace(plugin_id),
      %{plugin_id: plugin_id},
      fn ->
        case PluginManager.disable_plugin_group(plugin_id) do
          {:ok, msg} -> IO.puts("Plugin #{plugin_id} disabled successfully (#{msg}).")
          {:error, reason} -> Context.cli_error!("Error disabling plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  # A running plugin's namespace comes from its own registration; an unknown
  # id authorizes against `_system` (and then fails on its own terms).
  @doc "Install a plugin from a `.fpz` archive."
  def install_plugin(path, opts_json) do
    Context.with_cli_authorization(
      Context.json_token_opts(opts_json),
      :"plugin.install",
      "_system",
      %{path: path},
      fn ->
        case Installer.install(path) do
          {:ok, installed} ->
            IO.puts(
              "Installed plugin #{installed.id}@#{installed.version} (status: #{installed.status})"
            )

          {:error, reason} ->
            Context.cli_error!("Error installing plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  @doc "Upgrade an installed plugin to a new version in `path`."
  def upgrade_plugin(plugin_id, path, opts_json) do
    Context.with_cli_authorization(
      Context.json_token_opts(opts_json),
      :"plugin.upgrade",
      "_system",
      %{plugin_id: plugin_id},
      fn ->
        case Installer.upgrade(plugin_id, path) do
          {:ok, installed} ->
            IO.puts("Upgraded plugin #{installed.id} to #{installed.version}")

          {:error, reason} ->
            Context.cli_error!("Error upgrading plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  @doc "Uninstall an installed plugin by id."
  def uninstall_plugin(plugin_id, opts_json) do
    Context.with_cli_authorization(
      Context.json_token_opts(opts_json),
      :"plugin.uninstall",
      "_system",
      %{plugin_id: plugin_id},
      fn ->
        case Installer.uninstall(plugin_id) do
          :ok -> IO.puts("Uninstalled plugin #{plugin_id}.")
          {:error, reason} -> Context.cli_error!("Error uninstalling plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  @doc "Enable an installed plugin (routes through the Installer)."
  def installer_enable_plugin(plugin_id, opts_json) do
    Context.with_cli_authorization(
      Context.json_token_opts(opts_json),
      :"plugin.enable",
      "_system",
      %{plugin_id: plugin_id},
      fn ->
        case Installer.enable(plugin_id) do
          :ok -> IO.puts("Plugin #{plugin_id} enabled successfully.")
          {:error, reason} -> Context.cli_error!("Error enabling plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  @doc "Disable an installed plugin (routes through the Installer)."
  def installer_disable_plugin(plugin_id, opts_json) do
    Context.with_cli_authorization(
      Context.json_token_opts(opts_json),
      :"plugin.disable",
      "_system",
      %{plugin_id: plugin_id},
      fn ->
        case Installer.disable(plugin_id) do
          :ok -> IO.puts("Plugin #{plugin_id} disabled successfully.")
          {:error, reason} -> Context.cli_error!("Error disabling plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  @doc """
  List every installed plugin. `format` is `"text" | "json"`.
  """
  def list_installed_plugins(format \\ "text") do
    plugins = Installer.list()

    case format do
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
  def installed_plugin_info(plugin_id) do
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
        IO.puts("Plugin '#{plugin_id}' not installed.")
    end
  end

  @doc """
  Returns `true` when `plugin_id` is tracked by `Flezha.Plugin.Installer`,
  so the CLI's `enable`/`disable` commands know which code path to use.
  """
  def installed_plugin?(plugin_id) do
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
  def plugin_help(provider_id) do
    case get_plugin_provider_mod(provider_id) do
      {:ok, provider_mod} ->
        try do
          help_text = apply(provider_mod, :help, [])
          IO.puts(help_text)
          IO.puts("")
          print_supported_extensions(provider_mod)
        rescue
          UndefinedFunctionError ->
            IO.puts(
              "Help not available for plugin '#{provider_id}'. The plugin provider does not implement help/0."
            )

          e ->
            Context.cli_error!(
              "Error getting help for plugin '#{provider_id}': #{Exception.message(e)}"
            )
        end

      {:error, reason} ->
        Context.cli_error!("Error: #{reason}")
    end
  end

  defp print_supported_extensions(provider_mod) do
    extensions =
      for ext <- [
            :on_operative_init,
            :on_input,
            :on_output,
            :on_interval,
            :on_message,
            :on_snapshot,
            :on_terminate
          ],
          function_exported?(provider_mod, ext, 2),
          do: ext

    IO.puts("SUPPORTED EXTENSIONS:")
    Enum.each(extensions, &IO.puts("    --extension=#{&1}"))
  end

  @doc "List all available plugin providers."
  def list_plugins() do
    enabled_plugins = Config.get_enabled_plugins()

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
