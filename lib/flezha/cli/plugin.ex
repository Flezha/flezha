defmodule Flezha.Service.Cli.Plugin do
  @moduledoc """
  `flz plugin`: enable, disable, help and the provider list, the installed
  package list, and the entry the verbs that place packages run in a VM of their
  own.
  """

  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Error
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
  def enable_plugin(plugin_id, provider_id, _plugin_flags_json, _opts_json, %{row: row}) do
    with {:ok, plugin} <- plugin(plugin_id, row),
         {:ok, outcome} <- PluginManager.enable_plugin(plugin_id, plugin) do
      IO.puts(enabled_message(plugin_id, outcome))
      warn_lacking(row.namespace, provider_id)
    end
  end

  # A provider comes from a package installed per controller, so another
  # controller holding the namespace's hub may lack it. The enable proceeds; an
  # operator placed there parks naming the provider.
  defp warn_lacking(namespace, provider_id) do
    case lacking(Directory.hub_nodes(namespace) -- [node()], provider_id) do
      [] ->
        :ok

      lacking ->
        IO.puts(
          "warning: #{Enum.map_join(lacking, ", ", &to_string/1)} hold namespace " <>
            "'#{namespace}' and lack provider '#{provider_id}' or did not answer; an " <>
            "operator placed there parks"
        )
    end
  end

  @doc "The controllers of `peers` whose provider table has no `provider_id`, or that did not answer."
  @spec lacking([node()], binary()) :: [node()]
  def lacking(peers, provider_id) do
    answers =
      :erpc.multicall(
        peers,
        Providers,
        :fetch,
        [provider_id],
        Durations.get(:plugin_peer_check_ms)
      )

    for {peer, answer} <- Enum.zip(peers, answers), not match?({:ok, {:ok, _}}, answer), do: peer
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

  # A provider id a package declares that this controller did not load names
  # the package and why, rather than being unknown.
  defp get_plugin_provider_mod(provider_id) do
    with :error <- Providers.fetch(provider_id) do
      {:error, Installer.unavailable(provider_id) || {:unknown_provider, provider_id}}
    end
  end

  @doc """
  Disables a plugin — or, when the id was fanned out over a partition group,
  every `#pK` instance of it.
  """
  def disable_plugin(plugin_id, _call) do
    with {:ok, msg} <- PluginManager.disable_plugin_group(plugin_id),
         do: IO.puts("Plugin #{plugin_id} disabled successfully (#{msg}).")
  end

  @doc """
  The entry `flz plugin install`, `uninstall` and `sync` run in a VM of their
  own (`flzvm eval`), never inside a running controller: they act on the plugin
  root alone. Prints the answer and the absolute plugin root, and halts with the
  exit code of the answer's class.
  """
  @spec eval(binary(), [binary()], keyword()) :: no_return()
  def eval(verb, args, opts) do
    Logger.configure(level: :warning)
    {:ok, _} = Application.ensure_all_started([:crypto, :public_key, :ssl])
    format = Context.format(opts)

    answer =
      case {verb, args} do
        {"install", [source]} -> Installer.install(source)
        {"uninstall", [id]} -> Installer.uninstall(id)
        {"sync", []} -> Installer.sync()
      end

    case answer do
      {:ok, done} ->
        IO.puts(placed(verb, done, format))
        System.halt(0)

      {:error, reason} ->
        reason |> Error.from_reason(nil) |> Context.render!(opts)
    end
  end

  defp placed(_verb, done, "json"), do: Jason.encode!(done)

  defp placed("install", done, _text) do
    signed = if done.signed, do: "signed", else: "unsigned"

    "Installed #{done.id} #{done.version} (#{signed}) into #{done.root}.\n" <>
      "Restart the controller to load it." <>
      if(done.warning, do: "\nwarning: #{done.warning}", else: "")
  end

  defp placed("uninstall", done, _text),
    do: "Uninstalled #{done.id} from #{done.root}.\nRestart the controller to unload it."

  defp placed("sync", done, _text) do
    placed = Enum.map(done.placed, &"#{&1.id} #{&1.version}")
    removed = Enum.map(done.removed, &Path.basename/1)

    "Synced #{done.root}: placed #{names(placed)}, kept #{names(done.kept)}, " <>
      "removed #{names(removed)}.\nThe controller loads them at its next start."
  end

  defp names([]), do: "none"
  defp names(names), do: Enum.join(names, ", ")

  @doc """
  Every package this controller loaded or refused at boot, and what its plugin
  root holds since, as text or as the JSON the caller's `output` asks for.
  """
  def list_installed_plugins(%{opts: opts}) do
    rows = Installer.list()

    case Context.format(opts) do
      "json" ->
        rows |> Enum.map(&Map.from_struct/1) |> Jason.encode!() |> IO.puts()

      _ when rows == [] ->
        IO.puts("No plugin packages installed.")

      _ ->
        Enum.each(rows, &IO.puts(installed_line(&1)))
    end
  end

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

    case Enum.count(Installer.list(), &(&1.status == :refused)) do
      0 -> :ok
      n -> IO.puts("#{n} plugin package(s) were refused at boot: flz plugin list --installed")
    end
  end
end
