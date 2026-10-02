defmodule Flezha.Service.Cli.Plugin do
  @moduledoc """
  `flz plugin`: enable, bind, unbind, update, disable, the enabled plugins and
  the provider list, help, the installed package list, and the entry the verbs
  that place packages run in a VM of their own.
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

  # The plain options each verb reads. The verbs are open, so every other plain
  # word a caller types arrives in the same document and is refused.
  @read_options %{
    enable: ["namespace", "operator_id", "channel", "single"],
    bind: ["namespace", "operator_id", "channel"],
    unbind: ["namespace", "operator_id"],
    update: ["namespace"],
    disable: ["namespace"]
  }

  @doc """
  The row a `plugin enable` is judged by: the namespace in the caller's
  options, with the decoded provider, flags and options the body needs. A plain
  option the enable does not read is refused before anything is written.
  """
  def enable_row([_name, provider_id, plugin_flags_json, opts_json | _]) do
    with {:ok, provider_mod} <- get_plugin_provider_mod(provider_id),
         {:ok, row} <- row(:enable, plugin_flags_json, opts_json),
         do: {:ok, Map.put(row, :provider_mod, provider_mod)}
  end

  @doc "The row a `plugin bind` is judged by."
  def bind_row([_name, plugin_flags_json, opts_json | _]),
    do: row(:bind, plugin_flags_json, opts_json)

  @doc "The row a `plugin update` is judged by."
  def update_row([_name, plugin_flags_json, opts_json | _]),
    do: row(:update, plugin_flags_json, opts_json)

  @doc "The row a `plugin unbind` is judged by."
  def unbind_row([_name, opts_json | _]), do: row(:unbind, "{}", opts_json)

  @doc "The row a `plugin disable` is judged by."
  def disable_row([_name, opts_json | _]), do: row(:disable, "{}", opts_json)

  defp row(verb, plugin_flags_json, opts_json) do
    opts = Context.json!(opts_json, "plugin options")

    with :ok <- refuse_unread_options(opts, Map.fetch!(@read_options, verb)) do
      {:ok,
       %{
         namespace: Map.get(opts, "namespace", "default"),
         plugin_flags: Context.json!(plugin_flags_json, "plugin settings"),
         opts: opts
       }}
    end
  end

  @doc """
  Enable a plugin by name, and with `--operator_id` bind it in the same
  command. A bind that fails leaves the plugin enabled, and the enable's line
  says so before the refusal. A controller plugin's hooks are its own, and it
  is bound to no operator.
  """
  def enable_plugin(name, provider_id, _plugin_flags_json, _opts_json, %{row: row}) do
    with {:ok, hooks, provider_opts} <- read_settings(row.plugin_flags),
         entry = %Plugin{
           id: name,
           provider: row.provider_mod,
           namespace: row.namespace,
           opts: provider_opts,
           single: Map.get(row.opts, "single") == "true"
         },
         {:ok, entry} <- with_hooks(entry, hooks, row),
         {:ok, outcome} <- PluginManager.enable_plugin(name, entry) do
      IO.puts(enabled_message(name, outcome))
      warn_lacking(row.namespace, provider_id)

      if entry.operator_id == nil and Map.has_key?(row.opts, "operator_id"),
        do: bind(name, row, hooks),
        else: :ok
    end
  end

  # An operator plugin's hooks say what it sends to an operator, so they belong
  # to a binding.
  defp with_hooks(%Plugin{} = entry, hooks, %{opts: opts} = row) do
    cond do
      Plugin.controller?(entry) ->
        {:ok, %Plugin{entry | hooks: hooks, operator_id: Map.get(opts, "operator_id")}}

      hooks != [] and not is_map_key(opts, "operator_id") ->
        {:error, {:plugin_target_missing, row.namespace, nil}}

      true ->
        {:ok, entry}
    end
  end

  @doc "Bind an operator to an enabled plugin."
  def bind_plugin(name, _plugin_flags_json, _opts_json, %{row: row}) do
    with {:ok, hooks, provider_opts} <- read_settings(row.plugin_flags),
         :ok <- refuse_settings(provider_opts),
         do: bind(name, row, hooks)
  end

  defp bind(name, %{opts: opts} = row, hooks) do
    with {:ok, operator_id} <- operator_of(row) do
      case PluginManager.bind_plugin(row.namespace, name, operator_id,
             hooks: hooks,
             channel: Map.get(opts, "channel")
           ) do
        :ok -> IO.puts("Plugin #{name} bound to #{operator_id}.")
        :already_bound -> IO.puts("Plugin #{name} is already bound to #{operator_id}.")
        {:error, _} = refused -> refused
      end
    end
  end

  @doc "Unbind an operator from a plugin, which stays enabled."
  def unbind_plugin(name, _opts_json, %{row: row}) do
    with {:ok, operator_id} <- operator_of(row),
         :ok <- PluginManager.unbind_plugin(row.namespace, name, operator_id),
         do: IO.puts("Plugin #{name} unbound from #{operator_id}; it stays enabled.")
  end

  @doc """
  Change a plugin's options, and a controller plugin's hooks: in place when
  every changed option is live, otherwise by restarting every running copy,
  with a warning naming the operators bound to it.
  """
  def update_plugin(name, _plugin_flags_json, _opts_json, %{row: row}) do
    with {:ok, hooks, provider_opts} <- read_settings(row.plugin_flags),
         {:ok, %{applied: applied, operators: operators}} <-
           PluginManager.update_plugin(row.namespace, name, provider_opts, hooks) do
      case {applied, operators} do
        {:unchanged, _} ->
          IO.puts("Plugin #{name} is unchanged.")

        {:live, _} ->
          IO.puts("Plugin #{name} updated in place.")

        {:restarted, []} ->
          IO.puts("Plugin #{name} updated; its running copies restarted.")

        {:restarted, operators} ->
          IO.puts("Plugin #{name} updated; its running copies restarted.")
          IO.puts("warning: restarted under #{Enum.join(operators, ", ")}")
      end
    end
  end

  defp operator_of(%{opts: %{"operator_id" => operator_id}}) when operator_id != "",
    do: {:ok, operator_id}

  defp operator_of(row), do: {:error, {:plugin_target_missing, row.namespace, nil}}

  # A bind names a plugin and how one operator is bound to it; the plugin's
  # own settings are the enable's and the update's.
  defp refuse_settings(provider_opts) when provider_opts == %{}, do: :ok

  defp refuse_settings(provider_opts),
    do: {:error, {:unread_plugin_options, Enum.map(Map.keys(provider_opts), &"X#{&1}")}}

  # A provider comes from a package installed per controller, so another
  # controller holding the namespace's hub may lack it. The enable proceeds;
  # the plugin does not run there, and an operator bound to it there runs
  # without it.
  defp warn_lacking(namespace, provider_id) do
    case lacking(Directory.hub_nodes(namespace) -- [node()], provider_id) do
      [] ->
        :ok

      lacking ->
        IO.puts(
          "warning: #{Enum.map_join(lacking, ", ", &to_string/1)} hold namespace " <>
            "'#{namespace}' and lack provider '#{provider_id}' or did not answer; the " <>
            "plugin will not run there, and an operator bound to it there runs without it"
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

  defp enabled_message(name, :already_enabled) do
    "Plugin #{name} is already enabled; the existing plugin was left " <>
      "untouched (options were not compared)."
  end

  defp enabled_message(name, _msg), do: "Plugin #{name} enabled."

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

  # Dropping a word the caller typed would act with less than was written and
  # still report success.
  defp refuse_unread_options(opts, read) do
    case opts |> Map.drop(read) |> Map.keys() |> Enum.sort() do
      [] -> :ok
      unread -> {:error, {:unread_plugin_options, Enum.map(unread, &"--#{&1}")}}
    end
  end

  # A provider id a package declares that this controller did not load names
  # the package and why, rather than being unknown.
  defp get_plugin_provider_mod(provider_id) do
    with :error <- Providers.fetch(provider_id) do
      {:error, Installer.unavailable(provider_id) || {:unknown_provider, provider_id}}
    end
  end

  @doc "Disable a plugin: every running copy stops, every binding stays."
  def disable_plugin(name, _opts_json, %{row: row}) do
    with {:ok, _msg} <- PluginManager.disable_plugin(row.namespace, name),
         do: IO.puts("Plugin #{name} disabled.")
  end

  @doc """
  The plugins enabled in `namespace`: each one's provider, whether it is
  `single`, its bound operators, and the controllers it runs on or `idle`.
  """
  def list_enabled_plugins(namespace, %{opts: opts}) do
    rows = PluginManager.plugins(Flezha.AuthZ.Namespace.or_default(namespace))

    case Context.format(opts) do
      "json" -> IO.puts(Jason.encode!(rows))
      _ when rows == [] -> IO.puts("No plugins enabled.")
      _ -> Enum.each(rows, &IO.puts(enabled_line(&1)))
    end
  end

  defp enabled_line(row) do
    single = if row.single, do: " single", else: ""
    where = if row.controllers == [], do: "idle", else: Enum.join(row.controllers, ", ")
    bound = if row.operators == [], do: "none", else: Enum.join(row.operators, ", ")
    "#{row.name}  #{row.provider}#{single}  bound: #{bound}  runs on: #{where}" <> hooks(row)
  end

  # A controller plugin's own hooks, and where its copy falls behind.
  defp hooks(%{hooks: []}), do: ""

  defp hooks(%{hooks: hooks, lagging: lagging}) do
    behind = if lagging == [], do: "", else: "  lagging on: #{Enum.join(lagging, ", ")}"
    "  hooks: #{Enum.join(hooks, ", ")}" <> behind
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
          IO.puts("Type: #{provider_mod.plugin_type()}")
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
  def list_providers(_call) do
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
