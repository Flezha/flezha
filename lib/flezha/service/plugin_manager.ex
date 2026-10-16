defmodule Flezha.Service.PluginManager do
  @moduledoc """
  Named plugins and their bindings. A plugin is enabled by name in a namespace
  and recorded as its own `{:plugin, name}` entry in the namespace hub's
  declared set; a relay is bound to it by a binding on the relay's own
  entry. Both are written by the start authority, so a plugin and every binding
  survive an Overwatch restart, a controller restart and a cluster cold start,
  and nothing is kept per node.

  A namespace's records are read and written on a node that hosts its hub: this
  one when it does, otherwise one that does, under a bound. The order is
  validate, then durable write, then runtime: a refused enable or bind leaves
  nothing, and a failed runtime subscribe rolls its binding back.

  What runs where is `Flezha.Relay.Plugins`' and `Flezha.Plugin.Shared`'s:
  this module writes intent and asks the running copies to converge on it.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.Cluster.Directory
  alias Flezha.Config.Backoff
  alias Flezha.Config.Durations
  alias Flezha.Convergence.Probe
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Parallel
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Home
  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.ProviderBehaviour.Decl
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias Flezha.Plugin.Providers
  alias Flezha.Plugin.Shared
  alias Flezha.Process.Cspec
  alias Flezha.Service.CallerContext
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayApi
  alias Flezha.Service.RelayManager
  alias Flezha.Service.RelayManager.Init
  alias Flezha.Service.RelayManager.Teardown
  alias Flezha.Service.RegistryUpdate

  ## Enable

  @doc """
  Enable `name` in `plugin.namespace` with the provider and options `plugin`
  carries. Nothing starts: a plugin runs where a relay bound to it runs.
  Answers `{:ok, :already_enabled}` for a name enabled with the same provider,
  and refuses another provider with `plugin.id_taken`.

  With `plugin.relay_id` set it also binds that relay with `plugin.hooks`
  and `channel`, in one call; a bind that fails answers its refusal and leaves
  the plugin enabled.
  """
  @spec enable_plugin(binary(), Plugin.t(), binary() | nil) ::
          {:ok, binary() | :already_enabled} | {:error, term()}
  def enable_plugin(name, %Plugin{} = plugin, channel \\ nil) when is_binary(name) do
    namespace = to_string(plugin.namespace || "default")
    plugin = %Plugin{plugin | id: name, namespace: namespace}

    with {:ok, entry} <- entry_of(plugin),
         :ok <- hub_for(plugin),
         {:ok, enabled} <-
           on_hub_node(namespace, :declare_entry, [entry], {:hub_down, namespace}) do
      bind_after_enable(plugin, enabled, channel)
    end
  end

  # A provider bound to one namespace may be bound to one no runner serves
  # (`_system`, which no runner can join), whose hub is reclaimed as vacant
  # once nothing is declared in it: this node starts the hub its entry is
  # written to. Every other enable needs a hub its runners brought up.
  defp hub_for(%Plugin{provider: provider, namespace: namespace}) do
    with %{namespace: ^namespace} <- provider.__provider__(),
         [] <- Directory.hub_nodes(namespace),
         {:error, _reason} <- HubManager.ensure_hub(namespace) do
      {:error, :registry_unavailable}
    else
      _hub -> :ok
    end
  end

  # A controller plugin's relay is its entry's own, not a binding.
  defp bind_after_enable(%Plugin{} = plugin, enabled, channel) do
    if plugin.relay_id == nil or Plugin.controller?(plugin) do
      {:ok, if(enabled == :already_enabled, do: :already_enabled, else: "plugin enabled")}
    else
      plugin.namespace
      |> bind_plugin(plugin.id, plugin.relay_id, hooks: plugin.hooks, channel: channel)
      |> case do
        :already_bound when enabled == :already_enabled -> {:ok, :already_enabled}
        {:error, _} = refused -> refused
        _bound -> {:ok, "plugin enabled"}
      end
    end
  end

  # A relay plugin's entry is the plugin without a relay or hooks, its
  # options checked against the provider's declaration as any binding of it may
  # hook it: an enable names no relay, so the provider's own preparation runs
  # where a binding brings the plugin up. A controller plugin's keeps its hooks.
  defp entry_of(%Plugin{provider: provider, opts: opts} = plugin) do
    plugin = %Plugin{plugin | opts: opts || %{}}

    with :ok <- in_namespace(plugin),
         {:ok, entry} <- checked_entry(plugin, Plugin.controller?(plugin)),
         do: {:ok, Plugin.strip_transient(entry)}
  rescue
    UndefinedFunctionError -> {:error, not_loaded(provider)}
  end

  defp in_namespace(%Plugin{provider: provider, namespace: namespace}) do
    case provider.__provider__() do
      %{namespace: only, name: name} when only not in [nil, namespace] ->
        {:error, {:plugin_namespace_refused, name, only}}

      _any ->
        :ok
    end
  end

  defp checked_entry(plugin, true), do: Shared.checked_entry(plugin)

  defp checked_entry(%Plugin{provider: provider} = plugin, false) do
    bindable = %{subscribed_extensions: Decl.io_hooks(provider.__provider__())}

    with :ok <- refuse_init_hook(plugin),
         {:ok, _normalised} <- Options.normalise_opts(provider, Map.merge(plugin.opts, bindable)),
         do: {:ok, %Plugin{plugin | relay_id: nil, hooks: []}}
  end

  defp refuse_init_hook(%Plugin{hooks: hooks}) do
    if Enum.any?(hooks, &(elem(&1, 0) in [:on_relay_init, "on_relay_init"])),
      do: {:error, :init_plugin_after_start},
      else: :ok
  end

  @doc false
  # On a node hosting the namespace's hub.
  @spec declare_entry(Plugin.t()) :: {:ok, :enabled | :already_enabled} | {:error, term()}
  def declare_entry(%Plugin{id: name, namespace: namespace} = entry) do
    case HubManager.plugin_entry(namespace, name) do
      {:ok, %Plugin{provider: provider}} when provider == entry.provider ->
        {:ok, :already_enabled}

      {:ok, _other} ->
        {:error, {:plugin_id_taken, name, namespace}}

      :error ->
        start_home(entry)
    end
  end

  defp start_home(%Plugin{id: name, namespace: namespace} = entry) do
    spec =
      Cspec.build({:plugin, name}, {Home, :start_link, [{namespace, entry}]}, :worker_permanent)

    case Init.start_declared(namespace, spec, %{}, {:plugin, name}) do
      %ProcessHub.StartResult{started: [{_, [_ | _]} | _]} ->
        await_entry(namespace, name, 0)
        # A controller plugin's home answers once it has placed its copies.
        if Plugin.controller?(entry), do: Home.serialized(namespace, name, fn -> :ok end)
        _ = reconverge(namespace, name, &Overwatch.plugin_subscribe(&1, &2, &3, entry))
        {:ok, :enabled}

      # Two enables of one name raced and the other one won.
      _not_started ->
        case HubManager.plugin_entry(namespace, name) do
          {:ok, _} -> declare_entry(entry)
          :error -> {:error, {:plugin_enable_failed, name, namespace}}
        end
    end
  end

  # The hub answers a start once its child runs where it was placed; this
  # node's rows learn of it a moment later. An enable answers once the plugin
  # is readable here, so a bind that follows it finds it.
  defp await_entry(namespace, name, attempt) do
    if HubManager.plugin_entry(namespace, name) != :error or
         Backoff.give_up?(:no_response, attempt) do
      :ok
    else
      Backoff.wait(:no_response, attempt + 1, {:plugin, name})
      await_entry(namespace, name, attempt + 1)
    end
  end

  ## Bind

  @doc """
  Bind `relay_id` (or every member of the partition group it names) to the
  plugin `name`, with the `:hooks` and the `:channel` in `fields`. Answers
  `:already_bound` for a relay that holds the binding.
  """
  @spec bind_plugin(binary(), binary(), binary(), keyword()) ::
          :ok | :already_bound | {:error, term()}
  def bind_plugin(namespace, name, relay_id, fields \\ []) do
    binding = %Plugin{
      id: name,
      provider: nil,
      namespace: namespace,
      relay_id: relay_id,
      hooks: Keyword.get(fields, :hooks, []),
      opts: channel_opts(Keyword.get(fields, :channel))
    }

    on_hub_node(namespace, :bind_here, [binding], {:plugin_not_found, name})
  end

  defp channel_opts(channel), do: if(channel in [nil, ""], do: %{}, else: %{channel: channel})

  @doc false
  @spec bind_here(Plugin.t()) :: :ok | :already_bound | {:error, term()}
  def bind_here(%Plugin{namespace: namespace, relay_id: relay_id} = binding) do
    targets = targets(namespace, relay_id)

    with :ok <- refuse_init_hook(binding),
         {:ok, entry} <- fetch_entry(namespace, binding.id) do
      one_holder(entry, fn ->
        with {:ok, binding} <- checked_binding(binding, targets, nil) do
          targets
          |> Parallel.map(fn {member_id, identity} ->
            member = member_binding(binding, member_id, identity)
            {member_id, bind_one(namespace, member_id, member, entry)}
          end)
          |> settle_targets(:group_enable_partial)
        end
      end)
    end
  end

  # A plugin that cannot be shared has one holder, so its binds are checked
  # and written one at a time, in its home.
  defp one_holder(%Plugin{namespace: namespace, id: name} = entry, bind),
    do: if(shareable?(entry), do: bind.(), else: Home.serialized(namespace, name, bind))

  # A provider this controller did not load cannot say, and is held to one.
  defp shareable?(%Plugin{provider: provider}),
    do: Code.ensure_loaded?(provider) and provider.__provider__().shareable

  # The binding with its hooks canonical and its plugin's provider, or the
  # refusal its plugin's entry or its relay's announced channels give it; a
  # relay not declared yet announces `factory_id`'s.
  defp checked_binding(%Plugin{id: name, namespace: namespace} = binding, targets, factory_id) do
    with {:ok, entry} <- fetch_entry(namespace, name),
         binding = %Plugin{binding | provider: entry.provider},
         {:ok, binding} <- Table.canonicalise(Overwatch.__extensions_catalog__(), binding),
         :ok <- refuse_binding(entry, binding, targets),
         channel = Plugin.entry_channel(binding, entry),
         :ok <- RelayApi.refuse_unknown_entry(namespace, binding.relay_id, channel, factory_id),
         do: {:ok, binding}
  end

  @doc """
  The bindings a relay of `factory_id` starting with `--plugins` specs
  carries: each spec that names a provider enables its plugin when it is not
  enabled yet, and each is checked as a bind of `plugins`' relay would be, an
  undeclared relay against the channels `factory_id` announces. A spec that
  only binds may name the binding's `channel` and nothing else in its options.
  """
  @spec start_bindings(binary(), binary() | nil, [Plugin.t()]) ::
          {:ok, [Plugin.t()]} | {:error, term()}
  def start_bindings(namespace, factory_id, plugins) do
    Enum.reduce_while(plugins, {:ok, []}, fn plugin, {:ok, acc} ->
      case start_binding(namespace, factory_id, plugin) do
        {:ok, binding} -> {:cont, {:ok, acc ++ [binding]}}
        {:error, _} = refused -> {:halt, refused}
      end
    end)
  end

  defp start_binding(namespace, factory_id, %Plugin{provider: nil, opts: opts} = plugin) do
    case Map.keys(opts) -- [:channel] do
      [] ->
        args = [%Plugin{plugin | namespace: namespace}, factory_id]
        on_hub_node(namespace, :checked_start_binding, args, {:plugin_not_found, plugin.id})

      unread ->
        {:error, {:unread_plugin_options, Enum.map(unread, &"opts.#{&1}")}}
    end
  end

  defp start_binding(namespace, factory_id, %Plugin{id: name} = plugin) do
    with {:ok, _enabled} <-
           enable_plugin(name, %Plugin{plugin | namespace: namespace, relay_id: nil}) do
      start_binding(namespace, factory_id, %Plugin{plugin | provider: nil, opts: %{}})
    end
  end

  @doc false
  def checked_start_binding(%Plugin{relay_id: relay_id} = binding, factory_id),
    do: checked_binding(binding, [{relay_id, nil}], factory_id)

  # A group id names no relay — it names N of them, each bound under its
  # `#pK` id with its partition options.
  @doc false
  def targets(namespace, relay_id) do
    case PartitionGroup.members(namespace, relay_id) do
      [] -> [{relay_id, nil}]
      members -> Enum.map(members, &{&1.id, &1.partition})
    end
  end

  defp member_binding(binding, _member_id, nil), do: binding

  defp member_binding(binding, member_id, identity),
    do: Plugin.for_partition(binding, member_id, identity)

  defp refuse_binding(%Plugin{id: name} = entry, binding, targets) do
    cond do
      Plugin.controller?(entry) ->
        {:error, {:plugin_not_bindable, name}}

      entry.single and Enum.any?(binding.hooks, &(elem(&1, 0) == :on_output)) ->
        {:error, {:plugin_single_output, name}}

      shareable?(entry) ->
        :ok

      holder = other_holder(entry, targets) ->
        {:error, {:plugin_not_shareable, name, holder}}

      true ->
        :ok
    end
  end

  # The relay, or the group, already bound to a plugin that cannot be
  # shared. Members of one group are one holder.
  defp other_holder(%Plugin{id: name, namespace: namespace}, targets) do
    own = MapSet.new(targets, &Plugin.group_of(elem(&1, 0)))

    namespace
    |> bound_relays(name)
    |> Enum.map(&Plugin.group_of/1)
    |> Enum.reject(&MapSet.member?(own, &1))
    |> List.first()
  end

  defp bind_one(namespace, relay_id, %Plugin{id: id} = binding, entry) do
    case HubManager.relay_bindings(namespace, relay_id) do
      {:ok, bindings} ->
        if List.keymember?(bindings, id, 0),
          do: :already_bound,
          else: write_binding(namespace, relay_id, binding, entry)

      {:error, _} = refused ->
        refused
    end
  end

  # The relay is handed the plugin's entry with the binding: its own node's
  # rows may not have heard of a plugin enabled a moment ago.
  defp write_binding(namespace, relay_id, %Plugin{id: id} = binding, entry) do
    stored = Plugin.strip_transient(binding)

    with :ok <-
           update_cspec_plugins(namespace, relay_id, RegistryUpdate.add_plugin(id, stored)) do
      case subscribe(namespace, relay_id, id, binding, entry) do
        :ok ->
          :ok

        refused ->
          _ = update_cspec_plugins(namespace, relay_id, RegistryUpdate.remove_plugin(id))
          refused
      end
    end
  end

  defp subscribe(namespace, relay_id, id, binding, entry) do
    with {:ok, pid} <- RelayManager.get_relay_pid(namespace, relay_id),
         do: Overwatch.plugin_subscribe(pid, id, binding, entry)
  end

  ## Unbind

  @doc """
  Remove `relay_id`'s binding to `name` (from every member of the group it
  names). The plugin stays enabled.
  """
  @spec unbind_plugin(binary(), binary(), binary()) :: :ok | {:error, term()}
  def unbind_plugin(namespace, name, relay_id),
    do:
      on_hub_node(
        namespace,
        :unbind_here,
        [namespace, name, relay_id],
        {:plugin_not_bound, name, relay_id}
      )

  @doc false
  @spec unbind_here(binary(), binary(), binary()) :: :ok | {:error, term()}
  def unbind_here(namespace, name, relay_id) do
    held =
      for {member_id, _identity} <- targets(namespace, relay_id),
          {:ok, bindings} <- [HubManager.relay_bindings(namespace, member_id)],
          {id, _binding} <- bindings,
          Plugin.base_id(id) == name,
          do: {member_id, id}

    case held do
      [] ->
        {:error, {:plugin_not_bound, name, relay_id}}

      held ->
        held
        |> Parallel.map(fn {member_id, id} ->
          case unbind_one(namespace, member_id, id) do
            {:not_unsubscribed, _} -> {member_id, :ok}
            outcome -> {member_id, outcome}
          end
        end)
        |> settle_targets(:group_disable_partial)
    end
  end

  # The durable write decides the unbind; a relay that does not answer its
  # unsubscribe drops the binding at its next bring-up, and is named.
  defp unbind_one(namespace, relay_id, id) do
    case update_cspec_plugins(namespace, relay_id, RegistryUpdate.remove_plugin(id)) do
      :ok ->
        case RelayManager.get_relay_pid(namespace, relay_id) do
          {:ok, pid} -> unsubscribed(Overwatch.plugin_unsubscribe(pid, id))
          {:error, _gone} -> :ok
        end

      {:error, {:relay_not_found, _, _}} ->
        :ok

      {:error, _} = refused ->
        refused
    end
  end

  defp unsubscribed(:ok), do: :ok
  defp unsubscribed({:error, reason}), do: {:not_unsubscribed, reason}

  # Every member's outcome is reported: a partial bind or unbind is a real
  # state and must never read as success.
  defp settle_targets([{_member, one}], _partial), do: one

  defp settle_targets(outcomes, partial) do
    case Enum.reject(outcomes, &(elem(&1, 1) in [:ok, :already_bound])) do
      [] ->
        if Enum.all?(outcomes, &(elem(&1, 1) == :already_bound)), do: :already_bound, else: :ok

      [{_member, refused} | _] = failed when length(failed) == length(outcomes) ->
        refused

      failed ->
        {:error, partial(partial, length(outcomes), Enum.map(failed, &elem(&1, 0)))}
    end
  end

  defp partial(:group_enable_partial, total, failed),
    do: {:group_enable_partial, total - length(failed), total, failed}

  defp partial(:group_disable_partial, _total, failed), do: {:group_disable_partial, failed}

  @doc """
  Remove every binding of `relay_ids`, in one bounded fan-out: a partition
  rescale turns the old members' sources off this way. Answers `:ok`, or
  `{:error, {:undisabled, relay_ids}}` naming the ones it could not reach.
  """
  @spec unbind_all(binary(), [binary()]) :: :ok | {:error, {:undisabled, [binary()]}}
  def unbind_all(namespace, relay_ids) do
    outcomes =
      Parallel.results(
        relay_ids,
        fn relay_id ->
          {:ok, bindings} = HubManager.relay_bindings(namespace, relay_id)
          bindings |> Enum.map(&unbind_one(namespace, relay_id, elem(&1, 0))) |> Enum.uniq()
        end,
        timeout: Durations.get(:plugin_disable_fanout_ms)
      )

    undisabled = for {id, outcome} <- outcomes, outcome not in [{:ok, []}, {:ok, [:ok]}], do: id
    if undisabled == [], do: :ok, else: {:error, {:undisabled, undisabled}}
  end

  @doc """
  Bind back what `unbind_all/2` removed, given each relay's bindings as
  `HubManager.relay_bindings/2` answered them before the unbind: a rescale
  that cannot complete its quiesce restores its group's plugins this way.
  Answers `:ok`, or `{:error, {:unrebound, relay_ids}}`.
  """
  @spec rebind_all(binary(), [{binary(), [{binary(), Plugin.t()}]}]) ::
          :ok | {:error, {:unrebound, [binary()]}}
  def rebind_all(namespace, saved) do
    outcomes =
      Parallel.results(saved, &rebind_relay(namespace, &1),
        timeout: Durations.get(:plugin_disable_fanout_ms)
      )

    unrebound = for {{relay_id, _}, outcome} <- outcomes, not rebound?(outcome), do: relay_id
    if unrebound == [], do: :ok, else: {:error, {:unrebound, unrebound}}
  end

  # A member's binding id carries its partition suffix; the entry is enabled
  # under the base name.
  defp rebind_relay(namespace, {relay_id, bindings}) do
    for {id, binding} <- bindings do
      with {:ok, entry} <- fetch_entry(namespace, Plugin.base_id(id)),
           do: bind_one(namespace, relay_id, binding, entry)
    end
  end

  defp rebound?({:ok, results}), do: Enum.all?(results, &(&1 in [:ok, :already_bound]))
  defp rebound?(_failed), do: false

  ## Update

  @doc """
  Change `name`'s options, and a controller plugin's hooks when `hooks` names
  any. When every changed option is declared live, each running copy applies
  them in place; otherwise each restarts, and an update that changes none is
  `:unchanged`. Answers how it was applied and the relays bound to it.
  """
  @spec update_plugin(binary(), binary(), map(), [Plugin.hook()]) ::
          {:ok, %{applied: :live | :restarted | :unchanged, relays: [binary()]}}
          | {:error, term()}
  def update_plugin(namespace, name, opts, hooks \\ []),
    do:
      on_hub_node(
        namespace,
        :update_here,
        [namespace, name, opts, hooks],
        {:plugin_not_found, name}
      )

  @doc false
  def update_here(namespace, name, opts, hooks) do
    with {:ok, %Plugin{} = old} <- fetch_entry(namespace, name),
         {:ok, hooks} <- updated_hooks(old, hooks),
         opts = Map.merge(old.opts, Plugin.option_keys(old.provider, opts)),
         {:ok, new} <- entry_of(%Plugin{old | opts: opts, hooks: hooks}) do
      case change(old, new) do
        :none -> {:ok, %{applied: :unchanged, relays: bound_relays(namespace, name)}}
        mode -> apply_update(namespace, name, new, mode)
      end
    end
  end

  # Each bound relay's entry channel is checked before anything is rewritten.
  defp apply_update(namespace, name, new, mode) do
    with :ok <-
           Enum.find_value(bound_to(all_bindings(namespace), name), :ok, fn {relay_id, _, binding} ->
             channel = Plugin.entry_channel(binding, new)
             refused = RelayApi.refuse_unknown_entry(namespace, relay_id, channel)

             if refused != :ok, do: refused
           end),
         :ok <-
           update_cspec_plugins(namespace, {:plugin, name}, RegistryUpdate.add_plugin(name, new)) do
      Enum.each(Directory.plugin_hosts(namespace, name), &Shared.update(&1, new, mode))
      relays = bound_relays(namespace, name)
      _ = reconverge(namespace, name, &Overwatch.plugin_update(&1, &2, &3, mode, new))
      {:ok, %{applied: if(mode == :live, do: :live, else: :restarted), relays: relays}}
    end
  end

  # A relay plugin's hooks are its bindings'.
  defp updated_hooks(%Plugin{hooks: hooks}, []), do: {:ok, hooks}

  defp updated_hooks(%Plugin{} = entry, hooks) do
    if Plugin.controller?(entry),
      do: {:ok, hooks},
      else: {:error, {:unread_plugin_options, ["--hook"]}}
  end

  # A copy takes changed hooks only by a restart.
  defp change(%Plugin{provider: provider} = old, new) do
    live = Decl.live_options(provider.__provider__())
    changed = for {key, value} <- new.opts, Map.get(old.opts, key) != value, do: key

    cond do
      new.hooks != old.hooks -> :restart
      changed == [] -> :none
      Enum.all?(changed, &(&1 in live)) -> :live
      true -> :restart
    end
  end

  ## Disable

  @doc """
  Disable `name`: its entry leaves the declared set, every running copy stops,
  and every binding stays on its relay, reported not started until a plugin
  of that name is enabled again.
  """
  @spec disable_plugin(binary(), binary()) :: {:ok, binary()} | {:error, term()}
  def disable_plugin(namespace, name),
    do: on_hub_node(namespace, :disable_here, [namespace, name], {:plugin_not_found, name})

  @doc false
  def disable_here(namespace, name) do
    timeout = Durations.get(:hub_start_child_ms)

    with {:ok, _entry} <- fetch_entry(namespace, name),
         :ok <- Teardown.hub_stop_children(namespace, [{:plugin, name}], timeout) do
      Enum.each(Directory.plugin_hosts(namespace, name), &Shared.stop/1)

      case reconverge(namespace, name, &Overwatch.plugin_subscribe(&1, &2, &3, :none)) do
        [] -> {:ok, "plugin disabled"}
        undisabled -> {:error, {:undisabled, undisabled}}
      end
    end
  end

  ## Listeners

  @doc "Unbind and disable the plugin a listener enabled for itself on `relay_id`."
  @spec detach(binary(), binary(), binary()) :: :ok | {:error, term()}
  def detach(namespace, relay_id, name) do
    _ = unbind_plugin(namespace, name, relay_id)

    case disable_plugin(namespace, name) do
      {:ok, _} -> :ok
      {:error, {:plugin_not_found, _}} -> :ok
      {:error, _} = refused -> refused
    end
  end

  ## Reading

  @doc """
  Every plugin enabled in `namespace`: its name, its provider, whether it is
  `single`, the relays bound to it, and the controllers it runs on; for a
  controller plugin, its hooks and the controllers whose copy lags.
  """
  @spec plugins(binary()) :: [map()]
  def plugins(namespace) do
    with {:error, _} <- on_hub_node(namespace, :plugins_here, [namespace], :none), do: []
  end

  @doc false
  def plugins_here(namespace) do
    bound = all_bindings(namespace)
    running = Probe.plugins_running(namespace, relays_of(bound))

    for %Plugin{id: name} = entry <- HubManager.plugin_entries(namespace) do
      %{
        name: name,
        provider: provider_name(entry.provider),
        provider_id: provider_id(entry.provider),
        single: entry.single,
        relays: bound |> bound_to(name) |> relays_of() |> Enum.sort(),
        hooks: Enum.map(entry.hooks, &to_string(elem(&1, 0)))
      }
      |> Map.merge(Shared.running_on(%Plugin{entry | namespace: namespace}, running))
    end
  end

  # The id the provider is registered under, which `Fleet.provider/2` and an
  # enable take, and its name: `nil` and its module's for one not loaded here.
  defp provider_id(provider),
    do: Enum.find_value(Providers.all(), fn {id, module} -> module == provider && id end)

  defp provider_name(provider) do
    if Code.ensure_loaded?(provider), do: provider.__provider__().name, else: inspect(provider)
  end

  defp bound_relays(namespace, name),
    do: namespace |> all_bindings() |> bound_to(name) |> relays_of()

  # Every binding in the namespace as `{relay_id, id, binding}`: the
  # registry rows, and the declared set that holds a moving relay's
  # bindings while its row is between its two homes.
  defp all_bindings(namespace) do
    for {relay_id, bindings} <-
          HubManager.relay_bindings(namespace) ++ HubManager.declared_bindings(namespace),
        {id, binding} <- bindings,
        uniq: true,
        do: {relay_id, id, binding}
  end

  defp bound_to(bindings, name), do: Enum.filter(bindings, &(Plugin.base_id(elem(&1, 1)) == name))
  defp relays_of(bindings), do: bindings |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

  @doc """
  `binding` laid over its plugin's entry, or why it cannot come up here: its
  plugin is not enabled, or its provider is not loaded on this controller.
  The entry is `entry` when the caller holds it (`:none` when it knows there is
  none), and read on this node otherwise (a relay's node hosts its
  namespace's hub).
  """
  @spec resolve(binary() | atom(), Plugin.t(), Plugin.t() | :none | :read) ::
          {:ok, Plugin.t()} | {:not_started, term()}
  def resolve(namespace, %Plugin{id: id} = binding, entry \\ :read) do
    namespace = to_string(namespace)
    name = Plugin.base_id(id)

    case known_entry(entry, namespace, name) do
      :error ->
        {:not_started, {:plugin_not_found, name}}

      {:ok, %Plugin{provider: provider} = entry} ->
        if Code.ensure_loaded?(provider),
          do: {:ok, %Plugin{Plugin.resolve(binding, entry) | namespace: namespace}},
          else: {:not_started, not_loaded(provider)}
    end
  end

  defp not_loaded(provider),
    do: Installer.unavailable(provider) || {:module_not_loaded, provider, node()}

  defp known_entry(:read, namespace, name), do: HubManager.plugin_entry(namespace, name)
  defp known_entry(:none, _namespace, _name), do: :error
  defp known_entry(%Plugin{} = entry, _namespace, _name), do: {:ok, entry}

  defp fetch_entry(namespace, name) do
    with :error <- HubManager.plugin_entry(namespace, name),
         do: {:error, {:plugin_not_found, name}}
  end

  # Ask every relay bound to `name` to bring its binding to what the
  # plugin's entry now says: up after an enable, not started after a disable.
  # One bound for the whole walk; answers the relays it could not reach.
  defp reconverge(namespace, name, ask) do
    namespace
    |> all_bindings()
    |> bound_to(name)
    |> Parallel.results(
      fn {relay_id, id, binding} ->
        with {:ok, pid} <- RelayManager.get_relay_pid(namespace, relay_id),
             do: ask.(pid, id, binding)
      end,
      timeout: Durations.get(:plugin_disable_fanout_ms)
    )
    |> Enum.reject(fn {_target, outcome} ->
      match?({:ok, :ok}, outcome) or match?({:ok, {:error, {:relay_not_found, _, _}}}, outcome)
    end)
    |> Enum.map(fn {{relay_id, _id, _binding}, _outcome} -> relay_id end)
  end

  @doc """
  Persist a value a running provider committed onto the record it starts from:
  `owner` is a relay, or `{:plugin, name}` for a shared plugin's copy.
  """
  @spec persist_opt(binary() | atom(), binary() | {:plugin, binary()}, binary(), atom(), term()) ::
          :ok | {:error, term()}
  def persist_opt(namespace, owner, id, key, value),
    do: update_cspec_plugins(namespace, owner, RegistryUpdate.put_plugin_opt(id, key, value))

  ## The durable write

  defp update_cspec_plugins(namespace, child_id, update_fn),
    do: Init.write_plugins(namespace, child_id, RegistryUpdate.rewrite_cspec_plugins(update_fn))

  # A namespace's records are written and read on a node that hosts its hub:
  # this one when it does, otherwise one that does, under a bound. With no hub
  # anywhere nothing is enabled or bound there, which `none` says. `function`
  # is this module's, or a `{module, function}` reading the same records.
  @doc false
  def on_hub_node(namespace, function, args, none) do
    {module, function} = if is_atom(function), do: {__MODULE__, function}, else: function

    case Directory.hub_nodes(namespace) do
      [] ->
        {:error, none}

      hub_nodes ->
        if node() in hub_nodes,
          do: apply(module, function, args),
          else: remote(hd(hub_nodes), module, function, args)
    end
  end

  defp remote(hub_node, module, function, args) do
    :erpc.call(hub_node, module, function, args, Durations.get(:route_call_timeout_ms))
  catch
    _kind, _reason -> {:error, :registry_unavailable}
  end

  # ---------- Published surface: each function is judged in the plugin's own
  # namespace, which `fields` carries and the door stamps from a connection.

  @plugin_name [type: :string, doc: "The plugin's name."]
  @fields [type: :keyword, default: [], doc: "The plugin's fields."]

  @doc "Every plugin enabled in the caller's namespace."
  @spec list(CallerContext.t()) :: {:ok, [map()]} | {:error, term()}
  verb list(caller_context), verb: :plugin_list do
    {:ok, plugins(Map.get(caller_context, :namespace) || "default")}
  end

  @doc "Every plugin enabled in `namespace`, as `list/1` answers for the caller's own."
  @spec enabled(String.t(), CallerContext.t()) :: {:ok, [map()]} | {:error, term()}
  verb enabled(namespace, caller_context),
    verb: :plugin_enabled,
    args: [namespace: [type: :string, doc: "The namespace."]] do
    {:ok, plugins(Flezha.AuthZ.Namespace.or_default(namespace))}
  end

  @doc """
  Enable the plugin `name` built from `fields` (`:provider`, `:opts`,
  `:namespace`, `:single`, and `:relay_id`, `:hooks` and `:channel` to bind
  in the same call), answering how it was enabled and the controllers holding
  the namespace that lack its provider: the plugin does not run there.
  """
  @spec enable(String.t(), keyword(), CallerContext.t()) :: {:ok, map()} | {:error, term()}
  verb enable(name, fields, caller_context) when is_binary(name) and is_list(fields),
    verb: :plugin_enable,
    row: &fields_row/1,
    args: [name: @plugin_name, fields: @fields] do
    {channel, fields} = Keyword.pop(fields, :channel)

    with {:ok, plugin} <- Plugin.from_fields([{:id, name} | fields]),
         :ok <- Plugin.targeted(plugin),
         {:ok, outcome} <- enable_plugin(name, plugin, channel) do
      {:ok,
       %{
         name: name,
         outcome: if(outcome == :already_enabled, do: :already_enabled, else: :enabled),
         bound: if(Plugin.controller?(plugin), do: nil, else: plugin.relay_id),
         lacking: lacking(plugin.namespace, fields[:provider])
       }}
    end
  end

  @doc "Bind `fields[:relay_id]` to the plugin `name` with `:hooks` and `:channel`."
  @spec bind(String.t(), keyword(), CallerContext.t()) :: term() | {:error, term()}
  verb bind(name, fields, caller_context) when is_binary(name) and is_list(fields),
    verb: :plugin_bind,
    row: &fields_row/1,
    args: [name: @plugin_name, fields: @fields] do
    with {:ok, relay_id} <- Plugin.binding_target(fields, [:hooks, :channel]),
         {:ok, hooks} <- Plugin.optional_hooks(fields[:hooks]),
         do:
           bind_plugin(namespace(fields), name, relay_id,
             hooks: hooks,
             channel: fields[:channel]
           )
  end

  @doc "Unbind `fields[:relay_id]` from the plugin `name`, which stays enabled."
  @spec unbind(String.t(), keyword(), CallerContext.t()) :: term() | {:error, term()}
  verb unbind(name, fields, caller_context) when is_binary(name) and is_list(fields),
    verb: :plugin_unbind,
    row: &fields_row/1,
    args: [name: @plugin_name, fields: @fields] do
    with {:ok, relay_id} <- Plugin.binding_target(fields, []),
         do: unbind_plugin(namespace(fields), name, relay_id)
  end

  @doc """
  Change the plugin `name`'s options (`:opts`) and a controller plugin's
  `:hooks`, in place when every changed option is live, otherwise by
  restarting every running copy.
  """
  @spec update(String.t(), keyword(), CallerContext.t()) :: {:ok, map()} | {:error, term()}
  verb update(name, fields, caller_context) when is_binary(name) and is_list(fields),
    verb: :plugin_update,
    row: &fields_row/1,
    args: [name: @plugin_name, fields: @fields] do
    with :ok <- Plugin.read_only(fields, [:opts, :hooks]),
         {:ok, hooks} <- Plugin.optional_hooks(fields[:hooks]) do
      update_plugin(namespace(fields), name, Map.new(fields[:opts] || %{}), hooks)
    end
  end

  @doc "Disable the plugin `name`: every running copy stops, every binding stays."
  @spec disable(String.t(), keyword(), CallerContext.t()) :: term() | {:error, term()}
  verb disable(name, fields, caller_context) when is_binary(name) and is_list(fields),
    verb: :plugin_disable,
    row: &fields_row/1,
    args: [name: @plugin_name, fields: @fields] do
    with :ok <- Plugin.read_only(fields, []), do: disable_plugin(namespace(fields), name)
  end

  defp lacking(namespace, provider) when is_binary(provider),
    do: Providers.lacking(Directory.hub_nodes(to_string(namespace)) -- [node()], provider)

  defp lacking(_namespace, _provider), do: []

  defp namespace(fields), do: Keyword.get(fields, :namespace, "default")

  defp fields_row([_name, fields]), do: {:ok, %{namespace: namespace(fields)}}
end
