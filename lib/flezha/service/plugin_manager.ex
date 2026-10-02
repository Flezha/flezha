defmodule Flezha.Service.PluginManager do
  @moduledoc """
  Named plugins and their bindings. A plugin is enabled by name in a namespace
  and recorded as its own `{:plugin, name}` entry in the namespace hub's
  declared set; an operator is bound to it by a binding on the operator's own
  entry. Both are written by the start authority, so a plugin and every binding
  survive an Overwatch restart, a controller restart and a cluster cold start,
  and nothing is kept per node.

  A namespace's records are read and written on a node that hosts its hub: this
  one when it does, otherwise one that does, under a bound. The order is
  validate, then durable write, then runtime: a refused enable or bind leaves
  nothing, and a failed runtime subscribe rolls its binding back.

  What runs where is `Flezha.Operator.Plugins`' and `Flezha.Plugin.Shared`'s:
  this module writes intent and asks the running copies to converge on it.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.Cluster.Directory
  alias Flezha.Config.Backoff
  alias Flezha.Config.Durations
  alias Flezha.Operator.Overwatch
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Parallel
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Home
  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.ProviderBehaviour.Decl
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias Flezha.Plugin.Shared
  alias Flezha.Process.Cspec
  alias Flezha.Service.CallerContext
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.OperatorManager.Init
  alias Flezha.Service.OperatorManager.Teardown
  alias Flezha.Service.RegistryUpdate

  ## Enable

  @doc """
  Enable `name` in `plugin.namespace` with the provider and options `plugin`
  carries. Nothing starts: a plugin runs where an operator bound to it runs.
  Answers `{:ok, :already_enabled}` for a name enabled with the same provider,
  and refuses another provider with `plugin.id_taken`.

  With `plugin.operator_id` set it also binds that operator with `plugin.hooks`,
  in one call; a bind that fails answers its refusal and leaves the plugin
  enabled.
  """
  @spec enable_plugin(binary(), Plugin.t()) ::
          {:ok, binary() | :already_enabled} | {:error, term()}
  def enable_plugin(name, %Plugin{} = plugin) when is_binary(name) do
    namespace = to_string(plugin.namespace || "default")
    plugin = %Plugin{plugin | id: name, namespace: namespace}

    with {:ok, entry} <- entry_of(plugin),
         {:ok, enabled} <-
           on_hub_node(namespace, :declare_entry, [entry], :registry_unavailable) do
      bind_after_enable(plugin, enabled)
    end
  end

  defp bind_after_enable(%Plugin{operator_id: nil}, :enabled), do: {:ok, "plugin enabled"}
  defp bind_after_enable(%Plugin{operator_id: nil}, :already_enabled), do: {:ok, :already_enabled}

  defp bind_after_enable(%Plugin{} = plugin, enabled) do
    case {bind_plugin(plugin.namespace, plugin.id, plugin.operator_id, hooks: plugin.hooks),
          enabled} do
      {:already_bound, :already_enabled} -> {:ok, :already_enabled}
      {{:error, _} = refused, _} -> refused
      {_bound, _enabled} -> {:ok, "plugin enabled"}
    end
  end

  # An operator plugin's entry is the plugin without an operator or hooks, its
  # options checked against the provider's declaration as any binding of it may
  # hook it: an enable names no operator, so the provider's own preparation runs
  # where a binding brings the plugin up. A controller plugin's keeps its hooks.
  defp entry_of(%Plugin{provider: provider, opts: opts} = plugin) do
    plugin = %Plugin{plugin | opts: opts || %{}}

    with {:ok, entry} <- checked_entry(plugin, Plugin.controller?(plugin)),
         do: {:ok, Plugin.strip_transient(entry)}
  rescue
    UndefinedFunctionError -> {:error, not_loaded(provider)}
  end

  defp checked_entry(plugin, true), do: Shared.checked_entry(plugin)

  defp checked_entry(%Plugin{provider: provider} = plugin, false) do
    bindable = %{subscribed_extensions: Decl.io_hooks(provider.__provider__())}

    with :ok <- refuse_init_hook(plugin),
         {:ok, _normalised} <- Options.normalise_opts(provider, Map.merge(plugin.opts, bindable)),
         do: {:ok, %Plugin{plugin | operator_id: nil, hooks: []}}
  end

  defp refuse_init_hook(%Plugin{hooks: hooks}) do
    if Enum.any?(hooks, &(elem(&1, 0) in [:on_operator_init, "on_operator_init"])),
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
  Bind `operator_id` (or every member of the partition group it names) to the
  plugin `name`, with the `:hooks` and the `:channel` in `fields`. Answers
  `:already_bound` for an operator that holds the binding.
  """
  @spec bind_plugin(binary(), binary(), binary(), keyword()) ::
          :ok | :already_bound | {:error, term()}
  def bind_plugin(namespace, name, operator_id, fields \\ []) do
    binding = %Plugin{
      id: name,
      provider: nil,
      namespace: namespace,
      operator_id: operator_id,
      hooks: Keyword.get(fields, :hooks, []),
      opts: channel_opts(Keyword.get(fields, :channel))
    }

    on_hub_node(namespace, :bind_here, [binding], {:plugin_not_found, name})
  end

  defp channel_opts(channel) when channel in [nil, ""], do: %{}
  defp channel_opts(channel), do: %{channel: channel}

  @doc false
  @spec bind_here(Plugin.t()) :: :ok | :already_bound | {:error, term()}
  def bind_here(%Plugin{namespace: namespace, operator_id: operator_id} = binding) do
    targets = targets(namespace, operator_id)

    with :ok <- refuse_init_hook(binding),
         {:ok, entry} <- fetch_entry(namespace, binding.id) do
      one_holder(entry, fn ->
        with {:ok, binding} <- checked_binding(binding, targets) do
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
  # refusal its plugin's entry gives it.
  defp checked_binding(%Plugin{id: name, namespace: namespace} = binding, targets) do
    with {:ok, entry} <- fetch_entry(namespace, name),
         binding = %Plugin{binding | provider: entry.provider},
         {:ok, binding} <- Table.canonicalise(Overwatch.__extensions_catalog__(), binding),
         :ok <- refuse_binding(entry, binding, targets),
         do: {:ok, binding}
  end

  @doc """
  The bindings an operator starting with `--plugins` specs carries: each spec
  that names a provider enables its plugin when it is not enabled yet, and each
  is checked as a bind of `plugins`' operator would be. A spec that only binds
  may name the binding's `channel` and nothing else in its options.
  """
  @spec start_bindings(binary(), [Plugin.t()]) :: {:ok, [Plugin.t()]} | {:error, term()}
  def start_bindings(namespace, plugins) do
    Enum.reduce_while(plugins, {:ok, []}, fn plugin, {:ok, acc} ->
      case start_binding(namespace, plugin) do
        {:ok, binding} -> {:cont, {:ok, acc ++ [binding]}}
        {:error, _} = refused -> {:halt, refused}
      end
    end)
  end

  defp start_binding(namespace, %Plugin{provider: nil, opts: opts} = plugin) do
    case Map.keys(opts) -- [:channel] do
      [] ->
        binding = %Plugin{plugin | namespace: namespace}

        on_hub_node(
          namespace,
          :checked_start_binding,
          [binding],
          {:plugin_not_found, plugin.id}
        )

      unread ->
        {:error, {:unread_plugin_options, Enum.map(unread, &"opts.#{&1}")}}
    end
  end

  defp start_binding(namespace, %Plugin{id: name} = plugin) do
    with {:ok, _enabled} <-
           enable_plugin(name, %Plugin{plugin | namespace: namespace, operator_id: nil}) do
      start_binding(namespace, %Plugin{plugin | provider: nil, opts: %{}})
    end
  end

  @doc false
  def checked_start_binding(%Plugin{operator_id: operator_id} = binding),
    do: checked_binding(binding, [{operator_id, nil}])

  # A group id names no operator — it names N of them, each bound under its
  # `#pK` id with its partition options.
  @doc false
  def targets(namespace, operator_id) do
    case PartitionGroup.members(namespace, operator_id) do
      [] -> [{operator_id, nil}]
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
        {:error, {:plugin_single_sink, name}}

      shareable?(entry) ->
        :ok

      holder = other_holder(entry, targets) ->
        {:error, {:plugin_not_shareable, name, holder}}

      true ->
        :ok
    end
  end

  # The operator, or the group, already bound to a plugin that cannot be
  # shared. Members of one group are one holder.
  defp other_holder(%Plugin{id: name, namespace: namespace}, targets) do
    own = MapSet.new(targets, &Plugin.group_of(elem(&1, 0)))

    namespace
    |> bound_operators(name)
    |> Enum.map(&Plugin.group_of/1)
    |> Enum.reject(&MapSet.member?(own, &1))
    |> List.first()
  end

  defp bind_one(namespace, operator_id, %Plugin{id: id} = binding, entry) do
    case HubManager.operator_bindings(namespace, operator_id) do
      {:ok, bindings} ->
        if List.keymember?(bindings, id, 0),
          do: :already_bound,
          else: write_binding(namespace, operator_id, binding, entry)

      {:error, _} = refused ->
        refused
    end
  end

  # The operator is handed the plugin's entry with the binding: its own node's
  # rows may not have heard of a plugin enabled a moment ago.
  defp write_binding(namespace, operator_id, %Plugin{id: id} = binding, entry) do
    stored = Plugin.strip_transient(binding)

    with :ok <-
           update_cspec_plugins(namespace, operator_id, RegistryUpdate.add_plugin(id, stored)) do
      case subscribe(namespace, operator_id, id, binding, entry) do
        :ok ->
          :ok

        refused ->
          _ = update_cspec_plugins(namespace, operator_id, RegistryUpdate.remove_plugin(id))
          refused
      end
    end
  end

  defp subscribe(namespace, operator_id, id, binding, entry) do
    with {:ok, pid} <- OperatorManager.get_operator_pid(namespace, operator_id),
         do: Overwatch.plugin_subscribe(pid, id, binding, entry)
  end

  ## Unbind

  @doc """
  Remove `operator_id`'s binding to `name` (from every member of the group it
  names). The plugin stays enabled.
  """
  @spec unbind_plugin(binary(), binary(), binary()) :: :ok | {:error, term()}
  def unbind_plugin(namespace, name, operator_id),
    do:
      on_hub_node(
        namespace,
        :unbind_here,
        [namespace, name, operator_id],
        {:plugin_not_bound, name, operator_id}
      )

  @doc false
  @spec unbind_here(binary(), binary(), binary()) :: :ok | {:error, term()}
  def unbind_here(namespace, name, operator_id) do
    held =
      for {member_id, _identity} <- targets(namespace, operator_id),
          {:ok, bindings} <- [HubManager.operator_bindings(namespace, member_id)],
          {id, _binding} <- bindings,
          Plugin.base_id(id) == name,
          do: {member_id, id}

    case held do
      [] ->
        {:error, {:plugin_not_bound, name, operator_id}}

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

  # The durable write decides the unbind; an operator that does not answer its
  # unsubscribe drops the binding at its next bring-up, and is named.
  defp unbind_one(namespace, operator_id, id) do
    case update_cspec_plugins(namespace, operator_id, RegistryUpdate.remove_plugin(id)) do
      :ok ->
        case OperatorManager.get_operator_pid(namespace, operator_id) do
          {:ok, pid} -> unsubscribed(Overwatch.plugin_unsubscribe(pid, id))
          {:error, _gone} -> :ok
        end

      {:error, {:operator_not_found, _, _}} ->
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
  Remove every binding of `operator_ids`, in one bounded fan-out: a partition
  rescale turns the old members' sources off this way. Answers `:ok`, or
  `{:error, {:undisabled, operator_ids}}` naming the ones it could not reach.
  """
  @spec unbind_all(binary(), [binary()]) :: :ok | {:error, {:undisabled, [binary()]}}
  def unbind_all(namespace, operator_ids) do
    outcomes =
      Parallel.results(
        operator_ids,
        fn operator_id ->
          {:ok, bindings} = HubManager.operator_bindings(namespace, operator_id)
          bindings |> Enum.map(&unbind_one(namespace, operator_id, elem(&1, 0))) |> Enum.uniq()
        end,
        timeout: Durations.get(:plugin_disable_fanout_ms)
      )

    undisabled = for {id, outcome} <- outcomes, outcome not in [{:ok, []}, {:ok, [:ok]}], do: id
    if undisabled == [], do: :ok, else: {:error, {:undisabled, undisabled}}
  end

  ## Update

  @doc """
  Change `name`'s options, and a controller plugin's hooks when `hooks` names
  any. When every changed option is declared live, each running copy applies
  them in place; otherwise each restarts, and an update that changes none is
  `:unchanged`. Answers how it was applied and the operators bound to it.
  """
  @spec update_plugin(binary(), binary(), map(), [Plugin.hook()]) ::
          {:ok, %{applied: :live | :restarted | :unchanged, operators: [binary()]}}
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
         {:ok, new} <- entry_of(%Plugin{old | opts: Map.merge(old.opts, opts), hooks: hooks}) do
      case change(old, new) do
        :none -> {:ok, %{applied: :unchanged, operators: bound_operators(namespace, name)}}
        mode -> apply_update(namespace, name, new, mode)
      end
    end
  end

  defp apply_update(namespace, name, new, mode) do
    with :ok <-
           update_cspec_plugins(namespace, {:plugin, name}, RegistryUpdate.add_plugin(name, new)) do
      Enum.each(Directory.plugin_hosts(namespace, name), &Shared.update(&1, new, mode))
      operators = bound_operators(namespace, name)
      _ = reconverge(namespace, name, &Overwatch.plugin_update(&1, &2, &3, mode, new))
      {:ok, %{applied: if(mode == :live, do: :live, else: :restarted), operators: operators}}
    end
  end

  # An operator plugin's hooks are its bindings'.
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
  and every binding stays on its operator, reported not started until a plugin
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

  @doc "Unbind and disable the plugin a listener enabled for itself on `operator_id`."
  @spec detach(binary(), binary(), binary()) :: :ok | {:error, term()}
  def detach(namespace, operator_id, name) do
    _ = unbind_plugin(namespace, name, operator_id)

    case disable_plugin(namespace, name) do
      {:ok, _} -> :ok
      {:error, {:plugin_not_found, _}} -> :ok
      {:error, _} = refused -> refused
    end
  end

  ## Reading

  @doc """
  Every plugin enabled in `namespace`: its name, its provider, whether it is
  `single`, the operators bound to it, and the controllers it runs on; for a
  controller plugin, its hooks and the controllers whose copy lags.
  """
  @spec plugins(binary()) :: [map()]
  def plugins(namespace) do
    with {:error, _} <- on_hub_node(namespace, :plugins_here, [namespace], :none), do: []
  end

  @doc false
  def plugins_here(namespace) do
    bound = all_bindings(namespace)

    for %Plugin{id: name} = entry <- HubManager.plugin_entries(namespace) do
      %{
        name: name,
        provider: provider_name(entry.provider),
        single: entry.single,
        operators: bound |> bound_to(name) |> operators_of() |> Enum.sort(),
        controllers: Directory.plugin_nodes(namespace, name) |> Enum.sort(),
        hooks: Enum.map(entry.hooks, &to_string(elem(&1, 0))),
        lagging: if(Plugin.controller?(entry), do: Shared.lagging_on(namespace, name), else: [])
      }
    end
  end

  # A provider whose package this controller did not load still has a name to
  # show: its module's.
  defp provider_name(provider) do
    if Code.ensure_loaded?(provider), do: provider.__provider__().name, else: inspect(provider)
  end

  defp bound_operators(namespace, name),
    do: namespace |> all_bindings() |> bound_to(name) |> operators_of()

  # Every binding in the namespace as `{operator_id, id, binding}`: the
  # registry rows, and the declared set that holds a moving operator's
  # bindings while its row is between its two homes.
  defp all_bindings(namespace) do
    for {operator_id, bindings} <-
          HubManager.operator_bindings(namespace) ++ HubManager.declared_bindings(namespace),
        {id, binding} <- bindings,
        uniq: true,
        do: {operator_id, id, binding}
  end

  defp bound_to(bindings, name), do: Enum.filter(bindings, &(Plugin.base_id(elem(&1, 1)) == name))
  defp operators_of(bindings), do: bindings |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

  @doc """
  `binding` laid over its plugin's entry, or why it cannot come up here: its
  plugin is not enabled, or its provider is not loaded on this controller.
  The entry is `entry` when the caller holds it (`:none` when it knows there is
  none), and read on this node otherwise (an operator's node hosts its
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

  # Ask every operator bound to `name` to bring its binding to what the
  # plugin's entry now says: up after an enable, not started after a disable.
  # One bound for the whole walk; answers the operators it could not reach.
  defp reconverge(namespace, name, ask) do
    namespace
    |> all_bindings()
    |> bound_to(name)
    |> Parallel.results(
      fn {operator_id, id, binding} ->
        with {:ok, pid} <- OperatorManager.get_operator_pid(namespace, operator_id),
             do: ask.(pid, id, binding)
      end,
      timeout: Durations.get(:plugin_disable_fanout_ms)
    )
    |> Enum.reject(fn {_target, outcome} ->
      match?({:ok, :ok}, outcome) or match?({:ok, {:error, {:operator_not_found, _, _}}}, outcome)
    end)
    |> Enum.map(fn {{operator_id, _id, _binding}, _outcome} -> operator_id end)
  end

  ## Committed values

  @doc """
  Persist one value a running provider committed (a file reader's offset, a
  websocket's applied subscription set) onto the binding it belongs to.
  """
  @spec persist_binding_opt(binary() | atom(), binary(), binary(), atom(), term()) ::
          :ok | {:error, term()}
  def persist_binding_opt(namespace, operator_id, id, key, value),
    do:
      update_cspec_plugins(namespace, operator_id, RegistryUpdate.put_plugin_opt(id, key, value))

  @doc """
  Persist one value a shared plugin's running copy committed onto the plugin's
  own entry, which every copy of it starts from.
  """
  @spec persist_plugin_opt(binary() | atom(), binary(), atom(), term()) :: :ok | {:error, term()}
  def persist_plugin_opt(namespace, name, key, value),
    do:
      update_cspec_plugins(
        namespace,
        {:plugin, name},
        RegistryUpdate.put_plugin_opt(name, key, value)
      )

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

  # ---------- Published surface ----------
  #
  # The context-taking half a consumer is allowed to call. Each function binds
  # the row `flz plugin` binds for the same verb, judged in the plugin's own
  # namespace, and answers data. `fields` carries the namespace, which the
  # admin entry stamps from the connection.

  @doc "Every plugin enabled in the caller's namespace."
  @spec list(CallerContext.t()) :: {:ok, [map()]} | {:error, term()}
  verb list(caller_context), verb: :plugin_list do
    {:ok, plugins(Map.get(caller_context, :namespace) || "default")}
  end

  @doc """
  Enable the plugin `name` built from `fields` (`:provider`, `:opts`,
  `:namespace`, `:single`, and `:operator_id` and `:hooks` to bind in the same
  call).
  """
  @spec enable(String.t(), keyword(), CallerContext.t()) :: term() | {:error, term()}
  verb enable(name, fields, caller_context) when is_binary(name) and is_list(fields),
    verb: :plugin_enable,
    row: &fields_row/1 do
    with {:ok, plugin} <- Plugin.from_fields([{:id, name} | fields]),
         do: enable_plugin(name, plugin)
  end

  @doc "Bind `fields[:operator_id]` to the plugin `name` with `:hooks` and `:channel`."
  @spec bind(String.t(), keyword(), CallerContext.t()) :: term() | {:error, term()}
  verb bind(name, fields, caller_context) when is_binary(name) and is_list(fields),
    verb: :plugin_bind,
    row: &fields_row/1 do
    with {:ok, hooks} <- read_hooks(Keyword.get(fields, :hooks)) do
      bind_plugin(namespace(fields), name, Keyword.fetch!(fields, :operator_id),
        hooks: hooks,
        channel: Keyword.get(fields, :channel)
      )
    end
  end

  @doc "Unbind `fields[:operator_id]` from the plugin `name`."
  @spec unbind(String.t(), keyword(), CallerContext.t()) :: term() | {:error, term()}
  verb unbind(name, fields, caller_context) when is_binary(name) and is_list(fields),
    verb: :plugin_unbind,
    row: &fields_row/1 do
    unbind_plugin(namespace(fields), name, Keyword.fetch!(fields, :operator_id))
  end

  @doc "Disable the plugin `name`."
  @spec disable(String.t(), keyword(), CallerContext.t()) :: term() | {:error, term()}
  verb disable(name, fields, caller_context) when is_binary(name) and is_list(fields),
    verb: :plugin_disable,
    row: &fields_row/1 do
    disable_plugin(namespace(fields), name)
  end

  defp read_hooks(nil), do: {:ok, []}
  defp read_hooks(written), do: Plugin.hooks(written)

  defp namespace(fields), do: Keyword.get(fields, :namespace, "default")

  defp fields_row([_name, fields]), do: {:ok, %{namespace: namespace(fields)}}
end
