defmodule Flezha.Service.PluginRecords do
  @moduledoc """
  What a namespace's plugin records hold: every plugin enabled there, the
  relays bound to each and the controllers it runs on. The records are read
  here and written by `Flezha.Service.PluginManager`, both on a node that hosts
  the namespace's hub (`Flezha.Service.HubManager.on_hub_node/4`).
  """

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Probe
  alias Flezha.Plugin
  alias Flezha.Plugin.Providers
  alias Flezha.Plugin.Shared
  alias Flezha.Process.Poll
  alias Flezha.Service.HubManager

  @doc """
  Every plugin enabled in `namespace`: its name, its provider, whether it is
  `single`, the relays bound to it, and the controllers it runs on; for a
  controller plugin, its hooks and the controllers whose copy lags.
  """
  @spec plugins(binary()) :: [map()]
  def plugins(namespace) do
    with {:error, _} <-
           HubManager.on_hub_node(namespace, {__MODULE__, :plugins_here}, [namespace], :none),
         do: []
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

  @doc "The entry plugin `name` is enabled under in `namespace`, read on this node."
  @spec entry(binary(), binary()) :: {:ok, Plugin.t()} | {:error, {:plugin_not_found, binary()}}
  def entry(namespace, name) do
    with :error <- HubManager.plugin_entry(namespace, name),
         do: {:error, {:plugin_not_found, name}}
  end

  @doc """
  Wait, at most `timeout` ms, until no other node of the namespace's hub still
  resolves plugin `name`: they drop its record only when its removal reaches
  them, and a convergence tick there before that brings a binding back up
  against it. Answers the nodes still holding it when the time runs out; a
  node that does not answer holds nothing.
  """
  @spec await_forgotten(binary(), binary(), non_neg_integer()) ::
          {:ok, :forgotten} | {:error, [node()]}
  def await_forgotten(namespace, name, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    nodes = HubManager.get_hub_nodes(namespace)

    Poll.until(deadline, Durations.get(:plugin_forget_poll_ms), fn ->
      case Enum.filter(nodes, &resolves?(&1, namespace, name)) do
        [] -> {:ok, :forgotten}
        holding -> {:error, holding}
      end
    end)
  end

  defp resolves?(node, namespace, name) do
    timeout = Durations.get(:plugin_call_timeout_ms)
    match?({:ok, _}, :erpc.call(node, __MODULE__, :entry, [namespace, name], timeout))
  catch
    _kind, _reason -> false
  end

  @doc "The relays bound to plugin `name` in `namespace`."
  @spec relays(binary(), binary()) :: [binary()]
  def relays(namespace, name), do: namespace |> bindings(name) |> relays_of()

  @doc "Every binding to plugin `name` in `namespace`, as `{relay_id, id, binding}`."
  @spec bindings(binary(), binary()) :: [{binary(), binary(), Plugin.t()}]
  def bindings(namespace, name), do: namespace |> all_bindings() |> bound_to(name)

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
end
