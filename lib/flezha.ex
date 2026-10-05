defmodule Flezha do
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager

  @moduledoc """
  Flezha is a compute engine platform with an Elixir controller and SDK runtimes
  in Rust, Java, and others. It supports clustered relays with hot-swap
  migration and a single auto-restore-on-boot mechanism — ProcessHub's
  durable registry persists relay specs, and per-Cell WAL/snapshot
  files persist progress (see `docs/topics/basics/running_relays.md` and
  `docs/internals/state_persistence.md`).
  """

  defdelegate start_relay(namespace, relay), to: RelayManager

  defdelegate stop_relay(namespace, relay), to: RelayManager

  # Answers every running namespace, because it carries no caller context to
  # narrow by. `Flezha.Service.Relay.list_namespaces/1` is the spelling
  # that takes one and is narrowed to the caller; retiring this delegate is a
  # published-surface retirement and lands with the consumer that calls it.
  defdelegate list_namespaces(), to: HubManager, as: :get_running_namespaces

  defdelegate list_runners(namespace),
    to: Flezha.Runner.FleetManager,
    as: :global_runners

  defdelegate list_relays(namespace), to: RelayManager, as: :get_relay_ids

  defdelegate list_factories(namespace), to: Flezha.Runner.FleetManager

  defdelegate enable_plugin(plugin_id, plugin), to: PluginManager

  defdelegate disable_plugin(namespace, name), to: PluginManager

  defdelegate cell_state(namespace, relay),
    to: RelayManager,
    as: :get_cell_state
end
