defmodule Flezha.Process.Hub do
  @moduledoc """
  The one guarded seam for reaching a ProcessHub coordinator.

  `ProcessHub.is_alive?/1` is `Process.whereis/1`, which answers `true` for a
  coordinator that is terminating — so the check alone is not the guarantee
  and the call itself must survive the hub going away underneath it. Every
  hub read and every hub call runs through `with_live/3`, which answers the
  caller's declared value either way: a namespace hub dying may not take a
  node-wide process with it. `live_pid/1` picks the runnable pid from a hub
  registry row.
  """

  require Logger

  alias Flezha.Config.Durations

  @type hub_id :: atom()

  @doc "True while `hub_id`'s coordinator is registered."
  @spec alive?(hub_id()) :: boolean()
  def alive?(hub_id) when is_atom(hub_id), do: ProcessHub.is_alive?(hub_id)

  @doc """
  Run `fun` with `hub_id` when that hub is alive, else answer `on_dead` — and
  answer `on_dead` too when the hub goes away mid-call, which is the case the
  liveness check by itself cannot cover.
  """
  @spec with_live(hub_id(), term(), (hub_id() -> term())) :: term()
  def with_live(hub_id, on_dead, fun) when is_atom(hub_id) do
    if ProcessHub.is_alive?(hub_id), do: fun.(hub_id), else: on_dead
  catch
    kind, reason when kind in [:exit, :error] ->
      :telemetry.execute(
        [:flezha, :hub, :unavailable],
        %{count: 1},
        %{hub_id: hub_id, kind: kind, reason: reason}
      )

      Logger.warning(
        "[hub_manager] hub #{inspect(hub_id)} went away mid-call " <>
          "(#{kind} #{inspect(reason)}); answering #{inspect(on_dead)}"
      )

      on_dead
  end

  @doc """
  Resolve a relay's runnable pid from a ProcessHub `node_pids` list
  (`[{node, pid}]`, with 0, 1, or many entries).

  Drops entries on nodes absent from the live cluster, prefers the local node's
  pid (verified alive), and otherwise returns a pid on another live node.
  Returns `nil` when no live pid remains.
  """
  @spec live_pid([{node(), pid()}]) :: pid() | nil
  def live_pid(node_pids) when is_list(node_pids) do
    local = node()
    on_live_node = on_live_nodes(node_pids)

    Enum.find_value(on_live_node, fn {n, pid} -> n == local and Process.alive?(pid) and pid end) ||
      Enum.find_value(on_live_node, fn {n, pid} -> n != local and pid end)
  end

  def live_pid(_), do: nil

  @doc """
  Whether a `{node, pid}` registry entry's process still runs, asked of its own
  node under the Overwatch ping bound. A node that cannot answer in time keeps
  the entry: only a definite exit drops it.
  """
  @spec running?({node(), pid()}) :: boolean()
  def running?({_node, pid}) when node(pid) == node(), do: Process.alive?(pid)

  def running?({_node, pid}) do
    :erpc.call(node(pid), Process, :alive?, [pid], Durations.get(:overwatch_ping_timeout_ms))
  catch
    :error, {:erpc, _reason} -> true
  end

  @doc "The `node_pids` entries on nodes in the live cluster, in their order."
  @spec on_live_nodes([{node(), pid()}]) :: [{node(), pid()}]
  def on_live_nodes(node_pids) do
    live_nodes = [node() | Node.list()]
    Enum.filter(node_pids, fn {n, _pid} -> n in live_nodes end)
  end
end
