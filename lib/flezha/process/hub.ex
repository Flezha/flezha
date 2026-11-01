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

  alias Flezha.Config.Durations
  alias ProcessHub.Constant.StorageKey
  alias ProcessHub.Service.Storage
  alias ProcessHub.Service.Storage.Entry
  alias ProcessHub.Strategy.Migration.{ColdSwap, HotSwap}

  @type hub_id :: atom()
  @type migration :: :cold | :hot

  # A namespace's `migration` setting and the hub strategy it names.
  @strategies %{cold: ColdSwap, hot: HotSwap}

  @doc "The migration strategy module `migration` names."
  @spec strategy(migration()) :: module()
  def strategy(migration), do: Map.fetch!(@strategies, migration)

  @doc """
  How `hub_id`'s running hub moves its children: the strategy it started
  with, `:cold` for a hub that is not running.
  """
  @spec migration(hub_id()) :: migration()
  def migration(hub_id) do
    with_live(hub_id, :cold, fn hub_id ->
      with hub when hub != nil <- ProcessHub.Hub.get(hub_id),
           %{__struct__: module} <- Storage.get(hub.storage.misc, StorageKey.strmigr()),
           {migration, ^module} <- Enum.find(@strategies, &(elem(&1, 1) == module)),
           do: migration,
           else: (_cold -> :cold)
    end)
  end

  @doc """
  The children whose handover state `hub_id` stored on this node and has not
  delivered yet: the moves this node started that are unfinished. A cold child
  that missed the state query stored none, so it is not listed. An entry past
  its TTL is left out unless `include_expired: true`: the storage keeps it,
  and the hub can still deliver it, until its janitor purges it.
  """
  @spec pending_handovers(hub_id(), keyword()) :: [ProcessHub.child_id()]
  def pending_handovers(hub_id, opts \\ []) do
    misc = ProcessHub.Hub.get(hub_id).storage.misc
    since = if Keyword.get(opts, :include_expired, false), do: 0, else: Entry.now_ms()

    for kind <- [:coldswap_state, :hotswap_state],
        {child_id, expire} <- Storage.match(misc, {{kind, :"$1"}, :_, :"$2"}),
        expire > since,
        do: child_id
  end

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
        %{hub_id: hub_id, kind: kind, reason: reason, answer: on_dead}
      )

      on_dead
  end

  @doc """
  Resolve a relay's runnable pid from a ProcessHub `node_pids` list
  (`[{node, pid}]`, with 0, 1, or many entries).

  Drops entries on nodes absent from the live cluster, prefers the local node's
  pid (verified alive), and otherwise returns a pid on another live node: the
  only one unchecked, the first still `running?/1` of several. Returns `nil`
  when no live pid remains.
  """
  @spec live_pid([{node(), pid()}]) :: pid() | nil
  def live_pid(node_pids) when is_list(node_pids) do
    {here, there} = node_pids |> on_live_nodes() |> Enum.split_with(&(elem(&1, 0) == node()))
    Enum.find_value(here, fn {_n, pid} -> Process.alive?(pid) and pid end) || remote_pid(there)
  end

  def live_pid(_), do: nil

  defp remote_pid([{_node, pid}]), do: pid
  defp remote_pid(there), do: Enum.find_value(there, fn {_n, pid} = e -> running?(e) and pid end)

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
