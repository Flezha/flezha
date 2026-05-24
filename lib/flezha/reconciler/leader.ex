defmodule Flezha.Reconciler.Leader do
  @moduledoc """
  Tracks cluster-wide orchestration leadership for the reconciler.

  When `:flezha, :leadership_enabled` is true this opts the node into
  elector-backed leadership via `ProcessHub.start_leadership/0` and subscribes to
  leadership-change notifications so `is_leader?/0` reflects the elected leader
  with no polling. Only the elected leader runs the registry-mutating
  cluster-wide reconcile; every node always runs its own local reconcile, so this
  is never on the data path.

  A `Process.monitor` on the local leadership watcher is the fallback: if the
  leadership machinery dies, the tracked state is reset and re-derived from the
  next snapshot, so the reconciler never trusts a stale leader.

  When leadership is disabled (single-node / dev / test) the node reports itself
  as leader so the cluster-wide reconcile still runs for its solo cluster.
  """

  use GenServer

  alias ProcessHub.Service.Leadership

  require Logger

  defstruct enabled: false, leader: :none, am_i_leader: false, watcher_ref: nil

  @type t() :: %__MODULE__{
          enabled: boolean(),
          leader: node() | :none,
          am_i_leader: boolean(),
          watcher_ref: reference() | nil
        }

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc """
  Whether the local node should perform the cluster-wide reconcile: `true` when
  it is the elected leader, or when leadership is disabled (solo semantics).
  Reads tracked state — no election round-trip. Returns `false` if the tracker is
  unavailable (safe default: do not run the registry-mutating pass).
  """
  @spec is_leader?() :: boolean()
  def is_leader?() do
    GenServer.call(__MODULE__, :is_leader?)
  catch
    :exit, _ -> false
  end

  @doc """
  The current leader node, `:none` when leadership is started but unsettled, or
  `:disabled` when leadership is not enabled on this node.
  """
  @spec leader() :: node() | :none | :disabled
  def leader() do
    GenServer.call(__MODULE__, :leader)
  catch
    :exit, _ -> :none
  end

  @impl true
  def init(_) do
    enabled = Application.get_env(:flezha, :leadership_enabled, true)

    state =
      if enabled do
        start_and_track()
      else
        %__MODULE__{enabled: false}
      end

    {:ok, state}
  end

  @impl true
  def handle_call(:is_leader?, _from, state) do
    # Disabled → solo leader; enabled → tracked election result.
    {:reply, not state.enabled or state.am_i_leader, state}
  end

  def handle_call(:leader, _from, state) do
    reply = if state.enabled, do: state.leader, else: :disabled
    {:reply, reply, state}
  end

  # Leadership snapshot / transition pushed by ProcessHub's watcher.
  @impl true
  def handle_info(
        {:processhub_leadership, %{leader: leader, am_i_leader: am_i_leader} = info},
        state
      ) do
    if leader != state.leader do
      Logger.info(
        "[leadership] transition #{inspect(state.leader)} -> #{inspect(leader)} " <>
          "(am_i_leader=#{am_i_leader})"
      )

      :telemetry.execute(
        [:flezha, :leadership, :transition],
        %{count: 1},
        %{
          node: node(),
          leader: leader,
          previous: Map.get(info, :previous),
          am_i_leader: am_i_leader
        }
      )
    end

    {:noreply, %{state | leader: leader, am_i_leader: am_i_leader}}
  end

  # Process.monitor fallback: the leadership watcher died (leadership itself is
  # typically still up). Reset tracked state and re-subscribe — this re-ensures
  # the watcher singleton and re-arms the monitor — so we re-derive leadership
  # from the next snapshot rather than trusting a stale leader.
  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %{watcher_ref: ref, enabled: true} = state
      ) do
    Logger.warning("[leadership] watcher down; re-subscribing")
    :ok = Leadership.subscribe(self())
    {:noreply, %{state | leader: :none, am_i_leader: false, watcher_ref: monitor_watcher()}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Ensure leadership is started, subscribe for transitions (this also ensures
  # the watcher singleton exists), then monitor the watcher for the fallback.
  defp start_and_track() do
    case ProcessHub.start_leadership() do
      :ok ->
        :ok = Leadership.subscribe(self())
        %__MODULE__{enabled: true, watcher_ref: monitor_watcher()}

      {:error, reason} ->
        Logger.warning(
          "[leadership] start_leadership failed: #{inspect(reason)}; running local-only"
        )

        %__MODULE__{enabled: false}
    end
  end

  defp monitor_watcher() do
    case Leadership.Watcher.whereis() do
      nil -> nil
      pid -> Process.monitor(pid)
    end
  end
end
