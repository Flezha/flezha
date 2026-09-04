defmodule Flezha.Service.Controller do
  @moduledoc """
  Published service-API surface for the controller itself: its identity, its
  shutdown state, its durability posture, its memory snapshot, its per-node
  recovery state, and the event subscription a consumer needs to follow it.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument and answers data. `local_recovery_snapshot/1` is the exception and is
  not on the published surface: it is the remote half of `recovery_per_node/3`'s
  fan-out, which every peer calls by name.

  The controller's event subscription is deliberately NOT here.
  `Flezha.Service.EventManager.add_handler/1` is `pg:join(scope, event, self())`
  and `Blockade.add_handler/2` hard-codes that `self()`, while `pg` refuses a
  non-local pid — so a subscription taken over `:erpc` would register a process
  that dies with the call. Publishing one that took a pid would promise
  something `pg` cannot do for a remote consumer. Until the bus can register a
  remote process, or the surface grows a monitored local proxy, a consumer that
  needs controller events still ships code to the controller and compiles it
  there.
  """

  alias Flezha.Identificator
  alias Flezha.Service.CallerContext
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown.Coordinator
  alias Flezha.State.DurabilityTier
  alias Flezha.Transport.LivenessCounters

  @type caller_context :: CallerContext.t()

  # ---------- Reads ----------

  @doc "The hub id for `namespace`, as this controller composes it."
  @spec hub_id(String.t(), caller_context()) :: {:ok, atom()} | {:error, term()}
  def hub_id(namespace, caller_context) when is_binary(namespace) do
    CallerContext.scoped_read(caller_context, :"controller.status", namespace, fn ->
      {:ok, Identificator.hub(namespace)}
    end)
  end

  @doc "One hub's recovery state, by hub id."
  @spec recovery_state(atom(), caller_context()) :: {:ok, atom()} | {:error, term()}
  def recovery_state(hub_id, caller_context) when is_atom(hub_id) do
    status_read(caller_context, fn ->
      HubManager.recovery_state(hub_id)
    end)
  end

  @doc """
  Each node's recovery state for `namespace`, keyed by node.

  Asks every node in the list, including this one. A node that cannot be reached
  answers `:unreachable` rather than failing the whole read, so one unreachable
  peer does not blank the panel.
  """
  @spec recovery_per_node(String.t(), [node()], caller_context()) ::
          {:ok, map()} | {:error, term()}
  def recovery_per_node(namespace, nodes, caller_context)
      when is_binary(namespace) and is_list(nodes) do
    CallerContext.scoped_read(caller_context, :"operative.list", namespace, fn ->
      {:ok, recovery_map(namespace, nodes)}
    end)
  end

  @doc "Where the graceful shutdown sequence has reached."
  @spec shutdown_status(caller_context()) :: {:ok, term()} | {:error, term()}
  def shutdown_status(caller_context) do
    status_read(caller_context, fn ->
      {:ok, Coordinator.status()}
    end)
  end

  @doc "The durability backend this controller is configured for."
  @spec durability_backend(caller_context()) :: {:ok, term()} | {:error, term()}
  def durability_backend(caller_context) do
    status_read(caller_context, fn ->
      {:ok, DurabilityTier.configured_backend()}
    end)
  end

  @doc "Whether durable state is shipped to a remote tier or stays local."
  @spec durability_posture(caller_context()) :: {:ok, term()} | {:error, term()}
  def durability_posture(caller_context) do
    status_read(caller_context, fn ->
      {:ok, DurabilityTier.posture()}
    end)
  end

  @doc "The durability breaches recorded since the controller started."
  @spec durability_breaches(caller_context()) :: {:ok, term()} | {:error, term()}
  def durability_breaches(caller_context) do
    status_read(caller_context, fn ->
      {:ok, DurabilityTier.breaches()}
    end)
  end

  @doc "The durability shipper's own status."
  @spec durability_status(caller_context()) :: {:ok, term()} | {:error, term()}
  def durability_status(caller_context) do
    status_read(caller_context, fn ->
      {:ok, DurabilityTier.status()}
    end)
  end

  @doc "Every data-plane liveness counter this node holds."
  @spec liveness_counters(caller_context()) :: {:ok, term()} | {:error, term()}
  def liveness_counters(caller_context) do
    status_read(caller_context, fn ->
      {:ok, LivenessCounters.snapshot_all()}
    end)
  end

  @doc false
  # The remote half of `recovery_per_node/3`. Public because the fan-out calls it
  # by name on every peer.
  @spec local_recovery_snapshot(atom()) :: atom()
  def local_recovery_snapshot(hub_id) when is_atom(hub_id) do
    case HubManager.recovery_state(hub_id) do
      {:ok, state} -> state
      {:error, :unknown_hub} -> :unknown
    end
  end

  # Every controller-wide read is the same question — may this caller see the
  # controller's own state — so it is asked in one place.
  defp status_read(caller_context, fun) do
    CallerContext.scoped_read(
      caller_context,
      :"controller.status",
      CallerContext.system_scope(),
      fun
    )
  end

  defp recovery_map(namespace, nodes) do
    hub_id = Identificator.hub(namespace)
    Map.new(nodes, &{&1, node_recovery(&1, hub_id)})
  end

  defp node_recovery(peer, hub_id) when peer == node(), do: local_recovery_snapshot(hub_id)

  defp node_recovery(peer, hub_id) do
    case :rpc.call(peer, __MODULE__, :local_recovery_snapshot, [hub_id], 5_000) do
      {:badrpc, _reason} -> :unreachable
      state when is_atom(state) -> state
    end
  catch
    _kind, _reason -> :unreachable
  end
end
