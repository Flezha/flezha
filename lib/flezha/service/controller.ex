defmodule Flezha.Service.Controller do
  @moduledoc """
  Published service-API surface for the controller itself: its identity, its
  shutdown state, its durability posture, its memory snapshot, its per-node
  recovery state, and the event subscription a consumer needs to follow it.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data.
  `local_recovery_snapshot/1` is the exception and is not on the published
  surface: it is the remote half of `recovery_per_node/3`'s fan-out, which every
  peer calls by name; `recovery_map/2` is the effect the terminal shares.

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

  use Flezha.AuthZ.Verb

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
  verb hub_id(namespace, caller_context) when is_binary(namespace), verb: :controller_hub_id do
    {:ok, Identificator.hub(namespace)}
  end

  @doc "One hub's recovery state, by hub id."
  @spec recovery_state(atom(), caller_context()) :: {:ok, atom()} | {:error, term()}
  verb recovery_state(hub_id, caller_context) when is_atom(hub_id), verb: :controller_status do
    HubManager.recovery_state(hub_id)
  end

  @doc """
  Each node's recovery state for `namespace`, keyed by node.

  Asks every node in the list, including this one. A node that cannot be reached
  answers `:unreachable` rather than failing the whole read, so one unreachable
  peer does not blank the panel.
  """
  @spec recovery_per_node(String.t(), [node()], caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb recovery_per_node(namespace, nodes, caller_context)
       when is_binary(namespace) and is_list(nodes),
       verb: :controller_recovery do
    {:ok, recovery_map(namespace, nodes)}
  end

  @doc "Where the graceful shutdown sequence has reached."
  @spec shutdown_status(caller_context()) :: {:ok, term()} | {:error, term()}
  verb shutdown_status(caller_context), verb: :controller_status do
    {:ok, Coordinator.status()}
  end

  @doc "The durability backend this controller is configured for."
  @spec durability_backend(caller_context()) :: {:ok, term()} | {:error, term()}
  verb durability_backend(caller_context), verb: :controller_status do
    {:ok, DurabilityTier.configured_backend()}
  end

  @doc "Whether durable state is shipped to a remote tier or stays local."
  @spec durability_posture(caller_context()) :: {:ok, term()} | {:error, term()}
  verb durability_posture(caller_context), verb: :controller_status do
    {:ok, DurabilityTier.posture()}
  end

  @doc "The durability breaches recorded since the controller started."
  @spec durability_breaches(caller_context()) :: {:ok, term()} | {:error, term()}
  verb durability_breaches(caller_context), verb: :controller_status do
    {:ok, DurabilityTier.breaches()}
  end

  @doc "The durability shipper's own status."
  @spec durability_status(caller_context()) :: {:ok, term()} | {:error, term()}
  verb durability_status(caller_context), verb: :controller_status do
    {:ok, DurabilityTier.status()}
  end

  @doc "Every data-plane liveness counter this node holds."
  @spec liveness_counters(caller_context()) :: {:ok, term()} | {:error, term()}
  verb liveness_counters(caller_context), verb: :controller_status do
    {:ok, LivenessCounters.snapshot_all()}
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

  @doc false
  @spec recovery_map(String.t(), [node()]) :: map()
  def recovery_map(namespace, nodes) do
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
