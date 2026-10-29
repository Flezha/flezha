defmodule Flezha.Service.Dlq do
  @moduledoc """
  Service-API surface for the dead-letter queue. Every public function takes a
  `Flezha.Service.CallerContext` as its last argument, binds a row of
  `Flezha.AuthZ.Verbs`, and answers data.

  `Flezha.Dlq.Store` is a store: it deletes what it is told to delete and asks
  nothing about who asked. Every verb here is judged in the entry's own
  namespace rather than one the caller declares, which is what stops a consumer
  widening its reach by asking in a namespace it prefers. A verb that names no
  class reads across every namespace, so it is judged in `_system`.

  The reads carry actions of their own: `dlq.list` for the three listings and
  `dlq.show` for the one that answers payload bytes. Both are granted to the
  user, alongside the drop that destroys the same record — a parked payload
  is the failed business record held verbatim, so reading one is at least as
  privileged as discarding it.

  The entry is the row a `dlq show | replay | drop` is scoped by, so it is read
  only for a caller that is bound and whose role grants the action: an unbound
  caller is not told whether a reference exists.

  Each controller parks into its own store. The listings answer every
  controller's entries in the cluster answer shape (`rows`, each with its
  `node`, and `unreachable`), and `show`, `replay` and `drop` find the entry
  on whichever controller holds it and act there.

  """

  use Flezha.AuthZ.Verb

  @entry_ref [type: :string, doc: "The entry, as `<relay>/<tasklet>/<id>`."]
  @relay_class [
    type: :string,
    default: nil,
    doc: "The relay whose entries to read; every relay when absent."
  ]

  alias Flezha.AuthZ.Namespace
  alias Flezha.Cluster.Page
  alias Flezha.Cluster.Reads
  alias Flezha.Config.Durations
  alias Flezha.Dlq.Store
  alias Flezha.Relay.Lookup
  alias Flezha.Plugin.Host
  alias Flezha.Process.Poll
  alias Flezha.Service.CallerContext
  alias Flezha.Service.RelayManager

  @type caller_context :: CallerContext.t()
  @type entry :: map()

  @typedoc """
  What became of a re-driven record: removed after a clean replay, or parked
  again under a new generation.
  """
  @type replay_outcome :: %{outcome: :removed | :re_parked, generation: pos_integer() | nil}

  # ---------- Reads ----------

  @doc """
  Every controller's parked entries, nearest-to-expiry first, each with the
  `node` holding it. A `relay_class` of `nil` reads every class.

  With `opts` (`limit`, `after`) it answers one page of them across the
  cluster, newest first (`Flezha.Cluster.Page`, `Flezha.Dlq.Store.page_key/1`):
  each controller decodes only the entries it could put on the page.
  """
  @spec list(String.t() | nil, keyword(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb list(relay_class, opts \\ [], caller_context),
    verb: :dlq_list,
    row: &class_scope/1,
    args: [relay_class: @relay_class, opts: Page.arg()] do
    case Page.parse(opts, :dlq_list, sorts: [newest: :desc]) do
      {:ok, nil} ->
        {:ok, Reads.controllers() |> Reads.rows(Store, :list, [relay_class]) |> by_expiry()}

      {:ok, page} ->
        answer = Reads.rows(Reads.controllers(), Store, :list_page, [relay_class, page])
        {:ok, Page.merge(answer, page, &Store.page_key/1)}

      refused ->
        refused
    end
  end

  @doc "One parked entry, with the `node` holding it and its payload bytes if it carries any."
  @spec get(String.t(), caller_context()) ::
          {:ok, entry(), binary() | nil} | {:error, term()}
  verb get(entry_ref, caller_context),
    verb: :dlq_show,
    row: &locate/1,
    as: located,
    args: [entry_ref: @entry_ref] do
    {:ok, located.entry, located.payload}
  end

  @doc "Entry and byte counts per parked tasklet on every controller, each with its `node`."
  @spec stats(caller_context()) :: {:ok, Reads.rows()} | {:error, term()}
  verb stats(caller_context), verb: :dlq_stats do
    {:ok, Reads.rows(Reads.controllers(), Store, :stats, [])}
  end

  @doc """
  The tasklets that hold parked entries on every controller, each as
  `relay_class`, `tasklet_id` and `node`.
  """
  @spec tasklets(String.t() | nil, caller_context()) :: {:ok, Reads.rows()} | {:error, term()}
  verb tasklets(relay_class, caller_context),
    verb: :dlq_list,
    row: &class_scope/1,
    args: [relay_class: @relay_class] do
    {:ok, Reads.rows(Reads.controllers(), __MODULE__, :local_tasklets, [relay_class])}
  end

  # ---------- Mutations ----------

  @doc """
  Discard a parked entry and its payload. Deliberate data loss, so it is
  authorized as `dlq.drop` and audited with the caller that asked.
  """
  @spec drop(String.t(), caller_context()) :: :ok | {:error, term()}
  verb drop(entry_ref, caller_context),
    verb: :dlq_drop,
    row: &locate/1,
    as: located,
    args: [entry_ref: @entry_ref] do
    Reads.on(located.node, Store, :drop, [entry_ref])
  end

  @doc """
  Re-dispatch a parked payload to its relay. It goes through the ordinary input
  path. A clean replay removes the entry; a failing one re-parks it with
  `generation` incremented, which the store correlates on content identity.
  Replay identity is fresh by construction: the re-drive enters as a NEW input,
  so a tier-on invocation records fresh WAL seqs and downstream sinks see new
  `flz.seq` under the current epoch.
  """
  @spec replay(String.t(), caller_context()) :: {:ok, replay_outcome()} | {:error, term()}
  verb replay(entry_ref, caller_context),
    verb: :dlq_replay,
    row: &locate/1,
    as: located,
    args: [entry_ref: @entry_ref] do
    Reads.on(located.node, __MODULE__, :redrive, [entry_ref, located.entry, located.payload])
  end

  # ---------- The local halves ----------

  @doc false
  # The local half of `tasklets/2`.
  @spec local_tasklets(String.t() | nil) :: [map()]
  def local_tasklets(relay_class) do
    for {rc, sh} <- Store.tasklets(relay_class), do: %{relay_class: rc, tasklet_id: sh}
  end

  @doc false
  # The local half of `replay/2`, run on the controller holding the entry.
  @spec redrive(String.t(), entry(), binary() | nil) :: {:ok, replay_outcome()} | {:error, term()}
  def redrive(entry_ref, entry, payload) do
    with :ok <- replayable(entry_ref, payload),
         {:ok, overwatch} <- resolve_overwatch(entry["relay_class"]) do
      generation_before = entry["generation"] || 1
      :ok = Store.mark_replaying(entry_ref)
      Host.inject(overwatch, payload, %{"channel" => entry["channel"]}, :fire_and_forget)

      {:ok,
       case await_replay_outcome(entry_ref, generation_before, replay_grace_ms()) do
         :removed -> %{outcome: :removed, generation: nil}
         {:re_parked, generation} -> %{outcome: :re_parked, generation: generation}
       end}
    end
  end

  # ---------- The rows ----------

  # The entry on whichever controller holds it. A reference no answering
  # controller holds is not found, naming the controllers that did not answer.
  defp locate([entry_ref]) do
    with {:ok, _rc, _sh, _id} <- Store.parse_ref(entry_ref) do
      case Reads.find(Reads.controllers(), Store, :get, [entry_ref]) do
        {:ok, node, {:ok, entry, payload}} ->
          namespace = class_namespace(entry["relay_class"])
          entry = Map.put(entry, :node, node)
          {:ok, %{namespace: namespace, entry: entry, payload: payload, node: node}}

        {:error, []} ->
          {:error, {:entry_not_found, entry_ref}}

        {:error, unreachable} ->
          {:error, {:entry_unreachable, entry_ref, unreachable}}
      end
    end
  end

  defp class_scope([relay_class | _opts]), do: {:ok, %{namespace: class_namespace(relay_class)}}

  defp by_expiry(rows), do: Reads.newest(rows, & &1["ts_ms"], nil)

  # A verb that names a class is judged in that class's namespace; one that
  # names none reads across every namespace, which is a `_system` read.
  defp class_namespace(nil), do: Namespace.system_name()
  defp class_namespace(relay_class), do: RelayManager.namespace_of_class(relay_class)

  defp replayable(entry_ref, nil), do: {:error, {:payload_not_replayable, entry_ref}}
  defp replayable(_entry_ref, _payload), do: :ok

  defp resolve_overwatch(relay_class) do
    case Lookup.overwatch(relay_class) do
      {:ok, pid} -> {:ok, pid}
      {:error, _reason} -> resolve_overwatch_cluster(relay_class)
    end
  end

  # The class registry is node-local; a target hosted on a peer resolves
  # through the routed pid lookup and the re-injection cast rides distribution.
  defp resolve_overwatch_cluster(relay_class) do
    with {:ok, namespace} <- RelayManager.namespace_of(relay_class),
         {:ok, pid} <- RelayManager.get_relay_pid(namespace, relay_class) do
      {:ok, pid}
    else
      _ -> {:error, {:relay_not_running, relay_class}}
    end
  end

  # Poll for the re-park (generation bump) until the grace window closes; no
  # re-park within it means the replay processed cleanly.
  defp await_replay_outcome(entry_ref, generation_before, grace_ms) do
    deadline = System.monotonic_time(:millisecond) + grace_ms

    settled = fn ->
      case Store.get(entry_ref) do
        {:ok, %{"status" => "parked", "generation" => generation}, _payload}
        when generation > generation_before ->
          {:ok, {:re_parked, generation}}

        {:ok, _entry, _payload} ->
          {:error, :timeout}

        {:error, :not_found} ->
          {:ok, :removed}
      end
    end

    case Poll.until(deadline, Durations.get(:dlq_replay_poll_ms), settled) do
      {:ok, outcome} ->
        outcome

      {:error, :timeout} ->
        :ok = Store.resolve(entry_ref)
        :removed
    end
  end

  # How long a replay waits for a re-park before declaring success. A re-park
  # after the window simply creates a fresh entry (same content identity,
  # generation 1).
  defp replay_grace_ms do
    Durations.get(:dlq_replay_grace_ms)
  end
end
