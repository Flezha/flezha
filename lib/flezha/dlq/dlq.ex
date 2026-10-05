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

  """

  use Flezha.AuthZ.Verb

  @entry_ref [type: :string, doc: "The entry, as `<relay>/<tasklet>/<id>`."]
  @relay_class [
    type: :string,
    default: nil,
    doc: "The relay whose entries to read; every relay when absent."
  ]

  alias Flezha.Config.Durations
  alias Flezha.AuthZ.Namespace
  alias Flezha.Dlq.Store
  alias Flezha.Relay.Lookup
  alias Flezha.Plugin.Host
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

  @doc "Parked entries, nearest-to-expiry first; `nil` for every class."
  @spec list(String.t() | nil, caller_context()) :: {:ok, [entry()]} | {:error, term()}
  verb list(relay_class, caller_context),
    verb: :dlq_list,
    row: &class_scope/1,
    args: [relay_class: @relay_class] do
    {:ok, Store.list(relay_class)}
  end

  @doc "One entry and its payload bytes, or `nil` bytes when it carries none."
  @spec get(String.t(), caller_context()) ::
          {:ok, entry(), binary() | nil} | {:error, term()}
  verb get(entry_ref, caller_context),
    verb: :dlq_show,
    row: &locate/1,
    as: located,
    args: [entry_ref: @entry_ref] do
    {:ok, located.entry, located.payload}
  end

  @doc "Entry and byte counts per parked tasklet."
  @spec stats(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb stats(caller_context), verb: :dlq_stats do
    {:ok, Store.stats()}
  end

  @doc "The `{relay_class, tasklet_id}` pairs that hold parked entries."
  @spec tasklets(String.t() | nil, caller_context()) ::
          {:ok, [{String.t(), String.t()}]} | {:error, term()}
  verb tasklets(relay_class, caller_context),
    verb: :dlq_list,
    row: &class_scope/1,
    args: [relay_class: @relay_class] do
    {:ok, Store.tasklets(relay_class)}
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
    args: [entry_ref: @entry_ref] do
    Store.drop(entry_ref)
  end

  @doc """
  Re-dispatch a parked payload to its relay through the ordinary input
  path.

  A clean replay removes the entry; a failing one re-parks it with `generation`
  incremented, which the store correlates on content identity. Replay identity
  is fresh by construction: the re-drive enters as a NEW input, so a tier-on
  invocation records fresh WAL seqs and downstream sinks see new `flz.seq`
  under the current epoch.
  """
  @spec replay(String.t(), caller_context()) :: {:ok, replay_outcome()} | {:error, term()}
  verb replay(entry_ref, caller_context),
    verb: :dlq_replay,
    row: &locate/1,
    as: located,
    args: [entry_ref: @entry_ref] do
    redrive(entry_ref, located.entry, located.payload)
  end

  # ---------- The rows and the re-drive ----------

  defp locate([entry_ref]) do
    case Store.get(entry_ref) do
      {:ok, entry, payload} ->
        {:ok, %{namespace: class_namespace(entry["relay_class"]), entry: entry, payload: payload}}

      {:error, :not_found} ->
        {:error, {:entry_not_found, entry_ref}}

      {:error, _reason} = refused ->
        refused
    end
  end

  defp class_scope([relay_class]), do: {:ok, %{namespace: class_namespace(relay_class)}}

  defp redrive(entry_ref, entry, payload) do
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
  defp await_replay_outcome(entry_ref, _generation_before, remaining_ms)
       when remaining_ms <= 0 do
    :ok = Store.resolve(entry_ref)
    :removed
  end

  defp await_replay_outcome(entry_ref, generation_before, remaining_ms) do
    Process.sleep(100)

    case Store.get(entry_ref) do
      {:ok, %{"status" => "parked", "generation" => generation}, _payload}
      when generation > generation_before ->
        {:re_parked, generation}

      {:ok, _entry, _payload} ->
        await_replay_outcome(entry_ref, generation_before, remaining_ms - 100)

      {:error, :not_found} ->
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
