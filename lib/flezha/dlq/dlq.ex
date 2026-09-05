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
  operator, alongside the drop that destroys the same record — a parked payload
  is the failed business record held verbatim, so reading one is at least as
  privileged as discarding it.

  The entry is the row a `dlq show | replay | drop` is scoped by, so it is read
  only for a caller that is bound and whose role grants the action: an unbound
  caller is not told whether a reference exists.

  `Flezha.Service.Cli.Dlq` renders what this module answers and calls the same
  effects — the store, `redrive/3` — under the same rows.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.Config.Durations
  alias Flezha.AuthZ.Namespace
  alias Flezha.Dlq.Store
  alias Flezha.Operative.Lookup
  alias Flezha.Plugin.Host
  alias Flezha.Service.CallerContext
  alias Flezha.Service.OperativeManager

  @type caller_context :: CallerContext.t()
  @type entry :: map()

  @typedoc """
  What became of a re-driven record: removed after a clean replay, or parked
  again under the given generation.
  """
  @type replay_outcome :: :removed | {:re_parked, pos_integer()}

  # ---------- Reads ----------

  @doc "Parked entries, nearest-to-expiry first; `nil` for every class."
  @spec list(String.t() | nil, caller_context()) :: {:ok, [entry()]} | {:error, term()}
  verb list(operative_class, caller_context), verb: :dlq_list, row: &class_scope/1 do
    {:ok, Store.list(operative_class)}
  end

  @doc "One entry and its payload bytes, or `nil` bytes when it carries none."
  @spec get(String.t(), caller_context()) ::
          {:ok, entry(), binary() | nil} | {:error, term()}
  verb get(entry_ref, caller_context), verb: :dlq_show, row: &locate/1, as: located do
    {:ok, located.entry, located.payload}
  end

  @doc "Entry and byte counts per parked procedure."
  @spec stats(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb stats(caller_context), verb: :dlq_stats do
    {:ok, Store.stats()}
  end

  @doc "The `{operative_class, procedure_id}` pairs that hold parked entries."
  @spec procedures(String.t() | nil, caller_context()) ::
          {:ok, [{String.t(), String.t()}]} | {:error, term()}
  verb procedures(operative_class, caller_context), verb: :dlq_list, row: &class_scope/1 do
    {:ok, Store.procedures(operative_class)}
  end

  # ---------- Mutations ----------

  @doc """
  Discard a parked entry and its payload. Deliberate data loss, so it is
  authorized as `dlq.drop` and audited with the caller that asked.
  """
  @spec drop(String.t(), caller_context()) :: :ok | {:error, term()}
  verb drop(entry_ref, caller_context), verb: :dlq_drop, row: &locate/1 do
    Store.drop(entry_ref)
  end

  @doc """
  Re-dispatch a parked payload to its operative through the ordinary input
  path.

  A clean replay removes the entry; a failing one re-parks it with `generation`
  incremented, which the store correlates on content identity. Replay identity
  is fresh by construction: the re-drive enters as a NEW input, so a tier-on
  invocation records fresh WAL seqs and downstream sinks see new `flz.seq`
  under the current epoch.
  """
  @spec replay(String.t(), caller_context()) :: {:ok, replay_outcome()} | {:error, term()}
  verb replay(entry_ref, caller_context), verb: :dlq_replay, row: &locate/1, as: located do
    redrive(entry_ref, located.entry, located.payload)
  end

  # ---------- The rows and the effects the terminal shares ----------

  @doc false
  @spec entry_row(String.t()) :: {:ok, map()} | {:error, term()}
  def entry_row(entry_ref) do
    with {:ok, entry, payload} <- Store.get(entry_ref) do
      {:ok,
       %{namespace: class_namespace(entry["operative_class"]), entry: entry, payload: payload}}
    end
  end

  @doc false
  @spec class_row(String.t() | nil) :: {:ok, map()}
  def class_row(operative_class), do: {:ok, %{namespace: class_namespace(operative_class)}}

  @doc false
  @spec redrive(String.t(), entry(), binary() | nil) :: {:ok, replay_outcome()} | {:error, term()}
  def redrive(entry_ref, entry, payload) do
    with :ok <- replayable(payload),
         {:ok, overwatch} <- resolve_overwatch(entry["operative_class"]) do
      generation_before = entry["generation"] || 1
      :ok = Store.mark_replaying(entry_ref)
      Host.inject(overwatch, payload, %{"channel" => entry["channel"]}, :fire_and_forget)
      {:ok, await_replay_outcome(entry_ref, generation_before, replay_grace_ms())}
    end
  end

  defp locate([entry_ref]), do: entry_row(entry_ref)
  defp class_scope([operative_class]), do: class_row(operative_class)

  # A verb that names a class is judged in that class's namespace; one that
  # names none reads across every namespace, which is a `_system` read.
  defp class_namespace(nil), do: Namespace.system_name()
  defp class_namespace(operative_class), do: OperativeManager.namespace_of_class(operative_class)

  defp replayable(nil), do: {:error, :payload_not_replayable}
  defp replayable(_payload), do: :ok

  defp resolve_overwatch(operative_class) do
    case Lookup.overwatch(operative_class) do
      {:ok, pid} -> {:ok, pid}
      {:error, _reason} -> resolve_overwatch_cluster(operative_class)
    end
  end

  # The class registry is node-local; a target hosted on a peer resolves
  # through the routed pid lookup and the re-injection cast rides distribution.
  defp resolve_overwatch_cluster(operative_class) do
    with {:ok, namespace} <- OperativeManager.namespace_of(operative_class),
         {:ok, pid} <- OperativeManager.get_operative_pid(namespace, operative_class) do
      {:ok, pid}
    else
      _ -> {:error, :operative_not_running}
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
