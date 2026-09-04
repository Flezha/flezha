defmodule Flezha.Service.Dlq do
  @moduledoc """
  Service-API surface for the dead-letter queue. Every public function takes a
  `Flezha.Service.CallerContext` as its last argument, authorizes through it
  before any effect, and answers data.

  `Flezha.Dlq.Store` is a store: it deletes what it is told to delete and asks
  nothing about who asked. `platform-authn-authz` names `dlq.drop` and
  `dlq.replay` as authorized verbs, so those two are gated here — in the entry's
  own namespace rather than one the caller declares, which is what stops a
  consumer widening its reach by asking in a namespace it prefers.

  The reads validate the context and nothing more, because the policy names no
  DLQ read action to authorize against. Gating them would need three new
  entries in `Flezha.AuthZ.Action`, their role grants, and a `--token` threaded
  into `flz dlq list` and `flz dlq show`, which today take no options at all —
  who may read a parked payload is a decision for the repository owner, and it
  is recorded rather than guessed here.

  The entry is read before the action is authorized, because the scope is the
  entry's namespace and the entry is where it comes from; the context's *shape*
  is checked before that, so a caller with no context at all is not even told
  whether a reference exists.

  `Flezha.Service.Cli.Dlq` renders what this module answers; the mechanism —
  authorization, the audit, the re-drive and its outcome — lives here once so
  the CLI and the console share it.
  """

  alias Flezha.Dlq.Store
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Service.CallerContext
  alias Flezha.Service.OperativeManager

  @audit_source :dlq_service

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
  def list(operative_class, caller_context) do
    CallerContext.validated(caller_context, fn ->
      {:ok, Store.list(operative_class)}
    end)
  end

  @doc "One entry and its payload bytes, or `nil` bytes when it carries none."
  @spec get(String.t(), caller_context()) ::
          {:ok, entry(), binary() | nil} | {:error, term()}
  def get(entry_ref, caller_context) do
    CallerContext.validated(caller_context, fn ->
      Store.get(entry_ref)
    end)
  end

  @doc "Entry and byte counts per parked procedure."
  @spec stats(caller_context()) :: {:ok, [map()]} | {:error, term()}
  def stats(caller_context) do
    CallerContext.validated(caller_context, fn ->
      {:ok, Store.stats()}
    end)
  end

  @doc "The `{operative_class, procedure_id}` pairs that hold parked entries."
  @spec procedures(String.t() | nil, caller_context()) ::
          {:ok, [{String.t(), String.t()}]} | {:error, term()}
  def procedures(operative_class, caller_context) do
    CallerContext.validated(caller_context, fn ->
      {:ok, Store.procedures(operative_class)}
    end)
  end

  # ---------- Mutations ----------

  @doc """
  Discard a parked entry and its payload. Deliberate data loss, so it is
  authorized as `dlq.drop` and audited with the caller that asked.
  """
  @spec drop(String.t(), caller_context()) :: :ok | {:error, term()}
  def drop(entry_ref, caller_context) do
    with {:ok, entry, _payload} <- entry_for(entry_ref, caller_context) do
      gated(caller_context, :"dlq.drop", entry, entry_ref, fn ->
        :ok = Store.drop(entry_ref)

        :telemetry.execute([:flezha, :dlq, :operator_drop], %{count: 1}, %{
          entry: entry_ref,
          channel: entry["channel"],
          entity_key: entry["entity_key"],
          error: entry["error"],
          caller_source: Map.get(caller_context, :source)
        })

        :ok
      end)
    end
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
  def replay(entry_ref, caller_context) do
    with {:ok, entry, payload} <- entry_for(entry_ref, caller_context) do
      gated(caller_context, :"dlq.replay", entry, entry_ref, fn ->
        redrive(entry_ref, entry, payload, caller_context)
      end)
    end
  end

  # ---------- The gate ----------

  defp redrive(entry_ref, entry, payload, caller_context) do
    with :ok <- replayable(payload),
         {:ok, overwatch} <- resolve_overwatch(entry["operative_class"]) do
      generation_before = entry["generation"] || 1
      :ok = Store.mark_replaying(entry_ref)

      :telemetry.execute([:flezha, :dlq, :operator_replay], %{count: 1}, %{
        entry: entry_ref,
        channel: entry["channel"],
        generation: generation_before,
        caller_source: Map.get(caller_context, :source)
      })

      GenServer.cast(overwatch, {:io_in, payload, %{"channel" => entry["channel"]}})
      {:ok, await_replay_outcome(entry_ref, generation_before, replay_grace_ms())}
    end
  end

  # Shape first, then existence: a caller with no context at all is not told
  # whether a reference exists. The entry is read before the action is
  # authorized because the scope is the entry's own namespace.
  defp entry_for(entry_ref, caller_context) do
    with :ok <- CallerContext.validate(caller_context), do: Store.get(entry_ref)
  end

  # The scope is the entry's own namespace, so a refused caller learns only that
  # it was refused — it is told nothing about the entry's operative, because the
  # guard runs before the replayability and liveness checks.
  defp gated(caller_context, action, entry, entry_ref, fun) do
    CallerContext.gated(
      caller_context,
      action,
      OperativeManager.namespace_of_class(entry["operative_class"]),
      @audit_source,
      %{entry: entry_ref},
      fun
    )
  end

  defp replayable(nil), do: {:error, :payload_not_replayable}
  defp replayable(_payload), do: :ok

  defp resolve_overwatch(operative_class) do
    case ClassRegistry.overwatch_pid(operative_class) do
      {:ok, pid} -> {:ok, pid}
      :error -> resolve_overwatch_cluster(operative_class)
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
    Application.get_env(:flezha, :dlq, [])[:replay_grace_ms] || 5_000
  end
end
