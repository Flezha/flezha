defmodule Flezha.Logs.RelayHistory do
  @moduledoc """
  The `relay_history` kind's records: one `started` when a relay instance is
  deliberately started, one `ended` when it ends for good, and `start_failed`
  when a start the controller accepted fails.

  Every record stands alone: namespace, relay, factory, run, the `origin` and
  `first_started_at` its start stamped into the declared spec, and the spawn
  identity it was spawned with; an `ended` one adds the outcome, whether it was
  halted, how long it ran and a short error.

  `started` and `start_failed` are appended by the process that started the
  relay; `ended` by the Overwatch's first `:process` teardown step
  (`Flezha.Service.RelayManager.Teardown`), and only when it leaves for good: a
  move, a crash restart, a failed start or a rescale's restart of a member it
  keeps writes nothing.

  `seen/2` answers whether a relay id or a spawn identity was recorded within
  the dedup window, on this controller first, then on every other one.
  """

  use Flezha.Observability.Source,
    name: :relay_history,
    cardinality_budget: 50,
    area: :controller,
    title: "Relay history",
    description: "The relay history: its files starting and expiring, and records it refused.",
    signals: [
      relay_history_rotated:
        {:event, label: "New file", meaning: "The relay history started a new file."},
      relay_history_retention_sweep:
        {:event,
         label: "Old files deleted",
         meaning: "Relay history files older than the retention period were deleted."},
      log_overloaded:
        {:event,
         label: "Records refused",
         meaning: "The relay history refused records because its queue was full."}
    ]

  alias Flezha.Cluster.{Page, Reads}
  alias Flezha.Logs
  alias Flezha.Logs.{Kinds, Local, Seen}

  @events ~w(started ended start_failed)
  @outcomes ~w(completed undone failed stopped timed_out rescaled_away cancelled)
  @error_bytes 512

  @doc "Every event a record names."
  @spec events() :: [String.t()]
  def events(), do: @events

  @doc "Every outcome an `ended` record names."
  @spec outcomes() :: [String.t()]
  def outcomes(), do: @outcomes

  @doc """
  The origin a start stamps on its relay: `kind` (`operator`, `group`,
  `schedule`, `spawn`, `call`, `rescale`), who asked (`caller_context`'s
  principal, source and request id) and `extra`, leaving out what nobody gave.
  """
  @spec origin(String.t(), map(), map()) :: map()
  def origin(kind, caller_context, extra \\ %{}) do
    %{
      kind: kind,
      principal: Map.get(caller_context, :principal),
      source: caller_context |> Map.get(:source) |> then(&(&1 && to_string(&1))),
      request_id: Map.get(caller_context, :request_id)
    }
    |> Map.merge(extra)
    |> Map.reject(fn {_field, value} -> value in [nil, ""] end)
  end

  @doc "Record that `relay` (as its start resolved it) started in `namespace`."
  @spec started(String.t(), Flezha.Relay.Builder.t()) :: :ok | {:error, term()}
  def started(namespace, relay), do: append(record("started", namespace, relay, %{}))

  @doc "Record that `relay`'s start failed with `error` (a reason or an error)."
  @spec start_failed(String.t(), Flezha.Relay.Builder.t(), term()) :: :ok | {:error, term()}
  def start_failed(namespace, relay, error),
    do: append(record("start_failed", namespace, relay, %{"error" => error_text(error)}))

  @doc """
  The answer to "did this run already happen": `{:ended | :started, relay_id,
  ts_us}`, `:none` when every controller answered without it, `{:unknown,
  nodes}` when one that did not answer might hold it, `:disabled` when the kind
  is off. `key` is `{:relay, id}` or `{:spawn_identity, identity}`.
  """
  @spec seen(String.t(), Seen.key()) ::
          Seen.answer() | :none | {:unknown, [node()]} | :disabled
  def seen(namespace, key) do
    cond do
      not Kinds.enabled?(Kinds.fetch!(:relay_history)) ->
        :disabled

      answer = Seen.lookup(namespace, key) ->
        answer

      true ->
        case Reads.find(tl(Reads.controllers()), Local, :seen, [namespace, key]) do
          {:ok, _node, answer} -> answer
          {:error, []} -> :none
          {:error, unreachable} -> {:unknown, unreachable}
        end
    end
  end

  @doc """
  How `relay_id` in `namespace` (`:all` for any) ended, as the completion
  record its newest `ended` record stands for (`state_kept: false`), read on
  every controller; `:error` when none answered one. The inverse of
  `outcome/2`.
  """
  @spec last_outcome(String.t() | :all, String.t()) :: {:ok, map()} | :error
  def last_outcome(namespace, relay_id) do
    query = %{
      filters: %{"event" => "ended", "relay" => relay_id},
      since_us: nil,
      until_us: nil,
      namespace: namespace
    }

    {:ok, page} = Page.parse(%{limit: 1}, :relay_history, sorts: [newest: :desc], keys: [:limit])

    case Local.everywhere(:relay_history, query, page) do
      %{rows: [{_key, record} | _]} -> {:ok, completion(record)}
      %{rows: []} -> :error
    end
  end

  defp completion(record) do
    %{
      namespace: record["namespace"],
      run: record["run"],
      status: status(record["outcome"]),
      error: record["error"] || "",
      result: "",
      finished_at: div(record["ts_us"], 1_000_000),
      state_kept: false
    }
  end

  defp status("completed"), do: "ok"
  defp status(failed) when failed in ["failed", "undone"], do: "error"
  defp status(_stopped), do: "cancelled"

  @doc """
  The kind's encode: a record map's wall-clock time and its fields after
  `ts_iso`, in the record's order.
  """
  @spec fields(map()) :: {integer(), [{String.t(), term()}]}
  def fields(%{"ts_us" => ts_us} = record) do
    order =
      ~w(event namespace relay factory run origin first_started_at spawn_identity outcome halted duration_ms error)

    {ts_us, for(field <- order, Map.has_key?(record, field), do: {field, record[field]})}
  end

  @doc """
  Record that the run an Overwatch `state` holds ended for good. Its stop
  decides that and calls this; `ends_at` answers when the run's time limit
  fell, read only for a run that ended `cancelled` with no stop requested.
  """
  @spec ended(map(), (-> integer() | nil)) :: :ok | {:error, term()}
  def ended(state, ends_at) do
    {outcome, error} = outcome(state, ends_at)
    first = Keyword.get(state.builder.opts, :first_started_at)

    %{
      "outcome" => outcome,
      "halted" => state.halt_reason != nil,
      "duration_ms" => if(is_integer(first), do: max(System.os_time(:millisecond) - first, 0)),
      "error" => error_text(error)
    }
    |> then(
      &record("ended", state.namespace, %{state.builder | factory_id: state.factory_id}, &1)
    )
    |> append()
  end

  @doc false
  # How the run ended, from the facts its stop carried, and the error to show.
  @spec outcome(map(), (-> integer() | nil)) :: {String.t(), term()}
  def outcome(%{cause: :rescale_leave}, _ends_at), do: {"rescaled_away", nil}

  def outcome(%{terminal: %{status: status} = terminal} = state, ends_at) do
    error = Map.get(terminal, :error, "")

    case {status, Map.get(terminal, :undo, "")} do
      {done, _undo} when done in ["ok", "completed"] -> {"completed", error}
      {"error", "complete"} -> {"undone", error}
      {"error", "partial"} -> {"failed", "#{error}; rollback incomplete"}
      {"cancelled", _undo} -> {cancelled(state, ends_at), error}
      {_error, _undo} -> {"failed", error}
    end
  end

  def outcome(%{halt_reason: nil}, _ends_at), do: {"stopped", nil}
  def outcome(%{halt_reason: reason}, _ends_at), do: {"stopped", reason}

  defp cancelled(%{stop_requested_by: by}, _ends_at) when by != nil, do: "stopped"

  defp cancelled(_state, ends_at) do
    case ends_at.() do
      ends_at when is_integer(ends_at) ->
        if System.os_time(:millisecond) >= ends_at, do: "timed_out", else: "cancelled"

      _no_window ->
        "cancelled"
    end
  end

  defp record(event, namespace, relay, extra) do
    origin = Keyword.get(relay.opts, :origin) || %{}
    {identity, origin} = Map.pop(origin, :spawn_identity)

    %{
      "ts_us" => System.os_time(:microsecond),
      "event" => event,
      "namespace" => to_string(namespace),
      "relay" => relay.id,
      "factory" => relay.factory_id,
      "run" => relay.run && Atom.to_string(relay.run),
      "origin" => origin,
      "first_started_at" => Keyword.get(relay.opts, :first_started_at)
    }
    |> then(&if(identity in [nil, ""], do: &1, else: Map.put(&1, "spawn_identity", identity)))
    |> Map.merge(extra)
  end

  defp append(record) do
    Seen.note(record)
    Logs.append(:relay_history, record)
  end

  defp error_text(nil), do: ""
  defp error_text(%Flezha.Error{message: message}), do: error_text(message)
  defp error_text(text) when is_binary(text), do: truncate(text)

  defp error_text(reason),
    do: reason |> inspect(limit: 20, printable_limit: @error_bytes) |> truncate()

  defp truncate(text), do: String.byte_slice(text, 0, @error_bytes)
end
