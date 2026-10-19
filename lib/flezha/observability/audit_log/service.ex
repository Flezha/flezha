defmodule Flezha.Service.AuditLog do
  @moduledoc """
  Service-API surface for the durable audit log. Every public function takes a
  `Flezha.Service.CallerContext` as its last argument and binds a row of
  `Flezha.AuthZ.Verbs`; the rows audit every read on the `:audit_log` source
  BEFORE doing the work, under the function's own name, so a read is itself
  auditable.

  Disabled state has two answers: the master flag off
  (`:observability, :enabled: false`) gives `{:error, :observability_disabled}`,
  the subsystem flag off (`:audit_log, :enabled: false`) gives
  `{:error, :audit_log_disabled}`.

  `tail/2` hands each record to the context's `:emit`; without one it
  subscribes the calling process, where each record arrives as
  `{:audit_record, ref, record}` and `{:audit_writer_closed, ref}` ends the
  stream.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Observability
  alias Flezha.Observability.AuditLog.{Reader, Sink, Writer}
  alias Flezha.Observability.AuditLog.Rotation
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  @type read_params :: %{
          optional(:file) => String.t(),
          optional(:source) => String.t(),
          optional(:name) => String.t(),
          optional(:since_us) => integer(),
          optional(:until_us) => integer(),
          optional(:limit) => pos_integer()
        }

  # ---------- Public API ----------

  @doc """
  The audit log's files, each with its record times and digest. Each row holds
  the file's name, first and last record time, record count, size and digest.
  """
  @spec list_files(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list_files(caller_context), verb: :audit_list do
    with :ok <- check_enabled(), do: {:ok, Writer.catalog()}
  end

  @doc """
  The records of one audit file that match the given filters. Only the records
  of the caller's namespace are answered.
  """
  @spec read(read_params() | keyword(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb read(params, caller_context),
    verb: :audit_read,
    target: &read_target/1,
    args: [
      params: [
        type: :keyword,
        doc: "`file`, `source`, `name`, `since_us`, `until_us` and `limit`."
      ]
    ] do
    with :ok <- check_enabled(), do: read_records(Map.new(params), caller_context)
  end

  @doc """
  Stream every audit record the caller may read as it is written.

  With an `:emit` in the context (a function, or a pid that receives
  `{:flezha_emit, record}`), each record is handed to it until the watched
  process exits, `:emit` answers `:done`, the context's `window_ms` runs out
  or the writer closes, and the subscription is released on the way out; the
  answer is `{:ok, :caller_down | :done | :window_closed}`. Without one, the
  calling process is subscribed and answered the subscription's reference.
  """
  @spec tail(map() | keyword(), caller_context()) ::
          {:ok, reference() | atom()} | {:error, term()}
  verb tail(params, caller_context),
    verb: :audit_tail,
    stream: true,
    args: [params: [type: :keyword, default: [], doc: "Unused."]] do
    with :ok <- check_enabled() do
      if Map.has_key?(caller_context, :emit),
        do: forward(caller_context),
        else: Writer.subscribe_tail()
    end
  end

  @doc "Undo a `tail/2`."
  @spec unsubscribe(reference(), caller_context()) :: :ok | {:error, term()}
  verb unsubscribe(ref, caller_context),
    verb: :audit_unsubscribe,
    args: [ref: [type: :any, doc: "The subscription `tail/2` answered."]] do
    with :ok <- check_enabled(), do: Writer.unsubscribe_tail(ref)
  end

  @doc "The audit log's operational snapshot."
  @spec stats(caller_context()) :: {:ok, map()} | {:error, term()}
  verb stats(caller_context), verb: :audit_stats do
    with :ok <- check_enabled() do
      snapshot = Writer.stats()

      {:ok,
       snapshot
       |> Map.put(:total_bytes, snapshot.catalog_total_bytes + snapshot.current_file_bytes)
       |> Map.put(:total_files, snapshot.catalog_count + 1)
       |> Map.put(:record_count, snapshot.current_record_count)
       |> Map.put(:drop_count, Sink.post_close_dropped())
       |> Map.put(:oldest_ts_us, snapshot.oldest_ts_us)}
    end
  end

  @doc "Close the current audit file and start a new one."
  @spec rotate(caller_context()) :: {:ok, map()} | {:error, term()}
  verb rotate(caller_context), verb: :audit_rotate do
    with :ok <- check_enabled(), do: Rotation.rotate_now()
  end

  defp read_records(params, caller_context) do
    file = Map.get(params, :file) || "audit-current.jsonl"
    path = Path.join(audit_dir(), file)

    # Only a file of the audit directory itself: a name that walks out of it is
    # answered as missing, so the read says nothing about the host's files.
    if Path.basename(file) == file and file not in [".", ".."] and File.regular?(path) do
      limit = Map.get(params, :limit, 1_000)

      opts = [
        source: Map.get(params, :source),
        name: Map.get(params, :name),
        since_us: Map.get(params, :since_us),
        until_us: Map.get(params, :until_us),
        limit: limit
      ]

      records = Reader.stream(path, opts) |> Enum.take(limit)
      {:ok, filter_by_namespace(records, caller_context)}
    else
      {:error, {:file_not_found, Path.basename(file)}}
    end
  end

  # ---------- Internal helpers ----------

  # The tail of a caller that named an `:emit`: this process subscribes and
  # hands on what `read/2` would show the caller. A closed writer ends it.
  defp forward(caller_context) do
    {emit, watch} = CallerContext.emitter(caller_context)
    {:ok, ref} = Writer.subscribe_tail()

    try do
      outcome =
        CallerContext.receive_for(
          Map.get(caller_context, :window_ms, :infinity),
          fn
            {:audit_record, ^ref, record} ->
              CallerContext.hand_on(filter_by_namespace([record], caller_context), emit)

            {:audit_writer_closed, ^ref} ->
              :done

            _other ->
              :continue
          end,
          watch
        )

      {:ok, outcome}
    after
      Writer.unsubscribe_tail(ref)
    end
  end

  defp read_target([params | _]), do: %{file: params |> Map.new() |> Map.get(:file)}

  # Filter records to the caller's namespace, for a read and for a forwarded
  # tail alike, except for `admin@_system` and internal callers. Records that
  # pre-date the namespace field (or whose `namespace` is null) are treated as
  # `_system` for compatibility.
  defp filter_by_namespace(records, %{role: :admin, namespace: "_system"}), do: records
  defp filter_by_namespace(records, %{source: :internal}), do: records

  defp filter_by_namespace(records, caller_context) do
    bound_ns = Map.get(caller_context, :namespace)

    cond do
      not AuthZ.enabled?() ->
        records

      is_nil(bound_ns) ->
        records

      true ->
        Enum.filter(records, fn rec ->
          case Map.get(rec, "namespace") || get_in(rec, ["metadata", "namespace"]) do
            nil -> bound_ns == "_system"
            ns -> ns == bound_ns
          end
        end)
    end
  end

  defp check_enabled() do
    cond do
      not Observability.enabled?() ->
        {:error, :observability_disabled}

      not Flezha.Observability.AuditLog.Supervisor.enabled?() ->
        {:error, :audit_log_disabled}

      true ->
        :ok
    end
  end

  defp audit_dir(), do: Layout.dir(:audit)
end
