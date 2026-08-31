defmodule Flezha.Service.AuditLog do
  @moduledoc """
  Service-API surface for the durable audit log. Every public function takes a
  `Flezha.Service.CallerContext` as its last argument.

  Disabled state has two answers: the master flag off
  (`:observability, :enabled: false`) gives `{:error, :observability_disabled}`,
  the subsystem flag off (`:audit_log, :enabled: false`) gives
  `{:error, :audit_log_disabled}`.

  Every public function emits its audit-of-audit `:event` on the `:audit_log`
  source BEFORE doing the work, so a read is itself auditable.
  """

  alias Flezha.AuthZ
  alias Flezha.Observability
  alias Flezha.Observability.AuditLog.{Reader, Sink, Writer}
  alias Flezha.Observability.AuditLog.Rotation
  alias Flezha.Service.CallerContext

  @audit_source :audit_log

  @type caller_context :: CallerContext.t()

  # ---------- Public API ----------

  @spec list_files(caller_context()) :: {:ok, [map()]} | {:error, term()}
  def list_files(caller_context) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"audit.read") do
      emit_audit(:list_files, caller_context, %{})
      {:ok, Writer.catalog()}
    end
  end

  @type read_params :: %{
          optional(:file) => String.t(),
          optional(:source) => String.t(),
          optional(:name) => String.t(),
          optional(:since_us) => integer(),
          optional(:until_us) => integer(),
          optional(:limit) => pos_integer()
        }

  @spec read(read_params(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  def read(%{} = params, caller_context) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"audit.read") do
      emit_audit(:read, caller_context, %{file: Map.get(params, :file)})

      dir = audit_dir()
      file = Map.get(params, :file) || "audit-current.jsonl"
      path = Path.join(dir, file)

      if File.exists?(path) do
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
        {:error, :file_not_found}
      end
    end
  end

  @spec tail(map(), caller_context()) :: {:ok, reference()} | {:error, term()}
  def tail(_params, caller_context) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"audit.read") do
      emit_audit(:tail, caller_context, %{})
      Writer.subscribe_tail()
    end
  end

  @spec unsubscribe(reference(), caller_context()) :: :ok | {:error, term()}
  def unsubscribe(ref, caller_context) when is_reference(ref) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.validate(caller_context),
         :ok <- AuthZ.require_bound_principal(caller_context) do
      Writer.unsubscribe_tail(ref)
    end
  end

  @spec stats(caller_context()) :: {:ok, map()} | {:error, term()}
  def stats(caller_context) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"audit.read") do
      emit_audit(:stats, caller_context, %{})

      snapshot = Writer.stats()

      stats =
        snapshot
        |> Map.put(:total_bytes, snapshot.catalog_total_bytes + snapshot.current_file_bytes)
        |> Map.put(:total_files, snapshot.catalog_count + 1)
        |> Map.put(:record_count, snapshot.current_record_count)
        |> Map.put(:drop_count, Sink.post_close_dropped())
        |> Map.put(:oldest_ts_us, snapshot.oldest_ts_us)

      {:ok, stats}
    end
  end

  @spec rotate(caller_context()) :: {:ok, map()} | {:error, term()}
  def rotate(caller_context) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"audit.rotate") do
      emit_audit(:rotate, caller_context, %{})

      case Rotation.rotate_now() do
        {:ok, info} -> {:ok, info}
        {:error, _} = err -> err
      end
    end
  end

  # ---------- AuthZ helpers ----------

  # Filter JSONL records to the caller's namespace except for
  # `admin@_system` and internal callers. Records that pre-date the
  # namespace field (or whose `namespace` is null) are treated as
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

  # ---------- Internal helpers ----------

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

  # The principal rides the audit log's own signals and nothing else's, so it
  # goes on the metadata here rather than into the shared emit.
  defp emit_audit(action, caller_context, extra) do
    CallerContext.emit(
      @audit_source,
      action,
      caller_context,
      CallerContext.put_unless_nil(extra, :principal, Map.get(caller_context, :principal))
    )
  end

  defp audit_dir() do
    Application.get_env(:flezha, :audit_log, [])
    |> Keyword.get(:dir, "app_data/audit")
  end
end
