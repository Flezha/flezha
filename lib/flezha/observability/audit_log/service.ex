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

  The effects — the catalog, a filtered read, the tail subscription, the stats
  snapshot and a rotation — are public so `Flezha.Service.Cli.Audit` renders
  the same answers under the same rows.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ
  alias Flezha.Config
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

  @spec list_files(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list_files(caller_context), verb: :audit_list do
    catalog()
  end

  @spec read(read_params(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb read(params, caller_context) when is_map(params),
    verb: :audit_read,
    target: &read_target/1 do
    read_records(params, caller_context)
  end

  @spec tail(map(), caller_context()) :: {:ok, reference()} | {:error, term()}
  verb tail(params, caller_context), verb: :audit_tail do
    subscribe_tail()
  end

  @spec unsubscribe(reference(), caller_context()) :: :ok | {:error, term()}
  verb unsubscribe(ref, caller_context) when is_reference(ref), verb: :audit_unsubscribe do
    with :ok <- check_enabled(), do: Writer.unsubscribe_tail(ref)
  end

  @spec stats(caller_context()) :: {:ok, map()} | {:error, term()}
  verb stats(caller_context), verb: :audit_stats do
    stats_snapshot()
  end

  @spec rotate(caller_context()) :: {:ok, map()} | {:error, term()}
  verb rotate(caller_context), verb: :audit_rotate do
    rotate_now()
  end

  # ---------- The effects the terminal shares ----------

  @doc false
  @spec catalog() :: {:ok, [map()]} | {:error, term()}
  def catalog do
    with :ok <- check_enabled(), do: {:ok, Writer.catalog()}
  end

  @doc false
  @spec read_records(read_params(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  def read_records(params, caller_context) do
    with :ok <- check_enabled() do
      path = Path.join(audit_dir(), Map.get(params, :file) || "audit-current.jsonl")

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

  @doc false
  @spec subscribe_tail() :: {:ok, reference()} | {:error, term()}
  def subscribe_tail do
    with :ok <- check_enabled(), do: Writer.subscribe_tail()
  end

  @doc false
  @spec stats_snapshot() :: {:ok, map()} | {:error, term()}
  def stats_snapshot do
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

  @doc false
  @spec rotate_now() :: {:ok, map()} | {:error, term()}
  def rotate_now do
    with :ok <- check_enabled(), do: Rotation.rotate_now()
  end

  # ---------- Internal helpers ----------

  defp read_target([params | _]), do: %{file: Map.get(params, :file)}

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

  defp audit_dir() do
    Config.get(:audit_log_dir)
  end
end
