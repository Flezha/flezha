defmodule Flezha.Plugin.Provider.Postgres do
  @moduledoc """
  Transactional PostgreSQL sink (`:io_binding`): per flush batch, ONE database
  transaction upserts every record into `flezha_records` (keyed by `flz.key`)
  and advances the per-Entity `flezha_sink_progress` watermark row.

  That single transaction is the guarantee: effectively-once into the DB for
  tier operatives, plain inserts for everything else. See
  `docs/resilience/data_durability.md` "Delivery semantics".

  Required opts `:database` and `:username`; the rest, with their defaults, are
  the option rows below and `docs/plugins/postgres_sink.md`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Postgres.SinkWorker
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox

  use ProviderBehaviour,
    options: [
      database: [type: :string, required: true, doc: "Database name"],
      username: [type: :string, required: true, doc: "Role the sink connects as"],
      password: [type: :string, doc: "Password, redacted once the plugin is prepared"],
      password_env: [type: :string, doc: "Environment variable holding the password instead"],
      plugin_id: [type: :string, doc: "Plugin id; defaults to a generated one"],
      hostname: [type: :string, default: "localhost", doc: "Database host"],
      port: [type: :int, default: 5432, min: 1, max: 65_535, doc: "TCP port"],
      pool_size: [type: :int, default: 2, min: 1, doc: "Connections in the sink's pool"],
      linger_ms: [type: :int, default: 25, min: 1, doc: "How long a batch waits to fill"],
      max_batch: [type: :int, default: 500, min: 1, doc: "Records in one flush transaction"],
      queue_max: [type: :int, default: 10_000, min: 1, doc: "Records held before enqueue blocks"],
      retry_backoff_ms: [
        type: :int,
        default: 250,
        min: 1,
        doc: "First delay after a failed flush"
      ],
      retry_max_backoff_ms: [type: :int, default: 5_000, min: 1, doc: "Ceiling for that delay"],
      unhealthy_after_ms: [
        type: :int,
        default: 30_000,
        min: 1,
        doc: "Age of the last commit past which a backed-up sink is unhealthy"
      ],
      ssl: [type: :bool, default: false, doc: "TLS to the server"],
      auto_create: [type: :bool, default: false, doc: "Create the target tables when missing"]
    ],
    worker: [module: SinkWorker, restart: :permanent]

  @impl true
  def init_provider(opts) do
    plugin_id = Map.get(opts, :plugin_id) || "default-#{System.unique_integer([:positive])}"

    {:ok,
     opts
     |> Map.put(:plugin_id, plugin_id)
     |> redact_password()}
  end

  @doc """
  One sink worker per plugin, taking the connection the options describe. The
  password travels only inside the `configure` closure: it never lands in the
  worker state, the supervisor's child args, logs or telemetry.
  """
  @impl true
  def worker_specs(opts) do
    [
      {key(opts),
       %{
         plugin_id: opts.plugin_id,
         conn_opts: conn_opts(opts),
         conn_label: %{
           hostname: opts.hostname,
           port: opts.port,
           database: opts.database
         },
         linger_ms: opts.linger_ms,
         max_batch: opts.max_batch,
         queue_max: opts.queue_max,
         retry_backoff_ms: opts.retry_backoff_ms,
         retry_max_backoff_ms: opts.retry_max_backoff_ms,
         auto_create: opts.auto_create
       }}
    ]
  end

  @impl true
  def on_input(%Ctx.IO{} = ctx, opts), do: sink(ctx, opts)
  @impl true
  def on_output(%Ctx.IO{} = ctx, opts), do: sink(ctx, opts)

  defp sink(%Ctx.IO{message: msg, metadata: md} = ctx, opts) do
    # Take ownership of a delivery-guaranteed output before handing it over: the
    # worker answers the hold when the batch's transaction commits. A plain
    # output holds nothing and this is a no-op.
    Outbox.hold(md)
    :ok = SinkWorker.enqueue(worker(opts), msg, md)
    {:ok, ctx}
  end

  @doc """
  Health verdict (plugin health contract): unhealthy when the worker is
  gone, the target tables are missing with `auto_create` off, or no flush
  transaction has committed within `:unhealthy_after_ms` while records are
  pending — the convergence loop escalates by restarting the provider.
  """
  @impl true
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    ProviderBehaviour.worker_health(
      opts,
      [absent: :no_sink, dead: :worker_dead],
      &health_from_stats(&1, opts)
    )
  end

  defp health_from_stats(pid, opts) do
    threshold = opts.unhealthy_after_ms

    case SinkWorker.stats(pid) do
      %{missing_tables: tables} when is_list(tables) and tables != [] ->
        {:unhealthy, {:missing_tables, tables}}

      %{buffer_len: pending, consecutive_failures: failures, last_error: err} = stats
      when pending > 0 and failures > 0 ->
        if stalled?(stats, threshold),
          do: {:unhealthy, {:commit_stalled, err}},
          else: :healthy

      %{} ->
        :healthy
    end
  end

  defp stalled?(%{last_commit_age_ms: nil}, _threshold), do: true
  defp stalled?(%{last_commit_age_ms: age}, threshold), do: age > threshold

  defp conn_opts(opts) do
    password_fun = password_fun(opts)

    [
      hostname: opts.hostname,
      port: opts.port,
      database: opts.database,
      username: opts.username,
      ssl: opts.ssl,
      pool_size: opts.pool_size,
      configure: fn conn_opts ->
        case password_fun.() do
          nil -> conn_opts
          password -> Keyword.put(conn_opts, :password, password)
        end
      end
    ]
  end

  defp password_fun(opts) do
    case {Map.get(opts, :password), Map.get(opts, :password_env)} do
      {password, _} when is_binary(password) and password != "" -> fn -> password end
      {_, env} when is_binary(env) and env != "" -> fn -> System.get_env(env) end
      _ -> fn -> nil end
    end
  end

  defp redact_password(%{password: password} = opts)
       when is_binary(password) and password != "",
       do: Map.put(opts, :password, "**redacted**")

  defp redact_password(opts), do: opts

  # The key this provider's one worker is registered under, named once and read
  # by both the declaration and the dispatch path.
  defp key(opts), do: Map.fetch!(opts, :plugin_id)

  defp worker(opts), do: WorkerSup.worker(__MODULE__, key(opts))
end
