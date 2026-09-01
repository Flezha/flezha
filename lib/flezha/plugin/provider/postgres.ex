defmodule Flezha.Plugin.Provider.Postgres do
  @moduledoc """
  Transactional PostgreSQL sink (`:io_binding`): per flush batch, ONE database
  transaction upserts every record into `flezha_records` (keyed by `flz.key`)
  and advances the per-Entity `flezha_sink_progress` watermark row.

  That single transaction is the guarantee: effectively-once into the DB for
  tier operatives, plain inserts for everything else. See
  `docs/resilience/data_durability.md` "Delivery semantics".

  Required opts `:database` and `:username`; the rest, with their defaults,
  are the `@known_opts` table below.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Postgres.{SinkSup, SinkWorker}
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox

  use ProviderBehaviour

  import ProviderBehaviour, only: [convert_opts: 4, parse_bool: 2, parse_int: 2]

  # The one default table. `defstruct`, the option allow-list and the conversion
  # table all derive from it, so a new tunable is added in exactly one place.
  @int_opts [
    port: 5432,
    pool_size: 2,
    linger_ms: 25,
    max_batch: 500,
    queue_max: 10_000,
    retry_backoff_ms: 250,
    retry_max_backoff_ms: 5_000,
    unhealthy_after_ms: 30_000
  ]

  @bool_opts [ssl: false, auto_create: false]

  @plain_opts [:database, :username, :password, :password_env, :plugin_id, :sink_pid]

  # Every option `init_provider/1` interprets, converted once, before the sink
  # worker or its pool exists.
  @converted_fields Keyword.keys(@int_opts) ++ Keyword.keys(@bool_opts)

  # Both clauses below guard on the FIELD, so the table is already total through
  # `parse_int/2` and `parse_bool/2`; no option needs the injected refusal.
  @converted_opts []

  @enforce_keys [:database, :username]
  defstruct @enforce_keys ++
              (@plain_opts -- @enforce_keys) ++ [hostname: "localhost"] ++ @int_opts ++ @bool_opts

  # Declaring `@known_opts` is what makes this provider strict: `use
  # ProviderBehaviour` generates both the `build/1` refusal and `validate_opts/1`
  # — and `init_provider/1` calls the latter, because `Plugin.SpecDecoder` hands
  # a `--plugins` spec straight there without ever constructing the struct.
  @known_opts @plain_opts ++ [:hostname] ++ @converted_fields

  @impl true
  def plugin_type(), do: :io_binding

  def init_provider(opts) do
    with :ok <- validate_opts(opts),
         {:ok, tuned} <- convert_opts(__MODULE__, opts, @converted_fields, &convert_value/2),
         database = Map.fetch!(opts, :database),
         hostname = Map.get(opts, :hostname, "localhost"),
         plugin_id = Map.get(opts, :plugin_id, "default-#{System.unique_integer([:positive])}"),
         worker_opts = worker_opts(opts, tuned, hostname, database, plugin_id),
         {:ok, sink_pid} <- SinkSup.ensure_worker(worker_opts) do
      # The converted values are merged back, so the health probe reads an
      # integer threshold rather than converting one on every probe.
      opts =
        opts
        |> Map.merge(tuned)
        |> Map.put(:plugin_id, plugin_id)
        |> Map.put(:sink_pid, sink_pid)
        |> redact_password()

      {:ok, opts, sink_pid}
    end
  end

  # Only the keys the worker needs and the table cannot supply; the rest are the
  # converted tunables, minus the four the connection and the probe own.
  defp worker_opts(opts, tuned, hostname, database, plugin_id) do
    [
      plugin_id: plugin_id,
      conn_opts: conn_opts(opts, hostname, tuned, database, Map.fetch!(opts, :username)),
      conn_label: %{hostname: hostname, port: tuned.port, database: database}
    ] ++ Map.to_list(Map.drop(tuned, [:port, :pool_size, :ssl, :unhealthy_after_ms]))
  end

  def on_input(%Ctx.IO{} = ctx, opts), do: sink(ctx, opts)
  def on_output(%Ctx.IO{} = ctx, opts), do: sink(ctx, opts)

  defp sink(%Ctx.IO{message: msg, metadata: md} = ctx, opts) do
    # Take ownership of a delivery-guaranteed output before handing it over: the
    # worker answers the hold when the batch's transaction commits. A plain
    # output holds nothing and this is a no-op.
    Outbox.hold(md)
    :ok = SinkWorker.enqueue(Map.fetch!(opts, :sink_pid), msg, md)
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
      [pid: [:sink_pid, :owned_pid], absent: :no_sink, dead: :worker_dead],
      &health_from_stats(&1, opts)
    )
  end

  defp health_from_stats(pid, opts) do
    threshold = Map.get(opts, :unhealthy_after_ms) || @int_opts[:unhealthy_after_ms]

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

  defp conn_opts(opts, hostname, tuned, database, username) do
    # The password travels only inside this closure: it never lands in the
    # worker state, supervisor child args, logs, or telemetry.
    password_fun = password_fun(opts)

    [
      hostname: hostname,
      port: tuned.port,
      database: database,
      username: username,
      ssl: tuned.ssl,
      pool_size: tuned.pool_size,
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

  @int_fields Keyword.keys(@int_opts)
  @bool_fields Keyword.keys(@bool_opts)

  defp convert_value(field, value) when field in @int_fields, do: parse_int(value, field)
  defp convert_value(field, value) when field in @bool_fields, do: parse_bool(value, field)
end
