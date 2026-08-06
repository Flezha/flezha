defmodule Flezha.Plugin.Provider.Postgres do
  @moduledoc """
  Transactional PostgreSQL sink (`:io_binding`): per flush batch, ONE
  database transaction upserts every record into `flezha_records` (keyed by
  `flz.key`) and advances the per-Entity `flezha_sink_progress` watermark
  row — effectively-once into the DB for tier operatives, plain inserts for
  everything else. See `docs/resilience/data_durability.md` "Delivery
  semantics".

  Required opts:
    * `:database` — target database name
    * `:username` — connection role

  Optional opts (defaults below):
    * `:hostname`             — string, default `"localhost"`
    * `:port`                 — int, default `5432`
    * `:password`             — string; prefer `:password_env`
    * `:password_env`         — env var holding the password (never logged)
    * `:ssl`                  — bool, default `false`
    * `:pool_size`            — int, default `2`
    * `:linger_ms`            — int, default `25` (batch window)
    * `:max_batch`            — int, default `500` (records per transaction)
    * `:queue_max`            — int, default `10_000` (then callers park)
    * `:auto_create`          — bool, default `false` (opt-in DDL)
    * `:retry_backoff_ms`     — int, default `250`
    * `:retry_max_backoff_ms` — int, default `5_000`
    * `:unhealthy_after_ms`   — int, default `30_000` (commit-stall verdict)
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Postgres.{SinkSup, SinkWorker}
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour

  import ProviderBehaviour,
    only: [coerce_bool: 1, coerce_int: 1, parse_bool: 2, parse_int: 2]

  @enforce_keys [:database, :username]
  defstruct @enforce_keys ++
              [
                :password,
                :password_env,
                :plugin_id,
                :sink_pid,
                hostname: "localhost",
                port: 5432,
                ssl: false,
                pool_size: 2,
                linger_ms: 25,
                max_batch: 500,
                queue_max: 10_000,
                auto_create: false,
                retry_backoff_ms: 250,
                retry_max_backoff_ms: 5_000,
                unhealthy_after_ms: 30_000
              ]

  @impl true
  def plugin_type(), do: :io_binding

  def init_provider(opts) do
    database = Map.fetch!(opts, :database)
    username = Map.fetch!(opts, :username)
    hostname = Map.get(opts, :hostname, "localhost")
    port = coerce_int(Map.get(opts, :port, 5432))
    plugin_id = Map.get(opts, :plugin_id, "default-#{System.unique_integer([:positive])}")

    worker_opts = [
      plugin_id: plugin_id,
      conn_opts: conn_opts(opts, hostname, port, database, username),
      conn_label: %{hostname: hostname, port: port, database: database},
      auto_create: coerce_bool(Map.get(opts, :auto_create, false)),
      linger_ms: coerce_int(Map.get(opts, :linger_ms, 25)),
      max_batch: coerce_int(Map.get(opts, :max_batch, 500)),
      queue_max: coerce_int(Map.get(opts, :queue_max, 10_000)),
      retry_backoff_ms: coerce_int(Map.get(opts, :retry_backoff_ms, 250)),
      retry_max_backoff_ms: coerce_int(Map.get(opts, :retry_max_backoff_ms, 5_000))
    ]

    case SinkSup.ensure_worker(worker_opts) do
      {:ok, sink_pid} ->
        opts =
          opts
          |> Map.put(:port, port)
          |> Map.put(:plugin_id, plugin_id)
          |> Map.put(:sink_pid, sink_pid)
          |> redact_password()

        {:ok, opts, sink_pid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def on_input(%Ctx.IO{} = ctx, opts), do: sink(ctx, opts)
  def on_output(%Ctx.IO{} = ctx, opts), do: sink(ctx, opts)

  defp sink(%Ctx.IO{message: msg, metadata: md} = ctx, opts) do
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
    threshold = coerce_int(Map.get(opts, :unhealthy_after_ms, 30_000))

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

  defp conn_opts(opts, hostname, port, database, username) do
    # The password travels only inside this closure: it never lands in the
    # worker state, supervisor child args, logs, or telemetry.
    password_fun = password_fun(opts)

    [
      hostname: hostname,
      port: port,
      database: database,
      username: username,
      ssl: coerce_bool(Map.get(opts, :ssl, false)),
      pool_size: coerce_int(Map.get(opts, :pool_size, 2)),
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

  @int_opts [
    :port,
    :pool_size,
    :linger_ms,
    :max_batch,
    :queue_max,
    :retry_backoff_ms,
    :retry_max_backoff_ms,
    :unhealthy_after_ms
  ]

  defp convert_value(field, value) when field in @int_opts, do: parse_int(value, field)

  defp convert_value(field, value) when field in [:ssl, :auto_create],
    do: parse_bool(value, field)
end
