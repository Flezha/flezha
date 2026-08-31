defmodule Flezha.Plugin.Provider.Postgres.SinkWorker do
  @moduledoc """
  Per-`plugin_id` transactional Postgres sink. Per flush batch ONE transaction
  upserts every record into `flezha_records` keyed by `flz.key` AND upserts the
  per-Entity `flezha_sink_progress` watermark, so data and progress can never
  diverge.

  Every upsert is guarded by `(epoch, seq)` monotonicity, so a re-driven batch
  and a stale epoch are both no-ops. An unreachable database delays the flush;
  nothing is dropped, and enqueue callers park past `:queue_max`.

  The committed transaction IS the delivery confirmation: it answers the outbox
  hold each guaranteed record carries, which is what advances the controller's
  flush cursor.
  """

  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox

  use GenServer

  require Logger

  @records_table "flezha_records"
  @progress_table "flezha_sink_progress"

  @records_ddl """
  CREATE TABLE IF NOT EXISTS #{@records_table} (
    key text PRIMARY KEY,
    entity text,
    seq text,
    epoch bigint,
    payload bytea,
    updated_at timestamptz
  )
  """

  @progress_ddl """
  CREATE TABLE IF NOT EXISTS #{@progress_table} (
    entity text PRIMARY KEY,
    last_seq text,
    epoch bigint
  )
  """

  # `string_to_array(seq)::bigint[]` orders `<change_seq>.<emit_ordinal>`
  # numerically ("10.0" > "9.1"); the row comparison is the monotonicity
  # guard — not-strictly-newer identities leave the row untouched.
  @records_upsert """
  INSERT INTO #{@records_table} (key, entity, seq, epoch, payload, updated_at)
  SELECT u.key, u.entity, u.seq, u.epoch, u.payload, now()
  FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::bytea[])
       AS u(key, entity, seq, epoch, payload)
  ON CONFLICT (key) DO UPDATE SET
    entity = excluded.entity,
    seq = excluded.seq,
    epoch = excluded.epoch,
    payload = excluded.payload,
    updated_at = excluded.updated_at
  WHERE (excluded.epoch, string_to_array(excluded.seq, '.')::bigint[]) >
        (#{@records_table}.epoch, string_to_array(#{@records_table}.seq, '.')::bigint[])
  """

  @progress_upsert """
  INSERT INTO #{@progress_table} (entity, last_seq, epoch) VALUES ($1, $2, $3)
  ON CONFLICT (entity) DO UPDATE SET
    last_seq = excluded.last_seq,
    epoch = excluded.epoch
  WHERE (excluded.epoch, string_to_array(excluded.last_seq, '.')::bigint[]) >
        (#{@progress_table}.epoch, string_to_array(#{@progress_table}.last_seq, '.')::bigint[])
  """

  defstruct [
    :plugin_id,
    :pool,
    :conn_label,
    :auto_create,
    :linger_ms,
    :max_batch,
    :queue_max,
    :retry_backoff_ms,
    :retry_max_backoff_ms,
    :linger_timer,
    :retry_timer,
    :last_commit_ms,
    :last_error,
    :missing_tables,
    buffer: :queue.new(),
    buffer_len: 0,
    waiters: [],
    consecutive_failures: 0
  ]

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

  @doc """
  Enqueue one output record. Replies `:ok` immediately while the buffer is
  under `:queue_max`; past it the caller parks until a flush frees space —
  delay, never loss.
  """
  @spec enqueue(GenServer.server(), binary(), map()) :: :ok
  def enqueue(server, msg, metadata) when is_map(metadata) do
    GenServer.call(server, {:enqueue, msg, metadata}, :infinity)
  end

  @doc "Synchronous counters read (plugin health/stats contract)."
  @spec stats(GenServer.server()) :: map()
  def stats(server), do: GenServer.call(server, :stats, 5_000)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    conn_opts = Keyword.fetch!(opts, :conn_opts)
    conn_label = Keyword.fetch!(opts, :conn_label)

    case Postgrex.start_link(conn_opts) do
      {:ok, pool} ->
        state = %__MODULE__{
          plugin_id: Keyword.fetch!(opts, :plugin_id),
          pool: pool,
          conn_label: conn_label,
          auto_create: Keyword.get(opts, :auto_create, false),
          linger_ms: Keyword.get(opts, :linger_ms, 25),
          max_batch: Keyword.get(opts, :max_batch, 500),
          queue_max: Keyword.get(opts, :queue_max, 10_000),
          retry_backoff_ms: Keyword.get(opts, :retry_backoff_ms, 250),
          retry_max_backoff_ms: Keyword.get(opts, :retry_max_backoff_ms, 5_000)
        }

        {:ok, state, {:continue, :ensure_schema}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:ensure_schema, state) do
    state =
      if state.auto_create do
        case run_ddl(state) do
          :ok ->
            %{state | missing_tables: nil}

          {:error, reason} ->
            log_failure(state, "schema create failed", reason)
            %{state | last_error: sanitize_error(reason)}
        end
      else
        check_tables(state)
      end

    {:noreply, state}
  end

  @impl true
  def handle_call({:enqueue, msg, metadata}, from, state) do
    state = %{
      state
      | buffer: :queue.in(decode_record(msg, metadata), state.buffer),
        buffer_len: state.buffer_len + 1
    }

    state =
      cond do
        state.buffer_len >= state.max_batch and is_nil(state.retry_timer) ->
          flush(cancel_linger(state))

        is_nil(state.linger_timer) and is_nil(state.retry_timer) ->
          %{state | linger_timer: Process.send_after(self(), :linger, state.linger_ms)}

        true ->
          state
      end

    ProviderBehaviour.park_or_reply(state, from)
  end

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       buffer_len: state.buffer_len,
       consecutive_failures: state.consecutive_failures,
       last_error: state.last_error,
       last_commit_age_ms: commit_age_ms(state),
       missing_tables: state.missing_tables,
       pool_alive: Process.alive?(state.pool)
     }, state}
  end

  @impl true
  def handle_info(:linger, state) do
    {:noreply, flush(%{state | linger_timer: nil})}
  end

  def handle_info(:retry_flush, state) do
    {:noreply, flush(%{state | retry_timer: nil})}
  end

  def handle_info({:EXIT, pid, reason}, %{pool: pid} = state) do
    {:stop, {:pool_exit, reason}, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # Best-effort final flush so a graceful stop does not strand buffered
    # records behind an already-advanced controller flush cursor.
    if state.buffer_len > 0, do: flush(state)
    :ok
  end

  # ---------- flush ----------

  defp cancel_linger(%{linger_timer: nil} = state), do: state

  defp cancel_linger(state) do
    Process.cancel_timer(state.linger_timer)
    %{state | linger_timer: nil}
  end

  defp flush(%{buffer_len: 0} = state), do: state

  defp flush(state) do
    {batch, rest, rest_len} = take_batch(state)

    case commit_batch(state, batch) do
      :ok ->
        # The commit is the confirmation: every guaranteed record in the batch
        # answers its outbox hold in one report, and only now may its cursor
        # advance.
        Outbox.confirm_many(Enum.map(batch, & &1.metadata))

        state = %{
          state
          | buffer: rest,
            buffer_len: rest_len,
            consecutive_failures: 0,
            last_error: nil,
            missing_tables: nil,
            last_commit_ms: System.monotonic_time(:millisecond)
        }

        state = ProviderBehaviour.release_waiters(state)
        flush(state)

      {:error, reason} ->
        failures = state.consecutive_failures + 1
        log_failure(state, "flush failed (attempt #{failures})", reason)

        backoff =
          ProviderBehaviour.retry_backoff_ms(
            state.retry_backoff_ms,
            state.retry_max_backoff_ms,
            failures
          )

        %{
          state
          | consecutive_failures: failures,
            last_error: sanitize_error(reason),
            missing_tables: missing_tables_from(reason) || state.missing_tables,
            retry_timer: Process.send_after(self(), :retry_flush, backoff)
        }
    end
  end

  defp take_batch(state) do
    take = min(state.buffer_len, state.max_batch)
    {taken, rest} = :queue.split(take, state.buffer)
    {:queue.to_list(taken), rest, state.buffer_len - take}
  end

  defp commit_batch(state, batch) do
    records = dedup_newest_by_key(batch)
    progress = progress_rows(batch)

    result =
      Postgrex.transaction(state.pool, fn conn ->
        Postgrex.query!(conn, @records_upsert, record_params(records))

        Enum.each(progress, fn {entity, last_seq, epoch} ->
          Postgrex.query!(conn, @progress_upsert, [entity, last_seq, epoch])
        end)
      end)

    case result do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, e}
  catch
    :exit, reason -> {:error, reason}
  end

  # `ON CONFLICT DO UPDATE` rejects touching one row twice in a statement:
  # collapse same-key records to the newest identity before the upsert.
  defp dedup_newest_by_key(batch) do
    batch
    |> Enum.reduce(%{}, fn rec, acc -> Map.update(acc, rec.key, rec, &newer_of(rec, &1)) end)
    |> Map.values()
  end

  defp newer_of(rec, kept), do: if(rec.sort > kept.sort, do: rec, else: kept)

  defp progress_rows(batch) do
    batch
    |> Enum.reject(&(&1.entity == ""))
    |> Enum.group_by(& &1.entity)
    |> Enum.map(fn {entity, recs} ->
      newest = Enum.max_by(recs, & &1.sort)
      {entity, newest.seq, newest.epoch}
    end)
  end

  defp record_params(records) do
    [
      Enum.map(records, & &1.key),
      Enum.map(records, & &1.entity),
      Enum.map(records, & &1.seq),
      Enum.map(records, & &1.epoch),
      Enum.map(records, & &1.payload)
    ]
  end

  # ---------- record decoding ----------

  # A record earns the guarded upsert by carrying a replay-stable identity —
  # an entity and a numeric seq — NOT by carrying an epoch. A `.keyed()` channel
  # without the effectively-once tier is stamped by the SDK, which cannot mint an
  # epoch (only the controller's outbox lineage owns one), so gating on the epoch
  # would drop those records into the unidentified branch with a stable key but no
  # seq, where the monotonicity guard can never admit a second write for that key.
  #
  # An absent epoch is epoch 0, not a sentinel: the outbox lineage starts at 0 and
  # only a controller-side path (realign, correction, replay) bumps it — and every
  # one of those stamps the epoch. So seq alone orders a tier-off record correctly.
  #
  # Upsert key = `flz.key`, falling back to the identity itself for keyless tier
  # outputs. Fully unstamped records get a unique synthetic key — plain inserts,
  # the monotonicity guard never fires for them.
  defp decode_record(msg, metadata) do
    entity = Map.get(metadata, "flz.entity", "")
    seq = Map.get(metadata, "flz.seq", "")
    epoch = parse_epoch(Map.get(metadata, "flz.epoch")) || 0

    if valid_seq?(seq) and entity != "" do
      key = Map.get(metadata, "flz.key") || "#{entity}/#{seq}"

      %{
        key: key,
        entity: entity,
        seq: seq,
        epoch: epoch,
        sort: sort_key(epoch, seq),
        payload: msg,
        metadata: metadata
      }
    else
      key = Map.get(metadata, "flz.key") || anon_key()

      %{
        key: key,
        entity: "",
        seq: "",
        epoch: 0,
        sort: sort_key(0, ""),
        payload: msg,
        metadata: metadata
      }
    end
  end

  defp anon_key, do: "_anon/#{System.unique_integer([:positive, :monotonic])}"

  defp valid_seq?(seq), do: is_binary(seq) and seq =~ ~r/^\d+(\.\d+)*$/

  defp sort_key(epoch, ""), do: {epoch, []}

  defp sort_key(epoch, seq),
    do: {epoch, seq |> String.split(".") |> Enum.map(&String.to_integer/1)}

  defp parse_epoch(nil), do: nil

  defp parse_epoch(v) when is_binary(v) do
    case Integer.parse(v) do
      {i, ""} -> i
      _ -> nil
    end
  end

  defp parse_epoch(v) when is_integer(v), do: v

  # ---------- schema ----------

  defp run_ddl(state) do
    with {:ok, _} <- Postgrex.query(state.pool, @records_ddl, []),
         {:ok, _} <- Postgrex.query(state.pool, @progress_ddl, []) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp check_tables(state) do
    query = "SELECT to_regclass($1)::text, to_regclass($2)::text"

    case Postgrex.query(state.pool, query, [@records_table, @progress_table]) do
      {:ok, %{rows: [[r, p]]}} ->
        missing =
          Enum.reject([{@records_table, r}, {@progress_table, p}], fn {_, reg} -> reg end)
          |> Enum.map(&elem(&1, 0))

        if missing == [] do
          %{state | missing_tables: nil}
        else
          Logger.error(
            "[postgres-sink] #{conn_desc(state)} missing tables #{inspect(missing)} " <>
              "and auto_create is false — create them or enable auto_create"
          )

          %{state | missing_tables: missing, last_error: {:missing_tables, missing}}
        end

      {:error, reason} ->
        log_failure(state, "table check failed", reason)
        %{state | last_error: sanitize_error(reason)}
    end
  end

  # A flush failing with undefined_table pins the missing-tables verdict so
  # health reports the actionable cause, not a generic commit stall.
  defp missing_tables_from(%Postgrex.Error{postgres: %{code: :undefined_table}}),
    do: [@records_table, @progress_table]

  defp missing_tables_from(_), do: nil

  # ---------- credential-safe logging ----------

  defp log_failure(state, what, reason) do
    Logger.warning("[postgres-sink] #{conn_desc(state)} #{what}: #{sanitize_error(reason)}")
  end

  defp conn_desc(%{conn_label: %{hostname: h, port: p, database: d}}), do: "#{h}:#{p}/#{d}"

  # Only well-known message fields ever reach logs/health — never the
  # connection opts, which would carry password material.
  defp sanitize_error(%Postgrex.Error{postgres: %{code: code, message: m}}), do: "#{code}: #{m}"
  defp sanitize_error(%DBConnection.ConnectionError{message: m}), do: m
  defp sanitize_error(%{__exception__: true} = e), do: "#{e.__struct__}: #{Exception.message(e)}"
  defp sanitize_error({:missing_tables, tables}), do: "missing tables #{inspect(tables)}"
  defp sanitize_error(other), do: inspect(other)

  defp commit_age_ms(%{last_commit_ms: nil}), do: nil

  defp commit_age_ms(%{last_commit_ms: t}), do: System.monotonic_time(:millisecond) - t
end
