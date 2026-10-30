defmodule Flezha.Plugin.Provider.PostgresPoll.Reader do
  @moduledoc """
  The reading half of `Flezha.Plugin.Provider.PostgresPoll`: one process per
  binding, owning one connection, the cursor, the poll timer and the health
  facts.

  Each read runs in a monitored child (`PostgresPoll.Query.read/4`), so the
  worker answers its health probe while a slow query is outstanding. The rows
  it answers are handed to the relay with `Host.inject_many/4` in
  `:verdict` mode, and the cursor moves past the longest prefix the relay
  took: a refused row is read again. The committed cursor follows
  `Host.commit_position/3`: after every confirmed batch when the relay's
  input is durable, otherwise once caught up and at most every
  `commit_interval_ms` before, and at stop.

  Log lines, telemetry and verdicts name the table, a count or a server's error
  code and message, never a row or the connection's options.
  """

  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.Postgres.Connection
  alias Flezha.Plugin.Provider.PostgresPoll.Query
  alias Flezha.Plugin.Provider.Worker
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Relay.Ingest.Verdict

  # The facts the provider's `health/1` judges: a missing table or column, the
  # failure run, its last error and the time of the last settled read. A probe
  # that times out is also what a reader waiting out a parked relay's ingest
  # looks like.
  use Worker, facts: [:missing, :consecutive_failures, :last_error, :last_settled_at]
  use Flezha.Process.Timer, keys: [:poll]

  require Verdict
  require Logger

  # The prefix is derived from the provider's id; nothing here names a root.
  @provider "postgres_poll"

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)

  @doc """
  The reader's start arguments, from the plugin's prepared options: what it
  reads (`PostgresPoll.Query.spec/1`), where it commits, and its one connection,
  with no pool and a session in UTC, so the text of a timestamp cursor is the
  same on every controller. The password reaches it only inside the
  connection's closure.
  """
  @spec args(map()) :: map()
  def args(opts) do
    opts
    |> Map.take([
      :source_plugin_id,
      :overwatcher_pid,
      :durable_input,
      :commit_interval_ms,
      :cursor
    ])
    |> Map.merge(%{
      conn_opts: Connection.conn_opts(opts, pool_size: 1, parameters: [TimeZone: "UTC"]),
      # What a log line names: the table and where it is, never a credential.
      where: "table=#{opts.table} #{Connection.describe(Connection.label(opts))}",
      spec: Query.spec(opts)
    })
  end

  @impl true
  def init(opts) do
    # A stop commits the cursor, which only a trapped shutdown reaches.
    Process.flag(:trap_exit, true)

    case Postgrex.start_link(opts.conn_opts) do
      {:ok, conn} ->
        now = now()

        state =
          opts
          |> Map.drop([:name, :conn_opts])
          |> Map.merge(Worker.fields(opts))
          |> Map.merge(%{
            conn: conn,
            plan: nil,
            committed: opts.cursor,
            committed_at: now,
            read: nil,
            missing: nil,
            caught_up: false
          })

        # Scheduled, never run here: activation is synchronous in the host.
        {:ok, arm_timer(state, :poll, 0, :poll)}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:poll, state), do: {:noreply, state |> timer_fired(:poll) |> start_read()}

  def handle_info({:read_done, pid, result}, %{read: {pid, ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, settle(%{state | read: nil}, result)}
  end

  def handle_info({:DOWN, ref, :process, _child, reason}, %{read: {_pid, ref}} = state),
    do: {:noreply, settle(%{state | read: nil}, {:error, {:exit, reason}})}

  def handle_info({:EXIT, conn, reason}, %{conn: conn} = state),
    do: {:stop, {:connection_exit, reason}, state}

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state.read, do: Process.exit(elem(state.read, 0), :kill)
    _ = Host.commit_position(state, :cursor, :stopped)
    Process.exit(state.conn, :shutdown)
    :ok
  end

  # ---------- one read ----------

  defp start_read(%{read: nil} = state) do
    worker = self()
    {conn, spec, plan, cursor} = {state.conn, state.spec, state.plan, state.cursor}

    {pid, ref} =
      spawn_monitor(fn ->
        send(worker, {:read_done, self(), Query.read(conn, spec, plan, cursor)})
      end)

    %{state | read: {pid, ref}}
  end

  defp start_read(state), do: state

  defp settle(state, result) do
    {next, state} =
      case result do
        {:ok, plan, cursor, records} ->
          deliver(%{state | plan: plan, cursor: cursor, missing: nil}, records)

        {:error, reason} ->
          {:waiting, failed(state, reason)}
      end

    Worker.settled(state)
    |> Host.commit_position(:cursor, next)
    |> schedule(next)
  end

  defp schedule(state, :more), do: arm_timer(state, :poll, 0, :poll)

  defp schedule(state, _caught_up_or_waiting),
    do: arm_timer(state, :poll, state.spec.interval_ms, :poll)

  # ---------- delivery ----------

  defp deliver(state, []),
    do: {:caught_up, state |> Worker.recovered(subject(state)) |> caught_up()}

  defp deliver(state, records) do
    verdicts =
      Host.inject_many(
        state.overwatcher_pid,
        Enum.map(records, fn {payload, metadata, _at} -> {payload, metadata} end),
        :verdict,
        :infinity
      )

    {taken, refused} =
      records
      |> Enum.zip(verdicts)
      |> Enum.split_while(fn {_record, verdict} -> Verdict.taken?(verdict) end)

    state = advance(state, taken)

    case refused do
      [] when length(records) == state.spec.batch_size -> {:more, %{state | caught_up: false}}
      [] -> {:caught_up, caught_up(state)}
      [{_record, {:error, reason}} | _] -> {:waiting, failed(state, {:ingest, reason})}
    end
  end

  defp advance(state, []), do: state

  defp advance(state, taken) do
    {{_payload, _metadata, at}, _taken} = List.last(taken)
    emit(:shipped, %{count: length(taken)}, labels(state))
    Worker.recovered(%{state | cursor: %{state.cursor | at: at}}, subject(state))
  end

  defp caught_up(%{caught_up: true} = state), do: state

  defp caught_up(state) do
    emit(:caught_up, %{count: 1}, labels(state))
    %{state | caught_up: true}
  end

  # ---------- failures ----------

  # A missing table or column is reported at once, and logged once. A read that
  # names one the plan was built with reads the catalog again next time.
  defp failed(state, {kind, _name} = missing) when kind in [:table_missing, :column_missing] do
    if state.missing != missing do
      Logger.warning(
        "[Plugin.Provider.PostgresPoll] #{state.where} #{inspect(missing)}; " <>
          "reading again every #{state.spec.interval_ms} ms"
      )

      emit(
        :failed,
        %{consecutive_failures: state.consecutive_failures},
        reason_labels(state, kind)
      )
    end

    %{state | missing: missing, plan: nil}
  end

  defp failed(state, reason) do
    error = error_text(reason)
    state = Worker.failed(state, error, subject(state))
    failures = state.consecutive_failures

    emit(:failed, %{consecutive_failures: failures}, reason_labels(state, error))
    %{state | plan: replan(state.plan, reason)}
  end

  defp subject(state), do: "[Plugin.Provider.PostgresPoll] read #{state.where}"

  defp replan(_plan, %Postgrex.Error{postgres: %{code: code}})
       when code in [:undefined_table, :undefined_column],
       do: nil

  defp replan(plan, _reason), do: plan

  defp error_text({:ingest, reason}), do: "ingest refused: #{inspect(reason)}"
  defp error_text(reason), do: Connection.sanitize_error(reason)

  defp labels(%{spec: spec}),
    do: %{plugin_id: spec.plugin_id, table: spec.table, channel: spec.entry_channel}

  defp reason_labels(state, reason), do: Map.put(labels(state), :reason, reason)

  defp now(), do: :erlang.monotonic_time(:millisecond)
end
