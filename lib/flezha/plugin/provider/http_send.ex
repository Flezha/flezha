defmodule Flezha.Plugin.Provider.HttpSend do
  @moduledoc """
  The one worker a provider sends HTTP requests through: `hook_webhook`'s
  events and `http_push`'s outputs, one request per payload.

  Two ways in. `call/2` answers once its payload is sent or given up, which is
  what a controller hook that waits wants. A `{:push, body, metadata}` handed
  over by `Flezha.Process.Backlog.dispatch/2` is queued: the queue is bounded by payloads and bytes (`Flezha.Process.Backlog`)
  and its `queue_overflow` policy is one of the three every sink names, read by
  the queue's own admission: under `:drop_nothing` a dispatch waits for room,
  in a call, only while the worker's full flag is up.

  Requests run in `max_in_flight` connection processes
  (`Flezha.Plugin.Provider.HttpSend.Connection`), each owning one kept-alive
  connection, so this process never blocks in a request or a back-off and stays
  answerable to admissions, `stats/1` and health probes. A retry waits on a
  timer and keeps its connection's slot, so with one slot order is kept.

  A 2xx sends; a 5xx, a status the provider names retriable, a timeout or a
  transport error is retried on the provider's backoff row while the attempts
  after the first stay within `retries` and the time since the first within
  `delivery_timeout_ms`; anything else is given up at once. A delivery-
  guaranteed payload given up is parked with its error, never dropped: a hold
  lost here is never settled again. So is one still queued when this process
  stops. A plain one is dropped, counted and warned once per reason.

  Nothing it logs, emits or shows in its status holds the URL's path or query
  or a header value: the host stands for the endpoint.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:retry]

  require Logger

  alias Flezha.Config.Backoff
  alias Flezha.Error
  alias Flezha.Http.Client
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.HttpSend.Connection
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Process.Backlog
  alias Flezha.State.Outbox

  # What a caller that only calls (`call/2`) leaves unsaid: its own wait bounds
  # what it has in flight, so the queue bounds and the policy never bind it.
  @defaults %{
    method: "POST",
    max_in_flight: 1,
    retry_statuses: [],
    ladder_opts: [],
    delivery_timeout_ms: nil,
    queue_max: 1,
    queue_max_bytes: 1,
    queue_overflow: :drop_nothing,
    queue_block_timeout_ms: nil
  }

  @identity [
    {"flz.key", "flz-key"},
    {"flz.cell", "flz-cell"},
    {"flz.seq", "flz-seq"},
    {"flz.epoch", "flz-epoch"}
  ]

  # Mint's own refusals of a request it cannot write: no retry changes them.
  @unsendable [:invalid_request_target, :invalid_header_name, :invalid_header_value]

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts) when is_map(opts),
    do: GenServer.start_link(__MODULE__, opts, Enum.to_list(Map.take(opts, [:name])))

  @doc "Send `body` and answer once it is sent or given up."
  @spec call(GenServer.server(), iodata()) :: :ok
  def call(worker, body) do
    GenServer.call(worker, {:call, IO.iodata_to_binary(body)}, :infinity)
  catch
    # The plugin stopped with this payload in flight, which is lost.
    :exit, _gone -> :ok
  end

  @doc "Counters and the failure run, for health and tests; `%{}` when it does not answer in time."
  @spec stats(GenServer.server()) :: map()
  def stats(worker) do
    GenServer.call(worker, :stats, ProviderBehaviour.probe_budget_ms())
  catch
    :exit, _ -> %{}
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    opts = Map.merge(@defaults, opts)
    uri = URI.parse(opts.url)
    flag = Backlog.flag(opts[:name])

    endpoint = %{
      scheme: String.to_existing_atom(uri.scheme),
      host: uri.host,
      port: uri.port,
      target: Client.target(uri)
    }

    {:ok,
     %{
       conf: conf(opts, uri.host),
       backlog:
         Backlog.new(opts.queue_max_bytes, opts.queue_max,
           policy: opts.queue_overflow,
           block_ms: opts.queue_block_timeout_ms,
           flag: flag
         ),
       idle:
         for(_ <- 1..opts.max_in_flight, do: Connection.start_link(endpoint, opts.timeout_ms)),
       busy: %{},
       timers: %{},
       failures: 0,
       dropped: 0,
       overflowed: 0,
       warned: MapSet.new()
     }}
  end

  defp conf(opts, host) do
    opts
    |> Map.take([
      :provider,
      :method,
      :retries,
      :retry_statuses,
      :ladder,
      :ladder_opts,
      :delivery_timeout_ms,
      :queue_overflow
    ])
    |> Map.merge(%{
      plugin: opts[:source_plugin_id],
      host: host,
      headers: [{"content-type", opts.content_type} | Map.to_list(opts.headers)]
    })
  end

  @impl true
  def handle_call({:call, body}, from, state),
    do: {:noreply, state |> enqueue(entry(body, %{}, from)) |> dispatch()}

  def handle_call({:push, body, metadata}, from, state),
    do: {:noreply, state |> admit(entry(body, metadata, nil), {:wait, from}) |> dispatch()}

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       queued: state.backlog.frames,
       waiting: Backlog.waiting(state.backlog),
       in_flight: map_size(state.busy),
       failures: state.failures,
       dropped: state.dropped,
       overflowed: state.overflowed
     }, state}
  end

  @impl true
  def handle_cast({:push, body, metadata}, state),
    do: {:noreply, state |> admit(entry(body, metadata, nil), :cast) |> dispatch()}

  @impl true
  def handle_info({:response, conn, result}, state) do
    case Map.pop(state.busy, conn) do
      {nil, _busy} -> {:noreply, state}
      {job, busy} -> {:noreply, settle(%{state | busy: busy}, conn, job, outcome(result, state))}
    end
  end

  def handle_info({:retry, conn}, state) do
    state = timer_fired(state, {:retry, conn})

    case Map.fetch(state.busy, conn) do
      {:ok, job} -> {:noreply, send_job(state, conn, job)}
      :error -> {:noreply, state}
    end
  end

  def handle_info({:block_timeout, _tag, waiter}, state) do
    {backlog, overflowed} = Backlog.block_timeout(state.backlog, waiter)
    {:noreply, overflow(%{state | backlog: backlog}, overflowed)}
  end

  # A connection process ended under it: what it held is given up with the rest.
  def handle_info({:EXIT, pid, reason}, state) do
    if Map.has_key?(state.busy, pid) or pid in state.idle,
      do: {:stop, {:connection_down, reason}, state},
      else: {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.idle ++ Map.keys(state.busy), &Connection.stop/1)
    {waiting, backlog} = Backlog.release_waiting(state.backlog)
    queued = Backlog.to_list(backlog) ++ waiting

    Enum.each(
      queued ++ Map.values(state.busy),
      &park(state, &1, {"http_push.exhausted", %{reason: :stopped}})
    )

    :ok
  end

  @impl true
  def format_status(status),
    do: Map.update(status, :state, nil, &%{&1 | conf: Plugin.redact(&1.conf)})

  ## Admission

  defp entry(body, metadata, from),
    do: %{body: body, metadata: metadata, from: from, attempt: 0, first_at: nil, headers: nil}

  defp enqueue(state, entry),
    do: %{state | backlog: Backlog.push(state.backlog, entry, byte_size(entry.body))}

  # `:drop_oldest` drops plain payloads first and a guaranteed one only once no
  # plain one is left; under it every arrival is a cast, so offering again
  # answers no waiter twice.
  defp admit(state, entry, waiter) do
    size = byte_size(entry.body)

    case Backlog.offer(state.backlog, entry, size, waiter, &(not Outbox.guaranteed?(&1.metadata))) do
      {:refused, %{policy: :drop_oldest, frames: held} = backlog, plain} when held > 0 ->
        {verdict, backlog, dropped} = Backlog.offer(backlog, entry, size, waiter)
        admitted(state, verdict, backlog, plain ++ dropped, entry)

      {verdict, backlog, dropped} ->
        admitted(state, verdict, backlog, dropped, entry)
    end
  end

  defp admitted(state, :refused, backlog, dropped, entry),
    do: overflow(%{state | backlog: backlog}, dropped ++ [entry])

  defp admitted(state, _queued_or_waiting, backlog, dropped, _entry),
    do: overflow(%{state | backlog: backlog}, dropped)

  defp overflow(state, entries) do
    for entry <- entries,
        Outbox.guaranteed?(entry.metadata),
        do: park(state, entry, {"http_push.exhausted", %{reason: :queue_full}})

    case length(entries) do
      0 ->
        state

      n ->
        emit(state, :overflowed, n, %{policy: state.conf.queue_overflow})
        %{state | overflowed: state.overflowed + n}
    end
  end

  ## Sending

  defp dispatch(%{idle: [conn | idle]} = state) do
    case Backlog.out(state.backlog) do
      {:empty, _} ->
        state

      {entry, backlog} ->
        job = %{entry | first_at: System.monotonic_time(:millisecond), headers: identity(entry)}
        state = %{state | idle: idle, backlog: Backlog.room(backlog)}
        state |> send_job(conn, job) |> dispatch()
    end
  end

  defp dispatch(state), do: state

  defp send_job(state, conn, job) do
    Connection.request(conn, {state.conf.method, state.conf.headers ++ job.headers, job.body})
    %{state | busy: Map.put(state.busy, conn, job)}
  end

  # The output's identity, fixed for every attempt at it.
  defp identity(entry) do
    stamps =
      for {stamp, header} <- @identity,
          value = entry.metadata[stamp],
          value not in [nil, ""],
          do: {header, encode(to_string(value))}

    case Outbox.dedup_key(entry.metadata) do
      nil -> stamps
      durable -> stamps ++ [{"idempotency-key", encode(durable)}]
    end
  end

  # Visible ASCII is what a header value may carry; the rest, and `%`, is
  # percent-encoded, so a non-ASCII key reaches the receiver intact.
  defp encode(value), do: URI.encode(value, &(&1 in ?!..?~ and &1 != ?%))

  ## Answers

  defp outcome({:ok, status, _headers}, _state) when status in 200..299, do: :ok

  defp outcome({:ok, status, headers}, state) do
    if status >= 500 or status in state.conf.retry_statuses,
      do: {:retry, status, retry_after(headers)},
      else: {:give_up, {"http_push.rejected", %{status: status}}, status}
  end

  defp outcome({:error, %Mint.HTTPError{reason: reason}}, _state) do
    tag = reason_tag(reason)

    if tag in @unsendable,
      do: {:give_up, {"http_push.exhausted", %{reason: tag}}, tag},
      else: {:retry, tag, nil}
  end

  defp outcome({:error, reason}, _state), do: {:retry, reason_tag(reason), nil}

  # The tag alone: a reason's payload can quote a header value or the target.
  defp reason_tag(%{reason: reason}), do: reason_tag(reason)
  defp reason_tag(reason) when is_atom(reason), do: reason
  defp reason_tag(reason) when is_tuple(reason) and is_atom(elem(reason, 0)), do: elem(reason, 0)
  defp reason_tag(_reason), do: :error

  defp retry_after(headers) do
    with value when is_binary(value) <- Client.header(headers, "retry-after"),
         {seconds, ""} when seconds >= 0 <- Integer.parse(value),
         do: seconds,
         else: (_ -> nil)
  end

  defp settle(state, conn, job, :ok) do
    if Outbox.guaranteed?(job.metadata), do: Outbox.confirm(job.metadata)
    reply(job)
    emit(state, :shipped, 1, %{})
    dispatch(%{state | failures: 0, idle: [conn | state.idle]})
  end

  defp settle(state, conn, job, {:retry, why, wait}) do
    state = failed(state)
    job = %{job | attempt: job.attempt + 1}

    if retry?(state.conf, job) do
      emit(state, :retried, 1, %{reason: why})
      delay = retry_delay(state.conf, job.attempt, wait)

      arm_timer(
        %{state | busy: Map.put(state.busy, conn, job)},
        {:retry, conn},
        delay,
        {:retry, conn}
      )
    else
      give_up(state, conn, job, {"http_push.exhausted", %{reason: why}}, why)
    end
  end

  defp settle(state, conn, job, {:give_up, error, why}),
    do: give_up(failed(state), conn, %{job | attempt: job.attempt + 1}, error, why)

  defp failed(state), do: %{state | failures: state.failures + 1}

  defp retry?(conf, job) do
    job.attempt <= conf.retries and
      (conf.delivery_timeout_ms == nil or
         System.monotonic_time(:millisecond) - job.first_at < conf.delivery_timeout_ms)
  end

  # A `retry-after` lengthens the wait up to the curve's own ceiling, where the
  # provider gives one.
  defp retry_delay(conf, attempt, retry_after) do
    delay = Backoff.arm(conf.ladder, attempt, conf.plugin, conf.ladder_opts)

    case {retry_after, conf.ladder_opts[:cap]} do
      {seconds, cap} when is_integer(seconds) and is_integer(cap) ->
        max(delay, min(:timer.seconds(seconds), cap))

      _none ->
        delay
    end
  end

  defp give_up(state, conn, job, error, why) do
    state =
      if Outbox.guaranteed?(job.metadata),
        do: park(state, job, error),
        else: drop(state, why)

    reply(job)
    emit(state, :failed, 1, %{reason: why})
    dispatch(%{state | idle: [conn | state.idle]})
  end

  # The attempts a park records are the ones made: `attempt` counts the
  # answers already settled, and a payload never sent made none.
  defp park(state, entry, {code, details}) do
    attempts = if entry.first_at, do: max(entry.attempt, 1), else: 0
    Outbox.park(entry.metadata, entry.body, Error.new(code, details), attempts)
    state
  end

  defp drop(state, why) do
    unless MapSet.member?(state.warned, why) do
      Logger.warning(
        "[#{state.conf.provider}] #{state.conf.plugin} dropped a request: " <>
          "#{state.conf.host} answered #{inspect(why)}"
      )
    end

    %{state | dropped: state.dropped + 1, warned: MapSet.put(state.warned, why)}
  end

  defp reply(%{from: nil}), do: :ok
  defp reply(%{from: from}), do: GenServer.reply(from, :ok)

  # Each provider's family is named literally, which is how the telemetry
  # seam reads the events this module writes.
  defp emit(state, verb, count, metadata) do
    metadata = Map.put(metadata, :plugin, state.conf.plugin)

    case state.conf.provider do
      :hook_webhook -> ProviderBehaviour.emit(:hook_webhook, verb, %{count: count}, metadata)
      :http_push -> ProviderBehaviour.emit(:http_push, verb, %{count: count}, metadata)
    end
  end
end
