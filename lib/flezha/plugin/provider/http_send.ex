defmodule Flezha.Plugin.Provider.HttpSend do
  @moduledoc """
  The one worker a provider sends HTTP requests through: `hook_webhook`'s
  events and `http_push`'s outputs, one request per payload.

  Two ways in. `call/2` answers once its payload is sent or given up, which is
  what a controller hook that waits wants. `push/4` returns once the payload is
  queued: the queue is bounded by payloads and bytes (`Flezha.Process.Backlog`)
  and its `queue_overflow` policy is one of the three every sink names. Under
  `:drop_nothing` the worker raises a full flag, an `:atomics` cell published as
  its registry value, so a dispatcher reads it with the lookup it needs anyway
  and waits, in a call, only while the queue is full.

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
  use Flezha.Process.Timer, keys: [:retry, :block]

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

  @doc """
  Queue `body` with its output `metadata` on the worker `registry` (a provider's
  `WorkerSup` registry) holds under `key`,
  holding a guaranteed output first: `http_push`'s way in. With no worker
  running a guaranteed output stays held, waiting in the outbox, and a plain
  one is dropped, counted as `failed` with the reason `:not_running`.
  """
  @spec push(atom(), {term(), binary()}, binary(), map()) :: :ok
  def push(registry, key, body, metadata) do
    Outbox.hold(metadata)

    case Registry.lookup(registry, key) do
      # The flag is published once the worker's `init/1` runs; until then it is nil.
      [{pid, flag}] ->
        if flag != nil and :atomics.get(flag, 1) == 1,
          do: wait_for_room(pid, body, metadata),
          else: GenServer.cast(pid, {:push, body, metadata})

      [] ->
        unless Outbox.guaranteed?(metadata),
          do:
            ProviderBehaviour.emit(:http_push, :failed, %{count: 1}, %{
              plugin: elem(key, 1),
              reason: :not_running
            })

        :ok
    end
  end

  defp wait_for_room(pid, body, metadata) do
    GenServer.call(pid, {:push, body, metadata}, :infinity)
  catch
    # The worker stopped, and parked what it held.
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
    flag = :atomics.new(1, [])
    publish(opts[:name], flag)

    endpoint = %{
      scheme: String.to_existing_atom(uri.scheme),
      host: uri.host,
      port: uri.port,
      target: Client.target(uri)
    }

    {:ok,
     %{
       conf: conf(opts, uri.host),
       flag: flag,
       backlog: Backlog.new(opts.queue_max_bytes, opts.queue_max),
       waiters: [],
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
      :queue_overflow,
      :queue_block_timeout_ms
    ])
    |> Map.merge(%{
      plugin: opts[:source_plugin_id],
      host: host,
      headers: [{"content-type", opts.content_type} | Map.to_list(opts.headers)]
    })
  end

  defp publish({:via, Registry, {registry, key}}, flag),
    do: Registry.update_value(registry, key, fn _ -> flag end)

  defp publish(_unnamed, _flag), do: :ok

  @impl true
  def handle_call({:call, body}, from, state),
    do: {:noreply, state |> enqueue(entry(body, %{}, from)) |> dispatch()}

  def handle_call({:push, body, metadata}, from, state) do
    entry = entry(body, metadata, nil)

    if full?(state.backlog) do
      state = %{state | waiters: state.waiters ++ [{from, entry}]}

      {:noreply,
       arm_timer(state, {:block, from}, state.conf.queue_block_timeout_ms, {:block_timeout, from})}
    else
      {:reply, :ok, state |> enqueue(entry) |> dispatch()}
    end
  end

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       queued: state.backlog.frames,
       waiting: length(state.waiters),
       in_flight: map_size(state.busy),
       failures: state.failures,
       dropped: state.dropped,
       overflowed: state.overflowed
     }, state}
  end

  @impl true
  def handle_cast({:push, body, metadata}, state),
    do: {:noreply, state |> admit(entry(body, metadata, nil)) |> dispatch()}

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

  def handle_info({:block_timeout, from}, state) do
    state = timer_fired(state, {:block, from})

    case List.keytake(state.waiters, from, 0) do
      {{^from, entry}, waiters} ->
        GenServer.reply(from, :ok)
        {:noreply, overflow(%{state | waiters: waiters}, entry)}

      nil ->
        {:noreply, state}
    end
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
    queued = queued(state.backlog, []) ++ Enum.map(state.waiters, &elem(&1, 1))

    Enum.each(
      queued ++ Map.values(state.busy),
      &park(state, &1, {"http_push.exhausted", %{reason: :stopped}})
    )

    :ok
  end

  @impl true
  def format_status(status),
    do: Map.update(status, :state, nil, &%{&1 | conf: Plugin.redact(&1.conf)})

  defp queued(backlog, acc) do
    case Backlog.out(backlog) do
      {:empty, _} -> Enum.reverse(acc)
      {entry, rest} -> queued(rest, [entry | acc])
    end
  end

  ## Admission

  defp entry(body, metadata, from),
    do: %{body: body, metadata: metadata, from: from, attempt: 0, first_at: nil, headers: nil}

  defp enqueue(state, entry),
    do: put_backlog(state, Backlog.push(state.backlog, entry, byte_size(entry.body)))

  # A cast that was on its way when the queue filled is admitted past the bound
  # under `:drop_nothing`: the flag stops the next one.
  defp admit(%{conf: %{queue_overflow: :drop_nothing}} = state, entry),
    do: enqueue(state, entry)

  defp admit(%{conf: %{queue_overflow: :drop_newest}} = state, entry) do
    if Backlog.fits?(state.backlog, byte_size(entry.body)),
      do: enqueue(state, entry),
      else: overflow(state, entry)
  end

  defp admit(%{conf: %{queue_overflow: :drop_oldest}} = state, entry) do
    size = byte_size(entry.body)

    {backlog, dropped} =
      Backlog.make_room(state.backlog, size, &(not Outbox.guaranteed?(&1.metadata)))

    state = count_overflow(%{state | backlog: backlog}, dropped)

    cond do
      Backlog.fits?(backlog, size) -> enqueue(state, entry)
      backlog.frames == 0 -> overflow(state, entry)
      true -> state |> drop_oldest_held() |> admit(entry)
    end
  end

  defp drop_oldest_held(state) do
    {oldest, backlog} = Backlog.out(state.backlog)
    overflow(%{state | backlog: backlog}, oldest)
  end

  defp overflow(state, entry) do
    if Outbox.guaranteed?(entry.metadata),
      do: park(state, entry, {"http_push.exhausted", %{reason: :queue_full}})

    count_overflow(state, 1)
  end

  defp count_overflow(state, 0), do: state

  defp count_overflow(state, n) do
    emit(state, :overflowed, n, %{policy: state.conf.queue_overflow})
    put_backlog(%{state | overflowed: state.overflowed + n}, state.backlog)
  end

  defp full?(%Backlog{} = b), do: b.frames >= b.max_frames or b.bytes >= b.max_bytes

  # Every change to the queue passes here, which admits the dispatchers waiting
  # for room (only `:drop_nothing` has any) and keeps the flag in step.
  defp put_backlog(state, backlog) do
    case {state.waiters, full?(backlog)} do
      {[{from, entry} | rest], false} ->
        GenServer.reply(from, :ok)
        state = cancel_timer(%{state | waiters: rest}, {:block, from})
        put_backlog(state, Backlog.push(backlog, entry, byte_size(entry.body)))

      {_waiters, full} ->
        raised = full and state.conf.queue_overflow == :drop_nothing
        :atomics.put(state.flag, 1, if(raised, do: 1, else: 0))
        %{state | backlog: backlog}
    end
  end

  ## Sending

  defp dispatch(%{idle: [conn | idle]} = state) do
    case Backlog.out(state.backlog) do
      {:empty, _} ->
        state

      {entry, backlog} ->
        job = %{entry | first_at: System.monotonic_time(:millisecond), headers: identity(entry)}
        state = put_backlog(%{state | idle: idle}, backlog)
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
    Outbox.park(entry.metadata, entry.body, Error.object(Error.new(code, details)), attempts)
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
