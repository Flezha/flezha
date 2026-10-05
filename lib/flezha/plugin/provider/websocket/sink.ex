defmodule Flezha.Plugin.Provider.Websocket.Sink do
  @moduledoc """
  A websocket plugin's outbound half: what the socket SHIPS upstream, as opposed
  to what it asserts (`Websocket.Subscription`) or how it connects
  (`Websocket.Client`).

  Policy only — the bounded queue, the frame render, the correlation id and the
  settlement rules. It opens no socket, holds no connection state and arms no
  timer; the Client owns all three and calls in here, on the split the area
  already uses.

  Absent (`from_opts/1` returning `{:ok, nil}`) is a plugin bound in neither
  direction: it allocates no queue and settles nothing.

  Delivery is what `sink_ack_mode` declares and nothing more. `"none"` answers
  the outbox hold when the frame reaches the established socket, which is
  at-most-once past that point. `"required"` answers it only when an inbound
  frame echoes the correlation id, which is at-least-once — and because a repeat
  is then a normal event, that mode requires a template, through which the frame
  carries the correlation id and the `flz.cell` / `flz.seq` stamps a consumer
  dedups on. Neither mode is effectively-once: a websocket exposes no
  transaction to enlist and no key to overwrite, so the dedup is the consumer's.
  """

  alias Flezha.Config.Backoff
  alias Flezha.Plugin.Provider.Websocket.Subscription
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox

  @payload_placeholder "{{payload}}"
  @cell_placeholder "{{cell}}"
  @seq_placeholder "{{seq}}"

  @enforce_keys [:frame_type, :ack_mode, :queue_max]
  defstruct @enforce_keys ++
              [
                template: nil,
                # `park_or_reply/2` and `release_waiters/1` read these three by
                # name: the bounded-queue policy is theirs, not this module's.
                queue: nil,
                buffer_len: 0,
                waiters: [],
                # `"required"` only: correlation id => {entry, attempts,
                # sent_at}. An id is a string, so an upstream echoing one can
                # never be read as a subscription delta's integer.
                inflight: %{},
                next_req_id: 1
              ]

  @typedoc "One queued output: the frame to send, its metadata, its id or `nil`."
  @type entry() :: {{:text | :binary, binary()}, map(), binary() | nil}

  @type t() :: %__MODULE__{
          frame_type: :text | :binary,
          template: binary() | nil,
          ack_mode: binary(),
          queue_max: pos_integer(),
          queue: :queue.queue(entry()) | nil,
          buffer_len: non_neg_integer(),
          waiters: [GenServer.from()],
          inflight: %{binary() => {entry(), pos_integer(), integer()}},
          next_req_id: pos_integer()
        }

  @doc "The placeholder a `sink_template` must carry, for error messages."
  @spec payload_placeholder() :: binary()
  def payload_placeholder(), do: @payload_placeholder

  @doc """
  Build the outbound half from a plugin's opts, or `{:ok, nil}` when the plugin
  is bound in neither direction.

  A plugin never subscribed to `on_input` or `on_output` costs nothing here: no
  queue is allocated and no acknowledgement window opened, whatever its `sink_*`
  rows say.
  """
  @spec from_opts(map()) :: {:ok, t() | nil} | {:error, term()}
  def from_opts(opts) do
    if ProviderBehaviour.io_bound?(opts) do
      with {:ok, template} <- template(Map.get(opts, :sink_template)),
           :ok <- correlatable(opts.sink_ack_mode, template) do
        {:ok,
         %__MODULE__{
           frame_type: frame_type(opts.sink_frame_type),
           template: template,
           ack_mode: opts.sink_ack_mode,
           queue_max: opts.sink_queue_max,
           queue: :queue.new()
         }}
      end
    else
      {:ok, nil}
    end
  end

  defp frame_type("binary"), do: :binary
  defp frame_type(_text), do: :text

  # Refused at enable time, exactly as a `subscribe_template` without its own
  # placeholder is: a template that never carries the payload sends one constant
  # frame for every output, and discovering that on a live socket is the failure
  # this closes.
  defp template(nil), do: {:ok, nil}
  defp template(""), do: {:ok, nil}

  defp template(value) when is_binary(value) do
    if String.contains?(value, @payload_placeholder) do
      {:ok, value}
    else
      {:error, {:template_missing_placeholder, :sink_template, @payload_placeholder}}
    end
  end

  defp template(value), do: ProviderBehaviour.refuse_value(:sink_template, value)

  # An acknowledgement the frame cannot ask for is not a guarantee. Under
  # `"required"` the correlation id has to reach the upstream, and a verbatim
  # payload has nowhere to put it — so the mode requires the template that
  # carries it, and the same template is where the identity stamps ride.
  defp correlatable("required", template) do
    if is_binary(template) and String.contains?(template, Subscription.req_id_placeholder()) do
      :ok
    else
      {:error, {:ack_needs_correlation, Subscription.req_id_placeholder()}}
    end
  end

  defp correlatable(_none, _template), do: :ok

  @doc """
  The acknowledgement window this sink waits before re-sending, or `nil` when it
  awaits none. The Client folds it into its watchdog tick, so the sink arms no
  timer of its own. It is the first rung of the `:ws_sink_ack` ladder, so the two
  cannot drift apart.
  """
  @spec ack_window_ms(map()) :: pos_integer() | nil
  def ack_window_ms(%{sink: %__MODULE__{ack_mode: "required"}}),
    do: Backoff.ladder(:ws_sink_ack).base

  def ack_window_ms(_state), do: nil

  ## ---------- the frame ----------

  @doc """
  Take one dispatched output: render it as the frame that carries it, hold it in
  the bounded queue, and answer the caller through the shared overflow policy —
  at once while there is room, on the drain that frees space once there is not.

  The payload goes out verbatim when no template is configured. A template
  substitutes the payload, the correlation id and the `flz.cell` / `flz.seq`
  stamps the output already carries (`Flezha.State.Outbox.output_stamps/1`), so
  the identity a consumer dedups on is the one the relay committed.
  """
  @spec enqueue(map(), binary(), map(), GenServer.from()) :: map()
  def enqueue(%{sink: sink} = state, message, metadata, from) do
    {sink, entry} = render(sink, message, metadata)
    accept(%{state | sink: sink}, entry, from)
  end

  @spec render(t(), binary(), map()) :: {t(), entry()}
  defp render(%__MODULE__{} = sink, message, metadata) do
    {sink, req_id} = allocate(sink)

    body =
      case sink.template do
        nil -> message
        tmpl -> substitute(tmpl, message, req_id, metadata)
      end

    {sink, {{sink.frame_type, body}, metadata, req_id}}
  end

  defp allocate(%__MODULE__{ack_mode: "required"} = sink) do
    {%{sink | next_req_id: sink.next_req_id + 1}, "out-#{sink.next_req_id}"}
  end

  defp allocate(sink), do: {sink, nil}

  # `{{req_id}}` is the subscription half's placeholder, read from there so the
  # correlation contract has one spelling across both directions.
  defp substitute(template, message, req_id, metadata) do
    template
    |> String.replace(@payload_placeholder, message)
    |> String.replace(Subscription.req_id_placeholder(), req_id || "")
    |> String.replace(@cell_placeholder, Map.get(metadata, "flz.cell", ""))
    |> String.replace(@seq_placeholder, Map.get(metadata, "flz.seq", ""))
  end

  ## ---------- the queue ----------

  # Nothing here writes a second overflow rule: `park_or_reply/2` is the one
  # every bounded sink shares, and `{caller_pid, ref}` is a valid `from` for it
  # because `GenServer.reply/2` sends exactly the `{ref, payload}` the caller's
  # correlated ask awaits.
  @spec accept(map(), entry(), GenServer.from()) :: map()
  defp accept(%{sink: sink} = state, entry, from) do
    sink = %{sink | queue: :queue.in(entry, sink.queue), buffer_len: sink.buffer_len + 1}

    case ProviderBehaviour.park_or_reply(sink, from) do
      {:reply, :ok, sink} ->
        GenServer.reply(from, :ok)
        %{state | sink: sink}

      {:noreply, sink} ->
        emit(:overflowed, %{queued: sink.buffer_len}, state)
        %{state | sink: sink}
    end
  end

  @doc """
  Every frame the queue holds, in dispatch order, and the sink that has released
  whatever the drain freed.

  Under `"none"` the hold is answered here: handing the frame to the established
  socket IS the confirmation that mode declares. Under `"required"` the frame
  moves to the in-flight set instead and the hold stays open.
  """
  @spec drain(map(), integer()) :: {map(), [{:text | :binary, binary()}]}
  def drain(%{sink: %__MODULE__{buffer_len: 0}} = state, _now), do: {state, []}
  def drain(%{sink: %__MODULE__{}} = state, now), do: do_drain(state, now)
  def drain(state, _now), do: {state, []}

  defp do_drain(%{sink: sink} = state, now) do
    entries = :queue.to_list(sink.queue)

    sink =
      %{sink | queue: :queue.new(), buffer_len: 0}
      |> settle(entries, now)
      |> ProviderBehaviour.release_waiters()

    emit(:shipped, %{count: length(entries)}, state)
    {%{state | sink: sink}, Enum.map(entries, fn {frame, _md, _id} -> frame end)}
  end

  defp settle(%__MODULE__{ack_mode: "required"} = sink, entries, now) do
    Enum.reduce(entries, sink, fn {_frame, _md, req_id} = entry, acc ->
      %{acc | inflight: Map.put(acc.inflight, req_id, {entry, 1, now})}
    end)
  end

  defp settle(sink, entries, _now) do
    Outbox.confirm_many(Enum.map(entries, fn {_frame, md, _id} -> md end))
    sink
  end

  ## ---------- acknowledgement-gated settlement ----------

  @doc """
  Settle whatever this inbound frame acknowledges.

  It reads the correlation the subscription half already defines, so an upstream
  answers both directions the same way and there is no second contract to
  implement. A sink id is a string and a delta id an integer, so neither lookup
  can commit the other's work.
  """
  @spec commit_acknowledged(map(), binary()) :: map()
  def commit_acknowledged(%{sink: %__MODULE__{inflight: inflight}} = state, message)
      when inflight != %{} do
    case Subscription.acknowledgement(message) do
      {:ok, req_id, accepted?} when is_binary(req_id) -> settle_ack(state, req_id, accepted?)
      _other -> state
    end
  end

  def commit_acknowledged(state, _message), do: state

  defp settle_ack(state, req_id, accepted?) do
    case Map.fetch(state.sink.inflight, req_id) do
      {:ok, {{_frame, metadata, _id}, attempts, _sent_at}} when accepted? ->
        Outbox.confirm(metadata)
        # Only a re-sent frame walked the ladder, so only its ack is a reset.
        if attempts > 1, do: Backoff.reset(:ws_sink_ack, {state.plugin_id, req_id})
        %{state | sink: forget(state.sink, req_id)}

      # A refusal is not a delivery: the frame keeps its place in the in-flight
      # set and the next sweep re-sends it under the same id, which is what
      # makes the retry budget count refusals as well as silence.
      {:ok, _entry} ->
        emit(:failed, %{rejected: 1}, state)
        state

      :error ->
        state
    end
  end

  defp forget(sink, req_id), do: %{sink | inflight: Map.delete(sink.inflight, req_id)}

  @doc """
  The frames whose acknowledgement window has elapsed, re-sent, and the ones
  whose retry budget is spent, parked.

  Driven by the Client's existing watchdog tick, so the sink arms no timer. A
  parked frame is written to the dead-letter store carrying its cell and
  channel and its hold is answered, so the cursor advances past exactly the
  parked range rather than stalling on it.
  """
  @spec sweep(map(), integer()) :: {map(), [{:text | :binary, binary()}]}
  def sweep(%{sink: %__MODULE__{inflight: inflight}} = state, now) when inflight != %{},
    do: do_sweep(state, now)

  def sweep(state, _now), do: {state, []}

  defp do_sweep(%{sink: sink} = state, now) do
    {sink, frames} =
      Enum.reduce(sink.inflight, {sink, []}, fn {req_id, held}, acc ->
        sweep_one(acc, state, req_id, held, now)
      end)

    {%{state | sink: sink}, Enum.reverse(frames)}
  end

  defp sweep_one({sink, frames}, state, req_id, {entry, attempts, sent_at}, now) do
    cond do
      now - sent_at < Backoff.delay(:ws_sink_ack, attempts) ->
        {sink, frames}

      Backoff.give_up?(:ws_sink_ack, attempts) ->
        park(state, entry, attempts)
        {forget(sink, req_id), frames}

      true ->
        emit(:retried, %{attempt: attempts + 1}, state)
        # The rung this re-send waited out, announced as the ladder attempt it
        # is. A first send announces nothing: that path runs for every frame.
        _ = Backoff.arm(:ws_sink_ack, attempts, {state.plugin_id, req_id})
        {frame, _md, _id} = entry

        {%{sink | inflight: Map.put(sink.inflight, req_id, {entry, attempts + 1, now})},
         [frame | frames]}
    end
  end

  defp park(state, {{_type, body}, metadata, _req_id}, attempts) do
    emit(:failed, %{parked: 1}, state)
    Outbox.park(metadata, body, :ws_sink_unacknowledged, attempts)
  end

  # The prefix is derived from the provider's id; nothing here names a root.
  @provider "websocket"

  defp emit(verb, measurements, state),
    do: ProviderBehaviour.emit(@provider, verb, measurements, %{plugin_id: state.plugin_id})
end
