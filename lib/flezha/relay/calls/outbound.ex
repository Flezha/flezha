defmodule Flezha.Relay.Calls.Outbound do
  @moduledoc """
  The calls one relay makes: a call end's output becomes a request the
  delivery carries to its callee, and the caller's own row, keyed by the call
  id and held in the table's `out`, waits for its outcome, which `settle/2`
  admits once and `respond/4` turns into the call's `Response` input.
  """

  alias Flezha.Error
  alias Flezha.Process.CallDelivery
  alias Flezha.Process.Timer
  alias Flezha.Relay.Calls.{Inbound, Token}
  alias Flezha.Relay.Ingest
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Replay

  @call Token.call_key()

  @typedoc "One of this relay's own pending calls, by its call id."
  @type call_id :: binary()
  @type out_row :: %{
          target: binary(),
          key: binary() | nil,
          create: boolean(),
          channel: binary() | nil,
          answer_channel: binary() | nil,
          request: binary(),
          response: binary(),
          started_ms: integer(),
          deadline_at: integer(),
          deadline_ms: non_neg_integer(),
          callee: binary() | nil,
          created: boolean(),
          hold: map() | nil,
          settling: boolean()
        }

  @typedoc """
  What a callee hands back for a relay's call: its answer or the error
  that ended it, the instance that answered, and whether the call started it.
  """
  @type outcome :: %{
          result: {:ok, binary()} | {:error, Error.t()},
          target: binary(),
          created: boolean()
        }

  @doc """
  The call a call end's output makes, read off the stamps the runner put on
  it: its id and its row. A durable caller's id is a digest of the output's
  outbox identity, so every re-drive of the output names the same call; `hold`
  is then the output's metadata, whose outbox hold the call keeps until its
  `Response` is durable. Any other caller's id is random.
  """
  @spec request(binary(), map(), boolean()) :: {call_id(), out_row()}
  def request(message, metadata, durable?) do
    id =
      if durable?,
        do: Token.durable_id(metadata),
        else: Token.call_id(:crypto.strong_rand_bytes(16))

    deadline_at = String.to_integer(Map.fetch!(metadata, "flz.deadline_at"))

    row = %{
      target: Map.fetch!(metadata, @call),
      key: metadata["flz.key"],
      create: metadata["flz.call_create"] != "false",
      channel: metadata["flz.call_channel"],
      answer_channel: Inbound.answer_channel(metadata),
      request: message,
      response: Map.fetch!(metadata, "flz.call_response"),
      started_ms: System.monotonic_time(:millisecond),
      deadline_at: deadline_at,
      # What the caller waits from here, which `call.timeout` names.
      deadline_ms: Inbound.left_ms(deadline_at),
      callee: nil,
      created: false,
      hold: if(durable?, do: metadata),
      settling: false
    }

    {id, row}
  end

  @doc "This relay's pending call `id`, or `nil`."
  @spec pending(Inbound.t(), call_id()) :: out_row() | nil
  def pending(%Inbound{out: out}, id), do: Map.get(out, id)

  @doc "Wait for call `id`'s outcome."
  @spec open(Inbound.t(), call_id(), out_row()) :: Inbound.t()
  def open(%Inbound{} = calls, id, row), do: %{calls | out: Map.put(calls.out, id, row)}

  @doc "The callee took call `id`: it is `callee`, started by the call when `created`."
  @spec accepted(Inbound.t(), call_id(), binary(), boolean()) :: Inbound.t()
  def accepted(%Inbound{} = calls, id, callee, created) do
    case Map.fetch(calls.out, id) do
      {:ok, row} -> open(calls, id, %{row | callee: callee, created: created})
      :error -> calls
    end
  end

  @doc """
  Admit the first outcome of call `id`: its row, now settling, so a racing
  outcome finds none; `:none` when the call is not pending or already settling.
  """
  @spec settle(Inbound.t(), call_id()) :: {:ok, out_row(), Inbound.t()} | :none
  def settle(%Inbound{} = calls, id) do
    case Map.fetch(calls.out, id) do
      {:ok, %{settling: false} = row} ->
        {:ok, row, open(calls, id, %{row | settling: true})}

      _settling_or_gone ->
        :none
    end
  end

  @doc "Call `id`'s outcome could not be kept: it waits again."
  @spec unsettle(Inbound.t(), call_id()) :: Inbound.t()
  def unsettle(%Inbound{} = calls, id) do
    case Map.fetch(calls.out, id) do
      {:ok, row} -> open(calls, id, %{row | settling: false})
      :error -> calls
    end
  end

  @doc "Call `id` is over: its row, removed."
  @spec close(Inbound.t(), call_id()) :: {out_row() | nil, Inbound.t()}
  def close(%Inbound{} = calls, id) do
    {row, out} = Map.pop(calls.out, id)
    {row, %{calls | out: out}}
  end

  @doc """
  The `Response` a call's outcome becomes: one JSON document naming the call,
  the instance that answered (or the call's own target when none did), whether
  the call started it, the request, and the answer or the error, both bytes in
  base64.
  """
  @spec envelope(call_id(), out_row(), outcome()) :: binary()
  def envelope(id, row, outcome) do
    fields = %{
      "call" => id,
      "target" => outcome.target,
      "created" => outcome.created,
      "request" => Base.encode64(row.request)
    }

    fields =
      case outcome.result do
        {:ok, value} -> Map.put(fields, "value", Base.encode64(value))
        {:error, %Error{} = error} -> Map.put(fields, "error", Jason.decode!(Error.object(error)))
      end

    Jason.encode!(fields)
  end

  @doc "The outcome this relay gives call `id` itself: `call.timeout`, or a refusal."
  @spec own_outcome(out_row(), Error.t()) :: outcome()
  def own_outcome(row, %Error{} = error),
    do: %{result: {:error, error}, target: row.callee || target_name(row), created: row.created}

  defp target_name(%{target: "id:" <> id}), do: id
  defp target_name(%{target: "factory:" <> factory, key: key}), do: key || factory

  # The error that ends a call at its deadline, named after the call's target.
  defp timeout_error(row, callee) do
    Inbound.emit(:timeout, %{count: 1}, callee, %{target: target_name(row)})

    Error.new("call.timeout", %{
      relay: row.callee || target_name(row),
      deadline_ms: row.deadline_ms
    })
  end

  @doc """
  A re-driven call found past its deadline and the retention margin: settled
  with no `Response`, since one may have been appended and truncated since.
  """
  @spec recovered_unanswered(Inbound.callee(), out_row()) :: :ok
  def recovered_unanswered(caller, row),
    do: Inbound.emit(:recovered_unanswered, %{count: 1}, caller, %{target: target_name(row)})

  @doc """
  One of this relay's own calls leaves, its output's outbox hold already
  taken by the Overwatch when it is `durable?`. A call still pending (a
  re-dispatch of its output) changes nothing. A durable call the Overwatch has
  no row for is resolved before anything is sent: its `Response` retained in
  this relay's log settles it; with the deadline ahead it is sent (again:
  the callee absorbs a repeat by its message id) and its deadline armed; past
  the deadline, within `call_retention_margin_ms`, no `Response` was ever
  appended, so `call.timeout` is; past the margin one may have been appended
  and truncated since, so none is, and the call settles as
  `recovered_unanswered`.
  """
  @spec request(Overwatch.t(), binary(), map(), boolean()) :: Overwatch.t()
  def request(%Overwatch{} = state, message, metadata, durable?) do
    {id, row} = request(message, metadata, durable?)

    # A call sent again while it waits: its outcome answers the newest copy's hold.
    cond do
      pending = pending(state.calls, id) ->
        %{state | calls: open(state.calls, id, %{pending | hold: row.hold})}

      durable? and answered?(state, id) ->
        settle_hold(state, row)

      true ->
        send_request(state, id, row)
    end
  end

  defp answered?(%Overwatch{} = state, id), do: Ingest.retained?(state, "r:" <> id)

  defp send_request(state, id, row) do
    state = %{state | calls: open(state.calls, id, row)}

    cond do
      Inbound.left_ms(row.deadline_at) > 0 ->
        CallDelivery.request(state.namespace, state.id, request_of(id, row))
        Inbound.arm_call(state, {:call_timeout, id}, row.deadline_at)

      Inbound.retained_until(row.deadline_at) > System.system_time(:millisecond) ->
        timeout(state, id)

      true ->
        {row, calls} = close(state.calls, id)
        recovered_unanswered(Overwatch.callee(state), row)
        settle_hold(%{state | calls: calls}, row)
    end
  end

  defp request_of(id, row), do: row |> Map.drop([:hold, :settling]) |> Map.put(:id, id)

  defp settle_hold(state, row) do
    Overwatch.confirm_hold(row.hold)
    state
  end

  @doc """
  Call `id`'s deadline passed with no outcome: `call.timeout` is its outcome.
  """
  @spec timeout(Overwatch.t(), call_id()) :: Overwatch.t()
  def timeout(%Overwatch{} = state, id) do
    case pending(state.calls, id) do
      %{settling: false} = row ->
        error = timeout_error(row, Overwatch.callee(state))
        respond(state, id, own_outcome(row, error), nil)

      _settling_or_gone ->
        state
    end
  end

  @doc """
  The outcome of call `id` reached this caller. The first becomes a `Response`
  input on the call's Response channel, journaled with `flz.msg_id`
  `r:<id>` when this relay journals input; once it is durable (or at once
  off the tier) the call is over, its output's hold answered and `from`
  answered `:ok`. Any later outcome is dropped and counted, `from` answered
  `{:ok, :absorbed}`, unless this caller journals its `Response`s, has none
  for the call, and its writers still held calls it had no row for: `from` is
  then answered an error, so the delivery retries once they are dispatched
  again.
  """
  @spec respond(Overwatch.t(), call_id(), outcome(), GenServer.from() | nil) ::
          Overwatch.t()
  def respond(%Overwatch{} = state, id, outcome, from) do
    case settle(state.calls, id) do
      {:ok, row, calls} ->
        metadata = %{
          "channel" => row.response,
          "flz.msg_id" => "r:" <> id,
          "flz.deadline_at" => Integer.to_string(row.deadline_at)
        }

        state = %{state | calls: calls}
        envelope = envelope(id, row, outcome)

        case Ingest.confirming_ingest(state, envelope, metadata, {:call_answer, from, id}) do
          {:pending, state} -> state
          {verdict, state} -> responded(state, from, id, verdict)
        end

      :none ->
        Ingest.reply(from, unclaimed(state, id))
        state
    end
  end

  # An outcome for no row the caller can take: the call is over, or, for a
  # caller that journals its `Response`s and restarted since it made the call,
  # not relearned yet. Its writers then dispatch their held calls now, and the
  # delivery retries while one of them still holds this call.
  defp unclaimed(%Overwatch{} = state, id) do
    if Replay.floor_reporter(state) != nil and pending(state.calls, id) == nil and
         not answered?(state, id) and Replay.redrive_calls(state, id) > 0 do
      {:error, :call_not_relearned}
    else
      Inbound.answer_dropped(Overwatch.callee(state), "unclaimed")
      {:ok, :absorbed}
    end
  end

  @doc """
  The journal's verdict on call `id`'s `Response` input reached `from`, the
  delivery that is owed it: a taken `Response` ends the call. An append that
  failed leaves the call waiting again, the delivery retries, and a deadline
  that already passed tries its timeout again shortly.
  """
  @spec responded(Overwatch.t(), GenServer.from() | nil, call_id(), term()) :: Overwatch.t()
  def responded(%Overwatch{} = state, from, id, verdict) do
    if Ingest.taken?(verdict) do
      {row, calls} = close(state.calls, id)
      if row, do: Overwatch.confirm_hold(row.hold)
      Ingest.reply(from, :ok)
      %{state | calls: calls, timers: Timer.cancel(state.timers, {:call, id})}
    else
      Ingest.reply(from, verdict)
      state = %{state | calls: unsettle(state.calls, id)}

      case pending(state.calls, id) do
        %{deadline_at: at} ->
          Inbound.arm_call(
            state,
            {:call_timeout, id},
            max(at, System.system_time(:millisecond) + 1_000)
          )

        nil ->
          state
      end
    end
  end
end
