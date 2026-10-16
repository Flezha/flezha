defmodule Flezha.Relay.Calls do
  @moduledoc """
  The outside callers waiting on one relay for an answer: the table the
  Overwatch holds, and the client call every outside caller makes.

  A call is an ordinary input carrying a token the controller mints
  (`flz.reply_to`, see `entry_metadata/2`). The runner carries the token to
  every output the input causes, and stamps it on every sink output.
  `divert/4` hands the first such output, on the call's answer channel when it
  names one, to the row its token names, and lets every one of them continue
  to the output routes without the token.

  A relay is a caller too. A call end's output is a request: `divert/4`
  answers `:call` for it, and the caller's own row (`open/3`, keyed by the
  call id, held in `out`) waits for its outcome, which `settle/3` admits once.
  A relay's call reaches its callee's table as a row keyed by a relay
  token (`o:<caller>:<call id>:<created>:<deadline>:<answer channel>`), built
  only from the controller's own delivery (`relay_token/1`); its answer is
  handed to the delivery for the caller rather than replied to a waiting
  process.

  Data and decisions only: the Overwatch arms each row's deadline under the
  timer family `:call`, runs the ingest, and takes and answers outbox holds;
  this module never names a module above the relay layer.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Error
  alias Flezha.Process.CallDelivery

  @reply_to "flz.reply_to"
  @answer_channel "flz.call_answer_channel"
  @source "flz.call_source"

  defstruct rows: %{}, out: %{}

  @type token :: binary()
  @type row :: %{
          required(:from) => GenServer.from(),
          required(:channel) => binary() | nil,
          required(:answer_channel) => binary() | nil,
          required(:started_ms) => integer(),
          required(:deadline_ms) => pos_integer(),
          required(:answered?) => boolean(),
          optional(:sent_as) => {binary() | nil, binary()} | nil
        }
  @type t :: %__MODULE__{rows: %{token() => row()}, out: %{call_id() => out_row()}}

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

  @typedoc """
  Who the table answers for, named on every error and telemetry event, and
  the function that hands one call event to its `on_call` listeners, `nil`
  while there are none (`Flezha.Relay.Overwatch.callee/1`).
  """
  @type callee :: %{
          required(:namespace) => binary(),
          required(:relay_id) => binary(),
          optional(:activity) => (binary(), map() -> :ok) | nil
        }

  # ---------------------------------------------------------------
  # The caller's side
  # ---------------------------------------------------------------

  @doc """
  Call the Overwatch `overwatch` with one input and wait for its answer. The
  wait is the deadline plus `call_grace_ms`, so the Overwatch's own
  `call.timeout` always arrives first; an Overwatch that goes away answers
  `call.unavailable` at once. `relay` names the callee in that error.
  `source` says who calls (`cli`, `ui`, `plugin`, ...) and is written as
  `flz.call_source` over whatever `metadata` carries there.

  The wait follows its call through a handover: the departing Overwatch says
  the call is moving (`depart/1`) and the arriving one says where it now
  lives (`absorb/1`), so the caller stops watching the first and watches the
  second. A plain `GenServer.call` would take the departing process's exit for
  a crash.
  """
  @spec call(pid(), binary(), map(), pos_integer(), binary(), binary()) ::
          {:ok, binary(), map()} | {:error, Error.t()}
  def call(overwatch, message, metadata, deadline_ms, relay, source)
      when is_pid(overwatch) and is_integer(deadline_ms) and deadline_ms > 0 and
             is_binary(source) do
    metadata = Map.put(metadata, @source, source)
    tag = Process.alias()
    until = now_ms() + deadline_ms + Durations.get(:call_grace_ms)
    monitor = Process.monitor(overwatch)
    # The request `GenServer.call` sends, so the Overwatch answers it with
    # `GenServer.reply/2` wherever the call has moved to.
    send(
      overwatch,
      {:"$gen_call", {self(), [:alias | tag]}, {:call, message, metadata, deadline_ms}}
    )

    result = await([:alias | tag], {overwatch, monitor}, until, relay)
    Process.unalias(tag)
    result
  end

  # `watched` is the process the call lives in and its monitor, or `nil`
  # while the call moves between two.
  defp await(tag, watched, until, relay) do
    receive do
      {^tag, {:call_moving, from}} ->
        await(
          tag,
          if(match?({^from, _}, watched), do: unwatch(watched), else: watched),
          until,
          relay
        )

      {^tag, {:call_moved, to}} ->
        unwatch(watched)
        await(tag, {to, Process.monitor(to)}, until, relay)

      {^tag, reply} ->
        unwatch(watched)
        reply

      {:DOWN, monitor, :process, _pid, reason}
      when watched != nil and elem(watched, 1) == monitor ->
        unavailable(relay, reason)
    after
      max(until - now_ms(), 0) ->
        unwatch(watched)
        unavailable(relay, :timeout)
    end
  end

  defp unwatch({_pid, monitor}) do
    Process.demonitor(monitor, [:flush])
    nil
  end

  defp unwatch(nil), do: nil

  defp unavailable(relay, reason),
    do: {:error, Error.new("call.unavailable", %{relay: relay, reason: down_reason(reason)})}

  defp down_reason(:timeout), do: "no answer from its controller process"
  defp down_reason(reason) when reason in [:noproc, :normal], do: "its process is gone"
  defp down_reason(:noconnection), do: "its node is unreachable"
  defp down_reason(reason), do: "its process exited (#{inspect(reason)})"

  # ---------------------------------------------------------------
  # Tokens
  # ---------------------------------------------------------------

  @doc "A fresh outside-call token: `w:` and 16 random bytes, unpadded base64url."
  @spec mint() :: token()
  def mint, do: "w:" <> Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

  @doc """
  `metadata` carrying `token` as its one `flz.reply_to`, whatever the caller
  put there, and without the answer channel its row keeps or the source its
  call event named.
  """
  @spec entry_metadata(map(), token()) :: map()
  def entry_metadata(metadata, token),
    do: metadata |> Map.drop([@answer_channel, @source]) |> Map.put(@reply_to, token)

  @doc "`metadata` without a `flz.reply_to`: what every other entry point admits."
  @spec strip(map()) :: map()
  def strip(metadata), do: Map.delete(metadata, @reply_to)

  @doc """
  Put back on `after_hooks` the token `before_hooks` arrived with, so no
  `:on_input` hook adds, removes or rewrites one.
  """
  @spec restore(map(), map()) :: map()
  def restore(after_hooks, before_hooks) do
    case Map.fetch(before_hooks, @reply_to) do
      {:ok, token} -> Map.put(after_hooks, @reply_to, token)
      :error -> Map.delete(after_hooks, @reply_to)
    end
  end

  # ---------------------------------------------------------------
  # The table
  # ---------------------------------------------------------------

  @doc "How many callers still wait for an answer."
  @spec waiting(t()) :: non_neg_integer()
  def waiting(%__MODULE__{rows: rows}),
    do: Enum.count(rows, fn {_token, row} -> not row.answered? end)

  @doc """
  A row for a new call, keeping the entry channel and the answer channel its
  `metadata` names, or `{:error, busy}` when the relay already owes
  `call_pending_max` answers.
  """
  @spec admit(t(), callee(), GenServer.from(), map(), pos_integer()) ::
          {:ok, token(), t()} | {:error, Error.t()}
  def admit(%__MODULE__{} = calls, callee, from, metadata, deadline_ms) do
    max = Config.get(:call_pending_max)

    if waiting(calls) >= max do
      {:error, refused(callee, "call.busy", %{pending_max: max})}
    else
      token = mint()

      row = %{
        from: from,
        channel: Map.get(metadata, "channel"),
        answer_channel: answer_channel(metadata),
        started_ms: now_ms(),
        deadline_ms: deadline_ms,
        answered?: false
      }

      {:ok, token, %{calls | rows: Map.put(calls.rows, token, row)}}
    end
  end

  @doc """
  A row for a relay's call, keyed by its relay token, or
  `{:error, busy}` when the relay already owes `call_pending_max` answers.
  A call sent again while its first row waits keeps that row.
  """
  @spec admit_relay(t(), callee(), map()) :: {:ok, token(), t()} | {:error, Error.t()}
  def admit_relay(%__MODULE__{} = calls, callee, call) do
    token = relay_token(call)
    max = Config.get(:call_pending_max)

    cond do
      Map.has_key?(calls.rows, token) ->
        {:ok, token, calls}

      waiting(calls) >= max ->
        {:error, refused(callee, "call.busy", %{pending_max: max})}

      true ->
        {:ok, token, %{calls | rows: Map.put(calls.rows, token, relay_row(call))}}
    end
  end

  # A relay caller's row: nobody waits on it here, since the caller times out
  # on its own clock.
  defp relay_row(call) do
    %{
      from: nil,
      channel: nil,
      answer_channel: Map.get(call, :answer_channel),
      started_ms: now_ms(),
      deadline_ms: max(call.deadline_at - System.system_time(:millisecond), 0),
      answered?: false
    }
  end

  defp answer_channel(metadata) do
    case Map.get(metadata, @answer_channel) do
      "" -> nil
      channel -> channel
    end
  end

  @doc """
  The token a relay's call rides: built from the delivery's own fields,
  never read from metadata. It carries the answer channel, so a callee that
  restarted since it took the call still answers only from that channel.
  """
  @spec relay_token(map()) :: token()
  def relay_token(%{caller: caller, call: call_id, created: created, deadline_at: at} = call),
    do:
      "o:#{caller}:#{call_id}:#{if created, do: 1, else: 0}:#{at}:" <>
        Base.url_encode64(Map.get(call, :answer_channel) || "", padding: false)

  @doc """
  The caller, the call, whether it started its callee, its deadline and its
  answer channel, read off a relay token.
  """
  @spec caller_of(token()) ::
          {:ok,
           %{
             caller: binary(),
             call: call_id(),
             created: boolean(),
             deadline_at: integer(),
             answer_channel: binary() | nil
           }}
          | :error
  def caller_of("o:" <> rest) do
    with [answer, at, created, call_id, caller] when created in ["0", "1"] and caller != "" <-
           rest |> String.reverse() |> String.split(":", parts: 5),
         {deadline_at, ""} <- Integer.parse(String.reverse(at)),
         {:ok, answer_channel} <- Base.url_decode64(String.reverse(answer), padding: false) do
      {:ok,
       %{
         caller: String.reverse(caller),
         call: String.reverse(call_id),
         created: created == "1",
         deadline_at: deadline_at,
         answer_channel: if(answer_channel != "", do: answer_channel)
       }}
    else
      _ -> :error
    end
  end

  def caller_of(_token), do: :error

  @doc """
  Drop the row `token` names, answering nobody: its call was never taken, for
  the refusal `code` its listeners are told.
  """
  @spec forget(t(), callee(), token(), binary(), map()) :: t()
  def forget(%__MODULE__{} = calls, callee, token, code, details) do
    row = Map.get(calls.rows, token)
    outcome(callee, :refused, token, row, row && row.channel, {code, details})
    %{calls | rows: Map.delete(calls.rows, token)}
  end

  @doc """
  One output, before any plugin sees it: `:output` when it carries neither a
  request nor a call's token; `:call` when it is a request of this relay's own
  call. An output carrying a token answers the call it names when that call
  still waits and its answer channel is unset or the output's own, and is then
  an output like any other: `{:answered, calls, metadata}`, or
  `{:held, calls, metadata}` when it was handed to the delivery for a relay
  caller, whose outbox hold the Overwatch takes. `metadata` is the output's
  own without the token.
  """
  @spec divert(t(), callee(), binary(), map()) ::
          :output | :call | {:answered, t(), map()} | {:held, t(), map()}
  def divert(%__MODULE__{} = calls, callee, message, metadata) do
    case {Map.has_key?(metadata, "flz.call"), Map.fetch(metadata, @reply_to)} do
      {true, _} -> :call
      {false, :error} -> :output
      {false, {:ok, token}} -> answer(calls, callee, token, message, metadata)
    end
  end

  # The first answer reaches its caller and marks its row answered with the
  # outbox identity it left under; a relay's answer is handed to the
  # delivery, whose outbox hold the Overwatch takes. The outbox dispatching
  # that answer again, its delivery lost, hands it on again.
  defp answer(calls, callee, token, message, metadata) do
    output = Map.delete(metadata, @reply_to)
    caller = waiting(calls, token, sent_as(metadata))

    if answers?(caller, Map.get(calls.rows, token), Map.get(metadata, "channel")) do
      reply(callee, caller, {:ok, message}, metadata)

      calls =
        case {Map.get(calls.rows, token), caller} do
          {%{answered?: false} = row, _caller} ->
            emit(:answered, %{duration_us: duration_us(row)}, callee, %{})
            outcome(callee, :answered, token, row, metadata["channel"], message)
            row = Map.merge(row, %{answered?: true, sent_as: sent_as(metadata)})
            %{calls | rows: Map.put(calls.rows, token, row)}

          # A callee restarted since it took the call keeps no row: the one it
          # leaves now makes every later output of the call an ordinary one.
          {nil, {:relay, call}} ->
            outcome(callee, :answered, token, nil, metadata["channel"], message)
            row = Map.merge(relay_row(call), %{answered?: true, sent_as: sent_as(metadata)})
            %{calls | rows: Map.put(calls.rows, token, row)}

          {_answered, _caller} ->
            calls
        end

      if match?({:relay, _call}, caller),
        do: {:held, calls, output},
        else: {:answered, calls, output}
    else
      {:answered, calls, output}
    end
  end

  # The answer channel a relay's call names rides its token, which a row-less
  # callee still reads; an outside call's rides its row.
  defp answers?({:relay, call}, _row, channel), do: call.answer_channel in [nil, channel]
  defp answers?({:outside, _from}, row, channel), do: row.answer_channel in [nil, channel]
  defp answers?(_answered_or_unclaimed, _row, _channel), do: false

  # The outbox identity a durable answer leaves under, or `nil`.
  defp sent_as(%{"flz.seq" => seq} = metadata) when is_binary(seq),
    do: {metadata["flz.cell"], seq}

  defp sent_as(_metadata), do: nil

  # Who waits on `token`: an outside caller's process, or, for a relay's call, the caller its token names, with a row or
  # without one (a callee restarted since it took the call has none, and an
  # answer sent again as `sent_as` finds its own) until the call's deadline,
  # when the caller has answered itself `call.timeout`; else why nobody does.
  defp waiting(calls, token, sent_as \\ nil) do
    now = System.system_time(:millisecond)

    case {Map.get(calls.rows, token), caller_of(token)} do
      {%{sent_as: ^sent_as}, {:ok, call}} when sent_as != nil -> {:relay, call}
      {%{answered?: true}, _call} -> :answered
      {nil, {:ok, %{deadline_at: at}}} when at <= now -> :unclaimed
      {_row, {:ok, call}} -> {:relay, call}
      {%{from: from}, :error} when from != nil -> {:outside, from}
      {_row, :error} -> :unclaimed
    end
  end

  defp reply(_callee, {:outside, from}, {:ok, message}, metadata),
    do: GenServer.reply(from, {:ok, message, %{"channel" => Map.get(metadata, "channel")}})

  defp reply(_callee, {:outside, from}, {:error, _error} = result, _metadata),
    do: GenServer.reply(from, result)

  defp reply(callee, {:relay, call}, result, metadata) do
    outcome = %{result: result, target: callee.relay_id, created: call.created}
    CallDelivery.answer(callee.namespace, callee.relay_id, call, outcome, metadata)
  end

  @doc """
  A row's deadline passed: a caller still waiting is answered `call.timeout`;
  an answered row is retired. The input stays accepted either way. A relay
  caller times out on its own clock, so its unanswered row is retired
  answering nobody; its listeners are told it timed out all the same.
  """
  @spec expire(t(), callee(), token()) :: t()
  def expire(%__MODULE__{} = calls, callee, token) do
    case Map.pop(calls.rows, token) do
      {%{answered?: false, from: nil} = row, rows} ->
        outcome(
          callee,
          :timed_out,
          token,
          row,
          row.channel,
          {"call.timeout", %{deadline_ms: row.deadline_ms}}
        )

        %{calls | rows: rows}

      {%{answered?: false} = row, rows} ->
        emit(:timeout, %{count: 1}, callee, %{})
        error = error(callee, "call.timeout", %{deadline_ms: row.deadline_ms})
        GenServer.reply(row.from, {:error, error})
        outcome(callee, :timed_out, token, row, row.channel, error)
        %{calls | rows: rows}

      {_answered_or_gone, rows} ->
        %{calls | rows: rows}
    end
  end

  @doc """
  Answer the call `token` names with `code` at once, if it still waits, and
  retire its row. A relay's call waits on its token alone, row or not; an
  answered row, or an outside call with no row, is left as it is.
  """
  @spec refuse(t(), callee(), token() | nil, binary(), map()) :: t()
  def refuse(%__MODULE__{} = calls, callee, token, code, details \\ %{}) do
    case waiting(calls, token) do
      reason when reason in [:answered, :unclaimed] ->
        calls

      caller ->
        reply(callee, caller, {:error, refused(callee, code, details)}, %{})
        forget(calls, callee, token, code, details)
    end
  end

  @doc """
  Answer every waiting caller `call.callee_ended` with `status`, and empty the
  table. `tail` names the relay tokens of the calls the callee took and
  never ran, which are answered the same way.
  """
  @spec end_all(t(), callee(), binary(), [token()]) :: t()
  def end_all(%__MODULE__{} = calls, callee, status, tail \\ []) do
    # Best effort for a relay caller: one whose answer is lost times out.
    tokens = for({token, %{answered?: false}} <- calls.rows, do: token) ++ tail

    for token <- Enum.uniq(tokens),
        do: refuse(calls, callee, token, "call.callee_ended", %{status: status})

    %__MODULE__{}
  end

  @doc "The table without its relay callers' rows: its outside callers alone."
  @spec outside(t()) :: t()
  def outside(%__MODULE__{} = calls),
    do: %{
      calls
      | rows: Map.filter(calls.rows, fn {token, _row} -> caller_of(token) == :error end)
    }

  @doc """
  The rows a handover carries, each with the time it has left: a `from` is
  valid on any node, so the answer can still reach its caller from there.
  """
  @spec export(t()) :: [{token() | {:out, call_id()}, map()}]
  def export(%__MODULE__{rows: rows, out: out}) do
    now = now_ms()

    for(
      {token, row} <- rows,
      do: {token, Map.put(row, :remaining_ms, max(row.started_ms + row.deadline_ms - now, 0))}
    ) ++ for({id, row} <- out, do: {{:out, id}, %{row | settling: false}})
  end

  @doc """
  The rows left with a handover: tell every caller still waiting that its call
  is moving, so it stops watching this process, and empty the table.
  """
  @spec depart(t()) :: t()
  def depart(%__MODULE__{} = calls) do
    reply_waiting(calls.rows, fn -> {:call_moving, self()} end)
    %__MODULE__{}
  end

  @doc """
  The table a handover arrived with; each row's deadline is armed by the
  caller. Every caller still waiting is told its call now lives in this
  process.
  """
  @spec absorb([{token(), map()}]) :: t()
  def absorb(exported) do
    now = now_ms()
    out = for {{:out, id}, row} <- exported, into: %{}, do: {id, row}

    rows =
      for {token, row} when is_binary(token) <- exported, into: %{} do
        # Re-based on this node's clock, so the time left is what it was.
        started = now - (row.deadline_ms - row.remaining_ms)
        {token, row |> Map.delete(:remaining_ms) |> Map.put(:started_ms, started)}
      end

    reply_waiting(rows, fn -> {:call_moved, self()} end)
    %__MODULE__{rows: rows, out: out}
  end

  @doc "The time left before the row `token` names reaches its deadline, in ms."
  @spec remaining_ms(t(), token()) :: non_neg_integer()
  def remaining_ms(%__MODULE__{rows: rows}, token) do
    row = Map.fetch!(rows, token)
    max(row.started_ms + row.deadline_ms - now_ms(), 0)
  end

  # ---------------------------------------------------------------
  # This relay's own calls
  # ---------------------------------------------------------------

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
        do: digest([metadata["flz.cell"], metadata["flz.seq"], metadata["flz.epoch"]]),
        else: digest([:crypto.strong_rand_bytes(16)])

    deadline_at = String.to_integer(Map.fetch!(metadata, "flz.deadline_at"))

    row = %{
      target: Map.fetch!(metadata, "flz.call"),
      key: metadata["flz.key"],
      create: metadata["flz.call_create"] != "false",
      channel: metadata["flz.call_channel"],
      answer_channel: answer_channel(metadata),
      request: message,
      response: Map.fetch!(metadata, "flz.call_response"),
      deadline_at: deadline_at,
      # What the caller waits from here, which `call.timeout` names.
      deadline_ms: max(deadline_at - System.system_time(:millisecond), 0),
      callee: nil,
      created: false,
      hold: if(durable?, do: metadata),
      settling: false
    }

    {id, row}
  end

  defp digest(parts) do
    :sha256
    |> :crypto.hash(Enum.intersperse(Enum.map(parts, &to_string/1), <<0>>))
    |> binary_part(0, 16)
    |> Base.url_encode64(padding: false)
  end

  @doc "This relay's pending call `id`, or `nil`."
  @spec pending(t(), call_id()) :: out_row() | nil
  def pending(%__MODULE__{out: out}, id), do: Map.get(out, id)

  @doc "Wait for call `id`'s outcome."
  @spec open(t(), call_id(), out_row()) :: t()
  def open(%__MODULE__{} = calls, id, row), do: %{calls | out: Map.put(calls.out, id, row)}

  @doc "The callee took call `id`: it is `callee`, started by the call when `created`."
  @spec accepted(t(), call_id(), binary(), boolean()) :: t()
  def accepted(%__MODULE__{} = calls, id, callee, created) do
    case Map.fetch(calls.out, id) do
      {:ok, row} -> open(calls, id, %{row | callee: callee, created: created})
      :error -> calls
    end
  end

  @doc """
  Admit the first outcome of call `id`: its row, now settling, so a racing
  outcome finds none; `:none` when the call is not pending or already settling.
  """
  @spec settle(t(), call_id()) :: {:ok, out_row(), t()} | :none
  def settle(%__MODULE__{} = calls, id) do
    case Map.fetch(calls.out, id) do
      {:ok, %{settling: false} = row} ->
        {:ok, row, open(calls, id, %{row | settling: true})}

      _settling_or_gone ->
        :none
    end
  end

  @doc "Call `id`'s outcome could not be kept: it waits again."
  @spec unsettle(t(), call_id()) :: t()
  def unsettle(%__MODULE__{} = calls, id) do
    case Map.fetch(calls.out, id) do
      {:ok, row} -> open(calls, id, %{row | settling: false})
      :error -> calls
    end
  end

  @doc "Call `id` is over: its row, removed."
  @spec close(t(), call_id()) :: {out_row() | nil, t()}
  def close(%__MODULE__{} = calls, id) do
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

  @doc """
  The error that ends call `id` at its deadline, named after the call's target.
  """
  @spec timeout(out_row(), callee()) :: Error.t()
  def timeout(row, callee) do
    emit(:timeout, %{count: 1}, callee, %{target: target_name(row)})

    Error.new("call.timeout", %{
      relay: row.callee || target_name(row),
      deadline_ms: row.deadline_ms
    })
  end

  @doc "An outcome for a call this relay no longer waits on was dropped."
  @spec unclaimed(callee()) :: :ok
  def unclaimed(caller), do: emit(:answer_dropped, %{count: 1}, caller, %{reason: "unclaimed"})

  @doc """
  A re-driven call found past its deadline and the retention margin: settled
  with no `Response`, since one may have been appended and truncated since.
  """
  @spec recovered_unanswered(callee(), out_row()) :: :ok
  def recovered_unanswered(caller, row),
    do: emit(:recovered_unanswered, %{count: 1}, caller, %{target: target_name(row)})

  @doc """
  The error a call the relay cannot take is refused with, counted as
  `[:flezha, :call, :refused]`.
  """
  @spec refused(callee(), binary(), map()) :: Error.t()
  def refused(callee, code, details) do
    emit(:refused, %{count: 1}, callee, %{code: code})
    error(callee, code, details)
  end

  @doc "The error `code` names for a call to `callee`, counting nothing."
  @spec error(callee(), String.t(), map()) :: Error.t()
  def error(callee, code, details),
    do: Error.new(code, Map.put(details, :relay, callee.relay_id))

  # ---------------------------------------------------------------
  # Call events, for the `on_call` listeners
  # ---------------------------------------------------------------

  @doc """
  The id a call's events carry: a digest of its token, from which the token
  cannot be recovered.
  """
  @spec call_id(token()) :: binary()
  def call_id(token), do: digest([token])

  @doc """
  Announce a call the relay takes, `message` as its entry point took it:
  `deadline` is an outside call's deadline in ms, or a relay's call. With a
  `refusal` the call has no row: it is announced under an id of its own,
  together with its `refused` outcome. Nothing is built while nobody listens.
  """
  @spec announce(callee(), token() | nil, binary(), map(), pos_integer() | map(), Error.t() | nil) ::
          :ok
  def announce(callee, token, message, metadata, deadline, refusal \\ nil)

  def announce(%{activity: activity} = callee, token, message, metadata, deadline, refusal)
      when activity != nil do
    token = token || mint()

    called =
      %{
        "flz.call_id" => call_id(token),
        "flz.call_at" => System.system_time(:millisecond),
        "channel" => metadata["channel"]
      }
      |> Map.merge(called_by(metadata, deadline))
      |> named()

    activity.(message, Map.put(called, "flz.call_event", "call"))

    if refusal,
      do: outcome(callee, :refused, token, nil, metadata["channel"], refusal)

    :ok
  end

  def announce(_callee, _token, _message, _metadata, _deadline, _refusal), do: :ok

  defp called_by(metadata, deadline_ms) when is_integer(deadline_ms),
    do: %{
      @source => metadata[@source],
      @answer_channel => answer_channel(metadata),
      "flz.call_deadline_ms" => deadline_ms
    }

  defp called_by(_metadata, call),
    do: %{
      @source => "relay",
      "flz.call_caller" => call.caller,
      @answer_channel => Map.get(call, :answer_channel),
      "flz.call_deadline_ms" => max(call.deadline_at - System.system_time(:millisecond), 0)
    }

  # How the call `token` names ended, to its listeners: `answer` for an
  # answer, an error, or an error's code and details, built only for them.
  defp outcome(%{activity: activity} = callee, kind, token, row, channel, answer)
       when activity != nil do
    ended =
      named(%{
        "flz.call_event" => Atom.to_string(kind),
        "flz.call_id" => call_id(token),
        "flz.call_duration_ms" => if(row, do: now_ms() - row.started_ms, else: 0),
        "channel" => channel
      })

    activity.(event_payload(callee, answer), ended)
  end

  defp outcome(_callee, _kind, _token, _row, _channel, _answer), do: :ok

  # A channel or a source the call does not name is left out.
  defp named(metadata), do: Map.reject(metadata, fn {_key, value} -> value in [nil, ""] end)

  defp event_payload(_callee, answer) when is_binary(answer), do: answer
  defp event_payload(_callee, %Error{} = error), do: Error.render(error, :json)

  defp event_payload(callee, {code, details}),
    do: event_payload(callee, error(callee, code, details))

  # Every caller still waiting is answered `reply.()`, built (and an error
  # counted) once per caller.
  defp reply_waiting(rows, reply) do
    for {_token, %{answered?: false, from: from}} when from != nil <- rows,
        do: GenServer.reply(from, reply.())

    :ok
  end

  defp duration_us(row), do: (now_ms() - row.started_ms) * 1_000

  defp emit(event, measurements, callee, metadata) do
    :telemetry.execute(
      [:flezha, :call, event],
      measurements,
      Map.merge(%{namespace: callee.namespace, relay_id: callee.relay_id}, metadata)
    )
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
