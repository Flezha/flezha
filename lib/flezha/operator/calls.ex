defmodule Flezha.Operator.Calls do
  @moduledoc """
  The outside callers waiting on one operator for an answer: the table the
  Overwatch holds, and the client call every outside caller makes.

  A call is an ordinary input carrying a token the controller mints
  (`flz.reply_to`, see `entry_metadata/2`). The runner carries the token to
  every output the input causes, and stamps it on every output of a
  `response()` end; such an output is an answer. `divert/4` takes an answer off
  the output path before any plugin sees it and hands it to the row its token
  names. The first answer wins; the row stays until its deadline so a later
  answer is counted as `answered` rather than `unclaimed`.

  Data and decisions only: the Overwatch arms each row's deadline under the
  timer family `:call` and runs the ingest; this module never names a module
  above the operator layer.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Error

  @reply_to "flz.reply_to"

  defstruct rows: %{}

  @type token :: binary()
  @type row :: %{
          from: GenServer.from(),
          channel: binary() | nil,
          started_ms: integer(),
          deadline_ms: pos_integer(),
          answered?: boolean()
        }
  @type t :: %__MODULE__{rows: %{token() => row()}}

  @typedoc "Who the table answers for: named on every error and telemetry event."
  @type callee :: %{namespace: binary(), operator_id: binary()}

  # ---------------------------------------------------------------
  # The caller's side
  # ---------------------------------------------------------------

  @doc """
  Call the Overwatch `overwatch` with one input and wait for its answer. The
  wait is the deadline plus `call_grace_ms`, so the Overwatch's own
  `call.timeout` always arrives first; an Overwatch that goes away answers
  `call.unavailable` at once. `operator` names the callee in that error.

  The wait follows its call through a handover: the departing Overwatch says
  the call is moving (`depart/1`) and the arriving one says where it now
  lives (`absorb/1`), so the caller stops watching the first and watches the
  second. A plain `GenServer.call` would take the departing process's exit for
  a crash.
  """
  @spec call(pid(), binary(), map(), pos_integer(), binary()) ::
          {:ok, binary(), map()} | {:error, Error.t()}
  def call(overwatch, message, metadata, deadline_ms, operator)
      when is_pid(overwatch) and is_integer(deadline_ms) and deadline_ms > 0 do
    tag = Process.alias()
    until = now_ms() + deadline_ms + Durations.get(:call_grace_ms)
    monitor = Process.monitor(overwatch)
    # The request `GenServer.call` sends, so the Overwatch answers it with
    # `GenServer.reply/2` wherever the call has moved to.
    send(
      overwatch,
      {:"$gen_call", {self(), [:alias | tag]}, {:call, message, metadata, deadline_ms}}
    )

    result = await([:alias | tag], {overwatch, monitor}, until, operator)
    Process.unalias(tag)
    result
  end

  # `watched` is the process the call lives in and its monitor, or `nil`
  # while the call moves between two.
  defp await(tag, watched, until, operator) do
    receive do
      {^tag, {:call_moving, from}} ->
        await(
          tag,
          if(match?({^from, _}, watched), do: unwatch(watched), else: watched),
          until,
          operator
        )

      {^tag, {:call_moved, to}} ->
        unwatch(watched)
        await(tag, {to, Process.monitor(to)}, until, operator)

      {^tag, reply} ->
        unwatch(watched)
        reply

      {:DOWN, monitor, :process, _pid, reason}
      when watched != nil and elem(watched, 1) == monitor ->
        unavailable(operator, reason)
    after
      max(until - now_ms(), 0) ->
        unwatch(watched)
        unavailable(operator, :timeout)
    end
  end

  defp unwatch({_pid, monitor}) do
    Process.demonitor(monitor, [:flush])
    nil
  end

  defp unwatch(nil), do: nil

  defp unavailable(operator, reason),
    do:
      {:error, Error.new("call.unavailable", %{operator: operator, reason: down_reason(reason)})}

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

  @doc "`metadata` carrying `token` as its one `flz.reply_to`, whatever the caller put there."
  @spec entry_metadata(map(), token()) :: map()
  def entry_metadata(metadata, token), do: Map.put(metadata, @reply_to, token)

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
  A row for a new call, or `{:error, busy}` when the operator already owes
  `call_pending_max` answers.
  """
  @spec admit(t(), callee(), GenServer.from(), binary() | nil, pos_integer()) ::
          {:ok, token(), t()} | {:error, Error.t()}
  def admit(%__MODULE__{} = calls, callee, from, channel, deadline_ms) do
    max = Config.get(:call_pending_max)

    if waiting(calls) >= max do
      {:error, refused(callee, "call.busy", %{pending_max: max})}
    else
      token = mint()

      row = %{
        from: from,
        channel: channel,
        started_ms: now_ms(),
        deadline_ms: deadline_ms,
        answered?: false
      }

      {:ok, token, %{calls | rows: Map.put(calls.rows, token, row)}}
    end
  end

  @doc """
  One output, before any plugin sees it: `:output` when it is not an answer,
  `{:diverted, calls}` when it is — handed to the call its token names when that
  call still waits, else dropped and counted.
  """
  @spec divert(t(), callee(), binary(), map()) :: :output | {:diverted, t()}
  def divert(%__MODULE__{} = calls, callee, message, metadata) do
    case Map.fetch(metadata, @reply_to) do
      :error -> :output
      {:ok, token} -> {:diverted, answer(calls, callee, token, message, metadata)}
    end
  end

  defp answer(calls, callee, token, message, metadata) do
    case Map.get(calls.rows, token) do
      %{answered?: false} = row ->
        GenServer.reply(row.from, {:ok, message, %{"channel" => Map.get(metadata, "channel")}})

        emit(
          :answered,
          %{duration_us: (now_ms() - row.started_ms) * 1_000},
          callee,
          %{}
        )

        %{calls | rows: Map.put(calls.rows, token, %{row | answered?: true})}

      %{answered?: true} ->
        dropped(calls, callee, "answered")

      nil ->
        dropped(calls, callee, "unclaimed")
    end
  end

  defp dropped(calls, callee, reason) do
    emit(:answer_dropped, %{count: 1}, callee, %{reason: reason})
    calls
  end

  @doc """
  A row's deadline passed: a caller still waiting is answered `call.timeout`;
  an answered row is retired. The input stays accepted either way.
  """
  @spec expire(t(), callee(), token()) :: t()
  def expire(%__MODULE__{} = calls, callee, token) do
    case Map.pop(calls.rows, token) do
      {%{answered?: false} = row, rows} ->
        emit(:timeout, %{count: 1}, callee, %{})

        error =
          Error.new("call.timeout", %{operator: callee.operator_id, deadline_ms: row.deadline_ms})

        GenServer.reply(row.from, {:error, error})
        %{calls | rows: rows}

      {_answered_or_gone, rows} ->
        %{calls | rows: rows}
    end
  end

  @doc """
  Answer the call `token` names with `code` at once, if it still waits, and
  retire its row. An answered row, or no row, is left as it is.
  """
  @spec refuse(t(), callee(), token() | nil, binary(), map()) :: t()
  def refuse(%__MODULE__{} = calls, callee, token, code, details \\ %{}) do
    case Map.pop(calls.rows, token) do
      {%{answered?: false} = row, rows} ->
        GenServer.reply(row.from, {:error, refused(callee, code, details)})
        %{calls | rows: rows}

      {_answered_or_gone, _rows} ->
        calls
    end
  end

  @doc "Answer every waiting caller `call.callee_ended` with `status`, and empty the table."
  @spec end_all(t(), callee(), binary()) :: t()
  def end_all(%__MODULE__{} = calls, callee, status) do
    reply_waiting(calls.rows, fn ->
      {:error, refused(callee, "call.callee_ended", %{status: status})}
    end)

    %__MODULE__{}
  end

  @doc """
  The rows a handover carries, each with the time it has left: a `from` is
  valid on any node, so the answer can still reach its caller from there.
  """
  @spec export(t()) :: [{token(), row()}]
  def export(%__MODULE__{rows: rows}) do
    now = now_ms()

    for {token, row} <- rows,
        do: {token, Map.put(row, :remaining_ms, max(row.started_ms + row.deadline_ms - now, 0))}
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

    rows =
      for {token, row} <- exported, into: %{} do
        # Re-based on this node's clock, so the time left is what it was.
        started = now - (row.deadline_ms - row.remaining_ms)
        {token, row |> Map.delete(:remaining_ms) |> Map.put(:started_ms, started)}
      end

    reply_waiting(rows, fn -> {:call_moved, self()} end)
    %__MODULE__{rows: rows}
  end

  @doc "The time left before the row `token` names reaches its deadline, in ms."
  @spec remaining_ms(t(), token()) :: non_neg_integer()
  def remaining_ms(%__MODULE__{rows: rows}, token) do
    row = Map.fetch!(rows, token)
    max(row.started_ms + row.deadline_ms - now_ms(), 0)
  end

  @doc """
  The error a call the operator cannot take is refused with, counted as
  `[:flezha, :call, :refused]`.
  """
  @spec refused(callee(), binary(), map()) :: Error.t()
  def refused(callee, code, details) do
    emit(:refused, %{count: 1}, callee, %{code: code})
    Error.new(code, Map.put(details, :operator, callee.operator_id))
  end

  # Every caller still waiting is answered `reply.()`, built (and an error
  # counted) once per caller.
  defp reply_waiting(rows, reply) do
    for {_token, %{answered?: false, from: from}} <- rows, do: GenServer.reply(from, reply.())
    :ok
  end

  defp emit(event, measurements, callee, metadata) do
    :telemetry.execute(
      [:flezha, :call, event],
      measurements,
      Map.merge(%{namespace: callee.namespace, operator_id: callee.operator_id}, metadata)
    )
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
