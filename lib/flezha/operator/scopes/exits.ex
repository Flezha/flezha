defmodule Flezha.Operator.Scopes.Exits do
  @moduledoc """
  A stepped scope's exit values inside an outer instance whose input is
  journaled. They ride the scope's commit into its task journal; once that
  entry is final the commit worker hands them to the outer instance (`log/2`),
  which journals each to its own input log and forwards it as any input
  (`take/5`). Each value is named by the scope's incarnation, the entry it
  rode and its place there, so a commit sent again, a stage run again after a
  crash, and a bind that logs the journal's exits again (`relog/3`) append it
  once while the outer instance's log still holds it.
  """

  require Logger

  alias Flezha.Config.Durations
  alias Flezha.Operator.{Ingest, Lookup, Overwatch, Replay}

  @typedoc "The journal entry exit values rode: `{iteration, task index}`, or an iteration's."
  @type key :: {non_neg_integer(), non_neg_integer() | :iter}

  @typedoc "The exit values one entry carries, on the channel after the embedding node."
  @type exit :: {binary(), [binary()]}

  @sends 3

  @doc """
  Hand `exit`, which the entry `key` of `scope` made final, to the outer
  instance `outer_id`, sending it again up to #{@sends} times. Runs off that
  instance's process; a log that still fails is logged, and the scope's next
  bind logs the entry's exits again.
  """
  @spec log(binary(), {binary(), term(), key(), exit()}) :: :ok | {:error, term()}
  def log(namespace, {outer_id, scope, key, exit}) do
    Enum.reduce_while(1..@sends, nil, fn _send, _last ->
      case call(namespace, outer_id, {:scope_exit, scope, key, exit}) do
        :ok -> {:halt, :ok}
        refused -> {:cont, refused}
      end
    end)
    |> tap(fn
      :ok ->
        :ok

      refused ->
        Logger.warning(
          "[Overwatch] #{outer_id} did not log the exit of scope #{inspect(scope)} " <>
            "at #{inspect(key)}: #{inspect(refused)}; its next bind logs it again"
        )
    end)
  end

  defp call(namespace, outer_id, request) do
    with {:ok, pid} when is_pid(pid) <- Lookup.operator(namespace, outer_id) do
      case GenServer.call(pid, request, Durations.get(:sdk_commit_ack_ms)) do
        verdict when verdict in [:ok, {:ok, :absorbed}, {:ok, :suspended}] -> :ok
        refused -> {:error, refused}
      end
    end
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  @doc """
  In the outer instance's process: journal each value of `exit` as an input
  on its channel and forward it, `reply_to` told the last one's verdict. The
  log answers in append order, so the last is durable after the others.
  """
  @spec take(Overwatch.t(), binary(), key(), exit(), Ingest.reply_to()) ::
          {term() | :pending, Overwatch.t()}
  def take(%Overwatch{} = state, token, key, {channel, values}, reply_to) do
    last = length(values) - 1

    values
    |> Enum.with_index()
    |> Enum.reduce({:ok, state}, fn {value, n}, {_verdict, state} ->
      metadata = %{"flz.exit" => channel, "flz.msg_id" => id(token, key, n)}
      Ingest.exit_value(state, value, metadata, if(n == last, do: reply_to, else: :cast))
    end)
  end

  @doc """
  At a bind of the scope `scope_id`: log again the exits its journal holds for
  its open pass and for the iteration it last committed, which no ack may
  have followed.
  """
  @spec relog(Overwatch.t(), binary(), binary()) :: Overwatch.t()
  def relog(%Overwatch{} = state, token, scope_id) do
    Enum.reduce(Replay.owed_exits(scope_id), state, fn {key, exit}, state ->
      state |> take(token, key, exit, :cast) |> elem(1)
    end)
  end

  @doc "The `flz.msg_id` of value `n` of the entry `key` of the scope incarnation `token`."
  @spec id(binary(), key(), non_neg_integer()) :: binary()
  def id(token, {iter, :iter}, n), do: "x:#{token}:#{iter}:i:#{n}"
  def id(token, {iter, task_idx}, n), do: "x:#{token}:#{iter}:#{task_idx}:#{n}"
end
