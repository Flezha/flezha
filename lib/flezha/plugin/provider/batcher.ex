defmodule Flezha.Plugin.Provider.Batcher do
  @moduledoc """
  The one batching loop of a sink worker that buffers its outputs and commits
  them in batches: the file write worker and the Postgres sink worker.

      use Flezha.Process.Timer, keys: [:linger, :retry]
      use Flezha.Plugin.Provider.Batcher, ladder: :postgres_flush, retry: :retry_flush

  It injects two private functions. `batch/2` buffers one record and commits at
  once when `max_batch` are waiting, or arms the `:linger` timer. `flush/1`
  commits batches of at most `max_batch` while records wait: a committed batch
  confirms the outbox holds it names in one report, resets the `ladder` backoff
  row and releases the enqueue callers held past `queue_max`
  (`Flezha.Process.Backlog.release_waiters/1`); a failed one arms the `:retry`
  timer on that row, which sends `retry`. Nothing is dropped.

  What a batch commits stays the worker's, in the callbacks of this behaviour.
  The worker's state carries `buffer`, `buffer_len`, `max_batch`, `linger_ms`,
  `plugin_id`, `retry_backoff_ms`, `retry_max_backoff_ms`,
  `consecutive_failures`, `last_error`, `queue_max`, `waiters` and `timers`.
  """

  alias Flezha.Config.Backoff
  alias Flezha.Process.Backlog
  alias Flezha.Process.Timer

  @doc """
  Commit `records`: `{:ok, state, confirmed}`, `confirmed` the metadata whose
  holds the commit answers, or `{:error, reason, state}`.
  """
  @callback commit_batch(state :: map(), records :: [term()]) ::
              {:ok, map(), [map()]} | {:error, term(), map()}

  @doc "The state once a failed batch, the `failures`th in a row, is logged and recorded."
  @callback batch_failed(state :: map(), reason :: term(), failures :: pos_integer()) :: map()

  @doc """
  Work every flush does first, even with nothing buffered: `{:ok, state}` or
  `{:error, reason, state}`, a failure retried as a batch's is. Nothing by default.
  """
  @callback before_batch(state :: map()) :: {:ok, map()} | {:error, term(), map()}

  defmacro __using__(opts) do
    ladder = Keyword.fetch!(opts, :ladder)
    retry = Keyword.fetch!(opts, :retry)

    quote do
      @behaviour unquote(__MODULE__)
      @compile {:inline, batch: 2}

      @impl unquote(__MODULE__)
      def before_batch(state), do: {:ok, state}

      defoverridable before_batch: 1

      defp batch(state, record) do
        state = %{
          state
          | buffer: :queue.in(record, state.buffer),
            buffer_len: state.buffer_len + 1
        }

        cond do
          state.buffer_len >= state.max_batch and not timer_armed?(state, :retry) ->
            state |> cancel_timer(:linger) |> flush()

          not timer_armed?(state, :linger) and not timer_armed?(state, :retry) ->
            arm_timer(state, :linger, state.linger_ms, :linger)

          true ->
            state
        end
      end

      # The worker names the outbox, so the loop joins no compile cycle through it.
      defp flush(state) do
        loop = {unquote(ladder), unquote(retry), &Flezha.State.Outbox.confirm_many/1}
        unquote(__MODULE__).flush(state, __MODULE__, loop)
      end
    end
  end

  @doc false
  @spec flush(map(), module(), {atom(), term(), ([map()] -> term())}) :: map()
  def flush(state, worker, loop) do
    case worker.before_batch(state) do
      {:ok, %{buffer_len: 0} = state} -> state
      {:ok, state} -> commit_next(state, worker, loop)
      {:error, reason, state} -> retry_batch(state, reason, worker, loop)
    end
  end

  defp commit_next(state, worker, {ladder, _retry, confirm} = loop) do
    {taken, rest} = :queue.split(min(state.buffer_len, state.max_batch), state.buffer)
    records = :queue.to_list(taken)

    case worker.commit_batch(state, records) do
      {:ok, state, confirmed} ->
        confirm.(confirmed)

        %{
          state
          | buffer: rest,
            buffer_len: state.buffer_len - length(records),
            consecutive_failures: Backoff.reset(ladder, state.plugin_id),
            last_error: nil
        }
        |> Backlog.release_waiters()
        |> flush(worker, loop)

      {:error, reason, state} ->
        retry_batch(state, reason, worker, loop)
    end
  end

  defp retry_batch(state, reason, worker, {ladder, retry, _confirm}) do
    failures = state.consecutive_failures + 1
    state = worker.batch_failed(state, reason, failures)

    delay =
      Backoff.arm(ladder, failures, state.plugin_id,
        base: state.retry_backoff_ms,
        cap: state.retry_max_backoff_ms
      )

    %{
      state
      | consecutive_failures: failures,
        timers: Timer.arm(state.timers, :retry, delay, retry)
    }
  end
end
