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

  What a batch commits stays the worker's, in private functions it defines:

    * `commit_batch(state, records)`: `{:ok, state, confirmed}`, `confirmed`
      the metadata whose holds the commit answers, or `{:error, reason, state}`;
    * `batch_failed(state, reason, failures)`: the state once the failure is
      logged and recorded;
    * `before_batch(state)`, with `before_batch: true`: work every flush does
      first, even with nothing buffered: `{:ok, state}` or
      `{:error, reason, state}`, a failure retried as a batch's is.

  The worker's state carries `buffer`, `buffer_len`, `max_batch`, `linger_ms`,
  `plugin_id`, `retry_backoff_ms`, `retry_max_backoff_ms`,
  `consecutive_failures`, `last_error`, `queue_max` and `waiters`.
  """

  defmacro __using__(opts) do
    ladder = Keyword.fetch!(opts, :ladder)
    retry = Keyword.fetch!(opts, :retry)

    quote do
      @compile {:inline, batch: 2}

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

      unquote(flush(Keyword.get(opts, :before_batch, false)))
      unquote(loop(ladder, retry))
    end
  end

  defp flush(true = _before_batch) do
    quote do
      defp flush(state) do
        case before_batch(state) do
          {:ok, %{buffer_len: 0} = state} -> state
          {:ok, state} -> commit_next(state)
          {:error, reason, state} -> retry_batch(state, reason)
        end
      end
    end
  end

  defp flush(false) do
    quote do
      defp flush(%{buffer_len: 0} = state), do: state
      defp flush(state), do: commit_next(state)
    end
  end

  defp loop(ladder, retry) do
    quote do
      defp commit_next(state) do
        {taken, rest} = :queue.split(min(state.buffer_len, state.max_batch), state.buffer)
        records = :queue.to_list(taken)

        case commit_batch(state, records) do
          {:ok, state, confirmed} ->
            Flezha.State.Outbox.confirm_many(confirmed)

            %{
              state
              | buffer: rest,
                buffer_len: state.buffer_len - length(records),
                consecutive_failures:
                  Flezha.Config.Backoff.reset(unquote(ladder), state.plugin_id),
                last_error: nil
            }
            |> Flezha.Process.Backlog.release_waiters()
            |> flush()

          {:error, reason, state} ->
            retry_batch(state, reason)
        end
      end

      defp retry_batch(state, reason) do
        failures = state.consecutive_failures + 1
        state = batch_failed(state, reason, failures)

        delay =
          Flezha.Config.Backoff.arm(unquote(ladder), failures, state.plugin_id,
            base: state.retry_backoff_ms,
            cap: state.retry_max_backoff_ms
          )

        arm_timer(%{state | consecutive_failures: failures}, :retry, delay, unquote(retry))
      end
    end
  end
end
