defmodule Flezha.Plugin.Provider.Postgres.SinkWorkerTest do
  @moduledoc """
  The Postgres sink worker's linger and retry timers, with no database. The test
  process stands in for the worker's GenServer loop: each timer the worker arms
  fires into this mailbox and is handed back to its callback. The database paths
  are `postgres_sink_test.exs`, which the default run excludes.
  """

  # async: false: opens the worker's module log level, which is global.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Plugin.Provider.Postgres.SinkWorker

  # A 0 ms timer can arrive after ExUnit's default 100 ms on a loaded box.
  @fired_within 5_000

  setup do
    # The suite's primary level drops a warning; open the worker so the failed
    # flush's line reaches the capture.
    :ok = :logger.set_module_level(SinkWorker, :all)
    on_exit(fn -> :logger.unset_module_level(SinkWorker) end)
  end

  test "a linger flushes an enqueued record, and each failed flush arms the next retry" do
    {:reply, :ok, state} =
      SinkWorker.handle_call({:enqueue, "v1", %{}}, {self(), make_ref()}, worker())

    assert_receive :linger, @fired_within
    state = failed_flush(:linger, state, 1)
    assert_receive :retry_flush, @fired_within
    state = failed_flush(:retry_flush, state, 2)
    assert_receive :retry_flush, @fired_within

    # Re-arming hides a fired retry left armed, so hand this one back with the
    # buffer emptied: nothing re-arms, and a stale key would stall every enqueue.
    assert {:noreply, %{timers: timers}} =
             SinkWorker.handle_info(:retry_flush, %{state | buffer: :queue.new(), buffer_len: 0})

    assert timers == %{}, "a fired retry must clear its key"
  end

  # With no pool process the transaction exits `:noproc`, so every flush fails.
  defp worker do
    %SinkWorker{
      plugin_id: "sink-worker-test",
      conn_label: %{hostname: "db", port: 5432, database: "sink"},
      linger_ms: 0,
      max_batch: 10,
      queue_max: 10,
      retry_backoff_ms: 0,
      retry_max_backoff_ms: 0
    }
  end

  defp failed_flush(msg, state, attempt) do
    {{:noreply, state}, log} = with_log(fn -> SinkWorker.handle_info(msg, state) end)

    assert log =~ "[postgres-sink] db:5432/sink flush failed (attempt #{attempt})"
    assert %{buffer_len: 1, consecutive_failures: ^attempt} = state

    assert Map.keys(state.timers) == [:retry],
           "the fired timer is cleared and only the retry is armed"

    state
  end
end
