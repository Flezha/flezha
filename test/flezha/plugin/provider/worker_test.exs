defmodule Flezha.Plugin.Provider.WorkerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Flezha.Plugin.Provider.Worker

  @subject "[worker_test] read source=a"

  setup do
    :ok = :logger.set_module_level(Worker, :all)
    on_exit(fn -> :logger.unset_module_level(Worker) end)
  end

  defp run(failures) do
    Enum.reduce(1..failures, %{consecutive_failures: 0, last_error: nil}, fn _, state ->
      Worker.failed(state, :econnrefused, @subject)
    end)
  end

  test "a failure run is logged at its first attempt and at every tenfold count" do
    log = capture_log(fn -> run(120) end)

    logged =
      ~r/consecutive=(\d+)/ |> Regex.scan(log) |> Enum.map(fn [_, n] -> String.to_integer(n) end)

    assert logged == [1, 10, 100]
    assert log =~ "#{@subject} failed consecutive=1 reason=:econnrefused"
  end

  test "an error that changes mid-run is logged at once, and the new one counted quietly" do
    {state, _log} = with_log(fn -> run(3) end)

    log =
      capture_log(fn ->
        state
        |> Worker.failed({:http_status, 401}, @subject)
        |> Worker.failed({:http_status, 401}, @subject)
      end)

    assert [_once] = Regex.scan(~r/failed consecutive=4 reason=\{:http_status, 401\}/, log)
    refute log =~ "consecutive=5"
  end

  test "the end of a run is logged once, with its length; a clean read logs nothing" do
    {state, _log} = with_log(fn -> run(7) end)

    {clean, log} = with_log(fn -> Worker.recovered(state, @subject) end)
    assert log =~ "#{@subject} recovered after 7 consecutive failures"
    assert %{consecutive_failures: 0, last_error: nil} = clean

    refute capture_log(fn -> Worker.recovered(clean, @subject) end) =~ @subject
  end
end
