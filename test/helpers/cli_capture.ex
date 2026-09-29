defmodule FlezhaTest.Helper.CliCapture do
  @moduledoc """
  Run a CLI verb in-process and keep everything it said: `{outcome, stdout,
  stderr}`, the outcome `{:ok, value}` or `{:exit, reason}`. A refusal renders
  on stderr and then exits `{:shutdown, code}`, so a test reads either path
  from one call. `capture_io/2` runs its function in the calling process, so
  the nested captures hand their results back through the mailbox.
  """
  import ExUnit.CaptureIO

  @spec run((-> term())) :: {{:ok, term()} | {:exit, term()}, String.t(), String.t()}
  def run(fun) do
    err =
      capture_io(:stderr, fn ->
        out = capture_io(fn -> send(self(), {:cli_outcome, outcome(fun)}) end)
        receive do: ({:cli_outcome, outcome} -> send(self(), {:cli_captured, outcome, out}))
      end)

    receive do: ({:cli_captured, outcome, out} -> {outcome, out, err})
  end

  defp outcome(fun) do
    {:ok, fun.()}
  catch
    :exit, reason -> {:exit, reason}
  end
end
