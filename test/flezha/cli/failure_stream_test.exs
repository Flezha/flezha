defmodule Flezha.Service.Cli.FailureStreamTest do
  @moduledoc """
  Every `flz` failure reaches the operator on stderr, and none of it reaches
  the data.

  An operator who pipes a verb's output into a file or another program is
  collecting the answer, not the failure. Before the fold the stream was a
  per-call argument and 29 of the 65 sites took its `:stdio` default, so a
  script reading `flz operative stop` collected the error text as if it were
  the answer.
  """
  # async: false: these drive the live controller's own CLI surface.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Flezha.Service.Cli

  @call %{opts: []}

  # One real failure per family that renders one. Each is reached with the
  # controller as this suite boots it: no runner, no operative, no plugin.
  defp failures do
    [
      {"operative", fn -> Cli.Operative.operative_stop("default", "no-such-op-xyz", @call) end},
      {"operative", fn -> Cli.Operative.entity_state("default", "no-such-op-xyz", @call) end},
      {"plugin", fn -> Cli.Plugin.disable_plugin("no-such-plugin-xyz", @call) end},
      {"state", fn -> Cli.State.state_retention("some_class", "not-a-number", @call) end},
      {"observe", fn -> Cli.Observe.observability_render_enable("no-such-source-xyz", @call) end},
      {"schedule",
       fn -> Cli.Schedule.schedule_add("s", %{opts: [kind: "stream", cron: "* * * * *"]}) end},
      {"schedule", fn -> Cli.Schedule.schedule_add("s", %{opts: [init_values: "not json"]}) end}
    ]
  end

  describe "a rendered failure" do
    test "lands on stderr, leaves stdout empty and exits :cli_error" do
      wrong =
        for {family, fun} <- failures(), reduce: [] do
          acc ->
            {outcome, out, err} = drive(fun)

            cond do
              outcome != {:exit, :cli_error} ->
                ["#{family}: exited #{inspect(outcome)}, want {:exit, :cli_error}" | acc]

              err == "" ->
                ["#{family}: printed nothing on stderr" | acc]

              out != "" ->
                ["#{family}: printed #{inspect(out)} on stdout" | acc]

              true ->
                acc
            end
        end

      assert wrong == [], Enum.join(wrong, "\n")
    end

    test "carries one prefix, whatever family rendered it" do
      for {family, fun} <- failures() do
        {_outcome, _out, err} = drive(fun)

        assert String.starts_with?(err, "error: "),
               "#{family} rendered #{inspect(err)}, which does not start with the one prefix"
      end
    end
  end

  describe "the printer" do
    test "is not reachable from outside the context module" do
      exported = Cli.Context.__info__(:functions)

      refute Keyword.has_key?(exported, :cli_error!),
             "a Cli.* module can name the stream again while cli_error! is exported"
    end
  end

  # Run `fun`, capturing the two streams separately, and answer what it did.
  defp drive(fun) do
    err =
      capture_io(:stderr, fn ->
        out = capture_io(fn -> send(self(), {:outcome, catch_exit(fun.())}) end)
        receive do: ({:outcome, outcome} -> send(self(), {:pair, outcome, out}))
      end)

    assert_received {:pair, outcome, out}
    {{:exit, outcome}, out, err}
  end
end
