defmodule Flezha.Service.Cli.FailureStreamTest do
  @moduledoc """
  Everything a `flz` verb says that is not its answer reaches the operator on
  stderr — the operator's, on whichever node the verb ran and from whichever
  process says it — and none of it reaches the data.

  An operator who pipes a verb's output into a file or another program is
  collecting the answer, not the failure. Before the fold the stream was a
  per-call argument and 29 of the 65 sites took its `:stdio` default, so a
  script reading `flz operative stop` collected the error text as if it were
  the answer.

  The in-process drives below cannot see which node a device belongs to,
  because the caller and the callee are the same node here. `flzvm rpc` runs
  the verb on the controller, so the last two tests drive a real second node.
  """
  # async: false: these drive the live controller's own CLI surface.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Flezha.Service.Cli
  alias Flezha.Service.Cli.Controller
  alias FlezhaTest.Helper.NodeTestHelper

  @call %{opts: []}

  # One real failure per family that renders one. Each is reached with the
  # controller as this suite boots it: no runner, no operative, no plugin.
  defp failures do
    [
      {"operative", fn -> Cli.Operative.operative_stop("default", "no-such-op-xyz", @call) end},
      {"operative", fn -> Cli.Operative.cell_state("default", "no-such-op-xyz", @call) end},
      {"plugin", fn -> Cli.Plugin.disable_plugin("no-such-plugin-xyz", @call) end},
      {"state", fn -> Cli.State.state_retention("some_class", "not-a-number", @call) end},
      {"observe", fn -> Cli.Observe.observability_render_enable("no-such-source-xyz", @call) end},
      {"schedule",
       fn -> Cli.Schedule.schedule_add("s", %{opts: [kind: "stream", cron: "* * * * *"]}) end},
      {"schedule", fn -> Cli.Schedule.schedule_add("s", %{opts: [init_values: "not json"]}) end}
    ]
  end

  describe "a rendered failure" do
    test "lands on stderr, leaves stdout empty and exits non-zero" do
      wrong =
        for {family, fun} <- failures(), reduce: [] do
          acc ->
            {outcome, out, err} = drive(fun)

            cond do
              outcome != {:exit, {:shutdown, 1}} ->
                ["#{family}: exited #{inspect(outcome)}, want {:exit, {:shutdown, 1}}" | acc]

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
    test "keeps the device, the write and the exit to itself" do
      exported = Cli.Context.__info__(:functions)

      for name <- [:cli_error!, :operator_stderr, :puts, :cli_exit] do
        refute Keyword.has_key?(exported, name),
               "a Cli.* module can say how a failure is said while #{name} is exported"
      end
    end
  end

  describe "a failure rendered on the controller" do
    @describetag :capture_log

    test "lands on the caller's stderr, and exits without a banner over it" do
      # The path `flzvm rpc` takes: `elixir --rpc-eval` is
      # `:erpc.call(node, Kernel.CLI, :rpc_eval, [expr])` against a node that
      # holds this node's code, and `rpc_eval/1` catches the exit rather than
      # propagating it. Elixir must be started there to evaluate at all.
      {_pid, peer} = NodeTestHelper.start_bare_peer(:caller_stderr)
      {:ok, _apps} = :erpc.call(peer, :application, :ensure_all_started, [:elixir])

      {outcome, out, err} =
        drive(fn ->
          :erpc.call(peer, Kernel.CLI, :rpc_eval, [~s|Flezha.Service.Cli.Context.fail!("boom")|])
        end)

      assert err =~ "error: boom",
             "the failure stayed on #{peer}; the caller's stderr held #{inspect(err)}"

      assert out == ""

      # `Kernel.CLI.exec_fun/2` halts `{:shutdown, int}` with that int and
      # prints nothing; every other reason it prints `** (exit) …` for before
      # halting 1. Both halt 1, so no `flz` exit code moves and the sentence
      # above is not buried.
      assert {:ok, {:exit, {:shutdown, 1}, _stack}} = outcome
    end
  end

  describe "a progress line from another process" do
    # `flz controller stop`'s lines are emitted by the shutdown coordinator, so
    # the handler runs outside the rpc process and cannot resolve the operator
    # itself. It writes through the notifier bound on the way in.
    test "reaches the operator through the notifier bound at attach" do
      notify = Cli.Context.notifier()

      err =
        capture_io(:stderr, fn ->
          fn ->
            Controller.controller_stop_progress_handler(
              [:flezha, :shutdown, :phase_started],
              %{},
              %{phase: :quiesce},
              %{caller: self(), notify: notify}
            )
          end
          |> Task.async()
          |> Task.await()
        end)

      assert err =~ "[shutdown] phase=quiesce started"
    end
  end

  describe "the CLI tree" do
    # `Context` owns the device and is the one module allowed to name a stream;
    # every other file asking for `:stderr` by name gets the stream of the node
    # it runs on, which under `flzvm rpc` is the controller's log.
    @device_owner "lib/flezha/cli/context.ex"

    test "names no stream of its own" do
      offenders =
        "lib/flezha/cli/**/*.ex"
        |> Path.wildcard()
        |> Enum.reject(&(&1 == @device_owner))
        |> Enum.flat_map(fn path ->
          path
          |> File.read!()
          |> String.split("\n")
          |> Enum.with_index(1)
          |> Enum.filter(fn {line, _n} -> line =~ ":stderr" end)
          |> Enum.map(fn {line, n} -> "#{path}:#{n}: #{String.trim(line)}" end)
        end)

      assert offenders == [],
             "a CLI module writing to `:stderr` writes to the node it runs on, " <>
               "which under `flzvm rpc` is the controller. Use " <>
               "`Context.fail!/2`, `Context.halt!/1` or `Context.notifier/0`:\n" <>
               Enum.join(offenders, "\n")
    end
  end

  # Run `fun` with the two streams captured apart, and answer what it did:
  # `{:exit, reason}` for a verb that halted, `{:ok, value}` for one that
  # returned. `capture_io/2` runs its function in the calling process, so the
  # nested captures hand their results back through the mailbox.
  defp drive(fun) do
    err =
      capture_io(:stderr, fn ->
        out = capture_io(fn -> send(self(), {:outcome, outcome(fun)}) end)
        receive do: ({:outcome, outcome} -> send(self(), {:pair, outcome, out}))
      end)

    assert_received {:pair, outcome, out}
    {outcome, out, err}
  end

  defp outcome(fun) do
    {:ok, fun.()}
  catch
    :exit, reason -> {:exit, reason}
  end
end
