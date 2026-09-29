defmodule Flezha.Service.Cli.FailureStreamTest do
  @moduledoc """
  Everything a `flz` verb says that is not its answer reaches the caller on
  stderr — the caller's, on whichever node the verb ran and from whichever
  process says it — and none of it reaches the data.

  A caller who pipes a verb's output into a file or another program is
  collecting the answer, not the failure. Before the fold the stream was a
  per-call argument and 29 of the 65 sites took its `:stdio` default, so a
  script reading `flz operator stop` collected the error text as if it were
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
  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.CliCapture
  alias FlezhaTest.Helper.NodeTestHelper

  # One real failure per family that renders one, through the wire seam that
  # renders it. Each is reached with the controller as this suite boots it: no
  # runner, no operator, no plugin.
  defp failures do
    [
      {"operator", fn -> CliManager.operator_stop("default", "no-such-op-xyz") end},
      {"operator", fn -> CliManager.cell_state("default", "no-such-op-xyz") end},
      {"plugin", fn -> CliManager.disable_plugin("no-such-plugin-xyz") end},
      {"state", fn -> CliManager.state_retention("some_class", "not-a-number") end},
      {"observe", fn -> CliManager.observability_render_enable("no-such-source-xyz") end},
      {"schedule", fn -> CliManager.schedule_add("s", cron: "* * * * *") end},
      {"schedule", fn -> CliManager.schedule_add("s", init_values: "not json") end}
    ]
  end

  describe "a rendered failure" do
    test "lands on stderr, leaves stdout empty and exits non-zero" do
      wrong =
        for {family, fun} <- failures(), reduce: [] do
          acc ->
            {outcome, out, err} = CliCapture.run(fun)

            cond do
              not match?({:exit, {:shutdown, code}} when code in 1..7, outcome) ->
                ["#{family}: exited #{inspect(outcome)}, want a class's exit code" | acc]

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
        {_outcome, _out, err} = CliCapture.run(fun)

        assert String.starts_with?(err, "error: "),
               "#{family} rendered #{inspect(err)}, which does not start with the one prefix"
      end
    end
  end

  describe "the printer" do
    test "keeps the device, the write and the exit to itself" do
      exported = Cli.Context.__info__(:functions)

      for name <- [:cli_error!, :caller_stderr, :puts, :cli_exit] do
        refute Keyword.has_key?(exported, name),
               "a Cli.* module can say how a failure is said while #{name} is exported"
      end
    end
  end

  describe "the class of a failure" do
    # The two classes only `flz` decides, a malformed command line and a
    # controller it cannot reach, are proved through the binary in
    # `cli/tests/dispatch.rs` and `cli/tests/exit_codes.rs`; a refusal for want
    # of a role in `test/flezha/authz/enforcement_test.exs`, a conflict in the
    # authz render tests, and a controller that is not ready in
    # `cli_manager_boot_test.exs`.
    test "each refusal exits with its class's code" do
      Process.put(:flezha_uncatalogued_expected, true)
      injected = Flezha.Error.from_reason(:reason_no_catalogue_names, :"namespace.create")

      cases = [
        {2, fn -> CliManager.state_retention("some_class", "not-a-number") end},
        {5, fn -> CliManager.disable_plugin("no-such-plugin-xyz") end},
        {1, fn -> Cli.Context.render!(injected, []) end}
      ]

      for {code, fun} <- cases do
        {outcome, _out, err} = CliCapture.run(fun)
        assert outcome == {:exit, {:shutdown, code}}, "#{err} exited #{inspect(outcome)}"
      end
    end

    test "under --json the error is one object on stderr, and stdout carries none of it" do
      {outcome, out, err} =
        CliCapture.run(fn -> CliManager.disable_plugin("ghost", output: "json") end)

      assert outcome == {:exit, {:shutdown, 5}}
      assert out == ""
      assert [line] = String.split(err, "\n", trim: true)

      assert Jason.decode!(line) == %{
               "error" => %{
                 "code" => "plugin.not_found",
                 "class" => "not_found",
                 "message" => "plugin 'ghost' is not enabled",
                 "details" => %{"plugin" => "ghost"},
                 "hint" => "flz plugin list"
               }
             }
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

      expression =
        ~s|Flezha.Service.Cli.Context.render!(Flezha.Error.new("namespace.not_found", namespace: "ghost"), [])|

      {outcome, out, err} =
        CliCapture.run(fn -> :erpc.call(peer, Kernel.CLI, :rpc_eval, [expression]) end)

      assert err =~ "error: namespace 'ghost' does not exist (namespace.not_found)",
             "the failure stayed on #{peer}; the caller's stderr held #{inspect(err)}"

      assert out == ""

      # `Kernel.CLI.exec_fun/2` halts `{:shutdown, int}` with that int and
      # prints nothing, so the class's code reaches `flz` and the sentence above
      # is not buried under an exit banner.
      assert {:ok, {:exit, {:shutdown, 5}, _stack}} = outcome
    end
  end

  describe "a progress line from another process" do
    # `flz controller stop`'s lines are emitted by the shutdown coordinator, so
    # the handler runs outside the rpc process and cannot resolve the caller
    # itself. It writes through the notifier bound on the way in.
    test "reaches the caller through the notifier bound at attach" do
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
               "`Context.fail!/1` or `Context.notifier/0`:\n" <>
               Enum.join(offenders, "\n")
    end
  end
end
