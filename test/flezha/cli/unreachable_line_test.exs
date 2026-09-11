defmodule Flezha.Service.Cli.UnreachableLineTest do
  @moduledoc """
  `flz boot await` tells a controller it cannot reach from one that refused by a
  single line of stderr, and the line is Elixir's, not Flezha's: `elixir
  --rpc-eval` prints it for every connect that failed, with the exit 1 a reached
  controller's refusal also has. The CLI matches it exactly, as `NOCONNECTION`
  in `cli/src/rpc.rs`, and its own test answers with a shim, so only this drive
  can see a toolchain that rewords the line: it runs the `elixir --rpc-eval`
  that `flzvm rpc` runs, with the same flags, against a name nobody holds.
  """
  use ExUnit.Case, async: true

  @rpc_rs Path.expand("../../../cli/src/rpc.rs", __DIR__)

  @tag timeout: 20_000
  test "a node nobody holds gets exactly the line `flz boot await` waits on" do
    # The child registers with the epmd this suite's node started and
    # unregisters when it exits; with no epmd running it would start one.
    assert Node.alive?(), "the suite's node is not distributed, so no epmd is known to run"

    expected = cli_noconnection_line()
    elixir = System.find_executable("elixir") || flunk("no elixir on PATH")
    id = System.unique_integer([:positive])

    args = [
      "--hidden",
      "--cookie",
      "unreachable_line_#{id}",
      "--name",
      "unreachable_line_#{id}@127.0.0.1",
      "--rpc-eval",
      "no_such_node_#{id}@127.0.0.1",
      ":ok"
    ]

    {out, status} = System.cmd(elixir, args, stderr_to_stdout: true)

    assert status == 1, "`elixir --rpc-eval` exited #{status}, not 1:\n#{out}"

    assert expected in String.split(out, "\n", trim: true), """
    `elixir --rpc-eval` no longer prints `NOCONNECTION` from cli/src/rpc.rs for a node
    it cannot reach, so `flz boot await` would give up at once on a controller that
    is still starting. Match the constant to what `printed` shows, or change the check.
      NOCONNECTION: #{expected}
      printed:      #{out}
    """
  end

  defp cli_noconnection_line do
    case Regex.run(~r/^const NOCONNECTION: &str = "(.+)";$/m, File.read!(@rpc_rs)) do
      [_, line] -> line
      nil -> flunk("cli/src/rpc.rs declares no `const NOCONNECTION: &str`")
    end
  end
end
