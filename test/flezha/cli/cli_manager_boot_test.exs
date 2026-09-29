defmodule Flezha.Service.CliManagerBootTest do
  @moduledoc """
  `flz boot status` and `flz boot await` read one verdict and derive nothing of
  their own.

  The table-vs-tree case is the point of the file: every row
  `Flezha.Boot.Readiness` declares is rendered by `flz boot status`, and the
  status screen names no precondition the table does not carry — so a row added
  with no reader, or a reader inventing a key, fails here.

  `boot await` is a container's healthcheck as well as a caller's wait, so
  both of its shapes are pinned: a single evaluation at `--timeout 0`, and a
  refusal that names what is still unmet rather than reporting success because
  the set of things to wait for happens to be empty.
  """

  # async: false: the readiness flag and the recovery cache are node-global.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Flezha.Boot.Readiness
  alias Flezha.Convergence.Snapshot
  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.CliCapture

  setup do
    Snapshot.reset_recovery_states()
    Readiness.reset()

    on_exit(fn ->
      Snapshot.reset_recovery_states()
      # The application's own boot published it; leave the node as found.
      Readiness.publish_sequence_complete()
    end)

    :ok
  end

  describe "flz boot status" do
    test "renders the verdict and every row the table declares" do
      Readiness.publish_sequence_complete()

      output = capture_io(fn -> CliManager.boot_status() end)
      {verdict, rows} = Readiness.verdict()

      assert output =~ "readiness: #{verdict}"

      for %{key: key} <- rows do
        assert output =~ to_string(key),
               "`flz boot status` does not render the #{key} precondition"
      end
    end

    test "names no precondition the table does not carry" do
      Readiness.publish_sequence_complete()

      output = capture_io(fn -> CliManager.boot_status() end)
      declared = for %{key: key} <- elem(Readiness.verdict(), 1), do: to_string(key)

      named =
        output
        |> String.split("\n")
        |> Enum.flat_map(fn line ->
          case Regex.run(~r/^\s{2}(?:met|unmet)\s+(\S+)$/, line) do
            [_, key] -> [key]
            nil -> []
          end
        end)

      assert Enum.sort(named) == Enum.sort(declared)
    end

    test "reports the unmet row while the boot sequence is not complete" do
      output = capture_io(fn -> CliManager.boot_status() end)

      assert output =~ "readiness: booting"
      assert output =~ "unmet  boot_sequence_complete"
    end
  end

  describe "flz boot await" do
    test "exits non-zero naming the unmet row when no hub exists yet" do
      # The empty-set defect: with no namespace hub there is nothing to iterate,
      # and the verb used to report every namespace reached `:normal`.
      assert {{:shutdown, 7}, stderr} = await(0)

      assert stderr =~
               "its readiness is booting, with boot_sequence_complete unmet (boot.not_ready)"

      refute stderr =~ "reached :normal"
    end

    test "a zero timeout evaluates the verdict once and exits zero when ready" do
      Readiness.publish_sequence_complete()

      assert {:ok, stdout} = await(0)
      assert stdout =~ "boot await: ready"
    end

    test "a recovering hub holds the wait, and names the row when it elapses" do
      Readiness.publish_sequence_complete()
      hub_id = :"boot_await_hub_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :recovery_pending)

      assert {{:shutdown, 7}, stderr} = await(300)
      assert stderr =~ "its readiness is recovering, with hubs_recovered unmet (boot.not_ready)"

      Snapshot.observe_recovery(hub_id, :normal)
      assert {:ok, _stdout} = await(0)
    end
  end

  # `boot_await` exits `{:shutdown, 7}` after printing on stderr, or returns after
  # printing on stdout; either outcome is one assertable string.
  defp await(timeout_ms) do
    case CliCapture.run(fn -> CliManager.boot_await(timeout_ms) end) do
      {{:ok, _}, out, _err} -> {:ok, out}
      {{:exit, reason}, _out, err} -> {reason, err}
    end
  end
end
