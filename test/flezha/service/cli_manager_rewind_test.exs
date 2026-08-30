defmodule Flezha.Service.CliManagerRewindTest do
  @moduledoc """
  `flz operative rewind <instance> --to-iteration N` operator surface: how a
  rewind that went through, and each verdict the Overwatch guard can return, is
  reported. The guard's own logic is covered in `overwatch_suspension_test`.
  """
  # async: false: Mimic global mode; Mimic stubs.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Service.CliManager

  setup :set_mimic_global_until_exit

  defp instance_id(), do: "rw_#{System.unique_integer([:positive])}"

  # `operative_rewind` resolves a pid and calls `{:rewind, n}` on it, so a process
  # that answers with a fixed verdict exercises the whole CLI surface.
  defp stub_overwatch(inst, verdict) do
    pid = spawn_link(fn -> reply_loop(verdict) end)
    stub(Flezha.Service.OperativeManager, :get_operative_pid, fn _ns, ^inst -> {:ok, pid} end)
  end

  defp reply_loop(verdict) do
    receive do
      {:"$gen_call", from, {:rewind, _to_iter}} ->
        GenServer.reply(from, verdict)
        reply_loop(verdict)
    end
  end

  # Error verdicts exit(:cli_error) for the CLI exit code (see cli_error!/1) while
  # a successful rewind returns normally; swallow the exit either way so the
  # printed report stays assertable.
  defp rewind(inst, to_iter) do
    capture_io(fn ->
      try do
        CliManager.operative_rewind("default", inst, to_iter)
      catch
        :exit, _ -> :ok
      end
    end)
  end

  test "a rewind that goes through reports where the loop resumes" do
    inst = instance_id()
    stub_overwatch(inst, :ok)

    out = rewind(inst, 3)

    assert out =~ "rewound to iteration 3"
    assert out =~ "resumes at 4"
  end

  test "an uncommitted target is refused, naming the committed range" do
    inst = instance_id()
    stub_overwatch(inst, {:error, {:uncommitted_iteration, [0, 1, 2]}})

    out = rewind(inst, 7)

    assert out =~ "iteration 7 has not committed"
    assert out =~ "Committed iterations: 0..2"
  end

  test "a loop with nothing committed says so rather than printing an empty range" do
    inst = instance_id()
    stub_overwatch(inst, {:error, {:uncommitted_iteration, []}})

    assert rewind(inst, 0) =~ "no committed iterations"
  end

  test "an uncompensable emit is refused, naming the channels" do
    inst = instance_id()
    stub_overwatch(inst, {:error, {:uncovered, ["replies", "audit"]}})

    out = rewind(inst, 1)

    assert out =~ "refusing to rewind"
    assert out =~ "replies, audit"
    assert out =~ "no `compensate` hook"
  end

  test "an unknown operative is reported, not crashed on" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :get_operative_pid, fn _ns, ^inst -> {:error, :nf} end)

    assert rewind(inst, 1) =~ "not found"
  end
end
