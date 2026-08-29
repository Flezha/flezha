defmodule Flezha.State.CompletionTest do
  @moduledoc """
  §5b.3: the per-operative completion record — atomic write, read-back, and its
  presence as the retain-vs-delete signal.
  """
  # async: false: writes Application env; a shared on-disk root.
  use ExUnit.Case, async: false

  alias Flezha.State.{Completion, TestHelpers}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    :ok
  end

  test "write/read round-trip preserves the record" do
    rec = %{namespace: "ns", status: "ok", error: "", result: "R", finished_at: 123}

    refute Completion.exists?("op1")
    assert :ok = Completion.write("op1", rec)
    assert Completion.exists?("op1")
    assert {:ok, ^rec} = Completion.read("op1")
  end

  test "read/1 returns :error when no record is present" do
    assert :error = Completion.read("absent")
  end

  test "delete/1 removes the record" do
    :ok =
      Completion.write("op2", %{
        namespace: "ns",
        status: "cancelled",
        error: "",
        result: "",
        finished_at: 1
      })

    assert Completion.exists?("op2")
    assert :ok = Completion.delete("op2")
    refute Completion.exists?("op2")
  end

  test "path/1 sits at the operative_class level" do
    assert Completion.path("op3") =~ "/op3/completion.record"
  end
end
