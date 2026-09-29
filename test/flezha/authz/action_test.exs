defmodule Flezha.AuthZ.ActionTest do
  use ExUnit.Case, async: true

  alias Flezha.AuthZ.Action

  test "all/0 returns a non-empty enumeration of atoms" do
    actions = Action.all()
    assert is_list(actions)
    refute Enum.empty?(actions)
    assert Enum.all?(actions, &is_atom/1)
  end

  test "known?/1 accepts documented actions" do
    for action <- [
          :"operator.start",
          :"operator.listen",
          :"plugin.enable",
          :"audit.read",
          :"runner.connect"
        ] do
      assert Action.known?(action), "expected #{inspect(action)} to be known"
    end
  end

  test "known?/1 rejects unknown atoms" do
    refute Action.known?(:foo)
    refute Action.known?(:"some.other")
  end
end
