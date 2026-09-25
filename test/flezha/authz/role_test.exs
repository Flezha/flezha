defmodule Flezha.AuthZ.RoleTest do
  use ExUnit.Case, async: true

  alias Flezha.AuthZ.Role

  test "all/0 returns the seven v1 roles" do
    assert Enum.sort(Role.all()) ==
             [:admin, :app, :ops, :runner, :spawner, :stopper, :viewer]
  end

  test "known?/1 accepts documented roles" do
    for r <- [:viewer, :app, :ops, :admin, :runner, :spawner, :stopper],
        do: assert(Role.known?(r))
  end

  test "validate/1 returns :ok / {:error, :unknown_role}" do
    assert :ok = Role.validate(:ops)
    assert {:error, :unknown_role} = Role.validate(:superuser)
  end
end
