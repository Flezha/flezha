defmodule FlechaCoreTest do
  use ExUnit.Case
  doctest FlechaCore

  test "greets the world" do
    assert FlechaCore.hello() == :world
  end
end
