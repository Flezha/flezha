defmodule FlechaTest do
  use ExUnit.Case
  doctest Flecha

  test "add computation" do
    # TODO: should send a message that computation template has been registered.
    Process.sleep(1000)

    computation = %Flecha.Computation{
      computation_id: "add_compuation_test",
      computation_name: "do_nothing"
    }

    {:ok, {"add_compuation_test", [{node, pid}]}} = Flecha.add_computation(computation)

    assert is_pid(pid)
    assert node == Node.self()
  end
end
