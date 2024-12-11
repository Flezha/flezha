defmodule AflechaTest.IOManagerTest do
  alias Aflecha.IO.IOManager
  use ExUnit.Case
  use Aflecha.Constant

  import Aflecha.IO.IOManager

  doctest Aflecha.IO.IOManager

  @computation_id "test_io_manager"

  setup do
    computation = TestHelper.test_computation(@computation_id)

    %{computation: computation}
  end

  test "add IO input" do
    res1 =
      IOManager.add_io(
        @default_operator,
        @computation_id,
        %Aflecha.IO.Handler.BeamProcess{output_pid: self()},
        :input,
        []
      )

    res2 =
      IOManager.add_io(
        @default_operator,
        "none",
        %Aflecha.IO.Handler.BeamProcess{output_pid: self()},
        :input,
        []
      )

    assert elem(res1, 0) == :ok
    assert elem(res1, 1) |> is_binary()

    assert elem(res2, 0) == :error
    assert elem(res2, 1) === "computation not found"
  end
end
