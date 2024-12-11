ExUnit.start()

defmodule TestHelper do
  use Aflecha.Constant
  use ExUnit.Case

  def test_computation(computation_id) do
    computation =
      AflechaTest.Fixtures.Computation.get(
        computation_id,
        @factory_increment_state
      )
      |> AflechaTest.Fixtures.Computation.start()

    on_exit(fn ->
      AflechaTest.Fixtures.Computation.stop(computation.id)
    end)

    computation
  end
end
