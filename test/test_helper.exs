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

# Start the local node
{_, 0} = System.cmd("epmd", ["-daemon"])
Node.start(:"test_agent_node@127.0.0.1", :longnames)
