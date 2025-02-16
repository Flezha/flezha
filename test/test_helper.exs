ExUnit.start()

defmodule TestHelper do
  use Aflecha.Constant
  use ExUnit.Case

  def test_worker(worker_id) do
    worker =
      AflechaTest.Fixtures.Worker.get(
        worker_id,
        @factory_increment_state
      )
      |> AflechaTest.Fixtures.Worker.start()

    on_exit(fn ->
      AflechaTest.Fixtures.Worker.stop(worker.id)
    end)

    worker
  end
end

# Start the local node
{_, 0} = System.cmd("epmd", ["-daemon"])
Node.start(:"test_agent_node@127.0.0.1", :longnames)
