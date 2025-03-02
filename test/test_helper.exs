ExUnit.start()

Mimic.copy(KafkaEx)

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

  def node_name() do
    :"test_agent_node1@127.0.0.1"
  end
end
