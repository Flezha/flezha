ExUnit.start()

Mimic.copy(KafkaEx)

defmodule TestHelper do
  use Aflecha.Constant
  use ExUnit.Case

  def test_pipeline(pipeline_id) do
    pipeline =
      AflechaTest.Fixtures.PipelineFixture.get(
        pipeline_id,
        @factory_increment_state
      )
      |> AflechaTest.Fixtures.PipelineFixture.start()

    on_exit(fn ->
      AflechaTest.Fixtures.PipelineFixture.stop(pipeline.id)
    end)

    pipeline
  end

  def node_name() do
    :"test_agent_node1@127.0.0.1"
  end
end
