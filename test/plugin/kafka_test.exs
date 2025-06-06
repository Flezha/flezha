defmodule FlezhaTest.Plugin.KafkaTest do
  alias Flezha.Plugin.PipelineBinding

  use ExUnit.Case
  use Mimic

  @test_topic "flezha_test_topic"
  @test_broker {"kafka", 9092}
  @test_partition 0

  test "kafka producer bind" do
    KafkaEx
    |> expect(:start_link_worker, fn _x, _y -> {:ok, self()} end)

    settings = %Flezha.Plugin.Provider.Kafka.Producer{
      group: "test_group",
      pipeline_id: "test_pipeline",
      trigger: nil,
      hosts: [@test_broker],
      topic: @test_topic,
      partition: @test_partition
    }

    result = PipelineBinding.bind(settings)

    assert elem(result, 0) == :ok
    assert is_struct(elem(result, 1), Flezha.Plugin.Provider.Kafka.Producer)
    assert is_pid(elem(result, 1).conn)
  end

  test "kafka producer exec" do
    KafkaEx
    |> expect(:produce, fn _, _, _, _ -> {:ok, :success} end)

    settings = %Flezha.Plugin.Provider.Kafka.Producer{
      group: "test_group",
      pipeline_id: "test_pipeline",
      trigger: nil,
      hosts: [@test_broker],
      topic: @test_topic,
      partition: @test_partition,
      conn: self()
    }

    result = PipelineBinding.exec(settings, {:text, "test_data"})

    assert elem(result, 0) == :ok
    assert elem(result, 1) == {:text, "test_data"}
  end
end
