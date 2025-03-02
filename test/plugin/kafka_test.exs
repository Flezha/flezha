defmodule AflechaTest.Plugin.KafkaTest do
  alias Aflecha.Plugin.WorkerBinding

  use ExUnit.Case
  use Mimic

  @test_topic "aflecha_test_topic"
  @test_broker {"kafka", 9092}
  @test_partition 0

  test "kafka producer bind" do
    KafkaEx
    |> expect(:start_link_worker, fn _x, _y -> {:ok, self()} end)

    settings = %Aflecha.Plugin.Handler.Kafka.Producer{
      hosts: [@test_broker],
      topic: @test_topic,
      partition: @test_partition
    }

    result = WorkerBinding.bind(settings)

    assert elem(result, 0) == :ok
    assert is_struct(elem(result, 1), Aflecha.Plugin.Handler.Kafka.Producer)
    assert is_pid(elem(result, 1).conn)
  end

  test "kafka producer exec" do
    KafkaEx
    |> expect(:produce, fn _, _, _, _ -> {:ok, :success} end)

    settings = %Aflecha.Plugin.Handler.Kafka.Producer{
      hosts: [@test_broker],
      topic: @test_topic,
      partition: @test_partition,
      conn: self()
    }

    result = WorkerBinding.exec(settings, {"test_data", :text})

    assert elem(result, 0) == :ok
    assert elem(result, 1) == :success
  end
end
