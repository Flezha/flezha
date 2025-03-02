defmodule AflechaTest.Plugin.KafkaTest do
  alias Aflecha.Plugin.WorkerBinding

  use ExUnit.Case

  test "kafka producer bind" do
    settings = %Aflecha.Plugin.Handler.Kafka.Producer{
      hosts: [{"kafka", 9092}],
      topic: "aflecha_test_topic",
      partition: 0
    }

    result = WorkerBinding.bind(settings)

    assert elem(result, 0) == :ok
    assert is_struct(elem(result, 1), Aflecha.Plugin.Handler.Kafka.Producer)
    assert is_pid(elem(result, 1).conn)
  end
end
