defmodule FlezhaTest.Plugin.KafkaTest do
  alias Flezha.Computation.IOBinding

  use ExUnit.Case
  use Mimic

  @test_topic "flezha_test_topic"
  @test_broker {"kafka", 9092}
  @test_partition 0

  test "kafka producer bind" do
    KafkaEx
    |> expect(:start_link_worker, fn _x, _y -> {:ok, self()} end)

    provider = %Flezha.Plugin.Provider.Kafka.Producer{
      hosts: [@test_broker],
      topic: @test_topic,
      partition: @test_partition
    }

    result = IOBinding.bind(provider)

    assert elem(result, 0) == :ok
    assert is_struct(elem(result, 1), Flezha.Plugin.Provider.Kafka.Producer)
    assert is_pid(elem(result, 1).conn)
  end

  test "kafka producer exec" do
    KafkaEx
    |> expect(:produce, fn _, _ -> {:ok, :success} end)

    provider = %Flezha.Plugin.Provider.Kafka.Producer{
      hosts: [@test_broker],
      topic: @test_topic,
      partition: @test_partition,
      conn: self()
    }

    result = IOBinding.exec(provider, {"test_data", %{}})

    assert elem(result, 0) == :ok
    assert elem(result, 1) == {"test_data", %{}}
  end
end
