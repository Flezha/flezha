defmodule FlezhaTest.Plugin.KafkaTest do
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Kafka

  use ExUnit.Case, async: false
  use Mimic

  @test_topic "flezha_test_topic"
  @test_broker {"kafka", 9092}
  @test_partition 0

  test "kafka producer init_provider starts worker" do
    KafkaEx
    |> expect(:start_link_worker, fn _x, _y -> {:ok, self()} end)

    assert {:ok, opts, pid} =
             Kafka.Producer.init_provider(%{
               hosts: [@test_broker],
               topic: @test_topic,
               partition: @test_partition
             })

    assert is_pid(pid)
    assert Map.get(opts, :conn) == pid
  end

  test "kafka producer on_input produces" do
    KafkaEx
    |> expect(:produce, fn _, _ -> {:ok, :success} end)

    opts = %{
      hosts: [@test_broker],
      topic: @test_topic,
      partition: @test_partition,
      conn: self()
    }

    ctx = %Ctx.IO{processor_id: "p", namespace: "n", message: "test_data", metadata: %{}}

    assert {:ok, ^ctx} = Kafka.Producer.on_input(ctx, opts)
  end
end
