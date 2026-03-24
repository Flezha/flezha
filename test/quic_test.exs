defmodule FlezhaQuicTest do
  alias Flezha.ComputeEngine.FleetManager
  alias FlezhaTest.Fixtures.ProcessorFixture
  alias Flezha.Service.ProcessorManager
  alias FlezhaTest.Helper.ProcessorTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  use ExUnit.Case
  use Flezha.Constant

  @moduletag :quic

  @quic_namespace "quic_test_namespace"

  @test_processor_factory "do_nothing"
  @test_store_input_factory "store_input"

  setup_all do
    # Start 2 compute engines connected via QUIC for this test module.
    ce_id_1 = NodeTestHelper.start_compute_engine(@quic_namespace, transport: :quic)
    ce_id_2 = NodeTestHelper.start_compute_engine(@quic_namespace, transport: :quic)

    on_exit(fn ->
      FleetManager.stop_namespace(@quic_namespace)
    end)

    %{compute_engine_ids: [ce_id_1, ce_id_2]}
  end

  setup _context do
    on_exit(fn ->
      ProcessorManager.stop_all_processors(@quic_namespace)
    end)

    :ok
  end

  test "compute engines connected via QUIC" do
    res = FleetManager.active_compute_engines(@quic_namespace)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  test "processor creation over QUIC" do
    processor = %Flezha.Processor.Builder{
      id: "quic_processor_test",
      factory_id: @test_processor_factory
    }

    {status, info} = ProcessorManager.start_processor(@quic_namespace, processor)

    assert status === :ok,
           "start_processor failed: #{inspect(info)}"

    assert info[:processor_id] === "quic_processor_test"
    assert is_pid(info[:pid])
    assert is_binary(info[:compute_engine_id])
  end

  test "end-to-end message flow over QUIC" do
    processor_id = "quic_msg_flow_test"

    {:ok, _info} =
      processor_id
      |> ProcessorFixture.get(@test_processor_factory)
      |> ProcessorFixture.start(@quic_namespace)
      |> ProcessorFixture.attach_output_msg(@quic_namespace)

    ProcessorManager.send_io(processor_id, @quic_namespace, "quic_hello", %{})

    receive do
      {:message_forward, msg} ->
        assert msg === "quic_hello"
    after
      2000 -> flunk("Timeout waiting for message forward over QUIC")
    end
  end

  test "processor state retrieval over QUIC" do
    processor_id = "quic_state_test"

    {:ok, _info} =
      processor_id
      |> ProcessorFixture.get(@test_store_input_factory)
      |> ProcessorFixture.start(@quic_namespace)
      |> ProcessorFixture.attach_output_msg(@quic_namespace)

    ProcessorTestHelper.send_sync_io(processor_id, @quic_namespace, "quic_state_1", %{})
    ProcessorTestHelper.send_sync_io(processor_id, @quic_namespace, "quic_state_2", %{})

    {:ok, json_state} =
      ProcessorManager.get_processor_state(@quic_namespace, processor_id)

    processor_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert processor_state === %{
             "binary_inputs" => [],
             "text_inputs" => ["quic_state_1", "quic_state_2"]
           }
  end

  test "sync IO round-trip over QUIC" do
    processor_id = "quic_sync_io_test"

    {:ok, _info} =
      processor_id
      |> ProcessorFixture.get(@test_processor_factory)
      |> ProcessorFixture.start(@quic_namespace)
      |> ProcessorFixture.attach_output_msg(@quic_namespace)

    result = ProcessorTestHelper.send_sync_io(processor_id, @quic_namespace, "quic_echo", %{})

    assert result === "quic_echo"
  end
end
