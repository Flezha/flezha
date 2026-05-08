defmodule FlezhaQuicTest do
  alias Flezha.Executor.FleetManager
  alias FlezhaTest.Fixtures.RelayFixture
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.RelayTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  use ExUnit.Case
  use Flezha.Constant

  @moduletag :quic

  @quic_namespace "quic_test_namespace"

  @test_relay_factory "do_nothing"
  @test_store_input_factory "store_input"

  setup_all do
    # Start 2 compute engines connected via QUIC for this test module.
    ce_id_1 = NodeTestHelper.start_executor(@quic_namespace, transport: :quic)
    ce_id_2 = NodeTestHelper.start_executor(@quic_namespace, transport: :quic)

    on_exit(fn ->
      FleetManager.stop_namespace(@quic_namespace)
    end)

    %{executor_ids: [ce_id_1, ce_id_2]}
  end

  setup _context do
    on_exit(fn ->
      RelayManager.stop_all_relays(@quic_namespace)
    end)

    :ok
  end

  test "compute engines connected via QUIC" do
    res = FleetManager.active_executors(@quic_namespace)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  test "relay creation over QUIC" do
    relay = %Flezha.Relay.Builder{
      id: "quic_relay_test",
      factory_id: @test_relay_factory
    }

    {status, info} = RelayManager.start_relay(@quic_namespace, relay)

    assert status === :ok,
           "start_relay failed: #{inspect(info)}"

    assert info[:relay_id] === "quic_relay_test"
    assert is_pid(info[:pid])
    assert is_binary(info[:executor_id])
  end

  test "end-to-end message flow over QUIC" do
    relay_id = "quic_msg_flow_test"

    {:ok, _info} =
      relay_id
      |> RelayFixture.get(@test_relay_factory)
      |> RelayFixture.start(@quic_namespace)
      |> RelayFixture.attach_output_msg(@quic_namespace)

    RelayManager.send_io(relay_id, @quic_namespace, "quic_hello", %{})

    receive do
      {:message_forward, msg} ->
        assert msg === "quic_hello"
    after
      2000 -> flunk("Timeout waiting for message forward over QUIC")
    end
  end

  test "relay state retrieval over QUIC" do
    relay_id = "quic_state_test"

    {:ok, _info} =
      relay_id
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start(@quic_namespace)
      |> RelayFixture.attach_output_msg(@quic_namespace)

    RelayTestHelper.send_sync_io(relay_id, @quic_namespace, "quic_state_1", %{})
    RelayTestHelper.send_sync_io(relay_id, @quic_namespace, "quic_state_2", %{})

    {:ok, json_state} =
      RelayManager.get_partition_state(@quic_namespace, relay_id)

    partition_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert partition_state === %{
             "binary_inputs" => [],
             "text_inputs" => ["quic_state_1", "quic_state_2"]
           }
  end

  test "sync IO round-trip over QUIC" do
    relay_id = "quic_sync_io_test"

    {:ok, _info} =
      relay_id
      |> RelayFixture.get(@test_relay_factory)
      |> RelayFixture.start(@quic_namespace)
      |> RelayFixture.attach_output_msg(@quic_namespace)

    result = RelayTestHelper.send_sync_io(relay_id, @quic_namespace, "quic_echo", %{})

    assert result === "quic_echo"
  end
end
