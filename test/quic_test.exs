defmodule FlezhaQuicTest do
  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Fixtures.OperativeFixture
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.OperativeTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  use ExUnit.Case
  use Flezha.Constant

  @moduletag :quic

  @quic_namespace "quic_test_namespace"

  @test_operative_factory "do_nothing"
  @test_store_input_factory "store_input"

  setup_all do
    # Start 2 compute engines connected via QUIC for this test module.
    ce_id_1 = NodeTestHelper.start_runner(@quic_namespace, transport: :quic)
    ce_id_2 = NodeTestHelper.start_runner(@quic_namespace, transport: :quic)

    on_exit(fn ->
      FleetManager.stop_namespace(@quic_namespace)
    end)

    %{runner_ids: [ce_id_1, ce_id_2]}
  end

  setup _context do
    on_exit(fn ->
      OperativeManager.stop_all_operatives(@quic_namespace)
    end)

    :ok
  end

  test "compute engines connected via QUIC" do
    res = FleetManager.active_runners(@quic_namespace)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  test "operative creation over QUIC" do
    operative = %Flezha.Operative.Builder{
      id: "quic_operative_test",
      factory_id: @test_operative_factory
    }

    {status, info} = OperativeManager.start_operative(@quic_namespace, operative)

    assert status === :ok,
           "start_operative failed: #{inspect(info)}"

    assert info[:operative_id] === "quic_operative_test"
    assert is_pid(info[:pid])
    assert is_binary(info[:runner_id])
  end

  test "end-to-end message flow over QUIC" do
    operative_id = "quic_msg_flow_test"

    {:ok, _info} =
      operative_id
      |> OperativeFixture.get(@test_operative_factory)
      |> OperativeFixture.start(@quic_namespace)
      |> OperativeFixture.attach_output_msg(@quic_namespace)

    OperativeManager.send_io(operative_id, @quic_namespace, "quic_hello", %{})

    receive do
      {:message_forward, msg} ->
        assert msg === "quic_hello"
    after
      2000 -> flunk("Timeout waiting for message forward over QUIC")
    end
  end

  test "operative state retrieval over QUIC" do
    operative_id = "quic_state_test"

    {:ok, _info} =
      operative_id
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start(@quic_namespace)
      |> OperativeFixture.attach_output_msg(@quic_namespace)

    OperativeTestHelper.send_sync_io(operative_id, @quic_namespace, "quic_state_1", %{},
      timeout: :quic
    )

    OperativeTestHelper.send_sync_io(operative_id, @quic_namespace, "quic_state_2", %{},
      timeout: :quic
    )

    {:ok, json_state} =
      OperativeManager.get_segment_state(@quic_namespace, operative_id)

    segment_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert segment_state === %{
             "store_input_operation" => %{
               "binary_inputs" => [],
               "text_inputs" => ["quic_state_1", "quic_state_2"]
             }
           }
  end

  test "sync IO round-trip over QUIC" do
    operative_id = "quic_sync_io_test"

    {:ok, _info} =
      operative_id
      |> OperativeFixture.get(@test_operative_factory)
      |> OperativeFixture.start(@quic_namespace)
      |> OperativeFixture.attach_output_msg(@quic_namespace)

    result =
      OperativeTestHelper.send_sync_io(operative_id, @quic_namespace, "quic_echo", %{},
        timeout: :quic
      )

    assert result === "quic_echo"
  end
end
