defmodule FlezhaQuicTest do
  alias Flezha.Payloader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.EventManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Paths
  alias FlezhaTest.Fixtures.OperatorFixture
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.OperatorTestHelper

  use ExUnit.Case
  use Flezha.Constant

  @moduletag :quic

  @quic_namespace "quic_test_namespace"

  @test_operator_factory "do_nothing"
  @test_store_input_factory "store_input"

  setup_all do
    # This module's operators carry FIXED ids, so the Cells they persist
    # outlive the run. On the next run the same identity binds, the bind-time
    # restore correctly rebuilds it from that WAL, and the state assertions
    # below see the PREVIOUS run's inputs on top of their own. Clear this
    # namespace's tree before anything binds. Narrower than
    # `State.TestHelpers.reset_all_cells/0`, which wipes the whole state
    # root — nothing here is live yet, so no WalWriter has to be stopped.
    File.rm_rf(Paths.namespace_dir(@quic_namespace))

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
      OperatorManager.stop_all_operators(@quic_namespace)
    end)

    :ok
  end

  test "compute engines connected via QUIC" do
    res = FleetManager.active_runners(@quic_namespace)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  test "operator creation over QUIC" do
    operator = %Flezha.Operator.Builder{
      id: "quic_operator_test",
      factory_id: @test_operator_factory
    }

    {status, info} = OperatorManager.start_operator(@quic_namespace, operator)

    assert status === :ok,
           "start_operator failed: #{inspect(info)}"

    assert info[:operator_id] === "quic_operator_test"
    assert is_pid(info[:pid])
    assert is_binary(info[:runner_id])
  end

  test "end-to-end message flow over QUIC" do
    operator_id = "quic_msg_flow_test"

    {:ok, _info} =
      operator_id
      |> OperatorFixture.get(@test_operator_factory)
      |> OperatorFixture.start(@quic_namespace)
      |> OperatorFixture.attach_output_msg(@quic_namespace)

    assert OperatorTestHelper.send_sync_io(
             operator_id,
             @quic_namespace,
             "quic_hello",
             %{},
             timeout: :quic
           ) === "quic_hello"
  end

  test "operator state retrieval over QUIC" do
    operator_id = "quic_state_test"

    {:ok, _info} =
      operator_id
      |> OperatorFixture.get(@test_store_input_factory)
      |> OperatorFixture.start(@quic_namespace)
      |> OperatorFixture.attach_output_msg(@quic_namespace)

    OperatorTestHelper.send_sync_io(operator_id, @quic_namespace, "quic_state_1", %{},
      timeout: :quic
    )

    OperatorTestHelper.send_sync_io(operator_id, @quic_namespace, "quic_state_2", %{},
      timeout: :quic
    )

    {:ok, json_state} =
      OperatorManager.get_cell_state(@quic_namespace, operator_id)

    cell_state = Payloader.json_to_map(json_state)

    assert cell_state === %{
             "store_input_tasklet" => %{
               "binary_inputs" => [],
               "text_inputs" => ["quic_state_1", "quic_state_2"]
             }
           }
  end

  test "sync IO round-trip over QUIC" do
    operator_id = "quic_sync_io_test"

    {:ok, _info} =
      operator_id
      |> OperatorFixture.get(@test_operator_factory)
      |> OperatorFixture.start(@quic_namespace)
      |> OperatorFixture.attach_output_msg(@quic_namespace)

    result =
      OperatorTestHelper.send_sync_io(operator_id, @quic_namespace, "quic_echo", %{},
        timeout: :quic
      )

    assert result === "quic_echo"
  end

  # finish-platform-authn-authz §2.1: the server verifies a client cert
  # against the trust anchor, so a peer presenting none is refused at the
  # handshake. The connected runners in `setup_all` are the positive control
  # (same server, same CA, valid client identity) — and they warmed the cargo
  # target dir, so the refused runner below starts without a rebuild.
  test "a QUIC peer without a client certificate is refused" do
    refused_namespace = "quic_refused_namespace"
    EventManager.add_handler(:runner_connected_event)

    MessageTestHelper.forward_telemetry([:flezha, :transport, :connect_rejected], :refused)

    {:ok, current_path} = :file.get_cwd()

    command =
      "cargo run --quiet --manifest-path=#{current_path}/sdk/rust/test_app/Cargo.toml " <>
        "--target-dir=./target -- #{refused_namespace}"

    {port, _command} =
      NodeTestHelper.open_runner_port(command,
        transport: :quic,
        env: [
          {"FLEZHA_QUIC_CLIENT_CERT", "/nonexistent/client.pem"},
          {"FLEZHA_QUIC_CLIENT_KEY", "/nonexistent/client.key"}
        ]
      )

    try do
      # The refusal itself is the assertion: the server reports every
      # handshake-stage rejection on `connect_rejected`, so this waits for the
      # refusal to happen rather than waiting out a window to prove it did not
      # connect. The runner retries after a refusal, so it is still running here.
      assert_receive {:refused, %{count: 1}, %{stage: :handshake}}, 15_000
      refute_receive {:runner_connected_event, {^refused_namespace, _}}, 100
      assert FleetManager.active_runners(refused_namespace) == []
    after
      EventManager.rem_handler(:runner_connected_event)
      if port_alive?(port), do: Port.close(port)
    end
  end

  defp port_alive?(port), do: Port.info(port) != nil
end
