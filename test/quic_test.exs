defmodule FlezhaQuicTest do
  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Fixtures.OperativeFixture
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.OperativeTestHelper
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  use ExUnit.Case
  use Flezha.Constant

  @moduletag :quic

  @quic_namespace "quic_test_namespace"

  @test_operative_factory "do_nothing"
  @test_store_input_factory "store_input"

  setup_all do
    # This module's operatives carry FIXED ids, so the Entities they persist
    # outlive the run. On the next run the same identity binds, the bind-time
    # restore correctly rebuilds it from that WAL, and the state assertions
    # below see the PREVIOUS run's inputs on top of their own. Clear this
    # namespace's tree before anything binds. Narrower than
    # `State.TestHelpers.reset_all_entities/0`, which wipes the whole state
    # root — nothing here is live yet, so no WalWriter has to be stopped.
    File.rm_rf(Flezha.State.Registry.namespace_dir(@quic_namespace))

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

    assert OperativeTestHelper.send_sync_io(
             operative_id,
             @quic_namespace,
             "quic_hello",
             %{},
             timeout: :quic
           ) === "quic_hello"
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
      OperativeManager.get_entity_state(@quic_namespace, operative_id)

    entity_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert entity_state === %{
             "store_input_procedure" => %{
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

  # finish-platform-authn-authz §2.1: the server verifies a client cert
  # against the trust anchor, so a peer presenting none is refused at the
  # handshake. The connected runners in `setup_all` are the positive control
  # (same server, same CA, valid client identity) — and they warmed the cargo
  # target dir, so the refused runner below starts without a rebuild.
  test "a QUIC peer without a client certificate is refused" do
    refused_namespace = "quic_refused_namespace"
    Flezha.Service.EventManager.add_handler(:runner_connected_event)

    MessageTestHelper.forward_telemetry([:flezha, :transport, :connect_rejected], :refused)

    {:ok, current_path} = :file.get_cwd()

    command =
      "cargo run --quiet --manifest-path=#{current_path}/test/apps/rust_runner/Cargo.toml " <>
        "--target-dir=./target -- #{refused_namespace}"

    {port, _command} =
      Flezha.Service.PortPilot.open_port(command,
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
      Flezha.Service.EventManager.rem_handler(:runner_connected_event)
      if port_alive?(port), do: Port.close(port)
    end
  end

  defp port_alive?(port), do: Port.info(port) != nil
end
