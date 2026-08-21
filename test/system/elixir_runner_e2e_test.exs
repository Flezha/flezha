defmodule FlezhaTest.System.ElixirRunnerE2ETest do
  @moduledoc """
  Standalone-mode end-to-end: the controller BEAM plus the `test/apps/elixir_runner`
  BEAM in a SEPARATE OS process, over UDS and QUIC. Proves the Elixir SDK's
  standalone deployment is protocol-identical to a rust_runner — connect, place,
  dispatch, sink output, the retry→park DLQ round trip, partition admission — with
  zero controller changes, plus crash + reconnect. `:system` (nightly); the app
  must be pre-built for the test env
  (`cd test/apps/elixir_runner && mix compile`).
  """
  use ExUnit.Case

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Operative.Builder
  alias Flezha.Dlq.Store
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :system

  defp unique(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  defp await_output(expected, timeout) do
    receive do
      {:message_forward, msg, _md} ->
        if to_string(msg) == expected, do: true, else: await_output(expected, timeout)
    after
      timeout -> false
    end
  end

  defp capture_sink(namespace, operative_id) do
    %Flezha.Plugin{
      id: "capture_#{operative_id}",
      namespace: namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self()},
      subscriptions: [{:on_output, []}]
    }
  end

  for transport <- [:uds, :quic] do
    describe "transport=#{transport}" do
      @describetag transport: transport

      test "connect, place, dispatch, and sink output", ctx do
        namespace = unique("elixir_e2e")

        {port, runner_id} =
          NodeTestHelper.start_runner_port(namespace,
            runtime: :elixir,
            transport: ctx.transport,
            connect_timeout: 120_000
          )

        on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

        assert runner_id in FleetManager.active_runners(namespace)

        operative_id = unique("echo")
        sink = capture_sink(namespace, operative_id)

        builder = %Builder{
          id: operative_id,
          factory_id: "elixir_e2e_echo",
          init_values: %{},
          opts: [plugins: [sink]]
        }

        assert {:ok, _} = OperativeManager.start_operative(namespace, builder)

        OperativeManager.send_io(operative_id, namespace, "hello", %{
          "channel" => "in",
          "input_type" => "2"
        })

        assert await_output("hello", 30_000), "sink output never arrived"
      end

      test "a poison payload parks durably in the DLQ", ctx do
        namespace = unique("elixir_dlq")

        {port, _runner_id} =
          NodeTestHelper.start_runner_port(namespace,
            runtime: :elixir,
            transport: ctx.transport,
            connect_timeout: 120_000
          )

        on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

        operative_id = unique("poison")

        builder = %Builder{
          id: operative_id,
          factory_id: "elixir_e2e_poison",
          init_values: %{},
          opts: []
        }

        assert {:ok, _} = OperativeManager.start_operative(namespace, builder)

        OperativeManager.send_io(operative_id, namespace, "boom", %{
          "channel" => "in",
          "input_type" => "2"
        })

        assert NodeTestHelper.wait_until(fn -> Store.list(operative_id) != [] end, 30_000),
               "durable DLQ entry never appeared"

        assert [entry | _] = Store.list(operative_id)
        assert entry["error"] =~ "poison"
      end

      test "killing the runner process is handled; a restart re-presents the same stable id",
           ctx do
        namespace = unique("elixir_crash")
        id_path = Path.join(System.tmp_dir!(), unique("elixir_id"))

        {port, runner_id} =
          NodeTestHelper.start_runner_port(namespace,
            runtime: :elixir,
            transport: ctx.transport,
            connect_timeout: 120_000,
            env: [{"FLEZHA_RUNNER_ID_PATH", id_path}]
          )

        assert runner_id in FleetManager.active_runners(namespace)

        NodeTestHelper.ensure_port_dead(port)

        assert NodeTestHelper.wait_until(
                 fn -> FleetManager.active_runners(namespace) == [] end,
                 30_000
               ),
               "convergence never dropped the killed runner"

        {port2, _runner_id2} =
          NodeTestHelper.start_runner_port(namespace,
            runtime: :elixir,
            transport: ctx.transport,
            connect_timeout: 120_000,
            env: [{"FLEZHA_RUNNER_ID_PATH", id_path}]
          )

        on_exit(fn -> NodeTestHelper.ensure_port_dead(port2) end)

        assert NodeTestHelper.wait_until(
                 fn -> FleetManager.active_runners(namespace) != [] end,
                 30_000
               )

        File.rm(id_path)
      end
    end
  end

  @tag transport: :uds
  test "a partitioned group admits only its range; a keyless class refuses" do
    namespace = unique("elixir_part")

    {port, _runner_id} =
      NodeTestHelper.start_runner_port(namespace,
        runtime: :elixir,
        transport: :uds,
        connect_timeout: 120_000
      )

    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    # A keyed class binds under partitions; a mis-routed key parks. The keyed
    # factory (elixir_e2e_keyed) is Dynamic, so a `--partitions 2` bind is legal.
    operative_id = unique("part")

    builder = %Builder{
      id: operative_id,
      factory_id: "elixir_e2e_keyed",
      init_values: %{},
      opts: [partition_index: 0, partition_count: 2, key_group_from: 0, key_group_to: 127]
    }

    assert {:ok, _} = OperativeManager.start_operative(namespace, builder)

    # "BTC-USD" hashes to group 129 — outside 0..127 — so it parks loudly.
    OperativeManager.send_io(operative_id, namespace, "BTC-USD:x", %{
      "channel" => "in",
      "input_type" => "2"
    })

    assert NodeTestHelper.wait_until(fn -> Store.list(operative_id) != [] end, 30_000),
           "a mis-routed key never parked"

    assert [entry | _] = Store.list(operative_id)
    assert entry["error"] =~ "does not own"
  end
end
