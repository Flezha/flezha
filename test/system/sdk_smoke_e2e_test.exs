defmodule FlezhaTest.System.SdkSmokeE2ETest do
  @moduledoc """
  The cross-SDK smoke tier: everything an SDK must do before any capability
  suite is worth running, against the runtime `FLEZHA_E2E_RUNTIME` selects and
  every transport its manifest lists.

      FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke

  Handshake and `AssignRunnerId`, `ConnectSocket`, add/remove lifecycle,
  end-to-end data flow, `PipeToOperative` routing, migration, reconnect after a
  disconnect — plus the two assertions that make the manifest binding: the
  factory ids the controller lists for a fresh runner equal the union of the
  manifest's `fixtures`, and each declaration the controller decodes equals its
  committed golden under `test/fixtures/declarations/`.

  Regenerate the goldens with `FLEZHA_E2E_SNAPSHOT=update` under the reference
  runtime (`rust`); any other runtime refuses. See `docs/sdk/conformance.md`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle
  use Mimic
  import FlezhaTest.Helper.TierE2EHarness, only: [unique: 1, unique_id_path: 1]

  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.DeclarationSnapshot
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :sdk_smoke
  @moduletag timeout: 300_000

  # The tier drives only `runner_lifecycle` fixtures, so it runs to completion
  # on an SDK that defers every other capability; each snapshot below carries
  # the capability of the declaration it pins.
  @test_operative_factory "do_nothing"
  @test_filter_factory "compare_filter"

  # A namespace per test: these scenarios start, kill and migrate runners, and
  # a neighbour's teardown must never be able to reach into one of them.
  # A fresh fleet per test: the runner's connect is what most scenarios prove.
  setup do
    namespace = unique("smoke")
    on_exit(fn -> FleetManager.stop_namespace(namespace) end)
    {:ok, namespace: namespace}
  end

  for transport <- NodeTestHelper.transports() do
    describe "transport=#{transport}" do
      @describetag transport: transport

      @tag scenario: "handshake"
      test "handshake + AssignRunnerId succeeds", ctx do
        id = NodeTestHelper.start_runner(ctx.namespace, transport: ctx.transport)

        assert is_binary(id)
        assert String.length(id) > 0
      end

      @tag scenario: "ConnectSocket"
      test "ConnectSocket opens the correct number of stream pairs", ctx do
        id = NodeTestHelper.start_runner(ctx.namespace, transport: ctx.transport)

        assert id in FleetManager.active_runners(ctx.namespace)
      end

      @tag scenario: "live factory set"
      test "the runner advertises exactly the manifest's fixtures", ctx do
        NodeTestHelper.start_runner(ctx.namespace, transport: ctx.transport)

        {:ok, live} = FleetManager.list_factories(ctx.namespace)
        live_ids = live |> Enum.map(& &1.id) |> MapSet.new()

        declared =
          NodeTestHelper.fixtures()
          |> Map.values()
          |> List.flatten()
          |> MapSet.new()

        assert MapSet.difference(declared, live_ids) |> MapSet.to_list() == [],
               "the manifest lists fixtures the runner does not register"

        assert MapSet.difference(live_ids, declared) |> MapSet.to_list() == [],
               "the runner registers fixtures the manifest does not list"
      end

      @tag scenario: "AddOperative + data flow"
      test "AddOperative + send input + observe output", ctx do
        NodeTestHelper.start_runner(ctx.namespace, transport: ctx.transport)

        operative_id = unique("smoke_flow")

        assert {:ok, _info} =
                 OperativeManager.start_operative(ctx.namespace, %Builder{
                   id: operative_id,
                   factory_id: @test_operative_factory,
                   opts: [plugins: [sink(ctx.namespace, operative_id)]]
                 })

        OperativeManager.send_io(operative_id, ctx.namespace, "hello", %{"input_type" => "2"})

        assert_receive {:message_forward, payload}, 30_000
        assert to_string(payload) == "hello"
      end

      @tag scenario: "PipeToOperative local"
      test "PipeToOperative routes locally when target exists", ctx do
        NodeTestHelper.start_runner(ctx.namespace, transport: ctx.transport)

        for factory <- [@test_operative_factory, @test_filter_factory] do
          assert {:ok, _} =
                   OperativeManager.start_operative(ctx.namespace, %Builder{
                     id: unique("pipe_#{factory}"),
                     factory_id: factory
                   })
        end
      end

      # One emit, two destinations, on the shape the one-output-channel-per-
      # procedure model exists for. Registration alone has always passed: a
      # per-edge model declares a second channel for the sink, so the plugin
      # frame names the SINK rather than the producing channel and the durable
      # handoff a workflow journals is the wrong one. The channel name on the
      # frame is what makes that visible from the controller.
      @tag capability: :graph_builder,
           skip: FlezhaTest.SystemCase.deferral(:graph_builder),
           scenario: "hybrid channel"
      test "one emit on a hybrid channel reaches the plugin sink and the consumer", ctx do
        {port, _id} =
          NodeTestHelper.start_runner_port(ctx.namespace,
            transport: ctx.transport,
            port_opts: [:stderr_to_stdout]
          )

        # This is the module's only test that keeps the port, and it stops
        # reading it the moment its line appears — so the runner can be left
        # writing into a pipe nobody drains and miss the namespace teardown.
        # The test that opened the process ends it.
        {:os_pid, os_pid} = :erlang.port_info(port, :os_pid)

        on_exit(fn ->
          NodeTestHelper.ensure_port_dead(port)
          System.cmd("kill", ["-TERM", Integer.to_string(os_pid)], stderr_to_stdout: true)
        end)

        operative_id = unique("smoke_hybrid")

        assert {:ok, _} =
                 OperativeManager.start_operative(ctx.namespace, %Builder{
                   id: operative_id,
                   factory_id: "hybrid_channel",
                   opts: [plugins: [sink(ctx.namespace, operative_id, include_metadata: true)]]
                 })

        OperativeManager.send_io(operative_id, ctx.namespace, "42", %{
          "channel" => "input",
          "input_type" => "2"
        })

        assert_receive {:message_forward, payload, metadata}, 30_000
        assert payload == <<0, 0, 0, 42>>

        assert metadata["channel"] == "hybrid_indicator",
               "the plugin frame names #{inspect(metadata["channel"])}; one output channel per " <>
                 "producing procedure means it carries the producing channel's name, not the sink's"

        said = runner_said(port, "hybrid_consumer_procedure: received 42")

        assert said =~ "hybrid_consumer_procedure: received 42",
               "the inter-procedure consumer never ran, so the emit reached only the sink; " <>
                 "the runner printed #{inspect(said)}"
      end

      @tag scenario: "migration"
      test "an operative migrates between two runners of the same runtime", ctx do
        # Distinct identity files: an SDK that persists its stable id would
        # otherwise hand both processes the same one, and the second connect
        # would retire the first rather than join it.
        [first, second] =
          for _ <- 1..2 do
            NodeTestHelper.start_runner(ctx.namespace,
              transport: ctx.transport,
              env: [{"FLEZHA_RUNNER_ID_PATH", unique_id_path("sdk_smoke_id")}]
            )
          end

        assert first != second
        operative_id = unique("smoke_migrate")

        assert {:ok, info} =
                 OperativeManager.start_operative(ctx.namespace, %Builder{
                   id: operative_id,
                   factory_id: @test_operative_factory
                 })

        target = Enum.find([first, second], &(&1 != info.runner_id))

        # The migrate the loop's `rebind_operative` drives: a HotSwap handover
        # the operative confirms before its binding flips.
        {:ok, pid} = OperativeManager.get_operative_pid(ctx.namespace, operative_id)
        assert :ok = GenServer.call(pid, {:runner_migrate, target}, 60_000)

        assert {:ok, %{runner_id: ^target}} =
                 OperativeManager.get_overwatch_state(ctx.namespace, operative_id)
      end

      # QUIC's controller-side session teardown does not fire
      # `:runner_disconnected_event` inside the helper's window; the failure is
      # identical for every runtime, so it is a transport gap rather than an
      # SDK one.
      if transport != :quic do
        @tag scenario: "unexpected disconnect"
        test "unexpected disconnect triggers auto-reconnect", ctx do
          id1 = NodeTestHelper.start_runner(ctx.namespace, transport: ctx.transport)
          NodeTestHelper.stop_runner(ctx.namespace, id1)
          id2 = NodeTestHelper.start_runner(ctx.namespace, transport: ctx.transport)

          assert id2 != id1
        end

        @tag scenario: "intentional disconnect"
        test "intentional RunnerDisconnect shuts down cleanly", ctx do
          id = NodeTestHelper.start_runner(ctx.namespace, transport: ctx.transport)
          NodeTestHelper.stop_runner(ctx.namespace, id)
        end
      end
    end
  end

  # One fixture per declaration key the controller reads, plus the authoring
  # contract's two §6 anchors, grouped by the capability each declaration
  # belongs to — so a deferring SDK skips them by the same rule as everything
  # else.

  describe "declared surface: keyed_channels" do
    @describetag capability: :keyed_channels,
                 skip: FlezhaTest.SystemCase.deferral(:keyed_channels)

    test "a keyed output carries the identity stamps its golden pins", ctx do
      NodeTestHelper.start_runner(ctx.namespace)
      operative_id = unique("stamps")

      assert {:ok, _} =
               OperativeManager.start_operative(ctx.namespace, %Builder{
                 id: operative_id,
                 factory_id: "partition_relay",
                 opts: [plugins: [sink(ctx.namespace, operative_id, include_metadata: true)]]
               })

      OperativeManager.send_io(operative_id, ctx.namespace, "k1:one", %{
        "channel" => "in",
        "input_type" => "2"
      })

      assert_receive {:message_forward, _payload, metadata}, 30_000

      stamps =
        metadata
        |> Map.keys()
        |> Enum.map(&to_string/1)
        |> Enum.filter(&String.starts_with?(&1, "flz."))
        |> Enum.sort()

      assert stamps == DeclarationSnapshot.golden("keyed_output_stamps", stamps)
    end
  end

  # The runner's stdout until `needle` appears. A hybrid channel's consumer
  # procedure holds no state and answers nothing, so what it prints is its only
  # observable effect — and every SDK's fixture prints the same line.
  defp runner_said(port, needle, budget \\ 30_000) do
    read_port(port, needle, System.monotonic_time(:millisecond) + budget, "")
  end

  defp read_port(port, needle, deadline, seen) do
    remaining = deadline - System.monotonic_time(:millisecond)

    cond do
      String.contains?(seen, needle) ->
        seen

      remaining <= 0 ->
        seen

      true ->
        receive do
          {^port, {:data, data}} -> read_port(port, needle, deadline, seen <> data)
        after
          remaining -> seen
        end
    end
  end

  # Subscribed to every output channel, so it needs no channel name — only an
  # id unique in the namespace the tests share.
  defp sink(namespace, operative_id, opts \\ []) do
    %Flezha.Plugin{
      id: "smoke_sink_#{operative_id}",
      namespace: namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: Keyword.get(opts, :include_metadata, false)},
      subscriptions: [{:on_output, []}]
    }
  end
end
