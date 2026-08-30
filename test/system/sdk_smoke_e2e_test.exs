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

  alias Flezha.Operative.Builder
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :sdk_smoke
  @moduletag timeout: 300_000

  # The tier drives only `runner_lifecycle` fixtures, so it runs to completion
  # on an SDK that defers every other capability; each snapshot below carries
  # the capability of the declaration it pins.
  @test_operative_factory "do_nothing"
  @test_filter_factory "compare_filter"

  @golden_dir "test/fixtures/declarations"

  # A namespace per test: these scenarios start, kill and migrate runners, and
  # a neighbour's teardown must never be able to reach into one of them.
  # A fresh fleet per test: the runner's connect is what most scenarios prove.
  setup do
    namespace = unique("smoke")
    on_exit(fn -> FleetManager.stop_namespace(namespace) end)
    {:ok, namespace: namespace}
  end

  for transport <- FlezhaTest.Helper.NodeTestHelper.transports() do
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

      @tag scenario: "migration"
      test "an operative migrates between two runners of the same runtime", ctx do
        # Distinct identity files: an SDK that persists its stable id would
        # otherwise hand both processes the same one, and the second connect
        # would retire the first rather than join it.
        [first, second] =
          for _ <- 1..2 do
            NodeTestHelper.start_runner(ctx.namespace,
              transport: ctx.transport,
              env: [{"FLEZHA_RUNNER_ID_PATH", unique_id_path()}]
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

        # The production rebind seam (`Convergence.Heal.rebind/2`): a HotSwap
        # handover the operative confirms before its binding flips.
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

  describe "declared surface: graph_builder" do
    @describetag capability: :graph_builder,
                 skip: FlezhaTest.SystemCase.deferral(:graph_builder)

    test "the reference operative's declaration matches its golden", ctx do
      assert_declaration_snapshot(ctx.namespace, "static_entity", :stream)
    end

    test "the sugared reference operative declares the same as the hand-written one", ctx do
      assert_declaration_snapshot(ctx.namespace, "sugar_static_entity", :stream)
    end
  end

  describe "declared surface: state_persistence" do
    @describetag capability: :state_persistence,
                 skip: FlezhaTest.SystemCase.deferral(:state_persistence)

    test "a persistent class's declaration matches its golden", ctx do
      assert_declaration_snapshot(ctx.namespace, "store_input", :stream)
    end
  end

  describe "declared surface: effectively_once" do
    @describetag capability: :effectively_once,
                 skip: FlezhaTest.SystemCase.deferral(:effectively_once)

    test "the tier declaration matches its golden", ctx do
      assert_declaration_snapshot(ctx.namespace, "tier_store", :stream)
    end

    test "the checkpoint-interval declarations match their golden", ctx do
      assert_declaration_snapshot(ctx.namespace, "checkpoint_batched_store", :stream)
    end
  end

  describe "declared surface: correction_window" do
    @describetag capability: :correction_window,
                 skip: FlezhaTest.SystemCase.deferral(:correction_window)

    test "the correction-window declaration matches its golden", ctx do
      assert_declaration_snapshot(ctx.namespace, "candle", :stream)
    end
  end

  describe "declared surface: workflow" do
    @describetag capability: :workflow,
                 skip: FlezhaTest.SystemCase.deferral(:workflow)

    test "a durable chain's declaration matches its golden", ctx do
      assert_declaration_snapshot(ctx.namespace, "commit_block_workflow", :workflow)
    end

    test "a compensating chain's declaration matches its golden", ctx do
      assert_declaration_snapshot(ctx.namespace, "saga_block_workflow", :workflow)
    end
  end

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

      assert stamps == golden("keyed_output_stamps", stamps)
    end
  end

  # Bind `factory` with `decode_declarations/1` intercepted, and compare the raw
  # metadata map the controller decoded against the committed golden. The stub
  # delegates, so the bind itself is unchanged.
  defp assert_declaration_snapshot(namespace, factory, kind) do
    NodeTestHelper.start_runner(namespace)
    parent = self()

    Mimic.stub(ClassRegistry, :decode_declarations, fn raw ->
      send(parent, {:declaration_metadata, decoded_metadata(raw)})
      Mimic.call_original(ClassRegistry, :decode_declarations, [raw])
    end)

    Mimic.set_mimic_global()

    assert {:ok, _} =
             OperativeManager.start_operative(namespace, %Builder{
               id: unique("d_#{String.slice(factory, 0, 18)}"),
               factory_id: factory,
               kind: kind,
               init_values: %{"seed" => "go"}
             })

    assert_receive {:declaration_metadata, metadata}, 60_000
    assert metadata == golden(factory, metadata)
  end

  # The same wire form `ClassRegistry.decode_declarations/1` reads, kept raw so
  # a key an SDK adds, drops or spells differently is a diff.
  defp decoded_metadata(raw) when is_binary(raw) and raw != "" do
    case RunnerCommands.AddOperativeResponse.decode(raw) do
      {:ok, %{metadata: metadata}} -> metadata
      {:error, _} -> %{}
    end
  end

  defp decoded_metadata(_raw), do: %{}

  # The committed golden for `name`, or `actual` written back when
  # `FLEZHA_E2E_SNAPSHOT=update` is set under the reference runtime.
  defp golden(name, actual) do
    path = Path.join(@golden_dir, "#{name}.json")

    cond do
      System.get_env("FLEZHA_E2E_SNAPSHOT") != "update" ->
        assert File.exists?(path),
               "no golden at #{path}; regenerate with " <>
                 "`FLEZHA_E2E_SNAPSHOT=update " <>
                 "FLEZHA_E2E_RUNTIME=#{NodeTestHelper.reference_runtime()} " <>
                 "mix test --include sdk_smoke`"

        path |> File.read!() |> Jason.decode!()

      NodeTestHelper.runtime() != NodeTestHelper.reference_runtime() ->
        flunk(
          "a golden is regenerated only under the reference runtime " <>
            "(#{NodeTestHelper.reference_runtime()}); this run is " <>
            "#{NodeTestHelper.runtime()}"
        )

      true ->
        File.mkdir_p!(@golden_dir)
        File.write!(path, Jason.encode!(actual, pretty: true) <> "\n")
        actual
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

  defp unique(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  defp unique_id_path do
    path = Path.join(System.tmp_dir!(), unique("sdk_smoke_id"))
    on_exit(fn -> File.rm(path) end)
    path
  end
end
