defmodule Flezha.SDK.InNodeE2ETest do
  @moduledoc """
  In-node end-to-end: the Elixir SDK boots over the native transport against the
  same-BEAM controller, FleetManager lists it, an operative is placed, and an
  io_in frame drives `run/3` (observed directly, since the Cell runs in this
  BEAM). Exercises the full path — handshake, AssignRunnerId, ConnectSocket,
  AddOperative, primary-stream io_in routing — with no external process.
  """
  # async: false: the live controller and its fleet; registered singletons.
  use ExUnit.Case, async: false

  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.Ctx
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.NodeTestHelper

  @probe :sdk_in_node_probe

  defmodule ProbeTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "probe"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      case Process.whereis(:sdk_in_node_probe) do
        nil -> :ok
        pid -> send(pid, {:sdk_probe_ran, input})
      end

      Ctx.emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule ProbeOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "sdk_probe"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(ProbeTask) |> sink("probe")
    end
  end

  setup do
    ns = "sdk_in_node_#{System.unique_integer([:positive])}"
    Process.register(self(), @probe)

    handle =
      Flezha.SDK.create_runner(ns, transport: :native)
      |> Flezha.SDK.register_factory(ProbeOp)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{} = handle, "SDK start failed: #{inspect(handle)}"

    on_exit(fn ->
      Flezha.SDK.close(handle)
      if Process.whereis(@probe), do: Process.unregister(@probe)

      try do
        FleetManager.stop_namespace(ns)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, ns: ns, handle: handle}
  end

  test "the native runner connects and is listed with its factory", %{ns: ns} do
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000)
    assert [_runner_id] = FleetManager.active_runners(ns)
  end

  # The reference answers a non-terminal `RunnerDisconnect` with `Stopped` and
  # stays put; a runner that exits instead is restarted by its `:permanent`
  # supervisor and registers again, into a session the controller is tearing
  # down.
  test "a non-terminal RunnerDisconnect stops the runner in place: same process, no second registration",
       %{ns: ns, handle: handle} do
    :ok = Flezha.SDK.close(handle)
    kept = "#{ns}_kept"
    on_exit(fn -> FleetManager.stop_namespace(kept) end)

    pid =
      start_supervised!(
        Flezha.SDK.child_spec(namespace: kept, streams: [ProbeOp], transport: :native)
      )

    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(kept) != [] end, 15_000)
    [runner_id] = FleetManager.active_runners(kept)
    assert Process.whereis(Flezha.SDK.RunnerServer) == pid

    NodeTestHelper.stop_runner(kept, runner_id)

    assert NodeTestHelper.wait_until(
             fn -> Process.alive?(pid) and :sys.get_state(pid).stopped? end,
             5_000
           ),
           "the runner must stay put, stopped, after the controller's disconnect"

    assert Process.whereis(Flezha.SDK.RunnerServer) == pid
    assert FleetManager.active_runners(kept) == []
  end

  test "io_in drives run/3 through the placed operative", %{ns: ns} do
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000)

    operative_id = "probe_op_#{System.unique_integer([:positive])}"
    builder = %Builder{id: operative_id, factory_id: "sdk_probe", init_values: %{}, opts: []}

    # `start_operative` returns after the SDK acknowledged `AddOperative`, so
    # the operative is instantiated there when the first input is sent.
    assert {:ok, _info} = OperativeManager.start_operative(ns, builder)

    OperativeManager.send_io(operative_id, ns, "hello", %{"channel" => "in", "input_type" => "2"})

    assert_receive {:sdk_probe_ran, "hello"}, 10_000
  end

  test "child-spec form registers identically", %{ns: _ns} do
    # A second start on the same node must lose the process-wide slot.
    other = "sdk_in_node_other_#{System.unique_integer([:positive])}"

    result =
      Flezha.SDK.create_runner(other, transport: :native)
      |> Flezha.SDK.register_factory(ProbeOp)
      |> Flezha.SDK.start()

    assert {:error, {:already_running, ^other}} = result
  end
end

defmodule Flezha.SDK.SocketRoundtripTest do
  @moduledoc """
  Exercises the standalone-mode client transport (the `flz_nif` client entry
  points) end to end within a single node: the SDK dials the controller's own
  UDS listener over the binary protocol — the same handshake, per-runner data
  sockets and framing a rust_runner uses — and drives an io_in through to
  `run/3`. Proves the client NIFs, the reader/writer threads and the `:socket`
  transport seam without a second OS process.
  """
  # async: false: the live controller and its fleet; registered singletons; OS environment variables.
  use ExUnit.Case, async: false

  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.Ctx
  alias Flezha.Service.OperativeManager
  alias Flezha.Socket.Support
  alias Flezha.Transport.Native
  alias FlezhaTest.Helper.NodeTestHelper

  @probe :sdk_socket_probe

  defmodule ProbeTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "probe"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      case Process.whereis(:sdk_socket_probe) do
        nil -> :ok
        pid -> send(pid, {:sdk_socket_ran, input})
      end

      Ctx.emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule ProbeOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "sdk_socket_probe_factory"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(ProbeTask) |> sink("probe")
    end
  end

  test "the SDK and the controller resolve one directory, from the one ladder" do
    # The Elixir SDK derives no path of its own: the SDK's NIF and the
    # controller's both ask `flz_transport`, which is the same code a Rust, Java
    # or Node runner reaches through `flz_core`. That a Rust runner started in the
    # same environment lands on the same directory is proven end to end by
    # `test/system/connect_discovery_e2e_test.exs`, under every runtime.
    configured = Support.socket_dir()

    assert {:ok, runner_dir, _quic} =
             Flezha.SDK.Native.resolve_connect(configured, nil)

    assert {:ok, controller_dir, _quic} =
             Native.resolve_connect(configured, nil)

    assert runner_dir == controller_dir
    assert runner_dir == configured
    assert Path.type(runner_dir) == :absolute
  end

  setup do
    # The standalone SDK reads FLEZHA_SOCKET_FILES_PATH; point it at the
    # controller's own listener path so the same-node dial finds it.
    prev = System.get_env("FLEZHA_SOCKET_FILES_PATH")

    System.put_env("FLEZHA_SOCKET_FILES_PATH", Support.socket_dir())

    ns = "sdk_socket_#{System.unique_integer([:positive])}"
    Process.register(self(), @probe)

    handle =
      Flezha.SDK.create_runner(ns, transport: :uds)
      |> Flezha.SDK.register_factory(ProbeOp)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{} = handle, "SDK (uds) start failed: #{inspect(handle)}"

    on_exit(fn ->
      Flezha.SDK.close(handle)
      if Process.whereis(@probe), do: Process.unregister(@probe)

      if prev,
        do: System.put_env("FLEZHA_SOCKET_FILES_PATH", prev),
        else: System.delete_env("FLEZHA_SOCKET_FILES_PATH")

      try do
        FleetManager.stop_namespace(ns)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, ns: ns, handle: handle}
  end

  test "a standalone UDS runner's admin call is answered", %{ns: ns, handle: handle} do
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000),
           "runner never connected over UDS"

    assert {:ok, []} = Flezha.SDK.Admin.list_operatives(handle)
  end

  test "a standalone UDS runner starts an operative with and without an id, answered by id and outcome",
       %{ns: ns, handle: handle} do
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000),
           "runner never connected over UDS"

    factory = ProbeOp.factory_id()
    named = "socket_admin_#{System.unique_integer([:positive])}"

    assert {:ok, %{id: generated, outcome: :started} = unnamed} =
             Flezha.SDK.Admin.start_operative(handle, factory)

    assert Map.keys(unnamed) |> Enum.sort() == [:id, :outcome]
    assert generated =~ ~r/\Asdk_socket_\w*-[a-z2-7]{16}\z/

    assert {:ok, %{id: ^named, outcome: :started}} =
             Flezha.SDK.Admin.start_operative(handle, factory, id: named)

    assert {:ok, %{id: ^named, outcome: :already_running}} =
             Flezha.SDK.Admin.start_operative(handle, factory, id: named)

    assert {:ok, ids} = Flezha.SDK.Admin.list_operatives(handle)
    assert generated in ids
    assert named in ids

    assert {:error, reason} = Flezha.SDK.Admin.start_operative(handle, "no_such_factory")
    assert is_binary(reason)
  end

  test "a standalone UDS runner connects, places, and dispatches io_in to run/3", %{ns: ns} do
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000),
           "runner never connected over UDS"

    operative_id = "socket_probe_op_#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: operative_id,
      factory_id: ProbeOp.factory_id(),
      init_values: %{},
      opts: []
    }

    assert {:ok, _info} = OperativeManager.start_operative(ns, builder)

    # No settle: the operative's bind waits for its data stream, so the first
    # cast after a start cannot land on an unbound one.
    OperativeManager.send_io(operative_id, ns, "over-the-wire", %{
      "channel" => "in",
      "input_type" => "2"
    })

    assert_receive {:sdk_socket_ran, "over-the-wire"}, 10_000
  end
end

defmodule Flezha.SDK.LivenessReconnectE2ETest do
  @moduledoc """
  In-node integration tests for the SDK's liveness and lifecycle behaviour:
  heartbeats keep an idle runner from stalling (6.2), a killed SDK-side stream
  synthesizes a close that drives reconnect (6.3), a controller that refuses
  the connect is retried further up the ladder each time rather than at one
  rung for ever, and the runner-as-handle contract — reconnect with the same
  stable id, `UpdateFactories`, and removal-refusal (7.2). Native transport,
  in-process, so `run/3` and the streams live in this BEAM.
  """
  # async: false: the live controller and its fleet; global telemetry handlers; OS environment variables.
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.Ctx
  alias Flezha.SDK.RunnerServer
  alias Flezha.Service.OperativeManager
  alias Flezha.Transport.Server, as: TransportServer
  alias FlezhaTest.Helper.NodeTestHelper

  defmodule EchoTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "echo"
    def init_state(_), do: nil

    def run(input, s, ctx),
      do:
        (
          Ctx.emit(ctx, input)
          {:ok, s}
        )
  end

  defmodule EchoOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "lr_echo"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(EchoTask) |> sink("echo")
    end
  end

  defmodule OtherOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "lr_other"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(EchoTask) |> sink("echo")
    end
  end

  defmodule StoreTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "lr_store_tasklet"
    def init_state(_), do: %{inputs: []}
    def apply_change(s, {:push, v}), do: %{s | inputs: s.inputs ++ [v]}
    def run(input, s, ctx), do: {:ok, Ctx.apply(ctx, s, {:push, input})}
  end

  defmodule StoreOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "lr_store"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(StoreTask) |> sink("store")
    end
  end

  defp start_runner(ns, streams) do
    handle =
      Enum.reduce(streams, Flezha.SDK.create_runner(ns, transport: :native), fn m, acc ->
        Flezha.SDK.register_factory(acc, m)
      end)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{} = handle, "start failed: #{inspect(handle)}"
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000)
    handle
  end

  setup do
    ns = "lr_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      try do
        FleetManager.stop_namespace(ns)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, ns: ns}
  end

  test "an idle native runner emitting heartbeats fires no output_stall", %{ns: ns} do
    prev = System.get_env("FLEZHA_DATA_HEARTBEAT_MS")
    System.put_env("FLEZHA_DATA_HEARTBEAT_MS", "200")

    on_exit(fn ->
      if prev,
        do: System.put_env("FLEZHA_DATA_HEARTBEAT_MS", prev),
        else: System.delete_env("FLEZHA_DATA_HEARTBEAT_MS")
    end)

    test_pid = self()
    ref = make_ref()

    :telemetry.attach(
      "lr-stall-#{inspect(ref)}",
      [:flezha, :state, :output_stall],
      fn _e, _m, meta, _ -> send(test_pid, {:output_stall, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("lr-stall-#{inspect(ref)}") end)

    handle = start_runner(ns, [EchoOp])
    on_exit(fn -> Flezha.SDK.close(handle) end)

    # Idle across several heartbeat windows — the heartbeats show as the
    # secondary stream's `last_seen` moving on — and no stall is flagged.
    [runner_id] = FleetManager.active_runners(ns)
    channels = secondary_channels(runner_id)
    idle_from = System.monotonic_time(:millisecond)

    assert NodeTestHelper.wait_until(
             fn ->
               Enum.any?(channels, &(:sys.get_state(&1).last_seen >= idle_from + 3 * 200))
             end,
             10_000
           ),
           "the heartbeats never reached a secondary stream"

    refute_received {:output_stall, _}
  end

  test "a closed control stream drives reconnect and the runner recovers", %{ns: ns} do
    handle = start_runner(ns, [EchoOp])
    on_exit(fn -> Flezha.SDK.close(handle) end)

    [runner] = FleetManager.active_runners(ns)

    # The close a killed SDK-side LocalStream's peer monitor synthesizes.
    send(RunnerServer.whereis(), {:flz_closed, control_stream_id(), "test_injected"})

    # The runner reconnects under a fresh session, which the binding swap lists
    # in place of the old one.
    assert NodeTestHelper.wait_until(
             fn -> match?([session] when session != runner, FleetManager.active_runners(ns)) end,
             15_000
           )
  end

  test "a severed data plane is rebound inside the session, with no reconnect", %{ns: ns} do
    handle = start_runner(ns, [EchoOp])
    on_exit(fn -> Flezha.SDK.close(handle) end)

    [runner] = FleetManager.active_runners(ns)
    severed = for channel <- secondary_channels(runner), do: :sys.get_state(channel).stream_ref

    for ref <- severed, do: Flezha.Transport.Stream.close(ref)

    assert NodeTestHelper.wait_until(
             fn ->
               refs =
                 for channel <- secondary_channels(runner), do: :sys.get_state(channel).stream_ref

               refs != [] and Enum.all?(refs, &(&1 != nil and &1 not in severed))
             end,
             15_000
           ),
           "the severed secondaries were never rebound"

    assert FleetManager.active_runners(ns) == [runner], "the session was ended to repair it"
  end

  test "a data plane that cannot be rebound ends the session once its budget is spent", %{
    ns: ns
  } do
    handle = start_runner(ns, [EchoOp])
    on_exit(fn -> Flezha.SDK.close(handle) end)

    [runner] = FleetManager.active_runners(ns)
    test_pid = self()

    set_mimic_global_until_exit()

    stub(FleetManager, :rebind_data_streams, fn id ->
      send(test_pid, {:rebind_refused, id})
      {:error, :refused}
    end)

    # One stream is the whole data plane: the runner lets the rest go with it.
    [channel | _] = secondary_channels(runner)
    Flezha.Transport.Stream.close(:sys.get_state(channel).stream_ref)

    for _attempt <- 1..3, do: assert_receive({:rebind_refused, ^runner}, 5_000)

    assert NodeTestHelper.wait_until(
             fn -> match?([session] when session != runner, FleetManager.active_runners(ns)) end,
             15_000
           ),
           "the session outlived a data plane it could not rebind"

    refute_received {:rebind_refused, ^runner}
  end

  test "a refused reconnect climbs the ladder instead of spinning at one rung", %{ns: ns} do
    _handle = start_runner(ns, [EchoOp])
    server = RunnerServer.whereis()
    assert is_pid(server)

    test_pid = self()
    ref = make_ref()
    handler = "lr-backoff-#{inspect(ref)}"

    :telemetry.attach(
      handler,
      [:flezha, :backoff, :wait],
      fn _e, %{attempt: attempt, delay_ms: delay}, meta, _ ->
        if meta.ladder == :sdk_reconnect, do: send(test_pid, {ref, attempt, delay})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    # The controller refuses every further connect, which is what a runner
    # meets when its namespace is not one this controller runs.
    set_mimic_global_until_exit()
    stub(TransportServer, :connect, fn _payload, _controller -> {:error, :refused} end)

    {waits, _log} =
      with_log(fn ->
        send(server, {:flz_closed, control_stream_id(), "test_injected"})

        collected =
          for _ <- 1..3 do
            assert_receive {^ref, attempt, delay}, 10_000
            {attempt, delay}
          end

        # The retry loop is unbounded by design; the runner is taken down
        # rather than left climbing to the 30 s cap for the rest of the run.
        Process.unlink(server)
        Process.exit(server, :kill)
        collected
      end)

    assert [{1, first}, {2, _}, {3, third}] = waits,
           "three refused connects must announce attempts 1, 2 and 3, not one rung three times"

    assert third > first,
           "the third refused reconnect waited #{third} ms, no longer than the first (#{first} ms)"
  end

  test "post-start define_stream ships UpdateFactories; removal with a live instance refuses", %{
    ns: ns
  } do
    handle = start_runner(ns, [EchoOp])
    on_exit(fn -> Flezha.SDK.close(handle) end)

    # Post-start define → UpdateFactories → the controller learns the new factory.
    handle2 = Flezha.SDK.register_factory(handle, OtherOp)
    assert %Flezha.SDK{} = handle2

    assert NodeTestHelper.wait_until(
             fn ->
               case FleetManager.list_factories(ns) do
                 {:ok, factories} -> "lr_other" in factory_ids(factories)
                 _ -> false
               end
             end,
             10_000
           )

    # Placing an instance of EchoOp, then removing it, refuses while live.
    operative_id = "lr_inst_#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             OperativeManager.start_operative(ns, %Builder{
               id: operative_id,
               factory_id: "lr_echo",
               init_values: %{},
               opts: []
             })

    # `start_operative` returns after the SDK acknowledged `AddOperative`, so
    # the instance is live on the handle when the removal is refused.
    assert {:error, {:operative_type_in_use, "lr_echo", n}} =
             Flezha.SDK.unregister_factory(handle2, EchoOp)

    assert n >= 1
  end

  test "a live read answers through the controller, and a close ships the freshest state to the Overwatch",
       %{
         ns: ns
       } do
    handle = start_runner(ns, [StoreOp])
    on_exit(fn -> Flezha.SDK.close(handle) end)
    operative_id = "lr_store_#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             OperativeManager.start_operative(ns, %Builder{
               id: operative_id,
               factory_id: "lr_store",
               init_values: %{},
               opts: [snapshot_interval: 0]
             })

    {:ok, overwatch} = OperativeManager.get_operative_pid(ns, operative_id)
    assert :sys.get_state(overwatch).snapshot == nil

    for input <- ~w(alpha beta gamma) do
      OperativeManager.send_io(operative_id, ns, input, %{"channel" => "in", "input_type" => "2"})
    end

    # The live read is the residency signal, and the CellState command end to end.
    assert NodeTestHelper.wait_until(
             fn ->
               live_state(ns, operative_id) ==
                 %{"lr_store_tasklet" => %{"inputs" => ["alpha", "beta", "gamma"]}}
             end,
             15_000
           )

    assert {:ok, _json, seq} =
             OperativeManager.get_cell_state(ns, operative_id, %{
               tasklet: "lr_store_tasklet"
             })

    assert seq == 3, "three records shipped, so the one Cell read at change_seq 3"

    # A close is a graceful stop: terminate/2 ships the notification and the
    # controller's Overwatch keeps the state the runner held.
    :ok = Flezha.SDK.close(handle)

    assert NodeTestHelper.wait_until(
             fn -> Process.alive?(overwatch) and :sys.get_state(overwatch).snapshot != nil end,
             5_000
           ),
           "no RunnerShutdownNotification reached the Overwatch"

    assert Jason.decode!(:sys.get_state(overwatch).snapshot) ==
             %{"lr_store_tasklet" => %{"inputs" => ["alpha", "beta", "gamma"]}}
  end

  defp live_state(ns, operative_id) do
    case OperativeManager.get_cell_state(ns, operative_id) do
      {:ok, json} -> Jason.decode!(json)
      _ -> nil
    end
  end

  defp factory_ids(factories) do
    Enum.map(factories, fn
      %{id: id} -> id
      %{"id" => id} -> id
      {id, _} -> id
      id when is_binary(id) -> id
      other -> other
    end)
  end

  # The runner's secondary (heartbeat-carrying) stream channels.
  defp control_stream_id do
    Flezha.Transport.Stream.stream_id(:sys.get_state(RunnerServer.whereis()).control_handle)
  end

  defp secondary_channels(runner_id) do
    for name <- Flezha.Runner.StreamPool.stream_ids(runner_id, :secondary),
        pid = GenServer.whereis(name),
        is_pid(pid),
        do: pid
  end
end

defmodule Flezha.SDK.SpawnWorkflowE2ETest do
  @moduledoc """
  Tasklet-requested spawn against the same-BEAM controller: a stream
  tasklet on an in-node runner spawns an instance of a workflow class the same
  runner offers, delivers to it under the same id, and is answered
  `already_running` under the error policy. The request, the controller's
  resolution and the runner server's waiter run end to end.
  """
  # async: false: the live controller and its fleet; registered singletons.
  use ExUnit.Case, async: false

  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.{Ctx, Spawn}
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.NodeTestHelper

  @probe :sdk_spawn_probe

  # One spawn per `<instance id>:<policy>:<payload>` input, read inside the
  # invocation that made it. An empty instance id spawns without one.
  defmodule Requester do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "requester"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      [instance_id, policy, payload] = String.split(input, ":", parts: 3)
      handle = Ctx.spawn_workflow(ctx, "sdk_spawned_inbox", payload, options(instance_id, policy))
      result = Spawn.await(handle)

      case Process.whereis(:sdk_spawn_probe) do
        nil -> :ok
        pid -> send(pid, {:spawn_result, payload, result})
      end

      {:ok, s}
    end

    defp options("", _policy), do: []
    defp options(instance_id, "error"), do: [id: instance_id, if_running: :error]
    defp options(instance_id, _policy), do: [id: instance_id]
  end

  defmodule RequesterOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "sdk_spawn_requester"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(Requester) |> sink("requested")
    end
  end

  # What the spawned instance consumes, one iteration per input.
  defmodule Received do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "received"
    def init_state(_), do: nil

    def run(input, s, _ctx) do
      case Process.whereis(:sdk_spawn_probe) do
        nil -> :ok
        pid -> send(pid, {:workflow_received, input})
      end

      {:ok, s}
    end
  end

  defmodule InboxWorkflow do
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "sdk_spawned_inbox"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 60_000

    def tasklets(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(Received)
    end
  end

  setup do
    ns = "sdk_spawn_#{System.unique_integer([:positive])}"
    Process.register(self(), @probe)

    handle =
      Flezha.SDK.create_runner(ns, transport: :native)
      |> Flezha.SDK.register_factory(RequesterOp)
      |> Flezha.SDK.register_factory(InboxWorkflow)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{} = handle, "SDK start failed: #{inspect(handle)}"

    on_exit(fn ->
      Flezha.SDK.close(handle)

      try do
        FleetManager.stop_namespace(ns)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, ns: ns}
  end

  test "a tasklet spawns a workflow instance, delivers to it, is refused under the error policy, and spawns another without an id",
       %{ns: ns} do
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000)

    requester = "spawn_requester_#{System.unique_integer([:positive])}"
    instance = "spawned_#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: requester,
      factory_id: RequesterOp.factory_id(),
      init_values: %{},
      opts: []
    }

    assert {:ok, _info} = OperativeManager.start_operative(ns, builder)

    request = fn policy, payload ->
      OperativeManager.send_io(requester, ns, "#{instance}:#{policy}:#{payload}", %{
        "channel" => "in",
        "input_type" => "2"
      })
    end

    request.("deliver", "p1")
    assert_receive {:spawn_result, "p1", {:ok, %{id: ^instance, outcome: :spawned}}}, 20_000
    request.("deliver", "p2")
    assert_receive {:spawn_result, "p2", {:ok, %{id: ^instance, outcome: :delivered}}}, 20_000
    request.("error", "p3")
    assert_receive {:spawn_result, "p3", {:error, {:already_running, ^instance}}}, 20_000

    OperativeManager.send_io(requester, ns, "::p4", %{"channel" => "in", "input_type" => "2"})
    assert_receive {:spawn_result, "p4", {:ok, %{id: generated, outcome: :spawned}}}, 20_000
    assert generated =~ ~r/\Asdk_spawned_\w*-[a-z2-7]{16}\z/

    assert_receive {:workflow_received, "p1"}, 20_000
    assert_receive {:workflow_received, "p2"}, 20_000
    assert_receive {:workflow_received, "p4"}, 20_000
  end
end
