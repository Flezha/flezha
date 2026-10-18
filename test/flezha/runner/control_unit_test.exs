defmodule FlezhaTest.Runner.ControlUnitAddRelayTest do
  @moduledoc """
  The resuming bind's two instance-scoped fields on `AddRelay`, both stamped
  onto the relay's opts by `Flezha.Relay.Lifecycle`.

  `task_journal` rides the bind rather than a per-Cell `RestoreInitState`
  because it is instance-scoped: a durable steps class with no per-Cell
  state ships no restore at all, so a journal delivered there would never arrive
  and the driver would resume at iteration 0 on every wake.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Identificator
  alias Flezha.Relay.Builder
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-unit-add-relay-test"

  setup :set_mimic_global_until_exit

  setup do
    test_pid = self()
    runner_id = "exec-add-#{System.unique_integer([:positive])}"

    ControlChannel
    |> stub(:send_control, fn ^runner_id, command ->
      send(test_pid, {:sent, command})
      {:ok, make_ref()}
    end)

    {:ok, pid} = ControlUnit.start_link({@namespace, runner_id})
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000) end)

    %{runner_id: runner_id}
  end

  defp start_relay(runner_id, opts) do
    relay = %Builder{id: "op-1", factory_id: "f", run: :steps, opts: opts}
    # The reply only lands on the AddRelay response, which never arrives here.
    catch_exit(
      GenServer.call(
        Identificator.control_unit(runner_id),
        {:start_relay, relay},
        50
      )
    )
  end

  test "a resuming bind carries the task-journal even with nothing to restore", %{
    runner_id: runner_id
  } do
    journal = %RunnerCommands.TaskJournalState{iter: 7, consumed_offset: 3}
    start_relay(runner_id, resume_cell_count: 0, task_journal: journal)

    assert_receive {:sent, {:add_relay, add}}, 500
    assert add.resume_cell_count == 0
    assert add.task_journal == journal
  end

  test "a bind carries how many effect results it ships", %{runner_id: runner_id} do
    start_relay(runner_id, effect_results: List.duplicate(%RunnerCommands.EffectResult{}, 3))

    assert_receive {:sent, {:add_relay, add}}, 500
    assert add.effect_result_count == 3
  end

  test "a fresh bind carries no journal", %{runner_id: runner_id} do
    start_relay(runner_id, [])

    assert_receive {:sent, {:add_relay, add}}, 500
    assert add.resume_cell_count == 0
    assert add.task_journal == nil
  end
end

defmodule FlezhaTest.Runner.ControlUnitHealthTest do
  @moduledoc """
  ControlUnit stuck-caller unblocking. A control reply left unacked past the
  threshold would otherwise hang the caller: the ControlUnit fails the waiter
  with `:timeout` and drops the ref so `response_refs` cannot grow flow.

  Runner liveness/eviction is NOT this sweep's job — it is the
  `ControlChannel` watchdog's. This heartbeat evicts nothing.
  """
  # async: false: the primary Logger; registered runner channels.
  use ExUnit.Case, async: false

  # The stuck-caller sweep logs at warn by design; capture so a green run stays
  # quiet (logs still print on failure).
  @moduletag :capture_log

  alias Flezha.Runner.ControlUnit
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    on_exit(NodeTestHelper.override_log_level(:warning))
  end

  test "heartbeat unblocks the stuck caller when a control reply is unacked past the threshold" do
    now = System.monotonic_time(:millisecond)
    reply_ref = make_ref()

    state = %{
      namespace: "default",
      runner_id: "ex_dead",
      response_refs: %{
        make_ref() => {:reply, {self(), reply_ref}, now - 120_000, false}
      }
    }

    assert {:noreply, %{response_refs: refs}} = ControlUnit.handle_info(:heartbeat, state)

    # The stuck ref is dropped and its caller is unblocked with a clean error
    # (so a blocking caller never hangs forever on the dead runner).
    assert refs == %{}
    assert_receive {^reply_ref, {:error, :timeout}}
  end

  test "heartbeat leaves a fresh reply ref alone (no false eviction)" do
    now = System.monotonic_time(:millisecond)
    ref = make_ref()

    state = %{
      namespace: "default",
      runner_id: "ex_ok",
      response_refs: %{
        ref => {:reply, {self(), make_ref()}, now - 1_000, false}
      }
    }

    assert {:noreply, %{response_refs: refs}} = ControlUnit.handle_info(:heartbeat, state)

    assert Map.has_key?(refs, ref)
    refute_received {_ref, {:error, :timeout}}
  end
end

defmodule FlezhaTest.Runner.ControlUnitPartitionTest do
  @moduledoc """
  Partition identity on the `AddRelay` wire.

  The SDK's mis-route guard is armed by these four fields and nothing else, so
  a member whose bind loses them starts accepting keys it does not own — which
  is why the migrate-time bind is pinned here too. The unpartitioned frame must
  stay byte-identical: proto3 omits zero-valued scalars, and that is the whole
  compatibility story.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Identificator
  alias Flezha.Relay.Builder
  alias Flezha.Relay.Migration
  alias Flezha.Partition
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-unit-partition-test"

  setup :set_mimic_global_until_exit

  setup do
    test_pid = self()
    runner_id = "exec-part-#{System.unique_integer([:positive])}"

    ControlChannel
    |> stub(:send_control, fn ^runner_id, command ->
      send(test_pid, {:sent, command})
      {:ok, make_ref()}
    end)

    {:ok, pid} = ControlUnit.start_link({@namespace, runner_id})
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000) end)

    %{runner_id: runner_id}
  end

  defp add_relay(runner_id, opts) do
    relay = %Builder{id: "ohlc#p1", factory_id: "candles", run: :flow, opts: opts}

    catch_exit(
      GenServer.call(
        Identificator.control_unit(runner_id),
        {:start_relay, relay},
        50
      )
    )

    assert_receive {:sent, {:add_relay, add}}, 500
    add
  end

  test "a member's bind carries its index, count and owned range", %{runner_id: runner_id} do
    [_p0, range, _p2] = Partition.ranges(3)
    add = add_relay(runner_id, Partition.member_opts("ohlc", 1, 3, range))

    assert add.partition_index == 1
    assert add.partition_count == 3
    assert {add.key_group_from, add.key_group_to} == range
  end

  test "an unpartitioned bind encodes byte-identically to a partition-free build", %{
    runner_id: runner_id
  } do
    add = add_relay(runner_id, [])

    assert add.partition_index == 0
    assert add.partition_count == 0
    assert add.key_group_from == 0
    assert add.key_group_to == 0

    # The four fields are all zero, so proto3 emits none of them: the frame is
    # exactly the one a pre-partition controller produced.
    {:ok, encoded, _size} = RunnerCommands.AddRelay.encode(add)

    {:ok, reference, _ref_size} =
      RunnerCommands.AddRelay.encode(%RunnerCommands.AddRelay{
        relay_id: add.relay_id,
        factory_id: add.factory_id,
        init_values: add.init_values,
        resume_cell_count: add.resume_cell_count,
        task_journal: add.task_journal
      })

    assert IO.iodata_to_binary(encoded) == IO.iodata_to_binary(reference)
  end

  test "a one-member group is unpartitioned on the wire", %{runner_id: runner_id} do
    add = add_relay(runner_id, Partition.member_opts("ohlc", 0, 1, {0, 255}))

    assert add.partition_count == 0
    assert add.key_group_to == 0
  end

  test "the migrate-time bind re-arms the guard" do
    [_p0, range, _p2] = Partition.ranges(3)

    state = %Flezha.Relay.Overwatch{
      id: "ohlc#p1",
      factory_id: "candles",
      namespace: @namespace,
      builder: %Builder{
        id: "ohlc#p1",
        factory_id: "candles",
        run: :steps,
        opts: [{:plugins, []} | Partition.member_opts("ohlc", 1, 3, range)]
      }
    }

    migrated = Migration.bind_relay(state)

    # The run travels with the migrate: a steps instance re-added as a
    # flow class is refused by the runner and never heals.
    assert migrated.run == :steps

    assert Partition.identity(migrated.opts) == %{
             group: "ohlc",
             index: 1,
             count: 3,
             range: range
           }
  end
end

defmodule FlezhaTest.Runner.ControlUnitStepsStopTest do
  @moduledoc """
  §5a.1/§6.2: the cooperative external-stop send. `ControlUnit.steps_stop/2`
  ships a `StepsStop` command over the control channel — distinct from the hard
  `RemRelay` the stream stop uses. Fire-and-forget: the SDK trips its
  cancellation token and the teardown follows on `RelayCompleted`.
  """

  # async: false: Mimic global mode; Mimic stubs; registered runner channels.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Identificator
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-unit-steps-stop-test"

  setup :set_mimic_global_until_exit

  test "steps_stop/2 sends a StepsStop command (cooperative), not RemRelay" do
    test_pid = self()
    runner_id = "exec-bs-#{System.unique_integer([:positive])}"

    ControlChannel
    |> stub(:send_control_async, fn ^runner_id, command ->
      send(test_pid, {:sent, command})
      :ok
    end)

    {:ok, pid} = ControlUnit.start_link({@namespace, runner_id})
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000) end)

    assert :ok = ControlUnit.steps_stop(runner_id, "op-1")

    assert_receive {:sent, {:steps_stop, %RunnerCommands.StepsStop{relay_id: "op-1"}}},
                   500

    # Cooperative stop must NOT use the hard removal command.
    refute_received {:sent, {:rem_relay, _}}
  end

  test "steps_stop/2 returns {:error, :no_control_unit} when the runner is gone" do
    missing = "exec-missing-#{System.unique_integer([:positive])}"
    assert GenServer.whereis(Identificator.control_unit(missing)) == nil
    assert {:error, :no_control_unit} = ControlUnit.steps_stop(missing, "op-1")
  end
end

defmodule FlezhaTest.Runner.AddRelayResponseTest do
  @moduledoc """
  The SDK declares its class-level capabilities in the `AddRelay` response
  metadata (`AddRelayResponse.raw_data`) — the effectively-once tier and the
  chain positions that can compensate. The controller resolves absent, empty, or
  undecodable metadata to the defaults.
  """
  use ExUnit.Case, async: true

  alias Flezha.Relay.ClassRegistry

  defp declarations(metadata), do: ClassRegistry.decode_declarations(encode(metadata))

  test "tier declaration in the response metadata resolves true" do
    assert declarations(%{"exactly" => "effectively_once"}).effectively_once
  end

  test "metadata without the tier key resolves false" do
    refute declarations(%{"other" => "x"}).effectively_once
    refute declarations(%{"exactly" => "at_least_once"}).effectively_once
  end

  test "empty raw_data (pre-tier or Java runner) resolves the defaults" do
    for raw <- ["", nil] do
      assert ClassRegistry.decode_declarations(raw) == %{
               run: :flow,
               each: nil,
               on_failure: nil,
               durable: :local,
               sleep_after_ms: nil,
               max_duration_ms: nil,
               effectively_once: false,
               keyless: false,
               ack_checkpoint: false,
               compensable_tasks: nil,
               leaf_tasks: nil,
               correction_window_ms: nil,
               compact_after_bytes: nil,
               persistent: true
             }
    end
  end

  test "undecodable raw_data resolves the defaults" do
    assert ClassRegistry.decode_declarations(<<255, 255, 255, 1>>) == %{
             run: :flow,
             each: nil,
             on_failure: nil,
             durable: :local,
             sleep_after_ms: nil,
             max_duration_ms: nil,
             effectively_once: false,
             keyless: false,
             ack_checkpoint: false,
             compensable_tasks: nil,
             leaf_tasks: nil,
             correction_window_ms: nil,
             compact_after_bytes: nil,
             persistent: true
           }
  end

  test "a class keeps its work locally, and sleeps only after the interval it announced" do
    assert declarations(%{"other" => "x"}).persistent
    refute declarations(%{"durable" => "memory"}).persistent
    assert declarations(%{"durable" => "local"}).durable == :local
    assert declarations(%{"other" => "x"}).sleep_after_ms == nil
    assert declarations(%{"sleep_after_ms" => "21600000"}).sleep_after_ms == 21_600_000
  end

  test "keyless is declared only when true — a keyed class says nothing" do
    # `partitioned-relays`: the SDK announces `keyed: "false"` for a class
    # with no keyed entry-fed tasklet, so the controller can refuse a
    # partitioned start of it before touching a runner. A keyed class declares
    # nothing, which is what keeps its response byte-identical.
    assert declarations(%{"keyed" => "false"}).keyless
    refute declarations(%{}).keyless
    refute declarations(%{"keyed" => "true"}).keyless
  end

  test "a declared compact_after_bytes resolves from the metadata; absent or invalid is nil" do
    assert declarations(%{"compact_after_bytes" => "4194304"}).compact_after_bytes == 4_194_304
    # A class that declares nothing lets the global default govern.
    assert declarations(%{"other" => "x"}).compact_after_bytes == nil
    assert declarations(%{"compact_after_bytes" => "0"}).compact_after_bytes == nil
    assert declarations(%{"compact_after_bytes" => "nope"}).compact_after_bytes == nil
  end

  test "a correction window resolves from the metadata; absent or invalid is nil" do
    assert declarations(%{"correction_ms" => "600000"}).correction_window_ms == 600_000
    # A class that declares nothing never corrects — the cost-free default.
    assert declarations(%{"other" => "x"}).correction_window_ms == nil
    assert declarations(%{"correction_ms" => "0"}).correction_window_ms == nil
    assert declarations(%{"correction_ms" => "nope"}).correction_window_ms == nil
  end

  test "checkpoint ack-granularity resolves from the metadata; anything else is per-input" do
    assert declarations(%{"ack" => "checkpoint"}).ack_checkpoint
    # Per-input is the tier's default granularity — an undeclaring class keeps it.
    refute declarations(%{"exactly" => "effectively_once"}).ack_checkpoint
    refute declarations(%{"ack" => "per_input"}).ack_checkpoint
  end

  test "remote durability resolves from the metadata; anything else is local" do
    assert declarations(%{"durable" => "remote"}).durable == :remote
    # Local is the cost-free default: a class that declares nothing never makes
    # a commit barrier wait on a remote store.
    assert declarations(%{"other" => "x"}).durable == :local
  end

  test "compensable chain positions resolve to a list" do
    assert declarations(%{"compensable_tasks" => "0,2,3"}).compensable_tasks == [0, 2, 3]
  end

  test "a declared-but-empty compensable list is not the same as an absent one" do
    # "" = a structured chain, none of it compensable (a rewind of an emitting task
    # is refused); absent = the class declares nothing at all (no information).
    assert declarations(%{"compensable_tasks" => ""}).compensable_tasks == []
    assert declarations(%{"other" => "x"}).compensable_tasks == nil
  end

  test "a steps relay's run and loop shape resolve to atoms, and anything else to none" do
    assert declarations(%{"run" => "steps"}).run == :steps
    assert declarations(%{"run" => "bounded"}).run == :flow
    assert declarations(%{"on_failure" => "keep_progress"}).on_failure == :keep_progress
    assert declarations(%{"each" => "per_input"}).each == :per_input
    assert declarations(%{"each" => "once"}).each == :once
    assert declarations(%{"each" => "chained"}).each == :chained
    assert declarations(%{"each" => "sometimes"}).each == nil
    assert declarations(%{"other" => "x"}).each == nil
  end

  test "leaf positions resolve to a list — the tree's completion rule" do
    # A→(B,C): the relay is done when tasks 1 and 2 have committed.
    assert declarations(%{"leaf_tasks" => "1,2"}).leaf_tasks == [1, 2]
    # A linear chain's single terminal is the degenerate case.
    assert declarations(%{"leaf_tasks" => "1"}).leaf_tasks == [1]
    # Absent = a stream / an SDK that does not declare it — the
    # controller cannot derive completion and must not guess.
    assert declarations(%{"other" => "x"}).leaf_tasks == nil
  end

  defp encode(metadata) do
    {:ok, iodata, _size} =
      Protox.encode(%RunnerCommands.AddRelayResponse{metadata: metadata})

    IO.iodata_to_binary(iodata)
  end
end

defmodule FlezhaTest.Runner.ControlUnitPingTimeoutTest do
  @moduledoc """
  The overwatch ping timeout is one knob on two layers, and the published
  contract names both: the `:overwatch_ping_timeout_ms` application key
  `Flezha.Runner.ControlUnit` reads, and the `FLEZHA_OVERWATCH_PING_TIMEOUT_MS`
  variable `config/runtime.exs` resolves it from at boot. A user who sets
  the variable and gets the default back has been told a knob exists that does
  nothing.
  """
  # async: false: the OS environment is process-global.
  use ExUnit.Case, async: false

  @var "FLEZHA_OVERWATCH_PING_TIMEOUT_MS"

  setup do
    previous = System.get_env(@var)

    on_exit(fn ->
      case previous do
        nil -> System.delete_env(@var)
        value -> System.put_env(@var, value)
      end
    end)

    :ok
  end

  test "the variable the contract names reaches the application key" do
    System.put_env(@var, "7777")

    assert ping_timeout_from_runtime_config(:prod) == 7777
  end

  test "an unset variable leaves the 2 s default the ControlUnit reads" do
    System.delete_env(@var)

    assert ping_timeout_from_runtime_config(:prod) == 2_000
  end

  test "a value that is not an integer refuses at boot rather than degrading" do
    System.put_env(@var, "soon")

    assert_raise ArgumentError, fn -> ping_timeout_from_runtime_config(:prod) end
  end

  # Evaluates the real `config/runtime.exs`, which is where the two halves of
  # the knob meet: the variable is resolved once at boot into the application
  # key every reader takes.
  defp ping_timeout_from_runtime_config(env) do
    Config.Reader.read!("config/runtime.exs", env: env, target: :host)
    |> get_in([:flezha, :overwatch_ping_timeout_ms])
  end
end
