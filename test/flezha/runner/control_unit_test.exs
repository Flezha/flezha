defmodule FlezhaTest.Runner.ControlUnitAddOperativeTest do
  @moduledoc """
  The resuming bind's two instance-scoped fields on `AddOperative`, both stamped
  onto the operative's opts by `Flezha.Operative.Lifecycle`.

  `task_journal` rides the bind rather than a per-Cell `RestoreInitState`
  because it is instance-scoped: a durable workflow class with no per-Cell
  state ships no restore at all, so a journal delivered there would never arrive
  and the driver would resume at iteration 0 on every wake.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Identificator
  alias Flezha.Operative.Builder
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-unit-add-operative-test"

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

  defp start_operative(runner_id, opts) do
    operative = %Builder{id: "op-1", factory_id: "f", kind: :workflow, opts: opts}
    # The reply only lands on the AddOperative response, which never arrives here.
    catch_exit(
      GenServer.call(
        Identificator.control_unit(runner_id),
        {:start_operative, operative},
        50
      )
    )
  end

  test "a resuming bind carries the task-journal even with nothing to restore", %{
    runner_id: runner_id
  } do
    journal = %RunnerCommands.TaskJournalState{iter: 7, consumed_offset: 3}
    start_operative(runner_id, resume_cell_count: 0, task_journal: journal)

    assert_receive {:sent, {:add_operative, add}}, 500
    assert add.resume_cell_count == 0
    assert add.task_journal == journal
  end

  test "a fresh bind carries no journal", %{runner_id: runner_id} do
    start_operative(runner_id, [])

    assert_receive {:sent, {:add_operative, add}}, 500
    assert add.resume_cell_count == 0
    assert add.task_journal == nil
  end
end

defmodule FlezhaTest.Runner.ControlUnitHealthTest do
  @moduledoc """
  ControlUnit stuck-caller unblocking. A control reply left unacked past the
  threshold would otherwise hang the caller: the ControlUnit fails the waiter
  with `:timeout` and drops the ref so `response_refs` cannot grow unbounded.

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
  Partition identity on the `AddOperative` wire.

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
  alias Flezha.Operative.Builder
  alias Flezha.Operative.Migration
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

  defp add_operative(runner_id, opts) do
    operative = %Builder{id: "ohlc#p1", factory_id: "candles", kind: :stream, opts: opts}

    catch_exit(
      GenServer.call(
        Identificator.control_unit(runner_id),
        {:start_operative, operative},
        50
      )
    )

    assert_receive {:sent, {:add_operative, add}}, 500
    add
  end

  test "a member's bind carries its index, count and owned range", %{runner_id: runner_id} do
    [_p0, range, _p2] = Partition.ranges(3)
    add = add_operative(runner_id, Partition.member_opts("ohlc", 1, 3, range))

    assert add.partition_index == 1
    assert add.partition_count == 3
    assert {add.key_group_from, add.key_group_to} == range
  end

  test "an unpartitioned bind encodes byte-identically to a partition-free build", %{
    runner_id: runner_id
  } do
    add = add_operative(runner_id, [])

    assert add.partition_index == 0
    assert add.partition_count == 0
    assert add.key_group_from == 0
    assert add.key_group_to == 0

    # The four fields are all zero, so proto3 emits none of them: the frame is
    # exactly the one a pre-partition controller produced.
    {:ok, encoded, _size} = RunnerCommands.AddOperative.encode(add)

    {:ok, reference, _ref_size} =
      RunnerCommands.AddOperative.encode(%RunnerCommands.AddOperative{
        operative_id: add.operative_id,
        factory_id: add.factory_id,
        init_values: add.init_values,
        kind: add.kind,
        resume_cell_count: add.resume_cell_count,
        task_journal: add.task_journal
      })

    assert IO.iodata_to_binary(encoded) == IO.iodata_to_binary(reference)
  end

  test "a one-member group is unpartitioned on the wire", %{runner_id: runner_id} do
    add = add_operative(runner_id, Partition.member_opts("ohlc", 0, 1, {0, 255}))

    assert add.partition_count == 0
    assert add.key_group_to == 0
  end

  test "the migrate-time bind re-arms the guard" do
    [_p0, range, _p2] = Partition.ranges(3)

    state = %Flezha.Operative.Overwatch{
      id: "ohlc#p1",
      factory_id: "candles",
      namespace: @namespace,
      builder: %Builder{
        id: "ohlc#p1",
        factory_id: "candles",
        kind: :workflow,
        opts: [{:plugins, []} | Partition.member_opts("ohlc", 1, 3, range)]
      }
    }

    migrated = Migration.migrate_operative(state)

    # The kind travels with the migrate: a workflow instance re-added as a
    # stream class is refused by the runner and never heals.
    assert migrated.kind == :workflow

    assert Partition.identity(migrated.opts) == %{
             group: "ohlc",
             index: 1,
             count: 3,
             range: range
           }

    # Only the partition keys survive — a migrate still ships no plugins.
    assert Keyword.get(migrated.opts, :plugins) == nil
  end
end

defmodule FlezhaTest.Runner.ControlUnitWorkflowStopTest do
  @moduledoc """
  §5a.1/§6.2: the cooperative external-stop send. `ControlUnit.workflow_stop/2`
  ships a `WorkflowStop` command over the control channel — distinct from the hard
  `RemOperative` the stream stop uses. Fire-and-forget: the SDK trips its
  cancellation token and the teardown follows on `WorkflowCompleted`.
  """

  # async: false: Mimic global mode; Mimic stubs; registered runner channels.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Identificator
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-unit-workflow-stop-test"

  setup :set_mimic_global_until_exit

  test "workflow_stop/2 sends a WorkflowStop command (cooperative), not RemOperative" do
    test_pid = self()
    runner_id = "exec-bs-#{System.unique_integer([:positive])}"

    ControlChannel
    |> stub(:send_control_async, fn ^runner_id, command ->
      send(test_pid, {:sent, command})
      :ok
    end)

    {:ok, pid} = ControlUnit.start_link({@namespace, runner_id})
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000) end)

    assert :ok = ControlUnit.workflow_stop(runner_id, "op-1")

    assert_receive {:sent, {:workflow_stop, %RunnerCommands.WorkflowStop{operative_id: "op-1"}}},
                   500

    # Cooperative stop must NOT use the hard removal command.
    refute_received {:sent, {:rem_operative, _}}
  end

  test "workflow_stop/2 returns {:error, :no_control_unit} when the runner is gone" do
    missing = "exec-missing-#{System.unique_integer([:positive])}"
    assert GenServer.whereis(Identificator.control_unit(missing)) == nil
    assert {:error, :no_control_unit} = ControlUnit.workflow_stop(missing, "op-1")
  end
end

defmodule FlezhaTest.Runner.AddOperativeResponseTest do
  @moduledoc """
  The SDK declares its class-level capabilities in the `AddOperative` response
  metadata (`AddOperativeResponse.raw_data`) — the effectively-once tier and the
  chain positions that can compensate. The controller resolves absent, empty, or
  undecodable metadata to the defaults.
  """
  use ExUnit.Case, async: true

  alias Flezha.Operative.ClassRegistry

  defp declarations(metadata), do: ClassRegistry.decode_declarations(encode(metadata))

  test "tier declaration in the response metadata resolves true" do
    assert declarations(%{"effectively_once" => "true"}).effectively_once
  end

  test "metadata without the tier key resolves false" do
    refute declarations(%{"other" => "x"}).effectively_once
    refute declarations(%{"effectively_once" => "false"}).effectively_once
  end

  test "empty raw_data (pre-tier or Java runner) resolves the defaults" do
    for raw <- ["", nil] do
      assert ClassRegistry.decode_declarations(raw) == %{
               effectively_once: false,
               keyless: false,
               loop_mode: nil,
               checkpoint_ack: false,
               compensable_tasks: nil,
               leaf_tasks: nil,
               correction_window_ms: nil,
               compact_after_bytes: nil,
               sync_durability: false,
               persistent: true,
               sleep_when_idle: false
             }
    end
  end

  test "undecodable raw_data resolves the defaults" do
    assert ClassRegistry.decode_declarations(<<255, 255, 255, 1>>) == %{
             effectively_once: false,
             keyless: false,
             loop_mode: nil,
             checkpoint_ack: false,
             compensable_tasks: nil,
             leaf_tasks: nil,
             correction_window_ms: nil,
             compact_after_bytes: nil,
             sync_durability: false,
             persistent: true,
             sleep_when_idle: false
           }
  end

  test "a class persists and stays awake unless it says otherwise" do
    assert declarations(%{"other" => "x"}).persistent
    refute declarations(%{"persistent" => "false"}).persistent
    refute declarations(%{"other" => "x"}).sleep_when_idle
    assert declarations(%{"sleep_when_idle" => "true"}).sleep_when_idle
  end

  test "keyless is declared only when true — a keyed class says nothing" do
    # `partitioned-operatives`: the SDK announces `keyed: "false"` for a class
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
    assert declarations(%{"correction_window_ms" => "600000"}).correction_window_ms == 600_000
    # A class that declares nothing never corrects — the cost-free default.
    assert declarations(%{"other" => "x"}).correction_window_ms == nil
    assert declarations(%{"correction_window_ms" => "0"}).correction_window_ms == nil
    assert declarations(%{"correction_window_ms" => "nope"}).correction_window_ms == nil
  end

  test "checkpoint ack-granularity resolves from the metadata; anything else is per-input" do
    assert declarations(%{"ack_granularity" => "checkpoint"}).checkpoint_ack
    # Per-input is the tier's default granularity — an undeclaring class keeps it.
    refute declarations(%{"effectively_once" => "true"}).checkpoint_ack
    refute declarations(%{"ack_granularity" => "per_input"}).checkpoint_ack
  end

  test "synchronous remote durability resolves from the metadata; anything else is async" do
    assert declarations(%{"durability" => "sync_remote"}).sync_durability
    # Async is the cost-free default: a class that declares nothing never makes
    # a commit barrier wait on a remote store.
    refute declarations(%{"other" => "x"}).sync_durability
    refute declarations(%{"durability" => "async"}).sync_durability
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

  test "a workflow's loop mode resolves to an atom, and anything else to none" do
    assert declarations(%{"loop_mode" => "per_input"}).loop_mode == :per_input
    assert declarations(%{"loop_mode" => "once"}).loop_mode == :once
    assert declarations(%{"loop_mode" => "chained"}).loop_mode == :chained
    assert declarations(%{"loop_mode" => "sometimes"}).loop_mode == nil
    assert declarations(%{"other" => "x"}).loop_mode == nil
  end

  test "leaf positions resolve to a list — the tree's completion rule" do
    # A→(B,C): the operative is done when tasks 1 and 2 have committed.
    assert declarations(%{"leaf_tasks" => "1,2"}).leaf_tasks == [1, 2]
    # A linear chain's single terminal is the degenerate case.
    assert declarations(%{"leaf_tasks" => "1"}).leaf_tasks == [1]
    # Absent = a stream / an SDK that does not declare it — the
    # controller cannot derive completion and must not guess.
    assert declarations(%{"other" => "x"}).leaf_tasks == nil
  end

  defp encode(metadata) do
    {:ok, iodata, _size} =
      Protox.encode(%RunnerCommands.AddOperativeResponse{metadata: metadata})

    IO.iodata_to_binary(iodata)
  end
end

defmodule FlezhaTest.Runner.ControlUnitPingTimeoutTest do
  @moduledoc """
  The overwatch ping timeout is one knob on two layers, and the published
  contract names both: the `:overwatch_ping_timeout_ms` application key
  `Flezha.Runner.ControlUnit` reads, and the `FLEZHA_OVERWATCH_PING_TIMEOUT_MS`
  variable `config/runtime.exs` resolves it from at boot. An operator who sets
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
