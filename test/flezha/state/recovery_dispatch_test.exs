defmodule Flezha.State.RecoveryDispatchTest do
  @moduledoc """
  `snapshot-baseline-durability` D3: the resync dispatch path never ships a
  `RestoreInitState` whose assembled payload is empty — a Segment with no
  snapshot and no WAL records pulls a `CompactSegment` baseline instead, so
  the live executor's accumulated state is never rebuilt to `init_state`.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.State.{Recovery, Restore, Supervisor, TestHelpers, WalWriter}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    for spec <- [
          {Elixir.Registry, keys: :unique, name: Supervisor.registry_name()},
          Supervisor,
          Flezha.State.FsyncBatcher
        ] do
      case start_supervised(spec) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end

    parent = self()

    stub(Flezha.Executor.ControlChannel, :send_control, fn executor_id, {tag, _cmd} ->
      send(parent, {:control_sent, executor_id, tag})
      {:ok, make_ref()}
    end)

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{header: TestHelpers.stub_header()}
  end

  test "no persisted state resyncs via CompactSegment pull, never an empty RestoreInitState" do
    segment_id = TestHelpers.sample_segment_id("nodata_" <> uniq())
    # The writer's lazily-created zero-byte WAL is not persisted state.
    {:ok, _} = Supervisor.find_or_start(segment_id)

    refute Restore.has_persisted_state?(segment_id)

    assert {:ok, _ref} = Recovery.resync(segment_id, executor_id: "exec-1", reason: :seq_gap)

    assert_receive {:control_sent, "exec-1", :compact_segment}, 1_000
    refute_receive {:control_sent, _, :restore_init_state}, 200
  end

  test "persisted WAL records resync via RestoreInitState push", %{header: header} do
    segment_id = TestHelpers.sample_segment_id("data_" <> uniq())
    {:ok, _} = Supervisor.find_or_start(segment_id)
    {:ok, writer} = Supervisor.lookup(segment_id)
    :ok = WalWriter.append(writer, 0, header, "x")

    assert Restore.has_persisted_state?(segment_id)

    assert {:ok, _ref} = Recovery.resync(segment_id, executor_id: "exec-1", reason: :seq_gap)

    assert_receive {:control_sent, "exec-1", :restore_init_state}, 1_000
    refute_receive {:control_sent, _, :compact_segment}, 200
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end
