defmodule Flezha.State.RecoveryDispatchTest do
  @moduledoc """
  `snapshot-baseline-durability` D3: the resync dispatch path never ships a
  `RestoreInitState` whose assembled payload is empty — an Entity with no
  snapshot and no WAL records pulls a `CompactEntity` baseline instead, so
  the live runner's accumulated state is never rebuilt to `init_state`.
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

    stub(Flezha.Runner.ControlChannel, :send_control, fn runner_id, {tag, _cmd} ->
      send(parent, {:control_sent, runner_id, tag})
      {:ok, make_ref()}
    end)

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{header: TestHelpers.stub_header()}
  end

  test "no persisted state resyncs via CompactEntity pull, never an empty RestoreInitState" do
    entity_id = TestHelpers.sample_entity_id("nodata_" <> uniq())
    # The writer's lazily-created zero-byte WAL is not persisted state.
    {:ok, _} = Supervisor.find_or_start(entity_id)

    refute Restore.has_persisted_state?(entity_id)

    assert {:ok, _ref} = Recovery.resync(entity_id, runner_id: "exec-1", reason: :seq_gap)

    assert_receive {:control_sent, "exec-1", :compact_entity}, 1_000
    refute_receive {:control_sent, _, :restore_init_state}, 200
  end

  test "persisted WAL records resync via RestoreInitState push", %{header: header} do
    entity_id = TestHelpers.sample_entity_id("data_" <> uniq())
    {:ok, _} = Supervisor.find_or_start(entity_id)
    {:ok, writer} = Supervisor.lookup(entity_id)
    :ok = WalWriter.append(writer, 0, header, "x")

    assert Restore.has_persisted_state?(entity_id)

    assert {:ok, _ref} = Recovery.resync(entity_id, runner_id: "exec-1", reason: :seq_gap)

    assert_receive {:control_sent, "exec-1", :restore_init_state}, 1_000
    refute_receive {:control_sent, _, :compact_entity}, 200
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end
