defmodule Flezha.State.DeleteEntityTest do
  @moduledoc """
  dynamic entity mode Section 5: the agent-side hard-delete RPC.
  Covers `Flezha.State.Supervisor.delete_entity/1` — the target of both
  the SDK reaper's fire-and-forget `delete_entity_state` frame and the
  `flz state delete` CLI.
  """
  # async: false: writes Application env; a shared on-disk root; registered singletons.
  use ExUnit.Case, async: false

  alias Flezha.State.{Registry, Supervisor, TestHelpers, WalWriter}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    for child <- [
          {Elixir.Registry, keys: :unique, name: Supervisor.registry_name()},
          Supervisor,
          Flezha.State.FsyncBatcher
        ] do
      case start_supervised(child) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base, entity_id: TestHelpers.sample_entity_id("del_" <> uniq())}
  end

  test "deletes WAL + snapshot + outbox files and stops the writer", %{entity_id: pid} do
    # Materialise on-disk state via a live WalWriter.
    {:ok, writer} = Supervisor.find_or_start(pid)
    header = TestHelpers.stub_header()
    assert :ok = WalWriter.append(writer, 0, header, "a")
    assert :ok = WalWriter.append(writer, 1, header, "bb")
    WalWriter.fsync(writer)

    # Snapshot + outbox files so all three families are present.
    {:ok, _} = Flezha.State.Snapshot.write_snapshot(pid, 1, header, "snap")
    File.write!(Registry.outbox_dedup_path(pid), "dedup")

    assert File.exists?(Registry.wal_path(pid))
    assert Registry.snapshot_paths(pid) != []
    assert File.exists?(Registry.outbox_dedup_path(pid))

    assert {:ok, %{entity_id: ^pid, status: :ok}} = Supervisor.delete_entity(pid)

    # Writer stopped, every file gone.
    assert Supervisor.lookup(pid) == :error
    refute File.exists?(Registry.wal_path(pid))
    assert Registry.snapshot_paths(pid) == []
    refute File.exists?(Registry.outbox_dedup_path(pid))
  end

  test "deleting a never-seen entity is idempotent (returns :ok)", %{entity_id: pid} do
    assert Supervisor.lookup(pid) == :error
    assert {:ok, %{entity_id: ^pid, status: :ok}} = Supervisor.delete_entity(pid)
    # Still no writer, still no files — and no crash.
    assert Supervisor.lookup(pid) == :error
    refute File.exists?(Registry.wal_path(pid))
  end

  defp uniq, do: Integer.to_string(System.unique_integer([:positive]))
end
