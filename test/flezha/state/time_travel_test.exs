defmodule Flezha.State.TimeTravelTest do
  use ExUnit.Case, async: false

  alias Flezha.State.{FsyncBatcher, Snapshot, Supervisor, TestHelpers, TimeTravel, WalWriter}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    case start_supervised({Elixir.Registry, keys: :unique, name: Supervisor.registry_name()}) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(Supervisor) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(FsyncBatcher) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base, segment_id: TestHelpers.sample_segment_id("tt_" <> uniq())}
  end

  test "no_data error when segment has no persisted state", %{segment_id: pid} do
    assert {:error, %{kind: :no_data}} = TimeTravel.query(pid, {:seq, 100})
  end

  test "seq query returns the right slice", %{segment_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, srv} = Supervisor.lookup(pid)

    for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "v" <> Integer.to_string(n))
    {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "snap@2")

    assert {:ok, result} = TimeTravel.query(pid, {:seq, 4})
    assert result.snapshot_bytes == "snap@2"
    assert result.terminal_seq == 4

    expected =
      WalWriter.encode_record(3, header, "v3") <>
        WalWriter.encode_record(4, header, "v4")

    assert result.change_log_bytes == expected
  end

  test "outside_retention error for seq before earliest snapshot", %{segment_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 100, header, "snap@100")

    assert {:error, %{kind: :outside_retention, earliest_available: 100}} =
             TimeTravel.query(pid, {:seq, 50})
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end
