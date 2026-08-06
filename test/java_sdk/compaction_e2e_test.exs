defmodule FlezhaTest.JavaSdk.CompactionE2ETest do
  @moduledoc """
  Java arm of the compaction end-to-end proof (Rust arm:
  `test/system/compaction_e2e_test.exs`). Boots the real committed
  `java_runner.jar`, drives a changeful `store_input` operative's WAL, forces a
  Watchdog compaction sweep, and asserts a snapshot FILE appears and the WAL
  shrinks — the same observable-durable-state check, against the Java SDK's
  `CompactEntityHandler`.

  Before this change `test/java_sdk/` had zero compaction coverage and the Java
  handler returned `new byte[0]` (no snapshot). This FAILS against that stub.

  Tagged `:java_sdk` — needs the committed jar
  (`test/apps/java_runner/dist/java_runner.jar`, refreshed by
  `scripts/build-dist.sh`). Run via `mix test.java_sdk` or
  `mix test --include java_sdk test/java_sdk/compaction_e2e_test.exs`.
  """

  use ExUnit.Case, async: false

  alias FlezhaTest.Helper.CompactionAssertions, as: CA
  alias FlezhaTest.Helper.NodeTestHelper
  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager

  @moduletag :java_sdk
  @namespace "compaction_java_e2e"
  @factory "store_input"
  @procedure "store_input_procedure"

  setup_all do
    jar = Path.join(File.cwd!(), "test/apps/java_runner/dist/java_runner.jar")

    if File.exists?(jar) do
      :ok
    else
      {:skip, "committed java_runner.jar missing — run scripts/build-dist.sh"}
    end
  end

  setup do
    prev_age = Application.get_env(:flezha, :compact_after_age_ms)
    prev_timeout = Application.get_env(:flezha, :compaction_timeout_ms)

    on_exit(fn ->
      Application.put_env(:flezha, :compact_after_age_ms, prev_age)
      Application.put_env(:flezha, :compaction_timeout_ms, prev_timeout)

      try do
        OperativeManager.stop_all_operatives(@namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  test "a triggered compaction produces a durable snapshot file and shrinks the WAL (java)" do
    {port, ce_id} = NodeTestHelper.start_runner_port(@namespace, runtime: :java)
    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    operative_id = "compaction_java_#{System.unique_integer([:positive])}"
    entity_id = {operative_id, @procedure, ""}

    builder = %Builder{
      id: operative_id,
      factory_id: @factory,
      init_values: %{},
      opts: [snapshot_interval: 0]
    }

    assert {:ok, info} = OperativeManager.start_operative(@namespace, builder)
    assert info.runner_id == ce_id

    for i <- 1..50 do
      OperativeManager.send_io(operative_id, @namespace, "line-#{i}-#{operative_id}", %{
        "input_type" => "2"
      })
    end

    wal_before = CA.wait_for_wal_bytes(entity_id, 5_000)
    assert wal_before > 0, "precondition: the Entity's WAL must hold records before compaction"
    assert CA.snapshot_files(entity_id) == [], "precondition: no snapshot file exists yet"

    parent = self()
    done_id = {__MODULE__, :compaction_completed, System.unique_integer([:positive])}

    :telemetry.attach(
      done_id,
      [:flezha, :state, :compaction_completed],
      fn _e, _meas, meta, _ -> send(parent, {:compaction_completed, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(done_id) end)

    Application.put_env(:flezha, :compact_after_age_ms, 1)
    Application.put_env(:flezha, :compaction_timeout_ms, 30_000)
    Process.sleep(50)
    :ok = Flezha.State.Watchdog.sweep_now()

    assert_receive {:compaction_completed, %{operative: ^operative_id, procedure: @procedure}},
                   15_000,
                   "the Java SDK never produced a snapshot (the old stub returned new byte[0])"

    assert CA.wait_for_snapshot_file(entity_id, 10_000),
           "no snapshot file was written under the Entity's state dir"

    wal_after = CA.wait_for_wal_shrink(entity_id, wal_before, 10_000)

    assert wal_after < wal_before,
           "WAL did not shrink after compaction (before=#{wal_before}, after=#{wal_after})"
  end
end
