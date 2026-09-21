defmodule FlezhaTest.State.TaskInboxTest do
  @moduledoc """
  `workflow-durable-suspension` controller store: append-before-forward with
  monotonic offsets, dedup inside the undelivered window, truncation at the
  consumed offset, torn-tail recovery, and invisibility to the Cell scanners.
  """
  # async: false: writes Application env; a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.State.{Paths, TaskInbox, TaskJournal, TestHelpers, WalWriter}
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    # Every release rewrites the file at once, so the cases can read it.
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 0))

    :ok
  end

  defp instance_id(), do: "inbox_inst_#{System.unique_integer([:positive])}"

  # `WalWriter.close/1` returns before the writer has finished exiting, and the
  # next append resolves the writer through the registry — so without waiting
  # for it to be gone the append reaches a dying pid and exits with `:noproc`.
  defp close_inbox_writer(inst) do
    id = Paths.inbox_id(inst)
    {:ok, pid} = Flezha.State.Supervisor.find_or_start(id)
    ref = Process.monitor(pid)
    WalWriter.close(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    after
      5_000 -> flunk("the inbox writer never closed")
    end

    assert NodeTestHelper.wait_until(
             fn -> Flezha.State.Supervisor.lookup(id) == :error end,
             5_000
           ),
           "the closed inbox writer stayed in the registry"
  end

  test "inbox path lives with the instance's state files" do
    inst = instance_id()

    assert Paths.inbox_path(inst) ==
             Path.join([Paths.state_root(), "_system", inst, inst, "inbox.wal"])

    assert Paths.wal_path(Paths.inbox_id(inst)) == Paths.inbox_path(inst)
  end

  test "appends carry monotonic offsets and round-trip message + metadata" do
    inst = instance_id()

    assert {:ok, 1} = TaskInbox.append(inst, "one", %{"channel" => "chat"})
    assert {:ok, 2} = TaskInbox.append(inst, "two", %{"channel" => "chat"})

    assert [first, second] = TaskInbox.read(inst)
    assert %{offset: 1, message: "one", metadata: %{"channel" => "chat"}} = first
    assert %{offset: 2, message: "two"} = second

    assert TaskInbox.last_offset(inst) == 2
  end

  test "a duplicate in the undelivered window is absorbed, not delivered twice" do
    inst = instance_id()
    meta = %{"flz.msg_id" => "m-1"}

    assert {:ok, 1} = TaskInbox.append(inst, "hello", meta)
    assert {:duplicate, 1} = TaskInbox.append(inst, "hello", meta)
    # A re-send with the same id but different bytes is still the same message.
    assert {:duplicate, 1} = TaskInbox.append(inst, "hello (edited)", meta)

    assert length(TaskInbox.read(inst)) == 1

    # Without a sender id there is nothing to match: two identical payloads
    # are two messages, as they are on a stream.
    assert {:ok, 2} = TaskInbox.append(inst, "anon", %{})
    assert {:ok, 3} = TaskInbox.append(inst, "anon", %{})
    assert {:ok, 4} = TaskInbox.append(inst, "anon", %{"flz.msg_id" => ""})
    assert length(TaskInbox.read(inst)) == 4
  end

  test "the undelivered window is bounded by the consumed offset" do
    inst = instance_id()

    {:ok, 1} = TaskInbox.append(inst, "a", %{"flz.msg_id" => "a"})
    {:ok, 2} = TaskInbox.append(inst, "b", %{"flz.msg_id" => "b"})
    {:ok, 3} = TaskInbox.append(inst, "c", %{"flz.msg_id" => "c"})

    assert Enum.map(TaskInbox.undelivered(inst), & &1.offset) == [1, 2, 3]

    # Iteration 0 commits, having consumed through offset 2, and releases it.
    :ok = TaskJournal.append_iter_committed(inst, 0, 2, [])
    assert Enum.map(TaskInbox.undelivered(inst), & &1.offset) == [3]
    :ok = TaskInbox.truncate(inst, 2)

    # An id identical to a CONSUMED one is a new message: the dedup window
    # only covers what the operative has not seen yet.
    assert {:ok, 4} = TaskInbox.append(inst, "a", %{"flz.msg_id" => "a"})
    # … while an undelivered one is still absorbed.
    assert {:duplicate, 3} = TaskInbox.append(inst, "c", %{"flz.msg_id" => "c"})
    assert Enum.map(TaskInbox.undelivered(inst), & &1.offset) == [3, 4]
  end

  test "a message the run parked unread is released by its park record" do
    inst = instance_id()
    {:ok, 1} = TaskInbox.append(inst, "not json", %{})
    {:ok, 2} = TaskInbox.append(inst, "sent after the park", %{})

    # The run dead-lettered offset 1 without a pass, then suspended.
    :ok = TaskJournal.append_parked("default", inst, 0, nil, 1)

    assert Enum.map(TaskInbox.undelivered(inst), & &1.offset) == [2]
    assert TaskJournal.restore_state(inst).parked
  end

  test "a release below the cut threshold defers the rewrite but not the window" do
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 65_536))
    inst = instance_id()

    for n <- 1..3, do: {:ok, ^n} = TaskInbox.append(inst, "m#{n}", %{})
    :ok = TaskInbox.truncate(inst, 2)

    assert Enum.map(TaskInbox.read(inst), & &1.offset) == [1, 2, 3],
           "a few hundred bytes are not worth an fsync'd rewrite yet"

    # Released is released: a consumed id is a new message; an unreleased one
    # is still in the window.
    for n <- 1..3, do: {:ok, _} = TaskInbox.append(inst, "k#{n}", %{"flz.msg_id" => "k#{n}"})
    :ok = TaskInbox.truncate(inst, 5)
    assert {:ok, 7} = TaskInbox.append(inst, "k1", %{"flz.msg_id" => "k1"})
    assert {:duplicate, 6} = TaskInbox.append(inst, "k3", %{"flz.msg_id" => "k3"})
  end

  test "a duplicate is absorbed across a writer restart" do
    inst = instance_id()
    meta = %{"flz.msg_id" => "m-9"}

    assert {:ok, 1} = TaskInbox.append(inst, "hello", meta)
    close_inbox_writer(inst)

    # The reopened writer rebuilds its window from the file — no journal fold,
    # no second copy of the fact.
    assert {:duplicate, 1} = TaskInbox.append(inst, "hello", meta)
    assert {:ok, 2} = TaskInbox.append(inst, "other", %{})
  end

  test "the ingest's append answers once the batch has synced, in seq space" do
    inst = instance_id()

    meta = %{"flz.msg_id" => "one"}
    reqids = TaskInbox.append_async(inst, "one", meta, :gen_server.reqids_new(), :one)
    reqids = TaskInbox.append_async(inst, "one", meta, reqids, :dup)

    assert {{:reply, {:ok, seq}}, :one, reqids} = :gen_server.wait_response(reqids, 2_000, true)
    assert TaskInbox.offset(seq) == 1

    assert {{:reply, {:duplicate, ^seq}}, :dup, _} =
             :gen_server.wait_response(reqids, 2_000, true)

    assert [%{offset: 1, message: "one"}] = TaskInbox.read(inst)
  end

  test "truncation releases consumed entries and preserves the offsets above it" do
    inst = instance_id()

    for n <- 1..4, do: {:ok, ^n} = TaskInbox.append(inst, "m#{n}", %{})

    :ok = TaskJournal.append_iter_committed(inst, 0, 2, [])
    :ok = TaskInbox.truncate(inst, 2)

    # The cut is the writer's to run, behind the call.
    assert wait_until(fn -> Enum.map(TaskInbox.read(inst), & &1.offset) == [3, 4] end, 2_000),
           "offsets survive truncation — they are the durable delivery cursor"

    assert Enum.map(TaskInbox.undelivered(inst), & &1.offset) == [3, 4]

    # The next append continues the sequence rather than reusing a released
    # offset, so a consumed-offset filter can never mistake it for old input.
    assert {:ok, 5} = TaskInbox.append(inst, "m5", %{})

    # Idempotent, and a no-op on an instance that never received input.
    assert :ok = TaskInbox.truncate(inst, 2)
    assert :ok = TaskInbox.truncate(instance_id(), 9)
  end

  test "torn tail is discarded; complete prior entries remain readable" do
    inst = instance_id()
    {:ok, 1} = TaskInbox.append(inst, "kept", %{})

    close_inbox_writer(inst)
    File.write!(Paths.inbox_path(inst), <<7::64-big, 99::32-big, 1, 2, 3>>, [:append])

    assert [%{offset: 1, message: "kept"}] = TaskInbox.read(inst)

    # A re-opened writer truncates the torn tail and appends contiguously.
    assert {:ok, 2} = TaskInbox.append(inst, "next", %{})
    assert Enum.map(TaskInbox.read(inst), & &1.offset) == [1, 2]
  end

  test "the inbox never registers a phantom Cell" do
    inst = instance_id()
    {:ok, 1} = TaskInbox.append(inst, "x", %{})

    assert Paths.scan_dir({inst, inst, ""}).wal_files == [],
           "inbox.wal rides the Cell substrate but is not a Cell"

    assert Paths.scan_dir({inst, inst, ""}).outbox_files == []
    refute Enum.any?(Paths.all_cells(), fn {rc, _, _} -> rc == inst end)
  end

  test "the inbox is reclaimed with the instance's state" do
    inst = instance_id()
    {:ok, 1} = TaskInbox.append(inst, "x", %{})
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [])

    # The inbox lives inside the instance's own state dir, beside `task.journal` —
    # so every reclaimer of that tree (instance teardown, retention sweep,
    # `flz reset`) reclaims the inbox with it, and the reserved identity is
    # deletable through the same path a Cell is.
    assert Path.dirname(Paths.inbox_path(inst)) == Paths.state_dir(Paths.inbox_id(inst))
    assert Path.dirname(Paths.inbox_path(inst)) == Path.dirname(Paths.journal_path(inst))

    {:ok, %{status: :ok}} = Flezha.State.Supervisor.delete_cell(Paths.inbox_id(inst))

    refute File.exists?(Paths.inbox_path(inst))
    assert TaskInbox.read(inst) == []
  end
end
