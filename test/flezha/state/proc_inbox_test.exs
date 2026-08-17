defmodule FlezhaTest.State.ProcInboxTest do
  @moduledoc """
  `workflow-durable-suspension` controller store: append-before-forward with
  monotonic offsets, dedup inside the undelivered window, truncation at the
  consumed offset, torn-tail recovery, and invisibility to the Entity scanners.
  """
  use ExUnit.Case, async: false

  alias Flezha.State.{ProcInbox, ProcJournal, Registry, TestHelpers, WalWriter}

  setup do
    root = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(root)
    end)

    :ok
  end

  defp instance_id(), do: "inbox_inst_#{System.unique_integer([:positive])}"

  defp close_inbox_writer(inst) do
    {:ok, pid} = Flezha.State.Supervisor.find_or_start(Registry.inbox_id(inst))
    WalWriter.close(pid)
  end

  test "inbox path lives with the instance's state files" do
    inst = instance_id()

    assert Registry.inbox_path(inst) ==
             Path.join([Registry.state_root(), "_system", inst, inst, "inbox.wal"])

    assert Registry.wal_path(Registry.inbox_id(inst)) == Registry.inbox_path(inst)
  end

  test "appends carry monotonic offsets and round-trip message + metadata" do
    inst = instance_id()

    assert {:ok, 1} = ProcInbox.append(inst, "one", %{"channel" => "chat"})
    assert {:ok, 2} = ProcInbox.append(inst, "two", %{"channel" => "chat"})

    assert [first, second] = ProcInbox.read(inst)
    assert %{offset: 1, message: "one", metadata: %{"channel" => "chat"}} = first
    assert %{offset: 2, message: "two"} = second

    assert ProcInbox.last_offset(inst) == 2
  end

  test "a duplicate in the undelivered window is absorbed, not delivered twice" do
    inst = instance_id()
    meta = %{"flz.msg_id" => "m-1"}

    assert {:ok, 1} = ProcInbox.append(inst, "hello", meta)
    assert {:duplicate, 1} = ProcInbox.append(inst, "hello", meta)
    # A re-send with the same id but different bytes is still the same message.
    assert {:duplicate, 1} = ProcInbox.append(inst, "hello (edited)", meta)

    assert length(ProcInbox.read(inst)) == 1

    # Without a sender id, the payload hash is the dedup handle.
    assert {:ok, 2} = ProcInbox.append(inst, "anon", %{})
    assert {:duplicate, 2} = ProcInbox.append(inst, "anon", %{})
    # A different payload is a different message.
    assert {:ok, 3} = ProcInbox.append(inst, "anon-2", %{})
  end

  test "the undelivered window is bounded by the consumed offset" do
    inst = instance_id()

    {:ok, 1} = ProcInbox.append(inst, "a", %{})
    {:ok, 2} = ProcInbox.append(inst, "b", %{})
    {:ok, 3} = ProcInbox.append(inst, "c", %{})

    assert Enum.map(ProcInbox.undelivered(inst), & &1.offset) == [1, 2, 3]

    # Iteration 0 commits, having consumed through offset 2.
    :ok = ProcJournal.append_iter_committed(inst, 0, 2, [])

    assert Enum.map(ProcInbox.undelivered(inst), & &1.offset) == [3]

    # A payload identical to a CONSUMED one is a new message: the dedup window
    # only covers what the operative has not seen yet.
    assert {:ok, 4} = ProcInbox.append(inst, "a", %{})
  end

  test "truncation releases consumed entries and preserves the offsets above it" do
    inst = instance_id()

    for n <- 1..4, do: {:ok, ^n} = ProcInbox.append(inst, "m#{n}", %{})

    :ok = ProcJournal.append_iter_committed(inst, 0, 2, [])
    :ok = ProcInbox.truncate(inst, 2)

    assert Enum.map(ProcInbox.read(inst), & &1.offset) == [3, 4],
           "offsets survive truncation — they are the durable delivery cursor"

    assert Enum.map(ProcInbox.undelivered(inst), & &1.offset) == [3, 4]

    # The next append continues the sequence rather than reusing a released
    # offset, so a consumed-offset filter can never mistake it for old input.
    assert {:ok, 5} = ProcInbox.append(inst, "m5", %{})

    # Idempotent, and a no-op on an instance that never received input.
    assert :ok = ProcInbox.truncate(inst, 2)
    assert :ok = ProcInbox.truncate(instance_id(), 9)
  end

  test "torn tail is discarded; complete prior entries remain readable" do
    inst = instance_id()
    {:ok, 1} = ProcInbox.append(inst, "kept", %{})

    close_inbox_writer(inst)
    File.write!(Registry.inbox_path(inst), <<7::64-big, 99::32-big, 1, 2, 3>>, [:append])

    assert [%{offset: 1, message: "kept"}] = ProcInbox.read(inst)

    # A re-opened writer truncates the torn tail and appends contiguously.
    assert {:ok, 2} = ProcInbox.append(inst, "next", %{})
    assert Enum.map(ProcInbox.read(inst), & &1.offset) == [1, 2]
  end

  test "the inbox never registers a phantom Entity" do
    inst = instance_id()
    {:ok, 1} = ProcInbox.append(inst, "x", %{})

    assert Registry.scan_dir({inst, inst, ""}).wal_files == [],
           "inbox.wal rides the Entity substrate but is not an Entity"

    assert Registry.scan_dir({inst, inst, ""}).outbox_files == []
    refute Enum.any?(Registry.all_entities(), fn {rc, _, _} -> rc == inst end)
  end

  test "the inbox is reclaimed with the instance's state" do
    inst = instance_id()
    {:ok, 1} = ProcInbox.append(inst, "x", %{})
    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [], [])

    # The inbox lives inside the instance's own state dir, beside `proc.journal` —
    # so every reclaimer of that tree (instance teardown, retention sweep,
    # `flz reset`) reclaims the inbox with it, and the reserved identity is
    # deletable through the same path an Entity is.
    assert Path.dirname(Registry.inbox_path(inst)) == Registry.state_dir(Registry.inbox_id(inst))
    assert Path.dirname(Registry.inbox_path(inst)) == Path.dirname(Registry.journal_path(inst))

    {:ok, %{status: :ok}} = Flezha.State.Supervisor.delete_entity(Registry.inbox_id(inst))

    refute File.exists?(Registry.inbox_path(inst))
    assert ProcInbox.read(inst) == []
  end
end
