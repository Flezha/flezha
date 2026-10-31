defmodule Flezha.State.InputLogFullDiskTest do
  @moduledoc """
  An input log whose disk refuses a write or a sync stays up, refuses every
  append it has not answered, and cuts their records from its file, so a
  retry is journaled as new and forwarded rather than absorbed by a record no
  one acknowledged. `TestHelpers.fill_disk/1` stands in for the full disk.
  """
  # async: false: suspends the shared `FsyncBatcher`.
  use FlezhaTest.StateCase, async: false

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [override_log_level: 1, wait_until: 2]

  alias Flezha.State.{FsyncBatcher, InputLog, Supervisor, TestHelpers}
  alias Flezha.State.InputLog.Syncer

  unless File.exists?("/dev/full"), do: @moduletag(skip: "needs /dev/full")

  setup %{base: base} do
    class = "il_full_" <> uniq()
    batcher = Process.whereis(FsyncBatcher)
    on_exit(override_log_level(:warning))

    on_exit(fn ->
      if Process.alive?(batcher), do: :sys.resume(batcher)
      TestHelpers.close_input_log(class)
      File.rm_rf!(base)
    end)

    {:ok, 0} = InputLog.append(class, "acked", %{"flz.msg_id" => "id-0"})
    {:ok, log} = Supervisor.lookup({class, :input_log})
    # No tick: an append's answer stays owed until the test resumes it.
    :ok = :sys.suspend(batcher)
    %{class: class, log: log, batcher: batcher}
  end

  defp owe(class, body, id),
    do: InputLog.append_async(class, body, %{"flz.msg_id" => id}, :gen_server.reqids_new(), id)

  defp bodies(class), do: class |> InputLog.read_range(0, 10) |> Enum.map(&elem(&1, 1))

  test "a refused write refuses the owed and the in-hand append, and their retries are new",
       %{class: class, log: log, batcher: batcher} do
    owed = owe(class, "owed", "id-1")
    assert :timeout = :gen_server.wait_response(owed, 0, false)
    TestHelpers.fill_disk(log)

    log_text =
      capture_log(fn ->
        assert {:error, {:input_log_unavailable, :enospc}} =
                 InputLog.append(class, "in hand", %{"flz.msg_id" => "id-2"})
      end)

    assert log_text =~ "the disk refused a write or a sync (:enospc)"
    assert log_text =~ "appends from offset 1 on are refused"

    # The owed answer is a refusal, not silence and not an exit, and the
    # record it named is gone from the file with the one in hand.
    assert {{:reply, {:error, {:input_log_unavailable, :enospc}}}, "id-1", _} =
             :gen_server.wait_response(owed, 1_000, true)

    assert bodies(class) == ["acked"]
    assert {:ok, ^log} = Supervisor.lookup({class, :input_log})

    # Still full: the next append is refused too, by the same log, and the
    # episode already warned.
    TestHelpers.fill_disk(log)

    refute capture_log(fn ->
             assert {:error, {:input_log_unavailable, :enospc}} =
                      InputLog.append(class, "in hand", %{"flz.msg_id" => "id-2"})
           end) =~ "the disk refused"

    # Space again: both retries are journaled as new at the offsets the cut
    # handed back, and the acknowledged input still absorbs its own retry.
    :ok = :sys.resume(batcher)

    assert capture_log(fn ->
             assert {:ok, 1} = InputLog.append(class, "owed", %{"flz.msg_id" => "id-1"})
           end) =~ "the disk takes writes again"

    assert {:ok, 2} = InputLog.append(class, "in hand", %{"flz.msg_id" => "id-2"})
    assert {:duplicate, 0} = InputLog.append(class, "acked", %{"flz.msg_id" => "id-0"})
    assert bodies(class) == ["acked", "owed", "in hand"]
    assert {:ok, ^log} = Supervisor.lookup({class, :input_log})
  end

  test "a refused sync refuses what it held, and the retry is new",
       %{class: class, log: log, batcher: batcher} do
    owed = owe(class, "owed", "id-1")
    assert :timeout = :gen_server.wait_response(owed, 0, false)
    syncer = :sys.get_state(log).syncer
    path = :sys.get_state(log).path
    :ok = Syncer.call(syncer, &{:reopen, "/dev/full", &1, &2})

    log_text =
      capture_log(fn ->
        :ok = :sys.resume(batcher)

        assert {{:reply, {:error, {:input_log_unavailable, :einval}}}, "id-1", _} =
                 :gen_server.wait_response(owed, 2_000, true)

        assert bodies(class) == ["acked"]
      end)

    assert log_text =~ "the disk refused a write or a sync (:einval)"
    assert log_text =~ "appends from offset 1 on are refused"

    :ok = Syncer.call(syncer, &{:reopen, path, &1, &2})
    assert {:ok, 1} = InputLog.append(class, "owed", %{"flz.msg_id" => "id-1"})
    assert {:duplicate, 0} = InputLog.append(class, "acked", %{"flz.msg_id" => "id-0"})
    assert {:ok, ^log} = Supervisor.lookup({class, :input_log})
  end

  test "a log that stops on a refused last sync cuts what it held",
       %{class: class, log: log, batcher: batcher} do
    owed = owe(class, "owed", "id-1")
    assert :timeout = :gen_server.wait_response(owed, 0, false)
    syncer = :sys.get_state(log).syncer
    :ok = Syncer.call(syncer, &{:reopen, "/dev/full", &1, &2})

    assert capture_log(fn -> :ok = GenServer.stop(log) end) =~
             "appends from offset 1 on are refused"

    assert {{:reply, {:error, {:input_log_unavailable, :einval}}}, "id-1", _} =
             :gen_server.wait_response(owed, 1_000, true)

    # The restarted log indexes only what the file kept: the retry is new.
    :ok = :sys.resume(batcher)
    assert wait_until(fn -> Supervisor.lookup({class, :input_log}) == :error end, 2_000)
    assert {:ok, 1} = InputLog.append(class, "owed", %{"flz.msg_id" => "id-1"})
  end

  test "a refused effect write is cut back and refused, and its retry is kept",
       %{class: class, log: log} do
    :kept = InputLog.keep_effect(class, 0, "first", "cell", "r0")

    :sys.replace_state(log, fn state ->
      {:ok, full} = :file.open("/dev/full", [:raw, :write, :binary])
      put_in(state.effects.file, full)
    end)

    assert {:error, {:input_log_unavailable, :enospc}} =
             InputLog.keep_effect(class, 0, "second", "cell", "r1")

    assert InputLog.effect_results(class, -1) == [{0, "first", "cell", "r0"}]
    assert :kept = InputLog.keep_effect(class, 0, "second", "cell", "r1")
    assert length(InputLog.effect_results(class, -1)) == 2
  end

  test "a refused effect sync cuts the effect back and refuses it", %{class: class, log: log} do
    :kept = InputLog.keep_effect(class, 0, "first", "cell", "r0")
    %{syncer: syncer, effects: %{path: path}} = :sys.get_state(log)
    :ok = Syncer.call(syncer, &{:reopen_effects, "/dev/full", &1, &2})

    assert capture_log(fn ->
             assert {:error, {:input_log_unavailable, :einval}} =
                      InputLog.keep_effect(class, 0, "second", "cell", "r1")
           end) =~ "the disk refused a write or a sync (:einval)"

    # Cut, not only refused: a retry is synced again rather than told kept.
    assert InputLog.effect_results(class, -1) == [{0, "first", "cell", "r0"}]
    :ok = Syncer.call(syncer, &{:reopen_effects, path, &1, &2})
    assert :kept = InputLog.keep_effect(class, 0, "second", "cell", "r1")
    assert length(InputLog.effect_results(class, -1)) == 2
  end
end
