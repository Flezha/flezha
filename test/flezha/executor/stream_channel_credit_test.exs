defmodule FlezhaTest.Executor.StreamChannelCreditTest do
  @moduledoc """
  Credit-grant flow control on the executor→controller state stream:
  negotiation (offer/ack/nack/kill-switch), per-stream byte ledger,
  fsync-minted watermark-batched grants, overrun shedding, reconnect
  reset, and per-Segment receiver isolation.
  """

  use ExUnit.Case, async: false
  use Mimic

  import FlezhaTest.Helper.CreditTestHelper
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Executor.StreamChannel
  alias Flezha.Service.Identificator
  alias Flezha.State.TestHelpers

  setup :set_mimic_global

  # Stub SDK control unit: answers the controller's CreditWindowInit offer
  # the way a credit-speaking (:ack), broken (:wrong_session) or pre-credit
  # (:no_reply) SDK would.
  defmodule StubControlUnit do
    use GenServer

    def start_link({name, mode, notify}),
      do: GenServer.start_link(__MODULE__, {mode, notify}, name: name)

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:send_control, {:credit_window_init, init}}, _from, {mode, notify} = state) do
      send(notify, {:init_offered, init})

      case mode do
        :no_reply ->
          {:reply, {:error, :timeout}, state}

        mode ->
          session = if mode == :ack, do: init.stream_id, else: "stale-session"
          ack = %ExecutorCommands.CreditWindowAck{stream_id: session}
          {:ok, iodata, _} = Protox.encode(ack)
          {:reply, {:ok, IO.iodata_to_binary(iodata)}, state}
      end
    end
  end

  setup do
    base = TestHelpers.tmp_app_data_dir()

    # Keep fsync under test control: the real batcher never learns about the
    # writers, so credit is minted only by explicit WalWriter.fsync/1 calls.
    stub(Flezha.State.FsyncBatcher, :mark_dirty, fn _segment_id -> :ok end)

    # Capture every controller→SDK async control send (grants, mailbox status).
    test_pid = self()

    stub(Flezha.Executor.ControlChannel, :send_control_async, fn executor_id, command ->
      send(test_pid, {:ctl_async, executor_id, command})
      :ok
    end)

    on_exit(fn ->
      TestHelpers.reset_all_segments()
      Application.delete_env(:flezha, :state_root)
      Application.delete_env(:flezha, :credit_window_bytes)
      Application.delete_env(:flezha, :credit_flow_control_enabled)
      File.rm_rf!(base)
    end)

    uniq = System.unique_integer([:positive])
    executor_id = "credit_exec_#{uniq}"
    class = "credit_class_#{uniq}"
    name = :"sc_credit_#{uniq}"

    {:ok, channel} = StreamChannel.start_link({executor_id, name, :secondary})
    on_exit(fn -> if Process.alive?(channel), do: GenServer.stop(channel) end)

    %{channel: channel, executor_id: executor_id, class: class, name: name}
  end

  defp start_stub_control_unit(executor_id, mode) do
    name = Identificator.control_unit(executor_id)
    {:ok, pid} = StubControlUnit.start_link({name, mode, self()})
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    pid
  end

  defp push_frame(channel, frame) do
    send(channel, {:flz_data, :ref, "segment_state_change", frame, %{}})
    # Flush through the channel's mailbox.
    _ = GenServer.call(channel, :stream_ref_status)
    :ok
  end

  defp await(fun, timeout_ms \\ 2_000) do
    assert wait_until(fun, timeout_ms), "condition not met within #{timeout_ms}ms"
  end

  defp writer_pid(segment_id), do: wait_for_writer(segment_id, 2_000)

  defp establish_credit(ctx) do
    start_stub_control_unit(ctx.executor_id, :ack)
    :ok = push_frame(ctx.channel, state_change_frame(ctx.class, "k1", 0, "handshake-trigger"))
    assert_receive {:init_offered, init}, 2_000
    await(fn -> counters_row(ctx.executor_id, ctx.name).credit_active == 1 end)
    init
  end

  describe "negotiation (1.2)" do
    test "ack marks the session credit-speaking and the window resolves per class", ctx do
      Application.put_env(:flezha, :credit_window_bytes, %{ctx.class => 4_096})

      init = establish_credit(ctx)
      assert init.window_bytes == 4_096
      assert init.low_watermark_bytes == 2_048
      assert counters_row(ctx.executor_id, ctx.name).credit_ledger_level == 4_096
    end

    test "window defaults to wal_high_watermark_bytes", ctx do
      init = establish_credit(ctx)
      assert init.window_bytes == 16 * 1024 * 1024
    end

    test "no ack (pre-credit SDK) pins the session to the legacy path", ctx do
      start_stub_control_unit(ctx.executor_id, :no_reply)

      :ok = push_frame(ctx.channel, state_change_frame(ctx.class, "k1", 0, "one"))
      assert_receive {:init_offered, _init}, 2_000

      # The frame itself flowed to the WAL un-metered.
      writer = writer_pid({ctx.class, "op1", "k1"})
      await(fn -> Flezha.State.WalWriter.last_persisted_seq(writer) == 0 end)

      # Follow-up frames do not re-offer and stay legacy.
      :ok = push_frame(ctx.channel, state_change_frame(ctx.class, "k1", 1, "two"))
      refute_receive {:init_offered, _}, 200
      assert counters_row(ctx.executor_id, ctx.name).credit_active == 0
      await(fn -> Flezha.State.WalWriter.last_persisted_seq(writer) == 1 end)
    end

    test "a session-mismatched ack is treated as a nack", ctx do
      start_stub_control_unit(ctx.executor_id, :wrong_session)

      :ok = push_frame(ctx.channel, state_change_frame(ctx.class, "k1", 0, "one"))
      assert_receive {:init_offered, _init}, 2_000

      writer = writer_pid({ctx.class, "op1", "k1"})
      await(fn -> Flezha.State.WalWriter.last_persisted_seq(writer) == 0 end)
      assert counters_row(ctx.executor_id, ctx.name).credit_active == 0
    end

    test "kill switch disables the offer entirely", ctx do
      Application.put_env(:flezha, :credit_flow_control_enabled, false)
      start_stub_control_unit(ctx.executor_id, :ack)

      :ok = push_frame(ctx.channel, state_change_frame(ctx.class, "k1", 0, "one"))

      writer = writer_pid({ctx.class, "op1", "k1"})
      await(fn -> Flezha.State.WalWriter.last_persisted_seq(writer) == 0 end)
      refute_receive {:init_offered, _}, 200
      assert counters_row(ctx.executor_id, ctx.name).credit_active == 0
    end
  end

  describe "ledger, fsync-minted batched grants (2.1–2.3)" do
    test "receipt debits the ledger; fsync mints ONE batched grant; stalled fsync mints nothing",
         ctx do
      establish_credit(ctx)

      # The handshake trigger frame consumed seq 0 (un-metered); metered
      # frames continue contiguously from 1.
      f1 = state_change_frame(ctx.class, "k1", 1, String.duplicate("a", 100))
      f2 = state_change_frame(ctx.class, "k1", 2, String.duplicate("b", 100))
      f3 = state_change_frame(ctx.class, "k1", 3, String.duplicate("c", 100))
      total = byte_size(f1) + byte_size(f2) + byte_size(f3)

      :ok = push_frame(ctx.channel, f1)
      :ok = push_frame(ctx.channel, f2)
      :ok = push_frame(ctx.channel, f3)

      writer = writer_pid({ctx.class, "op1", "k1"})
      await(fn -> Flezha.State.WalWriter.last_persisted_seq(writer) == 3 end)

      # Ledger debited by the wire size of every frame.
      row = counters_row(ctx.executor_id, ctx.name)
      assert row.credit_ledger_level == 16 * 1024 * 1024 - total

      # Stalled fsync (the batcher is stubbed out): nothing is minted.
      refute_receive {:ctl_async, _, {:credit_grant, _}}, 300

      # One explicit fsync covering all three records mints exactly ONE
      # batched grant for the full freed byte count — never per record.
      :ok = Flezha.State.WalWriter.fsync(writer)

      assert_receive {:ctl_async, executor_id,
                      {:credit_grant, %ExecutorCommands.CreditGrant{bytes: bytes}}},
                     2_000

      assert executor_id == ctx.executor_id
      assert bytes == total
      refute_receive {:ctl_async, _, {:credit_grant, _}}, 200

      await(fn ->
        counters_row(ctx.executor_id, ctx.name).credit_ledger_level == 16 * 1024 * 1024
      end)
    end

    test "a conformant sender staying inside the window never sheds", ctx do
      Application.put_env(:flezha, :credit_window_bytes, %{ctx.class => 2_048})
      establish_credit(ctx)

      writer = writer_pid({ctx.class, "op1", "k1"})

      # 20 frames of ~170B against a 2 KiB window, draining (fsync) every
      # 5 frames — the way a sender honouring its grants behaves.
      for batch <- 0..3 do
        for i <- 1..5 do
          seq = batch * 5 + i

          :ok =
            push_frame(
              ctx.channel,
              state_change_frame(ctx.class, "k1", seq, String.duplicate("x", 100))
            )
        end

        await(fn -> Flezha.State.WalWriter.last_persisted_seq(writer) == batch * 5 + 5 end)
        :ok = Flezha.State.WalWriter.fsync(writer)
        assert_receive {:ctl_async, _, {:credit_grant, _}}, 2_000
      end

      row = counters_row(ctx.executor_id, ctx.name)
      assert row.credit_overrun_total == 0

      metrics = Flezha.State.Metrics.for_segment({ctx.class, "op1", "k1"})
      assert Flezha.State.Metrics.get(metrics, :wal_shed_total) == 0
      assert Flezha.State.WalWriter.last_persisted_seq(writer) == 20
    end
  end

  describe "overrun is a counted anomaly (2.5)" do
    test "a frame past the exhausted ledger is shed, counted, and repaired by seq-gap resync",
         ctx do
      Application.put_env(:flezha, :credit_window_bytes, %{ctx.class => 200})

      resync_notify = self()

      Application.put_env(:flezha, :state_resync_fun, fn segment_id, opts ->
        send(resync_notify, {:resync_called, segment_id, opts})
        :ok
      end)

      on_exit(fn -> Application.delete_env(:flezha, :state_resync_fun) end)

      establish_credit(ctx)

      f1 = state_change_frame(ctx.class, "k1", 1, String.duplicate("a", 150))
      f2 = state_change_frame(ctx.class, "k1", 2, String.duplicate("b", 150))
      f3 = state_change_frame(ctx.class, "k1", 3, String.duplicate("c", 150))

      # f1: un_fsynced == 0, admitted even though it exceeds the tiny window
      # (single-frame overshoot). f2: overrun — shed at the ledger.
      :ok = push_frame(ctx.channel, f1)
      :ok = push_frame(ctx.channel, f2)

      writer = writer_pid({ctx.class, "op1", "k1"})
      await(fn -> Flezha.State.WalWriter.last_persisted_seq(writer) == 1 end)
      assert counters_row(ctx.executor_id, ctx.name).credit_overrun_total == 1

      # The shed left a gap; the next admitted frame drives the existing
      # resync repair. Mint credit first so f3 is admitted.
      :ok = Flezha.State.WalWriter.fsync(writer)
      assert_receive {:ctl_async, _, {:credit_grant, _}}, 2_000
      :ok = push_frame(ctx.channel, f3)

      assert_receive {:resync_called, {_, _, "k1"}, opts}, 2_000
      assert opts[:reason] == :seq_gap
      assert Flezha.State.WalWriter.last_persisted_seq(writer) == 1
    end
  end

  describe "reconnect resets the ledger (2.6)" do
    test "re-attach re-runs the handshake with a fresh session; stale drains and grants are void",
         ctx do
      init1 = establish_credit(ctx)

      # Debit something under session 1.
      :ok =
        push_frame(
          ctx.channel,
          state_change_frame(ctx.class, "k1", 1, String.duplicate("a", 100))
        )

      # Connection loss voids the session.
      send(ctx.channel, {:flz_closed, :ref, :eof})
      _ = GenServer.call(ctx.channel, :stream_ref_status)
      assert counters_row(ctx.executor_id, ctx.name).credit_active == 0

      # Next frame after re-attach re-offers with a NEW session id.
      :ok = push_frame(ctx.channel, state_change_frame(ctx.class, "k1", 2, "post-reconnect"))
      assert_receive {:init_offered, init2}, 2_000
      assert init2.stream_id != init1.stream_id
      await(fn -> counters_row(ctx.executor_id, ctx.name).credit_active == 1 end)

      level = counters_row(ctx.executor_id, ctx.name).credit_ledger_level

      # A drain report tagged with the dead session (a grant-in-flight
      # equivalent) must not mint anything on the new ledger.
      send(ctx.channel, {:credit_drained, init1.stream_id, 999_999})
      _ = GenServer.call(ctx.channel, :stream_ref_status)

      refute_receive {:ctl_async, _, {:credit_grant, _}}, 200
      assert counters_row(ctx.executor_id, ctx.name).credit_ledger_level == level
    end
  end

  describe "per-Segment receiver queues (3.1)" do
    test "a slow Segment cannot stall a healthy sibling on the same stream", ctx do
      establish_credit(ctx)

      # k1's writer already exists from the handshake trigger frame (seq 0);
      # materialize k2's.
      :ok = push_frame(ctx.channel, state_change_frame(ctx.class, "k2", 0, "k2-seed"))
      k1_writer = writer_pid({ctx.class, "op1", "k1"})
      k2_writer = writer_pid({ctx.class, "op1", "k2"})
      await(fn -> Flezha.State.WalWriter.last_persisted_seq(k1_writer) == 0 end)
      await(fn -> Flezha.State.WalWriter.last_persisted_seq(k2_writer) == 0 end)

      # k1's WAL drains slowly (suspended writer); its appends queue in its
      # own mailbox.
      :ok = :sys.suspend(k1_writer)

      for seq <- 1..5 do
        :ok = push_frame(ctx.channel, state_change_frame(ctx.class, "k1", seq, "slow"))
        :ok = push_frame(ctx.channel, state_change_frame(ctx.class, "k2", seq, "healthy"))
      end

      # k2 keeps appending while k1 is wedged.
      await(fn -> Flezha.State.WalWriter.last_persisted_seq(k2_writer) == 5 end)
      assert Flezha.State.WalWriter.last_persisted_seq(k2_writer) == 5

      # k1 catches up from its queue once its WAL recovers.
      :ok = :sys.resume(k1_writer)
      await(fn -> Flezha.State.WalWriter.last_persisted_seq(k1_writer) == 5 end)
    end
  end
end
