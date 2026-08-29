defmodule FlezhaTest.Runner.IoStreamStaleRefReproTest do
  @moduledoc """
  Reproduction of the flezha-market prod incident (2026-06-05): the IO data
  stream silently severs and NEVER self-heals — WS ingest keeps flowing, the
  runner goes idle, all procedure WALs/topics freeze, and only a operative recreate
  (`start-asset.sh`) restores it.

  Root cause under test: the IO `StreamChannel` can hold a STALE non-nil
  `stream_ref` after its NIF stream dies WITHOUT emitting `{:flz_closed, ...}`.
  Because `{:bind}` only binds a `nil` ref, a reconnecting live stream is
  rejected with `:already_bound`, and there is NO liveness watchdog to reset
  the ref. This is the IO-side twin of the stale-handle bug that was fixed for
  the CONTROL channel on 2026-06-01 (`ControlChannel.reset_stream_ref/1`) but
  never ported to the IO channel.

  Fails when: a dead-but-unsignalled IO stream can be recovered without a
  process restart (i.e. if a watchdog/reset is ever added, step 3 changes).
  """
  alias Flezha.Runner.{StreamChannel, StreamSup}

  # async: false: registered runner channels.

  use ExUnit.Case, async: false

  setup context do
    name =
      ("io_repro_" <> to_string(context.test))
      |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
      |> String.to_atom()

    {:ok, pid} = StreamChannel.start_link({"ce_repro", name, :primary})
    on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(pid) end)
    %{pid: pid, name: name}
  end

  test "a live IO stream that dies WITHOUT flz_closed is permanently severed", %{
    pid: pid,
    name: name
  } do
    # 1. HEALTHY: a live NIF stream is bound — this is the 10:15→13:50 window
    #    where data flowed and all procedure WALs advanced every minute.
    :ok = GenServer.call(pid, {:bind, :stream_ref_live})
    assert GenServer.call(pid, :stream_ref_status) == :stream_ref_live

    # 2. PROD CONDITION (13:50:42): the NIF stream dies but emits NO
    #    {:flz_closed, ...}. In prod there were ZERO "Connection closed for IO
    #    socket" log lines all day, and no StreamChannel restart — so the only
    #    two paths that nil stream_ref (flz_closed / re-init) never ran.
    #    We therefore deliberately send NOTHING here. The ref stays stale.
    assert GenServer.call(pid, :stream_ref_status) == :stream_ref_live

    # 3. RECONNECT: the runner opens a fresh IO stream; the acceptor tries to
    #    claim a channel via the real bind path. The stale non-nil ref makes the
    #    bind return :already_bound, so bind_stream_to_first_nil/2 finds NO slot.
    #    => the live, healthy reconnecting stream can NEVER take over.
    assert nil == StreamSup.bind_stream_to_first_nil([name], :stream_ref_reconnect)

    # 4. The channel STILL points at the dead ref. Nothing — no watchdog, no
    #    timer, no heal — ever clears it. This is the permanent severance.
    assert GenServer.call(pid, :stream_ref_status) == :stream_ref_live

    # 5. PROOF THE MISSING SIGNAL IS THE GAP: had flz_closed been delivered
    #    (what the NIF failed to emit), recovery works immediately.
    send(pid, {:flz_closed, :stream_ref_live, :eof})
    assert GenServer.call(pid, :stream_ref_status) == nil

    assert {:bound, ^pid, ^name} =
             StreamSup.bind_stream_to_first_nil([name], :stream_ref_reconnect)

    assert GenServer.call(pid, :stream_ref_status) == :stream_ref_reconnect
  end

  test "the IO StreamChannel has no reset API while the control channel does (the 2026-06-01 asymmetry)" do
    refute function_exported?(StreamChannel, :reset_stream_ref, 1)
    assert function_exported?(Flezha.Runner.ControlChannel, :reset_stream_ref, 1)
  end
end
