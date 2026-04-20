defmodule Flezha.Recovery.SysModeTest do
  @moduledoc """
  Covers every transition edge listed in design §D1 and task 3.9:
    - `:recover → :normal` (peer_normal short-circuit)
    - `:recover → :normal` (window elapsed + recovery done)
    - `:bootstrap → :normal` (window elapsed, empty DETS)
    - `disabled → :normal` (recovery disabled via config)

  Tests use an isolated tmp dir for the DETS store and small window values so
  wall-clock waits stay under ~100 ms per test.
  """

  use ExUnit.Case, async: false

  alias Flezha.Recovery.Store
  alias Flezha.Recovery.SysMode
  alias Flezha.Processor.Builder

  @window_short 50

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_sysmode_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)

    # Stash existing config so we can restore.
    prev_dir = Application.get_env(:flezha, :recovery_data_dir)
    prev_win = Application.get_env(:flezha, :recovery_window_ms)
    prev_enabled = Application.get_env(:flezha, :recovery_enabled)

    Application.put_env(:flezha, :recovery_data_dir, tmp)
    Application.put_env(:flezha, :recovery_window_ms, @window_short)
    Application.put_env(:flezha, :recovery_enabled, true)

    stop_if_running(SysMode)
    stop_if_running(Store)

    telemetry_id = "sys_mode_test_#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach(
      telemetry_id,
      [:flezha, :recovery, :sysmode_changed],
      fn _n, _m, meta, _cfg -> send(test_pid, {:sysmode_changed, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(telemetry_id)
      stop_if_running(SysMode)
      stop_if_running(Store)
      restore(:recovery_data_dir, prev_dir)
      restore(:recovery_window_ms, prev_win)
      restore(:recovery_enabled, prev_enabled)
      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "disabled → :normal at init emits telemetry" do
    Application.put_env(:flezha, :recovery_enabled, false)

    {:ok, _} = Store.start_link([])
    {:ok, _} = SysMode.start_link([])

    assert SysMode.get() == :normal
    assert_received {:sysmode_changed, %{from: :bootstrap, to: :normal, reason: :disabled}}
  end

  test "empty DETS stays :bootstrap, then → :normal after window" do
    {:ok, _} = Store.start_link([])
    {:ok, _} = SysMode.start_link([])

    assert SysMode.get() == :bootstrap
    refute_received {:sysmode_changed, _}

    assert_receive {:sysmode_changed, %{from: :bootstrap, to: :normal, reason: :window_elapsed}},
                   @window_short * 10

    assert SysMode.get() == :normal
  end

  test "non-empty DETS enters :recover, then → :normal after timer + recovery" do
    {:ok, _} = Store.start_link([])
    :ok = Store.put("c-1", "ns", %Builder{id: "c-1", factory_id: "f"})
    {:ok, _} = SysMode.start_link([])

    # :init transition to :recover emits its own event.
    assert_received {:sysmode_changed, %{from: :bootstrap, to: :recover, reason: :init}}
    assert SysMode.get() == :recover

    # Coordinator is not running, so SysMode's fallback immediately calls
    # notify_recovery_done after the window elapses. Either way, we should
    # eventually see a transition to :normal with reason :window_elapsed.
    assert_receive {:sysmode_changed, %{from: :recover, to: :normal, reason: :window_elapsed}},
                   @window_short * 10

    assert SysMode.get() == :normal
  end

  test ":recover → :normal on peer_normal short-circuit, no window wait" do
    # Use a very long window to ensure we're aborting via peer, not via timer.
    Application.put_env(:flezha, :recovery_window_ms, 60_000)

    {:ok, _} = Store.start_link([])
    :ok = Store.put("c-1", "ns", %Builder{id: "c-1", factory_id: "f"})
    {:ok, _} = SysMode.start_link([])

    assert SysMode.get() == :recover
    assert_received {:sysmode_changed, %{from: :bootstrap, to: :recover, reason: :init}}

    # Simulate a peer reporting :normal.
    GenServer.cast(SysMode, {:peer_sysmode, :"peer@host", :normal})

    assert_receive {:sysmode_changed, %{from: :recover, to: :normal, reason: :peer_normal}}, 500
    assert SysMode.get() == :normal
  end

  test ":peer_sysmode with :bootstrap mode does NOT cancel our recovery" do
    Application.put_env(:flezha, :recovery_window_ms, 60_000)

    {:ok, _} = Store.start_link([])
    :ok = Store.put("c-1", "ns", %Builder{id: "c-1", factory_id: "f"})
    {:ok, _} = SysMode.start_link([])

    assert SysMode.get() == :recover

    GenServer.cast(SysMode, {:peer_sysmode, :"peer@host", :bootstrap})
    GenServer.cast(SysMode, {:peer_sysmode, :"peer@host", :recover})

    # Drain any messages for up to 100ms; must still be :recover.
    Process.sleep(100)
    assert SysMode.get() == :recover
    refute_received {:sysmode_changed, %{from: :recover, to: :normal}}
  end

  test "peer_connected/1 casts our mode to the peer's SysMode name" do
    # We don't have a real peer, but we can register a proxy process on the
    # local node under {SysMode, node()} to receive the outbound cast — the
    # cast `{SysMode, peer_node}` on the local node is equivalent to sending
    # to a named process. Here we just verify it does not crash when the peer
    # is unreachable.
    {:ok, _} = Store.start_link([])
    {:ok, _} = SysMode.start_link([])

    # :noproc target — must be tolerated.
    assert :ok = SysMode.peer_connected(:"nonexistent@nowhere")
    Process.sleep(20)
    assert SysMode.get() in [:bootstrap, :normal]
  end

  defp stop_if_running(mod) do
    try do
      case Process.whereis(mod) do
        nil -> :ok
        pid -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
      end
    catch
      :exit, _ -> :ok
    end
  end

  defp restore(key, nil), do: Application.delete_env(:flezha, key)
  defp restore(key, val), do: Application.put_env(:flezha, key, val)
end
