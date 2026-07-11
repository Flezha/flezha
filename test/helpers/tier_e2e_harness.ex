defmodule FlezhaTest.Helper.TierE2EHarness do
  @moduledoc """
  Shared crash-matrix harness for the effectively-once end-to-end suites
  (`test/system/effectively_once_*_e2e_test.exs`): boot a real Rust
  executor, SIGKILL it mid-stream, brutally crash the controller's state
  writers with a staled flush cursor, and force fresh rebinds — the sink
  under test must converge to the no-crash final state regardless.
  """

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Executor.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Outbox
  alias FlezhaTest.Helper.NodeTestHelper

  def send_inputs(operative_id, namespace, inputs) do
    for input <- inputs do
      OperativeManager.send_io(operative_id, namespace, input, %{"input_type" => "2"})
    end
  end

  @doc """
  Brutally kill the Segment's writers (bypassing `terminate/2` — the cursor
  persist at shutdown must not run) and reset the durable cursor to
  "nothing flushed": the crash-after-flush-before-cursor window at e2e
  scale. The restart must re-drive every durable output with its original
  identity.
  """
  def stale_cursor_and_crash_writers(operative_id, segment_id) do
    case Flezha.State.Supervisor.lookup(segment_id) do
      {:ok, pid} -> Process.exit(pid, :kill)
      :error -> :ok
    end

    case Flezha.State.Supervisor.lookup({operative_id, :input_log}) do
      {:ok, pid} -> Process.exit(pid, :kill)
      :error -> :ok
    end

    dedup = Outbox.read(segment_id)
    :ok = Outbox.write(segment_id, %{dedup | outbox_flushed_seq: -1})
  end

  def boot_executor(namespace, id_path) do
    {port, executor_id} =
      NodeTestHelper.start_executor_port(namespace,
        runtime: :rust,
        transport: :uds,
        env: [{"FLEZHA_EXECUTOR_ID_PATH", id_path}],
        connect_timeout: 120_000
      )

    on_exit(fn ->
      if is_port(port) and Enum.member?(:erlang.ports(), port) do
        try do
          Port.close(port)
        catch
          _, _ -> :ok
        end
      end
    end)

    {port, executor_id}
  end

  @doc """
  Kill the Overwatch and wait until its supervised replacement is live —
  inputs sent before that would race the restart (a real source blocks or
  retries; `send_io` is fire-and-forget).
  """
  def force_fresh_rebind(namespace, operative_id) do
    {:ok, old_pid} = OperativeManager.get_operative_pid(namespace, operative_id)
    Process.exit(old_pid, :kill)

    assert wait_until(
             fn ->
               case OperativeManager.get_operative_pid(namespace, operative_id) do
                 {:ok, pid} -> pid != old_pid and Process.alive?(pid)
                 _ -> false
               end
             end,
             60_000
           ),
           "no replacement Overwatch came up after the kill"
  end

  def os_pid(port) do
    {:os_pid, pid} = :erlang.port_info(port, :os_pid)
    pid
  end

  def sigkill(os_pid) do
    {_, 0} = System.cmd("kill", ["-KILL", Integer.to_string(os_pid)], stderr_to_stdout: true)
  end

  def unique(prefix), do: prefix <> "_" <> Integer.to_string(System.unique_integer([:positive]))

  def unique_id_path() do
    Path.join(System.tmp_dir!(), "eff_once_id_#{System.unique_integer([:positive])}")
  end

  def cleanup(namespace, operative_id, id_path) do
    try do
      OperativeManager.stop_all_operatives(namespace)
    catch
      _, _ -> :ok
    end

    try do
      FleetManager.stop_namespace(namespace)
    catch
      _, _ -> :ok
    end

    File.rm(id_path)
    File.rm_rf(Path.join(Flezha.State.Registry.state_root(), operative_id))
  end
end
