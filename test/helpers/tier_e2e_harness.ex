defmodule FlezhaTest.Helper.TierE2EHarness do
  @moduledoc """
  Shared crash-matrix harness for the effectively-once end-to-end suites
  (`test/system/effectively_once_*_e2e_test.exs`): boot a real runner of the
  selected SDK runtime, SIGKILL it mid-stream, brutally crash the
  controller's state writers with a staled flush cursor, and force fresh
  rebinds — the sink under test must converge to the no-crash final state
  regardless.
  """

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1, start_supervised!: 1]
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Outbox
  alias FlezhaTest.Helper.NodeTestHelper

  def send_inputs(operative_id, namespace, inputs) do
    for input <- inputs do
      OperativeManager.send_io(operative_id, namespace, input, %{"input_type" => "2"})
    end
  end

  @doc """
  Brutally kill the Entity's writers (bypassing `terminate/2` — the cursor
  persist at shutdown must not run) and reset the durable cursor to
  "nothing flushed": the crash-after-flush-before-cursor window at e2e
  scale. The restart must re-drive every durable output with its original
  identity.
  """
  def stale_cursor_and_crash_writers(operative_id, entity_id) do
    case Flezha.State.Supervisor.lookup(entity_id) do
      {:ok, pid} -> Process.exit(pid, :kill)
      :error -> :ok
    end

    case Flezha.State.Supervisor.lookup({operative_id, :input_log}) do
      {:ok, pid} -> Process.exit(pid, :kill)
      :error -> :ok
    end

    dedup = Outbox.read(entity_id)
    :ok = Outbox.write(entity_id, %{dedup | outbox_flushed_seq: -1})
  end

  def boot_runner(namespace, id_path, opts \\ []) do
    {port, runner_id} =
      NodeTestHelper.start_runner_port(namespace,
        transport: :uds,
        env: [{"FLEZHA_RUNNER_ID_PATH", id_path} | Keyword.get(opts, :env, [])],
        port_opts: Keyword.get(opts, :port_opts, [])
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

    {port, runner_id}
  end

  @doc """
  Kill the Overwatch so its supervised replacement re-binds, without waiting
  for the replacement to be observable.

  For a workflow operative the replacement is what `force_fresh_rebind/2`
  cannot wait on: a resumed chain can run to completion and deregister between
  two polls, so "a live replacement pid" is a state the test may legitimately
  never see. The terminal completion record is the thing to wait on instead.
  """
  def kill_overwatch(namespace, operative_id) do
    {:ok, pid} = OperativeManager.get_operative_pid(namespace, operative_id)
    Process.exit(pid, :kill)
    :ok
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

  @doc """
  Point the remote durability tier at the compose Postgres and start its
  shipper for the duration of the test, on a table of its own so concurrent
  cases never share a manifest namespace.

  The short ship interval is deliberate: the drill's whole claim is a bound on
  loss, so the backstop must be observable inside one test.
  """
  def install_durability_tier() do
    table = "flezha_durability_e2e_#{System.unique_integer([:positive])}"

    opts =
      FlezhaTest.Helper.PostgresSink.conn_opts()
      |> Keyword.merge(table: table, auto_create: true)

    Application.put_env(:flezha, :durability_tier,
      backend: {Flezha.State.DurabilityTier.Backend.Postgres, opts},
      ship_interval_ms: 2_000,
      rpo_target_ms: 30_000,
      gc_interval_ms: 3_600_000
    )

    start_supervised!(Flezha.State.DurabilityTier.Backend.Postgres.child_spec(opts))
    start_supervised!(Flezha.State.DurabilityTier.Shipper)

    on_exit(fn -> Application.delete_env(:flezha, :durability_tier) end)
    opts
  end

  @doc """
  Destroy every local durable byte one operative owns — its Entity WALs,
  snapshots, proc-journal, durable inbox, input log and class sidecars — and
  evict its in-memory class declarations.

  Scoped to the operative's own class directory rather than the whole state
  root: the suite shares one controller, so wiping the root would take other
  tests' state with it. For the operative under drill the effect is identical —
  nothing of it survives locally, and only the remote manifest can bring it
  back.
  """
  def destroy_local_state(operative_id) do
    reserved =
      Enum.map(
        Flezha.State.Registry.reserved_entity_keys(),
        &{operative_id, operative_id, &1}
      )

    # `delete_entity/1` is the existing hard-delete: it stops the writer, drops
    # its fsync mark, unlinks the files and forgets the watermark. The stop must
    # be graceful — a killed writer is transient-restarted, and the replacement
    # would hold an open handle to the unlinked file while hydration writes a
    # new one underneath it.
    for entity_id <- Flezha.State.Registry.entities_of(operative_id) ++ reserved do
      Flezha.State.Supervisor.delete_entity(entity_id)
    end

    case Flezha.State.Supervisor.lookup({operative_id, :input_log}) do
      {:ok, pid} -> GenServer.stop(pid, :normal, 5_000)
      :error -> :ok
    end

    File.rm_rf!(Flezha.State.Registry.class_dir(operative_id))
    Flezha.Operative.ClassRegistry.delete(operative_id)
    :ok
  catch
    :exit, _ -> :ok
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

  # `operative_id` is unused: the namespace is this scenario's alone, so its whole
  # state tree goes and every class under it with it. It stays in the signature
  # because every caller names what it created.
  def cleanup(namespace, _operative_id, id_path) do
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

    # Durable state lives under `<root>/<namespace>/<class>`
    # (`Flezha.State.Registry.class_dir/1`), and the namespace is this test's
    # alone — so the whole namespace tree goes. Removing `<root>/<class>`, a
    # path that has never existed, left every run's WAL on disk, and a later run
    # whose `unique/1` id collided with an earlier one restored it.
    File.rm_rf(Path.join(Flezha.State.Registry.state_root(), namespace))
  end

  @doc """
  Return once every Entity of `operative_id` has a transactional outbox that
  owes nothing: each durable entry dispatched and confirmed by its sinks.
  Everything the sink will ever forward for the inputs so far is in the
  caller's mailbox after this.
  """
  def await_outbox_drained(operative_id, timeout_ms \\ 30_000) do
    NodeTestHelper.wait_until(
      fn ->
        case Flezha.State.Registry.entities_of(operative_id) do
          [] -> false
          entities -> Enum.all?(entities, &outbox_drained?/1)
        end
      end,
      timeout_ms
    )
  end

  defp outbox_drained?(entity_id) do
    case Flezha.State.Supervisor.lookup(entity_id) do
      {:ok, writer} ->
        state = :sys.get_state(writer)
        state.outbox_awaiting == %{} and :queue.is_empty(state.pending_outbox)

      :error ->
        false
    end
  end

  @doc """
  Every keyed output the `BeamMsgForward` sink has forwarded to this process,
  as `{payload, metadata}` — WITHOUT consuming the mailbox, so a poll can be
  retried and the full history audited afterwards with `drain_outputs/0`.
  """
  def peek_outputs() do
    {:messages, messages} = Process.info(self(), :messages)

    for {:message_forward, msg, md} <- messages do
      {to_string(msg), md}
    end
  end

  @doc "Consume and return every forwarded output, in arrival order."
  def drain_outputs(acc \\ []) do
    receive do
      {:message_forward, msg, md} -> drain_outputs([{to_string(msg), md} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  @doc """
  Wait until the changelog built by upserting everything received so far on
  `flz.key` covers `expected`. This is the sink contract the tier exists to
  serve: duplicates collapse, the last value per key wins.
  """
  def await_changelog(expected, timeout) do
    wait_until(
      fn -> Map.take(changelog(peek_outputs()), Map.keys(expected)) == expected end,
      timeout
    )
  end

  @doc "Upsert `{payload, metadata}` outputs into a changelog keyed by `flz.key`."
  def changelog(outputs) do
    Enum.reduce(outputs, %{}, fn {msg, md}, acc -> Map.put(acc, md["flz.key"], msg) end)
  end

  @doc """
  Group outputs by their durable output identity `(flz.entity, flz.seq,
  flz.epoch)`. The tier's dedup contract is that one identity never carries two
  different payloads — a controller re-drive repeats an identity byte-for-byte.
  """
  def by_identity(outputs) do
    Enum.group_by(
      outputs,
      fn {_msg, md} -> {md["flz.entity"], md["flz.seq"], md["flz.epoch"]} end,
      fn {msg, _md} -> msg end
    )
  end

  @doc "Assert no output identity ever delivered two different payloads."
  def assert_identities_are_consistent(outputs) do
    for {identity, payloads} <- by_identity(outputs) do
      assert length(Enum.uniq(payloads)) == 1,
             "identity #{inspect(identity)} delivered divergent payloads: #{inspect(payloads)}"
    end
  end
end
