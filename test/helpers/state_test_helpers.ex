defmodule Flezha.State.TestHelpers do
  @moduledoc """
  Fixtures shared across the persistence-module test suite, plus
  `override_env/2` — the one app-env override every test overriding a
  `:flezha` key goes through.

  Lives under `test/helpers/`, which `elixirc_paths` compiles in `:dev` and
  `:test` but not `:prod`: `reset_all_cells/0` wipes the state root, and
  that must not be reachable from a release.
  """

  alias Flezha.SDK.RecordFrame
  alias Flezha.State.{FsyncBatcher, Outbox, Paths, Supervisor, WalWriter}
  alias FlezhaTest.Helper.NodeTestHelper

  # The outputs of a received dispatch batch not yet asserted on.
  @dispatched {__MODULE__, :dispatched}

  @doc """
  Start the state subsystem's process tree under the test supervisor — the
  writer registry, `Flezha.State.Supervisor`, the `FsyncBatcher` and any
  `extra` children — tolerating those the application already runs. Call from
  a `setup` block.
  """
  @spec start_state_tree([Supervisor.child_spec() | module()]) :: :ok
  def start_state_tree(extra \\ []) do
    children = [
      {Elixir.Registry, keys: :unique, name: Supervisor.registry_name()},
      Supervisor,
      FsyncBatcher | extra
    ]

    for child <- children do
      case ExUnit.Callbacks.start_supervised(child) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end

    :ok
  end

  @doc """
  Close `class`'s input log if one is running, so its queued truncation is done
  before the state root under it goes away.
  """
  def close_input_log(class) do
    case Supervisor.lookup({class, :input_log}) do
      {:ok, pid} -> NodeTestHelper.ensure_stopped(pid)
      _ -> :ok
    end
  end

  @doc """
  Allocates a per-test temporary directory under `System.tmp_dir!/0`, registers
  it as the controller's `:state_root`, and registers the `on_exit` that drops
  both again — the call that creates the root is the one that removes it, so a
  caller writes a cleanup of its own only when it has extra work to do.

  The directory is one this call created (`NodeTestHelper.claim_tmp_dir/1`):
  sharing a re-minted one would hand a test a WAL a previous run wrote, and
  `WalWriter.append/4` answers `:duplicate` for a seq it has already persisted.

  The root the claim replaces is put back, not deleted. `config/runtime.exs`
  sets `:state_root` for every test run; deleting the key drops the rest of the
  run onto the declared default, which is the checkout's `app_data/state`, and
  a live relay's WAL then lands in the working tree.
  """
  @spec tmp_app_data_dir() :: binary()
  def tmp_app_data_dir() do
    base = NodeTestHelper.claim_tmp_dir("flezha_state_test")
    restore = override_env(:state_root, base)

    # The claim is also driven from a bare BEAM with no ExUnit running, where
    # there is no test process to register against and the root is the
    # caller's to remove.
    if Process.whereis(ExUnit.Server) do
      ExUnit.Callbacks.on_exit(fn ->
        restore.()
        File.rm_rf!(base)
      end)
    end

    base
  end

  @doc """
  Stand in as the WalWriter that owns `cell_id`'s outbox cursor: the
  delivery-confirmation seam (`Flezha.State.Outbox`) resolves the owning writer
  through the state registry, so a registered test process receives the sink's
  `hold` / `settled` / `confirm` reports as plain casts.

  Used by the sink-plugin suites, which exercise the confirmation seam without a
  real persistence subsystem behind it.
  """
  @spec stub_outbox_owner(Paths.cell_id()) :: :ok
  def stub_outbox_owner(cell_id) do
    case Elixir.Registry.start_link(keys: :unique, name: Supervisor.registry_name()) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    {:ok, _} = Elixir.Registry.register(Supervisor.registry_name(), cell_id, nil)
    :ok
  end

  @doc """
  Metadata a durable outbox flush would stamp on one output of `cell_id`'s
  entry `seq` — identity plus the confirmation token the owning writer waits
  on. `extra` merges over it for the fields a given sink reads.
  """
  @spec guaranteed_metadata(Paths.cell_id(), non_neg_integer(), map()) :: map()
  def guaranteed_metadata({rc, sh, pk} = cell_id, seq, extra \\ %{}) do
    Map.merge(
      %{
        "channel" => "out",
        "flz.cell" => "#{rc}/#{sh}/#{pk}",
        "flz.seq" => "#{seq}.0",
        "flz.epoch" => "0",
        Outbox.ack_key() => Outbox.token(cell_id, {:cursor, seq})
      },
      extra
    )
  end

  @doc """
  The next durable output the chain was handed.

  The dispatcher hands a whole pass to the chain in ONE `{:io_out_batch,
  outputs}` cast, so a test standing in for the Overwatch buffers the outputs
  of that batch it has not asserted on yet. What one fsync window happened to
  gather into a batch is timing, never contract — a test about anything but the
  batching itself asserts on the outputs, in order.
  """
  def next_output(timeout \\ 100) do
    case Process.get(@dispatched, []) do
      [output | rest] ->
        Process.put(@dispatched, rest)
        output

      [] ->
        receive do
          {:"$gen_cast", {:io_out_batch, outputs}} ->
            Process.put(@dispatched, outputs)
            next_output(timeout)
        after
          timeout -> ExUnit.Assertions.flunk("no output reached the chain in #{timeout}ms")
        end
    end
  end

  @doc """
  The whole batch one dispatch pass handed over — what a test about the batching
  itself asserts on, rather than `next_output/1`'s stream.
  """
  def assert_batch(timeout) do
    receive do
      {:"$gen_cast", {:io_out_batch, outputs}} -> outputs
    after
      timeout -> ExUnit.Assertions.flunk("no dispatch batch reached the chain in #{timeout}ms")
    end
  end

  @doc """
  Stand in for the Overwatch's `:io_out_batch` handler: run the batch's chains
  exactly as the dispatcher does, so its entries settle in ONE report.
  """
  def run_batch(outputs, chain \\ fn _payload, _metadata -> :ok end),
    do:
      Outbox.run(outputs, :ok, fn payload, metadata, :ok ->
        chain.(payload, metadata)
        :ok
      end)

  @doc "The metadata a stamped output carries — what a sink confirms against."
  def metadata_of({_payload, metadata, _ack}), do: metadata

  @doc """
  Run `fun` with the shared 5 ms fsync batcher frozen, so a run of appends lands
  in ONE durable window and the dispatch pass under test is exactly one batch.
  """
  def in_one_window(fun) do
    :ok = :sys.suspend(FsyncBatcher)

    try do
      fun.()
    after
      :ok = :sys.resume(FsyncBatcher)
    end
  end

  @doc "No output reached the chain: nothing buffered and no batch arriving."
  def refute_dispatched(timeout \\ 50) do
    case Process.get(@dispatched, []) do
      [] ->
        receive do
          {:"$gen_cast", {:io_out_batch, outputs}} ->
            ExUnit.Assertions.flunk("unexpected dispatch: #{inspect(outputs)}")
        after
          timeout -> :ok
        end

      buffered ->
        ExUnit.Assertions.flunk("output queued from an earlier batch: #{inspect(buffered)}")
    end
  end

  @doc """
  Every named entry is confirmed to its owning writer. A sink confirms a
  completed batch in one report and parks one record at a time, and how many
  records a linger window gathered into a batch is timing — so this asserts on
  the entries confirmed, never on how they were reported.
  """
  def assert_confirmed(keys, timeout \\ 2_000), do: await_confirmed(MapSet.new(keys), timeout)

  defp await_confirmed(remaining, timeout) do
    if MapSet.size(remaining) == 0 do
      :ok
    else
      receive do
        {:"$gen_cast", {:outbox_confirmed_many, keys}} ->
          await_confirmed(Enum.reduce(keys, remaining, &MapSet.delete(&2, &1)), timeout)

        {:"$gen_cast", {:outbox_confirmed, key}} ->
          await_confirmed(MapSet.delete(remaining, key), timeout)
      after
        timeout ->
          ExUnit.Assertions.flunk("never confirmed: #{inspect(MapSet.to_list(remaining))}")
      end
    end
  end

  @doc """
  Override one `:flezha` application env key, returning a zero-arity restore
  closure for the caller's `on_exit` (mirrors `tmp_app_data_dir/0`'s
  caller-owned cleanup convention).
  """
  @spec override_env(atom(), term()) :: (-> :ok)
  def override_env(key, value) do
    previous = Application.fetch_env(:flezha, key)
    Application.put_env(:flezha, key, value)

    fn ->
      case previous do
        {:ok, old} -> Application.put_env(:flezha, key, old)
        :error -> Application.delete_env(:flezha, key)
      end
    end
  end

  @doc """
  Reset all per-Cell state: terminate every live `WalWriter` and wipe
  the on-disk `state_root`. Integration tests reuse relay ids across cases;
  now that restore actually rehydrates a relay from its WAL on (re)bind,
  leftover state from a prior test would leak into the next one. Call from a
  `setup` block so each test starts from empty cells.

  The root comes from `Paths.state_root/0` — the one resolver the writers, the
  bind-time restore and the relay teardown all use. Deriving it here from
  the application env alone would empty a different directory the moment a
  teardown deleted `:state_root`, because the resolver's fall-back is the
  checkout-relative default. (`FLEZHA_STATE_DIR` does not separate the two
  reads: nothing consults it after boot.)

  **This empties the live run root**, and for three callers that is the point:
  `flezha_test.exs`, `flezha_handover_test.exs` and `overwatch_test.exs` reset
  in a `setup` while driving the boot-time controller and fleet, and declare no
  root of their own. `Environment.per_run/2` derives that root from the OS
  process, so it is one run's disposable tree and no other suite shares it.
  What goes with it: `.instance.lock`, whose holder keeps its record lock on
  the now-unlinked inode while a claimant in another OS process would create a
  fresh file at the path and not be refused; `.layout`, read at boot only; and
  `<state_root>/process_hub`, which is where `per_run/2` nests
  `:process_hub_registry_root` in `:test`. None of that separates two suites —
  the per-run derivation does — and this helper never runs in `:prod`.

  Giving those three a root of their own is NOT the fix: `tmp_app_data_dir/0`
  repoints `:state_root` and nothing else, so the hub registry would stay on
  the boot-time root while the reset emptied a different one. That is a fleet
  lifecycle change, not a fixture declaration.
  """
  @spec reset_all_cells() :: :ok
  def reset_all_cells() do
    sup = Flezha.State.Supervisor

    for {_, pid, _, _} <- DynamicSupervisor.which_children(sup), is_pid(pid) do
      DynamicSupervisor.terminate_child(sup, pid)
    end

    state_root = Paths.state_root()
    File.rm_rf(state_root)
    File.mkdir_p(state_root)
    :ok
  end

  @doc """
  Move a live writer's clocks `ms` into the past — its advance window, its
  last-seen Change and its oldest un-snapshotted record — so a check judging
  age sees the window lapsed without the test waiting it out.
  """
  def age_writer(writer, ms) do
    :sys.replace_state(writer, fn state ->
      [:last_advance_at, :last_change_seen_at, :oldest_unsnapshotted_at]
      |> Enum.reduce(state, fn key, acc -> age_field(acc, key, ms) end)
    end)

    :ok
  end

  defp age_field(state, key, ms) do
    case Map.get(state, key) do
      at when is_integer(at) -> Map.put(state, key, at - ms)
      _ -> state
    end
  end

  @doc """
  Build a 16-byte WAL header. The controller treats the header as opaque
  bytes; tests only need a stable shape to round-trip records through
  `RecordFrame.encode_record/3` and back.
  """
  @spec stub_header() :: binary()
  def stub_header() do
    <<1::8, 0::8, 0::16, 0::32, 0::64>>
  end

  @doc """
  Build a fully-framed record from `seq` and `change_bytes` using a
  stub header. Returns the bytes that would land on disk.
  """
  @spec build_wal_record(non_neg_integer(), binary()) :: binary()
  def build_wal_record(seq, change_bytes) do
    RecordFrame.encode_record(seq, stub_header(), change_bytes)
  end

  @doc """
  The byte offset every record in the record log at `path` starts at, read out
  of the file's own framing rather than rebuilt from what a test appended.
  """
  @spec record_offsets(Path.t()) :: [non_neg_integer()]
  def record_offsets(path) do
    data = File.read!(path)

    Stream.unfold(0, fn
      offset when offset + 12 > byte_size(data) ->
        nil

      offset ->
        {:ok, _seq, _header, _body, _raw, consumed} =
          RecordFrame.decode_bytes(binary_part(data, offset, byte_size(data) - offset))

        {offset, offset + consumed}
    end)
    |> Enum.to_list()
  end

  @doc """
  Flip one bit in the body of the record starting at `offset` in the record log
  at `path`.

  The canonical corruption: the framing is left intact, so the record's crc32
  fails while its length still locates the record after it — which is what
  separates a corrupt record from the torn tail a crash mid-append leaves.
  """
  @spec corrupt_record_body(Path.t(), non_neg_integer()) :: :ok
  def corrupt_record_body(path, offset) do
    at = offset + 12 + RecordFrame.header_size()
    <<prefix::binary-size(^at), byte, rest::binary>> = File.read!(path)
    File.write!(path, <<prefix::binary, Bitwise.bxor(byte, 1), rest::binary>>)
  end

  @doc """
  Write one Cell's WAL holding a single record stamped by `sdk_lang`.

  The fixture behind every "what language wrote this state" assertion: the
  placement guard resolves a factory's established language from byte 0 of a
  record header, so a test needs a real on-disk WAL rather than a stub.
  """
  @spec write_wal_of_lang(Paths.cell_id(), byte()) :: :ok
  def write_wal_of_lang(cell_id, sdk_lang) do
    body = "state-bytes"
    header = RecordFrame.encode_header(sdk_lang, 0, 1, RecordFrame.format_id(:postcard), body)
    path = Paths.wal_path(cell_id)

    File.mkdir_p!(Path.dirname(path))
    File.write!(path, build_wal_record(0, header, body))
  end

  @doc """
  Build a record with a caller-supplied header.
  """
  @spec build_wal_record(non_neg_integer(), binary(), binary()) :: binary()
  def build_wal_record(seq, header, change_bytes) do
    RecordFrame.encode_record(seq, header, change_bytes)
  end

  @doc """
  Write `seqs` as a Cell's WAL, creating the directory. `tag` distinguishes
  two writers' records for the same seq range, which is what a cross-writer
  collision case needs.
  """
  def write_wal(cell_id, seqs, tag \\ "change") do
    path = Paths.wal_path(cell_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, wal_records(seqs, tag))
  end

  @doc "Append `seqs` to a Cell's existing WAL."
  def append_wal(cell_id, seqs, tag \\ "change") do
    File.write!(Paths.wal_path(cell_id), wal_records(seqs, tag), [:append])
  end

  @doc "The framed bytes of `seqs`, as they land on disk."
  def wal_records(seqs, tag \\ "change") do
    Enum.map_join(seqs, fn seq -> build_wal_record(seq, "#{tag}-#{seq}") end)
  end

  @doc "Remove every durable file one Cell owns."
  def wipe_cell(cell_id) do
    Enum.each(Paths.cell_files(cell_id), &File.rm/1)
  end

  @doc """
  A short decimal string no other caller in this BEAM has been handed, for the
  cell- and class-name prefixes a per-test fixture needs.

  The stream is node-global and per-scheduler striped, and it restarts low in
  every BEAM, so a name built from it is unique within a run and not across
  runs — a fixture that has to survive a restart claims its name instead
  (`NodeTestHelper.claim_tmp_dir/1`).
  """
  @spec uniq() :: binary()
  def uniq(), do: Integer.to_string(System.unique_integer([:positive]))

  @doc """
  Construct a deterministic `cell_id` tuple usable as a fixture.
  """
  @spec sample_cell_id(binary()) :: Paths.cell_id()
  def sample_cell_id(suffix \\ "p1") do
    {"TestRelay", "TestTasklet", suffix}
  end

  @doc """
  Close a writer and wait until its registry entry is gone.

  `WalWriter.close/1` is a `GenServer.stop/2`, so it returns once the process
  has exited — but `Elixir.Registry` drops the entry only when it processes the
  monitor's `:DOWN`, which happens after. A re-open in that window gets the
  dead pid back from `Supervisor.lookup/1` and the next call exits `:noproc`.
  """
  @spec close_and_await(pid(), Paths.cell_id(), timeout()) :: :ok
  def close_and_await(server, cell_id, timeout_ms \\ 2_000) do
    WalWriter.close(server)

    deregistered? =
      NodeTestHelper.wait_until(
        fn -> Supervisor.lookup(cell_id) == :error end,
        timeout_ms,
        5
      )

    if deregistered?,
      do: :ok,
      else: raise("writer for #{inspect(cell_id)} still registered after close")
  end

  @doc """
  Run `fun` in a fresh process and answer `{result, calls}`: every call it made
  into `:file`, in order, as `{function, args}`. What a walk asks the file
  server, including the `get_cwd` a relative path's expansion costs. The
  `:file` trace pattern is node-global while `fun` runs, so another process
  traced for calls at the same time sees `:file` calls too.
  """
  @spec file_calls((-> result)) :: {result, [{atom(), list()}]} when result: term()
  def file_calls(fun) do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        receive do
          :go -> send(parent, {:done, self(), fun.()})
        end
      end)

    _ = :erlang.trace_pattern({:file, :_, :_}, true, [:global])
    1 = :erlang.trace(pid, true, [:call, {:tracer, parent}])
    send(pid, :go)

    result =
      try do
        receive do
          {:done, ^pid, result} -> result
          {:DOWN, ^ref, :process, ^pid, reason} -> exit(reason)
        end
      after
        :erlang.trace_pattern({:file, :_, :_}, false, [:global])
      end

    trace_ref = :erlang.trace_delivered(pid)

    receive do
      {:trace_delivered, ^pid, ^trace_ref} -> :ok
    end

    {result, traced_file_calls(pid, [])}
  end

  defp traced_file_calls(pid, acc) do
    receive do
      {:trace, ^pid, :call, {:file, function, args}} ->
        traced_file_calls(pid, [{function, args} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
