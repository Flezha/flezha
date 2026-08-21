defmodule Flezha.State.TestHelpers do
  @moduledoc """
  Fixtures shared across the persistence-module test suite, plus
  `override_env/2` — the one app-env override every test overriding a
  `:flezha` key goes through.

  Lives under `test/helpers/`, which `elixirc_paths` compiles in `:dev` and
  `:test` but not `:prod`: `reset_all_entities/0` wipes the state root, and
  that must not be reachable from a release.
  """

  alias Flezha.State.{FsyncBatcher, Outbox, Registry, Supervisor, WalWriter}

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
  Allocates a per-test temporary directory under `System.tmp_dir!/0` and
  registers it as the controller's `:state_root`. Callers should
  `on_exit(fn -> Application.delete_env(:flezha, :state_root) end)` to
  reset the value between tests.
  """
  @spec tmp_app_data_dir() :: binary()
  def tmp_app_data_dir() do
    base =
      Path.join([
        System.tmp_dir!(),
        "flezha_state_test_" <> Integer.to_string(System.unique_integer([:positive]))
      ])

    File.mkdir_p!(base)
    Application.put_env(:flezha, :state_root, base)
    base
  end

  @doc """
  Stand in as the WalWriter that owns `entity_id`'s outbox cursor: the
  delivery-confirmation seam (`Flezha.State.Outbox`) resolves the owning writer
  through the state registry, so a registered test process receives the sink's
  `hold` / `settled` / `confirm` reports as plain casts.

  Used by the sink-plugin suites, which exercise the confirmation seam without a
  real persistence subsystem behind it.
  """
  @spec stub_outbox_owner(Registry.entity_id()) :: :ok
  def stub_outbox_owner(entity_id) do
    case Elixir.Registry.start_link(keys: :unique, name: Supervisor.registry_name()) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    {:ok, _} = Elixir.Registry.register(Supervisor.registry_name(), entity_id, nil)
    :ok
  end

  @doc """
  Metadata a durable outbox flush would stamp on one output of `entity_id`'s
  entry `seq` — identity plus the confirmation token the owning writer waits
  on. `extra` merges over it for the fields a given sink reads.
  """
  @spec guaranteed_metadata(Registry.entity_id(), non_neg_integer(), map()) :: map()
  def guaranteed_metadata({rc, sh, pk} = entity_id, seq, extra \\ %{}) do
    Map.merge(
      %{
        "channel" => "out",
        "flz.entity" => "#{rc}/#{sh}/#{pk}",
        "flz.seq" => "#{seq}.0",
        "flz.epoch" => "0",
        Outbox.ack_key() => Outbox.token(entity_id, {:cursor, seq})
      },
      extra
    )
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
  Reset all per-Entity state: terminate every live `WalWriter` and wipe
  the on-disk `state_root`. Integration tests reuse operative ids across cases;
  now that restore actually rehydrates a operative from its WAL on (re)bind,
  leftover state from a prior test would leak into the next one. Call from a
  `setup` block so each test starts from empty entities.
  """
  @spec reset_all_entities() :: :ok
  def reset_all_entities() do
    sup = Flezha.State.Supervisor

    for {_, pid, _, _} <- DynamicSupervisor.which_children(sup), is_pid(pid) do
      DynamicSupervisor.terminate_child(sup, pid)
    end

    state_root = Application.get_env(:flezha, :state_root, "app_data/state")
    File.rm_rf(state_root)
    File.mkdir_p(state_root)
    :ok
  end

  @doc """
  Build a 16-byte WAL header. The controller treats the header as opaque
  bytes; tests only need a stable shape to round-trip records through
  `WalWriter.encode_record/3` and back.
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
    WalWriter.encode_record(seq, stub_header(), change_bytes)
  end

  @doc """
  Build a record with a caller-supplied header.
  """
  @spec build_wal_record(non_neg_integer(), binary(), binary()) :: binary()
  def build_wal_record(seq, header, change_bytes) do
    WalWriter.encode_record(seq, header, change_bytes)
  end

  @doc """
  Write `seqs` as an Entity's WAL, creating the directory. `tag` distinguishes
  two writers' records for the same seq range, which is what a cross-writer
  collision case needs.
  """
  def write_wal(entity_id, seqs, tag \\ "change") do
    path = Flezha.State.Registry.wal_path(entity_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, wal_records(seqs, tag))
  end

  @doc "Append `seqs` to an Entity's existing WAL."
  def append_wal(entity_id, seqs, tag \\ "change") do
    File.write!(Flezha.State.Registry.wal_path(entity_id), wal_records(seqs, tag), [:append])
  end

  @doc "The framed bytes of `seqs`, as they land on disk."
  def wal_records(seqs, tag \\ "change") do
    Enum.map_join(seqs, fn seq -> build_wal_record(seq, "#{tag}-#{seq}") end)
  end

  @doc "Remove every durable file one Entity owns."
  def wipe_entity(entity_id) do
    Enum.each(Flezha.State.Registry.entity_files(entity_id), &File.rm/1)
  end

  @doc """
  Construct a deterministic `entity_id` tuple usable as a fixture.
  """
  @spec sample_entity_id(binary()) :: Registry.entity_id()
  def sample_entity_id(suffix \\ "p1") do
    {"TestOperative", "TestProcedure", suffix}
  end

  @doc """
  Close a writer and wait until its registry entry is gone.

  `WalWriter.close/1` is a `GenServer.stop/2`, so it returns once the process
  has exited — but `Elixir.Registry` drops the entry only when it processes the
  monitor's `:DOWN`, which happens after. A re-open in that window gets the
  dead pid back from `Supervisor.lookup/1` and the next call exits `:noproc`.
  """
  @spec close_and_await(pid(), Registry.entity_id(), timeout()) :: :ok
  def close_and_await(server, entity_id, timeout_ms \\ 2_000) do
    WalWriter.close(server)
    await_deregistered(entity_id, System.monotonic_time(:millisecond) + timeout_ms)
  end

  defp await_deregistered(entity_id, deadline) do
    case Supervisor.lookup(entity_id) do
      :error ->
        :ok

      {:ok, _pid} ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise "writer for #{inspect(entity_id)} still registered after close"
        end

        Process.sleep(5)
        await_deregistered(entity_id, deadline)
    end
  end
end
