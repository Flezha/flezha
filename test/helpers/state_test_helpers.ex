defmodule Flezha.State.TestHelpers do
  @moduledoc """
  Fixtures shared across the persistence-module test suite, plus
  `override_env/2` — the one app-env override every test overriding a
  `:flezha` key goes through.

  Lives under `test/helpers/`, which `elixirc_paths` compiles in `:dev` and
  `:test` but not `:prod`: `reset_all_entities/0` wipes the state root, and
  that must not be reachable from a release.
  """

  alias Flezha.State.{Registry, WalWriter}

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
  Construct a deterministic `entity_id` tuple usable as a fixture.
  """
  @spec sample_entity_id(binary()) :: Registry.entity_id()
  def sample_entity_id(suffix \\ "p1") do
    {"TestOperative", "TestProcedure", suffix}
  end
end
