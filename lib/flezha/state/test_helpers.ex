defmodule Flezha.State.TestHelpers do
  @moduledoc """
  Fixtures shared across the persistence-module test suite.

  Compiled in all envs because `lib/flezha/state/` modules are exercised
  from integration tests outside the unit-test tree; the helpers carry
  no runtime side effects unless invoked.
  """

  alias Flezha.State.{Registry, WalWriter}

  @doc """
  Allocates a per-test temporary directory under `System.tmp_dir!/0` and
  registers it as the agent's `:state_root`. Callers should
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
  Build a 16-byte WAL header. The agent treats the header as opaque
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
  Construct a deterministic `partition_id` tuple usable as a fixture.
  """
  @spec sample_partition_id(binary()) :: Registry.partition_id()
  def sample_partition_id(suffix \\ "p1") do
    {"ce-test", "TestRelay", "TestShard", suffix}
  end
end
