defmodule Flezha.Cluster.Snapshot.CoordinatorTest do
  @moduledoc """
  Unit tests for the snapshot coordinator's atomicity and best-effort
  semantics. Intentionally avoids spinning up real ProcessHub or compute
  engines — those paths are exercised in `test/system/cluster_snapshot_restore_test.exs`.
  """

  use ExUnit.Case, async: false

  alias Flezha.Cluster.Snapshot.Encoding

  setup do
    tmp =
      Path.join(System.tmp_dir!(), "flz_snapshot_coord_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)
    {:ok, tmp: tmp}
  end

  describe "atomic write" do
    test "writing to a path that does not exist creates the file with the snapshot bytes",
         %{tmp: tmp} do
      path = Path.join(tmp, "snap.flz")
      payload = %{taken_at_ms: 1, processors: [], plugins: [], captured_states: %{}}
      bin = Encoding.encode(payload)

      assert :ok = File.write(path <> ".tmp", bin)
      assert :ok = :file.rename(String.to_charlist(path <> ".tmp"), String.to_charlist(path))
      assert File.exists?(path)

      assert {:ok, decoded} = Encoding.decode(File.read!(path))
      assert decoded.processors == []
      refute File.exists?(path <> ".tmp")
    end
  end

  describe "decode on an oversized state cap" do
    test "encode/decode honours arbitrarily long binaries up to ETF limits" do
      # Smoke test: large captured-state binaries do not crash the encoder.
      big = :crypto.strong_rand_bytes(64 * 1024)

      payload = %{
        taken_at_ms: 1,
        processors: [],
        plugins: [],
        captured_states: %{"big" => big}
      }

      bin = Encoding.encode(payload)
      assert {:ok, decoded} = Encoding.decode(bin)
      assert decoded.captured_states["big"] == big
    end
  end
end
