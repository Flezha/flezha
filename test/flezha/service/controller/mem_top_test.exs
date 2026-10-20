defmodule Flezha.Service.Controller.MemTopTest do
  # async: false: the live controller and its fleet.
  use ExUnit.Case, async: false

  alias Flezha.Service.Controller.MemTop

  describe "the mem-top snapshot" do
    test "returns the documented keys with reasonable shapes" do
      snapshot = MemTop.snapshot()

      assert is_binary(snapshot.captured_at)
      assert is_binary(snapshot.node)

      mem = snapshot.erlang_memory
      assert is_map(mem)
      assert is_integer(mem.total) and mem.total > 0
      assert is_integer(mem.binary) and mem.binary >= 0
      assert is_integer(mem.processes) and mem.processes > 0
      assert is_integer(mem.ets) and mem.ets >= 0

      top = snapshot.top_processes
      assert is_list(top)
      assert length(top) <= 20
      assert top != [], "expected ≥ 1 BEAM process in the snapshot"

      # Descending order by :memory.
      memories = Enum.map(top, & &1.memory)
      assert memories == Enum.sort(memories, :desc)

      first = hd(top)

      for k <- [
            :pid,
            :memory,
            :initial_call,
            :current_function,
            :message_queue_len,
            :heap_size,
            :total_heap_size
          ] do
        assert Map.has_key?(first, k), "top_processes missing #{k}: #{inspect(first)}"
      end

      ets = snapshot.ets_tables
      assert is_list(ets)
      ets_memories = Enum.map(ets, & &1.memory_bytes)
      assert ets_memories == Enum.sort(ets_memories, :desc)

      assert is_list(snapshot.dets_files)

      assert is_map(snapshot.recon_alloc_allocated_types)
    end

    test "snapshot is JSON-encodable" do
      json = snapshot_json()
      assert is_binary(json)
      assert {:ok, decoded} = Jason.decode(json)
      assert is_map(decoded)
      assert is_list(decoded["top_processes"])
      assert is_map(decoded["erlang_memory"])
    end

    defp snapshot_json do
      Jason.encode!(MemTop.snapshot())
    end
  end
end
