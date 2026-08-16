defmodule FlezhaTest.Runner.AddOperativeResponseTest do
  @moduledoc """
  The SDK declares its class-level capabilities in the `AddOperative` response
  metadata (`AddOperativeResponse.raw_data`) — the effectively-once tier and the
  chain positions that can compensate. The controller resolves absent, empty, or
  undecodable metadata to the defaults.
  """
  use ExUnit.Case, async: true

  alias Flezha.Operative.ClassRegistry

  defp declarations(metadata), do: ClassRegistry.decode_declarations(encode(metadata))

  test "tier declaration in the response metadata resolves true" do
    assert declarations(%{"effectively_once" => "true"}).effectively_once
  end

  test "metadata without the tier key resolves false" do
    refute declarations(%{"other" => "x"}).effectively_once
    refute declarations(%{"effectively_once" => "false"}).effectively_once
  end

  test "empty raw_data (pre-tier or Java runner) resolves the defaults" do
    for raw <- ["", nil] do
      assert ClassRegistry.decode_declarations(raw) == %{
               effectively_once: false,
               keyless: false,
               checkpoint_ack: false,
               compensable_procs: nil,
               leaf_procs: nil,
               correction_window_ms: nil,
               compact_after_bytes: nil,
               sync_durability: false
             }
    end
  end

  test "undecodable raw_data resolves the defaults" do
    assert ClassRegistry.decode_declarations(<<255, 255, 255, 1>>) == %{
             effectively_once: false,
             keyless: false,
             checkpoint_ack: false,
             compensable_procs: nil,
             leaf_procs: nil,
             correction_window_ms: nil,
             compact_after_bytes: nil,
             sync_durability: false
           }
  end

  test "keyless is declared only when true — a keyed class says nothing" do
    # `partitioned-operatives`: the SDK announces `keyed: "false"` for a class
    # with no keyed entry-fed procedure, so the controller can refuse a
    # partitioned start of it before touching a runner. A keyed class declares
    # nothing, which is what keeps its response byte-identical.
    assert declarations(%{"keyed" => "false"}).keyless
    refute declarations(%{}).keyless
    refute declarations(%{"keyed" => "true"}).keyless
  end

  test "a declared compact_after_bytes resolves from the metadata; absent or invalid is nil" do
    assert declarations(%{"compact_after_bytes" => "4194304"}).compact_after_bytes == 4_194_304
    # A class that declares nothing lets the global default govern.
    assert declarations(%{"other" => "x"}).compact_after_bytes == nil
    assert declarations(%{"compact_after_bytes" => "0"}).compact_after_bytes == nil
    assert declarations(%{"compact_after_bytes" => "nope"}).compact_after_bytes == nil
  end

  test "a correction window resolves from the metadata; absent or invalid is nil" do
    assert declarations(%{"correction_window_ms" => "600000"}).correction_window_ms == 600_000
    # A class that declares nothing never corrects — the cost-free default.
    assert declarations(%{"other" => "x"}).correction_window_ms == nil
    assert declarations(%{"correction_window_ms" => "0"}).correction_window_ms == nil
    assert declarations(%{"correction_window_ms" => "nope"}).correction_window_ms == nil
  end

  test "checkpoint ack-granularity resolves from the metadata; anything else is per-input" do
    assert declarations(%{"ack_granularity" => "checkpoint"}).checkpoint_ack
    # Per-input is the tier's default granularity — an undeclaring class keeps it.
    refute declarations(%{"effectively_once" => "true"}).checkpoint_ack
    refute declarations(%{"ack_granularity" => "per_input"}).checkpoint_ack
  end

  test "synchronous remote durability resolves from the metadata; anything else is async" do
    assert declarations(%{"durability" => "sync_remote"}).sync_durability
    # Async is the cost-free default: a class that declares nothing never makes
    # a commit barrier wait on a remote store.
    refute declarations(%{"other" => "x"}).sync_durability
    refute declarations(%{"durability" => "async"}).sync_durability
  end

  test "compensable chain positions resolve to a list" do
    assert declarations(%{"compensable_procs" => "0,2,3"}).compensable_procs == [0, 2, 3]
  end

  test "a declared-but-empty compensable list is not the same as an absent one" do
    # "" = a structured chain, none of it compensable (a rewind of an emitting proc
    # is refused); absent = the class declares nothing at all (no information).
    assert declarations(%{"compensable_procs" => ""}).compensable_procs == []
    assert declarations(%{"other" => "x"}).compensable_procs == nil
  end

  test "leaf positions resolve to a list — the tree's completion rule" do
    # A→(B,C): the operative is done when procs 1 and 2 have committed.
    assert declarations(%{"leaf_procs" => "1,2"}).leaf_procs == [1, 2]
    # A linear chain's single terminal is the degenerate case.
    assert declarations(%{"leaf_procs" => "1"}).leaf_procs == [1]
    # Absent = run-sugar / stream / an SDK that does not declare it — the
    # controller cannot derive completion and must not guess.
    assert declarations(%{"other" => "x"}).leaf_procs == nil
  end

  defp encode(metadata) do
    {:ok, iodata, _size} =
      Protox.encode(%RunnerCommands.AddOperativeResponse{metadata: metadata})

    IO.iodata_to_binary(iodata)
  end
end
