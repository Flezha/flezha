defmodule FlezhaTest.Runner.AddOperativeResponseTest do
  @moduledoc """
  The SDK declares its class-level capabilities in the `AddOperative` response
  metadata (`AddOperativeResponse.raw_data`) — the effectively-once tier and the
  chain positions that can compensate. The controller resolves absent, empty, or
  undecodable metadata to the defaults.
  """
  use ExUnit.Case, async: true

  alias Flezha.Operative.Overwatch

  defp declarations(metadata), do: Overwatch.decode_declarations(encode(metadata))

  test "tier declaration in the response metadata resolves true" do
    assert declarations(%{"effectively_once" => "true"}).effectively_once
  end

  test "metadata without the tier key resolves false" do
    refute declarations(%{"other" => "x"}).effectively_once
    refute declarations(%{"effectively_once" => "false"}).effectively_once
  end

  test "empty raw_data (pre-tier or Java runner) resolves the defaults" do
    for raw <- ["", nil] do
      assert Overwatch.decode_declarations(raw) == %{
               effectively_once: false,
               checkpoint_ack: false,
               compensable_ops: nil,
               leaf_ops: nil,
               correction_window_ms: nil
             }
    end
  end

  test "undecodable raw_data resolves the defaults" do
    assert Overwatch.decode_declarations(<<255, 255, 255, 1>>) == %{
             effectively_once: false,
             checkpoint_ack: false,
             compensable_ops: nil,
             leaf_ops: nil,
             correction_window_ms: nil
           }
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

  test "compensable chain positions resolve to a list" do
    assert declarations(%{"compensable_ops" => "0,2,3"}).compensable_ops == [0, 2, 3]
  end

  test "a declared-but-empty compensable list is not the same as an absent one" do
    # "" = a structured chain, none of it compensable (a rewind of an emitting op
    # is refused); absent = the class declares nothing at all (no information).
    assert declarations(%{"compensable_ops" => ""}).compensable_ops == []
    assert declarations(%{"other" => "x"}).compensable_ops == nil
  end

  test "leaf positions resolve to a list — the tree's completion rule" do
    # A→(B,C): the operative is done when ops 1 and 2 have committed.
    assert declarations(%{"leaf_ops" => "1,2"}).leaf_ops == [1, 2]
    # A linear chain's single terminal is the degenerate case.
    assert declarations(%{"leaf_ops" => "1"}).leaf_ops == [1]
    # Absent = run-sugar / unbounded / an SDK that does not declare it — the
    # controller cannot derive completion and must not guess.
    assert declarations(%{"other" => "x"}).leaf_ops == nil
  end

  defp encode(metadata) do
    {:ok, iodata, _size} =
      Protox.encode(%RunnerCommands.AddOperativeResponse{metadata: metadata})

    IO.iodata_to_binary(iodata)
  end
end
