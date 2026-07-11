defmodule FlezhaTest.Executor.AddOperativeResponseTest do
  @moduledoc """
  §1.2: the SDK declares the effectively-once tier in the `AddOperative`
  response metadata (`AddOperativeResponse.raw_data`); the controller
  resolves absent, empty, or undecodable metadata to the tier-off default.
  """
  use ExUnit.Case, async: true

  alias Flezha.Operative.Overwatch

  test "tier declaration in the response metadata resolves true" do
    raw = encode(%{"effectively_once" => "true"})
    assert Overwatch.decode_effectively_once(raw)
  end

  test "metadata without the tier key resolves false" do
    refute Overwatch.decode_effectively_once(encode(%{"other" => "x"}))
    refute Overwatch.decode_effectively_once(encode(%{"effectively_once" => "false"}))
  end

  test "empty raw_data (pre-tier or Java executor) resolves false" do
    refute Overwatch.decode_effectively_once("")
    refute Overwatch.decode_effectively_once(nil)
  end

  test "undecodable raw_data resolves false" do
    refute Overwatch.decode_effectively_once(<<255, 255, 255, 1>>)
  end

  defp encode(metadata) do
    {:ok, iodata, _size} =
      Protox.encode(%ExecutorCommands.AddOperativeResponse{metadata: metadata})

    IO.iodata_to_binary(iodata)
  end
end
