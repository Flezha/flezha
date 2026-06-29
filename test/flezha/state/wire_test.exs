defmodule Flezha.State.WireTest do
  use ExUnit.Case, async: true

  alias Flezha.State.Wire

  describe "encode_segment_id / decode_segment_id" do
    test "round-trips structured identity" do
      bin = Wire.encode_segment_id("MarketOperative", "OhlcOperation", "M1")
      assert {:ok, {"MarketOperative", "OhlcOperation", "M1"}} = Wire.decode_segment_id(bin)
    end

    test "rejects operative_class > 255 bytes" do
      assert_raise ArgumentError, fn ->
        Wire.encode_segment_id(String.duplicate("x", 256), "S", "P")
      end
    end

    test "rejects segment_key > 65535 bytes" do
      assert_raise ArgumentError, fn ->
        Wire.encode_segment_id("R", "S", String.duplicate("x", 65_536))
      end
    end

    test "decode error for malformed bytes" do
      assert {:error, :bad_segment_id} = Wire.decode_segment_id(<<0xFF>>)
    end
  end

  describe "decode_frame" do
    test "round-trips a well-formed frame" do
      identity = Wire.encode_segment_id("R", "S", "PK")
      seq = 42
      header = <<1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0>>
      body = "hello world"
      crc = :erlang.crc32(body)

      frame =
        <<byte_size(identity)::16-big, identity::binary, seq::64-big, header::binary,
          byte_size(body)::32-big, body::binary, crc::32-big>>

      assert {:ok, decoded} = Wire.decode_frame(frame)
      assert decoded.segment_id == {"R", "S", "PK"}
      assert decoded.seq == 42
      assert decoded.header == header
      assert decoded.body == body
    end

    test "detects crc mismatch" do
      identity = Wire.encode_segment_id("R", "S", "PK")
      seq = 1
      header = <<1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0>>
      body = "x"
      bad_crc = :erlang.crc32(body) + 1

      frame =
        <<byte_size(identity)::16-big, identity::binary, seq::64-big, header::binary,
          byte_size(body)::32-big, body::binary, bad_crc::32-big>>

      assert {:error, :crc_mismatch} = Wire.decode_frame(frame)
    end

    test "detects short read" do
      assert {:error, :short_read} = Wire.decode_frame(<<0, 5>>)
    end
  end
end
