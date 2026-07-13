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

  describe "effect record body (effectively-once tier)" do
    # The exact postcard bytes the Rust SDK emits for this body — pinned on
    # the SDK side by flusher::tests::
    # effect_record_body_postcard_bytes_are_the_cross_language_fixture.
    @rust_fixture <<0x01, 0x01, 0xAA, 0x01, 0x03, ?o, ?u, ?t, 0x01, ?k, 0x01, 0x01, ?a, 0x01, ?b,
                    0x02, 0x01, 0x02, 0x01, 0xE7, 0x24, 0x00, 0xFB, 0xD0, 0x95, 0xFF, 0xBC, 0x31>>

    test "decodes the Rust SDK's postcard bytes" do
      assert {:ok, body} = Wire.decode_effect_body(@rust_fixture)
      assert body.changes == [<<0xAA>>]
      assert body.consumed_offset == 4711
      assert body.dlq == nil
      assert body.event_time == 1_700_000_000_123

      assert [%{channel: "out", key: "k", metadata: [{"a", "b"}], payload: <<1, 2>>}] =
               body.outputs
    end

    test "encode is the byte-exact inverse" do
      {:ok, body} = Wire.decode_effect_body(@rust_fixture)
      assert Wire.encode_effect_body(body) == @rust_fixture
    end

    test "round-trips empty and None fields" do
      body = %{changes: [], outputs: [], consumed_offset: nil, dlq: nil, event_time: 0}
      encoded = Wire.encode_effect_body(body)
      assert {:ok, ^body} = Wire.decode_effect_body(encoded)
    end

    test "round-trips a dead-letter park entry (atomic tier-on park)" do
      body = %{
        changes: [],
        outputs: [],
        consumed_offset: 99,
        dlq: %{channel: "in", error: "boom", attempts: 3, payload: <<1, 2>>},
        event_time: 1_720_000_000_000
      }

      assert {:ok, ^body} = body |> Wire.encode_effect_body() |> Wire.decode_effect_body()

      byteless = %{body | dlq: %{channel: "in", error: "boom", attempts: 1, payload: nil}}
      assert {:ok, ^byteless} = byteless |> Wire.encode_effect_body() |> Wire.decode_effect_body()
    end

    test "round-trips multi-byte varints and multiple entries" do
      body = %{
        changes: [<<1, 2, 3>>, :binary.copy(<<9>>, 300)],
        outputs: [
          %{channel: "candles", key: "BTC/1m/1720000000", metadata: [], payload: <<0>>},
          %{
            channel: "candles",
            key: "ETH/1m",
            metadata: [{"a", "b"}, {"c", "d"}],
            payload: :binary.copy(<<7>>, 200)
          }
        ],
        consumed_offset: 1_000_000_007,
        dlq: nil,
        event_time: 1_720_000_000_000
      }

      assert {:ok, ^body} = body |> Wire.encode_effect_body() |> Wire.decode_effect_body()
    end

    test "rejects trailing bytes and truncation" do
      assert {:error, :trailing_bytes} = Wire.decode_effect_body(@rust_fixture <> <<0>>)

      truncated = binary_part(@rust_fixture, 0, byte_size(@rust_fixture) - 1)
      assert {:error, _} = Wire.decode_effect_body(truncated)
    end

    test "decodes the SDKs' ParkBody postcard bytes (cross-language fixture)" do
      # Pinned on the SDK sides by park.rs
      # `park_body_postcard_bytes_are_the_cross_language_fixture` and the
      # Java DlqRetryTest fixture.
      fixture =
        <<0x03, ?s, ?#, ?0, 0x01, 0x07, 0x02, ?i, ?n, 0x04, ?b, ?o, ?o, ?m, 0x03, 0x01, 0x02,
          0x01, 0x02>>

      assert {:ok, park} = Wire.decode_park_body(fixture)
      assert park.park_id == "s#0"
      assert park.channel == "in"
      assert park.origin_offset == 7
      assert park.error == "boom"
      assert park.attempts == 3
      assert park.payload == <<1, 2>>

      byteless = <<0x01, ?x, 0x00, 0x02, ?i, ?n, 0x01, ?e, 0x01, 0x00>>
      assert {:ok, park} = Wire.decode_park_body(byteless)
      assert park.origin_offset == nil
      assert park.payload == nil

      assert {:error, :trailing_bytes} = Wire.decode_park_body(fixture <> <<0>>)
      assert {:error, _} = Wire.decode_park_body(binary_part(fixture, 0, 5))
    end

    test "effect_record_header? keys on header byte 9" do
      effect_header = <<1, 0, 0, 5, 3, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0>>
      plain_header = <<1, 0, 0, 5, 3, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0>>
      assert Wire.effect_record_header?(effect_header)
      refute Wire.effect_record_header?(plain_header)
      refute Wire.effect_record_header?(<<>>)
    end
  end
end
