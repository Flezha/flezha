package com.flezha.sdk.runtime.framing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FrameEncoderTest {

    @Test
    @DisplayName("spec §9.1 — handshake init frame has the correct byte layout")
    void handshakeInitFrameLayout() {
        String payloadId = "connect_control_channel";
        byte[] message = "{\"factory_ids\":[\"echo\"],\"namespace\":\"my_app\"}"
                .getBytes(StandardCharsets.UTF_8);

        byte[] frame = FrameEncoder.build(payloadId, message, Map.of());

        // Header: 32 bytes payload id (space-padded) + 4 bytes payload size.
        assertThat(frame).hasSizeGreaterThanOrEqualTo(36);

        byte[] paddedId = new byte[FrameEncoder.PAYLOAD_ID_WIDTH];
        System.arraycopy(frame, 0, paddedId, 0, FrameEncoder.PAYLOAD_ID_WIDTH);
        String paddedStr = new String(paddedId, StandardCharsets.UTF_8);
        assertThat(paddedStr.trim()).isEqualTo("connect_control_channel");

        int payloadSize = java.nio.ByteBuffer.wrap(frame, 32, 4).getInt();
        // payload_size = 4 (message size) + message.length + 2 (metadata size) + 0 (metadata)
        assertThat(payloadSize).isEqualTo(4 + message.length + 2);
        // Total size = 32 + 4 + payload_size.
        assertThat(frame).hasSize(32 + 4 + payloadSize);
    }

    @Test
    @DisplayName("metadata round-trip preserves keys and values")
    void metadataRoundtrip() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("input_type", "2");
        metadata.put("trace_id", "abc123");

        byte[] frame = FrameEncoder.build("p1", "hi".getBytes(StandardCharsets.UTF_8), metadata);
        FrameParser parser = new FrameParser();
        java.util.List<Frame> frames = parser.feed(frame);
        assertThat(frames).hasSize(1);
        Frame decoded = frames.get(0);
        assertThat(decoded.payloadId()).isEqualTo("p1");
        assertThat(decoded.metadata()).containsEntry("input_type", "2")
                .containsEntry("trace_id", "abc123");
    }

    @Test
    @DisplayName("metadata keys/values at boundary 0 and 255 bytes")
    void metadataBoundaries() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("", "");
        String key255 = "k".repeat(255);
        String val255 = "v".repeat(255);
        metadata.put(key255, val255);

        byte[] serialized = MetadataCodec.serialize(metadata);
        Map<String, String> roundtrip = MetadataCodec.deserialize(serialized);
        assertThat(roundtrip).containsEntry("", "").containsEntry(key255, val255);
    }

    @Test
    @DisplayName("metadata rejects > 255 byte key or value")
    void metadataTooLongRejected() {
        Map<String, String> tooLongKey = Map.of("k".repeat(256), "v");
        assertThatThrownBy(() -> MetadataCodec.serialize(tooLongKey))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key exceeds 255 bytes");

        Map<String, String> tooLongVal = Map.of("k", "v".repeat(256));
        assertThatThrownBy(() -> MetadataCodec.serialize(tooLongVal))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value exceeds 255 bytes");
    }

    @Test
    @DisplayName("empty metadata builds a valid frame")
    void emptyMetadata() {
        byte[] frame = FrameEncoder.build("p", new byte[0], Map.of());
        // total size = 32 + 4 + (4 + 0 + 2 + 0) = 42 bytes
        assertThat(frame).hasSize(32 + 4 + 4 + 2);

        FrameParser parser = new FrameParser();
        java.util.List<Frame> parsed = parser.feed(frame);
        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).message()).isEmpty();
        assertThat(parsed.get(0).metadata()).isEmpty();
    }
}
