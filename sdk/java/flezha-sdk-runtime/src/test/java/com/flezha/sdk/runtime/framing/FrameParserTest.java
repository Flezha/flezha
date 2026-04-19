package com.flezha.sdk.runtime.framing;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FrameParserTest {

    @Test
    @DisplayName("frame split across two reads is reassembled")
    void splitRead() {
        byte[] frame = FrameEncoder.build("p", "hello".getBytes(StandardCharsets.UTF_8), Map.of());
        int split = 40;
        byte[] chunk1 = new byte[split];
        byte[] chunk2 = new byte[frame.length - split];
        System.arraycopy(frame, 0, chunk1, 0, split);
        System.arraycopy(frame, split, chunk2, 0, chunk2.length);

        FrameParser parser = new FrameParser();
        assertThat(parser.feed(chunk1)).isEmpty();
        List<Frame> parsed = parser.feed(chunk2);
        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).payloadId()).isEqualTo("p");
        assertThat(new String(parsed.get(0).message(), StandardCharsets.UTF_8)).isEqualTo("hello");
    }

    @Test
    @DisplayName("multiple frames in one read are all emitted")
    void multipleFramesInOneChunk() {
        byte[] f1 = FrameEncoder.build("a", new byte[]{1, 2, 3}, Map.of());
        byte[] f2 = FrameEncoder.build("b", new byte[]{4, 5, 6, 7}, Map.of());
        byte[] joined = new byte[f1.length + f2.length];
        System.arraycopy(f1, 0, joined, 0, f1.length);
        System.arraycopy(f2, 0, joined, f1.length, f2.length);

        FrameParser parser = new FrameParser();
        List<Frame> parsed = parser.feed(joined);
        assertThat(parsed).hasSize(2);
        assertThat(parsed.get(0).payloadId()).isEqualTo("a");
        assertThat(parsed.get(1).payloadId()).isEqualTo("b");
    }

    @Test
    @DisplayName("byte-by-byte feed reassembles the frame")
    void byteByByteFeed() {
        byte[] frame = FrameEncoder.build("slow", "one byte at a time".getBytes(StandardCharsets.UTF_8), Map.of());
        FrameParser parser = new FrameParser();
        Frame decoded = null;
        for (int i = 0; i < frame.length; i++) {
            List<Frame> emitted = parser.feed(new byte[]{frame[i]});
            if (!emitted.isEmpty()) {
                decoded = emitted.get(0);
            }
        }
        assertThat(decoded).isNotNull();
        assertThat(decoded.payloadId()).isEqualTo("slow");
    }
}
