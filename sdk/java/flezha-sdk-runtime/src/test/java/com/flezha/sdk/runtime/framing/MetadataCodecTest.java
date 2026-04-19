package com.flezha.sdk.runtime.framing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MetadataCodecTest {

    @Test
    @DisplayName("empty metadata serializes to zero bytes")
    void emptyMetadata() {
        assertThat(MetadataCodec.serialize(Map.of())).isEmpty();
        assertThat(MetadataCodec.deserialize(new byte[0])).isEmpty();
    }

    @Test
    @DisplayName("wire layout: [key_len(u8)][val_len(u8)][key][value]")
    void wireLayout() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("k", "v");
        byte[] bytes = MetadataCodec.serialize(metadata);
        assertThat(bytes).containsExactly(0x01, 0x01, (byte) 'k', (byte) 'v');
    }

    @Test
    @DisplayName("roundtrip preserves insertion order")
    void roundtripOrder() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("a", "1");
        metadata.put("b", "2");
        metadata.put("c", "3");
        Map<String, String> back = MetadataCodec.deserialize(MetadataCodec.serialize(metadata));
        assertThat(back).containsExactlyEntriesOf(metadata);
    }
}
