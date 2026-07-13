package com.flezha.sdk.runtime.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The effect record is decoded by the controller
 * ({@code Flezha.State.Wire.decode_effect_body/1}), so its postcard bytes are a
 * cross-language contract. This fixture is the same one Rust pins in
 * {@code flusher.rs::effect_record_body_postcard_bytes_are_the_cross_language_fixture}
 * and Elixir pins in {@code wire_test.exs} — an encoding change breaks loudly
 * in all three.
 */
class EffectRecordTest {

    @Test
    @DisplayName("effect-record postcard bytes are the cross-language fixture")
    void postcardBytesAreTheCrossLanguageFixture() {
        EffectRecord body = new EffectRecord(
                List.of(new byte[] {(byte) 0xAA}),
                List.of(new EffectRecord.OutputRecord("out", "k", List.of(Map.entry("a", "b")), new byte[] {1, 2})),
                OptionalLong.of(4711),
                Optional.empty(),
                1_700_000_000_123L);

        assertThat(body.encode())
                .containsExactly(
                        0x01, 0x01, 0xAA, // changes: 1 × [0xAA]
                        0x01, 0x03, 'o', 'u', 't', 0x01, 'k', // outputs: 1 × channel "out", key "k"
                        0x01, 0x01, 'a', 0x01, 'b', // metadata: 1 × ("a","b")
                        0x02, 0x01, 0x02, // payload [1,2]
                        0x01, 0xE7, 0x24, // Some(4711)
                        0x00, // dlq: None
                        0xFB, 0xD0, 0x95, 0xFF, 0xBC, 0x31); // event_time: 1700000000123
    }

    @Test
    @DisplayName("a parked poison invocation carries its DLQ entry and its offset in ONE record")
    void parkRidesInsideTheEffectRecord() {
        EffectRecord body = new EffectRecord(
                List.of(),
                List.of(),
                OptionalLong.of(9),
                Optional.of(new DlqPark.DlqEntry("in", "boom", 3, Optional.of(new byte[] {7}))),
                42L);

        byte[] bytes = body.encode();
        Postcard.Reader r = new Postcard.Reader(bytes);

        assertThat(r.vec(Postcard.Reader::bytes)).isEmpty(); // changes
        assertThat(r.vec(x -> x)).isEmpty(); // outputs
        assertThat(r.optionalVarint()).hasValue(9); // the offset advances...
        assertThat(r.some()).isTrue(); // ...in the SAME record as the park
        assertThat(r.string()).isEqualTo("in");
        assertThat(r.string()).isEqualTo("boom");
        assertThat(r.varint()).isEqualTo(3);
        assertThat(r.optionalBytes()).get().isEqualTo(new byte[] {7});
        assertThat(r.varint()).isEqualTo(42);
        assertThat(r.exhausted()).isTrue();
    }
}
