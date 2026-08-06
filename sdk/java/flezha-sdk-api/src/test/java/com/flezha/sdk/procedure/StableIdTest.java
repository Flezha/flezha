package com.flezha.sdk.procedure;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * {@link StableId} is a cross-language contract: a user may embed the rendered
 * id in an emitted key, so Java's hand-rolled 128-bit FNV-1a (Java has no
 * {@code u128}) must agree with the Rust SDK's {@code StableId::derive} bit for
 * bit. The twin assertions live in Rust's
 * {@code stable_id_matches_the_cross_language_fixture}.
 */
class StableIdTest {

    private static byte[] identity() {
        return "instance/op/key".getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void matchesTheRustCrossLanguageFixture() {
        assertThat(StableId.derive(identity(), 42, 0).toString()).isEqualTo("bbf81fd4955afd1724203700a240e822");
        assertThat(StableId.derive(identity(), 42, 1).toString()).isEqualTo("bbf81fd4965afd1724203700a240e95d");
        assertThat(StableId.derive(new byte[0], 0, 0).toString()).isEqualTo("15915375576d2f389a938dc3a9940c9d");
    }

    @Test
    void rendersAs32LowercaseHexDigits() {
        String rendered = StableId.derive(identity(), 1, 0).toString();
        assertThat(rendered).hasSize(32).matches("[0-9a-f]{32}");
    }

    @Test
    void differsByOrdinalSeqAndIdentity() {
        StableId base = StableId.derive(identity(), 42, 0);
        assertThat(base).isNotEqualTo(StableId.derive(identity(), 42, 1));
        assertThat(base).isNotEqualTo(StableId.derive(identity(), 43, 0));
        assertThat(base).isNotEqualTo(StableId.derive("other/op/key".getBytes(StandardCharsets.UTF_8), 42, 0));
    }

    @Test
    void isDeterministicAcrossCalls() {
        assertThat(StableId.derive(identity(), 42, 0)).isEqualTo(StableId.derive(identity(), 42, 0));
    }
}
