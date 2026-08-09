package com.flezha.sdk.runtime.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The controller-shared Entity identity framing pin. */
class EntityIdTest {

    @Test
    @DisplayName("encodedBytes matches the controller framing: [rc_len][rc][sh_len][sh][pk_len:u16 BE][pk]")
    void encodedBytesMatchesControllerFraming() {
        byte[] id = new EntityId("MarketUnboundedOperative", "OhlcProcedure", "M1").encodedBytes();
        assertThat(id[0]).isEqualTo((byte) 24);
        assertThat(new String(id, 1, 24, StandardCharsets.UTF_8)).isEqualTo("MarketUnboundedOperative");
        assertThat(id[25]).isEqualTo((byte) 13);
        assertThat(new String(id, 26, 13, StandardCharsets.UTF_8)).isEqualTo("OhlcProcedure");
        ByteBuffer pk = ByteBuffer.wrap(id, 39, 4).order(ByteOrder.BIG_ENDIAN);
        assertThat(pk.getShort() & 0xFFFF).isEqualTo(2);
        assertThat(new String(id, 41, 2, StandardCharsets.UTF_8)).isEqualTo("M1");
        assertThat(id.length).isEqualTo(43);
    }
}
