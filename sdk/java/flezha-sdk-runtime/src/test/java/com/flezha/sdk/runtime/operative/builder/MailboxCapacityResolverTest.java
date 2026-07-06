package com.flezha.sdk.runtime.operative.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Java mirror of the Rust SDK's {@code resolve_mailbox_capacity_inner}
 * test cases ({@code sdk/rust/src/operative/builder.rs}).
 */
class MailboxCapacityResolverTest {

    @Test
    @DisplayName("explicit override on the Operation wins over env value")
    void overrideOnOperationTakesPrecedence() {
        int cap = MailboxCapacityResolver.resolveInner("ohlc", 4096, true, "2048");
        assertThat(cap).isEqualTo(4096);
    }

    @Test
    @DisplayName("env value is used when the Operation does not override")
    void envWinsOverDefault() {
        int cap = MailboxCapacityResolver.resolveInner("ohlc", 1024, false, "2048");
        assertThat(cap).isEqualTo(2048);
    }

    @Test
    @DisplayName("unparseable env value falls through to the default")
    void unparseableEnvFallsThroughToDefault() {
        int cap = MailboxCapacityResolver.resolveInner("ohlc", 1024, false, "banana");
        assertThat(cap).isEqualTo(MailboxCapacityResolver.DEFAULT_MAILBOX_CAPACITY);
    }

    @Test
    @DisplayName("missing env value falls through to the default")
    void missingEnvFallsThroughToDefault() {
        int cap = MailboxCapacityResolver.resolveInner("ohlc", 1024, false, null);
        assertThat(cap).isEqualTo(MailboxCapacityResolver.DEFAULT_MAILBOX_CAPACITY);
    }

    @Test
    @DisplayName("zero declared capacity is rejected by name")
    void zeroDeclaredIsRejected() {
        assertThatThrownBy(() -> MailboxCapacityResolver.resolveInner("ohlc", 0, true, null))
                .isInstanceOf(UnboundedOperativeConfigurationException.class)
                .hasMessageContaining("ohlc")
                .hasMessageContaining("mailboxCapacity = 0");
    }

    @Test
    @DisplayName("declared capacity above the upper bound is rejected by name + value")
    void tooLargeIsRejected() {
        assertThatThrownBy(() -> MailboxCapacityResolver.resolveInner(
                        "ohlc", MailboxCapacityResolver.MAX_MAILBOX_CAPACITY + 1, true, null))
                .isInstanceOf(UnboundedOperativeConfigurationException.class)
                .hasMessageContaining("ohlc")
                .hasMessageContaining(String.valueOf(MailboxCapacityResolver.MAX_MAILBOX_CAPACITY + 1));
    }

    @Test
    @DisplayName("zero declared override beats a valid env value (override always wins)")
    void zeroOverrideBeatsEnv() {
        assertThatThrownBy(() -> MailboxCapacityResolver.resolveInner("ohlc", 0, true, "4096"))
                .isInstanceOf(UnboundedOperativeConfigurationException.class)
                .hasMessageContaining("mailboxCapacity = 0");
    }

    @Test
    @DisplayName("capacity = 1 is allowed")
    void capacityOneIsAllowed() {
        int cap = MailboxCapacityResolver.resolveInner("tiny", 1, true, null);
        assertThat(cap).isEqualTo(1);
    }

    @Test
    @DisplayName("capacity = MAX_MAILBOX_CAPACITY is allowed")
    void maxIsAllowed() {
        int cap = MailboxCapacityResolver.resolveInner(
                "max_operation", MailboxCapacityResolver.MAX_MAILBOX_CAPACITY, true, null);
        assertThat(cap).isEqualTo(MailboxCapacityResolver.MAX_MAILBOX_CAPACITY);
    }
}
