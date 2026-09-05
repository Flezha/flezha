package com.flezha.sdk.runtime.operative.builder;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Java mirror of the Rust SDK's {@code resolve_mailbox_capacity_inner}
 * test cases ({@code sdk/rust/src/operative/builder.rs}). The resolver
 * RESOLVES and nothing else: an out-of-range value is written into the record
 * as resolved, and {@code flz_core} names it — those cases live in
 * {@code BoundedMailboxTopologyTest}.
 */
class MailboxCapacityResolverTest {

    @Test
    @DisplayName("explicit override on the Procedure wins over env value")
    void overrideOnProcedureTakesPrecedence() {
        assertThat(MailboxCapacityResolver.resolveInner(4096, true, "2048")).isEqualTo(4096);
    }

    @Test
    @DisplayName("env value is used when the Procedure does not override")
    void envWinsOverDefault() {
        assertThat(MailboxCapacityResolver.resolveInner(1024, false, "2048")).isEqualTo(2048);
    }

    @Test
    @DisplayName("unparseable env value falls through to the default")
    void unparseableEnvFallsThroughToDefault() {
        assertThat(MailboxCapacityResolver.resolveInner(1024, false, "banana"))
                .isEqualTo(MailboxCapacityResolver.DEFAULT_MAILBOX_CAPACITY);
    }

    @Test
    @DisplayName("missing env value falls through to the default")
    void missingEnvFallsThroughToDefault() {
        assertThat(MailboxCapacityResolver.resolveInner(1024, false, null))
                .isEqualTo(MailboxCapacityResolver.DEFAULT_MAILBOX_CAPACITY);
    }

    @Test
    @DisplayName("a zero declared override is resolved as declared, for the core to judge")
    void zeroDeclaredIsResolvedNotRefused() {
        assertThat(MailboxCapacityResolver.resolveInner(0, true, null)).isZero();
    }

    @Test
    @DisplayName("a declared override above the upper bound is resolved as declared")
    void tooLargeIsResolvedNotRefused() {
        assertThat(MailboxCapacityResolver.resolveInner(MailboxCapacityResolver.MAX_MAILBOX_CAPACITY + 1, true, null))
                .isEqualTo(MailboxCapacityResolver.MAX_MAILBOX_CAPACITY + 1);
    }

    @Test
    @DisplayName("zero declared override beats a valid env value (override always wins)")
    void zeroOverrideBeatsEnv() {
        assertThat(MailboxCapacityResolver.resolveInner(0, true, "4096")).isZero();
    }

    @Test
    @DisplayName("capacity = 1 is allowed")
    void capacityOneIsAllowed() {
        assertThat(MailboxCapacityResolver.resolveInner(1, true, null)).isEqualTo(1);
    }

    @Test
    @DisplayName("capacity = MAX_MAILBOX_CAPACITY is allowed")
    void maxIsAllowed() {
        assertThat(MailboxCapacityResolver.resolveInner(MailboxCapacityResolver.MAX_MAILBOX_CAPACITY, true, null))
                .isEqualTo(MailboxCapacityResolver.MAX_MAILBOX_CAPACITY);
    }
}
