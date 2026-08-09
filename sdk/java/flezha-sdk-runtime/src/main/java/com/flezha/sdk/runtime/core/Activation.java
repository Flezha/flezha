package com.flezha.sdk.runtime.core;

import javax.annotation.Nonnull;

/**
 * One unit of work handed out by the poll core ({@code flz_core}'s
 * {@code Activation}). Targets exactly one Entity (or one not-yet-keyed edge
 * for {@link ActivationKind.KeyRequest}) and is exclusive: the core hands out
 * no second activation for the same Entity until this one's
 * {@link ActivationResult} arrives.
 *
 * <p>{@code factory} is the declared class's factory id, so the host can
 * resolve which registered class the instance belongs to.
 *
 * <p>Postcard-encoded — field order is the wire contract, schema version
 * {@code 2}. Pinned cross-language by {@code ActivationCodecTest}.
 */
public record Activation(
        long token, @Nonnull EntityRef target, @Nonnull ActivationKind kind, long changeSeq, @Nonnull String factory) {}
