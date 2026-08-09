package com.flezha.sdk.runtime.core;

import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * The Entity an activation is addressed to. {@code entityKey} is empty only
 * for pre-keying work ({@link ActivationKind.KeyRequest}), where the key is
 * what's being computed.
 */
public record EntityRef(@Nonnull String operative, int procedure, @Nonnull Optional<String> entityKey) {}
