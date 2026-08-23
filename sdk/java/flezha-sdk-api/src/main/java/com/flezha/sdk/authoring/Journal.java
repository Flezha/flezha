package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares per-key journaling for a {@link State} map field, mirroring the Rust
 * {@code #[journal(...)]} field attribute. The {@code @State} processor
 * generates named per-key mutators, matching {@code Change} variants, and the
 * {@code applyChange} arms that replay them. See
 * {@code docs/sdk/authoring_contract.md} §3.
 *
 * <ul>
 *   <li>{@link Mode#MAP_SET} on {@code Map<K, V>} → {@code set<Field>(key, V)}.</li>
 *   <li>{@link Mode#MAP_VEC} on {@code Map<K, List<U>>} → {@code push<Field>(key, U)},
 *       {@code drain<Field>Front(key, int)}, {@code replaceLast<Field>(key, U)}.</li>
 *   <li>{@link Mode#MAP_DEQUE} on {@code Map<K, Deque<U>>} → {@code pushBack<Field>(key, U)},
 *       {@code popFront<Field>(key, int)}.</li>
 * </ul>
 *
 * <p>A map field whose per-entry mutation is not expressible by any mode needs a
 * hand-written {@link com.flezha.sdk.state.Change}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.FIELD)
public @interface Journal {

    /** Per-key journaling mode; must match the field's map value type. */
    Mode value();

    /** The journaling shape a map field opts into. */
    enum Mode {
        MAP_SET,
        MAP_VEC,
        MAP_DEQUE
    }
}
