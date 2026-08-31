package com.flezha.sdk.processor;

import java.util.List;

/**
 * What a {@code @State} class resolves to: the two sources to write, and one
 * entry per generated {@code Change} variant. Every string is already the text
 * the renderer emits, so rendering reads no {@code javax.lang.model} type.
 */
record StateModel(
        String pkg,
        String stateFqn,
        String changeSimple,
        String changeFqn,
        String handleSimple,
        String handleFqn,
        List<Variant> variants) {

    /**
     * One generated {@code Change} variant: the nested class, the static factory
     * and handle mutator that mint it, and the body of its {@code apply} arm.
     */
    record Variant(String name, String method, List<Field> fields, String applyBody) {}

    /**
     * One field of a variant class. {@code name} spells it on the class and in
     * the constructor ({@code k} / {@code v} / {@code n} / {@code value});
     * {@code param} spells it in the factory and the mutator ({@code key} /
     * {@code value} / {@code n}). A {@code primitive} field compares with
     * {@code ==}, every other with {@code Objects.equals}.
     */
    record Field(String type, String name, String param, boolean primitive) {}

    /** {@code (T value)} — one whole value the author supplies. */
    static Variant value(String name, String method, String type, String applyBody) {
        return new Variant(name, method, List.of(new Field(type, "value", "value", false)), applyBody);
    }

    /** {@code (K key, V value)} — a per-key map mutation. */
    static Variant keyed(String name, String method, String keyType, String valType, String applyBody) {
        return new Variant(
                name,
                method,
                List.of(new Field(keyType, "k", "key", false), new Field(valType, "v", "value", false)),
                applyBody);
    }

    /** {@code (K key, int n)} — a count-based mutation on one key's collection. */
    static Variant keyedCount(String name, String method, String keyType, String applyBody) {
        return new Variant(
                name,
                method,
                List.of(new Field(keyType, "k", "key", false), new Field("int", "n", "n", true)),
                applyBody);
    }

    /** {@code (int n)} — a count-based mutation on a flat collection. */
    static Variant count(String name, String method, String applyBody) {
        return new Variant(name, method, List.of(new Field("int", "n", "n", true)), applyBody);
    }
}
