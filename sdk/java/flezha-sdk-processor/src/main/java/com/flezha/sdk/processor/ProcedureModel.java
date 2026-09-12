package com.flezha.sdk.processor;

import java.util.List;

/**
 * What a {@code @Procedure} class resolves to: the source to write, the five
 * type arguments its {@code implements Procedure<…>} clause states, and the
 * shape of the {@code run} wrapper. Every string is already the text the
 * renderer emits, so rendering reads no {@code javax.lang.model} type.
 */
record ProcedureModel(
        String pkg,
        String userFqn,
        String procedureSimple,
        String name,
        String inputType,
        String outputType,
        String stateType,
        String changeType,
        String handleArg,
        String initBody,
        String entitiesForName,
        List<String> entityKeys,
        String emitKey,
        String id,
        String sink,
        String input,
        String onFull,
        int capacity,
        boolean stateful,
        boolean ctxParam,
        boolean emits,
        boolean optional,
        boolean hasOutput) {

    /** What the {@code run} return type declares: whether it emits, and of what. */
    record Output(boolean emits, boolean optional, String type) {}

    /** The state a generated procedure threads: its two types, and what the author's {@code run} receives. */
    record StateShape(String stateType, String changeType, String handleArg) {}
}
