package com.flezha.sdk.processor;

import com.flezha.sdk.authoring.ContinuousOperative;
import com.flezha.sdk.authoring.InitState;
import com.flezha.sdk.authoring.Journal;
import com.flezha.sdk.authoring.Operation;
import com.flezha.sdk.authoring.OperationKey;
import com.flezha.sdk.authoring.SegmentFor;
import com.flezha.sdk.authoring.State;
import java.io.IOException;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

/**
 * Generates {@code Operation} / {@code ContinuousOperative} implementations from the
 * {@code @Operation} / {@code @State} / {@code @ContinuousOperative} authoring annotations,
 * mirroring the Rust SDK macros. See {@code docs/sdk_authoring_contract.md}.
 *
 * <p>Per round: {@code @State} classes generate a {@code <Name>Change}
 * (field-granular variants + {@code apply} + {@code kryoTypes}); {@code @Operation}
 * classes generate a {@code <Name>Operation} (and, with a {@code sink}, a
 * single-operation {@code <Name>ContinuousOperative}); {@code @ContinuousOperative} classes are collected. On
 * the final round a {@code com.flezha.generated.GeneratedContinuousOperatives.registerAll()}
 * aggregator is emitted so authors register every sugared operative with one call.
 */
@SupportedAnnotationTypes({
    "com.flezha.sdk.authoring.Operation",
    "com.flezha.sdk.authoring.State",
    "com.flezha.sdk.authoring.ContinuousOperative"
})
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public final class FlezhaProcessor extends AbstractProcessor {

    private static final String STATE_HANDLE = "com.flezha.sdk.state.StateHandle";

    private Elements elements;
    private Types types;
    private Filer filer;

    /** Fully-qualified operative class names collected across rounds. */
    private final List<String> operativeFqns = new ArrayList<>();

    private boolean aggregatorEmitted = false;

    @Override
    public synchronized void init(javax.annotation.processing.ProcessingEnvironment env) {
        super.init(env);
        this.elements = env.getElementUtils();
        this.types = env.getTypeUtils();
        this.filer = env.getFiler();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element e : roundEnv.getElementsAnnotatedWith(State.class)) {
            generateState((TypeElement) e);
        }
        for (Element e : roundEnv.getElementsAnnotatedWith(Operation.class)) {
            generateOperation((TypeElement) e);
        }
        for (Element e : roundEnv.getElementsAnnotatedWith(ContinuousOperative.class)) {
            if (implementsContinuousOperative((TypeElement) e)) {
                operativeFqns.add(((TypeElement) e).getQualifiedName().toString());
            } else {
                error(
                        e,
                        "@ContinuousOperative requires the class to implement com.flezha.sdk.operative.ContinuousOperative");
            }
        }

        if (roundEnv.processingOver() && !aggregatorEmitted && !operativeFqns.isEmpty()) {
            emitAggregator();
            aggregatorEmitted = true;
        }
        return false;
    }

    // ---- @State -> <Name>Change ------------------------------------------

    private void generateState(TypeElement state) {
        String pkg = packageOf(state);
        String stateFqn = state.getQualifiedName().toString();
        String changeSimple = state.getSimpleName() + "Change";
        String changeFqn = pkg.isEmpty() ? changeSimple : pkg + "." + changeSimple;

        List<VariableElement> fields = new ArrayList<>();
        for (Element e : state.getEnclosedElements()) {
            if (e.getKind() == ElementKind.FIELD && !e.getModifiers().contains(Modifier.STATIC)) {
                if (!e.getModifiers().contains(Modifier.PUBLIC)
                        || e.getModifiers().contains(Modifier.FINAL)) {
                    error(
                            e,
                            "@State requires public, non-final fields for field-granular change "
                                    + "derivation; a conditional, multi-field, or computed mutation needs a "
                                    + "hand-written Change");
                    return;
                }
                fields.add((VariableElement) e);
            }
        }
        if (fields.isEmpty()) {
            error(state, "@State requires at least one mutable field");
            return;
        }

        StringBuilder b = new StringBuilder();
        if (!pkg.isEmpty()) {
            b.append("package ").append(pkg).append(";\n\n");
        }
        b.append("import com.flezha.sdk.state.Change;\n");
        b.append("import java.util.List;\n");
        b.append("import java.util.Objects;\n\n");
        b.append("/** Generated by FlezhaProcessor from @State on ")
                .append(stateFqn)
                .append(". */\n");
        b.append("public abstract class ").append(changeSimple).append(" implements Change {\n");
        b.append("    private ").append(changeSimple).append("() {}\n\n");

        List<String> variantClasses = new ArrayList<>();
        StringBuilder applyBody = new StringBuilder();
        StringBuilder handleMethods = new StringBuilder();

        for (VariableElement f : fields) {
            String fname = f.getSimpleName().toString();
            String pascal = pascal(fname);
            String ftype = f.asType().toString();

            Journal journal = f.getAnnotation(Journal.class);
            TypeMirror[] kv = mapKV(f.asType());
            if (journal != null) {
                if (!expandJournalField(
                        f,
                        fname,
                        pascal,
                        changeSimple,
                        journal.value(),
                        kv,
                        b,
                        variantClasses,
                        applyBody,
                        handleMethods)) {
                    return;
                }
                continue;
            }
            // A map field without @Journal is an unsupported mutation: there is
            // no single per-entry op to record. Direct to a hand-written Change.
            if (kv != null) {
                error(
                        f,
                        "@State: a map field needs a @Journal(MAP_SET | MAP_VEC | MAP_DEQUE) annotation to "
                                + "derive its per-key journaling mutators; hand-write a Change for anything richer");
                return;
            }

            // Set<Field>(T)
            String setVar = "Set" + pascal;
            variantClasses.add(setVar);
            b.append(factory(changeSimple, "set" + pascal, ftype, setVar));
            b.append(variant(changeSimple, setVar, ftype));
            applyBody
                    .append("        if (change instanceof ")
                    .append(setVar)
                    .append(" c) { state.")
                    .append(fname)
                    .append(" = c.value; return; }\n");
            handleMethods.append(handleMutator(changeSimple, "set" + pascal, ftype));

            // Push<Field>(E) for List<E>
            TypeMirror elem = listElement(f.asType());
            if (elem != null) {
                String pushVar = "Push" + pascal;
                String etype = elem.toString();
                variantClasses.add(pushVar);
                b.append(factory(changeSimple, "push" + pascal, etype, pushVar));
                b.append(variant(changeSimple, pushVar, etype));
                applyBody
                        .append("        if (change instanceof ")
                        .append(pushVar)
                        .append(" c) { state.")
                        .append(fname)
                        .append(".add(c.value); return; }\n");
                handleMethods.append(handleMutator(changeSimple, "push" + pascal, etype));
            }
        }

        b.append("    /** Fold a change into the state. */\n");
        b.append("    public static void apply(")
                .append(stateFqn)
                .append(" state, ")
                .append(changeSimple)
                .append(" change) {\n");
        b.append(applyBody);
        b.append("    }\n\n");

        b.append("    /** Kryo type registration for the state + change variants. */\n");
        b.append("    public static List<Class<?>> kryoTypes() {\n");
        b.append("        return List.of(")
                .append(stateFqn)
                .append(".class, ")
                .append(changeSimple)
                .append(".class");
        for (String v : variantClasses) {
            b.append(", ").append(v).append(".class");
        }
        b.append(");\n    }\n");

        b.append("}\n");

        write(changeFqn, b.toString(), state);

        // `<State>Handle` — the journaling handle a sugared `run` receives, with
        // a `set`/`push` mutator per field so authors never name the change type.
        String handleSimple = state.getSimpleName() + "Handle";
        String handleFqn = pkg.isEmpty() ? handleSimple : pkg + "." + handleSimple;
        StringBuilder h = new StringBuilder();
        if (!pkg.isEmpty()) {
            h.append("package ").append(pkg).append(";\n\n");
        }
        h.append("import com.flezha.sdk.state.StateHandle;\n\n");
        h.append("/** Journaling handle for ").append(stateFqn).append(", generated by @State. */\n");
        h.append("public final class ").append(handleSimple).append(" {\n");
        h.append("    private final StateHandle<")
                .append(stateFqn)
                .append(", ")
                .append(changeSimple)
                .append("> h;\n");
        h.append("    public ")
                .append(handleSimple)
                .append("(StateHandle<")
                .append(stateFqn)
                .append(", ")
                .append(changeSimple)
                .append("> h) { this.h = h; }\n");
        h.append("    public ").append(stateFqn).append(" get() { return h.get(); }\n");
        h.append("    public void apply(").append(changeSimple).append(" change) { h.apply(change); }\n");
        h.append(handleMethods);
        h.append("}\n");
        write(handleFqn, h.toString(), state);
    }

    private static String handleMutator(String changeSimple, String method, String paramType) {
        return "    public void " + method + "(" + paramType + " value) { h.apply(" + changeSimple + "." + method
                + "(value)); }\n";
    }

    // ---- @Journal map fields ---------------------------------------------

    /**
     * Expand a {@code @Journal} map field into per-key mutators, {@code Change}
     * variants, and {@code apply} arms. Returns {@code false} (after reporting)
     * on a type/mode mismatch.
     */
    private boolean expandJournalField(
            VariableElement f,
            String fname,
            String pascal,
            String changeSimple,
            Journal.Mode mode,
            TypeMirror[] kv,
            StringBuilder b,
            List<String> variantClasses,
            StringBuilder applyBody,
            StringBuilder handleMethods) {
        if (kv == null) {
            error(f, "@Journal requires a Map<K, V> field; hand-write a Change for anything richer");
            return false;
        }
        String keyType = kv[0].toString();
        TypeMirror valMirror = kv[1];

        switch (mode) {
            case MAP_SET -> {
                String valType = valMirror.toString();
                String var = "Set" + pascal;
                variantClasses.add(var);
                b.append(mapFactory(changeSimple, "set" + pascal, keyType, valType, var));
                b.append(mapVariant(changeSimple, var, keyType, valType));
                applyBody
                        .append("        if (change instanceof ")
                        .append(var)
                        .append(" c) { state.")
                        .append(fname)
                        .append(".put(c.k, c.v); return; }\n");
                handleMethods.append(mapHandleMutator(changeSimple, "set" + pascal, keyType, valType));
            }
            case MAP_VEC -> {
                TypeMirror elem = listElement(valMirror);
                if (elem == null) {
                    error(
                            f,
                            "@Journal(MAP_VEC) requires a Map<K, List<U>> field; hand-write a Change for "
                                    + "anything richer");
                    return false;
                }
                String u = elem.toString();
                String newColl = "new java.util.ArrayList<>()";

                String pushVar = "Push" + pascal;
                variantClasses.add(pushVar);
                b.append(mapFactory(changeSimple, "push" + pascal, keyType, u, pushVar));
                b.append(mapVariant(changeSimple, pushVar, keyType, u));
                applyBody
                        .append("        if (change instanceof ")
                        .append(pushVar)
                        .append(" c) { state.")
                        .append(fname)
                        .append(".computeIfAbsent(c.k, __k -> ")
                        .append(newColl)
                        .append(").add(c.v); return; }\n");
                handleMethods.append(mapHandleMutator(changeSimple, "push" + pascal, keyType, u));

                String rlVar = "ReplaceLast" + pascal;
                variantClasses.add(rlVar);
                b.append(mapFactory(changeSimple, "replaceLast" + pascal, keyType, u, rlVar));
                b.append(mapVariant(changeSimple, rlVar, keyType, u));
                applyBody
                        .append("        if (change instanceof ")
                        .append(rlVar)
                        .append(" c) { var __l = state.")
                        .append(fname)
                        .append(".computeIfAbsent(c.k, __k -> ")
                        .append(newColl)
                        .append("); if (__l.isEmpty()) { __l.add(c.v); } else { __l.set(__l.size() - 1, c.v); } "
                                + "return; }\n");
                handleMethods.append(mapHandleMutator(changeSimple, "replaceLast" + pascal, keyType, u));

                String drainVar = "Drain" + pascal + "Front";
                variantClasses.add(drainVar);
                b.append(intOpFactory(changeSimple, "drain" + pascal + "Front", keyType, drainVar));
                b.append(intOpVariant(changeSimple, drainVar, keyType));
                applyBody
                        .append("        if (change instanceof ")
                        .append(drainVar)
                        .append(" c) { var __l = state.")
                        .append(fname)
                        .append(".computeIfAbsent(c.k, __k -> ")
                        .append(newColl)
                        .append("); int __n = Math.min(c.n, __l.size()); __l.subList(0, __n).clear(); return; }\n");
                handleMethods.append(intOpHandleMutator(changeSimple, "drain" + pascal + "Front", keyType));
            }
            case MAP_DEQUE -> {
                TypeMirror elem = dequeElement(valMirror);
                if (elem == null) {
                    error(
                            f,
                            "@Journal(MAP_DEQUE) requires a Map<K, Deque<U>> field; hand-write a Change for "
                                    + "anything richer");
                    return false;
                }
                String u = elem.toString();
                String newColl = "new java.util.ArrayDeque<>()";

                String pushVar = "PushBack" + pascal;
                variantClasses.add(pushVar);
                b.append(mapFactory(changeSimple, "pushBack" + pascal, keyType, u, pushVar));
                b.append(mapVariant(changeSimple, pushVar, keyType, u));
                applyBody
                        .append("        if (change instanceof ")
                        .append(pushVar)
                        .append(" c) { state.")
                        .append(fname)
                        .append(".computeIfAbsent(c.k, __k -> ")
                        .append(newColl)
                        .append(").addLast(c.v); return; }\n");
                handleMethods.append(mapHandleMutator(changeSimple, "pushBack" + pascal, keyType, u));

                String popVar = "PopFront" + pascal;
                variantClasses.add(popVar);
                b.append(intOpFactory(changeSimple, "popFront" + pascal, keyType, popVar));
                b.append(intOpVariant(changeSimple, popVar, keyType));
                applyBody
                        .append("        if (change instanceof ")
                        .append(popVar)
                        .append(" c) { var __d = state.")
                        .append(fname)
                        .append(".computeIfAbsent(c.k, __k -> ")
                        .append(newColl)
                        .append("); int __n = Math.min(c.n, __d.size()); for (int __i = 0; __i < __n; __i++) { "
                                + "__d.pollFirst(); } return; }\n");
                handleMethods.append(intOpHandleMutator(changeSimple, "popFront" + pascal, keyType));
            }
        }
        return true;
    }

    /** Two-arg `(key, value)` factory for a map-journaling variant. */
    private static String mapFactory(
            String changeSimple, String method, String keyType, String valType, String variant) {
        return "    public static " + changeSimple + " " + method + "(" + keyType + " key, " + valType
                + " value) { return new " + variant + "(key, value); }\n";
    }

    /** Two-field `(K k, V v)` variant with structural equality. */
    private static String mapVariant(String changeSimple, String name, String keyType, String valType) {
        return "    public static final class " + name + " extends " + changeSimple + " {\n"
                + "        public final " + keyType + " k;\n"
                + "        public final " + valType + " v;\n"
                + "        public " + name + "(" + keyType + " k, " + valType + " v) { this.k = k; this.v = v; }\n"
                + "        @Override public boolean equals(Object o) { return o instanceof " + name
                + " c && Objects.equals(k, c.k) && Objects.equals(v, c.v); }\n"
                + "        @Override public int hashCode() { return Objects.hash(k, v); }\n"
                + "        @Override public String toString() { return \"" + name + "(\" + k + \", \" + v + \")\"; }\n"
                + "    }\n\n";
    }

    private static String mapHandleMutator(String changeSimple, String method, String keyType, String valType) {
        return "    public void " + method + "(" + keyType + " key, " + valType + " value) { h.apply(" + changeSimple
                + "." + method + "(key, value)); }\n";
    }

    /** Two-arg `(key, int n)` factory for a count-based map op (drain / pop). */
    private static String intOpFactory(String changeSimple, String method, String keyType, String variant) {
        return "    public static " + changeSimple + " " + method + "(" + keyType + " key, int n) { return new "
                + variant + "(key, n); }\n";
    }

    /** Two-field `(K k, int n)` variant with structural equality. */
    private static String intOpVariant(String changeSimple, String name, String keyType) {
        return "    public static final class " + name + " extends " + changeSimple + " {\n"
                + "        public final " + keyType + " k;\n"
                + "        public final int n;\n"
                + "        public " + name + "(" + keyType + " k, int n) { this.k = k; this.n = n; }\n"
                + "        @Override public boolean equals(Object o) { return o instanceof " + name
                + " c && Objects.equals(k, c.k) && n == c.n; }\n"
                + "        @Override public int hashCode() { return Objects.hash(k, n); }\n"
                + "        @Override public String toString() { return \"" + name + "(\" + k + \", \" + n + \")\"; }\n"
                + "    }\n\n";
    }

    private static String intOpHandleMutator(String changeSimple, String method, String keyType) {
        return "    public void " + method + "(" + keyType + " key, int n) { h.apply(" + changeSimple + "." + method
                + "(key, n)); }\n";
    }

    private TypeMirror[] mapKV(TypeMirror ty) {
        if (ty instanceof DeclaredType dt && types.erasure(dt).toString().equals("java.util.Map")) {
            List<? extends TypeMirror> ta = dt.getTypeArguments();
            if (ta.size() == 2) {
                return new TypeMirror[] {ta.get(0), ta.get(1)};
            }
        }
        return null;
    }

    private TypeMirror dequeElement(TypeMirror ty) {
        if (ty instanceof DeclaredType dt && types.erasure(dt).toString().equals("java.util.Deque")) {
            List<? extends TypeMirror> ta = dt.getTypeArguments();
            if (ta.size() == 1) {
                return ta.get(0);
            }
        }
        return null;
    }

    private static String factory(String changeSimple, String method, String paramType, String variant) {
        return "    public static "
                + changeSimple
                + " "
                + method
                + "("
                + paramType
                + " value) { return new "
                + variant
                + "(value); }\n";
    }

    private static String variant(String changeSimple, String name, String valueType) {
        return "    public static final class "
                + name
                + " extends "
                + changeSimple
                + " {\n"
                + "        public final "
                + valueType
                + " value;\n"
                + "        public "
                + name
                + "("
                + valueType
                + " value) { this.value = value; }\n"
                + "        @Override public boolean equals(Object o) { return o instanceof "
                + name
                + " c && Objects.equals(value, c.value); }\n"
                + "        @Override public int hashCode() { return Objects.hashCode(value); }\n"
                + "        @Override public String toString() { return \""
                + name
                + "(\" + value + \")\"; }\n"
                + "    }\n\n";
    }

    // ---- @Operation -> <Name>Operation (+ <Name>ContinuousOperative) ---------------------------

    private void generateOperation(TypeElement operation) {
        String pkg = packageOf(operation);
        String userFqn = operation.getQualifiedName().toString();
        String operationSimple = operation.getSimpleName() + "Operation";
        String operationFqn = pkg.isEmpty() ? operationSimple : pkg + "." + operationSimple;

        ExecutableElement run = findRun(operation);
        if (run == null) {
            error(operation, "@Operation requires exactly one public method named `run`");
            return;
        }
        List<? extends VariableElement> params = run.getParameters();
        if (params.isEmpty() || params.size() > 2) {
            error(
                    run,
                    "@Operation `run` must be `Output run(Input)` or `Output run(Input, StateHandle<State, Change>)`");
            return;
        }

        TypeMirror inputType = params.get(0).asType();
        TypeMirror returnType = run.getReturnType();
        // `void` = no output; `Optional<O>` = emit-on-present; bare `O` = always emit.
        boolean emits;
        boolean optional = false;
        String outputType;
        if (returnType.getKind() == TypeKind.VOID) {
            emits = false;
            outputType = "java.lang.Void";
        } else if (isOptional(returnType)) {
            TypeMirror oe = optionalElement(returnType);
            if (oe == null) {
                error(run, "@Operation `run` returning Optional must be parameterized as Optional<Output>");
                return;
            }
            emits = true;
            optional = true;
            outputType = oe.toString();
        } else {
            emits = true;
            outputType = returnType.toString();
        }

        boolean stateful = params.size() == 2;
        String stateType;
        String changeType;
        // What the generated operation passes to the user's run as the state arg.
        String handleArg = "state";
        if (stateful) {
            TypeMirror p1 = params.get(1).asType();
            if (p1 instanceof DeclaredType dt && types.erasure(dt).toString().equals(STATE_HANDLE)) {
                // Raw form: `run(Input, StateHandle<State, Change>)`.
                List<? extends TypeMirror> ta = dt.getTypeArguments();
                if (ta.size() != 2) {
                    error(run, "StateHandle must be parameterized as StateHandle<State, Change>");
                    return;
                }
                stateType = ta.get(0).toString();
                changeType = ta.get(1).toString();
            } else {
                // Sugar form: `run(Input, <State>Handle)` — the @State-generated
                // handle. Recover State from its name (it may be unresolved this
                // round); the handle is co-located with its @State class.
                String handleName = p1.toString();
                if (!handleName.endsWith("Handle")) {
                    error(
                            run,
                            "@Operation stateful `run` second parameter must be a @State-generated `<State>Handle` "
                                    + "or `StateHandle<State, Change>`");
                    return;
                }
                String handleFqn = handleName.contains(".") || pkg.isEmpty() ? handleName : pkg + "." + handleName;
                stateType = handleFqn.substring(0, handleFqn.length() - "Handle".length());
                changeType = stateType + "Change";
                handleArg = "new " + handleFqn + "(state)";
            }
        } else {
            stateType = "com.flezha.sdk.state.Stateless";
            changeType = "com.flezha.sdk.state.NoChange";
        }

        // Segment mode: @SegmentFor (static) xor @OperationKey (dynamic).
        ExecutableElement segmentFor = findAnnotated(operation, SegmentFor.class);
        ExecutableElement operationKey = findAnnotated(operation, OperationKey.class);
        if (segmentFor != null && operationKey != null) {
            error(operation, "@SegmentFor (static) and @OperationKey (dynamic) are mutually exclusive");
            return;
        }

        // Per-segment state init: @InitState maps the operation key to State.
        ExecutableElement initState = findAnnotated(operation, InitState.class);
        if (initState != null && !stateful) {
            error(
                    operation,
                    "@InitState requires a stateful run (a @State handle parameter); a stateless operation "
                            + "has no state to initialize");
            return;
        }

        Operation ann = operation.getAnnotation(Operation.class);
        List<String> keys = List.of(ann.keys());
        if (segmentFor != null && keys.isEmpty()) {
            error(operation, "static segmenting requires @Operation(keys = {...}) alongside the @SegmentFor method");
            return;
        }
        if (!keys.isEmpty() && segmentFor == null) {
            error(operation, "@Operation(keys = {...}) requires a @SegmentFor method");
            return;
        }

        String id = ann.id().isEmpty() ? snake(operation.getSimpleName().toString()) : ann.id();
        String sink = ann.sink();
        int capacity = ann.capacity();
        String onFull = ann.onFull();

        StringBuilder b = new StringBuilder();
        if (!pkg.isEmpty()) {
            b.append("package ").append(pkg).append(";\n\n");
        }
        b.append("import com.flezha.sdk.operation.Operation;\n");
        b.append("import com.flezha.sdk.operation.OperationCtx;\n");
        b.append("import com.flezha.sdk.operation.OperationException;\n");
        b.append("import com.flezha.sdk.state.RestoreInitPayload;\n");
        b.append("import com.flezha.sdk.state.StateHandle;\n");
        b.append("import com.flezha.sdk.state.WallClock;\n");
        b.append("import java.util.List;\n");
        b.append("import java.util.Optional;\n\n");

        b.append("/** Generated by FlezhaProcessor from @Operation on ")
                .append(userFqn)
                .append(". */\n");
        b.append("public final class ")
                .append(operationSimple)
                .append(" implements Operation<")
                .append(inputType)
                .append(", ")
                .append(stateType)
                .append(", ")
                .append(outputType)
                .append(", ")
                .append(changeType)
                .append(", WallClock, Void> {\n\n");
        b.append("    private final ")
                .append(userFqn)
                .append(" impl = new ")
                .append(userFqn)
                .append("();\n\n");

        b.append("    @Override public String name() { return \"")
                .append(snake(operation.getSimpleName().toString()))
                .append("\"; }\n");
        b.append("    @Override public Class<")
                .append(inputType)
                .append("> inputType() { return ")
                .append(classLit(inputType))
                .append("; }\n");
        b.append("    @Override public Class<")
                .append(outputType)
                .append("> outputType() { return ")
                .append(classLit(outputType))
                .append("; }\n");
        b.append("    @Override public Class<")
                .append(changeType)
                .append("> changeType() { return ")
                .append(classLit(changeType))
                .append("; }\n");
        b.append("    @Override public Class<WallClock> clockType() { return WallClock.class; }\n");
        b.append("    @Override public Class<Void> configType() { return Void.class; }\n");

        String initBody =
                initState != null ? "impl." + initState.getSimpleName() + "(operationKey)" : "new " + stateType + "()";
        b.append("    @Override public ")
                .append(stateType)
                .append(" initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) { return ")
                .append(initBody)
                .append("; }\n");

        if (stateful) {
            b.append("    @Override public void applyChange(")
                    .append(stateType)
                    .append(" state, ")
                    .append(changeType)
                    .append(" change) { ")
                    .append(changeType)
                    .append(".apply(state, change); }\n");
            b.append("    @Override public List<Class<?>> kryoTypes() { return ")
                    .append(changeType)
                    .append(".kryoTypes(); }\n");
        }

        if (segmentFor != null) {
            b.append("    @Override public List<String> operationKeys() { return List.of(")
                    .append(quotedJoin(keys))
                    .append("); }\n");
            b.append("    @Override public boolean segmentForOverridden() { return true; }\n");
            b.append("    @Override public List<String> segmentFor(")
                    .append(inputType)
                    .append(" input) { return impl.")
                    .append(segmentFor.getSimpleName())
                    .append("(input); }\n");
        }
        if (operationKey != null) {
            b.append("    @Override public Optional<String> operationKey(")
                    .append(inputType)
                    .append(" input) { return impl.")
                    .append(operationKey.getSimpleName())
                    .append("(input); }\n");
            b.append("    @Override public boolean operationKeyOverridden() { return true; }\n");
        }
        if (capacity > 0) {
            b.append("    @Override public int mailboxCapacity() { return ")
                    .append(capacity)
                    .append("; }\n");
            b.append("    @Override public boolean mailboxCapacityOverridden() { return true; }\n");
        }

        // run wrapper
        b.append("\n    @Override public void run(")
                .append(inputType)
                .append(" input, StateHandle<")
                .append(stateType)
                .append(", ")
                .append(changeType)
                .append("> state, OperationCtx ctx) throws OperationException {\n");
        String call = stateful ? "impl.run(input, " + handleArg + ")" : "impl.run(input)";
        if (!emits) {
            b.append("        ").append(call).append(";\n");
        } else if (optional) {
            b.append("        java.util.Optional<")
                    .append(outputType)
                    .append("> out = ")
                    .append(call)
                    .append(";\n");
            b.append("        if (out.isPresent()) { ctx.emit(out.get()); }\n");
        } else {
            b.append("        ")
                    .append(outputType)
                    .append(" out = ")
                    .append(call)
                    .append(";\n");
            b.append("        if (out != null) { ctx.emit(out); }\n");
        }
        b.append("    }\n");
        b.append("}\n");

        write(operationFqn, b.toString(), operation);

        // Single-operation auto-wire operative.
        if (!sink.isEmpty()) {
            if (!emits) {
                error(operation, "@Operation(sink = ...) requires a non-void return type to feed the plugin");
                return;
            }
            generateSingleOperationContinuousOperative(
                    pkg, operationSimple, id, sink, inputType.toString(), outputType, onFull, operation);
        }
    }

    private void generateSingleOperationContinuousOperative(
            String pkg,
            String operationSimple,
            String id,
            String sink,
            String inputType,
            String outputType,
            String onFull,
            Element origin) {
        String operativeSimple = operationSimple.replaceFirst("Operation$", "") + "ContinuousOperative";
        String operativeFqn = pkg.isEmpty() ? operativeSimple : pkg + "." + operativeSimple;
        String onFullCall = onFullCall(onFull, origin);

        StringBuilder b = new StringBuilder();
        if (!pkg.isEmpty()) {
            b.append("package ").append(pkg).append(";\n\n");
        }
        b.append("import com.flezha.sdk.channel.ChannelBuilder;\n");
        b.append("import com.flezha.sdk.operative.ContinuousOperative;\n\n");
        b.append("/** Generated single-operation auto-wire operative for ")
                .append(operationSimple)
                .append(". */\n");
        b.append("public final class ").append(operativeSimple).append(" implements ContinuousOperative {\n");
        b.append("    @Override public String factoryId() { return \"")
                .append(id)
                .append("\"; }\n");
        b.append("    @Override public void channels(ChannelBuilder c) {\n");
        b.append("        c.input(")
                .append(classLit(inputType))
                .append(", \"input\")")
                .append(onFullCall)
                .append(".into(")
                .append(operationSimple)
                .append(".class);\n");
        b.append("        c.channel(")
                .append(classLit(outputType))
                .append(", \"")
                .append(sink)
                .append("\").from(")
                .append(operationSimple)
                .append(".class).toPlugin(\"")
                .append(sink)
                .append("\");\n");
        b.append("    }\n}\n");

        write(operativeFqn, b.toString(), origin);
        operativeFqns.add(operativeFqn);
    }

    private void emitAggregator() {
        StringBuilder b = new StringBuilder();
        b.append("package com.flezha.generated;\n\n");
        b.append("import com.flezha.sdk.operative.ContinuousOperativeRegistry;\n\n");
        b.append("/** Generated aggregator: registers every sugared operative with one call. */\n");
        b.append("public final class GeneratedContinuousOperatives {\n");
        b.append("    private GeneratedContinuousOperatives() {}\n\n");
        b.append("    public static void registerAll() {\n");
        for (String fqn : operativeFqns) {
            b.append("        ContinuousOperativeRegistry.registerContinuous(")
                    .append(fqn)
                    .append(".class);\n");
        }
        b.append("    }\n}\n");
        write("com.flezha.generated.GeneratedContinuousOperatives", b.toString());
    }

    // ---- helpers ----------------------------------------------------------

    private ExecutableElement findRun(TypeElement operation) {
        ExecutableElement found = null;
        for (Element e : operation.getEnclosedElements()) {
            if (e.getKind() == ElementKind.METHOD
                    && e.getSimpleName().contentEquals("run")
                    && e.getModifiers().contains(Modifier.PUBLIC)
                    && !e.getModifiers().contains(Modifier.STATIC)) {
                if (found != null) {
                    return null;
                }
                found = (ExecutableElement) e;
            }
        }
        return found;
    }

    private ExecutableElement findAnnotated(TypeElement operation, Class<? extends Annotation> ann) {
        for (Element e : operation.getEnclosedElements()) {
            if (e.getKind() == ElementKind.METHOD && e.getAnnotation(ann) != null) {
                return (ExecutableElement) e;
            }
        }
        return null;
    }

    private boolean implementsContinuousOperative(TypeElement t) {
        TypeElement operative = elements.getTypeElement("com.flezha.sdk.operative.ContinuousOperative");
        if (operative == null) {
            return false;
        }
        for (TypeMirror itf : t.getInterfaces()) {
            if (types.isSameType(types.erasure(itf), types.erasure(operative.asType()))) {
                return true;
            }
        }
        return false;
    }

    private String onFullCall(String onFull, Element origin) {
        if (onFull == null || onFull.isEmpty()) {
            return "";
        }
        String variant =
                switch (onFull) {
                    case "block" -> "BLOCK";
                    case "drop_newest" -> "DROP_NEWEST";
                    case "drop_oldest" -> "DROP_OLDEST";
                    default -> null;
                };
        if (variant == null) {
            error(origin, "@Operation onFull must be one of: block, drop_newest, drop_oldest");
            return "";
        }
        return ".onFull(com.flezha.sdk.channel.OverflowPolicy." + variant + ")";
    }

    private TypeMirror listElement(TypeMirror ty) {
        if (ty instanceof DeclaredType dt && types.erasure(dt).toString().equals("java.util.List")) {
            List<? extends TypeMirror> ta = dt.getTypeArguments();
            if (ta.size() == 1) {
                return ta.get(0);
            }
        }
        return null;
    }

    private boolean isOptional(TypeMirror ty) {
        return ty instanceof DeclaredType dt && types.erasure(dt).toString().equals("java.util.Optional");
    }

    private TypeMirror optionalElement(TypeMirror ty) {
        if (ty instanceof DeclaredType dt) {
            List<? extends TypeMirror> ta = dt.getTypeArguments();
            if (ta.size() == 1) {
                return ta.get(0);
            }
        }
        return null;
    }

    private String classLit(String typeStr) {
        // Erase generics for the `.class` literal.
        int lt = typeStr.indexOf('<');
        String erased = lt >= 0 ? typeStr.substring(0, lt) : typeStr;
        return erased + ".class";
    }

    private String classLit(TypeMirror ty) {
        return classLit(ty.toString());
    }

    private String packageOf(Element e) {
        return elements.getPackageOf(e).getQualifiedName().toString();
    }

    private static String quotedJoin(List<String> items) {
        return items.stream().map(s -> '"' + s + '"').collect(java.util.stream.Collectors.joining(", "));
    }

    private static String pascal(String s) {
        if (s.isEmpty()) {
            return s;
        }
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static String snake(String s) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    b.append('_');
                }
                b.append(Character.toLowerCase(c));
            } else {
                b.append(c);
            }
        }
        return b.toString();
    }

    private void write(String fqn, String content, Element... origin) {
        try {
            var file = filer.createSourceFile(fqn, origin);
            try (Writer w = file.openWriter()) {
                w.write(content);
            }
        } catch (IOException ex) {
            processingEnv
                    .getMessager()
                    .printMessage(Diagnostic.Kind.ERROR, "FlezhaProcessor: failed to write " + fqn + ": " + ex);
        }
    }

    private void error(Element e, String msg) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, msg, e);
    }
}
