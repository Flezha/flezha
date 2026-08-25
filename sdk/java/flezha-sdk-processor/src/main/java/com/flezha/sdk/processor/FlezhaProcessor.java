package com.flezha.sdk.processor;

import com.flezha.sdk.authoring.EntitiesFor;
import com.flezha.sdk.authoring.EntityKey;
import com.flezha.sdk.authoring.InitState;
import com.flezha.sdk.authoring.Journal;
import com.flezha.sdk.authoring.Procedure;
import com.flezha.sdk.authoring.State;
import com.flezha.sdk.authoring.StreamOperative;
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
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

/**
 * Generates {@code Procedure} / {@code StreamOperative} implementations from the
 * {@code @Procedure} / {@code @State} / {@code @StreamOperative} authoring annotations,
 * mirroring the Rust SDK macros. See {@code docs/sdk/authoring_contract.md}.
 *
 * <p>Per round: {@code @State} classes generate a {@code <Name>Change}
 * (field-granular variants + {@code apply} + {@code kryoTypes}); {@code @Procedure}
 * classes generate a {@code <Name>Procedure} (and, with a {@code sink}, a
 * single-procedure {@code <Name>StreamOperative}); {@code @StreamOperative} classes are collected. On
 * the final round a {@code com.flezha.generated.GeneratedStreamOperatives.defineAll(FlezhaRunner)}
 * aggregator is emitted so authors register every sugared operative with one call.
 */
@SupportedAnnotationTypes({
    "com.flezha.sdk.authoring.Procedure",
    "com.flezha.sdk.authoring.State",
    "com.flezha.sdk.authoring.StreamOperative"
})
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public final class FlezhaProcessor extends AbstractProcessor {

    private static final String STATE_HANDLE = "com.flezha.sdk.state.StateHandle";
    private static final String PROCEDURE_CTX = "com.flezha.sdk.procedure.ProcedureCtx";

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
        for (Element e : roundEnv.getElementsAnnotatedWith(Procedure.class)) {
            generateProcedure((TypeElement) e);
        }
        for (Element e : roundEnv.getElementsAnnotatedWith(StreamOperative.class)) {
            if (implementsStreamOperative((TypeElement) e)) {
                operativeFqns.add(((TypeElement) e).getQualifiedName().toString());
            } else {
                error(e, "@StreamOperative requires the class to implement com.flezha.sdk.operative.StreamOperative");
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
            // no single per-entry proc to record. Direct to a hand-written Change.
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

            // PushBack<Field>(E) + PopFront<Field>(int) for Deque<E> — the same
            // deltas MAP_DEQUE records, for a window whose entity key already is
            // what the map would have keyed by. Without them a per-entity window
            // could only journal whole-collection sets.
            TypeMirror dequeElem = dequeElement(f.asType());
            if (dequeElem != null) {
                String etype = dequeElem.toString();
                String pushVar = "PushBack" + pascal;
                variantClasses.add(pushVar);
                b.append(factory(changeSimple, "pushBack" + pascal, etype, pushVar));
                b.append(variant(changeSimple, pushVar, etype));
                applyBody
                        .append("        if (change instanceof ")
                        .append(pushVar)
                        .append(" c) { state.")
                        .append(fname)
                        .append(".addLast(c.value); return; }\n");
                handleMethods.append(handleMutator(changeSimple, "pushBack" + pascal, etype));

                String popVar = "PopFront" + pascal;
                variantClasses.add(popVar);
                b.append(countProcFactory(changeSimple, "popFront" + pascal, popVar));
                b.append(countProcVariant(changeSimple, popVar));
                applyBody
                        .append("        if (change instanceof ")
                        .append(popVar)
                        .append(" c) { var __d = state.")
                        .append(fname)
                        .append("; int __n = Math.min(c.n, __d.size()); for (int __i = 0; __i < __n; __i++) { "
                                + "__d.pollFirst(); } return; }\n");
                handleMethods.append(countProcHandleMutator(changeSimple, "popFront" + pascal));
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
                b.append(intProcFactory(changeSimple, "drain" + pascal + "Front", keyType, drainVar));
                b.append(intProcVariant(changeSimple, drainVar, keyType));
                applyBody
                        .append("        if (change instanceof ")
                        .append(drainVar)
                        .append(" c) { var __l = state.")
                        .append(fname)
                        .append(".computeIfAbsent(c.k, __k -> ")
                        .append(newColl)
                        .append("); int __n = Math.min(c.n, __l.size()); __l.subList(0, __n).clear(); return; }\n");
                handleMethods.append(intProcHandleMutator(changeSimple, "drain" + pascal + "Front", keyType));
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
                b.append(intProcFactory(changeSimple, "popFront" + pascal, keyType, popVar));
                b.append(intProcVariant(changeSimple, popVar, keyType));
                applyBody
                        .append("        if (change instanceof ")
                        .append(popVar)
                        .append(" c) { var __d = state.")
                        .append(fname)
                        .append(".computeIfAbsent(c.k, __k -> ")
                        .append(newColl)
                        .append("); int __n = Math.min(c.n, __d.size()); for (int __i = 0; __i < __n; __i++) { "
                                + "__d.pollFirst(); } return; }\n");
                handleMethods.append(intProcHandleMutator(changeSimple, "popFront" + pascal, keyType));
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

    /** Two-arg `(key, int n)` factory for a count-based map proc (drain / pop). */
    private static String intProcFactory(String changeSimple, String method, String keyType, String variant) {
        return "    public static " + changeSimple + " " + method + "(" + keyType + " key, int n) { return new "
                + variant + "(key, n); }\n";
    }

    /** Two-field `(K k, int n)` variant with structural equality. */
    private static String intProcVariant(String changeSimple, String name, String keyType) {
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

    private static String intProcHandleMutator(String changeSimple, String method, String keyType) {
        return "    public void " + method + "(" + keyType + " key, int n) { h.apply(" + changeSimple + "." + method
                + "(key, n)); }\n";
    }

    /** One-arg `(int n)` factory for a count-based flat-collection proc (pop). */
    private static String countProcFactory(String changeSimple, String method, String variant) {
        return "    public static " + changeSimple + " " + method + "(int n) { return new " + variant + "(n); }\n";
    }

    /** Single-field `(int n)` variant with structural equality. */
    private static String countProcVariant(String changeSimple, String name) {
        return "    public static final class " + name + " extends " + changeSimple + " {\n"
                + "        public final int n;\n"
                + "        public " + name + "(int n) { this.n = n; }\n"
                + "        @Override public boolean equals(Object o) { return o instanceof " + name
                + " c && n == c.n; }\n"
                + "        @Override public int hashCode() { return Objects.hash(n); }\n"
                + "        @Override public String toString() { return \"" + name + "(\" + n + \")\"; }\n"
                + "    }\n\n";
    }

    private static String countProcHandleMutator(String changeSimple, String method) {
        return "    public void " + method + "(int n) { h.apply(" + changeSimple + "." + method + "(n)); }\n";
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

    // ---- @Procedure -> <Name>Procedure (+ <Name>StreamOperative) ---------------------------

    private void generateProcedure(TypeElement procedure) {
        String pkg = packageOf(procedure);
        String userFqn = procedure.getQualifiedName().toString();
        String procedureSimple = procedure.getSimpleName() + "Procedure";
        String procedureFqn = pkg.isEmpty() ? procedureSimple : pkg + "." + procedureSimple;

        ExecutableElement run = findRun(procedure);
        if (run == null) {
            error(procedure, "@Procedure requires exactly one public method named `run`");
            return;
        }
        List<? extends VariableElement> params = run.getParameters();
        // A trailing `ProcedureCtx` is the multi-emit shape: the body owns every
        // emit. It sits beside the value parameters, which are counted as before.
        boolean ctxParam = !params.isEmpty()
                && types.erasure(params.get(params.size() - 1).asType())
                        .toString()
                        .equals(PROCEDURE_CTX);
        int valueParams = params.size() - (ctxParam ? 1 : 0);
        if (valueParams < 1 || valueParams > 2) {
            error(
                    run,
                    "@Procedure `run` must be `Output run(Input)` or `Output run(Input, StateHandle<State, Change>)`,"
                            + " each optionally followed by a `ProcedureCtx ctx` parameter");
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
                error(run, "@Procedure `run` returning Optional must be parameterized as Optional<Output>");
                return;
            }
            emits = true;
            optional = true;
            outputType = oe.toString();
        } else {
            emits = true;
            outputType = returnType.toString();
        }

        boolean stateful = valueParams == 2;
        String stateType;
        String changeType;
        // What the generated procedure passes to the user's run as the state arg.
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
                            "@Procedure stateful `run` second parameter must be a @State-generated `<State>Handle` "
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

        // Entity mode: static keying is the only mode a procedure can declare
        // — dynamic keying moved to the wiring.
        ExecutableElement entitiesFor = findAnnotated(procedure, EntitiesFor.class);
        if (findAnnotated(procedure, EntityKey.class) != null) {
            error(
                    procedure,
                    "@EntityKey no longer exists; declare the Entity key on the wiring instead — "
                            + "`.keyBy(f)` on the stream that feeds this procedure");
            return;
        }

        // Per-entity state init: @InitState maps the procedure key to State.
        ExecutableElement initState = findAnnotated(procedure, InitState.class);
        if (initState != null && !stateful) {
            error(
                    procedure,
                    "@InitState requires a stateful run (a @State handle parameter); a stateless procedure "
                            + "has no state to initialize");
            return;
        }

        Procedure ann = procedure.getAnnotation(Procedure.class);
        List<String> keys = List.of(ann.keys());
        if (entitiesFor != null && keys.isEmpty()) {
            error(procedure, "static entity requires @Procedure(keys = {...}) alongside the @EntitiesFor method");
            return;
        }
        if (!keys.isEmpty() && entitiesFor == null) {
            error(procedure, "@Procedure(keys = {...}) requires a @EntitiesFor method");
            return;
        }

        String id = ann.id().isEmpty() ? snake(procedure.getSimpleName().toString()) : ann.id();
        String sink = ann.sink();
        int capacity = ann.capacity();
        String onFull = ann.onFull();
        String emitKey = ann.key();
        String emitsType = declaredEmits(ann);
        if (ctxParam) {
            if (emits) {
                error(
                        run,
                        "@Procedure `run` with a `ProcedureCtx` parameter makes the body responsible for every"
                                + " emit; it must return void, and `emits = Type.class` declares the output type");
                return;
            }
            if (!emitKey.isEmpty()) {
                error(
                        procedure,
                        "@Procedure(key = ...) conflicts with a `ProcedureCtx` parameter; the body keys its own"
                                + " emits with ctx.emitKeyed(..)");
                return;
            }
            if (emitsType != null) {
                outputType = emitsType;
            }
        } else if (emitsType != null) {
            error(
                    procedure,
                    "@Procedure(emits = ...) declares the output type of a `ProcedureCtx` `run`; without that"
                            + " parameter the return type declares it");
            return;
        }
        // An output channel exists when either spelling declares one.
        boolean hasOutput = emits || emitsType != null;
        if (!emitKey.isEmpty() && !emits) {
            error(procedure, "@Procedure(key = ...) requires a non-void return type; there is no emit to key");
            return;
        }

        StringBuilder b = new StringBuilder();
        if (!pkg.isEmpty()) {
            b.append("package ").append(pkg).append(";\n\n");
        }
        b.append("import com.flezha.sdk.procedure.Procedure;\n");
        b.append("import com.flezha.sdk.procedure.ProcedureCtx;\n");
        b.append("import com.flezha.sdk.procedure.ProcedureException;\n");
        b.append("import com.flezha.sdk.state.RestoreInitPayload;\n");
        b.append("import com.flezha.sdk.state.StateHandle;\n");
        b.append("import com.flezha.sdk.state.WallClock;\n");
        b.append("import java.util.List;\n");
        b.append("import java.util.Optional;\n\n");

        b.append("/** Generated by FlezhaProcessor from @Procedure on ")
                .append(userFqn)
                .append(". */\n");
        b.append("public final class ")
                .append(procedureSimple)
                .append(" implements Procedure<")
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

        // The five type tokens are not generated: the `implements Procedure<…>`
        // clause above states them concretely, and the interface defaults read
        // them off it — the same path a hand-written procedure takes.
        b.append("    @Override public String name() { return \"")
                .append(snake(procedure.getSimpleName().toString()))
                .append("\"; }\n");

        String initBody =
                initState != null ? "impl." + initState.getSimpleName() + "(entityKey)" : "new " + stateType + "()";
        b.append("    @Override public ")
                .append(stateType)
                .append(" initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) { return ")
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

        if (entitiesFor != null) {
            b.append("    @Override public List<String> entityKeys() { return List.of(")
                    .append(quotedJoin(keys))
                    .append("); }\n");
            b.append("    @Override public boolean entitiesForOverridden() { return true; }\n");
            b.append("    @Override public List<String> entitiesFor(")
                    .append(inputType)
                    .append(" input) { return impl.")
                    .append(entitiesFor.getSimpleName())
                    .append("(input); }\n");
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
                .append("> state, ProcedureCtx ctx) throws ProcedureException {\n");
        String ctxArg = ctxParam ? ", ctx" : "";
        String call = stateful ? "impl.run(input, " + handleArg + ctxArg + ")" : "impl.run(input" + ctxArg + ")";
        // `key = "m"` routes the emit through emitKeyed — the only form a
        // `.keyed()` channel accepts — keeping keyed sinks reachable from the sugar.
        String emitPresent =
                emitKey.isEmpty() ? "ctx.emit(%s);" : "ctx.emitKeyed(" + userFqn + "." + emitKey + "(%s), %s);";
        if (!emits) {
            b.append("        ").append(call).append(";\n");
        } else if (optional) {
            b.append("        java.util.Optional<")
                    .append(outputType)
                    .append("> out = ")
                    .append(call)
                    .append(";\n");
            b.append("        if (out.isPresent()) { ")
                    .append(emitPresent.replace("%s", "out.get()"))
                    .append(" }\n");
        } else {
            b.append("        ")
                    .append(outputType)
                    .append(" out = ")
                    .append(call)
                    .append(";\n");
            b.append("        if (out != null) { ")
                    .append(emitPresent.replace("%s", "out"))
                    .append(" }\n");
        }
        b.append("    }\n");
        b.append("}\n");

        write(procedureFqn, b.toString(), procedure);

        // Single-procedure auto-wire operative.
        if (!sink.isEmpty()) {
            if (!hasOutput) {
                error(
                        procedure,
                        "@Procedure(sink = ...) requires an output type to feed the plugin — a non-void return,"
                                + " or `emits = Type.class` on the context-parameter shape");
                return;
            }
            generateSingleProcedureStreamOperative(
                    pkg, procedureSimple, id, sink, inputType.toString(), outputType, onFull, procedure);
        }
    }

    private void generateSingleProcedureStreamOperative(
            String pkg,
            String procedureSimple,
            String id,
            String sink,
            String inputType,
            String outputType,
            String onFull,
            Element origin) {
        String operativeSimple = procedureSimple.replaceFirst("Procedure$", "") + "StreamOperative";
        String operativeFqn = pkg.isEmpty() ? operativeSimple : pkg + "." + operativeSimple;
        String onFullCall = onFullCall(onFull, origin);

        StringBuilder b = new StringBuilder();
        if (!pkg.isEmpty()) {
            b.append("package ").append(pkg).append(";\n\n");
        }
        b.append("import com.flezha.sdk.channel.ChannelBuilder;\n");
        b.append("import com.flezha.sdk.operative.StreamOperative;\n\n");
        b.append("/** Generated single-procedure auto-wire operative for ")
                .append(procedureSimple)
                .append(". */\n");
        b.append("public final class ").append(operativeSimple).append(" implements StreamOperative {\n");
        b.append("    @Override public String factoryId() { return \"")
                .append(id)
                .append("\"; }\n");
        b.append("    @Override public void channels(ChannelBuilder c) {\n");
        b.append("        c.input(")
                .append(classLit(inputType))
                .append(", \"input\")")
                .append(onFullCall)
                .append(".into(")
                .append(procedureSimple)
                .append(".class).named(\"")
                .append(sink)
                .append("\").sink(\"")
                .append(sink)
                .append("\");\n");
        b.append("    }\n}\n");

        write(operativeFqn, b.toString(), origin);
        operativeFqns.add(operativeFqn);
    }

    private void emitAggregator() {
        StringBuilder b = new StringBuilder();
        b.append("package com.flezha.generated;\n\n");
        b.append("import com.flezha.sdk.FlezhaRunner;\n\n");
        b.append("/** Generated aggregator: defines every sugared operative on the runner with one call. */\n");
        b.append("public final class GeneratedStreamOperatives {\n");
        b.append("    private GeneratedStreamOperatives() {}\n\n");
        b.append("    public static void defineAll(FlezhaRunner runner) {\n");
        for (String fqn : operativeFqns) {
            b.append("        runner.defineStream(").append(fqn).append(".class);\n");
        }
        b.append("    }\n}\n");
        write("com.flezha.generated.GeneratedStreamOperatives", b.toString());
    }

    // ---- helpers ----------------------------------------------------------

    /**
     * The {@code emits = Type.class} output declaration, or {@code null} when
     * unset. Reading a {@code Class} member during annotation processing throws
     * {@link MirroredTypeException} — the mirror it carries is the answer.
     */
    private String declaredEmits(Procedure ann) {
        TypeMirror mirror;
        try {
            Class<?> declared = ann.emits();
            return declared == void.class ? null : declared.getCanonicalName();
        } catch (MirroredTypeException e) {
            mirror = e.getTypeMirror();
        }
        return mirror.getKind() == TypeKind.VOID ? null : mirror.toString();
    }

    private ExecutableElement findRun(TypeElement procedure) {
        ExecutableElement found = null;
        for (Element e : procedure.getEnclosedElements()) {
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

    private ExecutableElement findAnnotated(TypeElement procedure, Class<? extends Annotation> ann) {
        for (Element e : procedure.getEnclosedElements()) {
            if (e.getKind() == ElementKind.METHOD && e.getAnnotation(ann) != null) {
                return (ExecutableElement) e;
            }
        }
        return null;
    }

    private boolean implementsStreamOperative(TypeElement t) {
        TypeElement operative = elements.getTypeElement("com.flezha.sdk.operative.StreamOperative");
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
                    case "drop_nothing" -> "DROP_NOTHING";
                    case "drop_newest" -> "DROP_NEWEST";
                    case "drop_oldest" -> "DROP_OLDEST";
                    default -> null;
                };
        if (variant == null) {
            error(origin, "@Procedure onFull must be one of: drop_nothing, drop_newest, drop_oldest");
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
