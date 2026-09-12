package com.flezha.sdk.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Drives {@link FlezhaProcessor} over malformed sources and asserts its diagnostics. */
class FlezhaProcessorTest {

    @TempDir
    Path out;

    @Test
    void rejectsEntityKeyAndNamesItsReplacement() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.*;
                import java.util.Optional;
                @Procedure(id = "bad")
                public class Bad {
                    @EntityKey public Optional<String> s(String in) { return Optional.empty(); }
                    public String run(String in) { return in; }
                }
                """;
        assertThat(diagnose("t.Bad", code)).anyMatch(m -> m.contains(".keyBy(f)"));
    }

    @Test
    void rejectsInitStateOnStatelessProcedure() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.InitState;
                import com.flezha.sdk.authoring.Procedure;
                import java.util.Optional;
                @Procedure(id = "bad_init")
                public class BadInit {
                    @InitState public String initState(Optional<String> key) { return ""; }
                    public String run(String in) { return in; }
                }
                """;
        assertThat(diagnose("t.BadInit", code)).anyMatch(m -> m.contains("@InitState requires a stateful run"));
    }

    @Test
    void rejectsTooManyRunParameters() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                @Procedure(id = "bad2")
                public class Bad2 {
                    public String run(String a, String b, String c) { return a; }
                }
                """;
        assertThat(diagnose("t.Bad2", code)).anyMatch(m -> m.contains("Output run(Input)"));
    }

    @Test
    void rejectsNonPublicStateField() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.State;
                import java.util.ArrayList;
                import java.util.List;
                @State
                public class BadState {
                    private List<String> xs = new ArrayList<>();
                }
                """;
        assertThat(diagnose("t.BadState", code)).anyMatch(m -> m.contains("public, non-final fields"));
    }

    @Test
    void rejectsUnannotatedMapField() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.State;
                import java.util.HashMap;
                import java.util.Map;
                @State
                public class MapState {
                    public Map<String, Double> last = new HashMap<>();
                }
                """;
        assertThat(diagnose("t.MapState", code)).anyMatch(m -> m.contains("@Journal"));
    }

    @Test
    void rejectsJournalTypeMismatch() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.Journal;
                import com.flezha.sdk.authoring.State;
                import java.util.HashMap;
                import java.util.Map;
                @State
                public class MismatchState {
                    @Journal(Journal.Mode.MAP_VEC)
                    public Map<String, Double> bad = new HashMap<>();
                }
                """;
        assertThat(diagnose("t.MismatchState", code)).anyMatch(m -> m.contains("Map<K, List<U>>"));
    }

    @Test
    void rejectsRawOptionalReturn() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                import java.util.Optional;
                @Procedure(id = "bad_opt")
                public class BadOpt {
                    @SuppressWarnings({"rawtypes", "unchecked"})
                    public Optional run(String in) { return Optional.of(in); }
                }
                """;
        assertThat(diagnose("t.BadOpt", code)).anyMatch(m -> m.contains("Optional<Output>"));
    }

    @Test
    void rejectsKeyOnVoidReturn() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                @Procedure(id = "bad_key", key = "k")
                public class BadKey {
                    public static String k(String out) { return out; }
                    public void run(String in) { }
                }
                """;
        assertThat(diagnose("t.BadKey", code)).anyMatch(m -> m.contains("there is no emit to key"));
    }

    @Test
    void keyOptInGeneratesAKeyedEmit() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                @Procedure(id = "keyed")
                public class Plain {
                    public String run(String in) { return in; }
                }
                """;
        assertThat(generated("t.Plain", "t.PlainProcedure", code)).contains("ctx.emit(out);");

        String keyed =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                @Procedure(id = "keyed", key = "k")
                public class Keyed {
                    public static String k(String out) { return out; }
                    public String run(String in) { return in; }
                }
                """;
        assertThat(generated("t.Keyed", "t.KeyedProcedure", keyed)).contains("ctx.emitKeyed(t.Keyed.k(out), out);");
    }

    /**
     * The generator used to write {@code .named(x).sink(x)} because a sink
     * named a plugin rather than a channel, so sugared and hand-written code
     * shipped different wire names for the same operative.
     */
    @Test
    void theGeneratedChannelsNamesTheChannelWithTheSinkAndNothingElse() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                @Procedure(id = "sugar", sink = "items_output")
                public class Sugared {
                    public String run(String in) { return in; }
                }
                """;
        String source = generated("t.Sugared", "t.SugaredStreamOperative", code);

        assertThat(source).contains(".sink(\"items_output\")");
        assertThat(source).doesNotContain(".named(");
    }

    @Test
    void contextParameterRunEmitsThroughTheBody() {
        String stateless =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                import com.flezha.sdk.procedure.ProcedureCtx;
                @Procedure(id = "fanout", emits = String.class)
                public class Fanout {
                    public void run(String in, ProcedureCtx ctx) { ctx.emit(in); ctx.emit(in); }
                }
                """;
        String generated = generated("t.Fanout", "t.FanoutProcedure", stateless);
        // The body owns every emit: the wrapper passes the context and adds none.
        assertThat(generated).contains("impl.run(input, ctx);");
        assertThat(generated).doesNotContain("ctx.emit(out)");
        // `emits` lands as the Output type argument, which is where the runtime
        // reads it from — the tokens are not restated.
        assertThat(generated)
                .contains("implements Procedure<java.lang.String, com.flezha.sdk.state.Stateless,"
                        + " java.lang.String, com.flezha.sdk.state.NoChange, WallClock, Void>");
        assertThat(generated).doesNotContain("outputType()");

        String stateful =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                import com.flezha.sdk.procedure.ProcedureCtx;
                import com.flezha.sdk.state.NoChange;
                import com.flezha.sdk.state.StateHandle;
                @Procedure(id = "fanout2", emits = String.class)
                public class Fanout2 {
                    public void run(String in, StateHandle<String, NoChange> state, ProcedureCtx ctx) {
                        ctx.emit(in);
                    }
                }
                """;
        assertThat(generated("t.Fanout2", "t.Fanout2Procedure", stateful)).contains("impl.run(input, state, ctx);");
    }

    @Test
    void rejectsKeyWithAContextParameter() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                import com.flezha.sdk.procedure.ProcedureCtx;
                @Procedure(id = "bad_ctx_key", key = "k", emits = String.class)
                public class BadCtxKey {
                    public static String k(String out) { return out; }
                    public void run(String in, ProcedureCtx ctx) { ctx.emit(in); }
                }
                """;
        assertThat(diagnose("t.BadCtxKey", code)).anyMatch(m -> m.contains("conflicts with a `ProcedureCtx`"));
    }

    @Test
    void rejectsAValueReturnWithAContextParameter() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                import com.flezha.sdk.procedure.ProcedureCtx;
                @Procedure(id = "bad_ctx_return")
                public class BadCtxReturn {
                    public String run(String in, ProcedureCtx ctx) { return in; }
                }
                """;
        assertThat(diagnose("t.BadCtxReturn", code)).anyMatch(m -> m.contains("it must return void"));
    }

    @Test
    void rejectsEmitsWithoutAContextParameter() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.Procedure;
                @Procedure(id = "bad_emits", emits = String.class)
                public class BadEmits {
                    public String run(String in) { return in; }
                }
                """;
        assertThat(diagnose("t.BadEmits", code)).anyMatch(m -> m.contains("the return type declares it"));
    }

    @Test
    void flatDequeFieldsGetSlidingWindowMutators() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.State;
                import java.util.ArrayDeque;
                import java.util.Deque;
                @State
                public class WindowState {
                    public Deque<Double> ticks = new ArrayDeque<>();
                }
                """;
        String change = generated("t.WindowState", "t.WindowStateChange", code);
        assertThat(change).contains("pushBackTicks");
        assertThat(change).contains("popFrontTicks");
        assertThat(change).contains(".addLast(c.value)");
        assertThat(change).contains("pollFirst()");
    }

    /** Runs the processor over a well-formed source and returns the generated procedure's text. */
    private String generated(String fqn, String generatedFqn, String code) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diags = new DiagnosticCollector<>();
        StandardJavaFileManager fm = compiler.getStandardFileManager(diags, null, StandardCharsets.UTF_8);
        Path gen;
        try {
            gen = Files.createDirectories(out.resolve("gen-" + generatedFqn));
            fm.setLocation(StandardLocation.CLASS_OUTPUT, List.of(out.toFile()));
            fm.setLocation(StandardLocation.SOURCE_OUTPUT, List.of(gen.toFile()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        JavaCompiler.CompilationTask task =
                compiler.getTask(null, fm, diags, List.of("-proc:only"), null, List.of(new StringSource(fqn, code)));
        task.setProcessors(List.of(new FlezhaProcessor()));
        assertThat(task.call())
                .as("processing a well-formed @Procedure should succeed: %s", diags.getDiagnostics())
                .isTrue();
        try {
            return Files.readString(gen.resolve(generatedFqn.replace('.', '/') + ".java"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> diagnose(String fqn, String code) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diags = new DiagnosticCollector<>();
        StandardJavaFileManager fm = compiler.getStandardFileManager(diags, null, StandardCharsets.UTF_8);
        try {
            fm.setLocation(StandardLocation.CLASS_OUTPUT, List.of(out.toFile()));
            fm.setLocation(
                    StandardLocation.SOURCE_OUTPUT,
                    List.of(Files.createDirectories(out.resolve("gen")).toFile()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        JavaCompiler.CompilationTask workflow =
                compiler.getTask(null, fm, diags, List.of("-proc:only"), null, List.of(new StringSource(fqn, code)));
        workflow.setProcessors(List.of(new FlezhaProcessor()));
        boolean ok = workflow.call();
        assertThat(ok).as("compilation should fail on a malformed annotation").isFalse();
        return diags.getDiagnostics().stream()
                .filter(d -> d.getKind() == Diagnostic.Kind.ERROR)
                .map(d -> d.getMessage(null))
                .toList();
    }

    private static final class StringSource extends SimpleJavaFileObject {
        private final String code;

        StringSource(String fqn, String code) {
            super(URI.create("string:///" + fqn.replace('.', '/') + ".java"), Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }
}
