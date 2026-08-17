package com.flezha.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Mirror of the Rust SDK's {@code OldApiNotImportable} test (per
 * {@code operative-mvp-rust/boundeds.md §9.5}). Asserts that user code importing
 * any of the deleted legacy types (e.g. {@code StreamOperativeHandler}) fails
 * to compile after the cutover.
 */
class OldApiNotImportableTest {

    /** Class output for the sources that do compile; JUnit removes it. */
    @TempDir
    static Path classOutput;

    @Test
    @DisplayName("user code importing StreamOperativeHandler fails to compile")
    void importingStreamOperativeHandlerFails() {
        Compilation result = compile(
                """
                package nowhere;
                import com.flezha.sdk.StreamOperativeHandler;
                public final class UsesLegacyApi {
                    void foo(StreamOperativeHandler<?, ?, ?> h) { }
                }
                """);

        assertThat(result.ok())
                .as("import com.flezha.sdk.StreamOperativeHandler should not resolve after operative-mvp-java")
                .isFalse();
        assertThat(result.diagnostics()).contains("com.flezha.sdk.StreamOperativeHandler");
    }

    @Test
    @DisplayName("user code importing FactoryRegistry fails to compile")
    void importingFactoryRegistryFails() {
        assertDoesNotCompile(
                """
                package nowhere;
                import com.flezha.sdk.FactoryRegistry;
                public final class UsesLegacyApi {
                    void foo() { FactoryRegistry.factoryIds(); }
                }
                """);
    }

    @Test
    @DisplayName("user code importing RunnerBuilder fails to compile")
    void importingRunnerBuilderFails() {
        assertDoesNotCompile(
                """
                package nowhere;
                import com.flezha.sdk.RunnerBuilder;
                public final class UsesLegacyApi {
                    void foo(RunnerBuilder b) { }
                }
                """);
    }

    @Test
    @DisplayName("user code importing the public registry facades fails to compile")
    void importingRegistryFacadesFails() {
        assertDoesNotCompile(
                """
                package nowhere;
                import com.flezha.sdk.operative.StreamOperativeRegistry;
                public final class UsesLegacyApi {
                    void foo() { StreamOperativeRegistry.factoryIds(); }
                }
                """);
        assertDoesNotCompile(
                """
                package nowhere;
                import com.flezha.sdk.workflow.WorkflowOperativeRegistry;
                public final class UsesLegacyApi {
                    void foo() { WorkflowOperativeRegistry.factoryIds(); }
                }
                """);
    }

    @Test
    @DisplayName("user code calling Flezha.newRunner fails to compile")
    void callingNewRunnerFails() {
        assertDoesNotCompile(
                """
                package nowhere;
                import com.flezha.sdk.Flezha;
                public final class UsesLegacyApi {
                    void foo() { Flezha.newRunner("ns"); }
                }
                """);
    }

    @Test
    @DisplayName("user code calling the removed output / alsoOutput egress fails to compile")
    void callingRemovedEgressMethodsFails() {
        // Control first: without it a classpath that resolved nothing would
        // make every refusal below vacuously true.
        assertThat(compiles(usesStream("s.sink(\"x\");")))
                .as("the replacement spelling must compile — otherwise the refusals prove nothing")
                .isTrue();

        assertDoesNotCompile(usesStream("s.output(\"x\");"));
        assertDoesNotCompile(usesStream("s.output(\"x\", v -> v.getBytes());"));
        assertDoesNotCompile(usesStream("s.alsoOutput(\"x\");"));
        assertDoesNotCompile(usesStream("s.alsoOutput(\"x\", v -> v.getBytes());"));
    }

    @Test
    @DisplayName("a sealed channel accepts no further declaration")
    void sealedRefusesFurtherDeclaration() {
        assertThat(compiles(usesStream("s.sink(\"x\").sealed();")))
                .as("sealing a sink is the supported terminator")
                .isTrue();

        assertDoesNotCompile(usesStream("s.sink(\"x\").sealed().named(\"y\");"));
        assertDoesNotCompile(usesStream("s.sink(\"x\").sealed().keyed();"));
    }

    /** One `Stream<String>` in scope, `body` the only thing under test. */
    private static String usesStream(String body) {
        return """
                package nowhere;
                import com.flezha.sdk.channel.Stream;
                public final class UsesLegacyApi {
                    void foo(Stream<String> s) { %s }
                }
                """
                .formatted(body);
    }

    private static void assertDoesNotCompile(String src) {
        assertThat(compiles(src))
                .as("deleted API must not resolve; source:\n" + src)
                .isFalse();
    }

    private static boolean compiles(String src) {
        return compile(src).ok();
    }

    /** Outcome of one in-process compile: whether it succeeded, and what javac said. */
    private record Compilation(boolean ok, String diagnostics) {}

    /**
     * Compile one source in-process against this module's own classpath, so a
     * reference to a live SDK type resolves and only the tested spelling
     * decides the outcome. The one compiler invocation in this file.
     */
    private static Compilation compile(String src) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        boolean ok = compiler.getTask(
                        new PrintWriter(err),
                        null,
                        null,
                        List.of(
                                "-proc:none",
                                "-classpath",
                                System.getProperty("java.class.path"),
                                "-d",
                                classOutput.toString()),
                        null,
                        List.of(new SourceFile("UsesLegacyApi.java", src)))
                .call();
        return new Compilation(ok, err.toString());
    }

    private static final class SourceFile extends SimpleJavaFileObject {
        private final String code;

        SourceFile(String name, String code) {
            super(URI.create("string:///" + name), JavaFileObject.Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }
}
