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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The api module's surface, asserted by compiling user code against it: a
 * deleted spelling must not resolve, and the spelling that replaced it must.
 * Mirrors the Rust SDK's {@code OldApiNotImportable}.
 */
class ApiSurfaceCompilationTest {

    /** Class output for the sources that do compile; JUnit removes it. */
    @TempDir
    static Path classOutput;

    @ParameterizedTest(name = "{0} does not resolve")
    @ValueSource(
            strings = {
                "com.flezha.sdk.StreamOperativeHandler",
                "com.flezha.sdk.FactoryRegistry",
                "com.flezha.sdk.RunnerBuilder",
                "com.flezha.sdk.operative.StreamOperativeRegistry",
                "com.flezha.sdk.operative.WorkflowOperativeRegistry",
            })
    void deletedTypesDoNotResolve(String type) {
        Compilation result = compile(usesImport(type));

        assertThat(result.ok()).as("import %s should not resolve", type).isFalse();
        assertThat(result.diagnostics())
                .as("the failure names the missing type")
                .contains(type);
    }

    @Test
    @DisplayName("the workflow kind is not in a package of its own: com.flezha.sdk.workflow does not resolve")
    void theWorkflowKindIsNotInAPackageOfItsOwn() {
        for (String type : List.of(
                "com.flezha.sdk.workflow.WorkflowOperative",
                "com.flezha.sdk.workflow.WorkflowOperativeCtx",
                "com.flezha.sdk.workflow.WorkflowOperativeException",
                "com.flezha.sdk.workflow.WorkflowOperativeConfigurationException",
                "com.flezha.sdk.workflow.WorkflowLoopBody",
                "com.flezha.sdk.workflow.LoopMode",
                "com.flezha.sdk.workflow.RollbackMode")) {
            assertDoesNotCompile(usesImport(type));
        }
    }

    @Test
    @DisplayName("both kinds and the base share one package, the workflow supporting types with them")
    void bothKindsAndTheBaseShareOnePackage() {
        assertThat(
                        compiles(
                                """
                        package nowhere;
                        import com.flezha.sdk.operative.LoopMode;
                        import com.flezha.sdk.operative.Operative;
                        import com.flezha.sdk.operative.RollbackMode;
                        import com.flezha.sdk.operative.StreamOperative;
                        import com.flezha.sdk.operative.WorkflowLoopBody;
                        import com.flezha.sdk.operative.WorkflowOperative;
                        import com.flezha.sdk.operative.WorkflowOperativeConfigurationException;
                        import com.flezha.sdk.operative.WorkflowOperativeCtx;
                        import com.flezha.sdk.operative.WorkflowOperativeException;
                        public final class UsesLegacyApi {
                            // Both kinds are the base, so one return type holds either.
                            Operative eitherKind(StreamOperative s, WorkflowOperative w, boolean b) {
                                return b ? s : w;
                            }

                            LoopMode loop;
                            RollbackMode rollback;
                            WorkflowLoopBody body;
                            WorkflowOperativeCtx ctx;
                            WorkflowOperativeException failure;
                            WorkflowOperativeConfigurationException refusal;
                        }
                        """))
                .as("the base, both kinds and the workflow supporting types resolve from one package")
                .isTrue();
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

    /** One import of {@code type}, the only thing under test. */
    private static String usesImport(String type) {
        return """
                package nowhere;
                import %s;
                public final class UsesLegacyApi { }
                """
                .formatted(type);
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
