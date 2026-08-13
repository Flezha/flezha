package com.flezha.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.List;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Mirror of the Rust SDK's {@code OldApiNotImportable} test (per
 * {@code operative-mvp-rust/boundeds.md §9.5}). Asserts that user code importing
 * any of the deleted legacy types (e.g. {@code StreamOperativeHandler}) fails
 * to compile after the cutover.
 */
class OldApiNotImportableTest {

    @Test
    @DisplayName("user code importing StreamOperativeHandler fails to compile")
    void importingStreamOperativeHandlerFails() {
        String src =
                """
                package nowhere;
                import com.flezha.sdk.StreamOperativeHandler;
                public final class UsesLegacyApi {
                    void foo(StreamOperativeHandler<?, ?, ?> h) { }
                }
                """;
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        boolean ok = compiler.getTask(
                        new PrintWriter(err),
                        null,
                        null,
                        List.of("-proc:none"),
                        null,
                        List.of(new SourceFile("UsesLegacyApi.java", src)))
                .call();

        assertThat(ok)
                .as("import com.flezha.sdk.StreamOperativeHandler should not resolve after operative-mvp-java")
                .isFalse();
        assertThat(err.toString()).contains("com.flezha.sdk.StreamOperativeHandler");
    }

    @Test
    @DisplayName("user code importing FactoryRegistry fails to compile")
    void importingFactoryRegistryFails() {
        String src =
                """
                package nowhere;
                import com.flezha.sdk.FactoryRegistry;
                public final class UsesLegacyApi {
                    void foo() { FactoryRegistry.factoryIds(); }
                }
                """;
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        boolean ok = compiler.getTask(
                        new PrintWriter(err),
                        null,
                        null,
                        List.of("-proc:none"),
                        null,
                        List.of(new SourceFile("UsesLegacyApi.java", src)))
                .call();

        assertThat(ok)
                .as("import com.flezha.sdk.FactoryRegistry should not resolve after operative-mvp-java")
                .isFalse();
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

    private static void assertDoesNotCompile(String src) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        boolean ok = compiler.getTask(
                        new PrintWriter(err),
                        null,
                        null,
                        List.of("-proc:none"),
                        null,
                        List.of(new SourceFile("UsesLegacyApi.java", src)))
                .call();
        assertThat(ok).as("deleted API must not resolve; source:\n" + src).isFalse();
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
