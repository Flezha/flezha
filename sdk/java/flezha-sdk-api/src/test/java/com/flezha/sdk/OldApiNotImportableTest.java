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
 * {@code relay-mvp-rust/tasks.md §9.5}). Asserts that user code importing
 * any of the deleted legacy types (e.g. {@code ProcessorHandler}) fails
 * to compile after the cutover.
 */
class OldApiNotImportableTest {

    @Test
    @DisplayName("user code importing ProcessorHandler fails to compile")
    void importingProcessorHandlerFails() {
        String src = """
                package nowhere;
                import com.flezha.sdk.ProcessorHandler;
                public final class UsesLegacyApi {
                    void foo(ProcessorHandler<?, ?, ?> h) { }
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
                List.of(new SourceFile("UsesLegacyApi.java", src))).call();

        assertThat(ok)
                .as("import com.flezha.sdk.ProcessorHandler should not resolve after relay-mvp-java")
                .isFalse();
        assertThat(err.toString())
                .contains("com.flezha.sdk.ProcessorHandler");
    }

    @Test
    @DisplayName("user code importing FactoryRegistry fails to compile")
    void importingFactoryRegistryFails() {
        String src = """
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
                List.of(new SourceFile("UsesLegacyApi.java", src))).call();

        assertThat(ok)
                .as("import com.flezha.sdk.FactoryRegistry should not resolve after relay-mvp-java")
                .isFalse();
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
