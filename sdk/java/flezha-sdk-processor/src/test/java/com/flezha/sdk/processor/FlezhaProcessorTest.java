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
    void rejectsStaticAndDynamicTogether() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.*;
                import java.util.List;
                import java.util.Optional;
                @Shard(id = "bad", keys = {"a"})
                public class Bad {
                    @PartitionFor public List<String> p(String in) { return List.of("a"); }
                    @ShardKey public Optional<String> s(String in) { return Optional.empty(); }
                    public String run(String in) { return in; }
                }
                """;
        assertThat(diagnose("t.Bad", code)).anyMatch(m -> m.contains("mutually exclusive"));
    }

    @Test
    void rejectsTooManyRunParameters() {
        String code =
                """
                package t;
                import com.flezha.sdk.authoring.Shard;
                @Shard(id = "bad2")
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
        JavaCompiler.CompilationTask task =
                compiler.getTask(null, fm, diags, List.of("-proc:only"), null, List.of(new StringSource(fqn, code)));
        task.setProcessors(List.of(new FlezhaProcessor()));
        boolean ok = task.call();
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
