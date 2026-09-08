package com.flezha.sdk;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The SDK's own logger: the level rule {@code FLEZHA_LOG_LEVEL} feeds, the
 * default sink's filtering and formatting, and the JVM-wide holder the runner
 * builder replaces. The holder is process state, so every test restores it.
 */
class LoggerTest {

    /** Records what reached it, so a test asserts on the sink rather than on a stream. */
    private static final class Recorder implements Logger {
        private final List<String> lines = new ArrayList<>();

        @Override
        public void log(LogLevel level, String format, Object... args) {
            lines.add(level + " " + Logger.render(format, args));
        }
    }

    private PrintStream realErr;
    private ByteArrayOutputStream captured;

    @BeforeEach
    void captureStandardError() {
        realErr = System.err;
        captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, UTF_8));
    }

    @AfterEach
    void restoreProcessState() {
        System.setErr(realErr);
        Flezha.currentRunner().ifPresent(FlezhaRunner::close);
        com.flezha.sdk.runtime.FlezhaImpl.resetForTests();
        Flezha.LOGGER.set(ConsoleLogger.fromEnv());
    }

    private String stderr() {
        return captured.toString(UTF_8);
    }

    @Test
    @DisplayName("a named level parses case-insensitively; unset, blank and unrecognised all mean warn")
    void levelNamesParse() {
        assertThat(LogLevel.from("debug")).isEqualTo(LogLevel.DEBUG);
        assertThat(LogLevel.from("INFO")).isEqualTo(LogLevel.INFO);
        assertThat(LogLevel.from(" Error ")).isEqualTo(LogLevel.ERROR);

        assertThat(LogLevel.from(null)).as("unset").isEqualTo(LogLevel.WARN);
        assertThat(LogLevel.from("")).as("empty").isEqualTo(LogLevel.WARN);
        assertThat(LogLevel.from("   ")).as("blank").isEqualTo(LogLevel.WARN);
        assertThat(LogLevel.from("trace")).as("unrecognised").isEqualTo(LogLevel.WARN);
    }

    @Test
    @DisplayName("the default sink writes at and above its level and nothing below it")
    void theDefaultSinkFilters() {
        new ConsoleLogger(LogLevel.WARN).debug("d");
        new ConsoleLogger(LogLevel.WARN).info("i");
        new ConsoleLogger(LogLevel.WARN).warn("w");
        new ConsoleLogger(LogLevel.WARN).error("e");
        assertThat(stderr()).contains("[flezha] w", "[flezha] e").doesNotContain("[flezha] d", "[flezha] i");

        captured.reset();
        new ConsoleLogger(LogLevel.DEBUG).debug("d");
        assertThat(stderr()).contains("[flezha] d");

        captured.reset();
        new ConsoleLogger(LogLevel.ERROR).warn("w");
        assertThat(stderr()).isEmpty();
    }

    @Test
    @DisplayName("placeholders are filled in order; surplus arguments are ignored and surplus placeholders stay")
    void placeholdersAreSubstituted() {
        assertThat(Logger.render("no arguments")).isEqualTo("no arguments");
        assertThat(Logger.render("core poll failed: {}", "boom")).isEqualTo("core poll failed: boom");
        assertThat(Logger.render("grew to {} (ceiling {})", 4, 20)).isEqualTo("grew to 4 (ceiling 20)");
        assertThat(Logger.render("Invalid {}={}; falling back to {}", "FLEZHA_WORKER_POOL_SIZE", "x", 2))
                .isEqualTo("Invalid FLEZHA_WORKER_POOL_SIZE=x; falling back to 2");
        assertThat(Logger.render("one {}", "a", "b"))
                .as("a surplus argument that is not a throwable is dropped, as in SLF4J")
                .isEqualTo("one a");
        assertThat(Logger.render("{} and {}", "a")).isEqualTo("a and {}");
        assertThat(Logger.render("value {}", (Object) null)).isEqualTo("value null");
    }

    @Test
    @DisplayName("an argument the placeholders leave over is the throwable, and its stack trace follows the message")
    void aTrailingThrowableIsRendered() {
        IllegalStateException cause = new IllegalStateException("wedged");

        String noPlaceholder = Logger.render("fetcher thread ended unexpectedly", cause);
        assertThat(noPlaceholder)
                .startsWith("fetcher thread ended unexpectedly")
                .contains("java.lang.IllegalStateException: wedged")
                .contains("aTrailingThrowableIsRendered");

        String withPlaceholder = Logger.render("could not start {} fetcher", "replacement", cause);
        assertThat(withPlaceholder)
                .startsWith("could not start replacement fetcher")
                .contains("java.lang.IllegalStateException: wedged");
    }

    @Test
    @DisplayName("a sink that cannot write does not propagate, so a drain stays clean")
    void aFailedWriteIsSwallowed() {
        System.setErr(new PrintStream(OutputStream.nullOutputStream(), true, UTF_8) {
            @Override
            public void println(String line) {
                throw new IllegalStateException("stream closed");
            }
        });

        assertThatCode(() -> new ConsoleLogger(LogLevel.WARN).warn("in-flight activations did not finish"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("the runner builder routes diagnostics to the supplied sink and off the default one")
    void theBuilderReplacesTheSink() {
        Recorder sink = new Recorder();
        Flezha.createRunner("logger_ns").logger(sink);

        Flezha.log().warn("in-flight activations did not finish within {}ms", 5000);

        assertThat(sink.lines).containsExactly("WARN in-flight activations did not finish within 5000ms");
        assertThat(stderr()).as("the default sink no longer sees it").isEmpty();
    }

    @Test
    @DisplayName("a sink supplied through the builder decides its own level")
    void aSuppliedSinkDecidesItsOwnLevel() {
        Recorder sink = new Recorder();
        Flezha.createRunner("logger_ns").logger(sink);

        Flezha.log().debug("syncFactories: removal of {} has no core surface", "a_factory");

        assertThat(sink.lines).containsExactly("DEBUG syncFactories: removal of a_factory has no core surface");
    }

    @Test
    @DisplayName("a class that captured the SDK logger before the sink was supplied still reaches it")
    void theHolderIsReadAtEveryCall() {
        Logger held = Flezha.log();
        Recorder sink = new Recorder();

        Flezha.createRunner("logger_ns").logger(sink);
        held.error("activation decode failed: {}", "bad frame");

        assertThat(sink.lines).containsExactly("ERROR activation decode failed: bad frame");
    }

    @Test
    @DisplayName("a sink supplied before start still receives what is emitted during and after close()")
    void theSinkOutlivesTheRunner() {
        Recorder sink = new Recorder();
        FlezhaRunner runner = Flezha.createRunner("logger_ns").logger(sink).start();

        Flezha.log().warn("while running");
        runner.close();
        Flezha.log().warn("after close");

        assertThat(Flezha.currentRunner()).as("the slot was released").isEmpty();
        assertThat(sink.lines).containsExactly("WARN while running", "WARN after close");
        assertThat(stderr()).isEmpty();
    }

    @Test
    @DisplayName("the builder refuses a null sink rather than silencing the SDK")
    void theBuilderRefusesNull() {
        assertThatThrownBy(() -> Flezha.createRunner("logger_ns").logger(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("logger");
    }
}
