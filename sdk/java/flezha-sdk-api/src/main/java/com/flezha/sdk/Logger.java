package com.flezha.sdk;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.annotation.Nonnull;

/**
 * The SDK's diagnostics, through a sink the host supplies.
 *
 * <p>The default writes to standard error at {@code warn} and above, which is
 * what the other SDKs' defaults amount to: a runner that says nothing about a
 * wedge is the failure mode diagnostics exist to close, and one that narrates
 * every activation is noise. There is no logging dependency: a host that wants
 * SLF4J, Log4j, spans or structured fields supplies its own sink through
 * {@link FlezhaRunner#logger(Logger)}.
 *
 * <p>One method carries the diagnostic, so an adapter is a lambda; the four
 * level methods are the call sites' spelling. Messages use SLF4J's {@code {}}
 * placeholder and are substituted by the sink, not before it, so a disabled
 * level costs no formatting. A sink that renders the line itself calls
 * {@link #render(String, Object...)} rather than writing a second
 * substitution.
 */
@FunctionalInterface
public interface Logger {

    String PLACEHOLDER = "{}";

    /**
     * Emit one diagnostic. {@code format} carries {@link #PLACEHOLDER} holes
     * for {@code args}; an argument the holes leave unconsumed is the
     * throwable, as in SLF4J.
     */
    void log(LogLevel level, String format, Object... args);

    default void debug(String format, Object... args) {
        log(LogLevel.DEBUG, format, args);
    }

    default void info(String format, Object... args) {
        log(LogLevel.INFO, format, args);
    }

    default void warn(String format, Object... args) {
        log(LogLevel.WARN, format, args);
    }

    default void error(String format, Object... args) {
        log(LogLevel.ERROR, format, args);
    }

    /**
     * SLF4J's substitution for the shapes the SDK uses: {@link #PLACEHOLDER}
     * consumed in order, arguments the holes leave over ignored, except a
     * single trailing {@link Throwable}, whose stack trace follows the
     * message.
     */
    @Nonnull
    static String render(String format, Object... args) {
        if (format == null) {
            return "null";
        }
        if (args == null || args.length == 0) {
            return format;
        }
        StringBuilder out = new StringBuilder(format.length() + 32);
        int arg = 0;
        int from = 0;
        int hole = format.indexOf(PLACEHOLDER, from);
        while (hole >= 0 && arg < args.length) {
            out.append(format, from, hole).append(args[arg++]);
            from = hole + PLACEHOLDER.length();
            hole = format.indexOf(PLACEHOLDER, from);
        }
        out.append(format, from, format.length());
        if (arg == args.length - 1 && args[arg] instanceof Throwable cause) {
            out.append(System.lineSeparator()).append(stackTrace(cause));
        }
        return out.toString();
    }

    private static String stackTrace(Throwable cause) {
        StringWriter text = new StringWriter();
        cause.printStackTrace(new PrintWriter(text));
        return text.toString().stripTrailing();
    }
}
