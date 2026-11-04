package com.flezha.examples.agentdesk;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One HTTP request, as the plugin hands it to the gateway. The gateway knows two routes:
 * {@code POST /sessions/<user>/messages}, whose body is the message, and
 * {@code GET /sessions/<user>/events}, the session's answers as they are written.
 */
@JsonIgnoreProperties(ignoreUnknown = true)                  // the envelope carries more than the gateway reads
public record Request(String method, String path, String body) {

    private static final Pattern ROUTE = Pattern.compile("/sessions/([a-z][a-z0-9_-]{0,63})/(messages|events)");

    boolean sends() {
        return is("POST", "messages");
    }

    boolean listens() {
        return is("GET", "events");
    }

    String user() {
        return route(1);
    }

    Message message() {
        return new Message(user(), Objects.requireNonNullElse(body, ""));
    }

    private boolean is(String verb, String what) {
        return method.equals(verb) && route(2).equals(what);
    }

    private String route(int part) {                         // "" when the path is not a route
        Matcher route = ROUTE.matcher(path);
        return route.matches() ? route.group(part) : "";
    }
}
