package com.flezha.examples.agentdesk;

import com.fasterxml.jackson.annotation.JsonInclude;

/** The gateway's answer to a request: a status and a body, or the session to stream. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Http(Integer status, String body, String stream) {

    static Http of(int status, String body) {
        return new Http(status, body, null);
    }

    static Http stream(String session) {
        return new Http(null, null, session);
    }
}
