package com.flezha.examples.agentdesk;

/** A message for a user's session, as the gateway sends it: the session is named after the user. */
public record Message(String user, String text) {}
