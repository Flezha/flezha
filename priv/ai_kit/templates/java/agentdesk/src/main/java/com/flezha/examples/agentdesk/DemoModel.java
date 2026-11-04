package com.flezha.examples.agentdesk;

import com.flezha.examples.agentdesk.Reply.Answer;
import com.flezha.sdk.tasklet.TaskletException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * The demo model: a canned answer, one token at a time, and the number of tokens it used. It
 * stands in for a real model so the example runs anywhere; a real model's client would be called
 * here instead, handing each token it streams to {@code part} the same way.
 */
final class DemoModel {

    private static final int MAX_MESSAGE = 200;
    private static final Duration PER_TOKEN = Duration.ofMillis(50);

    static Answer answer(List<String> earlier, String message, Consumer<Answer> part) throws TaskletException {
        if (message.length() > MAX_MESSAGE) {
            throw new TaskletException.NonRetryable(
                    "the demo model reads at most " + MAX_MESSAGE + " characters, and this message has " + message.length());
        }
        String text = "Message " + (earlier.size() + 1) + ": you said \"" + message + "\". I am a demo model.";
        String[] tokens = text.split("(?<= )");
        for (int i = 0; i < tokens.length; i++) {
            part.accept(new Answer(tokens[i], i + 1));
            LockSupport.parkNanos(PER_TOKEN.toNanos());
        }
        return new Answer(text, tokens.length);
    }

    private DemoModel() {}
}
