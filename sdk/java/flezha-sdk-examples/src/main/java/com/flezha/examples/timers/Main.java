package com.flezha.examples.timers;

import com.flezha.sdk.Flezha;

/**
 * Starts a runner: the process that connects to a controller and serves the operatives declared on
 * it. {@code run()} connects and blocks until shutdown; {@code start()} returns to the caller and
 * leaves the runner to be closed explicitly.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        Flezha.createRunner(args.length > 0 ? args[0] : "my_app")
                .defineStream(SessionOperative.class)
                .run();
    }

    private Main() {}
}
