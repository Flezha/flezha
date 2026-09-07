package com.flezha.examples.echo;

import com.flezha.sdk.Flezha;

/**
 * Starts a runner: a process that connects to the controller and serves your operatives.
 *
 * <p>Declare every operative you want served before calling {@code run()}. One runner can serve
 * several, and {@code defineWorkflow(...)} is the same call for the workflow kind.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        // run() connects and then blocks until shutdown, which is either a SIGTERM or a close()
        // from another thread. Use start() instead when you want to keep the calling thread.
        Flezha.createRunner(args.length > 0 ? args[0] : "my_app")
                .defineStream(EchoOperative.class)
                .run();
    }

    private Main() {}
}
