package com.flezha.examples.starter;

import com.flezha.sdk.Flezha;

/** The runner: connects to the controller and serves the starter relay until shutdown. */
public final class Main {

    public static void main(String[] args) throws Exception {
        Flezha.createRunner(args.length > 0 ? args[0] : "default") // the namespace
                .registerFactory(StarterRelay.class)
                .run();
    }

    private Main() {}
}
