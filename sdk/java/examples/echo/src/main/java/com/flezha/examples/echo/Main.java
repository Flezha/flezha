package com.flezha.examples.echo;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.FlezhaRunner;
import com.flezha.sdk.admin.Admin;
import com.flezha.sdk.admin.OperativeSpec;

/**
 * Starts a runner: a process that connects to the controller and serves your operatives.
 *
 * <p>Declare every operative you want served before calling {@code run()}. One runner can serve
 * several, and {@code defineWorkflow(...)} is the same call for the workflow kind.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        // run() is start() followed by awaitTermination(). Splitting them keeps the handle,
        // which is the only way to reach admin(), and still blocks until shutdown.
        FlezhaRunner runner = Flezha.createRunner(args.length > 0 ? args[0] : "default")
                .defineStream(EchoOperative.class)
                .start();

        // Everything `flz operative` and `flz plugin` do is also a method here. Uncomment to
        // drive this namespace from the application instead of from a terminal:
        //
        // runner.admin().startOperative(OperativeSpec.of("my_echo", "echo"));
        // runner.admin().send("my_echo", "hello world", "in");
        //
        // Attaching a plugin is the same handle; docs/sdk/admin.md has the whole surface. An
        // enforced controller refuses all of it until `flz role bind runner@default app default`.

        runner.awaitTermination();
    }

    private Main() {}
}
