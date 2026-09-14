package com.flezha.examples.echo;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.FlezhaRunner;
import com.flezha.sdk.admin.Admin;
import com.flezha.sdk.admin.OperativeSpec;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
        // Admin admin = runner.admin();
        // admin.startOperative(OperativeSpec.of("my_echo", "echo"));
        //
        // ObjectNode sink = JsonNodeFactory.instance.objectNode();
        // sink.put("id", "writer");
        // sink.put("provider", "operative_file_sink");
        // sink.put("operative_id", "my_echo");
        // sink.putObject("opts").put("path", "echo-out.txt");
        // sink.putArray("hooks").addArray().add("on_output").addObject();
        // admin.enablePlugin("writer", sink);
        //
        // admin.send("my_echo", "hello world", "in");
        //
        // docs/sdk/admin.md has the whole surface. An enforced controller refuses all of it until
        // `flz role bind runner@default app default`.

        runner.awaitTermination();
    }

    private Main() {}
}
