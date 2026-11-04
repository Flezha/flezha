package com.flezha.examples.echo;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.FlezhaRunner;
import com.flezha.sdk.SendOpts;
import com.flezha.sdk.control.Control;
import com.flezha.sdk.control.RelaySpec;
import com.flezha.sdk.control.PluginSpec;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Starts a runner: a process that connects to the controller and serves your relays.
 *
 * <p>Declare every relay you want served before calling {@code run()}. One runner can serve
 * several, and {@code registerFactory(...)} is the same call for the steps run.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        // run() is start() followed by awaitTermination(). Splitting them keeps the handle,
        // which is the only way to reach control() and send(), and still blocks until shutdown.
        FlezhaRunner runner = Flezha.createRunner(args.length > 0 ? args[0] : "default")
                .registerFactory(EchoRelay.class)     // the relays this runner serves
                .start();                                // connects to the controller

        // Everything `flz relay` and `flz plugin` do is also a method of control(), and
        // send() hands a message to a relay. Uncomment to drive this namespace from the
        // application instead of from a terminal:
        //
        // Control control = runner.control();
        // control.startRelay(RelaySpec.of("echo").id("my_echo"));
        //
        // ObjectNode opts = JsonNodeFactory.instance.objectNode().put("path", "echo-out.txt");
        // control.enablePlugin("writer", PluginSpec.of("relay_file_output").opts(opts));
        // ArrayNode hooks = JsonNodeFactory.instance.arrayNode();
        // hooks.addArray().add("on_output").addObject();
        // control.bindPlugin("writer", "my_echo", hooks, null);
        //
        // runner.send("my_echo", "hello world", "in", SendOpts.of().msgId("hello-1"));
        //
        // docs/public/topics/core/sending_and_calling.md covers sending. An enforced controller
        // refuses all of it until `flz role bind runner@default app default`.

        runner.awaitTermination();
    }

    private Main() {}
}
