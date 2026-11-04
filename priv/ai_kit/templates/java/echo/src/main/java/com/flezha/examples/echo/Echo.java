package com.flezha.examples.echo;

import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;

/**
 * A processing stage of the relay's graph, invoked once per message. This one prefixes the
 * input and emits the result.
 *
 * <p>{@code Tasklet<String, Void, String>} reads input, state, output: a string in, no state, a
 * string out. With {@code Void} state nothing is kept between messages and nothing is journaled.
 *
 * <p>{@code name} is the tasklet's stable identity. State is filed under it, so changing it later
 * declares a different tasklet rather than renaming this one.
 *
 * <p>Where the result goes is not decided here. The relay decides that, so the graph can be
 * rewired without touching this class.
 */
@Tasklet.Spec(name = "echo")                 // state is saved under this name, so choose it once
public final class Echo implements Tasklet<String, Void, String> {

    @Override
    public void run(String input, Ctx<Void, String> ctx) throws TaskletException {
        ctx.emit("echo: " + input);          // where it goes is the relay's business
    }
}
