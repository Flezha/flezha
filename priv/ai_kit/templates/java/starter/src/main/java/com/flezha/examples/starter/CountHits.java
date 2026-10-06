package com.flezha.examples.starter;

import com.flezha.sdk.state.Change;
import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;

/**
 * Counts the visits of one page and answers the new count.
 *
 * <p>The relay keys the channel by page, so each page has a cell of its own, and {@code Seen} is
 * that cell's state. The state changes only through the {@code Counted} change, which is recorded
 * as it happens, so a runner that restarts finds the count it had.
 */
@Tasklet.Spec(name = "count") // state is saved under this name, so choose it once
public final class CountHits implements Tasklet<CountHits.Hit, CountHits.Seen, CountHits.Count> {

    public record Hit(String page) {}

    public record Count(String page, long total) {}

    public static class Seen { // what each page's cell remembers
        public long total;
    }

    public record Counted() implements Change<Seen> { // how a visit changes it
        @Override
        public void applyTo(Seen seen) {
            seen.total++;
        }
    }

    @Override
    public void run(Hit hit, Ctx<Seen, Count> ctx) throws TaskletException {
        ctx.change(new Counted());
        ctx.emit(new Count(hit.page(), ctx.state().total));
    }
}
