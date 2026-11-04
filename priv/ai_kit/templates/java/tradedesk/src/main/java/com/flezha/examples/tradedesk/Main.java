package com.flezha.examples.tradedesk;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.FlezhaRunner;

/**
 * Starts the desk's runner: the process that connects to the controller and serves tradedesk's
 * relays.
 *
 * <p>The namespace is the first argument and defaults to {@code trading}. A runner serves one
 * namespace, and at most one runner may run per process. Under an enforced controller it proves
 * its namespace with that namespace's key, read from {@code FLEZHA_NAMESPACE_SECRET}.
 *
 * <p>The guides write the desk's relays one at a time, and each is registered here before
 * {@code start()}. Registering a factory starts nothing: it offers the controller relays of
 * that type, which are started from it later. {@code registerFactory(...)} is the same call for
 * either run, the flow that tracks prices and the steps relay that places one order.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        FlezhaRunner runner = Flezha.createRunner(args.length > 0 ? args[0] : "trading")
                .registerFactory(TradeWatcherRelay.class)
                .registerFactory(OrderExecutorRelay.class)
                .start();

        runner.awaitTermination();
    }

    private Main() {}
}
