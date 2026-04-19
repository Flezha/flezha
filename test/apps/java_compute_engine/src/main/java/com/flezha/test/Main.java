package com.flezha.test;

import com.flezha.sdk.FactoryMetadata;
import com.flezha.sdk.FactoryRegistry;
import com.flezha.sdk.Flezha;

/**
 * Mirror of {@code test/apps/rust_compute_engine/src/main.rs}. Accepts the
 * namespace as its first command-line argument and keeps the process alive
 * until stdin closes — matching the Rust test app's {@code std_keeper}.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String namespace = args.length > 0 ? args[0] : "default";
        defineFactories();
        Flezha unused = Flezha.newInstance(namespace);
        // Intentionally unused — the SDK instance holds the connection open;
        // the JVM stays alive until stdin closes.
        stdKeeper();
    }

    private static void defineFactories() {
        FactoryRegistry.registerFactory(
                "do_nothing",
                DoNothingProcessor.settings(),
                new DoNothingProcessor.State(),
                DoNothingProcessor::new,
                FactoryMetadata.of("Passes input through unchanged"));

        FactoryRegistry.registerFactory(
                "store_input",
                StoreInputProcessor.settings(),
                new StoreInputProcessor.State(),
                StoreInputProcessor::new,
                FactoryMetadata.of("Accumulates all inputs in state for later retrieval"));

        FactoryRegistry.registerFactory(
                "compare_filter",
                CompareFilterProcessor.settings(),
                new CompareFilterProcessor.State("default", false),
                CompareFilterProcessor::new,
                FactoryMetadata.of("Filters by comparing input against a stored value"));
    }

    private static void stdKeeper() {
        try {
            byte[] buf = new byte[1];
            while (true) {
                int n = System.in.read(buf);
                if (n == -1) {
                    System.exit(0);
                }
                Thread.sleep(1_000);
            }
        } catch (Exception e) {
            System.exit(0);
        }
    }
}
