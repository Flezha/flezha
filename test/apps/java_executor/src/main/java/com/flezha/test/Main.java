package com.flezha.test;

import com.flezha.sdk.Flezha;

/**
 * Mirror of {@code test/apps/rust_executor/src/main.rs}. Accepts the
 * namespace as its first command-line argument and keeps the process alive
 * until stdin closes — matching the Rust test app's {@code std_keeper}.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String namespace = args.length > 0 ? args[0] : "default";
        registerUnboundedOperatives();
        registerBoundedOperatives();
        Flezha unused = Flezha.newInstance(namespace);
        // Intentionally unused — the SDK instance holds the connection open;
        // the JVM stays alive until stdin closes.
        stdKeeper();
    }

    private static void registerUnboundedOperatives() {
        Flezha.registerUnbounded(DoNothingUnboundedOperative.class);
        Flezha.registerUnbounded(StoreInputUnboundedOperative.class);
        Flezha.registerUnbounded(CompareFilterUnboundedOperative.class);
        Flezha.registerUnbounded(StatefulCounterUnboundedOperative.class);
        Flezha.registerUnbounded(CoalescingCounterUnboundedOperative.class);
        Flezha.registerUnbounded(PersistenceOffUnboundedOperative.class);
        Flezha.registerUnbounded(HybridChannelUnboundedOperative.class);
        // EchoMessageUnboundedOperative is reintroduced once per-Operation `handleMessage` lands.
    }

    private static void registerBoundedOperatives() {
        Flezha.registerBounded(EchoBoundedOperative.class);
        Flezha.registerBounded(CommitBlockBoundedOperative.class);
        Flezha.registerBounded(SagaBlockBoundedOperative.class);
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
