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
        registerRelays();
        registerTasks();
        Flezha unused = Flezha.newInstance(namespace);
        // Intentionally unused — the SDK instance holds the connection open;
        // the JVM stays alive until stdin closes.
        stdKeeper();
    }

    private static void registerRelays() {
        Flezha.registerRelay(DoNothingRelay.class);
        Flezha.registerRelay(StoreInputRelay.class);
        Flezha.registerRelay(CompareFilterRelay.class);
        Flezha.registerRelay(StatefulCounterRelay.class);
        Flezha.registerRelay(CoalescingCounterRelay.class);
        Flezha.registerRelay(PersistenceOffRelay.class);
        Flezha.registerRelay(HybridChannelRelay.class);
        // EchoMessageRelay is reintroduced once per-Shard `handleMessage` lands.
    }

    private static void registerTasks() {
        Flezha.registerTask(EchoTask.class);
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
