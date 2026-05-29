package com.flezha.sdk.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Node-local executor-id persistence (Rust {@code persistent_id.rs} parity):
 * atomic write + read round-trip, missing/blank handling.
 */
class ExecutorIdStoreTest {

    @Test
    void persistThenLoadRoundTrips(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("id");
        ExecutorIdStore.persistTo(path, "exec-123");
        assertThat(ExecutorIdStore.loadFrom(path)).contains("exec-123");
    }

    @Test
    void loadMissingFileIsEmpty(@TempDir Path dir) {
        assertThat(ExecutorIdStore.loadFrom(dir.resolve("absent"))).isEmpty();
    }

    @Test
    void blankIdIsEmpty(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("id");
        ExecutorIdStore.persistTo(path, "   \n");
        assertThat(ExecutorIdStore.loadFrom(path)).isEmpty();
    }

    @Test
    void persistOverwritesPriorId(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("id");
        ExecutorIdStore.persistTo(path, "old");
        ExecutorIdStore.persistTo(path, "new");
        assertThat(ExecutorIdStore.loadFrom(path)).contains("new");
        // The temp sidecar must not linger.
        assertThat(Files.exists(path.resolveSibling("id.tmp"))).isFalse();
    }
}
