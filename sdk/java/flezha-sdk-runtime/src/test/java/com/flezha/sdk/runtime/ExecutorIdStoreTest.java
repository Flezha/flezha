package com.flezha.sdk.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Stable-identity store (Rust {@code persistent_id.rs} parity): atomic write +
 * read round-trip, missing/blank handling, derivation, reload, and the
 * no-persistence degradation contract.
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

    @Test
    void deriveIdHasNamespacePrefixProfileHashAndRandomSuffix() {
        String id = ExecutorIdStore.deriveId("default", List.of("b_factory", "a_factory"));
        String[] parts = id.split("-", 3);
        assertThat(parts).hasSize(3);
        assertThat(parts[0]).isEqualTo("default");
        assertThat(parts[1]).hasSize(8).matches("[0-9a-f]{8}");
        assertThat(parts[2]).hasSize(6);
    }

    @Test
    void deriveIdProfileSegmentIsOrderIndependentAndSetSensitive() {
        // Sorted before hashing → the profile segment is identical regardless of
        // input order, and differs for a different factory set.
        String a = ExecutorIdStore.deriveId("ns", List.of("a", "b", "c")).split("-", 3)[1];
        String b = ExecutorIdStore.deriveId("ns", List.of("c", "a", "b")).split("-", 3)[1];
        String c = ExecutorIdStore.deriveId("ns", List.of("a", "b")).split("-", 3)[1];
        assertThat(a).isEqualTo(b);
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    void deriveOrLoadReloadsAPersistedId(@TempDir Path dir) {
        // First derives + persists; a second call against the same path returns
        // the SAME id (binding continuity across restart).
        Path path = dir.resolve("id");
        String first = ExecutorIdStore.deriveOrLoad(path, "ns", List.of("f1"));
        String second = ExecutorIdStore.deriveOrLoad(path, "ns", List.of("f1"));
        assertThat(first).isEqualTo(second);
        assertThat(ExecutorIdStore.loadFrom(path)).contains(first);
    }

    @Test
    void unwritablePathDegradesToAFreshIdWithoutThrowing(@TempDir Path dir) throws Exception {
        // Parent is a FILE → createDirectories fails → persist fails, but
        // deriveOrLoad must still return a usable id and never throw.
        Path blocker = dir.resolve("blocker");
        Files.writeString(blocker, "x");
        Path path = blocker.resolve("nested");
        String id = ExecutorIdStore.deriveOrLoad(path, "ns", List.of("f1"));
        assertThat(id).startsWith("ns-");
        assertThat(ExecutorIdStore.loadFrom(path)).isEmpty();
    }
}
