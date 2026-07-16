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
class RunnerIdStoreTest {

    @Test
    void persistThenLoadRoundTrips(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("id");
        RunnerIdStore.persistTo(path, "exec-123");
        assertThat(RunnerIdStore.loadFrom(path)).contains("exec-123");
    }

    @Test
    void loadMissingFileIsEmpty(@TempDir Path dir) {
        assertThat(RunnerIdStore.loadFrom(dir.resolve("absent"))).isEmpty();
    }

    @Test
    void blankIdIsEmpty(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("id");
        RunnerIdStore.persistTo(path, "   \n");
        assertThat(RunnerIdStore.loadFrom(path)).isEmpty();
    }

    @Test
    void persistOverwritesPriorId(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("id");
        RunnerIdStore.persistTo(path, "old");
        RunnerIdStore.persistTo(path, "new");
        assertThat(RunnerIdStore.loadFrom(path)).contains("new");
        // The temp sidecar must not linger.
        assertThat(Files.exists(path.resolveSibling("id.tmp"))).isFalse();
    }

    @Test
    void deriveIdHasNamespacePrefixProfileHashAndRandomSuffix() {
        String id = RunnerIdStore.deriveId("default", List.of("b_factory", "a_factory"));
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
        String a = RunnerIdStore.deriveId("ns", List.of("a", "b", "c")).split("-", 3)[1];
        String b = RunnerIdStore.deriveId("ns", List.of("c", "a", "b")).split("-", 3)[1];
        String c = RunnerIdStore.deriveId("ns", List.of("a", "b")).split("-", 3)[1];
        assertThat(a).isEqualTo(b);
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    void deriveOrLoadReloadsAPersistedId(@TempDir Path dir) {
        // First derives + persists; a second call against the same path returns
        // the SAME id (binding continuity across restart).
        Path path = dir.resolve("id");
        String first = RunnerIdStore.deriveOrLoad(path, "ns", List.of("f1"));
        String second = RunnerIdStore.deriveOrLoad(path, "ns", List.of("f1"));
        assertThat(first).isEqualTo(second);
        assertThat(RunnerIdStore.loadFrom(path)).contains(first);
    }

    @Test
    void unwritablePathDegradesToAFreshIdWithoutThrowing(@TempDir Path dir) throws Exception {
        // Parent is a FILE → createDirectories fails → persist fails, but
        // deriveOrLoad must still return a usable id and never throw.
        Path blocker = dir.resolve("blocker");
        Files.writeString(blocker, "x");
        Path path = blocker.resolve("nested");
        String id = RunnerIdStore.deriveOrLoad(path, "ns", List.of("f1"));
        assertThat(id).startsWith("ns-");
        assertThat(RunnerIdStore.loadFrom(path)).isEmpty();
    }
}
