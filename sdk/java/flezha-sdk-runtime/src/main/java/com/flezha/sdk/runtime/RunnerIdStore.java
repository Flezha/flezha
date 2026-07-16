package com.flezha.sdk.runtime;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Self-owned, durable <b>stable identity</b> for the runner (Rust parity:
 * {@code runner/persistent_id.rs}).
 *
 * <p>Created once on first boot and persisted at {@code FLEZHA_RUNNER_ID_PATH}
 * (atomically: temp file + move) so the controller can trust it as the
 * operative-binding key across a process restart on the same node. Derived for
 * readability as {@code "<namespace>-<profile_hash8>-<rand6>"} (the boot-time
 * hostname is deliberately NOT load-bearing — a container hostname churns on
 * recreate).
 *
 * <p>Degradation: with no writable path a fresh per-boot id is generated, cached
 * for the life of the process (so reconnects present the same id), and a single
 * {@code warn} is logged. A restart then presents a new id whose old binding
 * ages out via {@code FLEZHA_RUNNER_KEEPALIVE}. The runner never refuses to
 * start.
 */
public final class RunnerIdStore {

    private static final Logger LOG = LoggerFactory.getLogger(RunnerIdStore.class);
    private static final String ENV_ID_PATH = "FLEZHA_RUNNER_ID_PATH";
    private static final String DEFAULT_ID_PATH = "/var/lib/flezha-runner/id";

    // One stable id per process — reconnects present the SAME id even in the
    // degraded no-persist case where nothing is on disk to re-load.
    private static final AtomicReference<String> STABLE_ID = new AtomicReference<>();

    private RunnerIdStore() {}

    @Nonnull
    public static Path idPath() {
        String env = System.getenv(ENV_ID_PATH);
        return Path.of(env != null && !env.isBlank() ? env : DEFAULT_ID_PATH);
    }

    /**
     * The runner's stable identity for this process. Loads the persisted id if
     * present; otherwise derives one from {@code namespace} + the declared
     * factory set, persists it best-effort, and caches it. Subsequent calls
     * (including every reconnect) return the same cached value.
     */
    @Nonnull
    public static String stableId(@Nonnull String namespace, @Nonnull List<String> factoryIds) {
        String existing = STABLE_ID.get();
        if (existing != null) {
            return existing;
        }
        synchronized (STABLE_ID) {
            existing = STABLE_ID.get();
            if (existing != null) {
                return existing;
            }
            String id = deriveOrLoad(idPath(), namespace, factoryIds);
            STABLE_ID.set(id);
            return id;
        }
    }

    @Nonnull
    static String deriveOrLoad(@Nonnull Path path, @Nonnull String namespace, @Nonnull List<String> factoryIds) {
        Optional<String> existing = loadFrom(path);
        if (existing.isPresent()) {
            return existing.get();
        }
        String id = deriveId(namespace, factoryIds);
        try {
            persistTo(path, id);
        } catch (IOException e) {
            // Degradation: no binding continuity across restart, but the
            // runner runs normally with a per-boot id.
            LOG.warn(
                    "runner stable-id persist failed at {}: {} — using a fresh per-boot id "
                            + "(no binding continuity across restart)",
                    path,
                    e.toString());
        }
        return id;
    }

    /**
     * {@code "<namespace>-<profile_hash8>-<rand6>"}. Order-independent (factory
     * ids are sorted before hashing) and dependency-free (FNV-1a).
     */
    @Nonnull
    static String deriveId(@Nonnull String namespace, @Nonnull List<String> factoryIds) {
        List<String> sorted = new ArrayList<>(factoryIds);
        sorted.sort(null);
        return namespace + "-" + profileHash8(sorted) + "-" + rand6();
    }

    @Nonnull
    static String profileHash8(@Nonnull List<String> sortedIds) {
        // FNV-1a over the sorted, NUL-delimited factory ids.
        long hash = 0xcbf29ce484222325L;
        for (String id : sortedIds) {
            for (byte b : id.getBytes(StandardCharsets.UTF_8)) {
                hash ^= (b & 0xff);
                hash *= 0x100000001b3L;
            }
            // NUL separator.
            hash *= 0x100000001b3L;
        }
        return String.format("%08x", hash & 0xffffffffL);
    }

    @Nonnull
    private static String rand6() {
        // Dependency-free entropy: clock noise mixed with the pid keeps two fresh
        // runners distinct (cryptographic randomness is not needed — the
        // persistence path already scopes uniqueness per runner).
        long seed = System.nanoTime() ^ (ProcessHandle.current().pid() << 32) ^ Long.rotateLeft(System.nanoTime(), 17);
        final String alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder out = new StringBuilder(6);
        for (int i = 0; i < 6; i++) {
            seed ^= seed << 13;
            seed ^= seed >>> 7;
            seed ^= seed << 17;
            out.append(alphabet.charAt(Math.floorMod(seed, alphabet.length())));
        }
        return out.toString();
    }

    @Nonnull
    static Optional<String> loadFrom(@Nonnull Path path) {
        try {
            String id = Files.readString(path, StandardCharsets.UTF_8).trim();
            return id.isEmpty() ? Optional.empty() : Optional.of(id);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    static void persistTo(@Nonnull Path path, @Nonnull String id) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(tmp, id, StandardCharsets.UTF_8);
        try {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
