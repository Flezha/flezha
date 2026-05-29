package com.flezha.sdk.runtime;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node-local persistence of the controller-assigned executor id (Rust Phase D
 * parity: {@code executor/persistent_id.rs}).
 *
 * <p>The id is written after every {@code AssignExecutorId} /
 * {@code ReassignExecutorId} (atomically: temp file + move) so a fresh process
 * that restarts on the same node can present it as {@code previous_executor_id}
 * in its {@code connect_control_channel} handshake. The controller then evicts
 * the stale binding before re-binding instead of leaving a corpse for the
 * takeover to rebind onto.
 *
 * <p>Best-effort: a write failure never disrupts the executor — it only
 * forfeits the reconnect optimisation. The path must live on a node-local
 * hostPath (not {@code emptyDir}, which does not survive a pod restart); ops
 * mounts it and points {@code FLEZHA_EXECUTOR_ID_PATH} at it.
 */
public final class ExecutorIdStore {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorIdStore.class);
    private static final String ENV_ID_PATH = "FLEZHA_EXECUTOR_ID_PATH";
    private static final String DEFAULT_ID_PATH = "/var/lib/flezha-executor/id";

    private ExecutorIdStore() {}

    @Nonnull
    public static Path idPath() {
        String env = System.getenv(ENV_ID_PATH);
        return Path.of(env != null && !env.isBlank() ? env : DEFAULT_ID_PATH);
    }

    /** Read the persisted id from a prior process, if present and non-empty. */
    @Nonnull
    public static Optional<String> load() {
        return loadFrom(idPath());
    }

    /** Atomically persist {@code id}. Best-effort: logs at debug on failure. */
    public static void persist(@Nonnull String id) {
        try {
            persistTo(idPath(), id);
        } catch (IOException e) {
            LOG.debug("executor id persist failed: {}", e.toString());
        }
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
