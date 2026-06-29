package com.flezha.sdk.state;

/**
 * Restore payload threaded into {@code Operation.initState} when the controller ships a
 * {@code RestoreInitState} command. Carries the latest snapshot bytes (or
 * empty array for "no snapshot") and the trailing change-log bytes since that
 * snapshot.
 *
 * <p>The user's {@code initState} is a fresh-start constructor; the runtime
 * auto-applies the payload (Kryo-deserialize the snapshot, fold the change
 * log via {@code Operation.applyChange}) AFTER {@code initState} returns. Users
 * may inspect the byte sizes for instrumentation but should not generally
 * consume the bytes themselves.
 */
public record RestoreInitPayload(byte[] snapshotBytes, byte[] changeLogBytes) {}
