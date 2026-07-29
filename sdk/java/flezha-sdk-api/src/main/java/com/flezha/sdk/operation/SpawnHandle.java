package com.flezha.sdk.operation;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The async result of a {@link OperationCtx#spawnBounded} request. Observing it
 * is optional: a handle nobody looks at logs one warning if the request failed,
 * so a fire-and-forget requester never loses the diagnosis.
 *
 * <p>"Nobody looked" is decided the moment the caller first asks — {@link #await}
 * and {@link #tryResult} claim the handle before they read it — so a caller that
 * handles its own failures is never also warned about. The exception is a
 * request that failed before it reached the wire (no control plane): it is
 * already resolved when the handle is built, so it always warns — a
 * misconfiguration worth one line whether or not anyone read it.
 */
public final class SpawnHandle {

    private static final Logger LOG = LoggerFactory.getLogger(SpawnHandle.class);

    /**
     * How long a result is awaited. Generous: the controller may have to start
     * an instance, which waits for a runner advertising the factory.
     */
    public static final long RESULT_TIMEOUT_MS = 30_000;

    private final String instanceId;
    private final String factoryId;
    private final CompletableFuture<SpawnOutcome> result;
    private final AtomicBoolean claimed = new AtomicBoolean(false);

    private SpawnHandle(String instanceId, String factoryId, CompletableFuture<SpawnOutcome> result) {
        this.instanceId = instanceId;
        this.factoryId = factoryId;
        this.result = result;
        result.whenComplete((outcome, error) -> {
            if (error != null && !claimed.get()) {
                LOG.warn(
                        "spawnBounded({}, {}) failed: {}",
                        instanceId,
                        factoryId,
                        unwrap(error).getMessage());
            }
        });
    }

    /** A handle awaiting the controller's {@code SpawnBoundedResponse}. */
    @Nonnull
    public static SpawnHandle pending(
            @Nonnull String instanceId, @Nonnull String factoryId, @Nonnull CompletableFuture<String> reply) {
        return new SpawnHandle(
                instanceId, factoryId, reply.thenApply(outcome -> interpret(instanceId, factoryId, outcome)));
    }

    /** A handle that already knows its answer — nothing was sent. */
    @Nonnull
    public static SpawnHandle resolved(
            @Nonnull String instanceId, @Nonnull String factoryId, @Nonnull SpawnOutcome outcome) {
        return new SpawnHandle(instanceId, factoryId, CompletableFuture.completedFuture(outcome));
    }

    /** A handle that already knows it failed — nothing was sent. */
    @Nonnull
    public static SpawnHandle failed(
            @Nonnull String instanceId, @Nonnull String factoryId, @Nonnull SpawnException error) {
        return new SpawnHandle(instanceId, factoryId, CompletableFuture.failedFuture(error));
    }

    /** Block for the controller's answer, up to {@link #RESULT_TIMEOUT_MS}. */
    @Nonnull
    public SpawnOutcome await() throws SpawnException {
        claimed.set(true);
        try {
            return result.get(RESULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SpawnException.Timeout();
        } catch (java.util.concurrent.TimeoutException e) {
            throw new SpawnException.Timeout();
        } catch (ExecutionException | CompletionException e) {
            throw unwrap(e);
        }
    }

    /**
     * The answer if it has already arrived, without blocking. An empty
     * {@link Optional} leaves the handle claimed but unresolved, so a later
     * {@link #await} still sees the result.
     */
    @Nonnull
    public Optional<SpawnOutcome> tryResult() throws SpawnException {
        claimed.set(true);
        if (!result.isDone()) {
            return Optional.empty();
        }
        return Optional.of(await());
    }

    /**
     * Map the controller's outcome string onto the public result.
     * {@code already_running} / {@code factory_unknown} arrive as outcomes
     * rather than errors because they are the controller's decision, not a
     * failed exchange; they become errors here, at the API boundary.
     */
    private static SpawnOutcome interpret(String instanceId, String factoryId, String outcome) {
        switch (outcome) {
            case "spawned":
                return SpawnOutcome.SPAWNED;
            case "delivered":
                return SpawnOutcome.DELIVERED;
            case "already_running":
                throw new CompletionException(new SpawnException.AlreadyRunning(instanceId));
            case "factory_unknown":
                throw new CompletionException(new SpawnException.FactoryUnknown(factoryId));
            default:
                throw new CompletionException(new SpawnException.Refused("unknown outcome `" + outcome + "`"));
        }
    }

    /** The SpawnException behind a future's completion failure. */
    private static SpawnException unwrap(Throwable error) {
        Throwable cause = error;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause instanceof SpawnException spawnException
                ? spawnException
                : new SpawnException.Refused(String.valueOf(cause));
    }
}
