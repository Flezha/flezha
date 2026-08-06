package com.flezha.sdk.procedure;

/**
 * Why a {@link ProcedureCtx#spawnBounded} request reached no instance. None of
 * these fault the requesting procedure — they resolve its
 * {@link SpawnHandle}.
 */
public class SpawnException extends Exception {

    public SpawnException(String message) {
        super(message);
    }

    /** The id is taken and the request carried {@link IfRunning#ERROR}. */
    public static final class AlreadyRunning extends SpawnException {
        public AlreadyRunning(String instanceId) {
            super("an instance of `" + instanceId + "` is already running and ifRunning was ERROR");
        }
    }

    /** No connected runner registers the requested factory. */
    public static final class FactoryUnknown extends SpawnException {
        public FactoryUnknown(String factoryId) {
            super("no connected runner registers factory `" + factoryId + "`");
        }
    }

    /** The controller declined — quota, shutdown, or a failed start. */
    public static final class Refused extends SpawnException {
        public Refused(String reason) {
            super("the controller refused the spawn: " + reason);
        }
    }

    /** No answer arrived within the result deadline. */
    public static final class Timeout extends SpawnException {
        public Timeout() {
            super("no spawn result within " + SpawnHandle.RESULT_TIMEOUT_MS + "ms");
        }
    }

    /** No control plane to request a spawn on. */
    public static final class Unavailable extends SpawnException {
        public Unavailable() {
            super("no control plane to request a spawn on");
        }
    }
}
