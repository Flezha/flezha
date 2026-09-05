<?php

declare(strict_types=1);

namespace Flezha\Ffi;

use FFI;
use Flezha\Exception\FlezhaException;

/**
 * The `flz_core_ffi` C ABI, as this interpreter sees it.
 *
 * ## Why this is per-thread and not a process-wide singleton
 *
 * `FFI::cdef` builds its handle inside the interpreter that calls it, and a
 * `Swoole\Thread` is a separate interpreter with its own class table and its
 * own statics. The static below is therefore already per-thread on a ZTS
 * build — the accessor needs no thread id, and a handle created on one thread
 * is invisible to another by construction rather than by discipline.
 *
 * What does NOT follow the interpreter is the `FlzContext` the core
 * allocated: it lives in the process's address space, so a worker thread
 * receives its address as an integer bootstrap argument and re-casts it here.
 * Ownership is asymmetric, and stated in one place — {@see Core::attach()}
 * never frees what it did not create.
 */
final class Binding
{
    /**
     * The directory to load `libflz_core_ffi` from, in preference to the
     * bundled one. The same variable every SDK that bundles a binary reads.
     */
    public const NATIVE_LIB_DIR_ENV = 'FLEZHA_NATIVE_LIB_DIR';

    /** Platform directory names, matching what `scripts/build-dist.sh` emits. */
    private const SUPPORTED = [
        'linux-x64' => 'so',
        'linux-arm64' => 'so',
        'macos-x64' => 'dylib',
        'macos-arm64' => 'dylib',
        'windows-x64' => 'dll',
    ];

    /**
     * Every exported entry point of the ABI.
     *
     * Three spellings depart from `flz_core_ffi/src/lib.rs`, all because this
     * is read by PHP's own C parser rather than by a compiler:
     *
     * - `namespace` is a PHP keyword, so that parameter is `ns`;
     * - every byte buffer is `char *` where Rust says `uint8_t *` — the same
     *   parameter at the ABI, and the one PHP converts a string to and from
     *   without a per-call copy;
     * - the context is `uintptr_t`, not `FlzContext *`. A pointer and a
     *   `uintptr_t` are the same argument on every supported platform, and
     *   carrying it as an integer is what makes {@see Core::attach()} a plain
     *   assignment: PHP's `FFI::cast` of an integer to a pointer type borrows
     *   the CData it was given, so a cast built over a local outlives nothing
     *   and the first dereference in a worker thread reads freed memory.
     *   Measured — it panicked inside the core's `DashMap` with a shift
     *   overflow, which is what a garbage `self.shift` looks like.
     */
    private const CDEF = <<<'C'
        uint32_t flz_core_schema_version(void);
        uintptr_t flz_core_context_new(uint32_t shim_schema_version);
        const char *flz_core_init_error(void);
        const char *flz_core_last_error(uintptr_t ctx);
        int32_t flz_core_declare_class(uintptr_t ctx, const char *json, size_t json_len);
        int32_t flz_core_check_class(uintptr_t ctx, const char *json, size_t json_len, char **buf_out, size_t *len_out);
        int32_t flz_core_start(uintptr_t ctx, const char *ns);
        int32_t flz_core_poll(uintptr_t ctx, uint64_t timeout_ms, char **buf_out, size_t *len_out);
        int32_t flz_core_wait_starvation(uintptr_t ctx, uint64_t timeout_ms);
        int32_t flz_core_complete(uintptr_t ctx, const char *buf, size_t len);
        int32_t flz_core_abandon(uintptr_t ctx, uint64_t token);
        void flz_core_shutdown(uintptr_t ctx);
        int32_t flz_core_workflow_emit(uintptr_t ctx, const char *instance_id, const char *buf, size_t len);
        int32_t flz_core_workflow_recv(uintptr_t ctx, const char *instance_id, uint64_t timeout_ms, char **buf_out, size_t *len_out);
        int32_t flz_core_workflow_cancelled(uintptr_t ctx, const char *instance_id);
        int32_t flz_core_spawn_workflow(uintptr_t ctx, const char *instance_id, const char *factory_id, const char *payload, size_t payload_len, const char *if_running, char **outcome_out, size_t *outcome_len_out);
        uint64_t flz_core_monotonic_nanos(void);
        uint64_t flz_core_entry_last_delivery_nanos(const char *channel);
        uint64_t flz_core_idle_watch_new(const char *channel, uint64_t threshold_nanos);
        int32_t flz_core_idle_watch_poll(uint64_t id, uint64_t *gap_nanos);
        void flz_core_idle_watch_free(uint64_t id);
        void flz_core_buf_free(char *buf, size_t len);
        void flz_core_context_free(uintptr_t ctx);
        void flz_core_set_log_callback(void (*callback)(int32_t level, const char *message));
        C;

    /** Poll produced an activation; the out buffer holds its encoded bytes. */
    public const POLL_ACTIVATION = 1;
    /** The starvation wait observed starvation: the host owes a thread. */
    public const STARVATION_OBSERVED = 1;
    /** Poll timed out with no work. */
    public const POLL_TIMEOUT = 0;
    /** The context is shutting down; every blocked poller is released. */
    public const POLL_SHUTTING_DOWN = 2;
    /** Generic failure; read `flz_core_last_error`. */
    public const ERR = -1;
    /** Success for non-poll calls. */
    public const OK = 0;
    /** `idleWatchPoll` crossed no episode boundary. */
    public const IDLE_NONE = 0;
    /** The watched channel's silence first exceeded the threshold: one episode. */
    public const IDLE_IDLE = 1;
    /** A frame ended the episode and re-armed the latch. */
    public const IDLE_RESUMED = 2;

    private static ?FFI $handle = null;

    private function __construct()
    {
    }

    /**
     * This interpreter's handle, created on first use.
     *
     * Lazy rather than eager because a thread that never reaches the loop must
     * not pay for a `cdef`, and because the actionable failures below belong
     * at the first call rather than at autoload.
     */
    public static function current(): FFI
    {
        if (self::$handle === null) {
            self::assertAvailable();
            self::$handle = FFI::cdef(self::CDEF, self::libraryPath());
        }

        return self::$handle;
    }

    /**
     * Refuse a build that cannot call the ABI, naming the extension and the
     * ini setting.
     *
     * `ffi.enable=preload` — the shipped default — permits FFI from the CLI
     * SAPI and forbids it everywhere else, which is exactly the posture a
     * runner wants; `false` forbids it outright and is the case worth naming.
     */
    public static function assertAvailable(): void
    {
        if (!class_exists(FFI::class)) {
            throw new FlezhaException(
                'the flezha SDK calls flz_core through ext-ffi, and this PHP build has no FFI '
                . 'class. Install the extension (it ships with PHP 8.0+; on Debian and Ubuntu it '
                . 'is in php-common, and the official Docker images need '
                . '`docker-php-ext-install ffi`).',
            );
        }

        $enable = (string) ini_get('ffi.enable');
        if ($enable === '' || $enable === '0' || strtolower($enable) === 'false') {
            throw new FlezhaException(
                'ext-ffi is present but ffi.enable is ' . var_export($enable, true)
                . ', which forbids the call. Set ffi.enable=preload (which permits FFI from the '
                . 'CLI SAPI, where a runner runs) or ffi.enable=true in your php.ini.',
            );
        }
    }

    /**
     * The shared library to load.
     *
     * The override wins over the bundle so an operator can point a runner at a
     * locally built core without repackaging — which is what this repository's
     * own build script and test app do.
     */
    public static function libraryPath(): string
    {
        $platform = self::platform();
        $extension = self::SUPPORTED[$platform] ?? null;
        if ($extension === null) {
            throw new FlezhaException(sprintf(
                'no bundled libflz_core_ffi for the detected platform %s (the package carries: '
                . '%s). Build the library for this platform and name its directory in %s.',
                $platform,
                implode(', ', array_keys(self::SUPPORTED)),
                self::NATIVE_LIB_DIR_ENV,
            ));
        }

        $file = ($platform === 'windows-x64' ? '' : 'lib') . 'flz_core_ffi.' . $extension;
        $candidates = [];

        $override = getenv(self::NATIVE_LIB_DIR_ENV);
        if (is_string($override) && $override !== '') {
            $candidates[] = rtrim($override, '/') . '/' . $file;
        }
        $candidates[] = dirname(__DIR__, 2) . '/lib/' . $platform . '/' . $file;

        foreach ($candidates as $candidate) {
            if (is_file($candidate)) {
                return $candidate;
            }
        }

        throw new FlezhaException(sprintf(
            'libflz_core_ffi was not found for %s; looked at %s. Set %s to a directory holding '
            . 'it, or reinstall the package with its bundled binaries.',
            $platform,
            implode(', ', $candidates),
            self::NATIVE_LIB_DIR_ENV,
        ));
    }

    /**
     * The platform directory this build reads, in the naming every SDK's
     * distribution already uses (`linux-x64`, `macos-arm64`, ...).
     */
    public static function platform(): string
    {
        $os = match (PHP_OS_FAMILY) {
            'Linux' => 'linux',
            'Darwin' => 'macos',
            'Windows' => 'windows',
            default => strtolower(PHP_OS_FAMILY),
        };

        $arch = match (strtolower(php_uname('m'))) {
            'x86_64', 'amd64' => 'x64',
            'aarch64', 'arm64' => 'arm64',
            default => strtolower(php_uname('m')),
        };

        return $os . '-' . $arch;
    }
}
