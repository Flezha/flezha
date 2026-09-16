<?php

declare(strict_types=1);

namespace Flezha\Ffi;

use Composer\InstalledVersions;
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
     * package's own. The same variable every SDK that bundles a binary reads.
     */
    public const NATIVE_LIB_DIR_ENV = 'FLEZHA_NATIVE_LIB_DIR';

    /** The Composer package this file ships in. */
    public const PACKAGE = 'flezha/sdk';

    /**
     * The root release assets are fetched from, read by
     * `scripts/build-native.sh --fetch-missing` too.
     */
    public const RELEASE_BASE_URL_ENV = 'FLEZHA_RELEASE_BASE_URL';

    private const RELEASE_BASE_URL = 'https://github.com/flezha/flezha/releases/download';

    /** The SHA-256 manifest `scripts/build-dist.sh --stage-release` publishes. */
    private const MANIFEST = 'SHA256SUMS.txt';

    /** The platforms a release publishes a library for, as `scripts/build-dist.sh` names them. */
    private const PUBLISHED = ['linux-x64', 'linux-arm64', 'macos-x64', 'macos-arm64', 'windows-x64'];

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
        int32_t flz_core_local_start(uintptr_t ctx, const char *factory_id, const char *operative_id);
        int32_t flz_core_local_on_sink(uintptr_t ctx, const char *channel);
        int32_t flz_core_local_feed(uintptr_t ctx, const char *channel, const char *payload, size_t len);
        int32_t flz_core_local_next_output(uintptr_t ctx, uint64_t timeout_ms, char **buf_out, size_t *len_out);
        int32_t flz_core_local_run_until_idle(uintptr_t ctx, uint64_t timeout_ms);
        int32_t flz_core_local_stop(uintptr_t ctx);
        int32_t flz_core_poll(uintptr_t ctx, uint64_t timeout_ms, char **buf_out, size_t *len_out);
        int32_t flz_core_wait_starvation(uintptr_t ctx, uint64_t timeout_ms);
        int32_t flz_core_complete(uintptr_t ctx, const char *buf, size_t len);
        int32_t flz_core_abandon(uintptr_t ctx, uint64_t token);
        void flz_core_shutdown(uintptr_t ctx);
        void flz_core_watch_stdin_eof(uint32_t grace_ms);
        int32_t flz_core_workflow_emit(uintptr_t ctx, const char *instance_id, const char *buf, size_t len);
        int32_t flz_core_workflow_recv(uintptr_t ctx, const char *instance_id, uint64_t timeout_ms, char **buf_out, size_t *len_out);
        int32_t flz_core_workflow_cancelled(uintptr_t ctx, const char *instance_id);
        int32_t flz_core_spawn_workflow(uintptr_t ctx, const char *instance_id, const char *factory_id, const char *payload, size_t payload_len, const char *if_running, char **outcome_out, size_t *outcome_len_out);
        int32_t flz_core_admin_call(uintptr_t ctx, const char *verb, const char *args, char **result_out, size_t *result_len_out);
        uint64_t flz_core_monotonic_nanos(void);
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
     * The override wins over the package's own directory so an operator can
     * point a runner at a locally built core without repackaging, which is
     * what this repository's own build script and test app do.
     */
    public static function libraryPath(): string
    {
        $platform = self::platform();
        $found = self::installed($platform);
        if ($found !== null) {
            return $found;
        }

        [$asset, $manifest] = self::release(
            $platform,
            class_exists(InstalledVersions::class) && InstalledVersions::isInstalled(self::PACKAGE)
                ? InstalledVersions::getPrettyVersion(self::PACKAGE)
                : null,
        );

        throw self::refusal($platform, sprintf(
            'The package\'s Composer plugin fetches it at install: allow it with `composer config '
            . 'allow-plugins.%1$s true`, then run `composer reinstall %1$s`. %2$s',
            self::PACKAGE,
            self::byHand($platform, $asset, $manifest),
        ));
    }

    /**
     * Make the host's library present in the package, fetching it from the
     * release matching `$version` when nothing is installed.
     *
     * The download is verified against the release's SHA-256 manifest before
     * a byte reaches the package directory, so a refused or failed fetch
     * leaves nothing behind for {@see libraryPath()} to load.
     *
     * @param \Closure(string): string $get the body at a URL; throws when it cannot be read
     *
     * @return string|null the library it installed, or null when one was already present
     */
    public static function obtain(string $platform, string $version, \Closure $get): ?string
    {
        if (self::installed($platform) !== null) {
            return null;
        }

        [$url, $manifestUrl] = self::release($platform, $version);
        try {
            $manifest = $get($manifestUrl);
            $body = $get($url);
        } catch (\Throwable $e) {
            throw new FlezhaException(sprintf(
                'could not fetch libflz_core_ffi for %s: %s. Nothing was installed. Retry, or %s',
                $platform,
                $e->getMessage(),
                lcfirst(self::byHand($platform, $url, $manifestUrl)),
            ), 0, $e);
        }

        $expected = preg_match('/^([0-9a-f]{64}) [ *]' . preg_quote(basename($url), '/') . '$/m', $manifest, $m) === 1
            ? $m[1]
            : null;
        $actual = hash('sha256', $body);
        if ($expected !== $actual) {
            throw new FlezhaException(sprintf(
                'refused %s: its SHA-256 is %s and %s %s. Nothing was installed.',
                $url,
                $actual,
                $manifestUrl,
                $expected === null ? 'lists no checksum for it' : 'publishes ' . $expected,
            ));
        }

        $target = self::packaged($platform);
        $directory = dirname($target);
        if (!is_dir($directory) && !mkdir($directory, 0o755, true) && !is_dir($directory)) {
            throw new FlezhaException(sprintf('could not create %s for libflz_core_ffi', $directory));
        }
        // Written beside the target and renamed over it, so an interrupted
        // write never leaves a truncated library where the loader looks.
        $partial = $target . '.part';
        if (file_put_contents($partial, $body) !== strlen($body) || !chmod($partial, 0o644) || !rename($partial, $target)) {
            @unlink($partial);
            throw new FlezhaException(sprintf('could not write libflz_core_ffi to %s', $target));
        }

        return $target;
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

    /** The first place the library already is, override first. */
    private static function installed(string $platform): ?string
    {
        foreach (self::candidates($platform) as $candidate) {
            if (is_file($candidate)) {
                return $candidate;
            }
        }

        return null;
    }

    /** @return list<string> */
    private static function candidates(string $platform): array
    {
        $candidates = [];
        $override = getenv(self::NATIVE_LIB_DIR_ENV);
        if (is_string($override) && $override !== '') {
            $candidates[] = rtrim($override, '/') . '/' . basename(self::packaged($platform));
        }
        $candidates[] = self::packaged($platform);

        return $candidates;
    }

    /** Where the package keeps the library, in the file naming `scripts/build-dist.sh` stages. */
    private static function packaged(string $platform): string
    {
        return dirname(__DIR__, 2) . '/lib/' . $platform . '/' . match (strstr($platform, '-', true)) {
            'windows' => 'flz_core_ffi.dll',
            'macos' => 'libflz_core_ffi.dylib',
            default => 'libflz_core_ffi.so',
        };
    }

    /**
     * The asset and manifest a release publishes the library under, or the
     * refusal naming why there is nothing to fetch.
     *
     * The asset name is `release_asset_name` in `scripts/native_triples.sh`.
     *
     * @return array{string, string}
     */
    private static function release(string $platform, ?string $version): array
    {
        if (!in_array($platform, self::PUBLISHED, true)) {
            throw self::refusal($platform, sprintf(
                'No release publishes it for %s, only for %s: build it for this platform and name its '
                . 'directory in %s.',
                $platform,
                implode(', ', self::PUBLISHED),
                self::NATIVE_LIB_DIR_ENV,
            ));
        }
        if ($version === null || preg_match('/^v?(\d+\.\d+\.\d+(?:-[0-9A-Za-z.]+)?)$/', $version, $m) !== 1) {
            throw self::refusal($platform, sprintf(
                '%s %s is not a published release, so there is nothing to fetch it from: build it from a '
                . 'flezha checkout and put it at %s, or name its directory in %s.',
                self::PACKAGE,
                $version ?? '(no installed version)',
                self::packaged($platform),
                self::NATIVE_LIB_DIR_ENV,
            ));
        }

        $root = rtrim((string) (getenv(self::RELEASE_BASE_URL_ENV) ?: self::RELEASE_BASE_URL), '/') . '/v' . $m[1];

        return [
            sprintf('%s/libflz_core_ffi-%s.%s', $root, $platform, pathinfo(self::packaged($platform), PATHINFO_EXTENSION)),
            $root . '/' . self::MANIFEST,
        ];
    }

    private static function refusal(string $platform, string $remedy): FlezhaException
    {
        return new FlezhaException(sprintf(
            'libflz_core_ffi for %s is not installed; looked at %s. %s',
            $platform,
            implode(', ', self::candidates($platform)),
            $remedy,
        ));
    }

    private static function byHand(string $platform, string $asset, string $manifest): string
    {
        return sprintf(
            'By hand: download %s, check its SHA-256 against %s, and put it at %s, or name the '
            . 'directory holding it in %s.',
            $asset,
            $manifest,
            self::packaged($platform),
            self::NATIVE_LIB_DIR_ENV,
        );
    }
}
