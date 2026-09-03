# The PHP SDK's toolchain image, in two targets.
#
# The SDK requires only `php >= 8.4` and `ext-ffi`, so the default target is a
# stock non-thread-safe PHP with FFI turned on — which is what a distribution
# ships and what `SyncExecutor` needs. The `zts` target adds the thread-safe
# build and Swoole, which `SwooleExecutor` needs and which no distribution
# packages; it exists so the threaded arm is a container away rather than a
# local source build.
#
#   docker build -f docker/dev/Dockerfile.php --target nts -t flezha-php .
#   docker build -f docker/dev/Dockerfile.php --target zts -t flezha-php-zts .
#
# Neither target carries a Rust toolchain: the SDK loads `libflz_core_ffi`
# through `ext-ffi`, so the library is built once on the host (or by
# `scripts/build-php-sdk.sh`) and mounted in.

ARG PHP_VERSION=8.4

# --- non-thread-safe: the default posture ----------------------------------
FROM php:${PHP_VERSION}-cli AS nts

# `ext-ffi` is bundled with PHP's source but not built by the official images,
# and it links against libffi. `ffi.enable` is left at its default
# (`preload`), which permits FFI from the CLI SAPI — the SDK's own check
# reports it when a build forbids it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libffi-dev git unzip \
    && rm -rf /var/lib/apt/lists/* \
    && docker-php-ext-install -j"$(nproc)" ffi pcntl

COPY --from=composer:2 /usr/bin/composer /usr/local/bin/composer

# The coverage driver. `phpunit/php-code-coverage` 10 dropped phpdbg, so a
# coverage run needs Xdebug or PCOV; PCOV is the coverage-only one and costs a
# fraction of Xdebug's runtime, which is all this image is asked for.
RUN pecl install pcov \
    && docker-php-ext-enable pcov

WORKDIR /work

# --- thread-safe plus Swoole: the threaded executor ------------------------
FROM php:${PHP_VERSION}-zts AS zts

RUN apt-get update \
    && apt-get install -y --no-install-recommends libffi-dev git unzip \
    && rm -rf /var/lib/apt/lists/* \
    && docker-php-ext-install -j"$(nproc)" ffi pcntl

# `Swoole\Thread` exists only in a Swoole built against a ZTS PHP with
# `--enable-swoole-thread`; the pecl default build has no thread API at all,
# which is the failure `Executors::swoole()` reports as an instruction.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl-dev \
    && rm -rf /var/lib/apt/lists/* \
    && pecl download swoole \
    && tar -xf swoole-*.tgz && rm swoole-*.tgz \
    && cd swoole-* \
    && phpize \
    && ./configure --enable-swoole-thread \
    && make -j"$(nproc)" && make install \
    && cd .. && rm -rf swoole-* \
    && docker-php-ext-enable swoole

# `ffi.enable=preload` — PHP's default — permits FFI from the CLI SAPI, and a
# `Swoole\Thread` is a fresh interpreter that is not it: every worker would be
# refused its first core call. This is the image the threaded arm runs on, so
# it carries the setting that arm needs.
RUN echo 'ffi.enable=1' > "$PHP_INI_DIR/conf.d/flezha-ffi.ini"

COPY --from=composer:2 /usr/bin/composer /usr/local/bin/composer

# The coverage driver. `phpunit/php-code-coverage` 10 dropped phpdbg, so a
# coverage run needs Xdebug or PCOV; PCOV is the coverage-only one and costs a
# fraction of Xdebug's runtime, which is all this image is asked for.
RUN pecl install pcov \
    && docker-php-ext-enable pcov

WORKDIR /work
