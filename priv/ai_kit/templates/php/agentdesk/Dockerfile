# The runner's image: the project and the packages `composer install` put in vendor/, on PHP 8.4
# with FFI, which the SDK loads its native library through.
FROM php:8.4-cli
RUN apt-get update && apt-get install -y --no-install-recommends libffi-dev \
    && rm -rf /var/lib/apt/lists/* && docker-php-ext-install ffi
COPY . /agentdesk
WORKDIR /agentdesk
# The runner stops when its standard input closes, and a container's is closed: it reads a pipe
# that stays open instead.
RUN mkfifo -m 600 stdin && chown 1000 stdin
USER 1000
ENTRYPOINT ["sh", "-c", "exec php runner.php <> stdin"]
