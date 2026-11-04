# The runner's image: the binary `go build` builds, on a Debian 13 runtime.
FROM debian:trixie-slim
COPY agentdesk /agentdesk/agentdesk
WORKDIR /agentdesk
# The runner stops when its standard input closes, and a container's is closed: it reads a pipe
# that stays open instead.
RUN mkfifo -m 600 stdin && chown 1000 stdin
USER 1000
ENTRYPOINT ["sh", "-c", "exec /agentdesk/agentdesk <> stdin"]
