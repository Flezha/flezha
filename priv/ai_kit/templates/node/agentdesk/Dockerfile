# The runner's image: the project and the packages `npm install` put in node_modules/, on Node 26.
FROM node:26.13.0-bookworm-slim
COPY . /agentdesk
WORKDIR /agentdesk
# The runner stops when its standard input closes, and a container's is closed: it reads a pipe
# that stays open instead.
RUN mkfifo -m 600 stdin && chown 1000 stdin
USER 1000
ENTRYPOINT ["sh", "-c", "exec node runner.mjs <> stdin"]
