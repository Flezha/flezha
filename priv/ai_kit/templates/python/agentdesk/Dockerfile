# The runner's image: the project and the packages `pip install --target vendor` put in vendor/,
# on Python 3.12.
FROM python:3.12-slim
COPY . /agentdesk
WORKDIR /agentdesk
ENV PYTHONPATH=/agentdesk/vendor
# The runner stops when its standard input closes, and a container's is closed: it reads a pipe
# that stays open instead.
RUN mkfifo -m 600 stdin && chown 1000 stdin
USER 1000
ENTRYPOINT ["sh", "-c", "exec python runner.py <> stdin"]
