# The echo runner: the three files of sdk/python/examples/echo, and the SDK
# they need.
#
# Two stages, because the flezha wheel is BUILT here rather than installed.
# The package is not on PyPI yet. When it is published, delete the wheel stage
# and install the package directly instead:
#
#     RUN pip install --no-cache-dir flezha==0.1.0
#
# The wheel is abi3, built against the 3.11 floor and valid on every later
# interpreter, so the build stage's Python version does not have to match the
# runtime image's.
#
# Build context is the repository root, because the wheel is built from this
# checkout's own sources. See compose.yaml.

FROM rust:1-slim AS wheel

RUN apt-get update \
 && apt-get install --yes --no-install-recommends python3 python3-venv \
 && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/build \
 && /opt/build/bin/pip install --no-cache-dir "maturin>=1.9,<2.0"

WORKDIR /src
COPY . .

# `python-abi3` is what makes one wheel serve every supported interpreter.
RUN cd sdk/python \
 && /opt/build/bin/maturin build --release --out /wheels \
      --features python-extension,python-abi3


FROM python:3.12-slim

# uid 1000 to match the controller image. The socket directory the two share is
# 0700 and owned by whoever created it, so a different uid cannot reach it.
RUN useradd --create-home --uid 1000 runner

COPY --from=wheel /wheels/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -f /tmp/*.whl

COPY sdk/python/examples/echo/*.py /app/

USER runner
WORKDIR /app

ENV FLEZHA_SOCKET_FILES_PATH=/flezha/app_data/sockets

# -u so the runner's own lines reach `docker compose logs` as they happen.
ENTRYPOINT ["python", "-u", "/app/runner.py"]
