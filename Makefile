.PHONY: build build-release nif nif-release test fmt clean \
        refresh-process-hub \
        docker-build docker-publish \
        docker-build-dev docker-build-toolchain \
        k8s-apply k8s-delete \
        stabilize-test-check

# ── Local build ──────────────────────────────────────────────────────────────

# Rust + Elixir (debug)
build:
	cargo build --workspace
	mix deps.get
	mix compile

# Rust + Elixir (prod release)
build-release:
	cargo build --release -p flz_nif -p flz
	MIX_ENV=prod mix deps.get --only prod
	MIX_ENV=prod mix release --overwrite

# Re-resolve process_hub within the version requirement mix.exs declares: drop
# its lock entry so `mix deps.get` picks the newest matching release. Plain
# `mix deps.get` honours the lock, so this is the only way to take a newer
# release locally. Commit the resulting mix.lock — CI and every other build
# path use it as-is.
refresh-process-hub:
	mix deps.unlock process_hub
	mix deps.get

# Just the NIF shared library (debug, fast iteration)
nif:
	cargo build -p flz_nif

# Just the NIF shared library (release)
nif-release:
	cargo build --release -p flz_nif

# Run Elixir test suite
test:
	mix test

# Format every language; `python3 scripts/naming_gate.py --run @format` is the gate
fmt:
	scripts/format-apply.sh

# Archive criterion for the `stabilize-test-suite` OpenSpec change. Runs
# the four invocations the proposal calls out — `mix test`, then a re-run
# (verifies test ordering does not flip pass/fail), the cargo workspace
# with bench-internals, and `mvn -f sdk/java verify` — and exits 0 only
# if every step does. Local Java users without the toolchain can skip
# the `mvn` step with `SKIP_JAVA=1 make stabilize-test-check`.
stabilize-test-check:
	mix test
	mix test
	cargo test --workspace --features flezha_runner/bench-internals
	@if [ "$(SKIP_JAVA)" = "1" ]; then \
	  echo "skipping mvn -f sdk/java verify (SKIP_JAVA=1)"; \
	else \
	  mvn -f sdk/java verify; \
	fi

# Remove all build artifacts
clean:
	cargo clean
	mix clean
	rm -rf _build deps priv/native/*.so

# ── Docker ───────────────────────────────────────────────────────────────────

# Build prod Docker image for the current host architecture.
# Usage: VERSION=0.2.0 make docker-build
docker-build:
	docker build \
	  -t alfetahe/flezha:$(VERSION) \
	  -f docker/prod/Dockerfile.controller \
	  .

# Publish a controller image to Docker Hub. Builds NATIVELY on the host arch
# (no QEMU/buildx), pushes the per-arch tag, and rebuilds the manifest list.
# `make docker-publish` → prod (:latest); `make docker-publish ARGS=--dev` → dev.
# Run once per architecture host. See docker/prod/README.md.
docker-publish:
	scripts/publish-image.sh $(ARGS)

# Build the runtime-dev image (`:latest-dev`) for the current host arch.
# Layers Node.js + a pre-built testnet WS server on top of `:latest`, so
# `:latest` must already exist in the local image cache (run
# `make docker-build VERSION=latest` first).
docker-build-dev:
	docker build \
	  -t alfetahe/flezha:latest-dev \
	  -f docker/dev/Dockerfile.runtime \
	  .

# Build the toolchain image (`:latest-toolchain`) for the current host arch.
# Used by `docker/dev/docker-compose.yml` to give a developer a ready-to-go
# shell against bind-mounted source while hacking on Flezha itself.
docker-build-toolchain:
	docker build \
	  -t alfetahe/flezha:latest-toolchain \
	  -f docker/dev/Dockerfile.toolchain \
	  .

# ── Kubernetes ───────────────────────────────────────────────────────────────

# Apply all K8s manifests (order matters for dependencies)
k8s-apply:
	kubectl apply -f kubernetes/flezha-namespace.yaml
	kubectl apply -f kubernetes/flezha-serviceaccount.yaml
	kubectl apply -f kubernetes/flezha-role.yaml
	kubectl apply -f kubernetes/flezha-rolebinding.yaml
	kubectl apply -f kubernetes/flezha-configmap.yaml
	kubectl apply -f kubernetes/flezha-daemonset.yaml

# Delete K8s resources (namespace last to avoid dependency errors)
k8s-delete:
	kubectl delete -f kubernetes/flezha-daemonset.yaml --ignore-not-found
	kubectl delete -f kubernetes/flezha-configmap.yaml --ignore-not-found
