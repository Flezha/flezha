.PHONY: build build-release nif nif-release test clean \
        docker-build docker-build-multiarch \
        docker-build-dev docker-build-toolchain \
        k8s-apply k8s-delete

# ── Local build ──────────────────────────────────────────────────────────────

# Rust + Elixir (debug)
build:
	cargo build --workspace
	mix deps.get
	mix compile

# Rust + Elixir (prod release)
build-release:
	cargo build --release -p flz_nif -p cli
	MIX_ENV=prod mix deps.get --only prod
	MIX_ENV=prod mix release --overwrite

# Just the NIF shared library (debug, fast iteration)
nif:
	cargo build -p flz_nif

# Just the NIF shared library (release)
nif-release:
	cargo build --release -p flz_nif

# Run Elixir test suite
test:
	mix test

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

# Build and push multi-arch image (amd64 + arm64) via buildx.
# Usage: VERSION=0.2.0 make docker-build-multiarch
docker-build-multiarch:
	docker buildx build \
	  --platform linux/amd64,linux/arm64 \
	  -t alfetahe/flezha:$(VERSION) \
	  -f docker/prod/Dockerfile.controller \
	  --push \
	  .

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
