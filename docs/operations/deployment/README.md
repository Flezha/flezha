# Deployment

This guide is for whoever runs a Flezha Controller: on a machine directly, in
Docker locally, or on Kubernetes. It names only the variables a deploy cannot
omit; every other knob, with its default and the file that reads it, is in
[`../../configuration.md`](../../configuration.md). All commands run from the
project root.

## Which page

| Page | Read it when |
|---|---|
| [`variables.md`](variables.md) | Any deploy. The variables a controller cannot start, cluster or be operated without. |
| [`building.md`](building.md) | Producing the artifacts: the Makefile targets, and what a release contains. |
| [`bare_metal.md`](bare_metal.md) | Running the controller and a runner as two ordinary processes. Also the socket ladder, several controllers on one machine, and what to do when a runner cannot connect. |
| [`images.md`](images.md) | Building, publishing and understanding the production container image. |
| [`local_docker.md`](local_docker.md) | Developing in the toolchain container, or smoke-testing the built image with docker compose. |
| [`kubernetes.md`](kubernetes.md) | Deploying the DaemonSet: manifests, ConfigMap, volumes, probes. |

## See also

- [`../../configuration.md`](../../configuration.md) - every knob, its default and the file that reads it
- [`../authn_authz.md`](../authn_authz.md) - cookie, TLS distribution, QUIC certificates, bootstrap claim
- [`../graceful_shutdown.md`](../graceful_shutdown.md) - what `flz controller stop` does inside the grace period
- [`../auto_restore_on_boot.md`](../auto_restore_on_boot.md) - what a restarted controller brings back from the recovery volume
- [`../remote_durability_tier.md`](../remote_durability_tier.md) - choosing and configuring a durability backend
- [`../reset.md`](../reset.md) - wiping a node's runtime data
