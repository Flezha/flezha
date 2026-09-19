# Durability

Part 2.

What survives a crash, what does not, and what each level costs.

TODO: not written yet.

## What survives

The catalog, the tokens and the keys are on disk under `app_data/auth/`. A
controller restart brings back both namespaces, their quotas and their hubs
before any runner reconnects, and the runner's key still works. Each
namespace's state lives under `app_data/state/<namespace>/`, apart from every
other's, which is what lets this page reason about the desk alone.
