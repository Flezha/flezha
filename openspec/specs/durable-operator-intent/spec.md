# durable-operator-intent Specification

## Purpose
Declared intent lives in the hub's declared set, distinct from runtime
registry rows: convergence re-derives runtime state from it, a deliberate
stop is remembered as set absence forever, and an off-cluster copy via the
durability tier survives total disk loss.
## Requirements
### Requirement: Declared intent lives in the hub declared set, not runtime rows

Every durable operative start SHALL be declared through the hub's declared-children set (`durable: true` at the single start authority), operator-issued, scheduled, and procedure-requested alike, and every deliberate stop or workflow completion SHALL remove the entry through the hub stop path. Flezha SHALL NOT maintain any intent store of its own, and no flezha code path outside the start/stop authority SHALL mutate declared intent.

Plugin mutations are declared intent by the same reasoning and SHALL be written to the declared set, through that same start authority, so a restart rebuilds an operative with the plugins it was last known to have. The declared set is written at start and at stop and has no update path of its own; that update is the start authority's to make, and no other flezha code path SHALL make it. A plugin mutation against an operative with no declared entry SHALL be refused rather than declare one, so it can never resurrect an operative a stop removed for good.

#### Scenario: Runtime loss does not lose intent

- **WHEN** the runtime registry rows are lost (peerless boot, wipe, or
  corruption) while the declared set survives locally or in the remote
  manifest
- **THEN** convergence reports the declared set as intended rather than
  reporting zero intended, and restores it

#### Scenario: A stop is never forgotten

- **WHEN** an operator stops an operative and any node returns later,
  including after longer than the stopped-row TTL
- **THEN** the operative's entry is absent from every adopted version of the
  declared set and nothing resurrects it

#### Scenario: Spawned workflows share the model

- **WHEN** a procedure-requested workflow instance is spawned, and later
  completes
- **THEN** its entry exists in the declared set for exactly that lifetime, so
  a whole-cluster cold start mid-lifetime restores it and a cold start after
  completion does not

#### Scenario: A plugin mutation reaches the declared set through the start authority

- **WHEN** `flz plugin enable` succeeds on a running operative
- **THEN** the operative's declared entry carries the plugin, the entry is replaced in place with nothing started or stopped, and the write was issued by the start authority rather than by the plugin manager

#### Scenario: A plugin mutation never declares an undeclared operative

- **WHEN** a plugin mutation names an operative that has no declared entry
- **THEN** it is refused naming that reason, and no entry is created

### Requirement: Convergence re-derives runtime state from the declared set

`Convergence.Intent.declared/0` SHALL read the hub declared set, not
runtime registry rows. A divergence between intent and reality SHALL heal
through the existing Diff → Planner → Heal pipeline, and every
restore-driven start SHALL flow through the registry's single idempotent
start authority so that re-deriving runtime state cannot double-bind.

#### Scenario: Wiped runtime heals back to the declared set

- **WHEN** runtime registry rows are wiped while the declared set declares N
  operatives
- **THEN** convergence restarts the N declared operatives through the normal
  start authority, and concurrent restore attempts for one operative yield
  exactly one live instance

### Requirement: The durability tier is the remote manifest adapter

Flezha SHALL implement `ProcessHub.Storage.RemoteManifest` over
`Flezha.State.DurabilityTier`, shipping the declared set off-cluster as one
more tier artifact class. No other remote-storage mechanism SHALL be added
for intent, and adapter failures SHALL never fail or delay a start/stop
command. The tier SHALL offer at least the same storage options as the
upstream adapters — file storage (`:local_path`, for paths that outlive the
cluster such as an NFS mount) and S3 (`:s3`) — alongside `:postgres`, with an
explicit `{module, opts}` backend as the extension point for any other store
(e.g. a Kubernetes-backed one).

#### Scenario: Total disk loss restores from the tier

- **GIVEN** a stop shipped to the durability tier at declared-set version V,
  after which every cluster disk holding version V is lost and one node
  returns with a stale local set
- **WHEN** that node boots and the hub fetches the remote manifest
- **THEN** version V is adopted: the stopped operative stays stopped and the
  operatives version V declares are restored

#### Scenario: Tier outage does not block commands

- **WHEN** the durability tier is unreachable while operatives are started
  and stopped
- **THEN** every command succeeds locally, the failed ships are retried, and
  the tier converges to the latest set version once reachable

