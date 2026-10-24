# Changelog

## Unreleased

Nothing has been published yet. The first entry is written when 0.1.0 is
released; until then, this section names what changes for a user of the
v0.1.0-alpha.1 pre-release.

### Changed

- `flz template list` and `flz template create` are removed. Each release now
  publishes every SDK language's starter project as
  `flezha-<language>-starter.tar.gz`, downloaded and unpacked with one
  `curl ... | tar xz` and nothing else installed
  ([Starter projects](docs/public/topics/core/starter_projects.md)). The MCP
  tool `flezha_new_project` answers the same files as before; its refusal of an
  unknown template now lists the valid ones.
- `Relay.overwatch_state` no longer sends a relay's own data: its init values,
  snapshot, held outputs and seed answer `nil`. A relay started with large init
  values, such as a long price history, made each read copy all of it, enough
  to exhaust a console that read it every few seconds.
- `Controller.liveness_counters` answers the counters of every connected
  controller, not only the one asked. A console pinned to one controller saw
  no counters for a relay running on another, so its relay graph never showed
  data moving.
- `flz` sends each verb to the controller as one request over the controller's
  control socket, the Unix socket its runners attach to, instead of starting
  an Erlang VM for every command. Verbs start faster, need no Erlang cookie, and
  a controller whose runners use QUIC still opens the socket for `flz`. A shell
  reaches the controller whose `FLEZHA_SOCKET_FILES_PATH` it shares
  ([CLI](docs/public/topics/cli/cli.md#how-it-reaches-the-controller)). Use the
  `flz` of the controller's own release: an older controller is reported as
  `cli.protocol_unsupported` after five seconds. `flz controller start`, the
  `flz plugin install | uninstall | sync` verbs and `flz mcp`'s knowledge tools
  with no controller still start a VM of their own.

### Fixed

- The convergence loop no longer lets heals that keep waiting fill every tick.
  Twenty or more relays it could not place, such as relays whose factory no
  runner offers, took the whole per-tick budget, so a plugin whose controller
  died was never restarted elsewhere. A heal that found its precondition unmet
  now waits behind the others on the next tick.
- A `ctx.effect` result kept while the relay's input log stops is answered to the
  runner instead of failing with `internal.uncatalogued`.
- A plugin copy a disable stopped is no longer still listed on its controller
  for a moment after `flz plugin disable` answers.
- A plugin disabled through one controller no longer comes back on a relay
  hosted by another. The disable now waits until every controller of the
  namespace has dropped the plugin's record before it tells the relays, so a
  convergence tick on a slower controller cannot bring the binding up again.
