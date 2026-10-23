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
