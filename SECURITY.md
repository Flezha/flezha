# Security policy

## Supported versions

Flezha is pre-1.0. Fixes land on `main` only; there are no maintained release
branches yet.

## Reporting a vulnerability

Please do not open a public issue for a security problem. Report it privately
through one of:

- GitHub private vulnerability reporting on this repository
  (Security → Report a vulnerability), or
- email to security@flezha.com.

Include the affected component (controller, an SDK, the CLI, a container
image), a reproduction or proof of concept, and the impact as you understand
it. You will get an acknowledgement within 5 days and a status update at least
every 14 days while the report is open.

## Disclosure

Coordinated disclosure with a 90-day window from acknowledgement, shorter if a
fix ships earlier, longer only by mutual agreement. Reporters are credited in
the fix's release notes unless they ask not to be. There is no bug bounty.

## Scope

The controller, the Rust/Java/Elixir SDKs, `flz_core` and its C ABI, the
transport layer, the `flz` CLI, and the published container images. Findings in
third-party dependencies should go to the upstream project; tell us as well if
Flezha's use of the dependency is what makes it exploitable.
