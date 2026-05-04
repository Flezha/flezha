# Java Compute Engine — RELAY_MVP_JAVA_PENDING

This Java test app currently does **not** connect to the agent under
the `relay-mvp-rust` cutover.

The Rust SDK's wire-format change (`plugin_group: u8` → `channel: String`)
landed in `relay-mvp-rust`, and the agent ships a single wire — there is
no dual-stack period. The Java SDK still uses the legacy
`ProcessorHandler` API and therefore fails to handshake against the new
agent.

`relay-mvp-java` (next-steps row 2 in the relay-architecture umbrella)
ports the Java SDK to the new `Relay` / `Shard` model and re-enables
the Java↔agent integration matrix in CI.

Until then:

- `mvn test` against `sdk/java/` continues to pass — Java SDK unit tests
  do not cross the wire.
- The Java SDK build still happens (committed natives + jar refresh
  flow is unchanged).
- The `mix-integration-matrix` job in
  `.github/workflows/java-sdk.yml` is gated `if: false` with a
  `RELAY_MVP_JAVA_PENDING` comment.
- Every ExUnit test tagged `:java_sdk` (excluded by default in
  `test/test_helper.exs`) remains parked for the duration.

To trace what's left when `relay-mvp-java` lands, grep for
`RELAY_MVP_JAVA_PENDING` across the repo.
