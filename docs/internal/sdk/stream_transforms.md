# Stream transforms: implementation notes

For SDK implementers and contributors. How to use it is on
[Streams](../../public/topics/core/streams.md).

## Where the code lives

| Piece | Rust | Java |
| --- | --- | --- |
| The check and its vocabulary | `crates/flz_core/src/host/{register,issue}.rs` — one implementation, every SDK | shared core |
| Builder surface, fusion (`attach_consumer`, `extend_pending`, `compose`) | `sdk/rust/src/channel/builder/` | `flezha-sdk-api/.../channel/{EdgeStream,EntryChannel,Stream}.java`, `flezha-sdk-runtime/.../relay/builder/RelayTopologyBuilder.java` |
| `ConsumerEdge`, `EdgeTransformFn`, `EdgeYield`, `transform_value`, `key_of` | `crates/flz_core/src/channel.rs` | `flezha-sdk-runtime/.../relay/{ConsumerEdge,EdgeYield}.java` |
| Routing seam and ladder (`route_to_consumers`, `deliver_edge`, `run_retry_park_ladder`) | `crates/flz_core/src/routing.rs` | `flezha-sdk-runtime/.../core/{EmitContext,CellRouting,EdgeTransforms}.java` |
| Steps relay containment (`extend_mapped`) and refusals | `crates/flz_core/src/steps/{driver,spec}.rs` | shared core |
| Elixir SDK: the builder records the links, the host runs the closures | `sdk/elixir/lib/flezha/sdk/{channels,declaration,dispatch}.ex` | — |

Tests: `sdk/rust/tests/stream_transforms.rs` (asserts fusion by counting
traversals), `sdk/rust/tests/ui/tasklet_cell_key_removed.rs`, Java
`RelayTopologyBuilderTest` / `CellRoutingTest` /
`StepsDispatchTest`, and
`test/system/dynamic_cell_e2e_test.exs` on both runtimes.
