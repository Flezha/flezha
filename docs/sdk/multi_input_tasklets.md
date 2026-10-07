# Merged inputs: implementation notes

For SDK implementers and contributors. How to use it is on
[Merged inputs](../topics/core/streams.md#merged-inputs).

## Why replay is free

The mapping is applied at the **routing seam** (`route_to_consumers`
in-process; host-side for Java), the one path live delivery and the
controller's re-forwarded replay slice both travel. Frames are journaled per
source Channel, unchanged, so the replay slice orders and folds exactly as the
live run did and a warm restart restores the same state. No wire change, no
controller change: the controller stamps `metadata["channel"]` on a frame and
has no model of a Channel's consumers, so a merge is invisible to it.

## Where the code lives

| Piece | Rust | Java |
| --- | --- | --- |
| `ChannelBuilder::merge`, `Merge`, `MergeSource`, the `TopologyIssue` variants; `attach_consumer` is the one place an edge is recorded and a second declaration on a mapped edge is refused | `sdk/rust/src/channel/builder/` | `flezha-sdk-api/.../channel/Merge.java`; `flezha-sdk-runtime/.../relay/builder/RelayTopologyBuilder.java` (`MergeImpl`, `wireConsumer`) |
| `ConsumerEdge` (`tasklet` + composed `transform` + optional `key`); `consumer_ids()` is the id-only view the graph walk and the steps relay DAG use | `flz_core/src/channel.rs` | `flezha-sdk-runtime/.../relay/ConsumerEdge.java` |
| The one mapping site on the routing path | `flz_core/src/routing.rs` | `flezha-sdk-runtime/.../core/ActivationDispatcher.java` |
| `ChainCtx::edge` + `extend_mapped` at the accumulate and re-seed sites | `flz_core/src/steps/{driver,spec}.rs` | shared core |

Tests: `sdk/rust/tests/merged_input.rs`; the steps fixtures
`MergeSteps`, `PanickingMergeSteps` and `PanickingTransformSteps` in
`sdk/rust/src/steps/mod.rs`, each run forward and resumed from a journal
so both mapping sites are exercised; Java `MergedInputTest`.
