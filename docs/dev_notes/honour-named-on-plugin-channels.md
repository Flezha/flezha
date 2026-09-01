# `.named()` reached every terminator but the one that ships the frame

`honour-named-on-plugin-channels` — KI-120. One expression per veneer SDK. What
the proposal says is not repeated; this is what the next person needs.

## It was an omission, and the file says so twice before the bug

`.named(name)` stores a pending channel name in all three veneers
(`authoring.ts:265`, `builder.rb:156`, `Edge.php:36`), and **both** edge paths
already read it as `pending.channelName ?? <default>` — `authoring.ts:308,334`,
`builder.rb:182,204`, `Edge.php:102,137`. Only `sink()` never read it. So the fix
is the file's own established pattern applied to the third call site, not a new
design: RULE #1's first rung.

**The tell for this class of defect**: a field written by one method and read by
`n − 1` of the `n` methods that should read it. Grep for the field, count the
readers, and compare with the number of terminators.

## Why a channel name is not cosmetic here

The erased-host declaration carries exactly ONE name per channel
(`flz_core/src/host/decl.rs:19-34`, pinned by
`test/fixtures/host_class_decl_keys.txt:31-38`) — there is no second slot for a
plugin's own name. That one name rides the frame as `channel` metadata, and
`operative_forward` uses it to select the receiving operative's entry channel. So
`named('input').sink('relay_out')` shipping `channel = "relay_out"` made the
partitioned-forward scenario impossible: `[flezha] operative … has no channel
'relay_out'`.

In Rust the two names are separate concepts — `sink_with` ATTACHES a
`PluginSink { name }` to the existing channel (`stream.rs:378-388`), so the
channel keeps its `.named()` name and `relay_out` is only the plugin's. The
veneers have no such split, which is why the sink name can only ever be the
channel-name default.

## The blast radius was measurable, and one test had pinned the defect

`.named(x).sink(y)` appears throughout all three veneers' fixtures and unit
tests, so this renames channels widely. That was checked by running rather than
reasoning: the three unit suites and the whole `--include system` suite against
node. **One assertion broke, and it was asserting the bug** —
`DispatchTest.php` expected `plugin_channel == 'echo_sink'` on a fixture
declaring `named('out')->sink('echo_sink')`. It is now `'out'`, which is what the
Rust twin produces.

The declaration goldens (`test/fixtures/declarations/*.json`) carry only
class-level metadata and no channel names, so nothing there could have caught it
either — worth knowing before assuming a golden covers a topology change.

## The divergence this did NOT close — read before "fixing" the default

`sdk-graph-authoring:59-62` says a sink with **no** `.named(...)` takes the
**producer node's** identity. Rust does exactly that
(`channel/builder/state.rs:236`: `let name = self.procedures[node].name`). All
three veneers use the sink's name instead. That is a second, larger divergence:
closing it renames every unnamed plugin channel in three SDKs' fixtures at once.

The compatibility test in each SDK is deliberately named "keeps the name it has
today" and commented as compatibility rather than conformance, so the suite does
not record the divergence as settled. Anyone closing it should start from those
three tests.
