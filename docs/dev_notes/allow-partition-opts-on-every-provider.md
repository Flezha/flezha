# The partition fan-out reaches every provider, so the allow-list must too

`allow-partition-opts-on-every-provider` — KI-119. Four atoms into one shared
list. What the proposal says is not repeated; this is what the next person needs.

## The invariant was already written down, for the other half of the same list

`ProviderBehaviour`'s `@host_opts` `@doc`:

> *"They live in one list because every provider that calls `reject_unknown/3`
> must allow all of them — a key added to the host side and missed in one
> provider's private copy turns that provider's every enable into a refusal."*

That is this defect verbatim, for a different key set. `partition_index` /
`partition_count` / `key_group_from` / `key_group_to` are injected by
`Plugin.for_partition/3` into **every** plugin of **every** group member, exactly
like the host identity — and were nonetheless kept in two providers' private
copies (`websocket.ex`, `http_poll.ex`). The third strict provider that can be a
partition member's plugin, `operative_forward`, did not have the copy.

**So the rule to carry forward: a key the platform merges unconditionally belongs
in `@host_opts`, never in a provider's `@known_opts`.** `@known_opts` also feeds
the generated `build/1` — the CLI struct path — and these keys are never
author-written, which the precedent's own comment says out loud.

## Where NOT to put them, and the two reasons

- **`@known_opts`** widens `build/1` (`provider_behaviour.ex:588,601`), so an
  operator could hand-write `partition_index` into a `--plugins` spec. That is
  what the two existing providers accidentally allow.
- **`@injected_opts`** in `operative_forward` feeds `defstruct` (`:55`). The four
  would become struct fields nothing populates: that provider reads none of them,
  routing instead by the TARGET group's ranges (`Partition.owner_of/2`,
  `group_of/1`, `key_groups/0`), which it resolves itself from `PartitionGroup`.

`validate_opts/1`'s allow-list is `@known_opts ++ @internal_opts ++ host_opts()`
(`:615`) while `build/1`'s is `@known_opts` alone. That asymmetry is the whole
design, and `@host_opts` is the half that means "the platform put it here".

## Why the default suite could not see it

`eb938c8b` — which introduced the regression by adding the `validate_opts/1` call
to `init_provider/1`, deliberately, as KI-116's site 5 — ran `mix test` green at
1960. It had to: the only coverage of a partitioned plugin start is
`test/system/partitioned_operatives_e2e_test.exs`, which is `@moduletag :system`
and excluded by default.

**A change to plugin option strictness is not verifiable by `mix test` alone.**
The two regression tests added here are, deliberately:
`provider_behaviour_test.exs` sweeps every enabled provider exporting
`validate_opts/1` against `Partition.provider_opts/1`, and drives
`operative_forward.init_provider/1` on the path a group start uses — asserting it
reaches the target check one step LATER, which is only possible because the
option check passed. The sweep is the one that closes the class rather than the
site: a provider added tomorrow is covered without anyone remembering.

## What the fix does NOT reach

The e2e goes green under the Rust reference and stays red on the veneer SDKs —
one step later, and for an unrelated reason: the forwarded frame's `channel`
metadata is the plugin's name there, not the channel's. Filed separately.
