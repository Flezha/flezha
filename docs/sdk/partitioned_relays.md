# Partitioning: implementation notes

For SDK implementers and contributors. How to use it is on
[Partitioning](../topics/core/partitioning.md).

## Key groups

```text
group(key) = fnv1a128(key_bytes) mod 256
```

The hash is the FNV-1a-128 the SDKs' `StableId` already freezes (offset basis
`0x6c62272e07bb014262b821756295c58d`, prime `2^88 + 0x13b`). Two implementations carry the key
group — `Flezha.Relay.KeyGroup.group_of/1` (the controller) and
`flz_core::util::key_group` (every SDK) — pinned against shared fixture vectors:

| key | key group |
| --- | --- |
| `SOL-USD` | 60 |
| `BTC-USD` | 129 |
| `ETH-USD` | 213 |
| `a` | 100 |
| `""` | 141 |

Assignment is a contiguous even split — `Partition.ranges(3)` is
`[{0,84}, {85,169}, {170,255}]` — but the owned range is **stored in the member's opts and
shipped on `AddRelay`** (`key_group_from` / `key_group_to`), never re-derived from
`(partition, partitions)`.
