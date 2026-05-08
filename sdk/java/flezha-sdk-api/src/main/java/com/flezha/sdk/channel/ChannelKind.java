package com.flezha.sdk.channel;

/** Kind of channel — entry (controller → SDK), inter-shard, or plugin-bound (SDK → plugin). */
public enum ChannelKind {
    ENTRY,
    INTER_SHARD,
    PLUGIN_BOUND
}
