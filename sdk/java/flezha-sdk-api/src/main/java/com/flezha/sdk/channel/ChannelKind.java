package com.flezha.sdk.channel;

/** Kind of channel — entry (controller → SDK), inter-operation, or plugin-bound (SDK → plugin). */
public enum ChannelKind {
    ENTRY,
    INTER_OPERATION,
    PLUGIN_BOUND
}
