package com.flezha.sdk.channel;

/** Kind of channel — entry (controller → SDK), inter-procedure, or plugin-bound (SDK → plugin). */
public enum ChannelKind {
    ENTRY,
    INTER_PROCEDURE,
    PLUGIN_BOUND
}
