package com.flezha.sdk.runtime.operative.builder;

/**
 * What a merge source resolves to: the channel it subscribes on and the chain
 * declared on the handle — the erasure-side stand-in for Rust's
 * {@code MergeSource::merge_site}.
 */
interface EdgeSite {
    int siteChannelId();

    Integer sitePending();
}
