package com.flezha.sdk.operation;

import javax.annotation.Nonnull;

/**
 * A deterministic, replay-stable 128-bit identifier — the determinism-contract
 * replacement for {@code UUID.randomUUID()}.
 *
 * <p>Derived by FNV-1a-128 over {@code segment identity ‖ big-endian seq ‖
 * big-endian ordinal}. The n-th {@code ctx.stableId()} call of a replayed
 * invocation equals the n-th call of the original, which is what lets a
 * replayed run reproduce the original's emitted identities byte for byte.
 *
 * <p>The hash is frozen by the cross-language spec: it must agree with the Rust
 * SDK's {@code StableId::derive} bit for bit, and {@link #toString()} must
 * render exactly as Rust's {@code {:032x}}. Java has no {@code u128}, so the
 * value is carried as two unsigned {@code long} halves.
 */
public record StableId(long hi, long lo) {

    private static final long BASIS_HI = 0x6c62272e07bb0142L;
    private static final long BASIS_LO = 0x62b821756295c58dL;

    /**
     * The FNV-128 prime is {@code 2^88 + 0x13b}. Because only two limbs are
     * non-zero, a full 128×128 multiply collapses to one shift plus a single
     * 64×64 widening multiply — no BigInteger, no allocation.
     */
    private static final long PRIME_LOW = 0x13bL;

    /**
     * Derive the id for one {@code ctx.stableId()} call.
     *
     * @param segmentIdentity the Segment's stable identity bytes
     * @param seq the invocation's durable change seq
     * @param ordinal the 0-based call ordinal within the invocation
     */
    @Nonnull
    public static StableId derive(@Nonnull byte[] segmentIdentity, long seq, int ordinal) {
        long hi = BASIS_HI;
        long lo = BASIS_LO;

        for (byte b : segmentIdentity) {
            lo ^= (b & 0xFFL);
            long nHi = mulHi(hi, lo);
            long nLo = mulLo(lo);
            hi = nHi;
            lo = nLo;
        }
        for (int shift = 56; shift >= 0; shift -= 8) {
            lo ^= ((seq >>> shift) & 0xFFL);
            long nHi = mulHi(hi, lo);
            long nLo = mulLo(lo);
            hi = nHi;
            lo = nLo;
        }
        for (int shift = 24; shift >= 0; shift -= 8) {
            lo ^= ((ordinal >>> shift) & 0xFFL);
            long nHi = mulHi(hi, lo);
            long nLo = mulLo(lo);
            hi = nHi;
            lo = nLo;
        }
        return new StableId(hi, lo);
    }

    /** High half of {@code (hi,lo) * PRIME} mod 2^128. */
    private static long mulHi(long hi, long lo) {
        return (lo << 24) + hi * PRIME_LOW + Math.unsignedMultiplyHigh(lo, PRIME_LOW);
    }

    /** Low half of {@code (hi,lo) * PRIME} mod 2^128. */
    private static long mulLo(long lo) {
        return lo * PRIME_LOW;
    }

    /** 32 lowercase hex digits, matching Rust's {@code {:032x}}. */
    @Override
    @Nonnull
    public String toString() {
        return String.format("%016x%016x", hi, lo);
    }
}
