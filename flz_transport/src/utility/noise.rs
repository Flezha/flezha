//! Dependency-free decorrelation noise: the clock's sub-millisecond jitter is
//! plenty to spread a fleet's timing or shuffle a choice, and a xorshift step
//! spreads one seed into a sequence — cryptographic randomness is not needed
//! anywhere this is used.

/// The clock's sub-millisecond jitter, as a seed.
pub fn noise() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0)
}

/// One xorshift64 step over `state`. A PRNG crate for choosing an order or a
/// delay would be a dependency for nothing.
pub fn xorshift64(state: &mut u64) {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
}
