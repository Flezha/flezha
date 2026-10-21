use std::time::Duration;

use crate::utility::noise::{noise, xorshift64};

/// `base * 2^min(attempt, factor_cap)`, at most `cap`, jittered and re-capped.
pub struct Ladder {
    pub base: Duration,
    pub factor_cap: u32,
    pub cap: Duration,
}

impl Ladder {
    pub fn delay(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.min(self.factor_cap));
        jitter(self.base.saturating_mul(factor).min(self.cap)).min(self.cap)
    }
}

/// The controller's draw (`Flezha.Config.Backoff.jitter/2`): `d/2 + u` ms, `u` uniform
/// in `1..=d`. Runtime scheduling, outside the determinism contract.
pub fn jitter(d: Duration) -> Duration {
    let ms = d.as_millis() as u64;
    let mut x = noise() | 1;
    xorshift64(&mut x);
    Duration::from_millis(ms / 2 + 1 + x % ms.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DRAWS: usize = 2_000;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn jitter_stays_within_the_controllers_band() {
        for d in [1, 2, 7, 1_000] {
            for _ in 0..DRAWS {
                let j = jitter(ms(d)).as_millis() as u64;
                assert!(j > d / 2 && j <= d / 2 + d, "jitter({d}) drew {j}");
            }
        }
    }

    #[test]
    fn a_delay_never_exceeds_the_cap() {
        let ladder = Ladder {
            base: ms(100),
            factor_cap: 16,
            cap: ms(250),
        };
        for attempt in 0..20 {
            for _ in 0..DRAWS / 20 {
                assert!(
                    ladder.delay(attempt) <= ms(250),
                    "attempt {attempt} past the cap"
                );
            }
        }
    }

    #[test]
    fn the_exponent_stops_at_its_cap() {
        let ladder = Ladder {
            base: ms(10),
            factor_cap: 3,
            cap: ms(10_000),
        };
        for _ in 0..DRAWS {
            // Rung 3 is 80 ms; rung 20 would be past the cap without the exponent cap.
            let d = ladder.delay(20).as_millis() as u64;
            assert!((41..=120).contains(&d), "attempt 20 drew {d}");
        }
    }
}
