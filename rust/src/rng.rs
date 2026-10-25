//! A tiny, dependency-free random source for jitter (retry backoff and lifetime jitter).
//!
//! The library never needs cryptographic randomness — only uniform `f64`s in `[0, 1)` to
//! spread retries and connection lifetimes. Rather than pull in the `rand` crate (and break
//! the zero-runtime-dependency rule), we ship a small [SplitMix64] generator seeded from the
//! standard library's process-randomized [`RandomState`], the same entropy source `HashMap`
//! uses. Tests inject a deterministic seed for reproducibility.
//!
//! [SplitMix64]: https://prng.di.unimi.it/splitmix64.c
//! [`RandomState`]: std::collections::hash_map::RandomState

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

/// A source of uniform `f64`s in `[0, 1)`. Mirrors `random.Random.random()` /
/// `rand.Rand.Float64()` from the sibling implementations.
pub trait Rng: Send {
    /// Returns the next uniform value in `[0, 1)`.
    fn next_f64(&mut self) -> f64;
}

/// The default [`Rng`]: a SplitMix64 PRNG seeded from the standard library's
/// process-randomized hasher state. Non-cryptographic — it exists only to jitter timings.
#[derive(Debug, Clone)]
pub struct SystemRng {
    state: u64,
}

impl SystemRng {
    /// Builds a generator with a fresh, process-randomized seed.
    pub fn new() -> Self {
        // RandomState is seeded from OS entropy per process; hashing nothing still yields a
        // random finish() because the keys themselves are random.
        let seed = RandomState::new().build_hasher().finish();
        Self::with_seed(seed)
    }

    /// Builds a generator from an explicit seed (deterministic; used in tests).
    pub fn with_seed(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

impl Default for SystemRng {
    fn default() -> Self {
        Self::new()
    }
}

impl Rng for SystemRng {
    fn next_f64(&mut self) -> f64 {
        // Take the top 53 bits for a uniform double in [0, 1), the usual construction.
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_in_unit_interval() {
        let mut rng = SystemRng::with_seed(42);
        for _ in 0..10_000 {
            let v = rng.next_f64();
            assert!((0.0..1.0).contains(&v), "value out of range: {v}");
        }
    }

    #[test]
    fn seeded_is_deterministic_and_default_runs() {
        let mut a = SystemRng::with_seed(123);
        let mut b = SystemRng::with_seed(123);
        for _ in 0..100 {
            assert_eq!(a.next_f64(), b.next_f64());
        }
        // A differently-seeded stream should diverge.
        let mut c = SystemRng::with_seed(124);
        assert_ne!(a.next_f64(), c.next_f64());
        // Exercise the process-seeded constructors (Default + new).
        let mut d = SystemRng::default();
        let _ = d.next_f64();
        let mut e = SystemRng::new();
        let _ = e.next_f64();
        // Debug/Clone are derived; touch them so they're covered.
        let cloned = a.clone();
        assert_eq!(format!("{cloned:?}"), format!("{a:?}"));
    }
}
