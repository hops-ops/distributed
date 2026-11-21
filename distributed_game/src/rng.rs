//! Seeded deterministic per-zone random numbers.
//!
//! [`ZoneRng`] is xoshiro256** seeded through SplitMix64. A zone's generator is
//! derived from the deployment's world seed and the zone id
//! ([`ZoneRng::for_zone`]), so the same world seed, zone id and input log
//! always reproduce the same simulation. Zones must take randomness only from
//! [`Ctx::rng`](crate::Ctx::rng) to stay deterministic.

/// 64-bit FNV-1a, used for seed derivation and as a cheap state-hash helper.
#[derive(Debug, Clone, Copy)]
pub struct Fnv64(u64);

impl Fnv64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    /// Fresh hasher.
    pub fn new() -> Self {
        Self(Self::OFFSET)
    }

    /// Mix raw bytes.
    pub fn write(&mut self, bytes: &[u8]) -> &mut Self {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
        self
    }

    /// Mix a little-endian u64.
    pub fn write_u64(&mut self, value: u64) -> &mut Self {
        self.write(&value.to_le_bytes())
    }

    /// Mix an f32 by its bit pattern.
    pub fn write_f32(&mut self, value: f32) -> &mut Self {
        self.write(&value.to_bits().to_le_bytes())
    }

    /// Current digest.
    pub fn finish(&self) -> u64 {
        self.0
    }
}

impl Default for Fnv64 {
    fn default() -> Self {
        Self::new()
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Deterministic xoshiro256** generator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneRng {
    s: [u64; 4],
}

impl ZoneRng {
    /// Seed directly.
    pub fn from_seed(seed: u64) -> Self {
        let mut sm = seed;
        let s = [
            splitmix64(&mut sm),
            splitmix64(&mut sm),
            splitmix64(&mut sm),
            splitmix64(&mut sm),
        ];
        Self { s }
    }

    /// Derive a zone's generator: `seed = world_seed ^ fnv1a64(zone_id)`.
    pub fn for_zone(world_seed: u64, zone_id: &str) -> Self {
        Self::from_seed(world_seed ^ Fnv64::new().write(zone_id.as_bytes()).finish())
    }

    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    /// Next 32 random bits (high half of [`next_u64`](Self::next_u64)).
    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// Uniform float in `[0, 1)` with 24 bits of precision.
    pub fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 * (1.0 / (1u32 << 24) as f32)
    }

    /// Uniform integer in `[low, high)`; returns `low` when the range is empty.
    pub fn range_u32(&mut self, low: u32, high: u32) -> u32 {
        if high <= low {
            return low;
        }
        let span = u64::from(high - low);
        low + ((u64::from(self.next_u32()) * span) >> 32) as u32
    }

    /// Bernoulli trial with probability `p`.
    pub fn chance(&mut self, p: f32) -> bool {
        self.next_f32() < p
    }
}
