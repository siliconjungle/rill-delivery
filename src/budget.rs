use crate::Error;

/// Deterministic integer token bucket. The application chooses the time unit.
/// No sleeping, clock reads, async runtime, floating point or congestion-control
/// assumptions. This is an application cap, not a substitute for transport pacing.
#[derive(Clone, Debug)]
pub struct TokenBucket {
    capacity: u64,
    rate: u64,
    tokens: u64,
    last: u64,
}
impl TokenBucket {
    pub fn new(capacity: u64, bytes_per_tick: u64, now: u64) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            capacity,
            rate: bytes_per_tick,
            tokens: capacity,
            last: now,
        })
    }
    pub fn available(&mut self, now: u64) -> Result<u64, Error> {
        if now < self.last {
            return Err(Error::ClockWentBack);
        }
        let refill = u128::from(now - self.last) * u128::from(self.rate);
        self.tokens = (u128::from(self.tokens) + refill).min(u128::from(self.capacity)) as u64;
        self.last = now;
        Ok(self.tokens)
    }
    pub fn consume(&mut self, bytes: u64) -> bool {
        if bytes > self.tokens {
            false
        } else {
            self.tokens -= bytes;
            true
        }
    }
    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}
