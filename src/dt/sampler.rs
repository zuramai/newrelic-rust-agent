//! Adaptive sampler deciding which transactions are sampled for distributed tracing.
//!
//! Port of the Go agent's `adaptive_sampler.go`: in the first period the first `target`
//! transactions are sampled; later periods sample by priority against a moving threshold and
//! back off exponentially once `target` is exceeded.

use std::time::{Duration, Instant};

#[derive(Debug)]
pub(crate) struct AdaptiveSampler {
    target: u64,
    period: Duration,
    period_end: Instant,
    priority_min: f32,
    num_seen: u64,
    num_sampled: u64,
}

impl AdaptiveSampler {
    pub(crate) fn new(target: u64, period: Duration, now: Instant) -> Self {
        Self {
            target,
            period,
            period_end: now + period,
            priority_min: 0.0,
            num_seen: 0,
            num_sampled: 0,
        }
    }

    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        reason = "sampler thresholds are approximate by design, mirroring the Go agent"
    )]
    pub(crate) fn compute_sampled(&mut self, priority: f32, now: Instant) -> bool {
        if self.target == 0 {
            return false;
        }
        if self.period.is_zero() {
            self.period = Duration::from_secs(60);
        }
        while now > self.period_end {
            self.priority_min = if self.num_seen > 0 {
                let ratio = self.target as f64 / self.num_seen as f64;
                (1.0 - ratio) as f32
            } else {
                0.0
            };
            self.num_seen = 0;
            self.num_sampled = 0;
            self.period_end += self.period;
        }
        self.num_seen += 1;
        let sampled = if self.num_sampled > self.target {
            let target = self.target as f64;
            let ratio = target / self.num_sampled as f64;
            let expected = target.powf(ratio) - target.powf(0.5);
            (rand::random_range(0..self.num_seen) as f64) < expected
        } else {
            priority >= self.priority_min
        };
        if sampled {
            self.num_sampled += 1;
        }
        sampled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_sampled_samples_first_target_calls_in_first_period() {
        let now = Instant::now();
        let mut s = AdaptiveSampler::new(10, Duration::from_secs(60), now);
        let sampled = (0..10).filter(|_| s.compute_sampled(0.0, now)).count();
        assert_eq!(sampled, 10);
    }

    #[test]
    fn compute_sampled_never_samples_when_target_is_zero() {
        let now = Instant::now();
        let mut s = AdaptiveSampler::new(0, Duration::from_secs(60), now);
        assert!(!(0..10).any(|_| s.compute_sampled(1.0, now)));
    }

    #[test]
    fn compute_sampled_raises_threshold_after_busy_period() {
        let now = Instant::now();
        let mut s = AdaptiveSampler::new(10, Duration::from_secs(60), now);
        for _ in 0..100 {
            s.compute_sampled(0.5, now);
        }
        // 10 of 100 seen => next period samples only priorities >= 0.9.
        let next = now + Duration::from_secs(61);
        assert!(!s.compute_sampled(0.5, next));
        assert!(s.compute_sampled(0.95, next));
    }
}
