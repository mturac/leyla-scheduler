use chrono::{DateTime, TimeDelta, Utc};
use std::sync::{Arc, Mutex};

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub struct FakeClock {
    inner: Arc<Mutex<DateTime<Utc>>>,
}

impl FakeClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(now)),
        }
    }

    pub fn advance(&self, delta: TimeDelta) {
        let mut t = self.inner.lock().unwrap();
        *t = *t + delta;
    }

    pub fn set(&self, time: DateTime<Utc>) {
        *self.inner.lock().unwrap() = time;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.inner.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_returns_now() {
        let clock = SystemClock;
        let before = Utc::now();
        let now = clock.now();
        let after = Utc::now();

        assert!(before <= now && now <= after);
    }

    #[test]
    fn fake_clock_returns_fixed_time() {
        let fixed_time = Utc::now();
        let clock = FakeClock::new(fixed_time);

        assert_eq!(clock.now(), fixed_time);
        // Call multiple times, should always return the same time
        assert_eq!(clock.now(), fixed_time);
        assert_eq!(clock.now(), fixed_time);
    }

    #[test]
    fn fake_clock_advance_moves_time() {
        let initial = Utc::now();
        let clock = FakeClock::new(initial);

        let delta = TimeDelta::try_seconds(60).unwrap();
        clock.advance(delta);

        let advanced = clock.now();
        assert_eq!(advanced, initial + delta);
    }

    #[test]
    fn fake_clock_set_changes_time() {
        let initial = Utc::now();
        let clock = FakeClock::new(initial);

        let new_time = initial + TimeDelta::try_seconds(3600).unwrap();
        clock.set(new_time);

        assert_eq!(clock.now(), new_time);
    }
}
