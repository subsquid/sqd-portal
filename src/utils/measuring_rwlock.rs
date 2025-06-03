pub struct MeasuringRwLock<T> {
    rwlock: parking_lot::RwLock<T>,
    name: &'static str,
}

pub struct MeasuringRwLockReadGuard<'a, T> {
    guard: parking_lot::RwLockReadGuard<'a, T>,
    start: std::time::Instant,
    name: &'static str,
}

pub struct MeasuringRwLockWriteGuard<'a, T> {
    guard: parking_lot::RwLockWriteGuard<'a, T>,
    start: std::time::Instant,
    name: &'static str,
}

impl<T> MeasuringRwLock<T> {
    pub fn new(data: T, name: &'static str) -> Self {
        crate::metrics::report_mutex_created(name);
        MeasuringRwLock {
            rwlock: parking_lot::RwLock::new(data),
            name,
        }
    }

    pub fn read(&self) -> MeasuringRwLockReadGuard<'_, T> {
        let start = std::time::Instant::now();
        MeasuringRwLockReadGuard {
            guard: self.rwlock.read(),
            start,
            name: self.name,
        }
    }

    pub fn write(&self) -> MeasuringRwLockWriteGuard<'_, T> {
        let start = std::time::Instant::now();
        MeasuringRwLockWriteGuard {
            guard: self.rwlock.write(),
            start,
            name: self.name,
        }
    }
}

impl<T> Drop for MeasuringRwLock<T> {
    fn drop(&mut self) {
        crate::metrics::report_mutex_destroyed(self.name);
    }
}

impl<'a, T> std::ops::Deref for MeasuringRwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a, T> Drop for MeasuringRwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        tracing::trace!("Read lock {} held for: {:?}", self.name, duration);
        crate::metrics::report_mutex_held_duration(
            self.name,
            duration,
            crate::metrics::MutexLockMode::Read,
        );
    }
}

impl<'a, T> std::ops::Deref for MeasuringRwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a, T> std::ops::DerefMut for MeasuringRwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<'a, T> Drop for MeasuringRwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        tracing::trace!("Write lock {} held for: {:?}", self.name, duration);
        crate::metrics::report_mutex_held_duration(
            self.name,
            duration,
            crate::metrics::MutexLockMode::Write,
        );
    }
}
