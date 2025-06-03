pub struct MeasuringRwLock<T> {
    rwlock: std::sync::RwLock<T>,
    name: &'static str,
}

pub struct MeasuringRwLockReadGuard<'a, T> {
    guard: std::sync::RwLockReadGuard<'a, T>,
    start: std::time::Instant,
    name: &'static str,
}

pub struct MeasuringRwLockWriteGuard<'a, T> {
    guard: std::sync::RwLockWriteGuard<'a, T>,
    start: std::time::Instant,
    name: &'static str,
}

impl<T> MeasuringRwLock<T> {
    pub fn new(data: T, name: &'static str) -> Self {
        crate::metrics::report_mutex_created(name);
        MeasuringRwLock {
            rwlock: std::sync::RwLock::new(data),
            name,
        }
    }

    pub fn read(&self) -> MeasuringRwLockReadGuard<'_, T> {
        MeasuringRwLockReadGuard {
            guard: self.rwlock.read().unwrap(),
            start: std::time::Instant::now(),
            name: self.name,
        }
    }

    pub fn write(&self) -> MeasuringRwLockWriteGuard<'_, T> {
        MeasuringRwLockWriteGuard {
            guard: self.rwlock.write().unwrap(),
            start: std::time::Instant::now(),
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
