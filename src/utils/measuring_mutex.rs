pub struct MeasuringMutex<T> {
    mutex: parking_lot::Mutex<T>,
    name: &'static str,
}

pub struct MeasuringMutexGuard<'a, T> {
    guard: parking_lot::MutexGuard<'a, T>,
    start: std::time::Instant,
    name: &'static str,
}

impl<T> MeasuringMutex<T> {
    pub fn new(data: T, name: &'static str) -> Self {
        crate::metrics::report_mutex_created(name);
        MeasuringMutex {
            mutex: parking_lot::Mutex::new(data),
            name,
        }
    }

    pub fn lock(&self) -> MeasuringMutexGuard<'_, T> {
        let start = std::time::Instant::now();
        MeasuringMutexGuard {
            guard: self.mutex.lock(),
            start,
            name: self.name,
        }
    }

    pub fn try_lock(&self) -> Option<MeasuringMutexGuard<'_, T>> {
        match self.mutex.try_lock() {
            Some(guard) => Some(MeasuringMutexGuard {
                guard,
                start: std::time::Instant::now(),
                name: self.name,
            }),
            None => None,
        }
    }
}

impl<T> Drop for MeasuringMutex<T> {
    fn drop(&mut self) {
        crate::metrics::report_mutex_destroyed(self.name);
    }
}

impl<'a, T> std::ops::Deref for MeasuringMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}
impl<'a, T> std::ops::DerefMut for MeasuringMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<'a, T> Drop for MeasuringMutexGuard<'a, T> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        tracing::trace!("Lock {} held for: {:?}", self.name, duration);
        crate::metrics::report_mutex_held_duration(
            self.name,
            duration,
            crate::metrics::MutexLockMode::Write,
        );
    }
}
