pub struct MeasuringMutex<T> {
    mutex: std::sync::Mutex<T>,
    name: &'static str,
}

pub struct MeasuringMutexGuard<'a, T> {
    guard: std::sync::MutexGuard<'a, T>,
    start: std::time::Instant,
    name: &'static str,
}

impl<T> MeasuringMutex<T> {
    pub fn new(data: T, name: &'static str) -> Self {
        crate::metrics::report_mutex_created(name);
        MeasuringMutex {
            mutex: std::sync::Mutex::new(data),
            name,
        }
    }

    pub fn lock(&self) -> MeasuringMutexGuard<'_, T> {
        MeasuringMutexGuard {
            guard: self.mutex.lock().unwrap(),
            start: std::time::Instant::now(),
            name: self.name,
        }
    }

    pub fn try_lock(&self) -> Option<MeasuringMutexGuard<'_, T>> {
        match self.mutex.try_lock() {
            Ok(guard) => Some(MeasuringMutexGuard {
                guard,
                start: std::time::Instant::now(),
                name: self.name,
            }),
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(e @ std::sync::TryLockError::Poisoned(_)) => {
                Err(e).unwrap()
            }
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
