use anyhow::{anyhow, Result};
use parking_lot::Mutex;

pub struct UseOnce<T> {
    inner: Mutex<Option<T>>,
}

impl<T> UseOnce<T> {
    pub fn new(value: T) -> Self {
        UseOnce {
            inner: Mutex::new(Some(value)),
        }
    }

    pub fn take(&self) -> Result<T> {
        self.inner
            .try_lock()
            .and_then(|mut opt| opt.take())
            .ok_or_else(|| anyhow!("Attempted to take value twice"))
    }
}
