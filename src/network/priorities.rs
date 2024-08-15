use std::{
    cmp::{max, min},
    collections::HashMap,
    sync::Arc,
};

use contract_client::PeerId;
use static_assertions::const_assert;

use crate::cli::Config;

type Priority = i8;

/// The number subtracted from the priority while the worker is busy executing a query
const LEASE_PENALTY: Priority = 2;

pub struct WorkersPool {
    config: Arc<Config>,
    priorities: HashMap<PeerId, Priority>,
}

impl WorkersPool {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            priorities: Default::default(),
        }
    }

    pub fn pick(&mut self, workers: impl IntoIterator<Item = PeerId>) -> Option<PeerId> {
        let (priority, worker) = workers
            .into_iter()
            .map(|worker| (*self.priorities.entry(worker).or_default(), worker))
            .max_by_key(|(priority, _worker)| *priority)?;
        if priority <= self.config.min_worker_priority {
            return None;
        }

        self.dec(worker, LEASE_PENALTY);

        Some(worker)
    }

    pub fn success(&mut self, worker: PeerId) {
        self.inc(worker, LEASE_PENALTY + 1);
    }

    pub fn error(&mut self, worker: PeerId) {
        const_assert!(LEASE_PENALTY >= 2);
        self.inc(worker, LEASE_PENALTY - 2);
    }

    pub fn timeout(&mut self, worker: PeerId) {
        self.inc(worker, LEASE_PENALTY - 1);
    }

    pub fn unavailable(&mut self, worker: PeerId) {
        let entry = self.priorities.entry(worker).or_default();
        *entry = self.config.min_worker_priority - 1;
    }

    pub fn reset(&mut self) {
        self.priorities.clear();
    }

    fn inc(&mut self, worker: PeerId, delta: i8) {
        let entry = self.priorities.entry(worker).or_default();
        *entry = min(*entry + delta, self.config.max_worker_priority);
        tracing::trace!("Worker {} priority increased to {}", worker, *entry);
    }

    fn dec(&mut self, worker: PeerId, delta: i8) {
        let entry = self.priorities.entry(worker).or_default();
        *entry = max(*entry - delta, self.config.min_worker_priority);
        tracing::trace!("Worker {} priority decreased to {}", worker, *entry);
    }
}
