use std::time::{Duration, SystemTime};

use num_rational::Ratio;
use serde::Serialize;
use sqd_contract_client::Worker;

#[derive(Clone, PartialEq, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    DataLoading,
    Registered,
    Unregistered,
}

#[derive(Debug, Clone)]
pub struct ContractsState {
    pub sqd_locked: Ratio<u128>,
    pub status: Status,
    pub operator: String,
    pub current_epoch: u32,
    pub current_epoch_started: SystemTime,
    pub compute_units_per_epoch: u64,
    pub epoch_length: Duration,
    pub uses_default_strategy: bool,
    pub active_workers_length: u64,
}

impl Default for ContractsState {
    fn default() -> Self {
        ContractsState {
            operator: Default::default(),
            current_epoch: Default::default(),
            sqd_locked: Default::default(),
            status: Status::DataLoading,
            uses_default_strategy: Default::default(),
            epoch_length: Default::default(),
            active_workers_length: Default::default(),
            current_epoch_started: SystemTime::UNIX_EPOCH,
            compute_units_per_epoch: Default::default(),
        }
    }
}

impl ContractsState {
    #[allow(clippy::too_many_arguments)]
    pub fn set(
        &mut self,
        current_epoch: u32,
        sqd_locked: Option<(String, Ratio<u128>)>,
        epoch_length: Duration,
        uses_default_strategy: bool,
        active_workers: &[Worker],
        current_epoch_started: SystemTime,
        compute_units_per_epoch: u64,
    ) {
        if let Some((operator, sqd)) = sqd_locked {
            self.operator = operator;
            self.sqd_locked = sqd;
            self.status = Status::Registered;
        } else {
            self.sqd_locked = Ratio::new(0, 1);
            self.status = Status::Unregistered;
        }

        self.current_epoch = current_epoch;
        self.epoch_length = epoch_length;
        self.uses_default_strategy = uses_default_strategy;
        self.active_workers_length = active_workers.len() as u64;
        self.current_epoch_started = current_epoch_started;
        self.compute_units_per_epoch = compute_units_per_epoch;
    }
}
