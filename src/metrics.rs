use prometheus_client::{
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use subsquid_messages::query_result;

use crate::types::DatasetId;

lazy_static::lazy_static! {
    pub static ref VALID_PINGS: Counter = Default::default();
    pub static ref IGNORED_PINGS: Counter = Default::default();
    pub static ref QUERIES_SENT: Counter = Default::default();
    static ref QUERY_RESULTS: Family<Vec<(String, String)>, Counter> = Default::default();
    pub static ref KNOWN_WORKERS: Gauge = Default::default();
    pub static ref ACTIVE_STREAMS: Gauge = Default::default();
    pub static ref COMPLETED_STREAMS: Counter = Default::default();
    static ref HIGHEST_BLOCK: Family<Vec<(String, String)>, Gauge> = Default::default();
    static ref FIRST_GAP: Family<Vec<(String, String)>, Gauge> = Default::default();
}

pub fn report_query_result(result: &query_result::Result) {
    let status = match result {
        query_result::Result::Ok(_) => "ok",
        query_result::Result::BadRequest(_) => "bad_request",
        query_result::Result::ServerError(_) => "server_error",
        query_result::Result::NoAllocation(_) => "no_allocation",
        query_result::Result::TimeoutV1(()) | query_result::Result::Timeout(_) => "timeout",
    }
    .to_owned();
    QUERY_RESULTS
        .get_or_create(&vec![("status".to_owned(), status)])
        .inc();
}

pub fn report_dataset_updated(dataset_id: &DatasetId, highest_block: u32, first_gap: u32) {
    HIGHEST_BLOCK
        .get_or_create(&vec![("dataset".to_owned(), dataset_id.0.clone())])
        .set(highest_block as i64);
    FIRST_GAP
        .get_or_create(&vec![("dataset".to_owned(), dataset_id.0.clone())])
        .set(first_gap as i64);
}

pub fn register_metrics(registry: &mut Registry) {
    registry.register(
        "pings",
        "Number of received valid pings",
        VALID_PINGS.clone(),
    );
    registry.register(
        "ignored_pings",
        "Number of pings from unsupported workers",
        IGNORED_PINGS.clone(),
    );
    registry.register(
        "queries_sent",
        "Number of sent queries",
        QUERIES_SENT.clone(),
    );
    registry.register(
        "queries_responded",
        "Number of received responses",
        QUERY_RESULTS.clone(),
    );
    registry.register(
        "known_workers",
        "Number of workers seen in the network",
        KNOWN_WORKERS.clone(),
    );
    registry.register(
        "streams_active",
        "Number of currently open client streams",
        ACTIVE_STREAMS.clone(),
    );
    registry.register(
        "streams_completed",
        "Number of completed client streams",
        COMPLETED_STREAMS.clone(),
    );
    registry.register(
        "dataset_highest_block",
        "Highest seen block",
        HIGHEST_BLOCK.clone(),
    );
    registry.register(
        "dataset_first_gap",
        "First block not owned by any worker",
        FIRST_GAP.clone(),
    );
}
