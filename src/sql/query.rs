use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use axum::body;
use prost::{DecodeError, Message};
use serde::{Deserialize, Serialize};
use substrait::proto::Plan;
use thiserror;

use crate::network::{ChunkNotFound, NetworkClient};
use crate::sql::rewrite_target;
use crate::types::{BlockNumber, DatasetId};

use sql_query_plan::plan::{self, Source, TargetPlan};

#[derive(Debug, thiserror::Error)]
pub enum QueryErr {
    #[error("cannot decode query plan: {0}")]
    DecodePlan(#[from] DecodeError),
    #[error("cannot transform plan: {0}")]
    Planning(#[from] plan::PlanErr),
    #[error("cannot compile rewritten plan: {0}")]
    RewriteTarget(#[from] rewrite_target::RewriteTargetErr),
    #[error("no worker available for chunk: {0}")]
    NoChunk(#[from] ChunkNotFound),
    #[error("no worker available for chunk: {0}")]
    NoWorker(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SqlQueryResponse {
    pub query_id: String,
    pub tables: Vec<TableItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableItem {
    pub schema_name: String,
    pub table_name: String,
    pub approx_num_rows: u64,
    pub workers: Vec<TableWorker>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableWorker {
    pub peer_id: String,
    pub chunks: Vec<TableChunk>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableChunk {
    pub sql: String,
    pub chunk_id: String,
}

pub fn get_sources(
    request: body::Bytes,
    ctx: &mut plan::TraversalContext,
) -> Result<Vec<Source>, QueryErr> {
    let plan = Plan::decode(request)?;
    let target = plan::traverse_plan::<rewrite_target::RewriteTarget>(&plan, ctx)?;
    let mut sources = target.get_sources();
    target.pushdown_filters(ctx, &mut sources)?;
    let mut res = Vec::new();
    for src in sources {
        if src.sqd {
            res.push(src);
        }
    }
    Ok(res)
}

pub fn get_chunks(
    dataset_id: &DatasetId,
    blocks: &[Range<u64>],
    network: &Arc<NetworkClient>,
) -> Result<HashMap<String, BlockNumber>, QueryErr> {
    let mut chunks = HashMap::new();
    for range in blocks {
        get_chunks_for_range(network, dataset_id, range, &mut chunks)?;
    }
    Ok(chunks)
}

pub fn get_chunks_for_range(
    network: &Arc<NetworkClient>,
    dataset_id: &DatasetId,
    range: &Range<u64>,
    chunks: &mut HashMap<String, BlockNumber>,
) -> Result<(), QueryErr> {
    match network.find_chunk(dataset_id, range.start) {
        Err(e) => tracing::info!("no chunks found for {}: {:?}", range.start, e),
        Ok(chunk) => {
            chunks.insert(chunk.to_string(), chunk.first_block);
            let mut previous = chunk;
            loop {
                if let Some(chunk) = network.next_chunk(dataset_id, &previous) {
                    if chunk.first_block < range.end {
                        chunks.insert(chunk.to_string(), chunk.first_block);
                        previous = chunk;
                        continue;
                    }
                }
                break;
            }
        }
    }
    Ok(())
}

pub fn get_workers(
    dataset_id: &DatasetId,
    sql: &str,
    chunks: &HashMap<String, BlockNumber>,
    network: &Arc<NetworkClient>,
) -> Result<Vec<TableWorker>, QueryErr> {
    let mut workers: HashMap<String, Vec<TableChunk>> = HashMap::new();
    for (chunk, block) in chunks.into_iter() {
        let mut wrk = None;
        // it would be convenient to search for workers directly by chunk;
        // here we search for the chunk again. Should be implemented in NetworkClient.
        for w in network.get_workers(dataset_id, *block) {
            let w = w.peer_id.to_string();
            //if COMPATIBLE_WORKERS.binary_search(&w.as_str()).is_ok() {
                wrk = Some(w);
                break;
            //}
        }
        if let Some(peer_id) = wrk {
            let tch = TableChunk {
                sql: sql.to_string(),
                chunk_id: chunk.to_string(),
            };
            workers
                .entry(peer_id.to_string())
                .and_modify(|v| v.push(tch.clone()))
                .or_insert(vec![tch]);
        } else {
            return Err(QueryErr::NoWorker(chunk.to_string()));
        }
    }
    let mut tws = Vec::new();
    for (peer_id, chunks) in workers.into_iter() {
        tws.push(TableWorker {
            peer_id: peer_id.to_string(),
            chunks: chunks,
        });
    }

    Ok(tws)
}

pub fn unwrap_field_ranges(frs: &[plan::FieldRange]) -> Vec<Range<u64>> {
    let mut v = Vec::new();
    for fr in frs {
        match fr {
            plan::FieldRange::BlockNumber(r) => v.push(Range {
                start: r.start as u64,
                end: r.end as u64,
            }),
            _ => continue, // TODO: timestamps to blocks
        }
    }
    v
}

pub fn compile_sql(src: &Source, ctx: &plan::TraversalContext) -> Result<String, QueryErr> {
    Ok(rewrite_target::compile_sql(src, ctx)?)
}

// These are workers I happen to know to support the SQL API;
// without this workaround a lot of queries fail because of
// network errors - which is inconvenient.
static COMPATIBLE_WORKERS: &[&str] = &[
    "12D3KooW9tTukw24wSZLUWoyAHsDHKmnEmRbiDv9TGNp5hLnFvaA",
    "12D3KooW9yQws9oxjj6aAAKov8zb69ZVj5UEBzFh6sD4srTNfQFb",
    "12D3KooWAtiaMNKiT1MTnugK7orXFGpyBFZTWF5gYFEUTEGeakvP",
    "12D3KooWBmg88KjHB2HArsshEESHEaVyH7K359TpwxZDeWxGaXJa",
    "12D3KooWBoMgij82fmcy1yuzds6yaCMw2m5gzqsYFaHvYF5whGsW",
    "12D3KooWBq5QaV8wUwv41nEadEXD7tSViwRiPAMVE2bXjTLRQV4j",
    "12D3KooWCGx27TMgJkinCu8ruPi4zSuFivK8WUDqraFayo1D9vkx",
    "12D3KooWCLa47XL36m2uUB1x3Uad4Kcp6JgEV2R1TrVD5NHfjE6z",
    "12D3KooWCWfj2YkK3n49EtiSMea2By9mVjRqos3FLksWNAQD6A1j",
    "12D3KooWCbdRypTsRPtSNg7hEbPZDGNC1D36nUeCw6ojBmwRt5gg",
    "12D3KooWD7A2vFVd5xg8MSfQU3EJX9novMSSdYmFfuTfzxruyWJ2",
    "12D3KooWD7Zh8h2pWtn7ucqn3WAGxALZEayyArYTPCMvfFpK795j",
    "12D3KooWDJbk4fUwkKFoMwev7YyUFx7pPAXbMyVkRH7sH42nv4vq",
    "12D3KooWDRSk2eih4G4Cc8BKMo7YJZzRiMse7zhxeLaqQ1rdHRJc",
    "12D3KooWDVy9rTvAR4pWc864EjF6QgBPd2iur7Af4jdPHpV79X54",
    "12D3KooWDYkXwAXTcnbTk9KKsBoNEoiZfEPbM9pAD1EJBHxA54Yg",
    "12D3KooWEJmVk686ksUCCr6dEKTX6ezSkMZqjLy1ZWasArNM1nbk",
    "12D3KooWEQFPG9druEtes4Bcjf5T3vuguxennMWM8nGsybKS78iH",
    "12D3KooWEYbv2PZnrjc2dWMwiH7zRP3AWCZLHQETNRtAUbBeh7rt",
    "12D3KooWEeypC5D4qhFRkG7UCvFp3H3QvZ1WuBDSfG5dtpreTCRr",
    "12D3KooWF9K7pqr9Rvbtch2eQdPE39QT9Vhw8UWzWhrhkoqMSm8J",
    "12D3KooWFChbfGXVyujF46xsKyKuAiFY31JYNPudWoByuJA2avYP",
    "12D3KooWFK7wkLC37RvFGAipqjRWFGj1NSKAviAtNUQmGRNYnLKN",
    "12D3KooWFYwzNoHCri6ZvWKi56e2H8krW22rccpUBzUHnEx3mcEe",
    "12D3KooWFtwxLHAQoySEE6S8dcZensQv9Ei3e2Vcg7kkNDW1KUqt",
    "12D3KooWFzpwx3mu75ZWE9kEsn6uiAV7sdAs3p9rYn4nNdHnNDsC",
    "12D3KooWG6gBg52bNt9Qu1932Nvo2YgT5SPwzyFr94FB55swqBJT",
    "12D3KooWG8x3aX2bp68p9cAWwTMEXLNWa9ia4Yr3veZQLKh4xKVm",
    "12D3KooWG9qAyDQCiLAt5d7faoquE1bWhtCKGDxLnKtBaNPNNF4M",
    "12D3KooWGpKy1UDDhK8SEuZ69vm7akZuXKeZV2QSTSwMTK2dwSdJ",
    "12D3KooWH4LTwdHinxwq3hNHqJZuNGSsM3ANdLhVKQ2Nnu63By3u",
    "12D3KooWHEFpp2BkY8Gnk4wGWwgGCXwTM8MF2HMVNjh9QACiwR2g",
    "12D3KooWHKBBGyYU83Jx3eCPCTPbysPe7e6bbVhYtvvHsP6QELfD",
    "12D3KooWJZRqw4thUjoCZMVnh1p64oDMSxJGPrGQw5xkDrWHQ72o",
    "12D3KooWK3oLhAWfUZHTEgcoTcEdsDaCSjqzVMK8W9cqVZ4SisUD",
    "12D3KooWKW4WDda7PMk3doPHU4STYqGWRpf6HuEMaTfZGpxeaiUS",
    "12D3KooWKdnQzTd5LqboBoNAc5AyEfEQmXR2zYQNweh2yR2dhHme",
    "12D3KooWKfwmLBAUZHSQRcEvQWAAUfmWaEhVkitPymEwXsycPoiH",
    "12D3KooWKwhKrYJMav2JrndFhNWXMDg6bFUXKPxZGAswWE2A55rA",
    "12D3KooWLHXNmCkoVLrXwUGVQruSMe779oeo6w4ANVUjx3ygdrjs",
    "12D3KooWLoJTLWH5FPnLuFMCJE9Ryf3zxivvbhs71jwKp7aER3yg",
    "12D3KooWLqHwBnqJNrKLSU5hCiqCUGcmKghwyQfr3JSpRDmAn6eR",
    "12D3KooWLubdDzVZ3UFuTFJVvmwtNwDbeGZC15n6H1FCedtpLPik",
    "12D3KooWM33BK2xQ5XYcdV8gduPXZJHCjkYQmcjZnSbJBkYxuCW2",
    "12D3KooWMKyadf8yAgmBSTnnC4LLb9VPzf5mNP4geedJxYCEHHoi",
    "12D3KooWMamLcjggpjfTDrLQaA1StxWvkznTdTJZ6yS6bz1y28SA",
    "12D3KooWNJUY4VMSmbD8xPwhJn19CB9hHQjJmEhXcEZBWsYuf6DB",
    "12D3KooWNXUhrbpjDWN6bwPTF2b4onvCoASPnwNxukuvPQq82r38",
    "12D3KooWNpaXfKgFTAnwZRN2fsb9wbrAmqDGYdMicY1pjKSZkBV3",
    "12D3KooWNzVwL5hNfodCWYesNYgfrzq3rFFhZTuquDi2EZvd1Sjy",
    "12D3KooWP7TBhxitq7i7ki8d4Zj5FgnvXQmjZHL2yHG8iGbCPeTk",
    "12D3KooWPKNvmGPUsw54mfUtc5CUEV2eHPNyTfYeUngr7G34pwZy",
    "12D3KooWPM3MFkaUxwFbM6YXCcEjgVTEsVRgKWk7Twx8yAvCgmZu",
    "12D3KooWPNtHLDXW1AETDx8a7rEoeXQPuWg2WFZTry4PxAvZMA4X",
    "12D3KooWPQAS6Fa58FaETMn4ow6HKGDmLgEyTsaV2aaEASQaZFur",
    "12D3KooWPaWndUFs7S4vU9XtF5hhdZabu3GTvfe691htdd1qUWrP",
    "12D3KooWPzqcwhtP7SJpwEDVdH4JE9eQc3fbhqABafpF6ZXxVuHR",
    "12D3KooWQ6Gcit7HkWxRgMGbdcPGSB1YKFhtPaQHXkSf8ZeGwrP4",
    "12D3KooWQ9xVxePsATagwKeEhPjjNxVESrqA36179YKGbi2KVCpn",
    "12D3KooWQNRwMvp9K9RF6bnLNmhWfwzmM5sMXXsHbSnD255GdEqq",
    "12D3KooWQtAGvDDgxkGmYzHBjofbtBRVP3zu18jJZ711E3YYnoUD",
    "12D3KooWQtuDcPbo4XM3vWFU228RwskwquLg9FxKKKMDCYiizo8u",
    "12D3KooWQzMSP65hMk4B3M8boeqt6fG3Uo6NweZSsLTGyFViPax5",
    "12D3KooWR1tiNYTFxKeUGMqEDdDdsC8UiBCajVWMjYda4Dy1iYoz",
    "12D3KooWR52HYG69STueKjb15rvNodWkP7oVKMhgRbCUCZ9Hw5Hh",
    "12D3KooWR52LhKYkcK5iDQyXBfnZ3bsUB6q9L8pcsDGxa4faLx7c",
    "12D3KooWRgAUM3V5irPt1V8Srj8VSGQRoQKM289fXaQH9ebk1qqK",
    "12D3KooWS5i46Lo8d49paEEwEE2pPmRrUWN8E3UTDtUVkkGj3AVN",
    "12D3KooWSDFxeb9DhC4kbSppBs77143dFWt9wkk3khTQjfd7tP94",
    "12D3KooWSyNaBqC5TMvCT7EATtgxCpM5vm9GCVtRCmCtKC3qbdrz",
];
