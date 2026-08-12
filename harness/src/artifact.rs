//! Assignment artifact construction (DC-2 input): the gzipped FlatBuffer the
//! publisher stub serves, built with `sqd-assignments`' builder — the same
//! crate/rev the portal reads with.

use std::io::Write;

use flate2::{write::GzEncoder, Compression};
use libp2p_identity::PeerId;
use sqd_assignments::{AssignmentBuilder, PortalAssignmentBuilder};

use crate::world::ToyWorld;

/// Which artifact the portal under test is configured to route from, mirroring its own
/// `assignment_source`. The publisher stub always serves both — that is the migration state
/// the portal has to work in — so this only picks which one the portal is pointed at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentSource {
    Legacy,
    Portal,
}

impl AssignmentSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Portal => "portal",
        }
    }
}

/// Build the artifact assigning every archival chunk to every worker, then
/// gzip it. The portal pre-leases `1 + retries` *distinct* workers per chunk,
/// so a realistic world needs at least two.
pub fn build_gzipped(world: &ToyWorld, workers: &[PeerId]) -> anyhow::Result<Vec<u8>> {
    // The builder requires workers in ascending PeerId order.
    let mut workers: Vec<PeerId> = workers.to_vec();
    workers.sort();
    let worker_indexes: Vec<u16> = (0..workers.len() as u16).collect();

    let mut b = AssignmentBuilder::new("conformance-secret");

    let mut chunk_indexes: Vec<u32> = Vec::new();
    let mut next_index: u32 = 0;
    for ds in &world.datasets {
        let Some(network_id) = &ds.network_id else {
            continue;
        };
        for chunk in &ds.chunks {
            b.new_chunk()
                .id(&chunk.id(&ds.name))
                .dataset_id(network_id)
                .dataset_base_url(&format!("https://{}.example.invalid", ds.name))
                .block_range(chunk.first..=chunk.last)
                .size(1_000)
                .worker_indexes(&worker_indexes)
                .last_block_hash(&world.hash(&ds.name, chunk.last))
                .last_block_timestamp(world.timestamp(chunk.last))
                .files(&["blocks.parquet".to_string()])
                .finish()
                .map_err(|e| anyhow::anyhow!("chunk build: {e}"))?;
            chunk_indexes.push(next_index);
            next_index += 1;
        }
        b.finish_dataset();
    }

    for worker in &workers {
        b.add_worker_with_timestamp(
            *worker,
            sqd_assignments::WorkerStatus::Ok,
            &chunk_indexes,
            1_700_000_000,
        );
    }

    gzip(b.finish())
}

/// The same world in the portal-oriented format. It carries no download or auth fields at
/// all, so what the portal routes from here is strictly less than the legacy artifact holds —
/// which is the point of running the smoke against both.
pub fn build_portal_gzipped(world: &ToyWorld, workers: &[PeerId]) -> anyhow::Result<Vec<u8>> {
    let mut workers: Vec<PeerId> = workers.to_vec();
    workers.sort();
    let worker_indexes: Vec<u16> = (0..workers.len() as u16).collect();

    let mut b = PortalAssignmentBuilder::new();

    for ds in &world.datasets {
        let Some(network_id) = &ds.network_id else {
            continue;
        };
        let mut head_hash = None;
        for chunk in &ds.chunks {
            b.new_chunk()
                .id(&chunk.id(&ds.name))
                .dataset_id(network_id)
                .block_range(chunk.first..=chunk.last)
                .last_block_timestamp(world.timestamp(chunk.last))
                .worker_indexes(&worker_indexes)
                .finish()
                .map_err(|e| anyhow::anyhow!("chunk build: {e}"))?;
            head_hash = Some(world.hash(&ds.name, chunk.last));
        }
        // Only the dataset's head hash survives the split; per-chunk hashes were dropped
        // because no query needs one. Schema ids are inert — the portal doesn't read them yet.
        b.finish_dataset(0, head_hash.as_deref());
    }

    for worker in &workers {
        b.add_worker(*worker, sqd_assignments::WorkerStatus::Ok);
    }

    gzip(b.finish())
}

fn gzip(bytes: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(&bytes)?;
    Ok(enc.finish()?)
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    use crate::keys;

    #[test]
    fn artifact_roundtrips_through_the_reader() {
        let dir = tempfile::tempdir().unwrap();
        let a = keys::generate(&dir.path().join("a")).unwrap();
        let b = keys::generate(&dir.path().join("b")).unwrap();
        let world = ToyWorld::standard();
        let gz = build_gzipped(&world, &[a.peer_id, b.peer_id]).unwrap();

        let mut raw = Vec::new();
        flate2::read::MultiGzDecoder::new(gz.as_slice())
            .read_to_end(&mut raw)
            .unwrap();
        let assignment = sqd_assignments::Assignment::from_owned_unchecked(raw);

        assert_eq!(assignment.workers().len(), 2);
        let chunk = assignment.find_chunk("s3://toy-dataset", 0).unwrap();
        let indexes: Vec<u16> = chunk.worker_indexes().iter().collect();
        assert_eq!(indexes, vec![0, 1], "chunk 0 worker_indexes");
        for idx in [0u16, 1] {
            let worker = assignment.get_worker_by_index(idx);
            assert_eq!(worker.status(), sqd_assignments::WorkerStatus::Ok);
            let peer = worker.peer_id().unwrap();
            assert!(peer == a.peer_id || peer == b.peer_id);
        }

        let mid = assignment.find_chunk("s3://toy-dataset", 85).unwrap();
        let indexes: Vec<u16> = mid.worker_indexes().iter().collect();
        assert_eq!(indexes, vec![0, 1], "chunk 2 worker_indexes");
    }
}
