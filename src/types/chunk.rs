use core::str;
use std::{
    fmt::{Debug, Display, Formatter},
    str::FromStr,
};

use super::{BlockRange, DatasetId};

pub type BlockNumber = u64;

const HASH_MAX_LEN: usize = 8;
const HASH_MIN_LEN: usize = 5;

/// Chunk ID which uniquely defines chunk in the dataset
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DataChunk {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    top_dir: BlockNumber,
    // TODO: use `SID` from the common library
    last_hash: [u8; HASH_MAX_LEN],
    /// Which copy of the chunk workers serve. Only the portal artifact carries one; a chunk read
    /// from the legacy artifact, or parsed from an id, is 0 — the ingested copy, and the value a
    /// query leaves off the wire.
    ///
    /// Counted by the derived `PartialEq`/`Ord` above, deliberately: two copies of one range are
    /// different bytes on the workers, so they are different chunks. Note that [`Display`] and
    /// [`FromStr`] do not carry it, so a chunk that round-trips through its id comes back as
    /// copy 0 and stops comparing equal to itself.
    version: u32,
}

impl DataChunk {
    /// Builds a chunk from fields a source already holds separately, for callers that would
    /// otherwise format them into an id only to parse them straight back out.
    ///
    /// `None` if the hash cannot appear in an id, since [`Display`] would then produce something
    /// [`FromStr`] rejects and a worker would not recognise.
    pub fn new(
        top_dir: BlockNumber,
        first_block: BlockNumber,
        last_block: BlockNumber,
        hash: &str,
    ) -> Option<Self> {
        let bytes = hash.as_bytes();
        if !(HASH_MIN_LEN..=HASH_MAX_LEN).contains(&bytes.len()) {
            return None;
        }
        let mut last_hash = [0; HASH_MAX_LEN];
        last_hash[..bytes.len()].copy_from_slice(bytes);
        Some(Self {
            first_block,
            last_block,
            top_dir,
            last_hash,
            version: 0,
        })
    }

    /// The ingested copy is 0, which is also what an id alone can say — an id names no version.
    #[must_use]
    pub fn with_version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn block_range(&self) -> BlockRange {
        self.first_block..=self.last_block
    }

    pub fn range_msg(&self) -> sqd_messages::Range {
        sqd_messages::Range {
            begin: self.first_block,
            end: self.last_block,
        }
    }

    pub fn last_hash(&self) -> &str {
        let hash_len = self
            .last_hash
            .iter()
            .position(|&ch| ch == 0)
            .unwrap_or(HASH_MAX_LEN);
        str::from_utf8(&self.last_hash[..hash_len]).unwrap()
    }
}

impl FromStr for DataChunk {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // "0000000000/0000000000-0000000000-xxxxx[xxx]"
        const BLOCK_NUM_LEN: usize = 10;
        const SLASH_POS: usize = BLOCK_NUM_LEN;
        const SEP1_POS: usize = BLOCK_NUM_LEN + 1 + BLOCK_NUM_LEN;
        const SEP2_POS: usize = BLOCK_NUM_LEN + 1 + BLOCK_NUM_LEN + 1 + BLOCK_NUM_LEN;
        const MIN_LEN: usize = BLOCK_NUM_LEN * 3 + HASH_MIN_LEN + 3;
        const MAX_LEN: usize = BLOCK_NUM_LEN * 3 + HASH_MAX_LEN + 3;

        let bytes = s.as_bytes();
        anyhow::ensure!(bytes.len() >= MIN_LEN, "string is too short");
        anyhow::ensure!(bytes.len() <= MAX_LEN, "string is too long");
        anyhow::ensure!(bytes[SLASH_POS] == b'/', "no '/' at required pos");
        anyhow::ensure!(
            bytes[SEP1_POS] == b'-' && bytes[SEP2_POS] == b'-',
            "no '-' at required pos"
        );
        let top_dir = s[0..SLASH_POS].parse()?;
        let first_block = s[SLASH_POS + 1..SEP1_POS].parse()?;
        let last_block = s[SEP1_POS + 1..SEP2_POS].parse()?;
        let hash_slice = &s.as_bytes()[SEP2_POS + 1..];
        let mut last_hash = [0; HASH_MAX_LEN];
        last_hash[..hash_slice.len()].copy_from_slice(hash_slice);

        Ok(Self {
            first_block,
            last_block,
            top_dir,
            last_hash,
            version: 0,
        })
    }
}

impl Display for DataChunk {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:010}/{:010}-{:010}-{}",
            self.top_dir,
            self.first_block,
            self.last_block,
            self.last_hash()
        )
    }
}

impl Debug for DataChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

/// Globally unique data chunk ID
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChunkId {
    pub dataset: DatasetId,
    pub chunk: DataChunk,
}

impl ChunkId {
    pub fn new(dataset: impl Into<DatasetId>, chunk: DataChunk) -> Self {
        Self {
            dataset: dataset.into(),
            chunk,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::DataChunk;

    #[test]
    fn test_data_chunk() {
        let chunk_str = "0000001000/0000001024-0000002047-0xabcdef";
        let chunk = DataChunk::from_str(chunk_str).unwrap();
        assert_eq!(chunk.first_block, 1024);
        assert_eq!(chunk.last_block, 2047);
        assert_eq!(chunk.top_dir, 1000);
        assert_eq!(&chunk.last_hash, "0xabcdef".as_bytes());
        assert_eq!(chunk.block_range(), 1024..=2047);
        assert_eq!(chunk.to_string(), chunk_str);

        let chunk_str = "0000000000/0000001024-0000002047-abcde";
        assert_eq!(
            chunk_str.parse::<DataChunk>().unwrap().to_string(),
            chunk_str
        );
    }

    #[test]
    fn test_data_chunk_new_matches_parsing() {
        let chunk_str = "0000001000/0000001024-0000002047-0xabcdef";

        let built = DataChunk::new(1000, 1024, 2047, "0xabcdef").unwrap();

        assert_eq!(built, DataChunk::from_str(chunk_str).unwrap());
        assert_eq!(built.to_string(), chunk_str);
    }

    #[test]
    fn test_data_chunk_new_rejects_hashes_an_id_cannot_carry() {
        assert!(DataChunk::new(0, 0, 1, "abcd").is_none());
        assert!(DataChunk::new(0, 0, 1, "abcdefghi").is_none());
    }
}
