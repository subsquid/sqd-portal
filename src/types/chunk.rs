use core::str;
use std::{
    fmt::{Debug, Display, Formatter},
    str::FromStr,
};

use super::{BlockRange, DatasetId};

pub type BlockNumber = u64;

const HASH_MAX_LEN: usize = 8;

/// Chunk ID which uniquely defines chunk in the dataset
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DataChunk {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub top_dir: BlockNumber,
    last_hash: [u8; HASH_MAX_LEN],
}

impl DataChunk {
    pub fn block_range(&self) -> BlockRange {
        self.first_block..=self.last_block
    }

    pub fn range_msg(&self) -> sqd_messages::Range {
        sqd_messages::Range {
            begin: self.first_block,
            end: self.last_block,
        }
    }
}

impl FromStr for DataChunk {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // "0000000000/0000000000-0000000000-xxxxx[xxx]"
        const BLOCK_NUM_LEN: usize = 10;
        const HASH_MIN_LEN: usize = 5;
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
        let hash_slice = s[SEP2_POS + 1..].as_bytes();
        let mut last_hash = [0; HASH_MAX_LEN];
        last_hash[..hash_slice.len()].copy_from_slice(hash_slice);

        Ok(Self {
            first_block,
            last_block,
            top_dir,
            last_hash,
        })
    }
}

impl Display for DataChunk {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let hash_len = self
            .last_hash
            .iter()
            .position(|&ch| ch == 0)
            .unwrap_or(HASH_MAX_LEN);
        let hash = str::from_utf8(&self.last_hash[..hash_len]).unwrap();
        write!(
            f,
            "{:010}/{:010}-{:010}-{}",
            self.top_dir, self.first_block, self.last_block, hash
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
}
