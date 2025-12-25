use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SparseSegment {
    /// A single isolated ID (e.g., 42)
    Single(u32),
    /// A continuous range [start, end], inclusive
    Range(u32, u32),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SparseU32List {
    /// Compressed segments of the original list
    pub segments: Vec<SparseSegment>,
}

impl SparseU32List {
    pub fn from_unsorted(mut ids: Vec<u32>) -> Self {
        ids.sort_unstable();
        Self::from_sorted(&ids)
    }
    pub fn from_sorted(ids: &[u32]) -> Self {
        let mut segments = Vec::new();
        let mut i = 0;
        while i < ids.len() {
            let start = ids[i];
            let mut end = start;
            while i + 1 < ids.len() && ids[i + 1] == ids[i] + 1 {
                i += 1;
                end = ids[i];
            }
            if start == end {
                segments.push(SparseSegment::Single(start));
            } else {
                segments.push(SparseSegment::Range(start, end));
            }
            i += 1;
        }
        Self { segments }
    }

    /// Decompress to Vec<u32>
    pub fn to_vec(&self) -> Vec<u32> {
        let mut out = Vec::new();
        for seg in &self.segments {
            match *seg {
                SparseSegment::Single(val) => out.push(val),
                SparseSegment::Range(start, end) => {
                    out.extend(start..=end);
                }
            }
        }
        out
    }
}

#[derive(Serialize, Clone, Debug, Default, Deserialize)]
pub struct BinBlob(#[serde(with = "serde_bytes")] pub Vec<u8>);

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
pub enum BinFormat {
    #[default]
    Parquet,
    MsgPack,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorldBinArchSnapshot {
    pub entities: SparseU32List,
    pub archetypes: Vec<BinBlob>,
    pub resources: HashMap<String, BinBlob>,
    pub format: BinFormat,
    pub meta: HashMap<String, String>,
}

impl WorldBinArchSnapshot {
    pub fn to_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }
}
