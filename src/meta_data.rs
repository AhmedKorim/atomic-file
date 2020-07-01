use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

use bincode;
use serde::export::Formatter;
use serde::{Deserialize, Serialize};

use crate::errors::Error;

#[derive(Serialize, Deserialize)]
pub struct AtomicFileMetaData {
    pub chunks_count: usize,
    pub chunk_size: u64,
    pub offsets: HashMap<usize, u64>,
}

impl AtomicFileMetaData {
    pub fn new(chunk_size: u64, chunks_count: usize) -> AtomicFileMetaData {
        let v: Vec<u64> = (0..chunks_count).map(|i| i as u64 * chunk_size).collect();
        let mut offsets: HashMap<usize, u64> = HashMap::new();
        for (i, value) in v.iter().enumerate() {
            offsets.insert(i, value.clone());
        }
        AtomicFileMetaData {
            chunks_count,
            chunk_size,
            offsets,
        }
    }
    pub fn set_progress(&mut self, key: usize, offset: u64) {
        self.offsets.insert(key, offset);
    }
    pub fn get_progress(&self, key: usize) -> Option<u64> {
        self.offsets.get(&key).map(|d| d.clone())
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let encoded: Vec<u8> = bincode::serialize(&self)?;
        let data_length = encoded.len() as u64;
        Ok([encoded, data_length.to_be_bytes().into()].concat())
    }
    pub(crate) fn decode(encoded_val: &[u8]) -> Result<AtomicFileMetaData, Error> {
        let val: AtomicFileMetaData = bincode::deserialize(&encoded_val[0..encoded_val.len() - 8])?;
        Ok(val)
    }
}

impl fmt::Debug for AtomicFileMetaData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AtomicFileMetaData")
            .field("chunk_size", &self.chunk_size)
            .field("chunks_count", &self.chunks_count)
            .finish()
    }
}

impl Clone for AtomicFileMetaData {
    fn clone(&self) -> Self {
        AtomicFileMetaData::new(self.chunk_size, self.chunks_count)
    }
}
