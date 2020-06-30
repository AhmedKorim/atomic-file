use std::fmt;

use bincode;
use serde::export::Formatter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Serialize, Deserialize)]
pub struct AtomicFileMetaData {
    pub chunks_count: usize,
    pub chunk_size: u64,
    pub offsets: HashMap<usize, u64>,
}

impl AtomicFileMetaData {
    pub fn new(chunk_size: u64, chunks_count: usize) -> AtomicFileMetaData {
        AtomicFileMetaData {
            chunks_count,
            chunk_size,
            offsets: HashMap::new(),
        }
    }
    pub fn set_progress(&mut self, key: usize, offset: u64) {
        self.offsets.insert(key, offset);
    }

    pub fn encode(&self) -> Vec<u8> {
        let encoded: Vec<u8> = bincode::serialize(&self).unwrap();
        let data_length = encoded.len() as u64;
        [encoded, data_length.to_be_bytes().into()].concat()
    }
    pub(crate) fn decode(encoded_val: &[u8]) -> AtomicFileMetaData {
        let val: AtomicFileMetaData =
            bincode::deserialize(&encoded_val[0..encoded_val.len() - 8]).unwrap();
        val
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
