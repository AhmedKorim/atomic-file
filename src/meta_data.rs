use bincode;
use serde::export::Formatter;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Copy)]
pub struct AtomicFileMetaData {
    pub chunks_count: usize,
    pub chunk_size: u64,
}

impl AtomicFileMetaData {
    pub fn new(chunk_size: u64, chunks_count: usize) -> AtomicFileMetaData {
        AtomicFileMetaData {
            chunks_count,
            chunk_size,
        }
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
