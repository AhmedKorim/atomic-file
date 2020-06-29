#![feature(core_intrinsics)]

use std::fmt;
use std::fmt::Formatter;
use std::fs::{File, Metadata};
use std::io::{Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

pub struct AtomicFile {
    source: Arc<Mutex<File>>,
    chunks: Arc<Vec<ChunkWriter>>,
}

impl Clone for AtomicFile {
    fn clone(&self) -> AtomicFile {
        AtomicFile {
            source: self.source.clone(),
            chunks: self.chunks.clone(),
        }
    }
}

impl AtomicFile {
    pub fn new(file: File) -> AtomicFile {
        AtomicFile {
            source: Arc::new(Mutex::new(file)),
            chunks: Arc::new(vec![]),
        }
    }
    pub fn new_with_known_chunk_count(file: File, chunk_count: usize) -> AtomicFile {
        let atomic_file = AtomicFile::new(file);
        atomic_file.init_with_chunk_count(chunk_count)
    }

    pub fn new_with_check_file_for_chunks(file: File) -> AtomicFile {
        let atomic_file = AtomicFile::new(file);
        let chunk_count = atomic_file.get_fs_chunk_count();
        atomic_file.init_with_chunk_count(chunk_count)
    }
    pub fn get_chunk_size(&self, chunk_count: usize) -> f64 {
        let file = self.source.lock().unwrap();
        let file_size = file.metadata().unwrap().len();
        file_size as f64 / chunk_count as f64
    }
    pub fn get_fs_chunk_count(&self) -> usize {
        let mut chunk_count = 0;
        // todo check the filter for a pattern where there there is  [111010101010100111000000 ,
        // 111010101010100111000000,111010101010100111000000 ];
        // very height cost
        let file = self.source.lock().unwrap();
        chunk_count
    }
    pub fn get_writen_chunks(&self, chunks_count: usize) -> Option<Vec<ChunkWriter>> {
        let chunk_size = self.get_chunk_size(chunks_count);
        let mut chunks = Vec::with_capacity(chunks_count);
        for chunk_offset in 0..chunks_count {
            let offset = chunk_offset as u64 * chunk_size as u64;
            let next_offset = chunk_offset as u64 * chunk_size as u64;
            // todo find none zero bites between the offset and the next offset
            let bytes_written_in_chunk: u64 = 0;
            let offset = offset + bytes_written_in_chunk;
            chunks.push(self.get_chunk_writer(offset));
        }
        Some(chunks)
    }
    pub fn write(&mut self, offset: u64, buf: &[u8]) {
        let mut file = self.source.lock().unwrap();
        file.seek(SeekFrom::Start(offset))
            .expect("Error seeking the file");
        file.write_all(buf).unwrap() // todo handle the error
    }

    // private
    fn get_chunk_writer(&self, offset: u64) -> ChunkWriter {
        ChunkWriter {
            file: self.clone(),
            offset,
        }
    }

    fn init_with_chunk_count(mut self, chunk_count: usize) -> AtomicFile {
        if let Some(chunks) = self.get_writen_chunks(chunk_count) {
            self.chunks = Arc::new(chunks);
            return self;
        }
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
struct AtomicFileMetaData {
    chunks_count: u8,
    chunk_size: f64,
}

impl AtomicFileMetaData {
    pub fn new(chunk_size: f64, chunks_count: u8) -> AtomicFileMetaData {
        AtomicFileMetaData {
            chunks_count,
            chunk_size,
        }
    }
    fn encode(&self) -> Vec<u8> {
        let encoded: Vec<u8> = bincode::serialize(&self).unwrap();
        let data_length = encoded.len() as u64;
        [encoded, data_length.to_be_bytes().into()].concat()
    }
    fn decode(encoded_val: &[u8]) -> AtomicFileMetaData {
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

pub struct ChunkWriter {
    file: AtomicFile,
    offset: u64,
}

impl ChunkWriter {
    pub fn write(&mut self, done_offset: u64, buf: &[u8]) {
        self.file.write(self.offset + done_offset, buf)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
