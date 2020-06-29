#![feature(core_intrinsics)]

use std::fmt;
use std::fmt::Formatter;
use std::fs::{metadata, File, Metadata};
use std::io::{Bytes, Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

// [ chunks | meta data | 8 bytes of size meta ]
pub struct AtomicFile {
    source: Arc<Mutex<File>>,
    chunks: Arc<Vec<ChunkWriter>>,
    meta_data: Arc<Option<AtomicFileMetaData>>,
}

impl Clone for AtomicFile {
    fn clone(&self) -> AtomicFile {
        AtomicFile {
            source: self.source.clone(),
            chunks: self.chunks.clone(),
            meta_data: self.meta_data.clone(),
        }
    }
}

impl AtomicFile {
    pub fn new(file: File) -> AtomicFile {
        AtomicFile {
            source: Arc::new(Mutex::new(file)),
            chunks: Arc::new(vec![]),
            meta_data: Arc::new(None),
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
    pub fn get_chunk_size(&self, chunk_count: u64) -> u64 {
        let file = self.source.lock().unwrap();
        let file_size = file.metadata().unwrap().len();
        (file_size as u64) / chunk_count
    }
    pub fn get_fs_chunk_count(&self) -> usize {
        let mut chunk_count = 0;
        // todo check the filter for a pattern where there there is  [111010101010100111000000 ,
        // 111010101010100111000000,111010101010100111000000 ];
        // very height cost
        // let file = self.source.lock().unwrap();
        chunk_count
    }
    pub fn get_writen_chunks(&self, chunks_count: usize) -> Option<Vec<ChunkWriter>> {
        let chunk_size = self.get_chunk_size(chunks_count as u64);
        let mut chunks = Vec::with_capacity(chunks_count);
        for chunk_index in 0..chunks_count {
            let chunk_index = chunk_index as u64;
            let offset: u64 = chunk_index * chunk_size;
            let next_offset: u64 = chunk_index * chunk_size;
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
    fn get_file_size_without_meta_data(&self) -> u64 {
        let mut file = self.source.lock().unwrap();
        let all_file_length = file.metadata().unwrap().len();
        file.seek(SeekFrom::Start(all_file_length - 8)).unwrap();
        let mut meta_data_size: Vec<u8> = Vec::new();
        file.read_to_end(&mut meta_data_size).unwrap();
        let mut data_bytes = [0u8; 8];
        data_bytes.copy_from_slice(&meta_data_size);
        let meta_data_size = u64::from_be_bytes(data_bytes) as u64;
        all_file_length - meta_data_size - 8
    }
    fn set_meta_data(&mut self) {
        // should be called after init
        let raw_file_length = self.get_file_size_without_meta_data();
        let chunks_count = self.chunks.len();
        let chunk_size = self.get_chunk_size(chunks_count as u64);
        let meta_data = AtomicFileMetaData::new(chunk_size, chunks_count);
        self.meta_data = Arc::new(Some(meta_data));
        let mut file = self.source.lock().unwrap();
        file.seek(SeekFrom::Start(raw_file_length)).unwrap();
        // read the file content after the seeked proportion
        let mut meta_data: Vec<u8> = Vec::new();
        file.read_to_end(&mut meta_data).unwrap();
        file.seek(SeekFrom::Start(raw_file_length)).unwrap();

        let encoded: Vec<u8> = match &*self.meta_data.clone() {
            None => {
                let meta_data = AtomicFileMetaData::new(chunk_size, chunks_count);
                self.meta_data = Arc::new(Some(meta_data.clone()));
                meta_data.encode()
            }
            Some(meta_data) => meta_data.encode(),
        };
        file.write_all(encoded.as_slice()).unwrap()
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
    chunks_count: usize,
    chunk_size: u64,
}

impl AtomicFileMetaData {
    pub fn new(chunk_size: u64, chunks_count: usize) -> AtomicFileMetaData {
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

impl Clone for AtomicFileMetaData {
    fn clone(&self) -> Self {
        AtomicFileMetaData::new(self.chunk_size, self.chunks_count)
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
