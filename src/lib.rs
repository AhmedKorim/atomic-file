use std::cmp::{max, min};
use std::fmt;
use std::fs::{create_dir, File, Metadata};
use std::i8::MAX;
use std::io::{Bytes, Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use errors::Error;

use crate::chunk_writer::ChunkWriter;
use crate::meta_data::AtomicFileMetaData;

mod meta_data;

mod chunk_writer;

mod errors;

// [ chunks | meta data | 8 bytes of size meta ]
pub struct AtomicFile {
    pub source: Arc<Mutex<File>>,
    pub chunks: Arc<Vec<ChunkWriter>>,
    // todo remove this
    pub meta_data: Arc<Option<AtomicFileMetaData>>,
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
    pub fn get_chunk_size(size: u64, chunks_count: usize) -> u64 {
        size / (chunks_count as u64)
    }
    fn init_chunks(&mut self) {
        if let Some(meta_data) = &*self.meta_data {
            let chunk_count = meta_data.chunks_count;
            let mut chunks = Vec::with_capacity(chunk_count);
            for chunk_index in 0..chunk_count {
                chunks.push(self.get_chunk_writer(meta_data.chunk_size * chunk_index as u64));
            }

            self.chunks = Arc::new(chunks);
        }
    }
    fn get_chunk_writer(&self, offset: u64) -> ChunkWriter {
        ChunkWriter {
            file: self.clone(),
            offset,
        }
    }
    fn get_chunk_writer_with_index(&self, index: usize) -> ChunkWriter {
        let meta_data = self.meta_data.unwrap();
        ChunkWriter {
            file: self.clone(),
            offset: meta_data.chunk_size * (index as u64),
        }
    }
    pub fn get_meta_data_from_file(&self) -> AtomicFileMetaData {
        let mut file = self.source.lock().unwrap();
        let file_length = file.metadata().unwrap().len();
        let meta_data_length_start = max(0, file_length - 8);
        dbg!(meta_data_length_start);
        dbg!(file_length);
        file.seek(SeekFrom::Start(meta_data_length_start)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        let mut data_bytes = [0u8; 8];
        data_bytes.copy_from_slice(&buf);
        let meta_data_size = u64::from_be_bytes(data_bytes);
        file.seek(SeekFrom::Start(file_length - 8 - meta_data_size))
            .unwrap();
        let mut buf_meta_data_with_size: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf_meta_data_with_size).unwrap();

        AtomicFileMetaData::decode(&buf_meta_data_with_size)
    }
    pub fn new_with_excited_file(file: File) -> AtomicFile {
        let mut atomic_file = AtomicFile::new(file);
        let meta_data = atomic_file.get_meta_data_from_file();
        atomic_file.meta_data = Arc::new(Some(meta_data));
        atomic_file.init_chunks();
        atomic_file
    }
    pub fn new_with_size(mut file: File, size: u64, chunks_count: usize) -> AtomicFile {
        let chunk_size = AtomicFile::get_chunk_size(size, chunks_count);
        let meta_data = AtomicFileMetaData::new(chunk_size, chunks_count);
        let encoded_meta_data = meta_data.encode();
        let final_size = encoded_meta_data.len() as u64 + size;
        let mut preload = vec![0u8; 2048];
        preload = [preload, encoded_meta_data].concat();
        file.set_len(final_size).unwrap();
        file.write_all(&preload).unwrap();
        let mut atomic_file = AtomicFile::new(file);
        atomic_file.meta_data = Arc::new(Some(meta_data));
        atomic_file.init_chunks();
        atomic_file
    }
    pub fn write(&mut self, offset: u64, buf: &[u8]) {
        let mut file = self.source.lock().unwrap();
        dbg!(offset);
        file.seek(SeekFrom::Start(offset))
            .expect("Error seeking the file");
        file.write_all(buf).expect("Failed to write file");
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs::{create_dir, File, OpenOptions};
    use std::io::Write;
    use std::path::Path;

    use crate::AtomicFile;

    #[test]
    fn set_file() {
        let temp = temp_dir().join("atomic-file");
        if !temp.exists() {
            create_dir(&temp);
        }
        let test_file = temp.join("testfile.text");
        dbg!(test_file.clone());

        let mut file: File;
        if test_file.exists() {
            dbg!("No");
            file = OpenOptions::new()
                .append(true)
                .read(true)
                .open(&test_file)
                .unwrap();
            dbg!(file.metadata().unwrap().len());
            let atomic_file = AtomicFile::new_with_excited_file(file);
            let meta_data = atomic_file.meta_data.unwrap();
            dbg!(meta_data);
        } else {
            file = File::create(&test_file).unwrap();

            let mut atomic_file = AtomicFile::new_with_size(file, 2048, 8);
            let mut chunks = vec![vec!(0u8; 2048 / 8); 8];
            for (index, chunk) in chunks.iter_mut().enumerate() {
                let mut writer = atomic_file.get_chunk_writer_with_index(index);
                writer.write(chunk.len() as u64, &chunk)
            }
            let file = atomic_file.source.lock().unwrap();
            let meta_data_length = atomic_file.meta_data.unwrap().encode().len();
            assert_eq!(
                file.metadata().unwrap().len(),
                meta_data_length as u64 + 2048
            );
        }
    }

    //
    #[test]
    fn read_meta_data_from_fs() {
        let temp = temp_dir().join("atomic-file");
        if !temp.exists() {
            create_dir(&temp);
        }
        let test_file = temp.join("testfile.text");
        dbg!(test_file.clone());

        let mut file: File;
        if test_file.exists() {
            dbg!("No");
            file = OpenOptions::new()
                .append(true)
                .read(true)
                .open(&test_file)
                .unwrap();
        } else {
            file = File::create(&test_file).unwrap();
        }
        let atomic_file = AtomicFile::new_with_excited_file(file);
        let meta_data = atomic_file.meta_data.unwrap();
        assert_eq!(meta_data.chunk_size, 2048 / 8);
        assert_eq!(meta_data.chunks_count, 8)
    }
}
