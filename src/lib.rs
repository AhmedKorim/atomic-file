use std::borrow::Borrow;
use std::cmp::{max, min};
use std::fmt;
use std::fs::{create_dir, File, Metadata};
use std::i8::MAX;
use std::io::{Bytes, Cursor, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

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
    pub meta_data: Arc<RwLock<Option<AtomicFileMetaData>>>,
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
            meta_data: Arc::new(RwLock::new(None)),
        }
    }
    pub fn get_chunk_size(size: u64, chunks_count: usize) -> u64 {
        size / (chunks_count as u64)
    }
    fn init_chunks(&mut self) -> Result<(), Error> {
        let guard = self.meta_data.read().map_err(|_| Error::PoisonError)?;
        if let Some(meta) = guard.as_ref() {
            let chunk_count = meta.chunks_count;
            let mut chunks = Vec::with_capacity(chunk_count);
            for chunk_index in 0..chunk_count {
                chunks
                    .push(self.get_chunk_writer(meta.chunk_size * chunk_index as u64, chunk_index));
            }

            self.chunks = Arc::new(chunks);
        };
        Ok(())
    }
    fn get_chunk_writer(&self, offset: u64, key: usize) -> ChunkWriter {
        ChunkWriter {
            file: self.clone(),
            offset,
            key,
        }
    }
    fn get_chunk_writer_with_index(&self, index: usize) -> Result<ChunkWriter, Error> {
        let guard = self.meta_data.read().map_err(|_| Error::PoisonError)?;
        if let Some(meta_data) = guard.as_ref() {
            Ok(ChunkWriter {
                file: self.clone(),
                offset: meta_data.chunk_size * (index as u64),
                key: index,
            })
        } else {
            Err(Error::FailedToGenerateChunkWriter)
        }
    }
    pub fn get_meta_data_from_file(&self) -> Result<AtomicFileMetaData, Error> {
        let mut file = self.source.lock().map_err(|_| Error::PoisonError)?;
        let file_length = file.metadata()?.len();
        let meta_data_length_start = max(0, file_length - 8);
        dbg!(meta_data_length_start);
        dbg!(file_length);
        file.seek(SeekFrom::Start(meta_data_length_start))?;
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut data_bytes = [0u8; 8];
        data_bytes.copy_from_slice(&buf);
        let meta_data_size = u64::from_be_bytes(data_bytes);
        file.seek(SeekFrom::Start(file_length - 8 - meta_data_size))?;
        let mut buf_meta_data_with_size: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf_meta_data_with_size)?;
        AtomicFileMetaData::decode(&buf_meta_data_with_size)
    }
    pub fn new_with_excited_file(file: File) -> Result<AtomicFile, Error> {
        let mut atomic_file = AtomicFile::new(file);
        let meta_data = atomic_file.get_meta_data_from_file()?;
        atomic_file.meta_data = Arc::new(RwLock::new(Some(meta_data)));
        atomic_file.init_chunks()?;
        Ok(atomic_file)
    }
    pub fn new_with_size(
        mut file: File,
        size: u64,
        chunks_count: usize,
    ) -> Result<AtomicFile, Error> {
        let chunk_size = AtomicFile::get_chunk_size(size, chunks_count);
        let meta_data = AtomicFileMetaData::new(chunk_size, chunks_count);
        let encoded_meta_data = meta_data.encode()?;
        let final_size = encoded_meta_data.len() as u64 + size;
        let mut preload = vec![0u8; size as usize];
        preload = [preload, encoded_meta_data].concat();
        file.set_len(final_size)?;
        file.write_all(&preload)?;
        let mut atomic_file = AtomicFile::new(file);
        atomic_file.meta_data = Arc::new(RwLock::new(Some(meta_data)));
        atomic_file.init_chunks()?;
        Ok(atomic_file)
    }
    pub fn into_inner(self) -> Result<File, Error> {
        let file = Arc::try_unwrap(self.source)
            .map_err(|_| Error::PoisonError)?
            .into_inner()
            .map_err(|_| Error::PoisonError)?;
        Ok(file)
    }
    pub fn get_encoded_meta_data(&self) -> Result<Vec<u8>, Error> {
        let guard = self.meta_data.read().map_err(|_| Error::PoisonError)?;
        if let Some(meta_data) = guard.as_ref() {
            Ok(meta_data.encode()?)
        } else {
            Ok(Vec::new())
        }
    }
    pub fn write(
        &mut self,
        offset: u64,
        buf: &[u8],
        chunk_index: usize,
        next_offset: u64,
    ) -> Result<(), Error> {
        let mut file = self.source.lock().map_err(|_| Error::PoisonError)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf)?;
        let mut guard = self.meta_data.write().map_err(|_| Error::PoisonError)?;
        if let Some(meta_data) = guard.as_mut() {
            let file_len = file.metadata()?.len();
            file.seek(SeekFrom::Start(file_len - meta_data.encode()?.len() as u64))?;
            meta_data.set_progress(chunk_index, next_offset);

            let meta_data_encoded = meta_data.encode()?;
            file.write_all(&meta_data_encoded)?;
            Ok(())
        } else {
            unreachable!("Meta data should be exists if we reached the write step")
        }
        // }
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs::{create_dir, File, OpenOptions};
    use std::io::Write;
    use std::path::Path;

    use crate::meta_data::AtomicFileMetaData;
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
            let atomic_file = AtomicFile::new_with_excited_file(file).unwrap();
            let meta_data = atomic_file.meta_data;
            dbg!(meta_data);
        } else {
            file = File::create(&test_file).unwrap();

            let mut atomic_file = AtomicFile::new_with_size(file, 2048, 8).unwrap();
            let mut chunks = vec![vec!(0u8; 2048 / 8); 8];
            for (index, writer) in atomic_file.chunks.iter_mut().enumerate() {
                writer.write(chunks.len() as u64, &chunks[index])
            }
            let meta_data_length = atomic_file.get_encoded_meta_data().unwrap().len();
            let file = atomic_file.into_inner().unwrap();
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
        let atomic_file = AtomicFile::new_with_excited_file(file).unwrap();
        let atomic_meta = atomic_file.meta_data.read().unwrap();
        let AtomicFileMetaData {
            chunk_size,
            chunks_count,
            ..
        } = atomic_meta.as_ref().unwrap();

        assert_eq!(*chunk_size, 2048 / 8);
        assert_eq!(*chunks_count, 8);
    }
}
