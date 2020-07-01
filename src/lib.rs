use std::borrow::Borrow;
use std::cmp::{max, min};
use std::fmt;
use std::fs::{create_dir, File, Metadata};
use std::i8::MAX;
use std::io::{Bytes, Cursor, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::{mpsc, Arc, Mutex, PoisonError, RwLock};

use serde::de::value::UsizeDeserializer;
use serde::{Deserialize, Serialize};

use errors::Error;

use crate::chunk_writer::ChunkWriter;
use crate::meta_data::AtomicFileMetaData;

mod meta_data;

mod chunk_writer;

mod errors;

#[derive(Clone)]
pub struct Chunk {
    data: Vec<u8>,
    start_offset: Option<u64>,
    index: usize,
}

// [ chunks | meta data | 8 bytes of size meta ]
pub struct AtomicFile {
    pub source: File,
    pub tx: mpsc::Sender<Chunk>,
    pub rx: mpsc::Receiver<Chunk>,
    pub meta_data: Arc<RwLock<Option<AtomicFileMetaData>>>,
}

impl AtomicFile {
    fn new(file: File) -> AtomicFile {
        let (tx, rx) = mpsc::channel();
        AtomicFile {
            source: file,
            tx,
            rx,
            meta_data: Arc::new(RwLock::new(None)),
        }
    }
    pub fn get_chunk_size(size: u64, chunks_count: usize) -> u64 {
        size / (chunks_count as u64)
    }

    pub fn get_meta_data_from_file(&self) -> Result<AtomicFileMetaData, Error> {
        let mut file = &self.source;
        let file_length = file.metadata()?.len();
        dbg!(file_length);
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
        Ok(atomic_file)
    }
    pub fn into_inner(self) -> Result<File, Error> {
        let file = self.source;
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
    pub fn get_meta_data(&self) -> Result<Option<AtomicFileMetaData>, Error> {
        let guard = self.meta_data.read().map_err(|_| Error::PoisonError)?;
        if let Some(meta_data) = guard.as_ref() {
            let m = AtomicFileMetaData {
                offsets: meta_data.offsets.clone(),
                chunk_size: meta_data.chunk_size,
                chunks_count: meta_data.chunks_count,
            };
            Ok(Some(m))
        } else {
            Ok(None)
        }
    }
    fn write(&self, offset: Option<u64>, buf: &[u8], chunk_index: usize) -> Result<(), Error> {
        let mut guard = self.meta_data.write().map_err(|_| Error::PoisonError)?;
        if let Some(meta_data) = guard.as_mut() {
            let mut file = &self.source;
            let offset: u64 = match offset {
                None => meta_data.get_progress(chunk_index).unwrap(),
                Some(offset) => offset,
            };
            let next_offset = offset + buf.len() as u64;
            let chunk_boundary = meta_data.chunk_size * (chunk_index as u64 + 1);
            if next_offset > chunk_boundary {
                println!(
                    "next_offset {} <  chunk_boundary {}",
                    &next_offset, &chunk_boundary
                );
                return Ok(());
            }
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(buf)?;
            let file_len = &file.metadata()?.len();
            let meta_data_encoded = meta_data.encode()?;
            file.seek(SeekFrom::Start(file_len - meta_data_encoded.len() as u64))?;
            meta_data.set_progress(chunk_index, next_offset);

            file.write_all(&meta_data_encoded)?;
            Ok(())
        } else {
            unreachable!("Meta data should be exists if we reached the write step")
        }
    }
    pub fn get_chunk_writer(&self) -> mpsc::Sender<Chunk> {
        self.tx.clone()
    }
    pub fn run(&self) -> Result<(), Error> {
        let rx = self.rx.try_iter();
        for chunk in rx {
            self.write(chunk.start_offset, &chunk.data, chunk.index)?
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use std::env::temp_dir;
    use std::fs::{create_dir, File, OpenOptions};
    use std::io::Write;
    use std::path::Path;
    use std::thread::{JoinHandle, Thread};
    use std::time::Duration;

    use crate::meta_data::AtomicFileMetaData;
    use crate::{AtomicFile, Chunk};

    #[test]
    fn set_file() {
        let temp = temp_dir().join("atomic-file");
        if !temp.exists() {
            create_dir(&temp);
        }
        let test_file = temp.join("data.data");
        dbg!(test_file.clone());
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();

        let mut file: File;
        let mut atomic_file: AtomicFile;
        if test_file.exists() {
            println!("file exists");
            file = OpenOptions::new()
                .append(true)
                .read(true)
                .open(&test_file)
                .unwrap();
            atomic_file = AtomicFile::new_with_excited_file(file).unwrap();
        } else {
            file = File::create(&test_file).unwrap();
            dbg!("create new");
            atomic_file = AtomicFile::new_with_size(file, 1024 * 8, 8).unwrap();
        }
        {
            let meta_data = atomic_file.meta_data.read().unwrap();
            let m = meta_data.as_ref().unwrap();
            dbg!(&m.offsets);
        }
        for i in 0..8 {
            let writer = atomic_file.get_chunk_writer();
            let handle = std::thread::spawn(move || {
                let data = vec![i as u8; 1024];
                data.chunks(8).for_each(move |chunk| {
                    std::thread::sleep(Duration::from_millis(20));
                    writer
                        .send(Chunk {
                            data: chunk.to_vec(),
                            start_offset: None,
                            index: i,
                        })
                        .unwrap();
                })
            });
            tasks.push(handle);
        }
        for i in tasks {
            i.join().unwrap();
        }
        atomic_file.run().unwrap();
        assert_eq!(1, 2);
    }

    #[test]
    #[ignore]
    fn read_meta_data_from_fs() {
        let temp = temp_dir().join("atomic-file");
        if !temp.exists() {
            create_dir(&temp);
        }
        let test_file = temp.join("data.data");
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
        let AtomicFileMetaData {
            chunk_size,
            chunks_count,
            offsets,
        } = atomic_file.get_meta_data().unwrap().unwrap();
        let meta_data_len = atomic_file.get_meta_data().unwrap().unwrap();
        assert_eq!(chunks_count as u64, 8);
        assert_eq!(chunks_count as u64, 8);
        dbg!(&meta_data_len);
        dbg!(&meta_data_len.offsets);
        assert_eq!(meta_data_len.encode().unwrap().len() as u64, 2);
    }
}
