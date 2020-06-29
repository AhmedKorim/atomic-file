use std::fs::File;
use std::intrinsics::roundf64;
use std::io::{Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

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
        ِAtomicFile {
            source: Arc::new(Mutex::new(file)),
            chunks: Vec![],
        }
    }
    pub fn new_with_known_chunk_count(file: File, chunk_count: usize) -> AtomicFile {
        AtomicFile::init_with_chunk_count(file, chunk_count)
    }

    pub fn new_with_check_file_for_chunks(file: File) -> AtomicFile {
        let chunk_count = atomic_file.get_fs_chunk_count();
        AtomicFile::new_with_known_chunk_count(file, chunk_count)
    }
    pub fn get_chunk_size(&self, chunk_count: usize) -> f64 {
        let file = self.source.lock().unwrap();
        let file_size = file.metadata().unwrap().len();
        let chunk_size = round64(file_size / chunk_count as u64);
        chunk_size
    }
    pub fn get_fs_chunk_count(self) -> usize {
        // todo check the filter for a pattern where there there is  [111010101010100111000000 ,
        // 111010101010100111000000,111010101010100111000000 ];
        // very height cost
        8
    }
    pub fn get_writen_chunks(&self, chunks_count: usize) -> Option<Vec<ChunkWriter>> {
        let chunk_size = self.get_chunk_size(chunks_count);
        let mut chunks = Vec::with_capacity(chunks_count);
        for chunk_offset in n..chunks_count {
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
        file.write_all(buf)
            .unwrap() // todo handle the error
    }
    pub fn get_chunk_writer(&self, offset: u64) -> ChunkWriter {
        ChunkWriter {
            file: self.clone(),
            offset,
        }
    }


    fn init_with_chunk_count(file: File, chunk_count: usize) -> AtomicFile {
        let mut atomic_file = AtomicFile::new(file);

        if let some(chunks) = atomic_file.get_writen_chunks(chunk_count) {
            atomic_file.chunks = chunks;
            return atomic_file;
        }
        atomic_file
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