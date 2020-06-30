use crate::AtomicFile;
use std::ops::DerefMut;

pub struct ChunkWriter {
    pub file: AtomicFile,
    pub offset: u64,
    pub key: usize,
}

impl ChunkWriter {
    pub fn write(&mut self, done_offset: u64, buf: &[u8]) {
        let next_offset = self.offset + done_offset;
        self.file.write(self.offset, buf, self.key, next_offset);
        self.offset = next_offset
    }
}
// impl Deref for ChunkWriter {}
// impl DerefMut for ChunkWriter {}
