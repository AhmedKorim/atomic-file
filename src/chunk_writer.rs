use crate::AtomicFile;

pub struct ChunkWriter {
    pub file: AtomicFile,
    pub offset: u64,
}

impl ChunkWriter {
    pub fn write(&mut self, done_offset: u64, buf: &[u8]) {
        self.file.write(self.offset, buf);
        self.offset = self.offset + done_offset;
    }
}
