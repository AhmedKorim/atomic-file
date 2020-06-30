impl AtomicFile {
    pub fn new(file: File) -> AtomicFile {
        AtomicFile {
            source: Arc::new(Mutex::new(file)),
            chunks: Arc::new(vec![]),
            meta_data: Arc::new(None),
        }
    }
    pub fn new_with_known_chunk_count(
        file: File,
        chunk_count: usize,
        file_size: Option<u64>,
    ) -> AtomicFile {
        let atomic_file = AtomicFile::new(file);
        if let Some(f_size) = file_size {
            let file = atomic_file.source.lock().unwrap();
            file.set_len(f_size).expect("Failed to set file size");
        }
        atomic_file.init_with_chunk_count(chunk_count)
    }

    pub fn new_with_check_file_for_chunks(file: File) -> Result<AtomicFile, Error> {
        let atomic_file = AtomicFile::new(file);
        atomic_file
            .get_fs_chunk_count()
            .map(|count| atomic_file.init_with_chunk_count(count))
            .ok_or_else(|| Error::NoMetaData)
    }
    pub fn get_chunk_size(&self, chunk_count: u64) -> u64 {
        let file = self.source.lock().unwrap();
        let file_size = file.c().unwrap().len();
        (file_size as u64) / chunk_count
    }
    fn get_fs_meta(&self) -> Option<AtomicFileMetaData> {
        let raw_file_size = self.get_file_size_without_meta_data();
        let mut file = self.source.lock().unwrap();
        if file.metadata().unwrap().len() - 8 == 0 {
            return None;
        }
        file.seek(SeekFrom::Start(raw_file_size)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        let meta_data = AtomicFileMetaData::decode(&buf.as_mut_slice());
        Some(meta_data)
    }
    pub fn get_fs_chunk_count(&self) -> Option<usize> {
        let mut chunk_count = 0;
        match &*self.meta_data {
            None => {
                if let Some(meta_data) = self.get_fs_meta() {
                    return Some(meta_data.chunks_count);
                }
                return None;
            }
            Some(meta_data) => chunk_count = meta_data.chunks_count,
        }
        Some(chunk_count)
    }
    pub fn get_writen_chunks(&self, chunks_count: usize) -> Option<Vec<ChunkWriter>> {
        let chunk_size = self.get_chunk_size(chunks_count as u64);
        let mut chunks = Vec::with_capacity(chunks_count);
        for chunk_index in 0..chunks_count {
            let chunk_index = chunk_index as u64;
            let offset: u64 = chunk_index * chunk_size;
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
        file.seek(SeekFrom::Start(max(all_file_length - 8, 0)))
            .unwrap();
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
        file.write_all(encoded.as_slice()).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
    }
    fn init_with_chunk_count(mut self, chunk_count: usize) -> AtomicFile {
        if let Some(chunks) = self.get_writen_chunks(chunk_count) {
            self.chunks = Arc::new(chunks);
            self.set_meta_data();
            return self;
        }
        self.set_meta_data();
        self
    }
}
