use std::fs;
use std::sync::Mutex;

use crate::reader::CachedReader;
use crate::sparseindex::SparseIndex;
use anyhow::Result;
use anyhow::anyhow;
use log::error;
use log::info;

#[derive(Debug)]
pub struct SSTable {
    pub sparse_index: SparseIndex,
    reader: Mutex<CachedReader>, // TODO: remove mutex, lock free data structure? each read thread create its own cache?
    pub file_size: u64,
    pub filename: String,
}

impl Drop for SSTable {
    fn drop(&mut self) {
        match fs::remove_file(&self.filename) {
            Err(err) => error!("failed to remove sstable file {}: {:?}", self.filename, err),
            Ok(_) => info!("removed sstable file {}", self.filename),
        }
    }
}

impl SSTable {
    pub fn new(filename: &str) -> Result<Self> {
        let mut reader = CachedReader::new(filename.to_string());
        let index = reader.read_sparse_index()?;
        let file_size = reader.get_file_size()?;
        let sparseindex = SparseIndex::new(index);
        Ok(Self {
            sparse_index: sparseindex,
            reader: Mutex::new(reader),
            file_size,
            filename: filename.to_string(),
        })
    }

    /// return (value, deleted)
    pub fn get(&self, key: &str) -> Result<(String, bool)> {
        let block_offset = self
            .sparse_index
            .get_containing_block_offset(key)
            .ok_or(anyhow!("not found in current sstable"))?;

        let mut reader = self.reader.lock().unwrap();
        reader.read_key(block_offset, key)
    }

    pub fn dump(&self) {
        let mut guard = self.reader.lock().unwrap();
        for (_, offset) in &self.sparse_index.index {
            for (k, v, deleted) in guard.kv_block_iter(*offset).unwrap() {
                println!("key = `{}`, val = `{}`, deleted = {}", k, v, deleted);
            }
        }
    }
}

// a iterator for sstable file owning its own cache
