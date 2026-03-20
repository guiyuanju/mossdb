use crate::reader::CachedReader;
use crate::sparseindex::{self, SparseIndex};
use crate::types::{Key, Offset, Value};
use anyhow::Result;
use anyhow::anyhow;

#[derive(Debug)]
pub struct SSTable {
    sparse_index: SparseIndex,
    reader: CachedReader,
    pub filename: String,
}

impl SSTable {
    pub fn new(filename: String) -> Result<Self> {
        let mut reader = CachedReader::new(filename.clone());
        let index = reader.read_sparse_index()?;
        let sparseindex = SparseIndex::new(index);
        Ok(Self {
            sparse_index: sparseindex,
            reader: reader,
            filename,
        })
    }

    pub fn get(&mut self, key: &str) -> Result<String> {
        let block_offset = self
            .sparse_index
            .get_containing_block_offset(key)
            .ok_or(anyhow!("not found in current sstable"))?;

        self.reader.read_key(block_offset, key)
    }

    pub fn len(&self) -> usize {
        todo!()
    }
}
