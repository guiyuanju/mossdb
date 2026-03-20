use crate::reader::Reader;
use crate::sparseindex::SparseIndex;
use crate::types::{Key, Offset, Value};
use anyhow::Result;
use anyhow::anyhow;

#[derive(Debug)]
pub struct SSTable {
    sparse_index: SparseIndex,
    reader: Reader,
    pub filename: String,
}

impl SSTable {
    pub fn new(filename: String) -> Self {
        Self {
            sparse_index: SparseIndex::new(vec![]),
            reader: Reader::new(),
            filename,
        }
    }

    pub fn get(&mut self, key: &str) -> Result<String> {
        let offset_range = self
            .sparse_index
            .get_closed_indice(key)
            .ok_or(anyhow!("not found in current sstable"))?;

        let mut cur_offset = offset_range.0;
        while cur_offset < offset_range.1 {
            let meta = self.reader.readMeta(&self.filename, cur_offset)?;

            let cur_key = self
                .reader
                .read(&self.filename, meta.key_offset, meta.key_len)?;

            // found the key, read and return the val
            if cur_key == key.as_bytes() {
                let cur_val = self
                    .reader
                    .read(&self.filename, meta.val_offset, meta.val_len)?;
                return Ok(String::from_utf8_lossy(&cur_val).to_string());
            }

            // not found, continue
            cur_offset = meta.val_offset + meta.val_len;
        }

        // not found
        Err(anyhow!("not found in current sstable"))
    }

    pub fn len(&self) -> usize {
        todo!()
    }
}
