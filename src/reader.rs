use anyhow::{Result, bail};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use crate::layout::{
    BLOCK_SIZE_BYTES, Block, Entry, KEY_LEN_BYTES, KeyValueMeta, Len, MetaData,
    SPARSE_INDEX_COUNT_PER_BLOCK, SPARSE_INDEX_ENTRY_BYTE_LEN, SPARSE_INDEX_START_OFFSET,
    SparseIndexEntry, VAL_LEN_BYTES,
};
use crate::sparseindex::SparseIndex;
use crate::types::{Key, Offset};

#[derive(Debug)]
pub struct CachedReader {
    cached_block: Option<Block>,
    block_offset: u64,
    filename: String,
}

impl CachedReader {
    pub fn new(filename: String) -> Self {
        Self {
            cached_block: None,
            block_offset: 0,
            filename: filename,
        }
    }

    pub fn read_key(&mut self, block_offset: u64, key: &str) -> Result<String> {
        if self.cached_block.is_none()
            || self.block_offset > block_offset
            || block_offset >= self.block_offset + self.cached_block.as_ref().unwrap().len() as u64
        {
            self.load_block_to_cache(block_offset)?;
            self.block_offset = block_offset;
        }

        let Some(block) = &mut self.cached_block else {
            panic!("block not cahced");
        };

        let mut cur = 0 as usize;
        while cur < block.len() {
            let entry = Entry::new(&mut block.inner[cur..]);
            cur += entry.retieve_entry_len();
            let (k, v) = entry.retrive_kv();
            let cur_key = String::from_utf8_lossy(k).to_string();
            if key == cur_key {
                return Ok(String::from_utf8_lossy(v).to_string());
            }
        }

        // TODO: add a jump array to block to accelerate search speed in one block
        bail!("key not found in current block");
    }

    pub fn read_sparse_index(&mut self) -> Result<Vec<(String, u64)>> {
        if self.cached_block.is_none() || self.block_offset != 0 {
            self.load_block_to_cache(0)?;
            self.block_offset = 0;
        }
        let meta = MetaData::new(&mut self.cached_block.as_mut().unwrap().inner);
        let mut cur_offset = meta.retrieve_sparse_index_block_start_offset();
        let data_block_start_offset = meta.retrieve_data_block_start_offset();
        let mut res: Vec<(String, u64)> = vec![];
        while cur_offset < data_block_start_offset {
            if self.cached_block.is_none() || self.block_offset != cur_offset {
                self.load_block_to_cache(cur_offset)?;
                self.block_offset = cur_offset;
            }

            let Some(block) = &mut self.cached_block else {
                panic!("cache is none");
            };

            for i in 0..SPARSE_INDEX_COUNT_PER_BLOCK {
                let sparse_index_entry =
                    SparseIndexEntry::new(&mut block.inner[(i * SPARSE_INDEX_ENTRY_BYTE_LEN)..]);
                let key = sparse_index_entry.retrieve_key();
                let offset = sparse_index_entry.retrieve_offset();
                res.push((key, offset));
            }

            cur_offset += BLOCK_SIZE_BYTES as u64;
        }

        Ok(res)
    }

    fn load_block_to_cache(&self, start: u64) -> Result<()> {
        todo!()
    }
}
