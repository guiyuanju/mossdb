use anyhow::{Context, Result, anyhow, bail};
use log::{Level, log};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use crate::layout::{
    BLOCK_SIZE_BYTES, Block, Entry, MetaData, SPARSE_INDEX_COUNT_PER_BLOCK,
    SPARSE_INDEX_ENTRY_BYTE_LEN, SparseIndexEntry,
};

#[derive(Debug)]
pub struct CachedReader {
    cached_block: Block,
    has_data_in_cache: bool, // TODO: rmv flag, use some type safe way, unsafe may needed
    block_offset: u64,
    filename: String,
}

impl CachedReader {
    pub fn new(filename: String) -> Self {
        Self {
            cached_block: Block::new(),
            has_data_in_cache: false,
            block_offset: 0,
            filename,
        }
    }

    pub fn read_key(&mut self, block_offset: u64, key: &str) -> Result<String> {
        if !self.has_data_in_cache
            || self.block_offset > block_offset
            || block_offset >= self.block_offset + self.cached_block.len() as u64
        {
            self.load_block_to_cache(block_offset)?;
            self.block_offset = block_offset;
        }

        let mut cur = 0_usize;
        while cur < self.cached_block.len() {
            let entry = Entry::new(&mut self.cached_block.inner[cur..]);
            let Some((k, v, entry_len)) = entry.retrive_kv() else {
                break;
            };
            let cur_key = String::from_utf8_lossy(k).to_string();
            if key == cur_key {
                return Ok(String::from_utf8_lossy(v).to_string());
            }
            cur += entry_len;
        }

        // TODO: add a jump array to block to accelerate search speed in one block
        bail!("key not found in current block");
    }

    pub fn read_sparse_index(&mut self) -> Result<Vec<(String, u64)>> {
        if !self.has_data_in_cache || self.block_offset != 0 {
            self.load_block_to_cache(0)?;
            self.block_offset = 0;
        }
        let meta = MetaData::new(&mut self.cached_block.inner);
        let mut cur_offset = meta.retrieve_sparse_index_block_start_offset();
        let data_block_start_offset = meta.retrieve_data_block_start_offset();
        let mut res: Vec<(String, u64)> = vec![];
        let mut has_more_data = true;
        while cur_offset < data_block_start_offset && has_more_data {
            if !self.has_data_in_cache || self.block_offset != cur_offset {
                self.load_block_to_cache(cur_offset)?;
                self.block_offset = cur_offset;
            }

            for i in 0..SPARSE_INDEX_COUNT_PER_BLOCK {
                let sparse_index_entry = SparseIndexEntry::new(
                    &mut self.cached_block.inner[(i * SPARSE_INDEX_ENTRY_BYTE_LEN)..],
                );
                let Some(key) = sparse_index_entry.retrieve_key() else {
                    has_more_data = false;
                    break;
                };
                let offset = sparse_index_entry.retrieve_offset();
                res.push((key, offset));
            }

            cur_offset += BLOCK_SIZE_BYTES as u64;
        }

        Ok(res)
    }

    fn load_block_to_cache(&mut self, start: u64) -> Result<()> {
        let mut file = OpenOptions::new().read(true).open(&self.filename)?;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut self.cached_block.inner[..])
            .context("failed to read block")?;
        self.has_data_in_cache = true;
        Ok(())
    }
}
