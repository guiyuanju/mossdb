use anyhow::{Context, Result, bail};
use std::fmt;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;

use crate::layout::{
    BLOCK_SIZE_BYTES, Block, KVBlockIter, MetaData, SPARSE_INDEX_COUNT_PER_BLOCK,
    SPARSE_INDEX_ENTRY_BYTE_LEN, SparseIndexEntry,
};

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

    pub fn kv_block_iter(&mut self, block_offset: u64) -> Result<KVBlockIter<'_>> {
        if !self.has_data_in_cache || self.block_offset != block_offset {
            self.load_block_to_cache(block_offset)?;
            self.block_offset = block_offset;
        }
        Ok(self.cached_block.kv_iter())
    }

    pub fn get_file_size(&self) -> Result<u64> {
        let file = OpenOptions::new().read(true).open(&self.filename)?;
        Ok(file.metadata()?.size())
    }

    // (value, deleted)
    pub fn read_key(&mut self, block_offset: u64, key: &str) -> Result<(String, bool)> {
        for (k, v, deleted) in self.kv_block_iter(block_offset)? {
            if k == key {
                return Ok((v, deleted));
            }
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

impl fmt::Debug for CachedReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedReader")
            .field("cached_block", &"hidden for less space")
            .field("has_data_in_cache", &self.has_data_in_cache)
            .field("block_offset", &self.block_offset)
            .field("filename", &self.filename)
            .finish()
    }
}
