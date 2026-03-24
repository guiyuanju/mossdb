use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::Mutex;
use std::sync::RwLock;

use crate::layout::BLOCK_SIZE_BYTES;
use crate::layout::Block;
use crate::layout::KVBlockIter;
use crate::layout::KVEntryReader;
use crate::reader::CachedReader;
use crate::sparseindex;
use crate::sparseindex::SparseIndex;
use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
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

    pub fn get(&self, key: &str) -> Result<String> {
        let block_offset = self
            .sparse_index
            .get_containing_block_offset(key)
            .ok_or(anyhow!("not found in current sstable"))?;

        let mut reader = self.reader.lock().unwrap();
        reader.read_key(block_offset, key)
    }

    pub fn cached_iter(&self) -> Result<SSTableCachedIter> {
        let file = OpenOptions::new().read(true).open(&self.filename)?;
        Ok(SSTableCachedIter {
            file,
            block: Block::new(),
            block_offset: 0,
            offset_in_block: 0,
            filled: false,
        })
    }
}

// a iterator for sstable file owning its own cache
pub struct SSTableCachedIter {
    file: File,
    block: Block,
    block_offset: usize,
    offset_in_block: usize,
    filled: bool,
}

impl Iterator for SSTableCachedIter {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.filled || self.offset_in_block >= self.block.len() {
            self.load_next_block().ok()?;
            self.offset_in_block = 0;
            self.filled = true;
        }
        let kv_reader = KVEntryReader::new(&self.block.inner[self.offset_in_block..]);
        let (k, v, length) = kv_reader.retrive_kv()?;
        self.offset_in_block += length;
        Some((
            String::from_utf8_lossy(k).to_string(),
            String::from_utf8_lossy(v).to_string(),
        ))
    }
}

impl SSTableCachedIter {
    fn load_next_block(&mut self) -> Result<()> {
        self.block_offset += BLOCK_SIZE_BYTES;
        self.load_block(self.block_offset)
    }

    fn load_block(&mut self, offset: usize) -> Result<()> {
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file
            .read_exact(&mut self.block.inner[..])
            .context("failed to read block")?;
        Ok(())
    }
}
