use anyhow::{Result, bail};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use crate::layout::{Block, KEY_LEN_BYTES, KeyValueMeta, Len, VAL_LEN_BYTES};
use crate::types::{Key, Offset};

#[derive(Debug)]
pub struct Reader {
    cached_block: Option<Block>,
    block_offset: u64,
    filename: String,
}

impl Reader {
    pub fn new() -> Self {
        Self {
            cached_block: None,
            block_offset: 0,
            filename: "".to_string(),
        }
    }

    pub fn readKey(&mut self, filename: &str, block_offset: u64, key: &str) -> Result<String> {
        if self.cached_block.is_none()
            || self.filename != filename
            || self.block_offset > block_offset
            || block_offset >= self.block_offset + self.cached_block.as_ref().unwrap().len() as u64
        {
            self.load_block_to_cache(filename, block_offset)?;
        }
        // TODO: add a jump array to block to accelerate search speed in one block
    }

    pub fn readMeta(&mut self, filename: &str, start: Offset) -> Result<KeyValueMeta> {
        if self.cached_block.is_none()
            || self.filename != filename
            || self.block_offset > start
            || start >= self.block_offset + self.cached_block.as_ref().unwrap().len() as u64
        {
            self.load_block_to_cache(filename, start)?;
        }

        let mut file = OpenOptions::new().read(true).open(filename)?;
        file.seek(SeekFrom::Start(start))?;

        let mut meta: KeyValueMeta = KeyValueMeta::new();

        // read key length
        let mut key_len = [0; KEY_LEN_BYTES];
        if file.read(&mut key_len[..])? < key_len.len() {
            bail!("read less key len in meta");
        }
        meta.key_len = u8::from_le_bytes(key_len) as u64;
        meta.key_offset = start;

        // read val length
        let mut val_len = [0; VAL_LEN_BYTES];
        if file.read(&mut val_len[..])? < val_len.len() {
            bail!("read less val len in meta");
        }
        meta.val_len = u16::from_be_bytes(val_len) as u64;
        meta.val_offset = start + meta.key_len;

        Ok(meta)
    }

    // TODO: implement cache to reduce disk io
    pub fn read(&mut self, filename: &str, start: Offset, len: u64) -> Result<Vec<u8>> {
        let mut file = OpenOptions::new().read(true).open(filename)?;
        file.seek(SeekFrom::Start(start))?;

        let mut val: Vec<u8> = vec![0; len as usize];
        if file.read(&mut val)? < val.len() {
            bail!("read less val");
        }

        Ok(val)
    }

    fn load_block_to_cache(&self, filename: &str, start: u64) -> Result<()> {
        todo!()
    }
}
