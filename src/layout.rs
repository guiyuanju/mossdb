use std::ops::Range;

use anyhow::{Result, bail};

// Disk file layout:
//  index block offset (INDEX_META_LEN) |
//  data block offset |
//  key length | key value | val length | val value ...
pub const LOG_FILE_EXT: &str = "log";
pub const BLOCK_SIZE_BYTES: usize = 16 * 1024; // 16 KB
pub const MEMTABLE_MAX_SIZE_BYTES: u64 = 16;
// pub const MEMTABLE_MAX_SIZE_BYTES: usize = 64 * 1024 * 1024; // 64 MB
// pub const BLOCK_COUNT: usize = MEMTABLE_MAX_SIZE_BYTES / BLOCK_SIZE_BYTES; // 4096 blocks in one level 0 log file

pub const INDEX_BLOCK_OFFSET_META_OFFSET: usize = 0; // start at 0 of a file
pub const INDEX_BLOCK_OFFSET_META_OFFSET_BYTES: usize = 8; // u64
pub const DATA_BLOCK_OFFSET_META_OFFSET: usize = 8; // start right after index block offset data
pub const DATA_BLOCK_OFFSET_META_OFFSET_BYTES: usize = 8; // u64
// the length of metadata at the start of a log file, must fit into one block
pub const META_DATA_BYTE_LEN: usize =
    INDEX_BLOCK_OFFSET_META_OFFSET_BYTES + DATA_BLOCK_OFFSET_META_OFFSET_BYTES;
// pub const SPARSE_INDEX_START_OFFSET: usize = META_DATA_BYTE_LEN;

// byte layout of a single pair of KV: [key_len] [val_len] [key] [val]
// key_len_len defines the byte size of key_len, limit the maximum length of byte in key
// val_len_len is similar
pub const KEY_LEN_BYTES: usize = 1; // 5 bits, use 1 byte to store physically, 32 Byte max key size, around 4 billion unique keys allowed
pub const VAL_LEN_BYTES: usize = 2; // 10 bits, use 2 bytes to store, 1 KB max value size, combined with key, if fully stored, max use ~4TB space
pub const MAX_KEY_LEN: usize = 32; // a key max 32 bytes, used to limit at runtime
pub const MAX_VAL_LEN: usize = 1024; // a val max 1024 bytes, used to limit at runtime

// use u64 for the offset in the log
// u64 has 8 bytes, but we use 32 bytes to store it
// becuase we want the block size can be dividable by the entry size
// -> easier implementation, 256 index entries in each block
// a entry in sparse index is fixed to MAX_KEY_LEN + 32 bytes
// A entry = [ key bytes + zeros | offset bytes (8 bytes) + 24 bytes zeros ]
pub const SPARSE_INDEX_ENTRY_BYTE_LEN: usize = MAX_KEY_LEN + 32;
pub const SPARSE_INDEX_COUNT_PER_BLOCK: usize = BLOCK_SIZE_BYTES / SPARSE_INDEX_ENTRY_BYTE_LEN;

pub struct Layout {}

impl Layout {
    pub fn build(kvs: impl Iterator<Item = (String, String)>) -> Result<Vec<Blocks>> {
        // write data blocks
        let mut data_blocks = Blocks::new();
        let mut first_keys_of_blocks: Vec<String> = vec![];
        let mut data = [0_u8; SPARSE_INDEX_ENTRY_BYTE_LEN];
        let mut kv_entry = Entry::new(&mut data);
        for (k, v) in kvs {
            let size = kv_entry.populate_with_key_val(k.as_bytes(), v.as_bytes())?;

            let is_in_new_block = data_blocks.write(&kv_entry.data[0..size]);
            if is_in_new_block {
                first_keys_of_blocks.push(k);
            }
        }

        let data_block_count = data_blocks.inner.len() as u64;
        let mut index_block_count = data_block_count / SPARSE_INDEX_COUNT_PER_BLOCK as u64;
        if !data_block_count.is_multiple_of(SPARSE_INDEX_COUNT_PER_BLOCK as u64) {
            index_block_count += 1;
        }
        let meta_block_count = 1_u64;

        // write index blocks
        let mut index_blocks = Blocks::new();
        let mut index_data = [0_u8; SPARSE_INDEX_ENTRY_BYTE_LEN];
        for (i, start_key) in first_keys_of_blocks.iter().enumerate() {
            let mut cur_idx = 0;

            // write key
            for &byte in start_key.as_bytes() {
                index_data[cur_idx] = byte;
                cur_idx += 1;
            }

            // jump to the start of offset
            cur_idx = MAX_KEY_LEN;

            // write offset, left aligned
            let key_offset_in_log: u64 =
                (meta_block_count + index_block_count + i as u64) * BLOCK_SIZE_BYTES as u64;
            for byte in key_offset_in_log.to_le_bytes() {
                index_data[cur_idx] = byte;
                cur_idx += 1;
            }

            index_blocks.write(&index_data);
        }

        // write meta data block
        let mut meta_block = Blocks::new();
        let mut meta_data = [0_u8; META_DATA_BYTE_LEN];
        // write index block offset
        let index_offset_in_log: [u8; INDEX_BLOCK_OFFSET_META_OFFSET_BYTES] =
            (meta_block_count * BLOCK_SIZE_BYTES as u64).to_le_bytes();
        meta_data[INDEX_BLOCK_OFFSET_META_OFFSET
            ..(INDEX_BLOCK_OFFSET_META_OFFSET + INDEX_BLOCK_OFFSET_META_OFFSET_BYTES)]
            .copy_from_slice(&index_offset_in_log[..]);
        // write data block offset
        let data_offset_in_log: [u8; DATA_BLOCK_OFFSET_META_OFFSET_BYTES] =
            ((meta_block_count + index_block_count) * BLOCK_SIZE_BYTES as u64).to_le_bytes();
        meta_data[DATA_BLOCK_OFFSET_META_OFFSET
            ..(DATA_BLOCK_OFFSET_META_OFFSET + DATA_BLOCK_OFFSET_META_OFFSET_BYTES)]
            .copy_from_slice(&data_offset_in_log[..]);
        meta_block.write(&meta_data);

        Ok(vec![meta_block, index_blocks, data_blocks])
    }
}

#[derive(Debug)]
pub struct Block {
    pub inner: [u8; BLOCK_SIZE_BYTES],
}

impl Block {
    pub fn new() -> Self {
        Self {
            inner: [0; BLOCK_SIZE_BYTES],
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

#[derive(Default)]
pub struct Blocks {
    pub inner: Vec<Block>,
    current_block_idx: isize,
    current_idx_in_block: usize,
}

impl Blocks {
    pub fn new() -> Self {
        Self {
            inner: vec![],
            current_block_idx: -1,
            current_idx_in_block: 0,
        }
    }

    // if current block remaning capacity is big enough to put the data, write to it
    // if not, create a new block and write from the start
    // return value:
    //  - true: this data is written to a new block
    //  - false: no new block is allocated
    pub fn write(&mut self, data: &[u8]) -> bool {
        let mut new_block_created = false;
        if self.inner.is_empty() || BLOCK_SIZE_BYTES - self.current_idx_in_block < data.len() {
            self.inner.push(Block::new());
            self.current_block_idx += 1;
            self.current_idx_in_block = 0;
            new_block_created = true;
        }
        for &byte in data {
            self.inner[self.current_block_idx as usize].inner[self.current_idx_in_block] = byte;
            self.current_idx_in_block += 1;
        }
        new_block_created
    }
}

pub struct Entry<'a> {
    pub data: &'a mut [u8],
}

impl<'a> Entry<'a> {
    pub fn new(data: &'a mut [u8]) -> Self {
        Self { data }
    }

    fn val_len_range() -> Range<usize> {
        KEY_LEN_BYTES..(KEY_LEN_BYTES + VAL_LEN_BYTES)
    }

    fn key_range(key_len: usize) -> Range<usize> {
        (KEY_LEN_BYTES + VAL_LEN_BYTES)..(KEY_LEN_BYTES + VAL_LEN_BYTES + key_len)
    }

    fn val_range(key_len: usize, val_len: usize) -> Range<usize> {
        let val_offset = KEY_LEN_BYTES + VAL_LEN_BYTES + key_len;
        val_offset..(val_offset + val_len)
    }

    // return the populated size
    pub fn populate_with_key_val(&mut self, key: &[u8], val: &[u8]) -> Result<usize> {
        if key.len() > MAX_KEY_LEN {
            bail!("key too long");
        }
        if val.len() > MAX_VAL_LEN {
            bail!("val too long");
        }

        self.data[0] = key.len() as u8;
        self.data[Self::key_range(key.len())].copy_from_slice(key);

        let val_len_bytes: [u8; VAL_LEN_BYTES] = (val.len() as u16).to_le_bytes();
        self.data[Self::val_len_range()].copy_from_slice(&val_len_bytes);
        self.data[Self::val_range(key.len(), val.len())].copy_from_slice(val);
        Ok(KEY_LEN_BYTES + VAL_LEN_BYTES + key.len() + val.len())
    }

    // return Option<(key_len, val_len)>
    // if none -> current data doesn't has enough lenght of data to be interpreted as meta
    fn retrive_meta(&self) -> Option<(usize, usize)> {
        if KEY_LEN_BYTES + VAL_LEN_BYTES > self.data.len() {
            return None;
        }
        let key_len = self.data[0] as usize;
        let mut val_len_bytes: [u8; VAL_LEN_BYTES] = [0; VAL_LEN_BYTES];
        val_len_bytes[..].copy_from_slice(&self.data[Self::val_len_range()]);
        let val_len = u16::from_le_bytes(val_len_bytes) as usize;

        Some((key_len, val_len))
    }

    // if none -> data is not valid as a kv entry
    // e.g. not long enough, possible passed in the empty space at the end of a block
    /// return Option<key, value, lenght of entry = key_len + value_len + meta_len>
    pub fn retrive_kv(&'a self) -> Option<(&'a [u8], &'a [u8], usize)> {
        let (key_len, val_len) = self.retrive_meta()?;
        if key_len + val_len + KEY_LEN_BYTES + VAL_LEN_BYTES > self.data.len() {
            return None;
        }
        let key = &self.data[Self::key_range(key_len)];
        let val = &self.data[Self::val_range(key_len, val_len)];

        Some((key, val, key_len + val_len + KEY_LEN_BYTES + VAL_LEN_BYTES))
    }
}

pub struct MetaData<'a> {
    pub data: &'a mut [u8],
}

impl<'a> MetaData<'a> {
    pub fn new(data: &'a mut [u8]) -> Self {
        Self { data }
    }

    pub fn retrieve_sparse_index_block_start_offset(&self) -> u64 {
        let offset_data = &self.data[INDEX_BLOCK_OFFSET_META_OFFSET
            ..(INDEX_BLOCK_OFFSET_META_OFFSET + INDEX_BLOCK_OFFSET_META_OFFSET_BYTES)];
        let mut index_offset_bytes = [0_u8; INDEX_BLOCK_OFFSET_META_OFFSET_BYTES];
        index_offset_bytes.copy_from_slice(offset_data);
        u64::from_le_bytes(index_offset_bytes)
    }

    pub fn retrieve_data_block_start_offset(&self) -> u64 {
        let data_offset_data = &self.data[DATA_BLOCK_OFFSET_META_OFFSET
            ..(DATA_BLOCK_OFFSET_META_OFFSET + DATA_BLOCK_OFFSET_META_OFFSET_BYTES)];
        let mut data_offset_bytes = [0_u8; DATA_BLOCK_OFFSET_META_OFFSET_BYTES];
        data_offset_bytes.copy_from_slice(data_offset_data);
        u64::from_le_bytes(data_offset_bytes)
    }
}

pub struct SparseIndexEntry<'a> {
    data: &'a mut [u8],
}

impl<'a> SparseIndexEntry<'a> {
    pub fn new(data: &'a mut [u8]) -> Self {
        Self { data }
    }

    // key may not exist because key len is zero
    pub fn retrieve_key(&self) -> Option<String> {
        // a key max 32 byte, in sparse index, even a key is smaller than 32, extra space is filled with \0
        // we need to retrive the true key
        let mut key_end = 0;
        for &byte in &self.data[0..MAX_KEY_LEN] {
            if byte == b'\0' {
                break;
            }
            key_end += 1;
        }
        if key_end == 0 {
            return None;
        }
        Some(String::from_utf8_lossy(&self.data[0..key_end]).to_string())
    }

    pub fn retrieve_offset(&self) -> u64 {
        // a value is 8 byte, a u64, but in sparse key index, it occupies 32 bytes, right padding with 0
        // so we only get the first 8 bytes
        let offset_data = &self.data[MAX_KEY_LEN..(MAX_KEY_LEN + 8)];
        let mut offset_bytes = [0_u8; 8];
        offset_bytes.copy_from_slice(offset_data);
        u64::from_le_bytes(offset_bytes)
    }
}
