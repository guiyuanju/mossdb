use crate::{reader::CachedReader, types::Offset};

#[derive(Debug)]
pub struct SparseIndex {
    index: Vec<(String, Offset)>,
}

impl SparseIndex {
    pub fn new(index: Vec<(String, Offset)>) -> Self {
        Self { index }
    }

    pub fn populate_from_log(filename: &str, reader: &CachedReader) {}

    pub fn get_containing_block_offset(&self, key: &str) -> Option<u64> {
        let res = self
            .index
            .binary_search_by_key(&key.to_string(), |(k, _)| k.to_string()); // TODO: a lot of to_string
        match res {
            Ok(offset) => Some(offset as u64),
            Err(offset) => {
                if offset == 0 {
                    return None;
                }
                return Some(offset as u64 - 1);
            }
        }
    }
}
