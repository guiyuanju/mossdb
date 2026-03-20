use crate::{reader::CachedReader, types::Offset};

#[derive(Debug)]
pub struct SparseIndex {
    index: Vec<(String, Offset)>,
}

impl SparseIndex {
    pub fn new(index: Vec<(String, Offset)>) -> Self {
        Self { index }
    }

    pub fn get_containing_block_offset(&self, key: &str) -> Option<u64> {
        let res = self
            .index
            .binary_search_by_key(&key.to_string(), |(k, _)| k.to_string()); // TODO: a lot of to_string
        match res {
            Ok(idx) => Some(self.index[idx].1),
            Err(idx) => {
                if idx == 0 {
                    return None;
                }
                return Some(self.index[idx - 1].1);
            }
        }
    }
}
