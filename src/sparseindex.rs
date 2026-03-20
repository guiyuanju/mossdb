use crate::types::Offset;

#[derive(Debug)]
pub struct SparseIndex {
    index: Vec<(String, Offset)>,
}

impl SparseIndex {
    pub fn new(index: Vec<(String, Offset)>) -> Self {
        Self { index }
    }

    pub fn get_closed_indice(&self, key: &str) -> Option<(Offset, Offset)> {
        let res = self
            .index
            .binary_search_by_key(&key.to_string(), |(k, _)| k.to_string()); // TODO: a lot of to_string
        match res {
            Ok(offset) => Some((offset as u64, offset as u64)),
            Err(offset) => {
                if offset == 0 {
                    return None;
                }
                return Some((offset as u64 - 1, offset as u64));
            }
        }
    }
}
