use std::collections::BTreeMap;
use std::iter::Iterator;

#[derive(Debug)]
pub struct MemTable {
    store: BTreeMap<String, String>,
    byte_size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            store: BTreeMap::new(),
            byte_size: 0,
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.store.get(key).cloned()
    }

    pub fn set(&mut self, key: String, value: String) {
        let key_len = key.as_bytes().len();
        let val_len = value.as_bytes().len();
        match self.store.insert(key, value) {
            None => {
                self.byte_size += key_len;
                self.byte_size += val_len;
            }
            Some(old) => {
                self.byte_size -= old.as_bytes().len();
                self.byte_size += val_len;
            }
        }
    }

    pub fn del(&mut self, key: String) {
        // TODO: need to set key len = 0 in log file
        todo!()
    }

    pub fn byte_size(&self) -> usize {
        self.byte_size
    }
}

impl Iterator for MemTable {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.store
            .iter()
            .next()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
    }
}
