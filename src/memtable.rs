use std::collections::BTreeMap;
use std::iter::Iterator;

#[derive(Debug)]
pub struct MemTable {
    store: BTreeMap<String, String>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            store: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.store.get(key).cloned()
    }

    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub(crate) fn flush(&self) {
        todo!()
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
