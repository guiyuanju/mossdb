use std::collections::{BTreeMap, btree_map};

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
        let key_len = key.len();
        let val_len = value.len();
        match self.store.insert(key, value) {
            None => {
                self.byte_size += key_len;
                self.byte_size += val_len;
            }
            Some(old) => {
                self.byte_size -= old.len();
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

impl<'a> IntoIterator for &'a MemTable {
    type Item = (String, String);

    type IntoIter = std::iter::Map<
        btree_map::Iter<'a, String, String>,
        fn((&'a String, &'a String)) -> (String, String),
    >;

    fn into_iter(self) -> Self::IntoIter {
        fn clone_pair((k, v): (&String, &String)) -> (String, String) {
            (k.clone(), v.clone())
        }

        self.store.iter().map(clone_pair)
    }
}
