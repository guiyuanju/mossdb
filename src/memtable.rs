use std::collections::{BTreeMap, btree_map};

#[derive(Debug)]
pub struct MemTable {
    store: BTreeMap<String, (String, bool)>, // key, (value, deleted)
    byte_size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            store: BTreeMap::new(),
            byte_size: 0,
        }
    }

    /// return (value, deleted)
    pub fn get(&self, key: &str) -> Option<(String, bool)> {
        let (val, deleted) = self.store.get(key)?;
        if *deleted {
            return Some(("".to_string(), true));
        }
        Some((val.to_string(), false))
    }

    pub fn put(&mut self, key: String, value: String) {
        self.set(key, value, false);
    }

    fn set(&mut self, key: String, value: String, deleted: bool) {
        let key_len = key.len();
        let val_len = value.len();
        match self.store.insert(key, (value, deleted)) {
            None => {
                self.byte_size += key_len;
                self.byte_size += val_len;
            }
            Some(old) => {
                self.byte_size -= old.0.len();
                self.byte_size += val_len;
            }
        }
    }

    pub fn del(&mut self, key: String) {
        self.set(key, "".to_string(), true);
    }

    pub fn byte_size(&self) -> usize {
        self.byte_size
    }
}

impl<'a> IntoIterator for &'a MemTable {
    type Item = (String, String, bool);

    type IntoIter = std::iter::Map<
        btree_map::Iter<'a, String, (String, bool)>,
        fn((&'a String, &'a (String, bool))) -> (String, String, bool),
    >;

    fn into_iter(self) -> Self::IntoIter {
        fn clone_pair((k, v): (&String, &(String, bool))) -> (String, String, bool) {
            (k.clone(), v.0.clone(), v.1.clone())
        }

        self.store.iter().map(clone_pair)
    }
}
