use std::collections::HashMap;

#[derive(Debug)]
pub struct Location {
    pub offset: u64,
    pub len: usize,
}

impl Location {
    pub fn new(offset: u64, len: usize) -> Self {
        Self { offset, len }
    }

    pub fn tombstone() -> Self {
        Self { offset: 0, len: 0 }
    }

    pub fn is_tombstone(&self) -> bool {
        self.len == 0
    }
}

#[derive(Debug)]
pub struct Map {
    pub inner: HashMap<Vec<u8>, Location>,
}

impl Map {
    pub fn new() -> Self {
        Map {
            inner: HashMap::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&Location> {
        self.inner.get(key)
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, key: &[u8]) {
        self.inner.remove(key);
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Location) {
        self.inner.insert(key, value);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}
