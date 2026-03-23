use std::sync::{Arc, RwLock};

use crate::{memtable::MemTable, sstable::SSTable};

#[derive(Debug)]
pub struct Version {
    pub imm_memtables: Vec<Arc<MemTable>>,
    pub sstables: Vec<Arc<SSTable>>,
}

impl Version {
    pub fn new() -> Self {
        Self {
            imm_memtables: vec![],
            sstables: vec![],
        }
    }
}
