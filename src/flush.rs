use log::{Level, error, info, log};
use std::{
    mem,
    sync::{Arc, mpsc},
};
use uuid::Uuid;

use crate::{
    engine::Engine,
    layout::LOG_FILE_EXT,
    memtable::MemTable,
    sstable::{self, SSTable},
    versionset::Version,
    writer::Writer,
};

pub struct Flush {
    rx: mpsc::Receiver<Arc<MemTable>>,
    engine: Arc<Engine>,
}

impl Flush {
    pub fn new(rx: mpsc::Receiver<Arc<MemTable>>, engine: Arc<Engine>) -> Self {
        Self { rx, engine }
    }

    pub fn start_loop(&self) {
        loop {
            let memtable: Arc<MemTable> = self.rx.recv().unwrap();

            let filename = Self::next_log_file_name();
            if let Err(err) = Writer::write(memtable.as_ref(), &filename) {
                error!("error when flushing memtable: {:?}", err);
                continue;
            }
            info!("flushed memtable to sstable file: {}", filename);
            match SSTable::new(&filename) {
                Err(err) => {
                    error!(
                        "error when create sstable from file: {:?}, {:?}",
                        filename, err
                    )
                }
                Ok(sstable) => {
                    self.install_new_version(&memtable, sstable);
                }
            }
        }
    }

    fn install_new_version(&self, memtable: &MemTable, sstable: SSTable) {
        let sstable = Arc::new(sstable);
        loop {
            // read version and release lock
            let mut version_ptr = std::ptr::null();
            let mut new_version = {
                let version = self.engine.version.read().unwrap();
                version_ptr = version.as_ref();
                let version = Arc::clone(&version);
                (*version).clone()
            };

            // remove memtable from queue
            let index = new_version
                .imm_memtables
                .iter()
                .position(|m| std::ptr::eq((*m).as_ref(), memtable))
                .unwrap();
            new_version.imm_memtables.remove(index);

            // add sstable
            new_version.sstables.push(sstable.clone());

            let mut guard = self.engine.version.write().unwrap();
            let current_version = guard.clone();
            if std::ptr::eq(current_version.as_ref(), version_ptr) {
                *guard = Arc::new(new_version);
                info!("new version installed after flushing");
                break;
            }
        }
    }

    fn next_log_file_name() -> String {
        let name = Uuid::now_v7().to_string();
        format!("{}.{}", name, LOG_FILE_EXT)
    }
}
