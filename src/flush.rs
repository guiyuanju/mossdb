use log::{Level, error, info, log};
use std::{
    mem,
    sync::{Arc, mpsc},
};
use uuid::Uuid;

use crate::{
    common::next_log_file_name,
    engine::Engine,
    layout::LOG_FILE_EXT,
    memtable::MemTable,
    sstable::{self, SSTable},
    versionset::Version,
    writer::Writer,
};

pub struct Flush {
    engine: Arc<Engine>,
    rx: mpsc::Receiver<Arc<MemTable>>,
    compact_tx: mpsc::Sender<bool>,
}

impl Flush {
    pub fn new(
        engine: Arc<Engine>,
        rx: mpsc::Receiver<Arc<MemTable>>,
        compact_tx: mpsc::Sender<bool>,
    ) -> Self {
        Self {
            rx,
            engine,
            compact_tx,
        }
    }

    pub fn start_loop(&self) {
        info!("flush thread started");
        loop {
            let memtable: Arc<MemTable> = self.rx.recv().unwrap();

            let filename = next_log_file_name(&self.engine.sstables_dir);
            if let Err(err) = Writer::write(memtable.into_iter(), &filename) {
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
                    info!("new version installed after flushing");
                    self.compact_tx.send(true);
                    info!("trigger message sent to compact thread");
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

            if self
                .engine
                .install_new_version(version_ptr, Arc::new(new_version))
                .is_ok()
            {
                break;
            }
        }
    }
}
