use anyhow::{Context, Result, anyhow, bail};
use log::{Level, info, log};
use std::{
    cell::RefCell,
    fs::{self},
    mem,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock, mpsc},
    thread,
};
use uuid::Uuid;

use crate::{
    flush::Flush,
    layout::{LOG_FILE_EXT, MEMTABLE_MAX_SIZE_BYTES},
    memtable::{self, MemTable},
    sstable::SSTable,
    versionset::Version,
    writer::Writer,
};

#[derive(Debug)]
pub struct Engine {
    pub version: RwLock<Arc<Version>>,
    pub memtable: Mutex<MemTable>, // TODO: use concurrent data structure for better performance
    pub sstables_dir: String,
    flush_tx: mpsc::Sender<Arc<MemTable>>,
}

impl Engine {
    pub fn new(path: &str) -> Arc<Engine> {
        let (flush_tx, flush_rx) = mpsc::channel();

        let mut engine = Self {
            version: RwLock::new(Arc::new(Version::new())),
            memtable: Mutex::new(MemTable::new()),
            sstables_dir: path.to_string(),
            flush_tx,
        };

        // load all logs to sstable
        engine.open_log_dir(&path);

        // start flush thread
        let engine = Arc::new(engine);
        let cloned = engine.clone();
        thread::spawn(move || {
            Flush::new(flush_rx, cloned).start_loop();
        });

        engine
    }

    pub fn open_log_dir(&mut self, dir: &str) -> Result<()> {
        let mut logs = vec![];
        let mut path = PathBuf::new();
        path.push(dir);
        if !path.is_dir() {
            bail!("not a directory");
        }

        for entry in fs::read_dir(&path).context("cannot open log dir")? {
            let path = entry?.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "log") {
                info!("Reading log file {}", path.to_str().unwrap());
                logs.push(path);
            }
        }

        logs.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));

        let mut sstables: Vec<Arc<SSTable>> = vec![];
        for log in logs {
            let file = log.to_string_lossy().to_string();
            sstables.push(Arc::new(SSTable::new(&file)?));
        }

        let mut new_version = Version::new();
        new_version.sstables = sstables;
        let mut current = self.version.write().unwrap();
        *current = Arc::new(new_version);

        Ok(())
    }

    // set key value, append to log, udpate hash, grow if neccessary
    pub fn set(&self, key: String, value: String) {
        let mut memtable = self.memtable.lock().unwrap();

        memtable.set(key, value);

        if memtable.byte_size() as u64 >= MEMTABLE_MAX_SIZE_BYTES {
            // replace full memtable with a new one
            let old_memtable = mem::replace(&mut *memtable, MemTable::new());
            let old_memtable = Arc::new(old_memtable);

            // install the full memtable to the newest version
            // use optimistic lock: cmpare and set
            // reason: full memtable installation is rare compare to read operation
            // optimistic lock is more performant
            // and cloning and push cost time when the vector is long
            // a simple mutex will block read operation for a long time
            loop {
                let mut version_ptr = std::ptr::null();
                // cheap read lock
                let mut new_version = {
                    // put version in a block to realease the read lock upon block end
                    let version = self.version.read().unwrap().clone();
                    version_ptr = version.as_ref();
                    (*version).clone()
                };
                new_version.imm_memtables.push(old_memtable.clone());

                // write lock with cheap operation
                let mut guard = self.version.write().unwrap();
                let current_version = Arc::clone(&guard);
                if std::ptr::eq(current_version.as_ref(), version_ptr) {
                    *guard = Arc::new(new_version);
                    break;
                }
            }

            // notify flush thread
            self.flush_tx.send(old_memtable.clone());
        }
    }

    // get value, check hash to find offset in log
    pub fn get(&self, key: &str) -> Result<String> {
        let memtable = self.memtable.lock().unwrap();
        if let Some(res) = memtable.get(key) {
            return Ok(res);
        }

        let version = self.version.read().unwrap();
        let version = Arc::clone(&version);

        for m in version.imm_memtables.iter().rev() {
            if let Some(res) = m.get(key) {
                return Ok(res);
            }
        }

        for t in version.sstables.iter().rev() {
            if let Ok(res) = t.get(key) {
                return Ok(res);
            }
        }

        Err(anyhow!("value not found"))
    }

    // delete key, the tombstone value is an empty byte array
    pub fn del(&self, key: &str) {
        todo!()
    }

    pub fn dump(&self) {
        let memtable = self.memtable.lock().unwrap();
        println!("memtable = {:?}", memtable);

        let version = self.version.read().unwrap();
        let version = Arc::new(&version);
        println!("immutable memtables = {:?}", version.imm_memtables);
        println!("sstables = {:?}", version.sstables);
    }
}
