use anyhow::{Context, Result, anyhow, bail};
use log::{Level, info, log};
use std::{
    cell::RefCell,
    fs::{self, OpenOptions, read_to_string, remove_file},
    io::Write,
    mem,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex, RwLock,
        mpsc::{self, Receiver, Sender},
    },
    thread,
};
use uuid::Uuid;

use crate::{
    compact::Compact,
    flush::Flush,
    layout::{DELETED_FLAG_BYTES, LOG_FILE_EXT, MEMTABLE_MAX_SIZE_BYTES},
    memtable::{self, MemTable},
    sstable::SSTable,
    versionset::Version,
    writer::Writer,
};

const METADATA_FILE: &str = "mossdb_metadata";

#[derive(Debug)]
pub struct Engine {
    pub version: RwLock<Arc<Version>>,
    pub memtable: Mutex<MemTable>, // TODO: use concurrent data structure for better performance
    pub sstables_dir: String,
    flush_tx: mpsc::Sender<Arc<MemTable>>,
}

impl Engine {
    pub fn new(path: &str) -> Result<Arc<Engine>> {
        let (flush_tx, flush_rx) = mpsc::channel();
        let (compact_tx, compact_rx) = mpsc::channel();

        let mut engine = Self {
            version: RwLock::new(Arc::new(Version::new())),
            memtable: Mutex::new(MemTable::new()),
            sstables_dir: path.to_string(),
            flush_tx,
        };

        // load all logs to sstable
        engine.open_log_dir(&path)?;

        // start flush thread
        let engine = Arc::new(engine);
        let cloned = engine.clone();
        thread::spawn(move || {
            Flush::new(cloned, flush_rx, compact_tx).start_loop();
        });

        // start compaction thread
        let cloned = engine.clone();
        thread::spawn(move || {
            Compact::new(cloned, compact_rx).start_loop();
        });

        Ok(engine)
    }

    // TODO: may fail during writing metadata file, need to store the order in the sstable files too
    // previous_version: compare and swap, used to compare
    pub fn install_new_version(
        &self,
        previous_version: *const Version,
        new_version: Arc<Version>,
    ) -> Result<()> {
        let mut guard = self.version.write().unwrap();
        let current_version = guard.clone();
        if std::ptr::eq(current_version.as_ref(), previous_version) {
            let cloned = Arc::clone(&new_version);
            *guard = new_version;
            self.write_metadata_file(cloned);
            return Ok(());
        }
        return Err(anyhow!("previous version has changed, please try again"));
    }

    fn write_metadata_file(&self, version: Arc<Version>) {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(METADATA_FILE)
            .unwrap();
        let mut meta = vec![];
        for s in &version.sstables {
            let mut path = PathBuf::new();
            path.push(s.filename.clone());
            let filename = path.file_name().unwrap().to_string_lossy().to_string();
            meta.push(filename.clone());
        }
        for l in meta {
            file.write_fmt(format_args!("{}\n", l)).unwrap();
        }
        info!("metadata file written");
    }

    pub fn list_sorted_log_files(&self) -> Result<Vec<PathBuf>> {
        let mut logs = vec![];
        let mut path = PathBuf::new();
        path.push(&self.sstables_dir);
        if !path.is_dir() {
            bail!("not a directory");
        }

        let filenames = self.read_from_metadata_file();

        for entry in fs::read_dir(&path).context("cannot open log dir")? {
            let path = entry?.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "log") {
                let filename = path.file_name().unwrap().to_string_lossy().to_string();
                if filenames.contains(&filename) {
                    info!("recognizing {} in newest version", filename);
                    logs.push(path);
                } else {
                    info!("{} is not in newest version, please fix it", filename);
                }
            }
        }

        logs.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));

        Ok(logs)
    }

    fn read_from_metadata_file(&self) -> Vec<String> {
        let res = read_to_string(METADATA_FILE).unwrap();
        res.lines().map(|s| s.to_string()).collect::<Vec<String>>()
    }

    pub fn open_log_dir(&mut self, dir: &str) -> Result<()> {
        let logs = self.list_sorted_log_files()?;

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
    pub fn put(&self, key: String, value: String) {
        self.flush_if(move |m: &mut MemTable| {
            m.put(key, value);
            m.byte_size() as u64 >= MEMTABLE_MAX_SIZE_BYTES
        });
    }

    // get value, check hash to find offset in log
    pub fn get(&self, key: &str) -> Result<String> {
        let memtable = self.memtable.lock().unwrap();
        if let Some((value, deleted)) = memtable.get(key) {
            if deleted {
                bail!("key deleted");
            }
            return Ok(value);
        }

        let version = self.version.read().unwrap();
        let version = Arc::clone(&version);

        for m in version.imm_memtables.iter().rev() {
            if let Some((value, deleted)) = m.get(key) {
                if deleted {
                    bail!("key deleted");
                }
                return Ok(value);
            }
        }

        for t in version.sstables.iter().rev() {
            if let Ok((val, deleted)) = t.get(key) {
                if deleted {
                    bail!("key deleted");
                }
                return Ok(val);
            }
        }

        Err(anyhow!("key not found"))
    }

    // delete key, the tombstone value is an empty byte array
    pub fn del(&self, key: &str) {
        self.flush_if(move |m: &mut MemTable| {
            m.del(key.to_string());
            m.byte_size() as u64 >= MEMTABLE_MAX_SIZE_BYTES
        });
    }

    /// flush immedieately to disk
    pub fn flush(&self) {
        self.flush_if(|_| true);
    }

    /// flush current memtable immediately to disk if predicate is true
    /// inside a mutext lock, so that flushing the correct one
    fn flush_if<F>(&self, predicate: F)
    where
        F: FnOnce(&mut MemTable) -> bool,
    {
        let mut memtable = self.memtable.lock().unwrap();
        if predicate(&mut memtable) {
            // replace full memtable with a new one
            let old_memtable = mem::replace(&mut *memtable, MemTable::new());
            let old_memtable = Arc::new(old_memtable);
            drop(memtable);

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
            let _ = self.flush_tx.send(old_memtable.clone());
        }
    }

    pub fn dump(&self) {
        let memtable = self.memtable.lock().unwrap();
        println!("memtable = {:?}", memtable);

        let version = self.version.read().unwrap();
        let version = Arc::new(&version);
        println!("immutable memtables = {:?}", version.imm_memtables);
        println!("sstables = {:?}", version.sstables);

        for s in &version.sstables {
            println!("SSTable<{}>", s.filename);
            s.dump();
        }
    }
}
