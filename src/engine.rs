use anyhow::{Context, Result, anyhow, bail};
use log::info;
use std::{
    cell::RefCell,
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    thread,
};

use crate::{
    memtable::MemTable,
    sstable::{self, SSTable},
    writer::Writer,
};

const LOG_SIZE_LIMIT: u64 = 36; // (8+1 + 8+1)*2: 2 kv pair

#[derive(Debug)]
pub struct Engine {
    pub memtable: RefCell<MemTable>,
    pub sstables: Vec<SSTable>,
    pub memtable_limit_bytes: u64,
    pub sstables_dir: PathBuf,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            memtable: RefCell::new(MemTable::new()),
            sstables: vec![],
            memtable_limit_bytes: LOG_SIZE_LIMIT,
            sstables_dir: PathBuf::new(),
        }
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
            if path.is_file() && path.extension().map_or(false, |ext| ext == "log") {
                info!("Reading log file {}", path.to_str().unwrap());
                logs.push(path);
            }
        }

        logs.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));

        for log in logs {
            let file = log.to_string_lossy().to_string();
            self.sstables.push(SSTable::new(file)?);
        }

        self.memtable_limit_bytes = LOG_SIZE_LIMIT;
        self.sstables_dir = path;

        Ok(())
    }

    fn next_log_file_name(&self) -> Result<String> {
        if !self.sstables_dir.is_dir() {
            bail!("please open a directory");
        }

        match self.sstables.last() {
            None => Ok("0".to_string()),
            Some(latest) => {
                let mut path = PathBuf::new();
                path.push(&latest.filename);
                let name = path.file_stem().unwrap().to_str().unwrap().to_string();
                let num: u64 = name.parse().unwrap();
                Ok((num + 1).to_string())
            }
        }
    }

    // set key value, append to log, udpate hash, grow if neccessary
    pub fn set(&mut self, key: String, value: String) {
        self.memtable.borrow_mut().set(key, value);
        if self.memtable.borrow().len() as u64 >= self.memtable_limit_bytes {
            let old_memtable = self.memtable.replace(MemTable::new());
            // flush the full memtable in a new thread
            // TODO: makes sure write thread finish even quit the engine
            let filename = self.next_log_file_name();
            thread::spawn(move || {
                match filename {
                    Err(err) => println!("failed when next log file name: {}", err),
                    Ok(filename) => {
                        _ = Writer::write(old_memtable, filename)
                            .map_err(|err| println!("failed to write {}", err));
                    }
                };
            });
        }
    }

    // get value, check hash to find offset in log
    pub fn get(&mut self, key: &str) -> Result<String> {
        if let Some(res) = self.memtable.borrow().get(key) {
            return Ok(res);
        }

        for t in self.sstables.iter_mut().rev() {
            if let Ok(res) = t.get(key) {
                return Ok(res);
            }
        }

        return Err(anyhow!("value not found"));
    }

    // delete key, the tombstone value is an empty byte array
    pub fn del(&mut self, key: &str) {
        todo!()
    }
}
