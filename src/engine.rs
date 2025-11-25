use anyhow::{Context, Result};
use log::info;
use std::{
    fs::{self, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use crate::{
    LogMerger,
    log::Log,
    map::{Location, Map},
};

const LOG_SIZE_LIMIT: u64 = 36; // (8+1 + 8+1)*2: 2 kv pair

#[derive(Debug)]
pub struct Engine {
    pub maps: Vec<Map>,
    pub logs: Vec<Log>,
    pub log_limit_bytes: u64,
    pub logs_dir: PathBuf,
}

impl Engine {
    pub fn new(logs_dir: &str) -> Result<Self> {
        let mut logs = vec![];
        let mut maps = vec![];
        let mut path = PathBuf::new();
        path.push(logs_dir);

        for entry in fs::read_dir(&path).context("cannot open log dir")? {
            let path = entry?.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "log") {
                info!("Reading log file {}", path.to_str().unwrap());
                logs.push(Log::new(&path)?);
                maps.push(Map::new());
            }
        }

        logs.sort_by(|a, b| a.name.to_string_lossy().cmp(&b.name.to_string_lossy()));

        let mut engine = Engine {
            maps,
            logs,
            log_limit_bytes: LOG_SIZE_LIMIT,
            logs_dir: path,
        };

        engine.rebuild();

        if engine.logs.len() == 0 {
            engine.grow();
        }

        return Ok(engine);
    }

    fn new_log(dir: &PathBuf, name: &str) -> io::Result<Log> {
        let mut path = PathBuf::new();
        path.push(dir);
        path.push(name.to_string() + ".log");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("cannot create log: {}", e))
            })?;
        Ok(Log::new(&path)?)
    }

    fn new_log_mono_increase(dir: &PathBuf, latest_log: Option<&Log>) -> io::Result<Log> {
        match latest_log {
            None => Self::new_log(dir, "0"),
            Some(latest) => {
                let name = latest
                    .name
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                let num: u64 = name.parse().unwrap();
                Self::new_log(dir, &(num + 1).to_string())
            }
        }
    }

    // grow the number of logs and hashmaps
    pub fn grow(&mut self) {
        self.logs
            .push(Self::new_log_mono_increase(&self.logs_dir, self.logs.last()).unwrap());
        self.maps.push(Map::new());
    }

    // rebuild from log files
    fn rebuild(&mut self) {
        let mut count = 0;
        for (i, log) in self.logs.iter_mut().enumerate() {
            count += self.maps[i].load_from_log(log);
        }
        info!(
            "processed {} entries, {} index rebuilt",
            count,
            self.maps.iter().map(|e| e.len()).sum::<usize>()
        );
    }

    // set key value, append to log, udpate hash, grow if neccessary
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        if self.logs.last_mut().unwrap().size().unwrap() >= self.log_limit_bytes {
            self.grow();
        }

        if self.logs.len() > 2 {
            let merged_log_name = Path::new("log.merging");
            let to_merge1 = self.logs[0].name.clone();
            let to_merge2 = self.logs[1].name.clone();
            // merge
            let mut merger =
                LogMerger::new(vec![to_merge1.clone(), to_merge2.clone()], merged_log_name)
                    .unwrap();
            merger.merge().unwrap();
            // update with minimum move in vector, ensure close file before delete and move
            // expect: both old logs are deleted; merged_lod is renamed; in memory map and log are
            // updated
            self.logs.remove(0); // remove and close the first log handler
            fs::remove_file(&to_merge1).unwrap(); // delete the first log file
            self.maps.splice(0..2, std::iter::once(merger.merged_map)); // update map for both logs
            drop(merger.merged_log); // close the merged log file
            fs::rename(merged_log_name, &to_merge1).unwrap(); // rename merged log to first log
            self.logs[0] = Log::new(&to_merge1).unwrap(); // replace the second log with merged log
            fs::remove_file(to_merge2).unwrap(); // delete the left second log file
        }

        let offset = self.logs.last_mut().unwrap().append(key, value).unwrap();
        self.maps
            .last_mut()
            .unwrap()
            .insert(key.to_vec(), Location::new(offset, value.len()));
    }

    // get value, check hash to find offset in log
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        for (i, m) in self.maps.iter_mut().enumerate().rev() {
            if let Some(loc) = m.get(key) {
                if loc.is_tombstone() {
                    return None;
                }
                return Some(self.logs[i].read(loc.offset, loc.len).unwrap());
            }
        }
        None
    }

    // delete key, the tombstone value is an empty byte array
    pub fn del(&mut self, key: &[u8]) {
        if let Some(_) = self.get(key) {
            self.logs
                .last_mut()
                .unwrap()
                .append(key, "".as_bytes())
                .unwrap();
            self.maps
                .last_mut()
                .unwrap()
                .insert(key.to_owned(), Location::tombstone());
        }
    }
}
