use std::{
    fs, io,
    path::{Path, PathBuf},
};

use log::info;

use crate::{
    engine::Engine,
    log::Log,
    map::{Location, Map},
};

pub struct LogMerger {
    pub maps: Vec<Map>,
    pub logs: Vec<Log>,
    pub merged_map: Map,
    pub merged_log: Log,
}

impl LogMerger {
    pub fn new(log_paths: Vec<PathBuf>, result_log: &Path) -> io::Result<Self> {
        let mut maps = vec![];
        let mut logs = vec![];
        for p in log_paths {
            let mut log = Log::new(&p)?;
            let mut m = Map::new();
            let _ = Engine::populate_map_from_log(&mut m, &mut log);
            maps.push(m);
            logs.push(Log::new(&p)?);
        }

        if result_log.exists() {
            info!("merge log exists, deleting...");
            fs::remove_file(result_log)?;
        }

        Ok(Self {
            maps: maps,
            logs: logs,
            merged_map: Map::new(),
            merged_log: Log::new(result_log)?,
        })
    }

    pub fn merge(&mut self) -> io::Result<()> {
        info!("merging...");
        for i in 0..self.maps.len() {
            for (k, v) in self.maps[i].inner.iter() {
                let overwritten = self.maps[i + 1..].iter().any(|m| m.get(k).is_some());
                info!("k = {:?}, overwritten = {}", k, overwritten);
                if !overwritten && !v.is_tombstone() {
                    let value = self.logs[i].read(v.offset, v.len)?;
                    let offset = self.merged_log.append(k, &value)?;
                    self.merged_map
                        .insert(k.to_owned(), Location::new(offset, value.len()));
                }
            }
        }

        let _ = self.merged_log.flush();

        Ok(())
    }

    // fn write_to_log(&mut self) {
    //     for (key, value) in &self.map.inner {
    //         // TODO:
    //         // 1. map only stores offset, not which log file, how to retrive and merge them?
    //         //    rethink the architecture
    //         // 2. the log that is not the current one should be read only, they are immutable,
    //         //    maybe have a ImmutableLog? which can avoid concurrent conflict
    //         self.result_log.append(key);
    //     }
    // }
}
