use crate::engine::Engine;
use crate::log::Log;
use crate::map::{Location, Map};
use ::log::{error, info};
use anyhow::Result;
use std::env;
use std::fs::{self};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

mod engine;
mod log;
mod map;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_main() -> io::Result<()> {
        let mut log = Log::new(Path::new("log"))?;

        let data: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (
                "Bob".as_bytes().to_vec(),
                "age: 23, gender: male".as_bytes().to_vec(),
            ),
            (
                "Alice".as_bytes().to_vec(),
                "age: 18, gender: female".as_bytes().to_vec(),
            ),
        ];

        let mut map: HashMap<Vec<u8>, Location> = HashMap::new();
        for d in data {
            println!("storing {:?}", d.0);
            map.insert(
                d.0.clone(),
                Location::new(log.append(&d.0, &d.1)?, d.1.len()),
            );
        }

        log.flush()?;

        for (k, v) in map {
            let res = log.read(v.offset, v.len)?;
            println!("retriving {:?}: {:?}", k, res);
        }

        log.dump()?;

        Ok(())
    }
}

fn main() -> io::Result<()> {
    env_logger::init();

    if let Ok(cwd) = env::current_dir() {
        println!("CWD = {}", cwd.as_path().to_str().unwrap());
    }
    let mut repl = Repl::new();
    repl.run();

    Ok(())
}

pub struct Repl {
    engine: Option<Engine>,
}

impl Repl {
    pub fn new() -> Self {
        Self { engine: None }
    }

    fn open(&mut self, name: &str) -> Result<()> {
        self.engine = Some(Engine::new(name)?);
        Ok(())
    }

    fn process_cmd(&mut self, cmd: &str, args: &[&str]) {
        let engine = self.engine.as_mut().unwrap();
        match cmd {
            "set" => engine.set(args[0].as_bytes(), args[1].as_bytes()),
            "get" => {
                if let Some(v) = engine.get(args[0].as_bytes()) {
                    println!("{}", String::from_utf8_lossy(&v));
                } else {
                    println!("no value found");
                }
            }
            "del" => engine.del(args[0].as_bytes()),
            "dump" => {
                for log in &mut engine.logs {
                    println!("{:?}:", log.name);
                    log.dump().unwrap();
                }
            }
            _ => {}
        }
    }

    fn process_line(&mut self, line: &[&str]) {
        match line[0] {
            "open" => {
                let _ = self.open(line[1]).map_err(|e| println!("{}", e));
            }
            cmd => {
                if self.engine.is_none() {
                    println!("open log file first");
                    return;
                }
                self.process_cmd(cmd, &line[1..]);
            }
        }
    }

    pub fn run(&mut self) {
        let mut line = "".to_string();
        loop {
            print!("> ");
            let _ = io::stdout().flush();

            line.clear();
            match io::stdin().read_line(&mut line) {
                Err(e) => {
                    error!("{}", e);
                    continue;
                }
                Ok(_) => {
                    let line: Vec<&str> = line.split_whitespace().collect();
                    self.process_line(&line);
                }
            }
        }
    }
}

struct LogMerger {
    maps: Vec<Map>,
    logs: Vec<Log>,
    merged_map: Map,
    merged_log: Log,
}

impl LogMerger {
    fn new(log_paths: Vec<PathBuf>, result_log: &Path) -> io::Result<Self> {
        let mut maps = vec![];
        let mut logs = vec![];
        for p in log_paths {
            let mut log = Log::new(&p)?;
            let mut m = Map::new();
            let _ = m.load_from_log(&mut log);
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

    fn merge(&mut self) -> io::Result<()> {
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
