use log::{error, info, warn};
use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests {
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

const LOG_SIZE_LIMIT: u64 = 36; // (8+1 + 8+1)*2: 2 kv pair

#[derive(Debug)]
pub struct Engine {
    maps: Vec<Map>,
    logs: Vec<Log>,
    log_limit_bytes: u64,
    logs_dir: PathBuf,
}

impl Engine {
    pub fn new(logs_dir: &str) -> io::Result<Self> {
        let mut logs = vec![];
        let mut maps = vec![];
        let mut path = PathBuf::new();
        path.push(logs_dir);

        for entry in fs::read_dir(&path)? {
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

        engine.rebuild()?;

        if engine.logs.len() == 0 {
            engine.grow()?;
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
    pub fn grow(&mut self) -> io::Result<()> {
        self.logs.push(Self::new_log_mono_increase(
            &self.logs_dir,
            self.logs.last(),
        )?);
        self.maps.push(Map::new());
        Ok(())
    }

    // rebuild from log files
    fn rebuild(&mut self) -> io::Result<()> {
        let mut count = 0;
        for (i, log) in self.logs.iter_mut().enumerate() {
            count += self.maps[i].load_from_log(log)?;
        }
        info!(
            "processed {} entries, {} index rebuilt",
            count,
            self.maps.iter().map(|e| e.len()).sum::<usize>()
        );
        Ok(())
    }

    // set key value, append to log, udpate hash, grow if neccessary
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        if self.logs.last_mut().unwrap().size()? >= self.log_limit_bytes {
            self.grow()?;
        }

        if self.logs.len() > 2 {
            let merged_log_name = Path::new("log.merging");
            let to_merge1 = self.logs[0].name.clone();
            let to_merge2 = self.logs[1].name.clone();
            // merge
            let mut merger =
                LogMerger::new(vec![to_merge1.clone(), to_merge2.clone()], merged_log_name)?;
            merger.merge()?;
            // update with minimum move in vector, ensure close file before delete and move
            // expect: both old logs are deleted; merged_lod is renamed; in memory map and log are
            // updated
            self.logs.remove(0); // remove and close the first log handler
            fs::remove_file(&to_merge1)?; // delete the first log file
            self.maps.splice(0..2, std::iter::once(merger.merged_map)); // update map for both logs
            drop(merger.merged_log); // close the merged log file
            fs::rename(merged_log_name, &to_merge1)?; // rename merged log to first log
            self.logs[0] = Log::new(&to_merge1)?; // replace the second log with merged log
            fs::remove_file(to_merge2)?; // delete the left second log file
        }

        let offset = self.logs.last_mut().unwrap().append(key, value)?;
        self.maps
            .last_mut()
            .unwrap()
            .insert(key.to_vec(), Location::new(offset, value.len()));
        Ok(())
    }

    // get value, check hash to find offset in log
    pub fn get(&mut self, key: &[u8]) -> io::Result<Vec<u8>> {
        let err_key_no_exist = Err(io::Error::new(
            io::ErrorKind::Other,
            "key doesn't exist in map",
        ));
        for (i, m) in self.maps.iter_mut().enumerate().rev() {
            if let Some(loc) = m.get(key) {
                if loc.is_tombstone() {
                    return err_key_no_exist;
                }
                return self.logs[i].read(loc.offset, loc.len);
            }
        }
        return err_key_no_exist;
    }

    // delete key, the tombstone value is an empty byte array
    pub fn del(&mut self, key: &[u8]) -> io::Result<()> {
        if let Ok(_) = self.get(key) {
            self.logs.last_mut().unwrap().append(key, "".as_bytes())?;
            self.maps
                .last_mut()
                .unwrap()
                .insert(key.to_owned(), Location::tombstone());
        }
        Ok(())
    }
}

pub struct Repl {
    engine: Option<Engine>,
}

impl Repl {
    pub fn new() -> Self {
        Self { engine: None }
    }

    fn open(&mut self, name: &str) -> io::Result<()> {
        self.engine = Some(Engine::new(name)?);
        Ok(())
    }

    fn process_cmd(&mut self, cmd: &str, args: &[&str]) -> io::Result<()> {
        let engine = self.engine.as_mut().unwrap();
        match cmd {
            "set" => engine.set(args[0].as_bytes(), args[1].as_bytes())?,
            "get" => println!(
                "{}",
                String::from_utf8_lossy(&engine.get(args[0].as_bytes())?)
            ),
            "del" => engine.del(args[0].as_bytes())?,
            "dump" => {
                for log in &mut engine.logs {
                    println!("{:?}:", log.name);
                    log.dump()?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn process_line(&mut self, line: &[&str]) {
        match line[0] {
            "open" => match self.open(line[1]) {
                Err(e) => {
                    println!("failed to open logs: {}", e);
                    return;
                }
                Ok(_) => {}
            },
            cmd => {
                if self.engine.is_none() {
                    println!("open log file first");
                    return;
                }
                match self.process_cmd(cmd, &line[1..]) {
                    Err(e) => println!("{}", e),
                    Ok(_) => {}
                }
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

#[derive(Debug)]
struct Map {
    inner: HashMap<Vec<u8>, Location>,
}

impl Map {
    fn new() -> Self {
        Map {
            inner: HashMap::new(),
        }
    }

    fn get(&self, key: &[u8]) -> Option<&Location> {
        self.inner.get(key)
    }

    fn remove(&mut self, key: &[u8]) {
        self.inner.remove(key);
    }

    fn insert(&mut self, key: Vec<u8>, value: Location) {
        self.inner.insert(key, value);
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    // load entry from a log file, can be called multiple times, same key is overwritten
    fn load_from_log(&mut self, log: &mut Log) -> io::Result<u64> {
        let mut count: u64 = 0;
        for entry in log.iter()? {
            let key = entry.key.value;
            let value = entry.value;
            self.insert(key, Location::new(value.offset, value.len));
            count += 1;
        }

        Ok(count)
    }
}

#[derive(Debug)]
struct Location {
    offset: u64,
    len: usize,
}

impl Location {
    fn new(offset: u64, len: usize) -> Self {
        Self { offset, len }
    }

    fn tombstone() -> Self {
        Self { offset: 0, len: 0 }
    }

    fn is_tombstone(&self) -> bool {
        self.len == 0
    }
}

#[derive(Debug)]
struct Log {
    name: PathBuf,
    handler: File,
}

impl Log {
    fn new(name: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&name)?;

        Ok(Self {
            name: name.to_owned(),
            handler: file,
        })
    }

    pub fn size(&mut self) -> io::Result<u64> {
        Ok(self.handler.metadata()?.len())
    }

    pub fn rename(from: Log, to: &Path) -> io::Result<Log> {
        // close file before rename for cross-platform compatibility
        drop(from.handler);
        fs::rename(from.name, to)?;
        Ok(Log::new(to)?)
    }

    // format: (lenght: 8 bytes) (value: variant)
    pub fn append(&mut self, key: &[u8], value: &[u8]) -> io::Result<u64> {
        let key_len = (key.len() as u64).to_be_bytes();
        self.handler.write_all(&key_len)?;
        self.handler.write_all(key)?;

        let value_len = (value.len() as u64).to_be_bytes();
        self.handler.write_all(&value_len)?;
        let position = self.handler.stream_position()?;
        self.handler.write_all(value)?;

        Ok(position)
    }

    pub fn read(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.handler.seek(io::SeekFrom::Start(offset))?;
        let mut buf = vec![0; len];
        self.handler.read_exact(&mut buf)?;
        return Ok(buf);
    }

    #[allow(dead_code)]
    pub fn flush(&mut self) -> io::Result<()> {
        self.handler.flush()
    }

    pub fn iter(&mut self) -> io::Result<LogIterator> {
        let mut data = vec![];
        self.handler.rewind()?;
        self.handler.read_to_end(&mut data)?;
        Ok(LogIterator {
            data: data,
            index: 0,
        })
    }

    pub fn dump(&mut self) -> io::Result<()> {
        for log_entry in self.iter()? {
            println!("{:?}: {:?}", log_entry.key.value, log_entry.value.value);
        }

        Ok(())
    }
}

#[derive(Default)]
struct Point {
    offset: u64,
    len: usize,
    value: Vec<u8>,
}

#[derive(Default)]
struct LogEntry {
    key: Point,
    value: Point,
}

struct LogIterator {
    data: Vec<u8>,
    index: usize,
}

impl Iterator for LogIterator {
    type Item = LogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let mut i = self.index;
        let mut entry = LogEntry::default();
        let data = &self.data;

        if i >= data.len() {
            return None;
        }

        let len = u64::from_log_len_bytes(&data[i..i + 8]).unwrap() as usize;
        i += 8;

        entry.key.offset = i as u64;
        entry.key.len = len;
        entry.key.value = data[i..i + len].to_vec();
        i += len;

        let len = u64::from_log_len_bytes(&data[i..i + 8]).unwrap() as usize;
        i += 8;

        entry.value.offset = i as u64;
        entry.value.len = len;
        entry.value.value = data[i..i + len].to_vec();
        i += len;

        self.index = i;
        return Some(entry);
    }
}

pub trait LogRecordLen {
    fn from_log_len_bytes(bytes: &[u8]) -> io::Result<u64>;
    fn to_log_len_bytes(len: u64) -> [u8; 8];
}

impl LogRecordLen for u64 {
    fn from_log_len_bytes(bytes: &[u8]) -> io::Result<u64> {
        if bytes.len() != 8 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to convert from u8 slices to a u64",
            ));
        }
        let buf: [u8; 8] = bytes.try_into().unwrap();
        return Ok(u64::from_be_bytes(buf));
    }

    fn to_log_len_bytes(len: u64) -> [u8; 8] {
        u64::to_be_bytes(len)
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
            let _ = m.load_from_log(&mut log)?;
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
