use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

// DONE: single log: append, delete(tombstone), get, dump
// DONE: store byte stream instead of string, let upper layer handle type store and retreival
// TODO: multiple log
//  - [x] get, set
//  - [ ] merge
//  - [ ] comnpression
// TODO: OS-like cache with LRU eviction, trade db space for performance, like in-memory cache db
// but with good durability
// TODO: support distribution

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
    if let Ok(cwd) = env::current_dir() {
        println!("CWD = {}", cwd.as_path().to_str().unwrap());
    }
    let mut repl = Repl::new();
    repl.run();

    Ok(())
}

const LOG_SIZE_LIMIT: u64 = 20;

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
                println!("Reading log file {}", path.to_str().unwrap());
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
            Some(lasted) => {
                let name = lasted
                    .name
                    .file_name()
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
        println!(
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

        let offset = self.logs.last_mut().unwrap().append(key, value)?;
        self.maps
            .last_mut()
            .unwrap()
            .insert(key.to_vec(), Location::new(offset, value.len()));
        Ok(())
    }

    // get value, check hash to find offset in log
    pub fn get(&mut self, key: &[u8]) -> io::Result<Vec<u8>> {
        match self.maps.last_mut().unwrap().get(key) {
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                "key doesn't exist in map",
            )),
            Some(entry) => self.logs.last_mut().unwrap().read(entry.offset, entry.len),
        }
    }

    // delete key, the tombstone value is an empty byte array
    pub fn del(&mut self, key: &[u8]) -> io::Result<()> {
        match self.maps.last().unwrap().get(key) {
            None => {}
            Some(_) => {
                self.logs.last_mut().unwrap().append(key, "".as_bytes())?;
                self.maps.last_mut().unwrap().remove(key);
            }
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
        print!("> ");
        let _ = io::stdout().flush();

        for line in io::stdin().lines() {
            if line.is_err() {
                return;
            }
            let line = line.unwrap();
            let line: Vec<&str> = line.split_whitespace().collect();

            self.process_line(&line);

            print!("> ");
            let _ = io::stdout().flush();
        }
    }
}

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
            // deleted entry
            if value.value.len() == 0 {
                self.remove(&key);
            } else {
                self.insert(key, Location::new(value.offset, value.len));
            }
            count += 1;
        }

        Ok(count)
    }
}

struct Location {
    offset: u64,
    len: usize,
}

impl Location {
    fn new(offset: u64, len: usize) -> Self {
        Self { offset, len }
    }
}

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

    // pub fn merge_with(&mut self, other: &Log) -> Log {
    //     let mut result:
    //     unimplemented!()
    // }
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
