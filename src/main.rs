use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

// DONE: single log: append, delete(tombstone), get, dump
// DONE: store byte stream instead of string, let upper layer handle type store and retreival
// TODO: multiple log: comnpression, merge
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
            ("Bob".as_bytes().to_vec(),  "age: 23, gender: male".as_bytes().to_vec()),
            ("Alice".as_bytes().to_vec(), "age: 18, gender: female".as_bytes().to_vec()),
        ];

        let mut map: HashMap<Vec<u8>, Location> = HashMap::new();
        for d in data {
            println!("storing {:?}", d.0);
            map.insert(
                d.0.clone(),
                Location::new(log.append(&d.0, &d.1)?, d.1.len())
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
    let mut repl = Repl::new();
    repl.run();

    Ok(())
}

pub struct Engine {
    maps: Vec<HashMap<Vec<u8>, Location>>,
    logs: Vec<Log>,
}

impl Engine {
    pub fn new(logs_dir: &str) -> io::Result<Self> {
        let mut logs = vec![];
        let mut maps = vec![];

        for entry in fs::read_dir(logs_dir)? {
            let path = entry?.path();
            if path.is_file() && path.ends_with(".log") {
                logs.push(Log::new(&path)?);
                maps.push(HashMap::new());
            }
        }

        logs.sort_by(|a, b| a.name.to_string_lossy().cmp(&b.name.to_string_lossy()));

        let mut engine = Engine {
            maps, logs,
        };

        engine.rebuild()?;

        return Ok(engine);
    }

    fn rebuild(&mut self) -> io::Result<()> {
        let mut count = 0;
        for (i, log) in self.logs.iter_mut().enumerate() {
            for entry in log.iter()? {
                let key = entry.key.value;
                let value = entry.value;
                // deleted entry
                if value.value.len() == 0 {
                    self.maps[i].remove(&key);
                } else {
                    self.maps[i].insert(key, Location::new(value.offset, value.len));
                }
                count += 1;
            }
        }
        println!("processed {} entries, {} index rebuilt", count, self.maps.iter().map(|e| e.len()).sum::<usize>());
        Ok(())
    }

    pub fn set(&mut self, key: &str, value: &str) -> io::Result<()> {
        let offset = self.log.append(key, value)?;
        self.map.insert(key.to_string(), Location::new(offset, value.as_bytes().len()));
        Ok(())
    }

    pub fn get(&mut self, key: &str) -> io::Result<String> {
        match self.map.get(key) {
            None => Err(io::Error::new(io::ErrorKind::Other, "key doesn't exist in map")),
            Some(entry) => self.log.read(entry.offset, entry.len),
        }
    }

    pub fn del(&mut self, key: &str) -> io::Result<()> {
        match self.map.get(key) {
            None => {}
            Some(_) => {
                self.log.append(key, "")?;
                self.map.remove(key);
            }
        }
        Ok(())
    }
}

pub struct Repl {
    engine: Option<Engine>
}

impl Repl {
    pub fn new() -> Self {
        Self{engine: None}
    }

    fn open(&mut self, name: &str) -> io::Result<()> {
        self.engine = Some(Engine::new(name)?);
        Ok(())
    }

    fn process_cmd(&mut self, cmd: &str, args: &[&str]) -> io::Result<()> {
        let engine = self.engine.as_mut().unwrap();
        match cmd {
            "set" => engine.set(args[0], args[1])?,
            "get" => println!("{}", engine.get(args[0])?),
            "del" => engine.del(args[0])?,
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
            "open" => {
                if self.open(line[1]).is_err() {
                    println!("failed to open file");
                    return;
                }
            }
            cmd => {
                if self.engine.is_none() {
                    println!("open log file first");
                    return;
                }
                match self.process_cmd(cmd, &line[1..]) {
                    Err(e) => println!("{}", e),
                    Ok(_) => {},
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

struct Location {
    offset: u64,
    len: usize,
}

impl Location {
    fn new(offset: u64, len: usize) -> Self {
        Self {offset, len}
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

    pub fn flush(&mut self) -> io::Result<()> {
        self.handler.flush()
    }

    pub fn iter(&mut self) -> io::Result<LogIterator> {
        let mut data = vec![];
        self.handler.rewind()?;
        self.handler.read_to_end(&mut data)?;
        Ok(LogIterator{data: data, index: 0})
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

        let len = u64::from_log_len_bytes(&data[i..i+8]).unwrap() as usize;
        i += 8;

        entry.key.offset = i as u64;
        entry.key.len = len;
        entry.key.value = data[i..i+len].to_vec();
        i += len;

        let len = u64::from_log_len_bytes(&data[i..i+8]).unwrap() as usize;
        i += 8;

        entry.value.offset = i as u64;
        entry.value.len = len;
        entry.value.value = data[i..i+len].to_vec();
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
            return Err(io::Error::new(io::ErrorKind::Other, "failed to convert from u8 slices to a u64"));
        }
        let buf: [u8; 8] = bytes.try_into().unwrap();
        return Ok(u64::from_be_bytes(buf));
    }

    fn to_log_len_bytes(len: u64) -> [u8; 8] {
        u64::to_be_bytes(len)
    }
}
