use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};

// DONE: single log: append, delete(tombstone), get, dump
// TODO: multiple log: comnpression, merge

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_main() -> io::Result<()> {
        let mut log = Log::new("log")?;

        let data = vec![
            ("Bob",  "age: 23, gender: male"),
            ("Alice", "age: 18, gender: female"),
        ];

        let mut map: HashMap<String, MapEntry> = HashMap::new();
        for d in data {
            println!("storing {:?}", d.0);
            map.insert(
                d.0.to_string(),
                MapEntry::new(log.append(d.0, d.1)?, d.1.as_bytes().len())
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
    map: HashMap<String, MapEntry>,
    log: Log,
}

impl Engine {
    pub fn new(name: &str) -> io::Result<Self> {
        let mut engine = Engine {
            map: HashMap::new(),
            log: Log::new(name)?,
        };
        engine.rebuild()?;
        return Ok(engine);
    }

    fn rebuild(&mut self) -> io::Result<()> {
        let mut count = 0;
        for log_entry in self.log.iter()? {
            let key = log_entry.key.value;
            let value = log_entry.value;
            // deleted entry
            if value.value.len() == 0 {
                self.map.remove(&key);
            } else {
                self.map.insert(key, MapEntry::new(value.offset, value.len));
            }
            count += 1;
        }
        println!("processed {} entries, {} index rebuilt", count, self.map.len());
        Ok(())
    }

    pub fn set(&mut self, key: &str, value: &str) -> io::Result<()> {
        let offset = self.log.append(key, value)?;
        self.map.insert(key.to_string(), MapEntry::new(offset, value.as_bytes().len()));
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
            "dump" => engine.log.dump()?,
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

struct MapEntry {
    offset: u64,
    len: usize,
}

impl MapEntry {
    fn new(offset: u64, len: usize) -> Self {
        Self {offset, len}
    }
}

struct Log {
    name: String,
    handler: File,
}

impl Log {
    fn new(name: &str) -> io::Result<Self> {
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

    pub fn append(&mut self, key: &str, value: &str) -> io::Result<u64> {
        let key_len = (key.as_bytes().len() as u64).to_be_bytes();
        self.handler.write_all(&key_len)?;
        self.handler.write_all(key.as_bytes())?;

        let value_len = (value.as_bytes().len() as u64).to_be_bytes();
        self.handler.write_all(&value_len)?;
        let position = self.handler.stream_position()?;
        self.handler.write_all(value.as_bytes())?;

        Ok(position)
    }

    pub fn read(&mut self, offset: u64, len: usize) -> io::Result<String> {
        self.handler.seek(io::SeekFrom::Start(offset))?;
        let mut buf = vec![0; len];
        self.handler.read_exact(&mut buf)?;
        return String::from_utf8(buf)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to read data as UTF-8"));
        
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
            println!("{}: {}", log_entry.key.value, log_entry.value.value);
        }

        Ok(())
    }
}

#[derive(Default)]
struct Point {
    offset: u64,
    len: usize,
    value: String,
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

        let key = String::from_utf8_lossy(&data[i..i+len]);
        entry.key.offset = i as u64;
        entry.key.len = len;
        entry.key.value = key.to_string();
        i += len;

        let len = u64::from_log_len_bytes(&data[i..i+8]).unwrap() as usize;
        i += 8;

        let value = String::from_utf8_lossy(&data[i..i+len]);
        entry.value.offset = i as u64;
        entry.value.len = len;
        entry.value.value = value.to_string();
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
