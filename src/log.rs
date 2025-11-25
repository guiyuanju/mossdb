use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct Log {
    pub name: PathBuf,
    pub handler: File,
}

impl Log {
    pub fn new(name: &Path) -> io::Result<Self> {
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

    #[allow(dead_code)]
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
pub struct Point {
    pub offset: u64,
    pub len: usize,
    pub value: Vec<u8>,
}

#[derive(Default)]
pub struct LogEntry {
    pub key: Point,
    pub value: Point,
}

pub struct LogIterator {
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
    #[allow(dead_code)]
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
