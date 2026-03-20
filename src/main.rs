use crate::repl::Repl;
use std::env;
use std::io;

mod engine;
mod layout;
mod memtable;
mod reader;
mod repl;
mod sparseindex;
mod sstable;
mod types;
mod writer;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use super::*;

    #[test]
    fn test_main() -> io::Result<()> {
        todo!()
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

// TODO: use version set, flush thread write sstable, then add a new version, then swap the lastest version
// TODO: add compaction thread
// TODO: use immutable memtable queue and a mutable current memtable + multiple flush threads
// TODO: add WAL
