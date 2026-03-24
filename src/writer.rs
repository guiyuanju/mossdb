use std::{fs::OpenOptions, io::Write};

use crate::{layout::Layout, memtable::MemTable};
use anyhow::Result;

pub struct Writer {}

impl Writer {
    pub fn write(
        memtable: impl IntoIterator<Item = (String, String, bool)>,
        filename: &str,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)?;

        let blocks_chain = Layout::build(memtable)?;

        for blocks in blocks_chain {
            for block in blocks.inner {
                file.write_all(&block.inner)?;
            }
        }

        Ok(())
    }
}
