use anyhow::Result;
use log::{error, info};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::PathBuf,
    sync::{Arc, mpsc},
};

use crate::{
    common::next_log_file_name,
    engine::Engine,
    layout::{BLOCK_SIZE_BYTES, Block, KVEntryReader},
    sparseindex::{self, SparseIndex},
    sstable::{SSTable, SSTableCachedIter},
    writer::Writer,
};

pub struct Compact {
    engine: Arc<Engine>,
    rx: mpsc::Receiver<bool>, // value doesn't matter, msg itself indicates a new sstable file generates
}

impl Compact {
    pub fn new(engine: Arc<Engine>, rx: mpsc::Receiver<bool>) -> Self {
        Self { engine, rx }
    }

    pub fn start_loop(&self) {
        info!("compact thread started");
        loop {
            let _ = self.rx.recv();
            info!("compact thread received trigger message, try to find and compact files");
            let sstables = self.get_sstables_to_compact();
            if sstables.len() < 2 {
                info!("less than 2 sstables for compaction found, skip");
                continue;
            }
            info!("found {} sstables to compact", sstables.len());
            let filenames: Vec<String> = sstables.iter().map(|s| s.filename.clone()).collect();
            info!("compacting files: {:?}", filenames);
            let res_file = self.compact(sstables);
            match res_file {
                Err(err) => error!("failed to compact files {:?}, error: {:?}", filenames, err),
                Ok(res) => info!("compacted files {:?} to {:?}", filenames, res),
            }
        }
    }

    fn get_sstables_to_compact(&self) -> Vec<Arc<SSTable>> {
        let guard = self.engine.version.read().unwrap();
        let version = Arc::clone(&guard);
        if version.sstables.len() < 5 {
            return vec![];
        }
        let res: Vec<Arc<SSTable>> = version
            .sstables
            .iter()
            .take(2)
            .map(|sstable| Arc::clone(sstable))
            .collect();
        res
    }

    fn compact(&self, sstables: Vec<Arc<SSTable>>) -> Result<String> {
        let merge_iter = SSTableMergeIterator::new(sstables)?;
        let filename = next_log_file_name();
        Writer::write(merge_iter, &filename)?;
        Ok(filename)
    }
}

struct SSTableMergeIterator {
    files: Vec<File>,
    sparseindex: Vec<SparseIndex>,
    blocks: Vec<Block>,
    block_index: Vec<usize>, // the index inside the sparse index, representing current block
    offset_in_block: Vec<usize>,
    heads: Vec<Option<(String, String)>>,
    loaded: bool,
}

impl Iterator for SSTableMergeIterator {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        // initialize
        if !self.loaded {
            for idx in 0..self.files.len() {
                self.load_next_block(idx).ok()?;
                self.load_next_kv_for_block(idx).ok()?;
            }
            self.loaded = true;
        }

        // find the smallest, retrieve next
        let min_idx = self
            .heads
            .iter()
            .enumerate()
            .filter_map(|(idx, kv)| {
                if kv.is_none() {
                    return None;
                }
                Some(idx)
            })
            .min_by(|a, b| {
                self.heads[*a]
                    .as_ref()
                    .unwrap()
                    .0
                    .cmp(&self.heads[*b].as_ref().unwrap().0)
            })?;

        // get the smallest, and retrieve the next element for it
        let res = self.heads[min_idx].as_ref().unwrap().to_owned();
        self.load_next_kv_for_block(min_idx).unwrap();

        Some(res)
    }
}

impl SSTableMergeIterator {
    pub fn new(sstables: Vec<Arc<SSTable>>) -> Result<Self> {
        let mut files = vec![];
        for s in &sstables {
            let file = OpenOptions::new().read(true).open(&s.filename)?;
            files.push(file);
        }
        let len = files.len();

        let sparseindex: Vec<SparseIndex> =
            sstables.iter().map(|s| s.sparse_index.clone()).collect();

        Ok(Self {
            files,
            blocks: vec![Block::new(); len],
            offset_in_block: vec![0; len],
            heads: vec![None; len],
            loaded: false,
            sparseindex,
            block_index: vec![0; len],
        })
    }

    // Err => file format error
    // Ok(bool) => true: read a block, false: no more block
    pub fn load_next_block(&mut self, idx: usize) -> Result<bool> {
        match self.sparseindex[idx].index.get(self.block_index[idx]) {
            None => Ok(false),
            Some((_, offset)) => {
                self.files[idx].seek(SeekFrom::Start(*offset))?;
                self.block_index[idx] += 1;
                self.files[idx].read_exact(&mut self.blocks[idx].inner[..])?;
                Ok(true)
            }
        }
    }

    pub fn load_next_kv_for_block(&mut self, idx: usize) -> Result<()> {
        if self.offset_in_block[idx] >= BLOCK_SIZE_BYTES {
            match self.load_next_block(idx) {
                Err(err) => return Err(err),
                Ok(has_block) => {
                    if !has_block {
                        self.heads[idx] = None;
                        return Ok(());
                    }
                    self.offset_in_block[idx] = 0;
                }
            }
        }
        let kv_entry = KVEntryReader::new(&self.blocks[idx].inner[self.offset_in_block[idx]..]);
        if let Some((k, v, len)) = kv_entry.retrive_kv() {
            self.heads[idx] = Some((
                String::from_utf8_lossy(k).to_string(),
                String::from_utf8_lossy(v).to_string(),
            ));
            self.offset_in_block[idx] += len;
        } else {
            self.heads[idx] = None;
        }
        Ok(())
    }
}
