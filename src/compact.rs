use anyhow::{Result, bail};
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
    sstable::SSTable,
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
            while self.get_sstable_size() > 4 {
                let sstables = self.get_sstables_to_compact();
                if sstables.len() < 2 {
                    info!("less than 2 sstables for compaction found, skip");
                    break;
                }
                info!("found {} sstables to compact", sstables.len());
                if let Err(err) = self.try_compact(sstables) {
                    error!("try compact error: {:?}", err);
                    break;
                }
            }
        }
    }

    fn try_compact(&self, sstables: Vec<Arc<SSTable>>) -> Result<()> {
        let filenames: Vec<String> = sstables.iter().map(|s| s.filename.clone()).collect();
        info!("compacting files: {:?}", filenames);
        let res_file = self.compact(sstables);
        match res_file {
            Err(err) => bail!("failed to compact files {:?}, error: {:?}", filenames, err),
            Ok(res) => {
                info!("compacted files {:?} to {:?}", filenames, res);
                if let Err(err) = self.install_new_version(&filenames, &res) {
                    bail!("failed to install new version after compaction: {:?}", err);
                } else {
                    info!("installed new version after compaction");
                    Ok(())
                }
            }
        }
    }

    fn install_new_version(&self, from: &[String], to: &str) -> Result<()> {
        let sstable = Arc::new(SSTable::new(to.clone())?);
        loop {
            // read version and release lock
            let mut version_ptr = std::ptr::null();
            let mut version_sstable_len = 0;
            let mut new_version = {
                let version = self.engine.version.read().unwrap();
                version_ptr = version.as_ref();
                version_sstable_len = version.sstables.len();
                let version = Arc::clone(&version);
                (*version).clone()
            };

            // remove compacted sstables with the result sstable
            let first_replaced_idx = new_version
                .sstables
                .iter()
                .position(|s| from.contains(&s.filename))
                .unwrap();
            new_version.sstables.retain(|s| !from.contains(&s.filename));
            new_version
                .sstables
                .insert(first_replaced_idx, Arc::clone(&sstable));
            let new_version_sstable_len = new_version.sstables.len();
            assert!(new_version.sstables.len() < version_sstable_len);

            if self
                .engine
                .install_new_version(version_ptr, Arc::new(new_version))
                .is_ok()
            {
                info!(
                    "new version installed after compaction, old version sstable size = {}, new version sstable size = {}",
                    version_sstable_len, new_version_sstable_len,
                );
                return Ok(());
            }
        }
    }

    fn get_sstable_size(&self) -> usize {
        let guard = self.engine.version.read().unwrap();
        let version = Arc::clone(&guard);
        version.sstables.len()
    }

    // TODO: improve this method, too many arc clone
    /// current strategy: get the smallest two adjavent sstables, newest at the start
    fn get_sstables_to_compact(&self) -> Vec<Arc<SSTable>> {
        let guard = self.engine.version.read().unwrap();
        let version = Arc::clone(&guard);

        if version.sstables.len() < 2 {
            return vec![];
        }

        // get the smallest size sstable
        let mut sorted = version
            .sstables
            .iter()
            .enumerate()
            .map(|(idx, s)| (idx, s.file_size))
            .collect::<Vec<(usize, u64)>>();
        sorted.sort_by_cached_key(|(_, size)| *size);
        let (idx, _) = &sorted[0];

        // get the smaller ajacent sstable
        if *idx == 0 {
            return vec![
                Arc::clone(&version.sstables[1]),
                Arc::clone(&version.sstables[0]),
            ];
        }
        if *idx == version.sstables.len() - 1 {
            return vec![
                Arc::clone(&version.sstables[*idx]),
                Arc::clone(&version.sstables[*idx - 1]),
            ];
        }
        if version.sstables[*idx - 1].file_size < version.sstables[*idx + 1].file_size {
            return vec![
                Arc::clone(&version.sstables[*idx]),
                Arc::clone(&version.sstables[*idx - 1]),
            ];
        } else {
            return vec![
                Arc::clone(&version.sstables[*idx + 1]),
                Arc::clone(&version.sstables[*idx]),
            ];
        }
    }

    fn compact(&self, sstables: Vec<Arc<SSTable>>) -> Result<String> {
        let merge_iter = SSTableMergeIterator::new(sstables)?;
        let filename = next_log_file_name(&self.engine.sstables_dir);
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
    heads: Vec<Option<(String, String, bool)>>, // key, val, deleted
    loaded: bool,
    prev: Option<String>, // previous outputed key, used to skip value that should be discarded
}

impl Iterator for SSTableMergeIterator {
    type Item = (String, String, bool);

    fn next(&mut self) -> Option<Self::Item> {
        // initialize
        if !self.loaded {
            for idx in 0..self.files.len() {
                self.load_next_block(idx).ok()?;
                self.load_next_kv_for_block(idx).ok()?;
            }
            self.loaded = true;
        }

        let res = self.retrieve_next_not_deleted_unique_smallest();
        res
    }
}

impl SSTableMergeIterator {
    // newest sstable should at the start
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
            prev: None,
        })
    }

    pub fn retrieve_next_not_deleted_unique_smallest(&mut self) -> Option<(String, String, bool)> {
        let mut cur = self.retrieve_next_unique_smallest()?;
        // while deleted, skip all deleted value
        while cur.2 {
            cur = self.retrieve_next_unique_smallest()?;
        }
        Some(cur)
    }

    pub fn retrieve_next_unique_smallest(&mut self) -> Option<(String, String, bool)> {
        let mut cur = self.retrieve_smallest()?;
        if self.prev.is_none() || !self.prev.as_ref().unwrap().eq(&cur.0) {
            self.prev = Some(cur.0.clone());
            return Some(cur);
        }
        while self.prev.as_ref().unwrap().eq(&cur.0) {
            cur = self.retrieve_smallest()?;
        }
        self.prev = Some(cur.0.clone());
        return Some(cur);
    }

    // find the smallest, retrieve next
    pub fn retrieve_smallest(&mut self) -> Option<(String, String, bool)> {
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
        if let Some((k, v, deleted, len)) = kv_entry.retrive_kv() {
            self.heads[idx] = Some((
                String::from_utf8_lossy(k).to_string(),
                String::from_utf8_lossy(v).to_string(),
                deleted,
            ));
            self.offset_in_block[idx] += len;
        } else {
            self.heads[idx] = None;
        }
        Ok(())
    }
}
