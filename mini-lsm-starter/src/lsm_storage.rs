#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::fs;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_iterator::{get_bound_inner, FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        self.compaction_notifier.send(())?;
        let flush_thread = self.flush_thread.lock().take();
        let compact_thread = self.compaction_thread.lock().take();
        if let Some(flush_thread) = flush_thread {
            flush_thread
                .join()
                .expect("Couldn't join on the flush thread");
        }
        if let Some(compact_thread) = compact_thread {
            compact_thread
                .join()
                .expect("Couldn't join on the compact thread");
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };
        // Create LSM database directory if it doesn't exist
        if !path.exists() {
            fs::create_dir(path)?;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let state = self.state.read();
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        let value_in_memtable = snapshot.memtable.get(_key);
        if let Some(value) = value_in_memtable {
            if !value.is_empty() {
                return Ok(Some(value));
            } else {
                return Ok(None);
            }
        }
        for imm_table in &state.imm_memtables {
            let value_in_immemtable = imm_table.get(_key);
            if let Some(value) = value_in_immemtable {
                if !value.is_empty() {
                    return Ok(Some(value));
                } else {
                    return Ok(None);
                }
            }
        }
        let mut sst_iters = Vec::new();
        for sst_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables.get(sst_id).unwrap().clone();
            if key_within(
                _key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(_key),
                )?));
            }
        }
        let mut sst_iter = MergeIterator::create(sst_iters);
        while sst_iter.is_valid() {
            let key = sst_iter.key().raw_ref();
            let value = sst_iter.value();
            if key == _key {
                if !value.is_empty() {
                    return Ok(Some(Bytes::copy_from_slice(value)));
                } else {
                    return Ok(None);
                }
            } else if key > _key {
                return Ok(None);
            } else {
                sst_iter.next()?;
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        // Note: Never hold ReadLock while acquiring state_lock mutex
        if self.memtable_reach_capacity() {
            let state_lock = self.state_lock.lock();
            if self.memtable_reach_capacity() {
                // Double check
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        self.state.read().memtable.put(_key, _value)
    }

    fn memtable_reach_capacity(&self) -> bool {
        self.state.read().memtable.approximate_size() >= self.options.num_memtable_limit
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let empty_value = [0_u8; 0];
        self.put(_key, &empty_value)
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // Why state is Arc<RwLock<Arc<LsmStorageState>>>?
        // This way we have to copy LsmStorageState each time force_freeze_memtable is called
        let memtable_new = MemTable::create(self.next_sst_id());
        let mut state = self.state.write();
        let mut snapshot = state.as_ref().clone();
        let memtable_old = std::mem::replace(&mut snapshot.memtable, Arc::new(memtable_new));
        snapshot.imm_memtables.insert(0, memtable_old);
        *state = Arc::new(snapshot);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // 1. Select a memtable to flush
        let snapshot = {
            let guard = self.state.read();
            // imm_memtables is FIFO, so we don't need to worry about imm_memtable changing
            if let Some(imm_memtable) = guard.imm_memtables.last() {
                imm_memtable.clone()
            } else {
                return Ok(());
            }
        };
        // 2. Create an SST file corresponding to a memtable
        let mut builder = SsTableBuilder::new(self.options.block_size);
        // flush memtable content to SsTableBuilder
        snapshot.flush(&mut builder)?;
        let new_sst_table = Arc::new(builder.build(
            snapshot.id(),
            Some(self.block_cache.clone()),
            self.path_of_sst(snapshot.id()),
        )?);
        // 3. Remove the memtable from the immutable memtable list and add the SST file to L0 SSTs
        let _state_lock = self.state_lock.lock();
        let mut state_guard = self.state.write();
        let mut new_lsm_state = state_guard.as_ref().clone();
        let _ = new_lsm_state.imm_memtables.pop().unwrap();
        new_lsm_state.l0_sstables.insert(0, new_sst_table.sst_id());
        new_lsm_state
            .sstables
            .insert(new_sst_table.sst_id(), new_sst_table);
        *state_guard = Arc::new(new_lsm_state);
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let mut memtable_iters = Vec::new();
        let mut sst_iters = Vec::new();
        let snapshot = {
            // Creating iterators might take a lot of time because of I/O operations, we should drop the lock early with safety
            // In order to achieve this goal, multiple LsmStorageState instance might exists, with Arc<State>
            // if we want to modify inner state, we must create a copy of it
            // So we can first acquire its lock, get arc state, then drop the lock
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        memtable_iters.push(Box::new(snapshot.memtable.scan(_lower, _upper)));
        for imm_memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(imm_memtable.scan(_lower, _upper)));
        }

        let lower_bound = KeyBytes::from_bytes(get_bound_inner(_lower.clone()));
        for sst_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables.get(sst_id).unwrap().clone();
            if range_overlap(
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
                _lower,
                _upper,
            ) {
                sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    lower_bound.as_key_slice(),
                )?));
            }
        }
        let upper_bound = map_bound(_upper.clone());
        let final_iter = FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                MergeIterator::create(memtable_iters),
                MergeIterator::create(sst_iters),
            )?,
            upper_bound,
        )?);
        Ok(final_iter)
    }
}

// return whether start_key <= key <= end_key
fn key_within(key: &[u8], start_key: KeySlice, end_key: KeySlice) -> bool {
    return start_key.raw_ref() <= key && key <= end_key.raw_ref();
}

fn range_overlap(
    start_key: KeySlice,
    end_key: KeySlice,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> bool {
    match lower {
        Bound::Included(lower) => {
            if lower > end_key.raw_ref() {
                return false;
            }
        }
        Bound::Excluded(lower) => {
            if lower >= end_key.raw_ref() {
                return false;
            }
        }
        Bound::Unbounded => {}
    }
    match upper {
        Bound::Included(upper) => {
            if upper < start_key.raw_ref() {
                return false;
            }
        }
        Bound::Excluded(upper) => {
            if upper <= start_key.raw_ref() {
                return false;
            }
        }
        Bound::Unbounded => {}
    }
    true
}
