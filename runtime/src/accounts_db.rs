//! Persistent accounts are stored in below path location:
//!  <path>/<pid>/data/
//!
//! The persistent store would allow for this mode of operation:
//!  - Concurrent single thread append with many concurrent readers.
//!
//! The underlying memory is memory mapped to a file. The accounts would be
//! stored across multiple files and the mappings of file and offset of a
//! particular account would be stored in a shared index. This will allow for
//! concurrent commits without blocking reads, which will sequentially write
//! to memory, ssd or disk, and should be as fast as the hardware allow for.
//! The only required in memory data structure with a write lock is the index,
//! which should be fast to update.
//!
//! AppendVec's only store accounts for single slots.  To bootstrap the
//! index from a persistent store of AppendVec's, the entries include
//! a "write_version".  A single global atomic `AccountsDb::write_version`
//! tracks the number of commits to the entire data store. So the latest
//! commit for each slot entry would be indexed.

use crate::{
    accounts_background_service::{DroppedSlotsSender, SendDroppedBankCallback},
    accounts_cache::{AccountsCache, CachedAccount, SlotCache},
    accounts_hash::{AccountsHash, CalculateHashIntermediate, HashStats, PreviousPass},
    accounts_index::{
        AccountIndexGetResult, AccountSecondaryIndexes, AccountsIndex, AccountsIndexRootsStats,
        IndexKey, IsCached, ScanResult, SlotList, SlotSlice, ZeroLamport,
    },
    ancestors::Ancestors,
    append_vec::{AppendVec, StoredAccountMeta, StoredMeta, StoredMetaWriteVersion},
    contains::Contains,
    pubkey_bins::PubkeyBinCalculator16,
    read_only_accounts_cache::ReadOnlyAccountsCache,
    sorted_storages::SortedStorages,
};
use blake3::traits::digest::Digest;
use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::{
    mapref::entry::Entry::{Occupied, Vacant},
    DashMap, DashSet,
};
use lazy_static::lazy_static;
use log::*;
use rand::{prelude::SliceRandom, thread_rng, Rng};
use rayon::{prelude::*, ThreadPool};
use serde::{Deserialize, Serialize};
use solana_measure::measure::Measure;
use solana_rayon_threadlimit::get_thread_count;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    clock::{BankId, Epoch, Slot},
    genesis_config::ClusterType,
    hash::{Hash, Hasher},
    pubkey::Pubkey,
};
use solana_vote_program::vote_state::MAX_LOCKOUT_HISTORY;
use std::{
    borrow::{Borrow, Cow},
    boxed::Box,
    collections::{hash_map::Entry, BTreeSet, HashMap, HashSet},
    convert::TryFrom,
    io::{Error as IoError, Result as IoResult},
    ops::{Range, RangeBounds},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    sync::{Arc, Condvar, Mutex, MutexGuard, RwLock},
    thread::Builder,
    time::Instant,
};
use tempfile::TempDir;

#[cfg(test)]
use std::{thread::sleep, time::Duration};

const PAGE_SIZE: u64 = 4 * 1024;
const MAX_RECYCLE_STORES: usize = 1000;
const STORE_META_OVERHEAD: usize = 256;
const MAX_CACHE_SLOTS: usize = 200;
const FLUSH_CACHE_RANDOM_THRESHOLD: usize = MAX_LOCKOUT_HISTORY;
const SCAN_SLOT_PAR_ITER_THRESHOLD: usize = 4000;

pub const DEFAULT_FILE_SIZE: u64 = PAGE_SIZE * 1024;
pub const DEFAULT_NUM_THREADS: u32 = 8;
pub const DEFAULT_NUM_DIRS: u32 = 4;

// A specially reserved storage id just for entries in the cache, so that
// operations that take a storage entry can maintain a common interface
// when interacting with cached accounts. This id is "virtual" in that it
// doesn't actually refer to an actual storage entry.
const CACHE_VIRTUAL_STORAGE_ID: usize = AppendVecId::MAX;

// A specially reserved write version (identifier for ordering writes in an AppendVec)
// for entries in the cache, so that  operations that take a storage entry can maintain
// a common interface when interacting with cached accounts. This version is "virtual" in
// that it doesn't actually map to an entry in an AppendVec.
const CACHE_VIRTUAL_WRITE_VERSION: StoredMetaWriteVersion = 0;

// A specially reserved offset (represents an offset into an AppendVec)
// for entries in the cache, so that  operations that take a storage entry can maintain
// a common interface when interacting with cached accounts. This version is "virtual" in
// that it doesn't actually map to an entry in an AppendVec.
const CACHE_VIRTUAL_OFFSET: usize = 0;
const CACHE_VIRTUAL_STORED_SIZE: usize = 0;

#[cfg(not(test))]
const ABSURD_CONSECUTIVE_FAILED_ITERATIONS: usize = 100;

type DashMapVersionHash = DashMap<Pubkey, (u64, Hash)>;

lazy_static! {
    // FROZEN_ACCOUNT_PANIC is used to signal local_cluster that an AccountsDb panic has occurred,
    // as |cargo test| cannot observe panics in other threads
    pub static ref FROZEN_ACCOUNT_PANIC: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
}

#[derive(Debug, Clone, Copy)]
pub enum AccountShrinkThreshold {
    /// Measure the total space sparseness across all candididates
    /// And select the candidiates by using the top sparse account storage entries to shrink.
    /// The value is the overall shrink threshold measured as ratio of the total live bytes
    /// over the total bytes.
    TotalSpace { shrink_ratio: f64 },
    /// Use the following option to shrink all stores whose alive ratio is below
    /// the specified threshold.
    IndividalStore { shrink_ratio: f64 },
}
pub const DEFAULT_ACCOUNTS_SHRINK_OPTIMIZE_TOTAL_SPACE: bool = true;
pub const DEFAULT_ACCOUNTS_SHRINK_RATIO: f64 = 0.80;
// The default extra account space in percentage from the ideal target
const DEFAULT_ACCOUNTS_SHRINK_THRESHOLD_OPTION: AccountShrinkThreshold =
    AccountShrinkThreshold::TotalSpace {
        shrink_ratio: DEFAULT_ACCOUNTS_SHRINK_RATIO,
    };

impl Default for AccountShrinkThreshold {
    fn default() -> AccountShrinkThreshold {
        DEFAULT_ACCOUNTS_SHRINK_THRESHOLD_OPTION
    }
}

pub enum ScanStorageResult<R, B> {
    Cached(Vec<R>),
    Stored(B),
}

#[derive(Debug, Default)]
pub struct ErrorCounters {
    pub total: usize,
    pub account_in_use: usize,
    pub account_loaded_twice: usize,
    pub account_not_found: usize,
    pub blockhash_not_found: usize,
    pub blockhash_too_old: usize,
    pub call_chain_too_deep: usize,
    pub already_processed: usize,
    pub instruction_error: usize,
    pub insufficient_funds: usize,
    pub invalid_account_for_fee: usize,
    pub invalid_account_index: usize,
    pub invalid_program_for_execution: usize,
    pub not_allowed_during_cluster_maintenance: usize,
}

#[derive(Default, Debug)]
struct GenerateIndexTimings {
    pub index_time: u64,
    pub scan_time: u64,
    pub insertion_time_us: u64,
    pub min_bin_size: usize,
    pub max_bin_size: usize,
    pub total_items: usize,
    pub storage_size_accounts_map_us: u64,
    pub storage_size_storages_us: u64,
    pub storage_size_accounts_map_flatten_us: u64,
}

impl GenerateIndexTimings {
    pub fn report(&self) {
        datapoint_info!(
            "generate_index",
            // we cannot accurately measure index insertion time because of many threads and lock contention
            ("total_us", self.index_time, i64),
            ("scan_stores_us", self.scan_time, i64),
            ("insertion_time_us", self.insertion_time_us, i64),
            ("min_bin_size", self.min_bin_size as i64, i64),
            ("max_bin_size", self.max_bin_size as i64, i64),
            (
                "storage_size_accounts_map_us",
                self.storage_size_accounts_map_us as i64,
                i64
            ),
            (
                "storage_size_storages_us",
                self.storage_size_storages_us as i64,
                i64
            ),
            (
                "storage_size_accounts_map_flatten_us",
                self.storage_size_accounts_map_flatten_us as i64,
                i64
            ),
        );
    }
}

#[derive(Default, Debug, PartialEq, Clone)]
pub struct AccountInfo {
    /// index identifying the append storage
    store_id: AppendVecId,

    /// offset into the storage
    offset: usize,

    /// needed to track shrink candidacy in bytes. Used to update the number
    /// of alive bytes in an AppendVec as newer slots purge outdated entries
    stored_size: usize,

    /// lamports in the account used when squashing kept for optimization
    /// purposes to remove accounts with zero balance.
    lamports: u64,
}
impl IsCached for AccountInfo {
    fn is_cached(&self) -> bool {
        self.store_id == CACHE_VIRTUAL_STORAGE_ID
    }
}

impl ZeroLamport for AccountInfo {
    fn is_zero_lamport(&self) -> bool {
        self.lamports == 0
    }
}

struct MultiThreadProgress<'a> {
    last_update: Instant,
    my_last_report_count: u64,
    total_count: &'a AtomicU64,
    report_delay_secs: u64,
    first_caller: bool,
    ultimate_count: u64,
}

impl<'a> MultiThreadProgress<'a> {
    fn new(total_count: &'a AtomicU64, report_delay_secs: u64, ultimate_count: u64) -> Self {
        Self {
            last_update: Instant::now(),
            my_last_report_count: 0,
            total_count,
            report_delay_secs,
            first_caller: false,
            ultimate_count,
        }
    }
    fn report(&mut self, my_current_count: u64) {
        let now = Instant::now();
        if now.duration_since(self.last_update).as_secs() >= self.report_delay_secs {
            let my_total_newly_processed_slots_since_last_report =
                my_current_count - self.my_last_report_count;

            self.my_last_report_count = my_current_count;
            let previous_total_processed_slots_across_all_threads = self.total_count.fetch_add(
                my_total_newly_processed_slots_since_last_report,
                Ordering::Relaxed,
            );
            self.first_caller =
                self.first_caller || 0 == previous_total_processed_slots_across_all_threads;
            if self.first_caller {
                info!(
                    "generating index: {}/{} slots...",
                    previous_total_processed_slots_across_all_threads
                        + my_total_newly_processed_slots_since_last_report,
                    self.ultimate_count
                );
            }
            self.last_update = now;
        }
    }
}

/// An offset into the AccountsDb::storage vector
pub type AppendVecId = usize;
pub type SnapshotStorage = Vec<Arc<AccountStorageEntry>>;
pub type SnapshotStorages = Vec<SnapshotStorage>;

// Each slot has a set of storage entries.
pub(crate) type SlotStores = Arc<RwLock<HashMap<usize, Arc<AccountStorageEntry>>>>;

type AccountSlots = HashMap<Pubkey, HashSet<Slot>>;
type AppendVecOffsets = HashMap<AppendVecId, HashSet<usize>>;
type ReclaimResult = (AccountSlots, AppendVecOffsets);
type StorageFinder<'a> = Box<dyn Fn(Slot, usize) -> Arc<AccountStorageEntry> + 'a>;
type ShrinkCandidates = HashMap<Slot, HashMap<AppendVecId, Arc<AccountStorageEntry>>>;

trait Versioned {
    fn version(&self) -> u64;
}

impl Versioned for (u64, Hash) {
    fn version(&self) -> u64 {
        self.0
    }
}

impl Versioned for (u64, AccountInfo) {
    fn version(&self) -> u64 {
        self.0
    }
}

// Some hints for applicability of additional sanity checks for the do_load fast-path;
// Slower fallback code path will be taken if the fast path has failed over the retry
// threshold, regardless of these hints. Also, load cannot fail not-deterministically
// even under very rare circumstances, unlike previously did allow.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LoadHint {
    // Caller hints that it's loading transactions for a block which is
    // descended from the current root, and at the tip of its fork.
    // Thereby, further this assumes AccountIndex::max_root should not increase
    // during this load, meaning there should be no squash.
    // Overall, this enables us to assert!() strictly while running the fast-path for
    // account loading, while maintaining the determinism of account loading and resultant
    // transaction execution thereof.
    FixedMaxRoot,
    // Caller can't hint the above safety assumption. Generally RPC and miscellaneous
    // other call-site falls into this category. The likelihood of slower path is slightly
    // increased as well.
    Unspecified,
}

#[derive(Debug)]
pub enum LoadedAccountAccessor<'a> {
    // StoredAccountMeta can't be held directly here due to its lifetime dependency to
    // AccountStorageEntry
    Stored(Option<(Arc<AccountStorageEntry>, usize)>),
    // None value in Cached variant means the cache was flushed
    Cached(Option<(Pubkey, Cow<'a, CachedAccount>)>),
}

impl<'a> LoadedAccountAccessor<'a> {
    fn check_and_get_loaded_account(&mut self) -> LoadedAccount {
        // all of these following .expect() and .unwrap() are like serious logic errors,
        // ideal for representing this as rust type system....

        match self {
            LoadedAccountAccessor::Cached(None) | LoadedAccountAccessor::Stored(None) => {
                panic!("Should have already been taken care of when creating this LoadedAccountAccessor");
            }
            LoadedAccountAccessor::Cached(Some(_cached_account)) => {
                // Cached(Some(x)) variant always produces `Some` for get_loaded_account() since
                // it just returns the inner `x` without additional fetches
                self.get_loaded_account().unwrap()
            }
            LoadedAccountAccessor::Stored(Some(_maybe_storage_entry)) => {
                // If we do find the storage entry, we can guarantee that the storage entry is
                // safe to read from because we grabbed a reference to the storage entry while it
                // was still in the storage map. This means even if the storage entry is removed
                // from the storage map after we grabbed the storage entry, the recycler should not
                // reset the storage entry until we drop the reference to the storage entry.
                self.get_loaded_account()
                    .expect("If a storage entry was found in the storage map, it must not have been reset yet")
            }
        }
    }

    fn get_loaded_account(&mut self) -> Option<LoadedAccount> {
        match self {
            LoadedAccountAccessor::Cached(cached_account) => {
                let cached_account: (Pubkey, Cow<'a, CachedAccount>) =
                    cached_account.take().expect(
                        "Cache flushed/purged should be handled before trying to fetch account",
                    );
                Some(LoadedAccount::Cached(cached_account))
            }
            LoadedAccountAccessor::Stored(maybe_storage_entry) => {
                // storage entry may not be present if slot was cleaned up in
                // between reading the accounts index and calling this function to
                // get account meta from the storage entry here
                maybe_storage_entry
                    .as_ref()
                    .and_then(|(storage_entry, offset)| {
                        storage_entry
                            .get_stored_account_meta(*offset)
                            .map(LoadedAccount::Stored)
                    })
            }
        }
    }
}

pub enum LoadedAccount<'a> {
    Stored(StoredAccountMeta<'a>),
    Cached((Pubkey, Cow<'a, CachedAccount>)),
}

impl<'a> LoadedAccount<'a> {
    pub fn owner(&self) -> &Pubkey {
        match self {
            LoadedAccount::Stored(stored_account_meta) => &stored_account_meta.account_meta.owner,
            LoadedAccount::Cached((_, cached_account)) => cached_account.account.owner(),
        }
    }

    pub fn executable(&self) -> bool {
        match self {
            LoadedAccount::Stored(stored_account_meta) => {
                stored_account_meta.account_meta.executable
            }
            LoadedAccount::Cached((_, cached_account)) => cached_account.account.executable(),
        }
    }

    pub fn loaded_hash(&self) -> Hash {
        match self {
            LoadedAccount::Stored(stored_account_meta) => *stored_account_meta.hash,
            LoadedAccount::Cached((_, cached_account)) => cached_account.hash(),
        }
    }

    pub fn pubkey(&self) -> &Pubkey {
        match self {
            LoadedAccount::Stored(stored_account_meta) => &stored_account_meta.meta.pubkey,
            LoadedAccount::Cached((pubkey, _)) => pubkey,
        }
    }

    pub fn write_version(&self) -> StoredMetaWriteVersion {
        match self {
            LoadedAccount::Stored(stored_account_meta) => stored_account_meta.meta.write_version,
            LoadedAccount::Cached(_) => CACHE_VIRTUAL_WRITE_VERSION,
        }
    }

    pub fn compute_hash(&self, slot: Slot, pubkey: &Pubkey) -> Hash {
        match self {
            LoadedAccount::Stored(stored_account_meta) => {
                AccountsDb::hash_stored_account(slot, stored_account_meta)
            }
            LoadedAccount::Cached((_, cached_account)) => {
                AccountsDb::hash_account(slot, &cached_account.account, pubkey)
            }
        }
    }

    pub fn stored_size(&self) -> usize {
        match self {
            LoadedAccount::Stored(stored_account_meta) => stored_account_meta.stored_size,
            LoadedAccount::Cached(_) => CACHE_VIRTUAL_STORED_SIZE,
        }
    }

    pub fn lamports(&self) -> u64 {
        match self {
            LoadedAccount::Stored(stored_account_meta) => stored_account_meta.account_meta.lamports,
            LoadedAccount::Cached((_, cached_account)) => cached_account.account.lamports(),
        }
    }

    pub fn take_account(self) -> AccountSharedData {
        match self {
            LoadedAccount::Stored(stored_account_meta) => stored_account_meta.clone_account(),
            LoadedAccount::Cached((_, cached_account)) => match cached_account {
                Cow::Owned(cached_account) => cached_account.account.clone(),
                Cow::Borrowed(cached_account) => cached_account.account.clone(),
            },
        }
    }

    pub fn is_cached(&self) -> bool {
        match self {
            LoadedAccount::Stored(_) => false,
            LoadedAccount::Cached(_) => true,
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct AccountStorage(pub DashMap<Slot, SlotStores>);

impl AccountStorage {
    fn get_account_storage_entry(
        &self,
        slot: Slot,
        store_id: AppendVecId,
    ) -> Option<Arc<AccountStorageEntry>> {
        self.get_slot_stores(slot)
            .and_then(|storage_map| storage_map.read().unwrap().get(&store_id).cloned())
    }

    fn get_slot_stores(&self, slot: Slot) -> Option<SlotStores> {
        self.0.get(&slot).map(|result| result.value().clone())
    }

    fn get_slot_storage_entries(&self, slot: Slot) -> Option<Vec<Arc<AccountStorageEntry>>> {
        self.get_slot_stores(slot)
            .map(|res| res.read().unwrap().values().cloned().collect())
    }

    fn slot_store_count(&self, slot: Slot, store_id: AppendVecId) -> Option<usize> {
        self.get_account_storage_entry(slot, store_id)
            .map(|store| store.count())
    }

    fn all_slots(&self) -> Vec<Slot> {
        self.0.iter().map(|iter_item| *iter_item.key()).collect()
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Deserialize, Serialize, AbiExample, AbiEnumVisitor)]
pub enum AccountStorageStatus {
    Available = 0,
    Full = 1,
    Candidate = 2,
}

impl Default for AccountStorageStatus {
    fn default() -> Self {
        Self::Available
    }
}

#[derive(Debug)]
pub enum BankHashVerificationError {
    MismatchedAccountHash,
    MismatchedBankHash,
    MissingBankHash,
    MismatchedTotalLamports(u64, u64),
}

#[derive(Default)]
struct CleanKeyTimings {
    collect_delta_keys_us: u64,
    delta_insert_us: u64,
    hashset_to_vec_us: u64,
    dirty_store_processing_us: u64,
    delta_key_count: u64,
    dirty_pubkeys_count: u64,
}

/// Persistent storage structure holding the accounts
#[derive(Debug)]
pub struct AccountStorageEntry {
    pub(crate) id: AtomicUsize,

    pub(crate) slot: AtomicU64,

    /// storage holding the accounts
    pub(crate) accounts: AppendVec,

    /// Keeps track of the number of accounts stored in a specific AppendVec.
    ///  This is periodically checked to reuse the stores that do not have
    ///  any accounts in it
    /// status corresponding to the storage, lets us know that
    ///  the append_vec, once maxed out, then emptied, can be reclaimed
    count_and_status: RwLock<(usize, AccountStorageStatus)>,

    /// This is the total number of accounts stored ever since initialized to keep
    /// track of lifetime count of all store operations. And this differs from
    /// count_and_status in that this field won't be decremented.
    ///
    /// This is used as a rough estimate for slot shrinking. As such a relaxed
    /// use case, this value ARE NOT strictly synchronized with count_and_status!
    approx_store_count: AtomicUsize,

    alive_bytes: AtomicUsize,
}

impl AccountStorageEntry {
    pub fn new(path: &Path, slot: Slot, id: usize, file_size: u64) -> Self {
        let tail = AppendVec::file_name(slot, id);
        let path = Path::new(path).join(tail);
        let accounts = AppendVec::new(&path, true, file_size as usize);

        Self {
            id: AtomicUsize::new(id),
            slot: AtomicU64::new(slot),
            accounts,
            count_and_status: RwLock::new((0, AccountStorageStatus::Available)),
            approx_store_count: AtomicUsize::new(0),
            alive_bytes: AtomicUsize::new(0),
        }
    }

    pub(crate) fn new_existing(
        slot: Slot,
        id: AppendVecId,
        accounts: AppendVec,
        num_accounts: usize,
    ) -> Self {
        Self {
            id: AtomicUsize::new(id),
            slot: AtomicU64::new(slot),
            accounts,
            count_and_status: RwLock::new((0, AccountStorageStatus::Available)),
            approx_store_count: AtomicUsize::new(num_accounts),
            alive_bytes: AtomicUsize::new(0),
        }
    }

    pub fn set_status(&self, mut status: AccountStorageStatus) {
        let mut count_and_status = self.count_and_status.write().unwrap();

        let count = count_and_status.0;

        if status == AccountStorageStatus::Full && count == 0 {
            // this case arises when the append_vec is full (store_ptrs fails),
            //  but all accounts have already been removed from the storage
            //
            // the only time it's safe to call reset() on an append_vec is when
            //  every account has been removed
            //          **and**
            //  the append_vec has previously been completely full
            //
            self.accounts.reset();
            status = AccountStorageStatus::Available;
        }

        *count_and_status = (count, status);
    }

    pub fn recycle(&self, slot: Slot, id: usize) {
        let mut count_and_status = self.count_and_status.write().unwrap();
        self.accounts.reset();
        *count_and_status = (0, AccountStorageStatus::Available);
        self.slot.store(slot, Ordering::Release);
        self.id.store(id, Ordering::Relaxed);
        self.approx_store_count.store(0, Ordering::Relaxed);
        self.alive_bytes.store(0, Ordering::Relaxed);
    }

    pub fn status(&self) -> AccountStorageStatus {
        self.count_and_status.read().unwrap().1
    }

    pub fn count(&self) -> usize {
        self.count_and_status.read().unwrap().0
    }

    pub fn approx_stored_count(&self) -> usize {
        self.approx_store_count.load(Ordering::Relaxed)
    }

    pub fn alive_bytes(&self) -> usize {
        self.alive_bytes.load(Ordering::SeqCst)
    }

    pub fn written_bytes(&self) -> u64 {
        self.accounts.len() as u64
    }

    pub fn total_bytes(&self) -> u64 {
        self.accounts.capacity()
    }

    pub fn has_accounts(&self) -> bool {
        self.count() > 0
    }

    pub fn slot(&self) -> Slot {
        self.slot.load(Ordering::Acquire)
    }

    pub fn append_vec_id(&self) -> AppendVecId {
        self.id.load(Ordering::Relaxed)
    }

    pub fn flush(&self) -> Result<(), IoError> {
        self.accounts.flush()
    }

    fn get_stored_account_meta(&self, offset: usize) -> Option<StoredAccountMeta> {
        Some(self.accounts.get_account(offset)?.0)
    }

    fn add_account(&self, num_bytes: usize) {
        let mut count_and_status = self.count_and_status.write().unwrap();
        *count_and_status = (count_and_status.0 + 1, count_and_status.1);
        self.approx_store_count.fetch_add(1, Ordering::Relaxed);
        self.alive_bytes.fetch_add(num_bytes, Ordering::SeqCst);
    }

    fn try_available(&self) -> bool {
        let mut count_and_status = self.count_and_status.write().unwrap();
        let (count, status) = *count_and_status;

        if status == AccountStorageStatus::Available {
            *count_and_status = (count, AccountStorageStatus::Candidate);
            true
        } else {
            false
        }
    }

    pub fn all_accounts(&self) -> Vec<StoredAccountMeta> {
        self.accounts.accounts(0)
    }

    fn remove_account(&self, num_bytes: usize, reset_accounts: bool) -> usize {
        let mut count_and_status = self.count_and_status.write().unwrap();
        let (mut count, mut status) = *count_and_status;

        if count == 1 && status == AccountStorageStatus::Full && reset_accounts {
            // this case arises when we remove the last account from the
            //  storage, but we've learned from previous write attempts that
            //  the storage is full
            //
            // the only time it's safe to call reset() on an append_vec is when
            //  every account has been removed
            //          **and**
            //  the append_vec has previously been completely full
            //
            // otherwise, the storage may be in flight with a store()
            //   call
            self.accounts.reset();
            status = AccountStorageStatus::Available;
        }

        // Some code path is removing accounts too many; this may result in an
        // unintended reveal of old state for unrelated accounts.
        assert!(
            count > 0,
            "double remove of account in slot: {}/store: {}!!",
            self.slot(),
            self.append_vec_id(),
        );

        self.alive_bytes.fetch_sub(num_bytes, Ordering::SeqCst);
        count -= 1;
        *count_and_status = (count, status);
        count
    }

    pub fn get_path(&self) -> PathBuf {
        self.accounts.get_path()
    }
}

pub fn get_temp_accounts_paths(count: u32) -> IoResult<(Vec<TempDir>, Vec<PathBuf>)> {
    let temp_dirs: IoResult<Vec<TempDir>> = (0..count).map(|_| TempDir::new()).collect();
    let temp_dirs = temp_dirs?;
    let paths: Vec<PathBuf> = temp_dirs.iter().map(|t| t.path().to_path_buf()).collect();
    Ok((temp_dirs, paths))
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, AbiExample)]
pub struct BankHashStats {
    pub num_updated_accounts: u64,
    pub num_removed_accounts: u64,
    pub num_lamports_stored: u64,
    pub total_data_len: u64,
    pub num_executable_accounts: u64,
}

impl BankHashStats {
    pub fn update<T: ReadableAccount>(&mut self, account: &T) {
        if account.lamports() == 0 {
            self.num_removed_accounts += 1;
        } else {
            self.num_updated_accounts += 1;
        }
        self.total_data_len = self
            .total_data_len
            .wrapping_add(account.data().len() as u64);
        if account.executable() {
            self.num_executable_accounts += 1;
        }
        self.num_lamports_stored = self.num_lamports_stored.wrapping_add(account.lamports());
    }

    pub fn merge(&mut self, other: &BankHashStats) {
        self.num_updated_accounts += other.num_updated_accounts;
        self.num_removed_accounts += other.num_removed_accounts;
        self.total_data_len = self.total_data_len.wrapping_add(other.total_data_len);
        self.num_lamports_stored = self
            .num_lamports_stored
            .wrapping_add(other.num_lamports_stored);
        self.num_executable_accounts += other.num_executable_accounts;
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, AbiExample)]
pub struct BankHashInfo {
    pub hash: Hash,
    pub snapshot_hash: Hash,
    pub stats: BankHashStats,
}

#[derive(Debug)]
struct FrozenAccountInfo {
    pub hash: Hash,    // Hash generated by hash_frozen_account_data()
    pub lamports: u64, // Account balance cannot be lower than this amount
}

#[derive(Default)]
pub struct StoreAccountsTiming {
    store_accounts_elapsed: u64,
    update_index_elapsed: u64,
    handle_reclaims_elapsed: u64,
}

#[derive(Debug, Default)]
struct RecycleStores {
    entries: Vec<(Instant, Arc<AccountStorageEntry>)>,
    total_bytes: u64,
}

// 30 min should be enough to be certain there won't be any prospective recycle uses for given
// store entry
// That's because it already processed ~2500 slots and ~25 passes of AccountsBackgroundService
pub const EXPIRATION_TTL_SECONDS: u64 = 1800;

impl RecycleStores {
    fn add_entry(&mut self, new_entry: Arc<AccountStorageEntry>) {
        self.total_bytes += new_entry.total_bytes();
        self.entries.push((Instant::now(), new_entry))
    }

    fn iter(&self) -> std::slice::Iter<(Instant, Arc<AccountStorageEntry>)> {
        self.entries.iter()
    }

    fn add_entries(&mut self, new_entries: Vec<Arc<AccountStorageEntry>>) {
        self.total_bytes += new_entries.iter().map(|e| e.total_bytes()).sum::<u64>();
        let now = Instant::now();
        for new_entry in new_entries {
            self.entries.push((now, new_entry));
        }
    }

    fn expire_old_entries(&mut self) -> Vec<Arc<AccountStorageEntry>> {
        let mut expired = vec![];
        let now = Instant::now();
        let mut expired_bytes = 0;
        self.entries.retain(|(recycled_time, entry)| {
            if now.duration_since(*recycled_time).as_secs() > EXPIRATION_TTL_SECONDS {
                if Arc::strong_count(entry) >= 2 {
                    warn!(
                        "Expiring still in-use recycled StorageEntry anyway...: id: {} slot: {}",
                        entry.append_vec_id(),
                        entry.slot(),
                    );
                }
                expired_bytes += entry.total_bytes();
                expired.push(entry.clone());
                false
            } else {
                true
            }
        });

        self.total_bytes -= expired_bytes;

        expired
    }

    fn remove_entry(&mut self, index: usize) -> Arc<AccountStorageEntry> {
        let (_added_time, removed_entry) = self.entries.swap_remove(index);
        self.total_bytes -= removed_entry.total_bytes();
        removed_entry
    }

    fn entry_count(&self) -> usize {
        self.entries.len()
    }

    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

/// Removing unrooted slots in Accounts Background Service needs to be synchronized with flushing
/// slots from the Accounts Cache.  This keeps track of those slots and the Mutex + Condvar for
/// synchronization.
#[derive(Debug, Default)]
struct RemoveUnrootedSlotsSynchronization {
    // slots being flushed from the cache or being purged
    slots_under_contention: Mutex<HashSet<Slot>>,
    signal: Condvar,
}

type AccountInfoAccountsIndex = AccountsIndex<AccountInfo>;

// This structure handles the load/store of the accounts
#[derive(Debug)]
pub struct AccountsDb {
    /// Keeps tracks of index into AppendVec on a per slot basis
    pub accounts_index: AccountInfoAccountsIndex,

    pub storage: AccountStorage,

    pub accounts_cache: AccountsCache,

    sender_bg_hasher: Option<Sender<CachedAccount>>,
    pub read_only_accounts_cache: ReadOnlyAccountsCache,

    recycle_stores: RwLock<RecycleStores>,

    /// distribute the accounts across storage lists
    pub next_id: AtomicUsize,

    /// Set of shrinkable stores organized by map of slot to append_vec_id
    pub shrink_candidate_slots: Mutex<ShrinkCandidates>,

    /// Legacy shrink slots to support non-cached code-path.
    pub shrink_candidate_slots_v1: Mutex<Vec<Slot>>,

    pub(crate) write_version: AtomicU64,

    /// Set of storage paths to pick from
    pub(crate) paths: Vec<PathBuf>,

    pub shrink_paths: RwLock<Option<Vec<PathBuf>>>,

    /// Directory of paths this accounts_db needs to hold/remove
    pub(crate) temp_paths: Option<Vec<TempDir>>,

    /// Starting file size of appendvecs
    file_size: u64,

    /// Accounts that will cause a panic! if data modified or lamports decrease
    frozen_accounts: HashMap<Pubkey, FrozenAccountInfo>,

    /// Thread pool used for par_iter
    pub thread_pool: ThreadPool,

    pub thread_pool_clean: ThreadPool,

    /// Number of append vecs to create to maximize parallelism when scanning
    /// the accounts
    min_num_stores: usize,

    pub bank_hashes: RwLock<HashMap<Slot, BankHashInfo>>,

    stats: AccountsStats,

    clean_accounts_stats: CleanAccountsStats,

    // Stats for purges called outside of clean_accounts()
    external_purge_slots_stats: PurgeStats,

    shrink_stats: ShrinkStats,

    pub cluster_type: Option<ClusterType>,

    pub account_indexes: AccountSecondaryIndexes,

    pub caching_enabled: bool,

    /// Set of unique keys per slot which is used
    /// to drive clean_accounts
    /// Generated by get_accounts_delta_hash
    uncleaned_pubkeys: DashMap<Slot, Vec<Pubkey>>,

    #[cfg(test)]
    load_delay: u64,

    #[cfg(test)]
    load_limit: AtomicU64,

    is_bank_drop_callback_enabled: AtomicBool,

    /// Set of slots currently being flushed by `flush_slot_cache()` or removed
    /// by `remove_unrooted_slot()`. Used to ensure `remove_unrooted_slots(slots)`
    /// can safely clear the set of unrooted slots `slots`.
    remove_unrooted_slots_synchronization: RemoveUnrootedSlotsSynchronization,

    shrink_ratio: AccountShrinkThreshold,

    /// Set of stores which are recently rooted or had accounts removed
    /// such that potentially a 0-lamport account update could be present which
    /// means we can remove the account from the index entirely.
    dirty_stores: DashMap<(Slot, AppendVecId), Arc<AccountStorageEntry>>,
}

#[derive(Debug, Default)]
struct AccountsStats {
    delta_hash_scan_time_total_us: AtomicU64,
    delta_hash_accumulate_time_total_us: AtomicU64,
    delta_hash_num: AtomicU64,

    last_store_report: AtomicU64,
    store_hash_accounts: AtomicU64,
    calc_stored_meta: AtomicU64,
    store_accounts: AtomicU64,
    store_update_index: AtomicU64,
    store_handle_reclaims: AtomicU64,
    store_append_accounts: AtomicU64,
    store_find_store: AtomicU64,
    store_num_accounts: AtomicU64,
    store_total_data: AtomicU64,
    recycle_store_count: AtomicU64,
    create_store_count: AtomicU64,
    store_get_slot_store: AtomicU64,
    store_find_existing: AtomicU64,
    dropped_stores: AtomicU64,
    store_uncleaned_update: AtomicU64,
}

#[derive(Debug, Default)]
struct PurgeStats {
    last_report: AtomicU64,
    safety_checks_elapsed: AtomicU64,
    remove_cache_elapsed: AtomicU64,
    remove_storage_entries_elapsed: AtomicU64,
    drop_storage_entries_elapsed: AtomicU64,
    num_cached_slots_removed: AtomicUsize,
    num_stored_slots_removed: AtomicUsize,
    total_removed_storage_entries: AtomicUsize,
    total_removed_cached_bytes: AtomicU64,
    total_removed_stored_bytes: AtomicU64,
    recycle_stores_write_elapsed: AtomicU64,
    scan_storages_elasped: AtomicU64,
    purge_accounts_index_elapsed: AtomicU64,
    handle_reclaims_elapsed: AtomicU64,
}

impl PurgeStats {
    fn report(&self, metric_name: &'static str, report_interval_ms: Option<u64>) {
        let should_report = report_interval_ms
            .map(|report_interval_ms| {
                let last = self.last_report.load(Ordering::Relaxed);
                let now = solana_sdk::timing::timestamp();
                now.saturating_sub(last) > report_interval_ms
                    && self.last_report.compare_exchange(
                        last,
                        now,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) == Ok(last)
                    && last != 0
            })
            .unwrap_or(true);

        if should_report {
            datapoint_info!(
                metric_name,
                (
                    "safety_checks_elapsed",
                    self.safety_checks_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "remove_cache_elapsed",
                    self.remove_cache_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "remove_storage_entries_elapsed",
                    self.remove_storage_entries_elapsed
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "drop_storage_entries_elapsed",
                    self.drop_storage_entries_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "num_cached_slots_removed",
                    self.num_cached_slots_removed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "num_stored_slots_removed",
                    self.num_stored_slots_removed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "total_removed_storage_entries",
                    self.total_removed_storage_entries
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "total_removed_cached_bytes",
                    self.total_removed_cached_bytes.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "total_removed_stored_bytes",
                    self.total_removed_stored_bytes.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "recycle_stores_write_elapsed",
                    self.recycle_stores_write_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "scan_storages_elasped",
                    self.scan_storages_elasped.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "purge_accounts_index_elapsed",
                    self.purge_accounts_index_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "handle_reclaims_elapsed",
                    self.handle_reclaims_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
            );
        }
    }
}

#[derive(Debug)]
struct FlushStats {
    slot: Slot,
    num_flushed: usize,
    num_purged: usize,
    total_size: u64,
}

#[derive(Debug, Default)]
struct LatestAccountsIndexRootsStats {
    roots_len: AtomicUsize,
    uncleaned_roots_len: AtomicUsize,
    previous_uncleaned_roots_len: AtomicUsize,
    roots_range: AtomicU64,
    rooted_cleaned_count: AtomicUsize,
    unrooted_cleaned_count: AtomicUsize,
}

impl LatestAccountsIndexRootsStats {
    fn update(&self, accounts_index_roots_stats: &AccountsIndexRootsStats) {
        self.roots_len
            .store(accounts_index_roots_stats.roots_len, Ordering::Relaxed);
        self.uncleaned_roots_len.store(
            accounts_index_roots_stats.uncleaned_roots_len,
            Ordering::Relaxed,
        );
        self.previous_uncleaned_roots_len.store(
            accounts_index_roots_stats.previous_uncleaned_roots_len,
            Ordering::Relaxed,
        );
        self.roots_range
            .store(accounts_index_roots_stats.roots_range, Ordering::Relaxed);
        self.rooted_cleaned_count.fetch_add(
            accounts_index_roots_stats.rooted_cleaned_count,
            Ordering::Relaxed,
        );
        self.unrooted_cleaned_count.fetch_add(
            accounts_index_roots_stats.unrooted_cleaned_count,
            Ordering::Relaxed,
        );
    }

    fn report(&self) {
        datapoint_info!(
            "accounts_index_roots_len",
            (
                "roots_len",
                self.roots_len.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "uncleaned_roots_len",
                self.uncleaned_roots_len.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "previous_uncleaned_roots_len",
                self.previous_uncleaned_roots_len.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "roots_range_width",
                self.roots_range.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "unrooted_cleaned_count",
                self.unrooted_cleaned_count.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "rooted_cleaned_count",
                self.rooted_cleaned_count.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
        );

        // Don't need to reset since this tracks the latest updates, not a cumulative total
    }
}

#[derive(Debug, Default)]
struct CleanAccountsStats {
    purge_stats: PurgeStats,
    latest_accounts_index_roots_stats: LatestAccountsIndexRootsStats,
}

impl CleanAccountsStats {
    fn report(&self) {
        self.purge_stats.report("clean_purge_slots_stats", None);
        self.latest_accounts_index_roots_stats.report();
    }
}

#[derive(Debug, Default)]
struct ShrinkStats {
    last_report: AtomicU64,
    num_slots_shrunk: AtomicUsize,
    storage_read_elapsed: AtomicU64,
    index_read_elapsed: AtomicU64,
    find_alive_elapsed: AtomicU64,
    create_and_insert_store_elapsed: AtomicU64,
    store_accounts_elapsed: AtomicU64,
    update_index_elapsed: AtomicU64,
    handle_reclaims_elapsed: AtomicU64,
    write_storage_elapsed: AtomicU64,
    rewrite_elapsed: AtomicU64,
    drop_storage_entries_elapsed: AtomicU64,
    recycle_stores_write_elapsed: AtomicU64,
    accounts_removed: AtomicUsize,
    bytes_removed: AtomicU64,
    bytes_written: AtomicU64,
    skipped_shrink: AtomicU64,
}

impl ShrinkStats {
    fn report(&self) {
        let last = self.last_report.load(Ordering::Relaxed);
        let now = solana_sdk::timing::timestamp();

        // last is initialized to 0 by ::default()
        // thus, the first 'report' call would always log.
        // Instead, the first call now initialializes 'last_report' to now.
        let is_first_call = last == 0;
        let should_report = now.saturating_sub(last) > 1000
            && self
                .last_report
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                == Ok(last);

        if !is_first_call && should_report {
            datapoint_info!(
                "shrink_stats",
                (
                    "num_slots_shrunk",
                    self.num_slots_shrunk.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "storage_read_elapsed",
                    self.storage_read_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "index_read_elapsed",
                    self.index_read_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "find_alive_elapsed",
                    self.find_alive_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "create_and_insert_store_elapsed",
                    self.create_and_insert_store_elapsed
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "store_accounts_elapsed",
                    self.store_accounts_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "update_index_elapsed",
                    self.update_index_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "handle_reclaims_elapsed",
                    self.handle_reclaims_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "write_storage_elapsed",
                    self.write_storage_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "rewrite_elapsed",
                    self.rewrite_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "drop_storage_entries_elapsed",
                    self.drop_storage_entries_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "recycle_stores_write_time",
                    self.recycle_stores_write_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "accounts_removed",
                    self.accounts_removed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "bytes_removed",
                    self.bytes_removed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "bytes_written",
                    self.bytes_written.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "skipped_shrink",
                    self.skipped_shrink.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
            );
        }
    }
}

pub fn make_min_priority_thread_pool() -> ThreadPool {
    // Use lower thread count to reduce priority.
    let num_threads = std::cmp::max(2, num_cpus::get() / 4);
    rayon::ThreadPoolBuilder::new()
        .thread_name(|i| format!("solana-cleanup-accounts-{}", i))
        .num_threads(num_threads)
        .build()
        .unwrap()
}

#[cfg(all(test, RUSTC_WITH_SPECIALIZATION))]
impl solana_frozen_abi::abi_example::AbiExample for AccountsDb {
    fn example() -> Self {
        let accounts_db = AccountsDb::new_single();
        let key = Pubkey::default();
        let some_data_len = 5;
        let some_slot: Slot = 0;
        let account = AccountSharedData::new(1, some_data_len, &key);
        accounts_db.store_uncached(some_slot, &[(&key, &account)]);
        accounts_db.add_root(0);

        accounts_db
    }
}

impl<'a> ReadableAccount for StoredAccountMeta<'a> {
    fn lamports(&self) -> u64 {
        self.account_meta.lamports
    }
    fn data(&self) -> &[u8] {
        self.data
    }
    fn owner(&self) -> &Pubkey {
        &self.account_meta.owner
    }
    fn executable(&self) -> bool {
        self.account_meta.executable
    }
    fn rent_epoch(&self) -> Epoch {
        self.account_meta.rent_epoch
    }
}

impl Default for AccountsDb {
    fn default() -> Self {
        let num_threads = get_thread_count();
        const MAX_READ_ONLY_CACHE_DATA_SIZE: usize = 200_000_000;

        let mut bank_hashes = HashMap::new();
        bank_hashes.insert(0, BankHashInfo::default());
        AccountsDb {
            accounts_index: AccountsIndex::default(),
            storage: AccountStorage::default(),
            accounts_cache: AccountsCache::default(),
            sender_bg_hasher: None,
            read_only_accounts_cache: ReadOnlyAccountsCache::new(MAX_READ_ONLY_CACHE_DATA_SIZE),
            recycle_stores: RwLock::new(RecycleStores::default()),
            uncleaned_pubkeys: DashMap::new(),
            next_id: AtomicUsize::new(0),
            shrink_candidate_slots_v1: Mutex::new(Vec::new()),
            shrink_candidate_slots: Mutex::new(HashMap::new()),
            write_version: AtomicU64::new(0),
            paths: vec![],
            shrink_paths: RwLock::new(None),
            temp_paths: None,
            file_size: DEFAULT_FILE_SIZE,
            thread_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .thread_name(|i| format!("solana-db-accounts-{}", i))
                .build()
                .unwrap(),
            thread_pool_clean: make_min_priority_thread_pool(),
            min_num_stores: num_threads,
            bank_hashes: RwLock::new(bank_hashes),
            frozen_accounts: HashMap::new(),
            external_purge_slots_stats: PurgeStats::default(),
            clean_accounts_stats: CleanAccountsStats::default(),
            shrink_stats: ShrinkStats::default(),
            stats: AccountsStats::default(),
            cluster_type: None,
            account_indexes: AccountSecondaryIndexes::default(),
            caching_enabled: false,
            #[cfg(test)]
            load_delay: u64::default(),
            #[cfg(test)]
            load_limit: AtomicU64::default(),
            is_bank_drop_callback_enabled: AtomicBool::default(),
            remove_unrooted_slots_synchronization: RemoveUnrootedSlotsSynchronization::default(),
            shrink_ratio: AccountShrinkThreshold::default(),
            dirty_stores: DashMap::default(),
        }
    }
}

type GenerateIndexAccountsMap<'a> =
    HashMap<Pubkey, (StoredMetaWriteVersion, AppendVecId, StoredAccountMeta<'a>)>;

impl AccountsDb {
    pub fn new(paths: Vec<PathBuf>, cluster_type: &ClusterType) -> Self {
        AccountsDb::new_with_config(
            paths,
            cluster_type,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        )
    }

    pub fn new_with_config(
        paths: Vec<PathBuf>,
        cluster_type: &ClusterType,
        account_indexes: AccountSecondaryIndexes,
        caching_enabled: bool,
        shrink_ratio: AccountShrinkThreshold,
    ) -> Self {
        let mut new = if !paths.is_empty() {
            Self {
                paths,
                temp_paths: None,
                cluster_type: Some(*cluster_type),
                account_indexes,
                caching_enabled,
                shrink_ratio,
                ..Self::default()
            }
        } else {
            // Create a temporary set of accounts directories, used primarily
            // for testing
            let (temp_dirs, paths) = get_temp_accounts_paths(DEFAULT_NUM_DIRS).unwrap();
            Self {
                paths,
                temp_paths: Some(temp_dirs),
                cluster_type: Some(*cluster_type),
                account_indexes,
                caching_enabled,
                shrink_ratio,
                ..Self::default()
            }
        };

        new.start_background_hasher();
        {
            for path in new.paths.iter() {
                std::fs::create_dir_all(path).expect("Create directory failed.");
            }
        }
        new
    }

    pub fn set_shrink_paths(&self, paths: Vec<PathBuf>) {
        assert!(!paths.is_empty());
        let mut shrink_paths = self.shrink_paths.write().unwrap();
        for path in &paths {
            std::fs::create_dir_all(path).expect("Create directory failed.");
        }
        *shrink_paths = Some(paths);
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn new_single() -> Self {
        AccountsDb {
            min_num_stores: 0,
            ..AccountsDb::new(Vec::new(), &ClusterType::Development)
        }
    }

    fn new_storage_entry(&self, slot: Slot, path: &Path, size: u64) -> AccountStorageEntry {
        AccountStorageEntry::new(
            path,
            slot,
            self.next_id.fetch_add(1, Ordering::Relaxed),
            size,
        )
    }

    pub fn expected_cluster_type(&self) -> ClusterType {
        self.cluster_type
            .expect("Cluster type must be set at initialization")
    }

    /// Reclaim older states of accounts older than max_clean_root for AccountsDb bloat mitigation
    fn clean_accounts_older_than_root(
        &self,
        purges: Vec<Pubkey>,
        max_clean_root: Option<Slot>,
    ) -> ReclaimResult {
        if purges.is_empty() {
            return ReclaimResult::default();
        }
        // This number isn't carefully chosen; just guessed randomly such that
        // the hot loop will be the order of ~Xms.
        const INDEX_CLEAN_BULK_COUNT: usize = 4096;

        let mut clean_rooted = Measure::start("clean_old_root-ms");
        let reclaim_vecs = purges
            .par_chunks(INDEX_CLEAN_BULK_COUNT)
            .map(|pubkeys: &[Pubkey]| {
                let mut reclaims = Vec::new();
                for pubkey in pubkeys {
                    self.accounts_index
                        .clean_rooted_entries(pubkey, &mut reclaims, max_clean_root);
                }
                reclaims
            });
        let reclaims: Vec<_> = reclaim_vecs.flatten().collect();
        clean_rooted.stop();
        inc_new_counter_info!("clean-old-root-par-clean-ms", clean_rooted.as_ms() as usize);

        let mut measure = Measure::start("clean_old_root_reclaims");

        // Don't reset from clean, since the pubkeys in those stores may need to be unref'ed
        // and those stores may be used for background hashing.
        let reset_accounts = false;

        let mut reclaim_result = ReclaimResult::default();
        self.handle_reclaims(
            &reclaims,
            None,
            Some(&self.clean_accounts_stats.purge_stats),
            Some(&mut reclaim_result),
            reset_accounts,
        );
        measure.stop();
        debug!("{} {}", clean_rooted, measure);
        inc_new_counter_info!("clean-old-root-reclaim-ms", measure.as_ms() as usize);
        reclaim_result
    }

    fn do_reset_uncleaned_roots(&self, max_clean_root: Option<Slot>) {
        self.accounts_index.reset_uncleaned_roots(max_clean_root);
    }

    fn calc_delete_dependencies(
        purges: &HashMap<Pubkey, (SlotList<AccountInfo>, u64)>,
        store_counts: &mut HashMap<AppendVecId, (usize, HashSet<Pubkey>)>,
    ) {
        // Another pass to check if there are some filtered accounts which
        // do not match the criteria of deleting all appendvecs which contain them
        // then increment their storage count.
        let mut already_counted = HashSet::new();
        for (pubkey, (account_infos, ref_count_from_storage)) in purges.iter() {
            let no_delete = if account_infos.len() as u64 != *ref_count_from_storage {
                debug!(
                    "calc_delete_dependencies(),
                    pubkey: {},
                    account_infos: {:?},
                    account_infos_len: {},
                    ref_count_from_storage: {}",
                    pubkey,
                    account_infos,
                    account_infos.len(),
                    ref_count_from_storage,
                );
                true
            } else {
                let mut no_delete = false;
                for (_slot, account_info) in account_infos {
                    debug!(
                        "calc_delete_dependencies()
                        storage id: {},
                        count len: {}",
                        account_info.store_id,
                        store_counts.get(&account_info.store_id).unwrap().0,
                    );
                    if store_counts.get(&account_info.store_id).unwrap().0 != 0 {
                        no_delete = true;
                        break;
                    }
                }
                no_delete
            };
            if no_delete {
                let mut pending_store_ids: HashSet<usize> = HashSet::new();
                for (_bank_id, account_info) in account_infos {
                    if !already_counted.contains(&account_info.store_id) {
                        pending_store_ids.insert(account_info.store_id);
                    }
                }
                while !pending_store_ids.is_empty() {
                    let id = pending_store_ids.iter().next().cloned().unwrap();
                    pending_store_ids.remove(&id);
                    if already_counted.contains(&id) {
                        continue;
                    }
                    store_counts.get_mut(&id).unwrap().0 += 1;
                    already_counted.insert(id);

                    let affected_pubkeys = &store_counts.get(&id).unwrap().1;
                    for key in affected_pubkeys {
                        for (_slot, account_info) in &purges.get(key).unwrap().0 {
                            if !already_counted.contains(&account_info.store_id) {
                                pending_store_ids.insert(account_info.store_id);
                            }
                        }
                    }
                }
            }
        }
    }

    fn background_hasher(receiver: Receiver<CachedAccount>) {
        loop {
            let result = receiver.recv();
            match result {
                Ok(account) => {
                    // if we hold the only ref, then this account doesn't need to be hashed, we ignore this account and it will disappear
                    if Arc::strong_count(&account) > 1 {
                        // this will cause the hash to be calculated and store inside account if it needs to be calculated
                        let _ = (*account).hash();
                    };
                }
                Err(_) => {
                    break;
                }
            }
        }
    }

    fn start_background_hasher(&mut self) {
        let (sender, receiver) = unbounded();
        Builder::new()
            .name("solana-db-store-hasher-accounts".to_string())
            .spawn(move || {
                Self::background_hasher(receiver);
            })
            .unwrap();
        self.sender_bg_hasher = Some(sender);
    }

    fn purge_keys_exact<'a, C: 'a>(
        &'a self,
        pubkey_to_slot_set: impl Iterator<Item = &'a (Pubkey, C)>,
    ) -> Vec<(u64, AccountInfo)>
    where
        C: Contains<'a, Slot>,
    {
        let mut reclaims = Vec::new();
        let mut dead_keys = Vec::new();

        for (pubkey, slots_set) in pubkey_to_slot_set {
            let is_empty = self
                .accounts_index
                .purge_exact(pubkey, slots_set, &mut reclaims);
            if is_empty {
                dead_keys.push(pubkey);
            }
        }

        self.accounts_index
            .handle_dead_keys(&dead_keys, &self.account_indexes);
        reclaims
    }

    fn max_clean_root(&self, proposed_clean_root: Option<Slot>) -> Option<Slot> {
        match (
            self.accounts_index.min_ongoing_scan_root(),
            proposed_clean_root,
        ) {
            (None, None) => None,
            (Some(min_scan_root), None) => Some(min_scan_root),
            (None, Some(proposed_clean_root)) => Some(proposed_clean_root),
            (Some(min_scan_root), Some(proposed_clean_root)) => {
                Some(std::cmp::min(min_scan_root, proposed_clean_root))
            }
        }
    }

    /// Collect all the uncleaned slots, up to a max slot
    ///
    /// Search through the uncleaned Pubkeys and return all the slots, up to a maximum slot.
    fn collect_uncleaned_slots_up_to_slot(&self, max_slot: Slot) -> Vec<Slot> {
        self.uncleaned_pubkeys
            .iter()
            .filter_map(|entry| {
                let slot = *entry.key();
                (slot <= max_slot).then(|| slot)
            })
            .collect()
    }

    /// Remove `slots` from `uncleaned_pubkeys` and collect all pubkeys
    ///
    /// For each slot in the list of uncleaned slots, remove it from the `uncleaned_pubkeys` Map
    /// and collect all the pubkeys to return.
    fn remove_uncleaned_slots_and_collect_pubkeys(
        &self,
        uncleaned_slots: Vec<Slot>,
    ) -> Vec<Vec<Pubkey>> {
        uncleaned_slots
            .into_iter()
            .filter_map(|uncleaned_slot| {
                self.uncleaned_pubkeys
                    .remove(&uncleaned_slot)
                    .map(|(_removed_slot, removed_pubkeys)| removed_pubkeys)
            })
            .collect()
    }

    /// Remove uncleaned slots, up to a maximum slot, and return the collected pubkeys
    ///
    fn remove_uncleaned_slots_and_collect_pubkeys_up_to_slot(
        &self,
        max_slot: Slot,
    ) -> Vec<Vec<Pubkey>> {
        let uncleaned_slots = self.collect_uncleaned_slots_up_to_slot(max_slot);
        self.remove_uncleaned_slots_and_collect_pubkeys(uncleaned_slots)
    }

    // Construct a vec of pubkeys for cleaning from:
    //   uncleaned_pubkeys - the delta set of updated pubkeys in rooted slots from the last clean
    //   dirty_stores - set of stores which had accounts removed or recently rooted
    fn construct_candidate_clean_keys(
        &self,
        max_clean_root: Option<Slot>,
        timings: &mut CleanKeyTimings,
    ) -> Vec<Pubkey> {
        let mut dirty_store_processing_time = Measure::start("dirty_store_processing");
        let max_slot = max_clean_root.unwrap_or_else(|| self.accounts_index.max_root());
        let mut dirty_stores = Vec::with_capacity(self.dirty_stores.len());
        self.dirty_stores.retain(|(slot, _store_id), store| {
            if *slot > max_slot {
                true
            } else {
                dirty_stores.push((*slot, store.clone()));
                false
            }
        });
        let dirty_stores_len = dirty_stores.len();
        let pubkeys = DashSet::new();
        for (_slot, store) in dirty_stores {
            for account in store.accounts.accounts(0) {
                pubkeys.insert(account.meta.pubkey);
            }
        }
        trace!(
            "dirty_stores.len: {} pubkeys.len: {}",
            dirty_stores_len,
            pubkeys.len()
        );
        timings.dirty_pubkeys_count = pubkeys.len() as u64;
        dirty_store_processing_time.stop();
        timings.dirty_store_processing_us += dirty_store_processing_time.as_us();

        let mut collect_delta_keys = Measure::start("key_create");
        let delta_keys = self.remove_uncleaned_slots_and_collect_pubkeys_up_to_slot(max_slot);
        collect_delta_keys.stop();
        timings.collect_delta_keys_us += collect_delta_keys.as_us();

        let mut delta_insert = Measure::start("delta_insert");
        self.thread_pool_clean.install(|| {
            delta_keys.par_iter().for_each(|keys| {
                for key in keys {
                    pubkeys.insert(*key);
                }
            });
        });
        delta_insert.stop();
        timings.delta_insert_us += delta_insert.as_us();

        timings.delta_key_count = pubkeys.len() as u64;

        let mut hashset_to_vec = Measure::start("flat_map");
        let pubkeys: Vec<Pubkey> = pubkeys.into_iter().collect();
        hashset_to_vec.stop();
        timings.hashset_to_vec_us += hashset_to_vec.as_us();

        pubkeys
    }

    // Purge zero lamport accounts and older rooted account states as garbage
    // collection
    // Only remove those accounts where the entire rooted history of the account
    // can be purged because there are no live append vecs in the ancestors
    pub fn clean_accounts(&self, max_clean_root: Option<Slot>, is_startup: bool) {
        let max_clean_root = self.max_clean_root(max_clean_root);

        // hold a lock to prevent slot shrinking from running because it might modify some rooted
        // slot storages which can not happen as long as we're cleaning accounts because we're also
        // modifying the rooted slot storages!
        let mut candidates_v1 = self.shrink_candidate_slots_v1.lock().unwrap();
        self.report_store_stats();

        let mut key_timings = CleanKeyTimings::default();
        let pubkeys = self.construct_candidate_clean_keys(max_clean_root, &mut key_timings);

        let total_keys_count = pubkeys.len();
        let mut accounts_scan = Measure::start("accounts_scan");
        // parallel scan the index.
        let (mut purges_zero_lamports, purges_old_accounts) = {
            let do_clean_scan = || {
                pubkeys
                    .par_chunks(4096)
                    .map(|pubkeys: &[Pubkey]| {
                        let mut purges_zero_lamports = HashMap::new();
                        let mut purges_old_accounts = Vec::new();
                        for pubkey in pubkeys {
                            match self.accounts_index.get(pubkey, None, max_clean_root) {
                                AccountIndexGetResult::Found(locked_entry, index) => {
                                    let slot_list = locked_entry.slot_list();
                                    let (slot, account_info) = &slot_list[index];
                                    if account_info.lamports == 0 {
                                        purges_zero_lamports.insert(
                                            *pubkey,
                                            self.accounts_index
                                                .roots_and_ref_count(&locked_entry, max_clean_root),
                                        );
                                    }
                                    // Release the lock
                                    let slot = *slot;
                                    drop(locked_entry);

                                    if self.accounts_index.is_uncleaned_root(slot) {
                                        // Assertion enforced by `accounts_index.get()`, the latest slot
                                        // will not be greater than the given `max_clean_root`
                                        if let Some(max_clean_root) = max_clean_root {
                                            assert!(slot <= max_clean_root);
                                        }
                                        purges_old_accounts.push(*pubkey);
                                    }
                                }
                                AccountIndexGetResult::NotFoundOnFork => {
                                    // This pubkey is in the index but not in a root slot, so clean
                                    // it up by adding it to the to-be-purged list.
                                    //
                                    // Also, this pubkey must have been touched by some slot since
                                    // it was in the dirty list, so we assume that the slot it was
                                    // touched in must be unrooted.
                                    purges_old_accounts.push(*pubkey);
                                }
                                AccountIndexGetResult::Missing(_lock) => {}
                            };
                        }
                        (purges_zero_lamports, purges_old_accounts)
                    })
                    .reduce(
                        || (HashMap::new(), Vec::new()),
                        |mut m1, m2| {
                            // Collapse down the hashmaps/vecs into one.
                            m1.0.extend(m2.0);
                            m1.1.extend(m2.1);
                            m1
                        },
                    )
            };
            if is_startup {
                do_clean_scan()
            } else {
                self.thread_pool_clean.install(do_clean_scan)
            }
        };
        accounts_scan.stop();

        let mut clean_old_rooted = Measure::start("clean_old_roots");
        let (purged_account_slots, removed_accounts) =
            self.clean_accounts_older_than_root(purges_old_accounts, max_clean_root);

        if self.caching_enabled {
            self.do_reset_uncleaned_roots(max_clean_root);
        } else {
            self.do_reset_uncleaned_roots_v1(&mut candidates_v1, max_clean_root);
        }
        clean_old_rooted.stop();

        let mut store_counts_time = Measure::start("store_counts");

        // Calculate store counts as if everything was purged
        // Then purge if we can
        let mut store_counts: HashMap<AppendVecId, (usize, HashSet<Pubkey>)> = HashMap::new();
        for (key, (account_infos, ref_count)) in purges_zero_lamports.iter_mut() {
            if purged_account_slots.contains_key(key) {
                *ref_count = self.accounts_index.ref_count_from_storage(key);
            }
            account_infos.retain(|(slot, account_info)| {
                let was_slot_purged = purged_account_slots
                    .get(key)
                    .map(|slots_removed| slots_removed.contains(slot))
                    .unwrap_or(false);
                if was_slot_purged {
                    // No need to look up the slot storage below if the entire
                    // slot was purged
                    return false;
                }
                // Check if this update in `slot` to the account with `key` was reclaimed earlier by
                // `clean_accounts_older_than_root()`
                let was_reclaimed = removed_accounts
                    .get(&account_info.store_id)
                    .map(|store_removed| store_removed.contains(&account_info.offset))
                    .unwrap_or(false);
                if was_reclaimed {
                    return false;
                }
                if let Some(store_count) = store_counts.get_mut(&account_info.store_id) {
                    store_count.0 -= 1;
                    store_count.1.insert(*key);
                } else {
                    let mut key_set = HashSet::new();
                    key_set.insert(*key);
                    let count = self
                        .storage
                        .slot_store_count(*slot, account_info.store_id)
                        .unwrap()
                        - 1;
                    debug!(
                        "store_counts, inserting slot: {}, store id: {}, count: {}",
                        slot, account_info.store_id, count
                    );
                    store_counts.insert(account_info.store_id, (count, key_set));
                }
                true
            });
        }
        store_counts_time.stop();

        let mut calc_deps_time = Measure::start("calc_deps");
        Self::calc_delete_dependencies(&purges_zero_lamports, &mut store_counts);
        calc_deps_time.stop();

        // Only keep purges_zero_lamports where the entire history of the account in the root set
        // can be purged. All AppendVecs for those updates are dead.
        let mut purge_filter = Measure::start("purge_filter");
        purges_zero_lamports.retain(|_pubkey, (account_infos, _ref_count)| {
            for (_slot, account_info) in account_infos.iter() {
                if store_counts.get(&account_info.store_id).unwrap().0 != 0 {
                    return false;
                }
            }
            true
        });
        purge_filter.stop();

        let mut reclaims_time = Measure::start("reclaims");
        // Recalculate reclaims with new purge set
        let pubkey_to_slot_set: Vec<_> = purges_zero_lamports
            .into_iter()
            .map(|(key, (slots_list, _ref_count))| {
                (
                    key,
                    slots_list
                        .into_iter()
                        .map(|(slot, _)| slot)
                        .collect::<HashSet<Slot>>(),
                )
            })
            .collect();

        let reclaims = self.purge_keys_exact(pubkey_to_slot_set.iter());

        // Don't reset from clean, since the pubkeys in those stores may need to be unref'ed
        // and those stores may be used for background hashing.
        let reset_accounts = false;
        let mut reclaim_result = ReclaimResult::default();
        let reclaim_result = Some(&mut reclaim_result);
        self.handle_reclaims(
            &reclaims,
            None,
            Some(&self.clean_accounts_stats.purge_stats),
            reclaim_result,
            reset_accounts,
        );

        reclaims_time.stop();

        self.clean_accounts_stats.report();
        datapoint_info!(
            "clean_accounts",
            (
                "collect_delta_keys_us",
                key_timings.collect_delta_keys_us,
                i64
            ),
            (
                "dirty_store_processing_us",
                key_timings.dirty_store_processing_us,
                i64
            ),
            ("accounts_scan", accounts_scan.as_us() as i64, i64),
            ("clean_old_rooted", clean_old_rooted.as_us() as i64, i64),
            ("store_counts", store_counts_time.as_us() as i64, i64),
            ("purge_filter", purge_filter.as_us() as i64, i64),
            ("calc_deps", calc_deps_time.as_us() as i64, i64),
            ("reclaims", reclaims_time.as_us() as i64, i64),
            ("delta_key_count", key_timings.delta_key_count, i64),
            ("dirty_pubkeys_count", key_timings.dirty_pubkeys_count, i64),
            ("total_keys_count", total_keys_count, i64),
        );
    }

    /// Removes the accounts in the input `reclaims` from the tracked "count" of
    /// their corresponding  storage entries. Note this does not actually free
    /// the memory from the storage entries until all the storage entries for
    /// a given slot `S` are empty, at which point `process_dead_slots` will
    /// remove all the storage entries for `S`.
    ///
    /// # Arguments
    /// * `reclaims` - The accounts to remove from storage entries' "count". Note here
    ///    that we should not remove cache entries, only entries for accounts actually
    ///    stored in a storage entry.
    ///
    /// * `expected_single_dead_slot` - A correctness assertion. If this is equal to `Some(S)`,
    ///    then the function will check that the only slot being cleaned up in `reclaims`
    ///    is the slot == `S`. This is true for instance when `handle_reclaims` is called
    ///    from store or slot shrinking, as those should only touch the slot they are
    ///    currently storing to or shrinking.
    ///
    /// * `purge_stats` - The stats used to track performance of purging dead slots. This
    ///    also serves a correctness assertion. If `purge_stats.is_none()`, this implies
    ///    there can be no dead slots that happen as a result of this call, and the function
    ///    will check that no slots are cleaned up/removed via `process_dead_slots`. For instance,
    ///    on store, no slots should be cleaned up, but during the background clean accounts
    ///    purges accounts from old rooted slots, so outdated slots may be removed.
    ///
    /// * `reclaim_result` - Information about accounts that were removed from storage, does
    ///    not include accounts that were removed from the cache
    ///
    /// * `reset_accounts` - Reset the append_vec store when the store is dead (count==0)
    ///    From the clean and shrink paths it should be false since there may be an in-progress
    ///    hash operation and the stores may hold accounts that need to be unref'ed.
    fn handle_reclaims(
        &self,
        reclaims: SlotSlice<AccountInfo>,
        expected_single_dead_slot: Option<Slot>,
        // TODO: coalesce `purge_stats` and `reclaim_result` together into one option, as they
        // are both either Some or None
        purge_stats: Option<&PurgeStats>,
        reclaim_result: Option<&mut ReclaimResult>,
        reset_accounts: bool,
    ) {
        if reclaims.is_empty() {
            return;
        }
        let (purged_account_slots, reclaimed_offsets) =
            if let Some((ref mut x, ref mut y)) = reclaim_result {
                (Some(x), Some(y))
            } else {
                (None, None)
            };
        let dead_slots = self.remove_dead_accounts(
            reclaims,
            expected_single_dead_slot,
            reclaimed_offsets,
            reset_accounts,
        );
        if purge_stats.is_none() {
            assert!(dead_slots.is_empty());
        } else if let Some(expected_single_dead_slot) = expected_single_dead_slot {
            assert!(dead_slots.len() <= 1);
            if dead_slots.len() == 1 {
                assert!(dead_slots.contains(&expected_single_dead_slot));
            }
        }

        if let Some(purge_stats) = purge_stats {
            self.process_dead_slots(&dead_slots, purged_account_slots, purge_stats);
        }
    }

    // Must be kept private!, does sensitive cleanup that should only be called from
    // supported pipelines in AccountsDb
    fn process_dead_slots(
        &self,
        dead_slots: &HashSet<Slot>,
        purged_account_slots: Option<&mut AccountSlots>,
        purge_stats: &PurgeStats,
    ) {
        if dead_slots.is_empty() {
            return;
        }
        let mut clean_dead_slots = Measure::start("reclaims::clean_dead_slots");
        self.clean_stored_dead_slots(dead_slots, purged_account_slots);
        clean_dead_slots.stop();

        let mut purge_removed_slots = Measure::start("reclaims::purge_removed_slots");
        self.purge_dead_slots_from_storage(dead_slots.iter(), purge_stats);
        purge_removed_slots.stop();

        // If the slot is dead, remove the need to shrink the storages as
        // the storage entries will be purged.
        {
            let mut list = self.shrink_candidate_slots.lock().unwrap();
            for slot in dead_slots {
                list.remove(slot);
            }
        }

        debug!(
            "process_dead_slots({}): {} {} {:?}",
            dead_slots.len(),
            clean_dead_slots,
            purge_removed_slots,
            dead_slots,
        );
    }

    fn do_shrink_slot_stores<'a, I>(&'a self, slot: Slot, stores: I, _is_startup: bool) -> usize
    where
        I: Iterator<Item = &'a Arc<AccountStorageEntry>>,
    {
        struct FoundStoredAccount<'a> {
            pub account: StoredAccountMeta<'a>,
            pub store_id: AppendVecId,
            pub account_size: usize,
        }
        debug!("do_shrink_slot_stores: slot: {}", slot);
        let mut stored_accounts: HashMap<Pubkey, FoundStoredAccount> = HashMap::new();
        let mut original_bytes = 0;
        let mut num_stores = 0;
        for store in stores {
            let mut start = 0;
            original_bytes += store.total_bytes();
            while let Some((account, next)) = store.accounts.get_account(start) {
                let new_entry = FoundStoredAccount {
                    account,
                    store_id: store.append_vec_id(),
                    account_size: next - start,
                };
                match stored_accounts.entry(new_entry.account.meta.pubkey) {
                    Entry::Occupied(mut occupied_entry) => {
                        if new_entry.account.meta.write_version
                            > occupied_entry.get().account.meta.write_version
                        {
                            occupied_entry.insert(new_entry);
                        }
                    }
                    Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(new_entry);
                    }
                }
                start = next;
            }
            num_stores += 1;
        }

        let mut index_read_elapsed = Measure::start("index_read_elapsed");
        let mut alive_total = 0;

        let mut alive_accounts: Vec<_> = Vec::with_capacity(stored_accounts.len());
        let mut unrefed_pubkeys = vec![];
        for (pubkey, stored_account) in &stored_accounts {
            let lookup = self.accounts_index.get_account_read_entry(pubkey);
            if let Some(locked_entry) = lookup {
                let is_alive = locked_entry.slot_list().iter().any(|(_slot, i)| {
                    i.store_id == stored_account.store_id
                        && i.offset == stored_account.account.offset
                });
                if !is_alive {
                    // This pubkey was found in the storage, but no longer exists in the index.
                    // It would have had a ref to the storage from the initial store, but it will
                    // not exist in the re-written slot. Unref it to keep the index consistent with
                    // rewriting the storage entries.
                    unrefed_pubkeys.push(pubkey);
                    locked_entry.unref()
                } else {
                    alive_accounts.push((pubkey, stored_account));
                    alive_total += stored_account.account_size;
                }
            }
        }

        index_read_elapsed.stop();
        let aligned_total: u64 = Self::page_align(alive_total as u64);

        // This shouldn't happen if alive_bytes/approx_stored_count are accurate
        if Self::should_not_shrink(aligned_total, original_bytes, num_stores) {
            self.shrink_stats
                .skipped_shrink
                .fetch_add(1, Ordering::Relaxed);
            for pubkey in unrefed_pubkeys {
                if let Some(locked_entry) = self.accounts_index.get_account_read_entry(pubkey) {
                    locked_entry.addref();
                }
            }
            return 0;
        }

        let total_starting_accounts = stored_accounts.len();
        let total_accounts_after_shrink = alive_accounts.len();
        debug!(
            "shrinking: slot: {}, accounts: ({} => {}) bytes: ({} ; aligned to: {}) original: {}",
            slot,
            total_starting_accounts,
            total_accounts_after_shrink,
            alive_total,
            aligned_total,
            original_bytes,
        );

        let mut rewrite_elapsed = Measure::start("rewrite_elapsed");
        let mut dead_storages = vec![];
        let mut find_alive_elapsed = 0;
        let mut create_and_insert_store_elapsed = 0;
        let mut write_storage_elapsed = 0;
        let mut store_accounts_timing = StoreAccountsTiming::default();
        if aligned_total > 0 {
            let mut start = Measure::start("find_alive_elapsed");
            let mut accounts = Vec::with_capacity(alive_accounts.len());
            let mut hashes = Vec::with_capacity(alive_accounts.len());
            let mut write_versions = Vec::with_capacity(alive_accounts.len());

            for (pubkey, alive_account) in alive_accounts {
                accounts.push((pubkey, &alive_account.account));
                hashes.push(alive_account.account.hash);
                write_versions.push(alive_account.account.meta.write_version);
            }
            start.stop();
            find_alive_elapsed = start.as_us();

            let mut start = Measure::start("create_and_insert_store_elapsed");
            let shrunken_store = if let Some(new_store) =
                self.try_recycle_and_insert_store(slot, aligned_total, aligned_total + 1024)
            {
                new_store
            } else {
                let maybe_shrink_paths = self.shrink_paths.read().unwrap();
                if let Some(ref shrink_paths) = *maybe_shrink_paths {
                    self.create_and_insert_store_with_paths(
                        slot,
                        aligned_total,
                        "shrink-w-path",
                        shrink_paths,
                    )
                } else {
                    self.create_and_insert_store(slot, aligned_total, "shrink")
                }
            };
            start.stop();
            create_and_insert_store_elapsed = start.as_us();

            // here, we're writing back alive_accounts. That should be an atomic operation
            // without use of rather wide locks in this whole function, because we're
            // mutating rooted slots; There should be no writers to them.
            store_accounts_timing = self.store_accounts_frozen(
                slot,
                &accounts,
                Some(&hashes),
                Some(Box::new(move |_, _| shrunken_store.clone())),
                Some(Box::new(write_versions.into_iter())),
            );

            // `store_accounts_frozen()` above may have purged accounts from some
            // other storage entries (the ones that were just overwritten by this
            // new storage entry). This means some of those stores might have caused
            // this slot to be read to `self.shrink_candidate_slots`, so delete
            // those here
            self.shrink_candidate_slots.lock().unwrap().remove(&slot);

            // Purge old, overwritten storage entries
            let mut start = Measure::start("write_storage_elapsed");
            if let Some(slot_stores) = self.storage.get_slot_stores(slot) {
                slot_stores.write().unwrap().retain(|_key, store| {
                    if store.count() == 0 {
                        self.dirty_stores
                            .insert((slot, store.append_vec_id()), store.clone());
                        dead_storages.push(store.clone());
                        false
                    } else {
                        true
                    }
                });
            }
            start.stop();
            write_storage_elapsed = start.as_us();
        }
        rewrite_elapsed.stop();

        let mut recycle_stores_write_elapsed = Measure::start("recycle_stores_write_time");
        let mut recycle_stores = self.recycle_stores.write().unwrap();
        recycle_stores_write_elapsed.stop();

        let mut drop_storage_entries_elapsed = Measure::start("drop_storage_entries_elapsed");
        if recycle_stores.entry_count() < MAX_RECYCLE_STORES {
            recycle_stores.add_entries(dead_storages);
            drop(recycle_stores);
        } else {
            self.stats
                .dropped_stores
                .fetch_add(dead_storages.len() as u64, Ordering::Relaxed);
            drop(recycle_stores);
            drop(dead_storages);
        }
        drop_storage_entries_elapsed.stop();

        self.shrink_stats
            .num_slots_shrunk
            .fetch_add(1, Ordering::Relaxed);
        self.shrink_stats
            .index_read_elapsed
            .fetch_add(index_read_elapsed.as_us(), Ordering::Relaxed);
        self.shrink_stats
            .find_alive_elapsed
            .fetch_add(find_alive_elapsed, Ordering::Relaxed);
        self.shrink_stats
            .create_and_insert_store_elapsed
            .fetch_add(create_and_insert_store_elapsed, Ordering::Relaxed);
        self.shrink_stats.store_accounts_elapsed.fetch_add(
            store_accounts_timing.store_accounts_elapsed,
            Ordering::Relaxed,
        );
        self.shrink_stats.update_index_elapsed.fetch_add(
            store_accounts_timing.update_index_elapsed,
            Ordering::Relaxed,
        );
        self.shrink_stats.handle_reclaims_elapsed.fetch_add(
            store_accounts_timing.handle_reclaims_elapsed,
            Ordering::Relaxed,
        );
        self.shrink_stats
            .write_storage_elapsed
            .fetch_add(write_storage_elapsed, Ordering::Relaxed);
        self.shrink_stats
            .rewrite_elapsed
            .fetch_add(rewrite_elapsed.as_us(), Ordering::Relaxed);
        self.shrink_stats
            .drop_storage_entries_elapsed
            .fetch_add(drop_storage_entries_elapsed.as_us(), Ordering::Relaxed);
        self.shrink_stats
            .recycle_stores_write_elapsed
            .fetch_add(recycle_stores_write_elapsed.as_us(), Ordering::Relaxed);
        self.shrink_stats.accounts_removed.fetch_add(
            total_starting_accounts - total_accounts_after_shrink,
            Ordering::Relaxed,
        );
        self.shrink_stats.bytes_removed.fetch_add(
            original_bytes.saturating_sub(aligned_total),
            Ordering::Relaxed,
        );
        self.shrink_stats
            .bytes_written
            .fetch_add(aligned_total, Ordering::Relaxed);

        self.shrink_stats.report();

        total_accounts_after_shrink
    }

    // Reads all accounts in given slot's AppendVecs and filter only to alive,
    // then create a minimum AppendVec filled with the alive.
    fn shrink_slot_forced(&self, slot: Slot, is_startup: bool) -> usize {
        debug!("shrink_slot_forced: slot: {}", slot);

        if let Some(stores_lock) = self.storage.get_slot_stores(slot) {
            let stores: Vec<Arc<AccountStorageEntry>> =
                stores_lock.read().unwrap().values().cloned().collect();
            if !Self::is_shrinking_productive(slot, &stores) {
                return 0;
            }
            self.do_shrink_slot_stores(slot, stores.iter(), is_startup)
        } else {
            0
        }
    }

    fn all_slots_in_storage(&self) -> Vec<Slot> {
        self.storage.all_slots()
    }

    fn all_root_slots_in_index(&self) -> Vec<Slot> {
        self.accounts_index.all_roots()
    }

    /// Given the input `ShrinkCandidates`, this function sorts the stores by their alive ratio
    /// in increasing order with the most sparse entries in the front. It will then simulate the
    /// shrinking by working on the most sparse entries first and if the overall alive ratio is
    /// achieved, it will stop and return the filtered-down candidates.
    fn select_candidates_by_total_usage(
        &self,
        shrink_slots: &ShrinkCandidates,
        shrink_ratio: f64,
    ) -> ShrinkCandidates {
        struct StoreUsageInfo {
            slot: Slot,
            alive_ratio: f64,
            store: Arc<AccountStorageEntry>,
        }
        let mut measure = Measure::start("select_top_sparse_storage_entries-ms");
        let mut store_usage: Vec<StoreUsageInfo> = Vec::with_capacity(shrink_slots.len());
        let mut total_alive_bytes: u64 = 0;
        let mut candidates_count: usize = 0;
        let mut total_bytes: u64 = 0;
        for (slot, slot_shrink_candidates) in shrink_slots {
            candidates_count += slot_shrink_candidates.len();
            for store in slot_shrink_candidates.values() {
                total_alive_bytes += Self::page_align(store.alive_bytes() as u64);
                total_bytes += store.total_bytes();
                let alive_ratio = Self::page_align(store.alive_bytes() as u64) as f64
                    / store.total_bytes() as f64;
                store_usage.push(StoreUsageInfo {
                    slot: *slot,
                    alive_ratio,
                    store: store.clone(),
                });
            }
        }
        store_usage.sort_by(|a, b| {
            a.alive_ratio
                .partial_cmp(&b.alive_ratio)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Working from the beginning of store_usage which are the most sparse and see when we can stop
        // shrinking while still achieving the overall goals.
        let mut shrink_slots: ShrinkCandidates = HashMap::new();
        for usage in &store_usage {
            let alive_ratio = (total_alive_bytes as f64) / (total_bytes as f64);
            if alive_ratio > shrink_ratio {
                // we have reached our goal, stop
                debug!(
                    "Shrinking goal can be achieved at slot {:?}, total_alive_bytes: {:?} \
                    total_bytes: {:?}, alive_ratio: {:}, shrink_ratio: {:?}",
                    usage.slot, total_alive_bytes, total_bytes, alive_ratio, shrink_ratio
                );
                break;
            }
            let store = &usage.store;
            let current_store_size = store.total_bytes();
            let after_shrink_size = Self::page_align(store.alive_bytes() as u64);
            let bytes_saved = current_store_size.saturating_sub(after_shrink_size);
            total_bytes -= bytes_saved;
            shrink_slots
                .entry(usage.slot)
                .or_default()
                .insert(store.append_vec_id(), store.clone());
        }
        measure.stop();
        inc_new_counter_info!(
            "select_top_sparse_storage_entries-ms",
            measure.as_ms() as usize
        );
        inc_new_counter_info!("select_top_sparse_storage_entries-seeds", candidates_count);
        shrink_slots
    }

    pub fn shrink_candidate_slots(&self) -> usize {
        let shrink_candidates_slots =
            std::mem::take(&mut *self.shrink_candidate_slots.lock().unwrap());
        let shrink_slots = {
            if let AccountShrinkThreshold::TotalSpace { shrink_ratio } = self.shrink_ratio {
                self.select_candidates_by_total_usage(&shrink_candidates_slots, shrink_ratio)
            } else {
                shrink_candidates_slots
            }
        };

        let mut measure_shrink_all_candidates = Measure::start("shrink_all_candidate_slots-ms");
        let num_candidates = shrink_slots.len();
        let mut shrink_candidates_count: usize = 0;
        for (slot, slot_shrink_candidates) in shrink_slots {
            shrink_candidates_count += slot_shrink_candidates.len();
            let mut measure = Measure::start("shrink_candidate_slots-ms");
            self.do_shrink_slot_stores(slot, slot_shrink_candidates.values(), false);
            measure.stop();
            inc_new_counter_info!("shrink_candidate_slots-ms", measure.as_ms() as usize);
        }
        measure_shrink_all_candidates.stop();
        inc_new_counter_info!(
            "shrink_all_candidate_slots-ms",
            measure_shrink_all_candidates.as_ms() as usize
        );
        inc_new_counter_info!("shrink_all_candidate_slots-count", shrink_candidates_count);
        num_candidates
    }

    pub fn shrink_all_slots(&self, is_startup: bool) {
        if is_startup && self.caching_enabled {
            let slots = self.all_slots_in_storage();
            let chunk_size = std::cmp::max(slots.len() / 8, 1); // approximately 400k slots in a snapshot
            slots.par_chunks(chunk_size).for_each(|slots| {
                for slot in slots {
                    self.shrink_slot_forced(*slot, is_startup);
                }
            });
        } else {
            for slot in self.all_slots_in_storage() {
                if self.caching_enabled {
                    self.shrink_slot_forced(slot, false);
                } else {
                    self.do_shrink_slot_forced_v1(slot);
                }
            }
        }
    }

    pub fn scan_accounts<F, A>(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        scan_func: F,
    ) -> ScanResult<A>
    where
        F: Fn(&mut A, Option<(&Pubkey, AccountSharedData, Slot)>),
        A: Default,
    {
        let mut collector = A::default();

        // This can error out if the slots being scanned over are aborted
        self.accounts_index
            .scan_accounts(ancestors, bank_id, |pubkey, (account_info, slot)| {
                let account_slot = self
                    .get_account_accessor(slot, pubkey, account_info.store_id, account_info.offset)
                    .get_loaded_account()
                    .map(|loaded_account| (pubkey, loaded_account.take_account(), slot));
                scan_func(&mut collector, account_slot)
            })?;

        Ok(collector)
    }

    pub fn unchecked_scan_accounts<F, A>(
        &self,
        metric_name: &'static str,
        ancestors: &Ancestors,
        scan_func: F,
    ) -> A
    where
        F: Fn(&mut A, (&Pubkey, LoadedAccount, Slot)),
        A: Default,
    {
        let mut collector = A::default();
        self.accounts_index.unchecked_scan_accounts(
            metric_name,
            ancestors,
            |pubkey, (account_info, slot)| {
                if let Some(loaded_account) = self
                    .get_account_accessor(slot, pubkey, account_info.store_id, account_info.offset)
                    .get_loaded_account()
                {
                    scan_func(&mut collector, (pubkey, loaded_account, slot));
                }
            },
        );
        collector
    }

    pub fn range_scan_accounts<F, A, R>(
        &self,
        metric_name: &'static str,
        ancestors: &Ancestors,
        range: R,
        scan_func: F,
    ) -> A
    where
        F: Fn(&mut A, Option<(&Pubkey, AccountSharedData, Slot)>),
        A: Default,
        R: RangeBounds<Pubkey>,
    {
        let mut collector = A::default();
        self.accounts_index.range_scan_accounts(
            metric_name,
            ancestors,
            range,
            |pubkey, (account_info, slot)| {
                // unlike other scan fns, this is called from Bank::collect_rent_eagerly(),
                // which is on-consensus processing in the banking/replaying stage.
                // This requires infallible and consistent account loading.
                // So, we unwrap Option<LoadedAccount> from get_loaded_account() here.
                // This is safe because this closure is invoked with the account_info,
                // while we lock the index entry at AccountsIndex::do_scan_accounts() ultimately,
                // meaning no other subsystems can invalidate the account_info before making their
                // changes to the index entry.
                // For details, see the comment in retry_to_get_account_accessor()
                let account_slot = self
                    .get_account_accessor(slot, pubkey, account_info.store_id, account_info.offset)
                    .get_loaded_account()
                    .map(|loaded_account| (pubkey, loaded_account.take_account(), slot))
                    .unwrap();
                scan_func(&mut collector, Some(account_slot))
            },
        );
        collector
    }

    pub fn index_scan_accounts<F, A>(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        index_key: IndexKey,
        scan_func: F,
    ) -> ScanResult<(A, bool)>
    where
        F: Fn(&mut A, Option<(&Pubkey, AccountSharedData, Slot)>),
        A: Default,
    {
        let key = match &index_key {
            IndexKey::ProgramId(key) => key,
            IndexKey::SplTokenMint(key) => key,
            IndexKey::SplTokenOwner(key) => key,
        };
        if !self.account_indexes.include_key(key) {
            // the requested key was not indexed in the secondary index, so do a normal scan
            let used_index = false;
            let scan_result = self.scan_accounts(ancestors, bank_id, scan_func)?;
            return Ok((scan_result, used_index));
        }

        let mut collector = A::default();
        self.accounts_index.index_scan_accounts(
            ancestors,
            bank_id,
            index_key,
            |pubkey, (account_info, slot)| {
                let account_slot = self
                    .get_account_accessor(slot, pubkey, account_info.store_id, account_info.offset)
                    .get_loaded_account()
                    .map(|loaded_account| (pubkey, loaded_account.take_account(), slot));
                scan_func(&mut collector, account_slot)
            },
        )?;
        let used_index = true;
        Ok((collector, used_index))
    }

    /// Scan a specific slot through all the account storage in parallel
    pub fn scan_account_storage<R, B>(
        &self,
        slot: Slot,
        cache_map_func: impl Fn(LoadedAccount) -> Option<R> + Sync,
        storage_scan_func: impl Fn(&B, LoadedAccount) + Sync,
    ) -> ScanStorageResult<R, B>
    where
        R: Send,
        B: Send + Default + Sync,
    {
        if let Some(slot_cache) = self.accounts_cache.slot_cache(slot) {
            // If we see the slot in the cache, then all the account information
            // is in this cached slot
            if slot_cache.len() > SCAN_SLOT_PAR_ITER_THRESHOLD {
                ScanStorageResult::Cached(self.thread_pool.install(|| {
                    slot_cache
                        .par_iter()
                        .filter_map(|cached_account| {
                            cache_map_func(LoadedAccount::Cached((
                                *cached_account.key(),
                                Cow::Borrowed(cached_account.value()),
                            )))
                        })
                        .collect()
                }))
            } else {
                ScanStorageResult::Cached(
                    slot_cache
                        .iter()
                        .filter_map(|cached_account| {
                            cache_map_func(LoadedAccount::Cached((
                                *cached_account.key(),
                                Cow::Borrowed(cached_account.value()),
                            )))
                        })
                        .collect(),
                )
            }
        } else {
            let retval = B::default();
            // If the slot is not in the cache, then all the account information must have
            // been flushed. This is guaranteed because we only remove the rooted slot from
            // the cache *after* we've finished flushing in `flush_slot_cache`.
            let storage_maps: Vec<Arc<AccountStorageEntry>> = self
                .storage
                .get_slot_storage_entries(slot)
                .unwrap_or_default();
            self.thread_pool.install(|| {
                storage_maps
                    .par_iter()
                    .flat_map(|storage| storage.all_accounts())
                    .for_each(|account| storage_scan_func(&retval, LoadedAccount::Stored(account)));
            });

            ScanStorageResult::Stored(retval)
        }
    }

    pub fn set_hash(&self, slot: Slot, parent_slot: Slot) {
        let mut bank_hashes = self.bank_hashes.write().unwrap();
        if bank_hashes.get(&slot).is_some() {
            error!(
                "set_hash: already exists; multiple forks with shared slot {} as child (parent: {})!?",
                slot, parent_slot,
            );
            return;
        }

        let new_hash_info = BankHashInfo {
            hash: Hash::default(),
            snapshot_hash: Hash::default(),
            stats: BankHashStats::default(),
        };
        bank_hashes.insert(slot, new_hash_info);
    }

    pub fn load(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
        load_hint: LoadHint,
    ) -> Option<(AccountSharedData, Slot)> {
        let load_into_read_only_cache_only = false;
        self.do_load(
            ancestors,
            pubkey,
            None,
            load_hint,
            load_into_read_only_cache_only,
        )
    }

    pub fn load_accounts_into_read_only_cache(&self, ancestors: &Ancestors, pubkey: &Pubkey) {
        self.do_load(ancestors, pubkey, None, LoadHint::Unspecified, true);
    }

    pub fn load_with_fixed_root(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        self.load(ancestors, pubkey, LoadHint::FixedMaxRoot)
    }

    pub fn load_without_fixed_root(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        self.load(ancestors, pubkey, LoadHint::Unspecified)
    }

    fn read_index_for_accessor_or_load_slow<'a>(
        &'a self,
        ancestors: &Ancestors,
        pubkey: &'a Pubkey,
        max_root: Option<Slot>,
        clone_in_lock: bool,
    ) -> Option<(Slot, AppendVecId, usize, Option<LoadedAccountAccessor<'a>>)> {
        let (lock, index) = match self.accounts_index.get(pubkey, Some(ancestors), max_root) {
            AccountIndexGetResult::Found(lock, index) => (lock, index),
            // we bail out pretty early for missing.
            AccountIndexGetResult::NotFoundOnFork => {
                return None;
            }
            AccountIndexGetResult::Missing(_) => {
                return None;
            }
        };

        let slot_list = lock.slot_list();
        let (
            slot,
            AccountInfo {
                store_id, offset, ..
            },
        ) = slot_list[index];

        let some_from_slow_path = if clone_in_lock {
            // the fast path must have failed.... so take the slower approach
            // of copying potentially large Account::data inside the lock.

            // calling check_and_get_loaded_account is safe as long as we're guaranteed to hold
            // the lock during the time and there should be no purge thanks to alive ancestors
            // held by our caller.
            Some(self.get_account_accessor(slot, pubkey, store_id, offset))
        } else {
            None
        };

        Some((slot, store_id, offset, some_from_slow_path))
        // `lock` is dropped here rather pretty quickly with clone_in_lock = false,
        // so the entry could be raced for mutation by other subsystems,
        // before we actually provision an account data for caller's use from now on.
        // This is traded for less contention and resultant performance, introducing fair amount of
        // delicate handling in retry_to_get_account_accessor() below ;)
        // you're warned!
    }

    fn retry_to_get_account_accessor<'a>(
        &'a self,
        mut slot: Slot,
        mut store_id: usize,
        mut offset: usize,
        ancestors: &'a Ancestors,
        pubkey: &'a Pubkey,
        max_root: Option<Slot>,
        load_hint: LoadHint,
    ) -> Option<(LoadedAccountAccessor<'a>, Slot)> {
        // Happy drawing time! :)
        //
        // Reader                               | Accessed data source for cached/stored
        // -------------------------------------+----------------------------------
        // R1 read_index_for_accessor_or_load_slow()| cached/stored: index
        //          |                           |
        //        <(store_id, offset, ..)>      |
        //          V                           |
        // R2 retry_to_get_account_accessor()/  | cached: map of caches & entry for (slot, pubkey)
        //        get_account_accessor()        | stored: map of stores
        //          |                           |
        //        <Accessor>                    |
        //          V                           |
        // R3 check_and_get_loaded_account()/   | cached: N/A (note: basically noop unwrap)
        //        get_loaded_account()          | stored: store's entry for slot
        //          |                           |
        //        <LoadedAccount>               |
        //          V                           |
        // R4 take_account()                    | cached/stored: entry of cache/storage for (slot, pubkey)
        //          |                           |
        //        <AccountSharedData>           |
        //          V                           |
        //    Account!!                         V
        //
        // Flusher                              | Accessed data source for cached/stored
        // -------------------------------------+----------------------------------
        // F1 flush_slot_cache()                | N/A
        //          |                           |
        //          V                           |
        // F2 store_accounts_frozen()/          | map of stores (creates new entry)
        //        write_accounts_to_storage()   |
        //          |                           |
        //          V                           |
        // F3 store_accounts_frozen()/          | index
        //        update_index()                | (replaces existing store_id, offset in caches)
        //          |                           |
        //          V                           |
        // F4 accounts_cache.remove_slot()      | map of caches (removes old entry)
        //                                      V
        //
        // Remarks for flusher: So, for any reading operations, it's a race condition where F4 happens
        // between R1 and R2. In that case, retrying from R1 is safu because F3 should have
        // been occurred.
        //
        // Shrinker                             | Accessed data source for stored
        // -------------------------------------+----------------------------------
        // S1 do_shrink_slot_stores()           | N/A
        //          |                           |
        //          V                           |
        // S2 store_accounts_frozen()/          | map of stores (creates new entry)
        //        write_accounts_to_storage()   |
        //          |                           |
        //          V                           |
        // S3 store_accounts_frozen()/          | index
        //        update_index()                | (replaces existing store_id, offset in stores)
        //          |                           |
        //          V                           |
        // S4 do_shrink_slot_stores()/          | map of stores (removes old entry)
        //        dead_storages
        //
        // Remarks for shrinker: So, for any reading operations, it's a race condition
        // where S4 happens between R1 and R2. In that case, retrying from R1 is safu because S3 should have
        // been occurred, and S3 atomically replaced the index accordingly.
        //
        // Cleaner                              | Accessed data source for stored
        // -------------------------------------+----------------------------------
        // C1 clean_accounts()                  | N/A
        //          |                           |
        //          V                           |
        // C2 clean_accounts()/                 | index
        //        purge_keys_exact()            | (removes existing store_id, offset for stores)
        //          |                           |
        //          V                           |
        // C3 clean_accounts()/                 | map of stores (removes old entry)
        //        handle_reclaims()             |
        //
        // Remarks for cleaner: So, for any reading operations, it's a race condition
        // where C3 happens between R1 and R2. In that case, retrying from R1 is safu.
        // In that case, None would be returned while bailing out at R1.
        //
        // Purger                                 | Accessed data source for cached/stored
        // ---------------------------------------+----------------------------------
        // P1 purge_slot()                        | N/A
        //          |                             |
        //          V                             |
        // P2 purge_slots_from_cache_and_store()  | map of caches/stores (removes old entry)
        //          |                             |
        //          V                             |
        // P3 purge_slots_from_cache_and_store()/ | index
        //       purge_slot_cache()/              |
        //          purge_slot_cache_pubkeys()    | (removes existing store_id, offset for cache)
        //       purge_slot_storage()/            |
        //          purge_keys_exact()            | (removes accounts index entries)
        //          handle_reclaims()             | (removes storage entries)
        //      OR                                |
        //    clean_accounts()/                   |
        //        clean_accounts_older_than_root()| (removes existing store_id, offset for stores)
        //                                        V
        //
        // Remarks for purger: So, for any reading operations, it's a race condition
        // where P2 happens between R1 and R2. In that case, retrying from R1 is safu.
        // In that case, we may bail at index read retry when P3 hasn't been run

        #[cfg(test)]
        {
            // Give some time for cache flushing to occur here for unit tests
            sleep(Duration::from_millis(self.load_delay));
        }

        // Failsafe for potential race conditions with other subsystems
        let mut num_acceptable_failed_iterations = 0;
        loop {
            let account_accessor = self.get_account_accessor(slot, pubkey, store_id, offset);
            match account_accessor {
                LoadedAccountAccessor::Cached(Some(_)) | LoadedAccountAccessor::Stored(Some(_)) => {
                    // Great! There was no race, just return :) This is the most usual situation
                    return Some((account_accessor, slot));
                }
                LoadedAccountAccessor::Cached(None) => {
                    num_acceptable_failed_iterations += 1;
                    // Cache was flushed in between checking the index and retrieving from the cache,
                    // so retry. This works because in accounts cache flush, an account is written to
                    // storage *before* it is removed from the cache
                    match load_hint {
                        LoadHint::FixedMaxRoot => {
                            // it's impossible for this to fail for transaction loads from
                            // replaying/banking more than once.
                            // This is because:
                            // 1) For a slot `X` that's being replayed, there is only one
                            // latest ancestor containing the latest update for the account, and this
                            // ancestor can only be flushed once.
                            // 2) The root cannot move while replaying, so the index cannot continually
                            // find more up to date entries than the current `slot`
                            assert!(num_acceptable_failed_iterations <= 1);
                        }
                        LoadHint::Unspecified => {
                            // Because newer root can be added to the index (= not fixed),
                            // multiple flush race conditions can be observed under very rare
                            // condition, at least theoretically
                        }
                    }
                }
                LoadedAccountAccessor::Stored(None) => {
                    match load_hint {
                        LoadHint::FixedMaxRoot => {
                            // When running replay on the validator, or banking stage on the leader,
                            // it should be very rare that the storage entry doesn't exist if the
                            // entry in the accounts index is the latest version of this account.
                            //
                            // There are only a few places where the storage entry may not exist
                            // after reading the index:
                            // 1) Shrink has removed the old storage entry and rewritten to
                            // a newer storage entry
                            // 2) The `pubkey` asked for in this function is a zero-lamport account,
                            // and the storage entry holding this account qualified for zero-lamport clean.
                            //
                            // In both these cases, it should be safe to retry and recheck the accounts
                            // index indefinitely, without incrementing num_acceptable_failed_iterations.
                            // That's because if the root is fixed, there should be a bounded number
                            // of pending cleans/shrinks (depends how far behind the AccountsBackgroundService
                            // is), termination to the desired condition is guaranteed.
                            //
                            // Also note that in both cases, if we do find the storage entry,
                            // we can guarantee that the storage entry is safe to read from because
                            // we grabbed a reference to the storage entry while it was still in the
                            // storage map. This means even if the storage entry is removed from the storage
                            // map after we grabbed the storage entry, the recycler should not reset the
                            // storage entry until we drop the reference to the storage entry.
                            //
                            // eh, no code in this arm? yes!
                        }
                        LoadHint::Unspecified => {
                            // RPC get_account() may have fetched an old root from the index that was
                            // either:
                            // 1) Cleaned up by clean_accounts(), so the accounts index has been updated
                            // and the storage entries have been removed.
                            // 2) Dropped by purge_slots() because the slot was on a minor fork, which
                            // removes the slots' storage entries but doesn't purge from the accounts index
                            // (account index cleanup is left to clean for stored slots). Note that
                            // this generally is impossible to occur in the wild because the RPC
                            // should hold the slot's bank, preventing it from being purged() to
                            // begin with.
                            num_acceptable_failed_iterations += 1;
                        }
                    }
                }
            }
            #[cfg(not(test))]
            let load_limit = ABSURD_CONSECUTIVE_FAILED_ITERATIONS;

            #[cfg(test)]
            let load_limit = self.load_limit.load(Ordering::Relaxed);

            let fallback_to_slow_path = if num_acceptable_failed_iterations >= load_limit {
                // The latest version of the account existed in the index, but could not be
                // fetched from storage. This means a race occurred between this function and clean
                // accounts/purge_slots
                let message = format!(
                    "do_load() failed to get key: {} from storage, latest attempt was for \
                     slot: {}, storage_entry: {} offset: {}, load_hint: {:?}",
                    pubkey, slot, store_id, offset, load_hint,
                );
                datapoint_warn!("accounts_db-do_load_warn", ("warn", message, String));
                true
            } else {
                false
            };

            // Because reading from the cache/storage failed, retry from the index read
            let (new_slot, new_store_id, new_offset, maybe_account_accessor) = self
                .read_index_for_accessor_or_load_slow(
                    ancestors,
                    pubkey,
                    max_root,
                    fallback_to_slow_path,
                )?;
            // Notice the subtle `?` at previous line, we bail out pretty early if missing.

            if new_slot == slot && new_store_id == store_id {
                // Considering that we're failed to get accessor above and further that
                // the index still returned the same (slot, store_id) tuple, offset must be same
                // too.
                assert!(new_offset == offset);

                // If the entry was missing from the cache, that means it must have been flushed,
                // and the accounts index is always updated before cache flush, so store_id must
                // not indicate being cached at this point.
                assert!(new_store_id != CACHE_VIRTUAL_STORAGE_ID);

                // If this is not a cache entry, then this was a minor fork slot
                // that had its storage entries cleaned up by purge_slots() but hasn't been
                // cleaned yet. That means this must be rpc access and not replay/banking at the
                // very least. Note that purge shouldn't occur even for RPC as caller must hold all
                // of ancestor slots..
                assert!(load_hint == LoadHint::Unspecified);

                // Everything being assert!()-ed, let's panic!() here as it's an error condition
                // after all....
                // That reasoning is based on the fact all of code-path reaching this fn
                // retry_to_get_account_accessor() must outlive the Arc<Bank> (and its all
                // ancestors) over this fn invocation, guaranteeing the prevention of being purged,
                // first of all.
                // For details, see the comment in AccountIndex::do_checked_scan_accounts(),
                // which is referring back here.
                panic!(
                    "Bad index entry detected ({}, {}, {}, {}, {:?})",
                    pubkey, slot, store_id, offset, load_hint
                );
            } else if fallback_to_slow_path {
                // the above bad-index-entry check must had been checked first to retain the same
                // behavior
                return Some((
                    maybe_account_accessor.expect("must be some if clone_in_lock=true"),
                    new_slot,
                ));
            }

            slot = new_slot;
            store_id = new_store_id;
            offset = new_offset;
        }
    }

    fn do_load(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
        max_root: Option<Slot>,
        load_hint: LoadHint,
        load_into_read_only_cache_only: bool,
    ) -> Option<(AccountSharedData, Slot)> {
        #[cfg(not(test))]
        assert!(max_root.is_none());

        let (slot, store_id, offset, _maybe_account_accesor) =
            self.read_index_for_accessor_or_load_slow(ancestors, pubkey, max_root, false)?;
        // Notice the subtle `?` at previous line, we bail out pretty early if missing.

        if self.caching_enabled {
            if load_into_read_only_cache_only {
                if store_id == CACHE_VIRTUAL_STORAGE_ID {
                    return None;
                }
                if self.read_only_accounts_cache.in_cache(pubkey, slot) {
                    return None;
                }
            } else if store_id != CACHE_VIRTUAL_STORAGE_ID {
                let result = self.read_only_accounts_cache.load(pubkey, slot);
                if let Some(account) = result {
                    return Some((account, slot));
                }
            }
        }

        let (mut account_accessor, slot) = self.retry_to_get_account_accessor(
            slot, store_id, offset, ancestors, pubkey, max_root, load_hint,
        )?;
        let loaded_account = account_accessor.check_and_get_loaded_account();
        let is_cached = loaded_account.is_cached();
        let account = loaded_account.take_account();

        if self.caching_enabled && !is_cached {
            /*
            We show this store into the read-only cache for account 'A' and future loads of 'A' from the read-only cache are
            safe/reflect 'A''s latest state on this fork.
            This safety holds if during replay of slot 'S', we show we only read 'A' from the write cache,
            not the read-only cache, after it's been updated in replay of slot 'S'.
            Assume for contradiction this is not true, and we read 'A' from the read-only cache *after* it had been updated in 'S'.
            This means an entry '(S, A)' was added to the read-only cache after 'A' had been updated in 'S'.
            Now when '(S, A)' was being added to the read-only cache, it must have been true that  'is_cache == false',
            which means '(S', A)' does not exist in the write cache yet.
            However, by the assumption for contradiction above ,  'A' has already been updated in 'S' which means '(S, A)'
            must exist in the write cache, which is a contradiction.
            */
            self.read_only_accounts_cache.store(pubkey, slot, &account);
        }
        Some((account, slot))
    }

    pub fn load_account_hash(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
        max_root: Option<Slot>,
        load_hint: LoadHint,
    ) -> Option<Hash> {
        let (slot, store_id, offset, _maybe_account_accesor) =
            self.read_index_for_accessor_or_load_slow(ancestors, pubkey, max_root, false)?;
        // Notice the subtle `?` at previous line, we bail out pretty early if missing.

        let (mut account_accessor, _) = self.retry_to_get_account_accessor(
            slot, store_id, offset, ancestors, pubkey, max_root, load_hint,
        )?;
        let loaded_account = account_accessor.check_and_get_loaded_account();
        Some(loaded_account.loaded_hash())
    }

    fn get_account_accessor<'a>(
        &'a self,
        slot: Slot,
        pubkey: &'a Pubkey,
        store_id: usize,
        offset: usize,
    ) -> LoadedAccountAccessor<'a> {
        if store_id == CACHE_VIRTUAL_STORAGE_ID {
            let maybe_cached_account = self
                .accounts_cache
                .load(slot, pubkey)
                .map(|cached_account| (*pubkey, Cow::Owned(cached_account)));
            LoadedAccountAccessor::Cached(maybe_cached_account)
        } else {
            let maybe_storage_entry = self
                .storage
                .get_account_storage_entry(slot, store_id)
                .map(|account_storage_entry| (account_storage_entry, offset));
            LoadedAccountAccessor::Stored(maybe_storage_entry)
        }
    }

    fn try_recycle_and_insert_store(
        &self,
        slot: Slot,
        min_size: u64,
        max_size: u64,
    ) -> Option<Arc<AccountStorageEntry>> {
        let store = self.try_recycle_store(slot, min_size, max_size)?;
        self.insert_store(slot, store.clone());
        Some(store)
    }

    fn try_recycle_store(
        &self,
        slot: Slot,
        min_size: u64,
        max_size: u64,
    ) -> Option<Arc<AccountStorageEntry>> {
        let mut max = 0;
        let mut min = std::u64::MAX;
        let mut avail = 0;
        let mut recycle_stores = self.recycle_stores.write().unwrap();
        for (i, (_recycled_time, store)) in recycle_stores.iter().enumerate() {
            if Arc::strong_count(store) == 1 {
                max = std::cmp::max(store.accounts.capacity(), max);
                min = std::cmp::min(store.accounts.capacity(), min);
                avail += 1;

                if store.accounts.capacity() >= min_size && store.accounts.capacity() < max_size {
                    let ret = recycle_stores.remove_entry(i);
                    drop(recycle_stores);
                    let old_id = ret.append_vec_id();
                    ret.recycle(slot, self.next_id.fetch_add(1, Ordering::Relaxed));
                    debug!(
                        "recycling store: {} {:?} old_id: {}",
                        ret.append_vec_id(),
                        ret.get_path(),
                        old_id
                    );
                    return Some(ret);
                }
            }
        }
        debug!(
            "no recycle stores max: {} min: {} len: {} looking: {}, {} avail: {}",
            max,
            min,
            recycle_stores.entry_count(),
            min_size,
            max_size,
            avail,
        );
        None
    }

    fn find_storage_candidate(&self, slot: Slot, size: usize) -> Arc<AccountStorageEntry> {
        let mut create_extra = false;
        let mut get_slot_stores = Measure::start("get_slot_stores");
        let slot_stores_lock = self.storage.get_slot_stores(slot);
        get_slot_stores.stop();
        self.stats
            .store_get_slot_store
            .fetch_add(get_slot_stores.as_us(), Ordering::Relaxed);
        let mut find_existing = Measure::start("find_existing");
        if let Some(slot_stores_lock) = slot_stores_lock {
            let slot_stores = slot_stores_lock.read().unwrap();
            if !slot_stores.is_empty() {
                if slot_stores.len() <= self.min_num_stores {
                    let mut total_accounts = 0;
                    for store in slot_stores.values() {
                        total_accounts += store.count();
                    }

                    // Create more stores so that when scanning the storage all CPUs have work
                    if (total_accounts / 16) >= slot_stores.len() {
                        create_extra = true;
                    }
                }

                // pick an available store at random by iterating from a random point
                let to_skip = thread_rng().gen_range(0, slot_stores.len());

                for (i, store) in slot_stores.values().cycle().skip(to_skip).enumerate() {
                    if store.try_available() {
                        let ret = store.clone();
                        drop(slot_stores);
                        if create_extra {
                            if self
                                .try_recycle_and_insert_store(slot, size as u64, std::u64::MAX)
                                .is_none()
                            {
                                self.stats
                                    .create_store_count
                                    .fetch_add(1, Ordering::Relaxed);
                                self.create_and_insert_store(slot, self.file_size, "store extra");
                            } else {
                                self.stats
                                    .recycle_store_count
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        find_existing.stop();
                        self.stats
                            .store_find_existing
                            .fetch_add(find_existing.as_us(), Ordering::Relaxed);
                        return ret;
                    }
                    // looked at every store, bail...
                    if i == slot_stores.len() {
                        break;
                    }
                }
            }
        }
        find_existing.stop();
        self.stats
            .store_find_existing
            .fetch_add(find_existing.as_us(), Ordering::Relaxed);

        let store = if let Some(store) = self.try_recycle_store(slot, size as u64, std::u64::MAX) {
            self.stats
                .recycle_store_count
                .fetch_add(1, Ordering::Relaxed);
            store
        } else {
            self.stats
                .create_store_count
                .fetch_add(1, Ordering::Relaxed);
            self.create_store(slot, self.file_size, "store", &self.paths)
        };

        // try_available is like taking a lock on the store,
        // preventing other threads from using it.
        // It must succeed here and happen before insert,
        // otherwise another thread could also grab it from the index.
        assert!(store.try_available());
        self.insert_store(slot, store.clone());
        store
    }

    fn page_align(size: u64) -> u64 {
        (size + (PAGE_SIZE - 1)) & !(PAGE_SIZE - 1)
    }

    fn has_space_available(&self, slot: Slot, size: u64) -> bool {
        let slot_storage = self.storage.get_slot_stores(slot).unwrap();
        let slot_storage_r = slot_storage.read().unwrap();
        for (_id, store) in slot_storage_r.iter() {
            if store.status() == AccountStorageStatus::Available
                && (store.accounts.capacity() - store.accounts.len() as u64) > size
            {
                return true;
            }
        }
        false
    }

    fn create_store(
        &self,
        slot: Slot,
        size: u64,
        from: &str,
        paths: &[PathBuf],
    ) -> Arc<AccountStorageEntry> {
        let path_index = thread_rng().gen_range(0, paths.len());
        let store = Arc::new(self.new_storage_entry(
            slot,
            Path::new(&paths[path_index]),
            Self::page_align(size),
        ));

        if store.append_vec_id() == CACHE_VIRTUAL_STORAGE_ID {
            panic!("We've run out of storage ids!");
        }

        debug!(
            "creating store: {} slot: {} len: {} size: {} from: {} path: {:?}",
            store.append_vec_id(),
            slot,
            store.accounts.len(),
            store.accounts.capacity(),
            from,
            store.accounts.get_path()
        );

        store
    }

    fn create_and_insert_store(
        &self,
        slot: Slot,
        size: u64,
        from: &str,
    ) -> Arc<AccountStorageEntry> {
        self.create_and_insert_store_with_paths(slot, size, from, &self.paths)
    }

    fn create_and_insert_store_with_paths(
        &self,
        slot: Slot,
        size: u64,
        from: &str,
        paths: &[PathBuf],
    ) -> Arc<AccountStorageEntry> {
        let store = self.create_store(slot, size, from, paths);
        let store_for_index = store.clone();

        self.insert_store(slot, store_for_index);
        store
    }

    fn insert_store(&self, slot: Slot, store: Arc<AccountStorageEntry>) {
        let slot_storages: SlotStores = self.storage.get_slot_stores(slot).unwrap_or_else(||
            // DashMap entry.or_insert() returns a RefMut, essentially a write lock,
            // which is dropped after this block ends, minimizing time held by the lock.
            // However, we still want to persist the reference to the `SlotStores` behind
            // the lock, hence we clone it out, (`SlotStores` is an Arc so is cheap to clone).
            self.storage
                .0
                .entry(slot)
                .or_insert(Arc::new(RwLock::new(HashMap::new())))
                .clone());

        assert!(slot_storages
            .write()
            .unwrap()
            .insert(store.append_vec_id(), store)
            .is_none());
    }

    pub fn create_drop_bank_callback(
        &self,
        pruned_banks_sender: DroppedSlotsSender,
    ) -> SendDroppedBankCallback {
        self.is_bank_drop_callback_enabled
            .store(true, Ordering::SeqCst);
        SendDroppedBankCallback::new(pruned_banks_sender)
    }

    /// This should only be called after the `Bank::drop()` runs in bank.rs, See BANK_DROP_SAFETY
    /// comment below for more explanation.
    /// `is_from_abs` is true if the caller is the AccountsBackgroundService
    pub fn purge_slot(&self, slot: Slot, bank_id: BankId, is_from_abs: bool) {
        if self.is_bank_drop_callback_enabled.load(Ordering::SeqCst) && !is_from_abs {
            panic!("bad drop callpath detected; Bank::drop() must run serially with other logic in ABS like clean_accounts()")
        }
        // BANK_DROP_SAFETY: Because this function only runs once the bank is dropped,
        // we know that there are no longer any ongoing scans on this bank, because scans require
        // and hold a reference to the bank at the tip of the fork they're scanning. Hence it's
        // safe to remove this bank_id from the `removed_bank_ids` list at this point.
        if self
            .accounts_index
            .removed_bank_ids
            .lock()
            .unwrap()
            .remove(&bank_id)
        {
            // If this slot was already cleaned up, no need to do any further cleans
            return;
        }

        self.purge_slots(std::iter::once(&slot));
    }

    fn recycle_slot_stores(
        &self,
        total_removed_storage_entries: usize,
        slot_stores: &[SlotStores],
    ) -> u64 {
        let mut recycled_count = 0;

        let mut recycle_stores_write_elapsed = Measure::start("recycle_stores_write_elapsed");
        let mut recycle_stores = self.recycle_stores.write().unwrap();
        recycle_stores_write_elapsed.stop();

        for slot_entries in slot_stores {
            let entry = slot_entries.read().unwrap();
            for (_store_id, stores) in entry.iter() {
                if recycle_stores.entry_count() > MAX_RECYCLE_STORES {
                    let dropped_count = total_removed_storage_entries - recycled_count;
                    self.stats
                        .dropped_stores
                        .fetch_add(dropped_count as u64, Ordering::Relaxed);
                    return recycle_stores_write_elapsed.as_us();
                }
                recycle_stores.add_entry(stores.clone());
                recycled_count += 1;
            }
        }
        recycle_stores_write_elapsed.as_us()
    }

    /// Purges every slot in `removed_slots` from both the cache and storage. This includes
    /// entries in the accounts index, cache entries, and any backing storage entries.
    fn purge_slots_from_cache_and_store<'a>(
        &self,
        removed_slots: impl Iterator<Item = &'a Slot>,
        purge_stats: &PurgeStats,
    ) {
        let mut remove_cache_elapsed_across_slots = 0;
        let mut num_cached_slots_removed = 0;
        let mut total_removed_cached_bytes = 0;
        for remove_slot in removed_slots {
            // This function is only currently safe with respect to `flush_slot_cache()` because
            // both functions run serially in AccountsBackgroundService.
            let mut remove_cache_elapsed = Measure::start("remove_cache_elapsed");
            // Note: we cannot remove this slot from the slot cache until we've removed its
            // entries from the accounts index first. This is because `scan_accounts()` relies on
            // holding the index lock, finding the index entry, and then looking up the entry
            // in the cache. If it fails to find that entry, it will panic in `get_loaded_account()`
            if let Some(slot_cache) = self.accounts_cache.slot_cache(*remove_slot) {
                // If the slot is still in the cache, remove the backing storages for
                // the slot and from the Accounts Index
                num_cached_slots_removed += 1;
                total_removed_cached_bytes += slot_cache.total_bytes();
                self.purge_slot_cache(*remove_slot, slot_cache);
                remove_cache_elapsed.stop();
                remove_cache_elapsed_across_slots += remove_cache_elapsed.as_us();
                // Nobody else shoud have removed the slot cache entry yet
                assert!(self.accounts_cache.remove_slot(*remove_slot).is_some());
            } else {
                self.purge_slot_storage(*remove_slot, purge_stats);
            }
            // It should not be possible that a slot is neither in the cache or storage. Even in
            // a slot with all ticks, `Bank::new_from_parent()` immediately stores some sysvars
            // on bank creation.
        }

        purge_stats
            .remove_cache_elapsed
            .fetch_add(remove_cache_elapsed_across_slots, Ordering::Relaxed);
        purge_stats
            .num_cached_slots_removed
            .fetch_add(num_cached_slots_removed, Ordering::Relaxed);
        purge_stats
            .total_removed_cached_bytes
            .fetch_add(total_removed_cached_bytes, Ordering::Relaxed);
    }

    /// Purge the backing storage entries for the given slot, does not purge from
    /// the cache!
    fn purge_dead_slots_from_storage<'a>(
        &'a self,
        removed_slots: impl Iterator<Item = &'a Slot> + Clone,
        purge_stats: &PurgeStats,
    ) {
        // Check all slots `removed_slots` are no longer "relevant" roots.
        // Note that the slots here could have been rooted slots, but if they're passed here
        // for removal it means:
        // 1) All updates in that old root have been outdated by updates in newer roots
        // 2) Those slots/roots should have already been purged from the accounts index root
        // tracking metadata via `accounts_index.clean_dead_slot()`.
        let mut safety_checks_elapsed = Measure::start("safety_checks_elapsed");
        assert!(self
            .accounts_index
            .get_rooted_from_list(removed_slots.clone())
            .is_empty());
        safety_checks_elapsed.stop();
        purge_stats
            .safety_checks_elapsed
            .fetch_add(safety_checks_elapsed.as_us(), Ordering::Relaxed);

        let mut total_removed_storage_entries = 0;
        let mut total_removed_stored_bytes = 0;
        let mut all_removed_slot_storages = vec![];

        let mut remove_storage_entries_elapsed = Measure::start("remove_storage_entries_elapsed");
        for remove_slot in removed_slots {
            // Remove the storage entries and collect some metrics
            if let Some((_, slot_storages_to_be_removed)) = self.storage.0.remove(remove_slot) {
                {
                    let r_slot_removed_storages = slot_storages_to_be_removed.read().unwrap();
                    total_removed_storage_entries += r_slot_removed_storages.len();
                    total_removed_stored_bytes += r_slot_removed_storages
                        .values()
                        .map(|i| i.accounts.capacity())
                        .sum::<u64>();
                }
                all_removed_slot_storages.push(slot_storages_to_be_removed.clone());
            }
        }
        remove_storage_entries_elapsed.stop();
        let num_stored_slots_removed = all_removed_slot_storages.len();

        let recycle_stores_write_elapsed =
            self.recycle_slot_stores(total_removed_storage_entries, &all_removed_slot_storages);

        let mut drop_storage_entries_elapsed = Measure::start("drop_storage_entries_elapsed");
        // Backing mmaps for removed storages entries explicitly dropped here outside
        // of any locks
        drop(all_removed_slot_storages);
        drop_storage_entries_elapsed.stop();
        purge_stats
            .remove_storage_entries_elapsed
            .fetch_add(remove_storage_entries_elapsed.as_us(), Ordering::Relaxed);
        purge_stats
            .drop_storage_entries_elapsed
            .fetch_add(drop_storage_entries_elapsed.as_us(), Ordering::Relaxed);
        purge_stats
            .num_stored_slots_removed
            .fetch_add(num_stored_slots_removed, Ordering::Relaxed);
        purge_stats
            .total_removed_storage_entries
            .fetch_add(total_removed_storage_entries, Ordering::Relaxed);
        purge_stats
            .total_removed_stored_bytes
            .fetch_add(total_removed_stored_bytes, Ordering::Relaxed);
        purge_stats
            .recycle_stores_write_elapsed
            .fetch_add(recycle_stores_write_elapsed, Ordering::Relaxed);
    }

    fn purge_slot_cache(&self, purged_slot: Slot, slot_cache: SlotCache) {
        let mut purged_slot_pubkeys: HashSet<(Slot, Pubkey)> = HashSet::new();
        let pubkey_to_slot_set: Vec<(Pubkey, Slot)> = slot_cache
            .iter()
            .map(|account| {
                purged_slot_pubkeys.insert((purged_slot, *account.key()));
                (*account.key(), purged_slot)
            })
            .collect();
        self.purge_slot_cache_pubkeys(purged_slot, purged_slot_pubkeys, pubkey_to_slot_set, true);
    }

    fn purge_slot_cache_pubkeys(
        &self,
        purged_slot: Slot,
        purged_slot_pubkeys: HashSet<(Slot, Pubkey)>,
        pubkey_to_slot_set: Vec<(Pubkey, Slot)>,
        is_dead: bool,
    ) {
        // Slot purged from cache should not exist in the backing store
        assert!(self.storage.get_slot_stores(purged_slot).is_none());
        let num_purged_keys = pubkey_to_slot_set.len();
        let reclaims = self.purge_keys_exact(pubkey_to_slot_set.iter());
        assert_eq!(reclaims.len(), num_purged_keys);
        if is_dead {
            self.remove_dead_slots_metadata(
                std::iter::once(&purged_slot),
                purged_slot_pubkeys,
                None,
            );
        }
    }

    fn purge_slot_storage(&self, remove_slot: Slot, purge_stats: &PurgeStats) {
        // Because AccountsBackgroundService synchronously flushes from the accounts cache
        // and handles all Bank::drop() (the cleanup function that leads to this
        // function call), then we don't need to worry above an overlapping cache flush
        // with this function call. This means, if we get into this case, we can be
        // confident that the entire state for this slot has been flushed to the storage
        // already.
        let mut scan_storages_elasped = Measure::start("scan_storages_elasped");
        type ScanResult = ScanStorageResult<Pubkey, Arc<Mutex<HashSet<(Pubkey, Slot)>>>>;
        let scan_result: ScanResult = self.scan_account_storage(
            remove_slot,
            |loaded_account: LoadedAccount| Some(*loaded_account.pubkey()),
            |accum: &Arc<Mutex<HashSet<(Pubkey, Slot)>>>, loaded_account: LoadedAccount| {
                accum
                    .lock()
                    .unwrap()
                    .insert((*loaded_account.pubkey(), remove_slot));
            },
        );
        scan_storages_elasped.stop();
        purge_stats
            .scan_storages_elasped
            .fetch_add(scan_storages_elasped.as_us(), Ordering::Relaxed);

        let mut purge_accounts_index_elapsed = Measure::start("purge_accounts_index_elapsed");
        let reclaims;
        match scan_result {
            ScanStorageResult::Cached(_) => {
                panic!("Should not see cached keys in this `else` branch, since we checked this slot did not exist in the cache above");
            }
            ScanStorageResult::Stored(stored_keys) => {
                // Purge this slot from the accounts index
                reclaims = self.purge_keys_exact(stored_keys.lock().unwrap().iter());
            }
        }
        purge_accounts_index_elapsed.stop();
        purge_stats
            .purge_accounts_index_elapsed
            .fetch_add(purge_accounts_index_elapsed.as_us(), Ordering::Relaxed);

        // `handle_reclaims()` should remove all the account index entries and
        // storage entries
        let mut handle_reclaims_elapsed = Measure::start("handle_reclaims_elapsed");
        // Slot should be dead after removing all its account entries
        let expected_dead_slot = Some(remove_slot);
        self.handle_reclaims(
            &reclaims,
            expected_dead_slot,
            Some(purge_stats),
            Some(&mut ReclaimResult::default()),
            false,
        );
        handle_reclaims_elapsed.stop();
        purge_stats
            .handle_reclaims_elapsed
            .fetch_add(handle_reclaims_elapsed.as_us(), Ordering::Relaxed);
        // After handling the reclaimed entries, this slot's
        // storage entries should be purged from self.storage
        assert!(self.storage.get_slot_stores(remove_slot).is_none());
    }

    #[allow(clippy::needless_collect)]
    fn purge_slots<'a>(&self, slots: impl Iterator<Item = &'a Slot>) {
        // `add_root()` should be called first
        let mut safety_checks_elapsed = Measure::start("safety_checks_elapsed");
        let non_roots = slots
            // Only safe to check when there are  duplciate versions of a slot
            // because ReplayStage will not make new roots before dumping the
            // duplicate slots first. Thus we will not be in a case where we
            // root slot `S`, then try to dump some other version of slot `S`, the
            // dumping has to finish first
            //
            // Also note roots are never removed via `remove_unrooted_slot()`, so
            // it's safe to filter them out here as they won't need deletion from
            // self.accounts_index.removed_bank_ids in `purge_slots_from_cache_and_store()`.
            .filter(|slot| !self.accounts_index.is_root(**slot));
        safety_checks_elapsed.stop();
        self.external_purge_slots_stats
            .safety_checks_elapsed
            .fetch_add(safety_checks_elapsed.as_us(), Ordering::Relaxed);
        self.purge_slots_from_cache_and_store(non_roots, &self.external_purge_slots_stats);
        self.external_purge_slots_stats
            .report("external_purge_slots_stats", Some(1000));
    }

    pub fn remove_unrooted_slots(&self, remove_slots: &[(Slot, BankId)]) {
        let rooted_slots = self
            .accounts_index
            .get_rooted_from_list(remove_slots.iter().map(|(slot, _)| slot));
        assert!(
            rooted_slots.is_empty(),
            "Trying to remove accounts for rooted slots {:?}",
            rooted_slots
        );

        let RemoveUnrootedSlotsSynchronization {
            slots_under_contention,
            signal,
        } = &self.remove_unrooted_slots_synchronization;

        {
            // Slots that are currently being flushed by flush_slot_cache()

            let mut currently_contended_slots = slots_under_contention.lock().unwrap();

            // Slots that are currently being flushed by flush_slot_cache() AND
            // we want to remove in this function
            let mut remaining_contended_flush_slots: Vec<Slot> = remove_slots
                .iter()
                .filter_map(|(remove_slot, _)| {
                    let is_being_flushed = currently_contended_slots.contains(remove_slot);
                    if !is_being_flushed {
                        // Reserve the slots that we want to purge that aren't currently
                        // being flushed to prevent cache from flushing those slots in
                        // the future.
                        //
                        // Note that the single replay thread has to remove a specific slot `N`
                        // before another version of the same slot can be replayed. This means
                        // multiple threads should not call `remove_unrooted_slots()` simultaneously
                        // with the same slot.
                        currently_contended_slots.insert(*remove_slot);
                    }
                    // If the cache is currently flushing this slot, add it to the list
                    Some(remove_slot).filter(|_| is_being_flushed)
                })
                .cloned()
                .collect();

            // Wait for cache flushes to finish
            loop {
                if !remaining_contended_flush_slots.is_empty() {
                    // Wait for the signal that the cache has finished flushing a slot
                    //
                    // Don't wait if the remaining_contended_flush_slots is empty, otherwise
                    // we may never get a signal since there's no cache flush thread to
                    // do the signaling
                    currently_contended_slots = signal.wait(currently_contended_slots).unwrap();
                } else {
                    // There are no slots being flushed to wait on, so it's safe to continue
                    // to purging the slots we want to purge!
                    break;
                }

                // For each slot the cache flush has finished, mark that we're about to start
                // purging these slots by reserving it in `currently_contended_slots`.
                remaining_contended_flush_slots.retain(|flush_slot| {
                    let is_being_flushed = currently_contended_slots.contains(flush_slot);
                    if !is_being_flushed {
                        // Mark that we're about to delete this slot now
                        currently_contended_slots.insert(*flush_slot);
                    }
                    is_being_flushed
                });
            }
        }

        // Mark down these slots are about to be purged so that new attempts to scan these
        // banks fail, and any ongoing scans over these slots will detect that they should abort
        // their results
        {
            let mut locked_removed_bank_ids = self.accounts_index.removed_bank_ids.lock().unwrap();
            for (_slot, remove_bank_id) in remove_slots.iter() {
                locked_removed_bank_ids.insert(*remove_bank_id);
            }
        }

        let remove_unrooted_purge_stats = PurgeStats::default();
        self.purge_slots_from_cache_and_store(
            remove_slots.iter().map(|(slot, _)| slot),
            &remove_unrooted_purge_stats,
        );
        remove_unrooted_purge_stats.report("remove_unrooted_slots_purge_slots_stats", Some(0));

        let mut currently_contended_slots = slots_under_contention.lock().unwrap();
        for (remove_slot, _) in remove_slots {
            assert!(currently_contended_slots.remove(remove_slot));
        }
    }

    pub fn hash_stored_account(slot: Slot, account: &StoredAccountMeta) -> Hash {
        Self::hash_account_data(
            slot,
            account.account_meta.lamports,
            &account.account_meta.owner,
            account.account_meta.executable,
            account.account_meta.rent_epoch,
            account.data,
            &account.meta.pubkey,
        )
    }

    pub fn hash_account<T: ReadableAccount>(slot: Slot, account: &T, pubkey: &Pubkey) -> Hash {
        Self::hash_account_data(
            slot,
            account.lamports(),
            account.owner(),
            account.executable(),
            account.rent_epoch(),
            account.data(),
            pubkey,
        )
    }

    fn hash_frozen_account_data(account: &AccountSharedData) -> Hash {
        let mut hasher = Hasher::default();

        hasher.hash(account.data());
        hasher.hash(account.owner().as_ref());

        if account.executable() {
            hasher.hash(&[1u8; 1]);
        } else {
            hasher.hash(&[0u8; 1]);
        }

        hasher.result()
    }

    fn hash_account_data(
        slot: Slot,
        lamports: u64,
        owner: &Pubkey,
        executable: bool,
        rent_epoch: Epoch,
        data: &[u8],
        pubkey: &Pubkey,
    ) -> Hash {
        if lamports == 0 {
            return Hash::default();
        }

        let mut hasher = blake3::Hasher::new();

        hasher.update(&lamports.to_le_bytes());

        hasher.update(&slot.to_le_bytes());

        hasher.update(&rent_epoch.to_le_bytes());

        hasher.update(data);

        if executable {
            hasher.update(&[1u8; 1]);
        } else {
            hasher.update(&[0u8; 1]);
        }

        hasher.update(owner.as_ref());
        hasher.update(pubkey.as_ref());

        Hash(<[u8; solana_sdk::hash::HASH_BYTES]>::try_from(hasher.finalize().as_slice()).unwrap())
    }

    fn bulk_assign_write_version(&self, count: usize) -> StoredMetaWriteVersion {
        self.write_version
            .fetch_add(count as StoredMetaWriteVersion, Ordering::Relaxed)
    }

    fn write_accounts_to_storage<F: FnMut(Slot, usize) -> Arc<AccountStorageEntry>>(
        &self,
        slot: Slot,
        hashes: &[impl Borrow<Hash>],
        mut storage_finder: F,
        accounts_and_meta_to_store: &[(StoredMeta, Option<&impl ReadableAccount>)],
    ) -> Vec<AccountInfo> {
        assert_eq!(hashes.len(), accounts_and_meta_to_store.len());
        let mut infos: Vec<AccountInfo> = Vec::with_capacity(accounts_and_meta_to_store.len());
        let mut total_append_accounts_us = 0;
        let mut total_storage_find_us = 0;
        while infos.len() < accounts_and_meta_to_store.len() {
            let mut storage_find = Measure::start("storage_finder");
            let data_len = accounts_and_meta_to_store[infos.len()]
                .1
                .map(|account| account.data().len())
                .unwrap_or_default();
            let storage = storage_finder(slot, data_len + STORE_META_OVERHEAD);
            storage_find.stop();
            total_storage_find_us += storage_find.as_us();
            let mut append_accounts = Measure::start("append_accounts");
            let rvs = storage.accounts.append_accounts(
                &accounts_and_meta_to_store[infos.len()..],
                &hashes[infos.len()..],
            );
            assert!(!rvs.is_empty());
            append_accounts.stop();
            total_append_accounts_us += append_accounts.as_us();
            if rvs.len() == 1 {
                storage.set_status(AccountStorageStatus::Full);

                // See if an account overflows the append vecs in the slot.
                let data_len = (data_len + STORE_META_OVERHEAD) as u64;
                if !self.has_space_available(slot, data_len) {
                    let special_store_size = std::cmp::max(data_len * 2, self.file_size);
                    if self
                        .try_recycle_and_insert_store(slot, special_store_size, std::u64::MAX)
                        .is_none()
                    {
                        self.stats
                            .create_store_count
                            .fetch_add(1, Ordering::Relaxed);
                        self.create_and_insert_store(slot, special_store_size, "large create");
                    } else {
                        self.stats
                            .recycle_store_count
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                continue;
            }

            for (offsets, (_, account)) in rvs
                .windows(2)
                .zip(&accounts_and_meta_to_store[infos.len()..])
            {
                let stored_size = offsets[1] - offsets[0];
                storage.add_account(stored_size);
                infos.push(AccountInfo {
                    store_id: storage.append_vec_id(),
                    offset: offsets[0],
                    stored_size,
                    lamports: account
                        .map(|account| account.lamports())
                        .unwrap_or_default(),
                });
            }
            // restore the state to available
            storage.set_status(AccountStorageStatus::Available);
        }

        self.stats
            .store_append_accounts
            .fetch_add(total_append_accounts_us, Ordering::Relaxed);
        self.stats
            .store_find_store
            .fetch_add(total_storage_find_us, Ordering::Relaxed);
        infos
    }

    pub fn mark_slot_frozen(&self, slot: Slot) {
        if let Some(slot_cache) = self.accounts_cache.slot_cache(slot) {
            slot_cache.mark_slot_frozen();
            slot_cache.report_slot_store_metrics();
        }
        self.accounts_cache.report_size();
    }

    pub fn expire_old_recycle_stores(&self) {
        let mut recycle_stores_write_elapsed = Measure::start("recycle_stores_write_time");
        let recycle_stores = self.recycle_stores.write().unwrap().expire_old_entries();
        recycle_stores_write_elapsed.stop();

        let mut drop_storage_entries_elapsed = Measure::start("drop_storage_entries_elapsed");
        drop(recycle_stores);
        drop_storage_entries_elapsed.stop();

        self.clean_accounts_stats
            .purge_stats
            .drop_storage_entries_elapsed
            .fetch_add(drop_storage_entries_elapsed.as_us(), Ordering::Relaxed);
        self.clean_accounts_stats
            .purge_stats
            .recycle_stores_write_elapsed
            .fetch_add(recycle_stores_write_elapsed.as_us(), Ordering::Relaxed);
    }

    // `force_flush` flushes all the cached roots `<= requested_flush_root`. It also then
    // flushes:
    // 1) Any remaining roots if there are > MAX_CACHE_SLOTS remaining slots in the cache,
    // 2) It there are still > MAX_CACHE_SLOTS remaining slots in the cache, the excess
    // unrooted slots
    pub fn flush_accounts_cache(&self, force_flush: bool, requested_flush_root: Option<Slot>) {
        #[cfg(not(test))]
        assert!(requested_flush_root.is_some());

        if !force_flush && self.accounts_cache.num_slots() <= MAX_CACHE_SLOTS {
            return;
        }

        // Flush only the roots <= requested_flush_root, so that snapshotting has all
        // the relevant roots in storage.
        let mut flush_roots_elapsed = Measure::start("flush_roots_elapsed");
        let mut account_bytes_saved = 0;
        let mut num_accounts_saved = 0;

        // Note even if force_flush is false, we will still flush all roots <= the
        // given `requested_flush_root`, even if some of the later roots cannot be used for
        // cleaning due to an ongoing scan
        let (total_new_cleaned_roots, num_cleaned_roots_flushed) = self
            .flush_rooted_accounts_cache(
                requested_flush_root,
                Some((&mut account_bytes_saved, &mut num_accounts_saved)),
            );
        flush_roots_elapsed.stop();

        // Note we don't purge unrooted slots here because there may be ongoing scans/references
        // for those slot, let the Bank::drop() implementation do cleanup instead on dead
        // banks

        // If there are > MAX_CACHE_SLOTS, then flush the excess ones to storage
        let (total_new_excess_roots, num_excess_roots_flushed) =
            if self.accounts_cache.num_slots() > MAX_CACHE_SLOTS {
                // Start by flushing the roots
                //
                // Cannot do any cleaning on roots past `requested_flush_root` because future
                // snapshots may need updates from those later slots, hence we pass `None`
                // for `should_clean`.
                self.flush_rooted_accounts_cache(None, None)
            } else {
                (0, 0)
            };
        let old_slots = self.accounts_cache.find_older_frozen_slots(MAX_CACHE_SLOTS);
        let excess_slot_count = old_slots.len();
        let mut unflushable_unrooted_slot_count = 0;
        let max_flushed_root = self.accounts_cache.fetch_max_flush_root();
        let old_slot_flush_stats: Vec<_> = old_slots
            .into_iter()
            .filter_map(|old_slot| {
                // Don't flush slots that are known to be unrooted
                if old_slot > max_flushed_root {
                    Some(self.flush_slot_cache(old_slot, None::<&mut fn(&_, &_) -> bool>))
                } else {
                    unflushable_unrooted_slot_count += 1;
                    None
                }
            })
            .collect();
        info!(
            "req_flush_root: {:?} old_slot_flushes: {:?}",
            requested_flush_root, old_slot_flush_stats
        );

        datapoint_info!(
            "accounts_db-flush_accounts_cache",
            ("total_new_cleaned_roots", total_new_cleaned_roots, i64),
            ("num_cleaned_roots_flushed", num_cleaned_roots_flushed, i64),
            ("total_new_excess_roots", total_new_excess_roots, i64),
            ("num_excess_roots_flushed", num_excess_roots_flushed, i64),
            ("excess_slot_count", excess_slot_count, i64),
            (
                "unflushable_unrooted_slot_count",
                unflushable_unrooted_slot_count,
                i64
            ),
            (
                "flush_roots_elapsed",
                flush_roots_elapsed.as_us() as i64,
                i64
            ),
            ("account_bytes_saved", account_bytes_saved, i64),
            ("num_accounts_saved", num_accounts_saved, i64),
        );

        // Flush a random slot out after every force flush to catch any inconsistencies
        // between cache and written state (i.e. should cause a hash mismatch between validators
        // that flush and don't flush if such a bug exists).
        let num_slots_remaining = self.accounts_cache.num_slots();
        if force_flush && num_slots_remaining >= FLUSH_CACHE_RANDOM_THRESHOLD {
            // Don't flush slots that are known to be unrooted
            let mut frozen_slots = self.accounts_cache.find_older_frozen_slots(0);
            frozen_slots.retain(|s| *s > max_flushed_root);
            // Remove a random index 0 <= i < `frozen_slots.len()`
            let rand_slot = frozen_slots.choose(&mut thread_rng());
            if let Some(rand_slot) = rand_slot {
                let random_flush_stats =
                    self.flush_slot_cache(*rand_slot, None::<&mut fn(&_, &_) -> bool>);
                info!(
                    "Flushed random slot: num_remaining: {} {:?}",
                    num_slots_remaining, random_flush_stats,
                );
            }
        }
    }

    fn flush_rooted_accounts_cache(
        &self,
        requested_flush_root: Option<Slot>,
        should_clean: Option<(&mut usize, &mut usize)>,
    ) -> (usize, usize) {
        let max_clean_root = should_clean.as_ref().and_then(|_| {
            // If there is a long running scan going on, this could prevent any cleaning
            // based on updates from slots > `max_clean_root`.
            self.max_clean_root(requested_flush_root)
        });

        // Use HashMap because HashSet doesn't provide Entry api
        let mut written_accounts = HashMap::new();

        // If `should_clean` is None, then`should_flush_f` is also None, which will cause
        // `flush_slot_cache` to flush all accounts to storage without cleaning any accounts.
        let mut should_flush_f = should_clean.map(|(account_bytes_saved, num_accounts_saved)| {
            move |&pubkey: &Pubkey, account: &AccountSharedData| {
                use std::collections::hash_map::Entry::{Occupied, Vacant};
                let should_flush = match written_accounts.entry(pubkey) {
                    Vacant(vacant_entry) => {
                        vacant_entry.insert(());
                        true
                    }
                    Occupied(_occupied_entry) => {
                        *account_bytes_saved += account.data().len();
                        *num_accounts_saved += 1;
                        // If a later root already wrote this account, no point
                        // in flushing it
                        false
                    }
                };
                should_flush
            }
        });

        // Always flush up to `requested_flush_root`, which is necessary for things like snapshotting.
        let cached_roots: BTreeSet<Slot> = self.accounts_cache.clear_roots(requested_flush_root);

        // Iterate from highest to lowest so that we don't need to flush earlier
        // outdated updates in earlier roots
        let mut num_roots_flushed = 0;
        for &root in cached_roots.iter().rev() {
            let should_flush_f = if let Some(max_clean_root) = max_clean_root {
                if root > max_clean_root {
                    // Only if the root is greater than the `max_clean_root` do we
                    // have to prevent cleaning, otherwise, just default to `should_flush_f`
                    // for any slots <= `max_clean_root`
                    None
                } else {
                    should_flush_f.as_mut()
                }
            } else {
                should_flush_f.as_mut()
            };

            if self.flush_slot_cache(root, should_flush_f).is_some() {
                num_roots_flushed += 1;
            }

            // Regardless of whether this slot was *just* flushed from the cache by the above
            // `flush_slot_cache()`, we should update the `max_flush_root`.
            // This is because some rooted slots may be flushed to storage *before* they are marked as root.
            // This can occur for instance when:
            // 1) The cache is overwhelmed, we we flushed some yet to be rooted frozen slots
            // 2) Random evictions
            // These slots may then *later* be marked as root, so we still need to handle updating the
            // `max_flush_root` in the accounts cache.
            self.accounts_cache.set_max_flush_root(root);
        }

        // Only add to the uncleaned roots set *after* we've flushed the previous roots,
        // so that clean will actually be able to clean the slots.
        let num_new_roots = cached_roots.len();
        self.accounts_index.add_uncleaned_roots(cached_roots);
        (num_new_roots, num_roots_flushed)
    }

    fn do_flush_slot_cache(
        &self,
        slot: Slot,
        slot_cache: &SlotCache,
        mut should_flush_f: Option<&mut impl FnMut(&Pubkey, &AccountSharedData) -> bool>,
    ) -> FlushStats {
        let mut num_purged = 0;
        let mut total_size = 0;
        let mut num_flushed = 0;
        let iter_items: Vec<_> = slot_cache.iter().collect();
        let mut purged_slot_pubkeys: HashSet<(Slot, Pubkey)> = HashSet::new();
        let mut pubkey_to_slot_set: Vec<(Pubkey, Slot)> = vec![];
        let (accounts, hashes): (Vec<(&Pubkey, &AccountSharedData)>, Vec<Hash>) = iter_items
            .iter()
            .filter_map(|iter_item| {
                let key = iter_item.key();
                let account = &iter_item.value().account;
                let should_flush = should_flush_f
                    .as_mut()
                    .map(|should_flush_f| should_flush_f(key, account))
                    .unwrap_or(true);
                if should_flush {
                    let hash = iter_item.value().hash();
                    total_size += (account.data().len() + STORE_META_OVERHEAD) as u64;
                    num_flushed += 1;
                    Some(((key, account), hash))
                } else {
                    // If we don't flush, we have to remove the entry from the
                    // index, since it's equivalent to purging
                    purged_slot_pubkeys.insert((slot, *key));
                    pubkey_to_slot_set.push((*key, slot));
                    num_purged += 1;
                    None
                }
            })
            .unzip();

        let is_dead_slot = accounts.is_empty();
        // Remove the account index entries from earlier roots that are outdated by later roots.
        // Safe because queries to the index will be reading updates from later roots.
        self.purge_slot_cache_pubkeys(slot, purged_slot_pubkeys, pubkey_to_slot_set, is_dead_slot);

        if !is_dead_slot {
            let aligned_total_size = Self::page_align(total_size);
            // This ensures that all updates are written to an AppendVec, before any
            // updates to the index happen, so anybody that sees a real entry in the index,
            // will be able to find the account in storage
            let flushed_store =
                self.create_and_insert_store(slot, aligned_total_size, "flush_slot_cache");
            self.store_accounts_frozen(
                slot,
                &accounts,
                Some(&hashes),
                Some(Box::new(move |_, _| flushed_store.clone())),
                None,
            );
            // If the above sizing function is correct, just one AppendVec is enough to hold
            // all the data for the slot
            assert_eq!(
                self.storage
                    .get_slot_stores(slot)
                    .unwrap()
                    .read()
                    .unwrap()
                    .len(),
                1
            );
        }

        // Remove this slot from the cache, which will to AccountsDb's new readers should look like an
        // atomic switch from the cache to storage.
        // There is some racy condition for existing readers who just has read exactly while
        // flushing. That case is handled by retry_to_get_account_accessor()
        assert!(self.accounts_cache.remove_slot(slot).is_some());
        FlushStats {
            slot,
            num_flushed,
            num_purged,
            total_size,
        }
    }

    /// `should_flush_f` is an optional closure that determines whether a given
    /// account should be flushed. Passing `None` will by default flush all
    /// accounts
    fn flush_slot_cache(
        &self,
        slot: Slot,
        should_flush_f: Option<&mut impl FnMut(&Pubkey, &AccountSharedData) -> bool>,
    ) -> Option<FlushStats> {
        let is_being_purged = {
            let mut slots_under_contention = self
                .remove_unrooted_slots_synchronization
                .slots_under_contention
                .lock()
                .unwrap();
            // If we're purging this slot, don't flush it here
            if slots_under_contention.contains(&slot) {
                true
            } else {
                slots_under_contention.insert(slot);
                false
            }
        };

        if !is_being_purged {
            let flush_stats = self.accounts_cache.slot_cache(slot).map(|slot_cache| {
                #[cfg(test)]
                {
                    // Give some time for cache flushing to occur here for unit tests
                    sleep(Duration::from_millis(self.load_delay));
                }
                // Since we added the slot to `slots_under_contention` AND this slot
                // still exists in the cache, we know the slot cannot be removed
                // by any other threads past this point. We are now responsible for
                // flushing this slot.
                self.do_flush_slot_cache(slot, &slot_cache, should_flush_f)
            });

            // Nobody else should have been purging this slot, so should not have been removed
            // from `self.remove_unrooted_slots_synchronization`.
            assert!(self
                .remove_unrooted_slots_synchronization
                .slots_under_contention
                .lock()
                .unwrap()
                .remove(&slot));

            // Signal to any threads blocked on `remove_unrooted_slots(slot)` that we have finished
            // flushing
            self.remove_unrooted_slots_synchronization
                .signal
                .notify_all();
            flush_stats
        } else {
            None
        }
    }

    fn write_accounts_to_cache(
        &self,
        slot: Slot,
        hashes: Option<&[impl Borrow<Hash>]>,
        accounts_and_meta_to_store: &[(StoredMeta, Option<&impl ReadableAccount>)],
    ) -> Vec<AccountInfo> {
        let len = accounts_and_meta_to_store.len();
        let hashes = hashes.map(|hashes| {
            assert_eq!(hashes.len(), len);
            hashes
        });

        accounts_and_meta_to_store
            .iter()
            .enumerate()
            .map(|(i, (meta, account))| {
                let hash = hashes.map(|hashes| hashes[i].borrow());

                let account = account
                    .map(|account| account.to_account_shared_data())
                    .unwrap_or_default();
                let account_info = AccountInfo {
                    store_id: CACHE_VIRTUAL_STORAGE_ID,
                    offset: CACHE_VIRTUAL_OFFSET,
                    stored_size: CACHE_VIRTUAL_STORED_SIZE,
                    lamports: account.lamports(),
                };

                let cached_account = self.accounts_cache.store(slot, &meta.pubkey, account, hash);
                // hash this account in the bg
                match &self.sender_bg_hasher {
                    Some(ref sender) => {
                        let _ = sender.send(cached_account);
                    }
                    None => (),
                };
                account_info
            })
            .collect()
    }

    fn store_accounts_to<
        F: FnMut(Slot, usize) -> Arc<AccountStorageEntry>,
        P: Iterator<Item = u64>,
    >(
        &self,
        slot: Slot,
        accounts: &[(&Pubkey, &impl ReadableAccount)],
        hashes: Option<&[impl Borrow<Hash>]>,
        storage_finder: F,
        mut write_version_producer: P,
        is_cached_store: bool,
    ) -> Vec<AccountInfo> {
        let mut calc_stored_meta_time = Measure::start("calc_stored_meta");
        let accounts_and_meta_to_store: Vec<_> = accounts
            .iter()
            .map(|(pubkey, account)| {
                self.read_only_accounts_cache.remove(pubkey, slot);
                // this is the source of Some(Account) or None.
                // Some(Account) = store 'Account'
                // None = store a default/empty account with 0 lamports
                let (account, data_len) = if account.lamports() == 0 {
                    (None, 0)
                } else {
                    (Some(*account), account.data().len() as u64)
                };
                let meta = StoredMeta {
                    write_version: write_version_producer.next().unwrap(),
                    pubkey: **pubkey,
                    data_len,
                };
                (meta, account)
            })
            .collect();
        calc_stored_meta_time.stop();
        self.stats
            .calc_stored_meta
            .fetch_add(calc_stored_meta_time.as_us(), Ordering::Relaxed);

        if self.caching_enabled && is_cached_store {
            self.write_accounts_to_cache(slot, hashes, &accounts_and_meta_to_store)
        } else {
            match hashes {
                Some(hashes) => self.write_accounts_to_storage(
                    slot,
                    hashes,
                    storage_finder,
                    &accounts_and_meta_to_store,
                ),
                None => {
                    // hash any accounts where we were lazy in calculating the hash
                    let mut hash_time = Measure::start("hash_accounts");
                    let mut stats = BankHashStats::default();
                    let len = accounts_and_meta_to_store.len();
                    let mut hashes = Vec::with_capacity(len);
                    for account in accounts {
                        stats.update(account.1);
                        let hash = Self::hash_account(slot, account.1, account.0);
                        hashes.push(hash);
                    }
                    hash_time.stop();
                    self.stats
                        .store_hash_accounts
                        .fetch_add(hash_time.as_us(), Ordering::Relaxed);

                    self.write_accounts_to_storage(
                        slot,
                        &hashes,
                        storage_finder,
                        &accounts_and_meta_to_store,
                    )
                }
            }
        }
    }

    fn report_store_stats(&self) {
        let mut total_count = 0;
        let mut min = std::usize::MAX;
        let mut min_slot = 0;
        let mut max = 0;
        let mut max_slot = 0;
        let mut newest_slot = 0;
        let mut oldest_slot = std::u64::MAX;
        for iter_item in self.storage.0.iter() {
            let slot = iter_item.key();
            let slot_stores = iter_item.value().read().unwrap();
            total_count += slot_stores.len();
            if slot_stores.len() < min {
                min = slot_stores.len();
                min_slot = *slot;
            }

            if slot_stores.len() > max {
                max = slot_stores.len();
                max_slot = *slot;
            }
            if *slot > newest_slot {
                newest_slot = *slot;
            }

            if *slot < oldest_slot {
                oldest_slot = *slot;
            }
        }
        info!("total_stores: {}, newest_slot: {}, oldest_slot: {}, max_slot: {} (num={}), min_slot: {} (num={})",
              total_count, newest_slot, oldest_slot, max_slot, max, min_slot, min);
        datapoint_info!(
            "accounts_db-stores",
            ("total_count", total_count, i64),
            (
                "recycle_count",
                self.recycle_stores.read().unwrap().entry_count() as u64,
                i64
            ),
        );
        datapoint_info!(
            "accounts_db-perf-stats",
            (
                "delta_hash_num",
                self.stats.delta_hash_num.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "delta_hash_scan_us",
                self.stats
                    .delta_hash_scan_time_total_us
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "delta_hash_accumulate_us",
                self.stats
                    .delta_hash_accumulate_time_total_us
                    .swap(0, Ordering::Relaxed),
                i64
            ),
        );
    }

    pub fn checked_iterative_sum_for_capitalization(total_cap: u64, new_cap: u64) -> u64 {
        let new_total = total_cap as u128 + new_cap as u128;
        AccountsHash::checked_cast_for_capitalization(new_total)
    }

    pub fn checked_sum_for_capitalization<T: Iterator<Item = u64>>(balances: T) -> u64 {
        AccountsHash::checked_cast_for_capitalization(balances.map(|b| b as u128).sum::<u128>())
    }

    fn calculate_accounts_hash(
        &self,
        slot: Slot,
        ancestors: &Ancestors,
        check_hash: bool,
    ) -> Result<(Hash, u64), BankHashVerificationError> {
        use BankHashVerificationError::*;
        let mut collect = Measure::start("collect");
        let keys: Vec<_> = self
            .accounts_index
            .account_maps
            .iter()
            .map(|btree| btree.read().unwrap().keys().cloned().collect::<Vec<_>>())
            .flatten()
            .collect();
        collect.stop();

        let mut scan = Measure::start("scan");
        let mismatch_found = AtomicU64::new(0);
        // Pick a chunk size big enough to allow us to produce output vectors that are smaller than the overall size.
        // We'll also accumulate the lamports within each chunk and fewer chunks results in less contention to accumulate the sum.
        let chunks = crate::accounts_hash::MERKLE_FANOUT.pow(4);
        let total_lamports = Mutex::<u64>::new(0);
        let get_hashes = || {
            keys.par_chunks(chunks)
                .map(|pubkeys| {
                    let mut sum = 0u128;
                    let result: Vec<Hash> = pubkeys
                        .iter()
                        .filter_map(|pubkey| {
                            if let AccountIndexGetResult::Found(lock, index) =
                                self.accounts_index.get(pubkey, Some(ancestors), Some(slot))
                            {
                                let (slot, account_info) = &lock.slot_list()[index];
                                if account_info.lamports != 0 {
                                    // Because we're keeping the `lock' here, there is no need
                                    // to use retry_to_get_account_accessor()
                                    // In other words, flusher/shrinker/cleaner is blocked to
                                    // cause any Accessor(None) situtation.
                                    // Anyway this race condition concern is currently a moot
                                    // point because calculate_accounts_hash() should not
                                    // currently race with clean/shrink because the full hash
                                    // is synchronous with clean/shrink in
                                    // AccountsBackgroundService
                                    self.get_account_accessor(
                                        *slot,
                                        pubkey,
                                        account_info.store_id,
                                        account_info.offset,
                                    )
                                    .get_loaded_account()
                                    .and_then(
                                        |loaded_account| {
                                            let loaded_hash = loaded_account.loaded_hash();
                                            let balance = account_info.lamports;
                                            if check_hash {
                                                let computed_hash =
                                                    loaded_account.compute_hash(*slot, pubkey);
                                                if computed_hash != loaded_hash {
                                                    info!("hash mismatch found: computed: {}, loaded: {}, pubkey: {}", computed_hash, loaded_hash, pubkey);
                                                    mismatch_found
                                                        .fetch_add(1, Ordering::Relaxed);
                                                    return None;
                                                }
                                            }

                                            sum += balance as u128;
                                            Some(loaded_hash)
                                        },
                                    )
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect();
                    let mut total = total_lamports.lock().unwrap();
                    *total =
                        AccountsHash::checked_cast_for_capitalization(*total as u128 + sum);
                    result
                }).collect()
        };

        let hashes: Vec<Vec<Hash>> = if check_hash {
            get_hashes()
        } else {
            self.thread_pool_clean.install(get_hashes)
        };
        if mismatch_found.load(Ordering::Relaxed) > 0 {
            warn!(
                "{} mismatched account hash(es) found",
                mismatch_found.load(Ordering::Relaxed)
            );
            return Err(MismatchedAccountHash);
        }

        scan.stop();
        let total_lamports = *total_lamports.lock().unwrap();

        let mut hash_time = Measure::start("hash");
        let (accumulated_hash, hash_total) = AccountsHash::calculate_hash(hashes);
        hash_time.stop();
        datapoint_info!(
            "update_accounts_hash",
            ("accounts_scan", scan.as_us(), i64),
            ("hash", hash_time.as_us(), i64),
            ("hash_total", hash_total, i64),
            ("collect", collect.as_us(), i64),
        );
        Ok((accumulated_hash, total_lamports))
    }

    pub fn get_accounts_hash(&self, slot: Slot) -> Hash {
        let bank_hashes = self.bank_hashes.read().unwrap();
        let bank_hash_info = bank_hashes.get(&slot).unwrap();
        bank_hash_info.snapshot_hash
    }

    pub fn update_accounts_hash(&self, slot: Slot, ancestors: &Ancestors) -> (Hash, u64) {
        self.update_accounts_hash_with_index_option(true, false, slot, ancestors, None, false)
    }

    pub fn update_accounts_hash_test(&self, slot: Slot, ancestors: &Ancestors) -> (Hash, u64) {
        self.update_accounts_hash_with_index_option(true, true, slot, ancestors, None, false)
    }

    fn scan_multiple_account_storages_one_slot<F, B>(
        storages: &[Arc<AccountStorageEntry>],
        scan_func: &F,
        slot: Slot,
        retval: &mut B,
    ) where
        F: Fn(LoadedAccount, &mut B, Slot) + Send + Sync,
        B: Send + Default,
    {
        // we have to call the scan_func in order of write_version within a slot if there are multiple storages per slot
        let mut len = storages.len();
        let mut progress = Vec::with_capacity(len);
        let mut current = Vec::with_capacity(len);
        for storage in storages {
            let accounts = storage.accounts.accounts(0);
            let mut iterator: std::vec::IntoIter<StoredAccountMeta<'_>> = accounts.into_iter();
            if let Some(item) = iterator
                .next()
                .map(|stored_account| (stored_account.meta.write_version, Some(stored_account)))
            {
                current.push(item);
                progress.push(iterator);
            }
        }
        while !progress.is_empty() {
            let mut min = current[0].0;
            let mut min_index = 0;
            for (i, (item, _)) in current.iter().enumerate().take(len).skip(1) {
                if item < &min {
                    min_index = i;
                    min = *item;
                }
            }
            let mut account = (0, None);
            std::mem::swap(&mut account, &mut current[min_index]);
            scan_func(LoadedAccount::Stored(account.1.unwrap()), retval, slot);
            let next = progress[min_index]
                .next()
                .map(|stored_account| (stored_account.meta.write_version, Some(stored_account)));
            match next {
                Some(item) => {
                    current[min_index] = item;
                }
                None => {
                    current.remove(min_index);
                    progress.remove(min_index);
                    len -= 1;
                }
            }
        }
    }

    /// Scan through all the account storage in parallel
    fn scan_account_storage_no_bank<F, F2, B, C>(
        accounts_cache_and_ancestors: Option<(
            &AccountsCache,
            &Ancestors,
            &AccountInfoAccountsIndex,
        )>,
        snapshot_storages: &SortedStorages,
        scan_func: F,
        after_func: F2,
    ) -> Vec<C>
    where
        F: Fn(LoadedAccount, &mut B, Slot) + Send + Sync,
        F2: Fn(B) -> C + Send + Sync,
        B: Send + Default,
        C: Send + Default,
    {
        // Without chunks, we end up with 1 output vec for each outer snapshot storage.
        // This results in too many vectors to be efficient.
        const MAX_ITEMS_PER_CHUNK: Slot = 5_000;
        let chunks = 1 + (snapshot_storages.range_width() as Slot / MAX_ITEMS_PER_CHUNK);
        (0..chunks)
            .into_par_iter()
            .map(|chunk| {
                let mut retval = B::default();
                let start = snapshot_storages.range().start + chunk * MAX_ITEMS_PER_CHUNK;
                let end = std::cmp::min(start + MAX_ITEMS_PER_CHUNK, snapshot_storages.range().end);
                for slot in start..end {
                    let sub_storages = snapshot_storages.get(slot);
                    let mut valid_slot = false;
                    if let Some(sub_storages) = sub_storages {
                        valid_slot = true;
                        Self::scan_multiple_account_storages_one_slot(
                            sub_storages,
                            &scan_func,
                            slot,
                            &mut retval,
                        );
                    }
                    if let Some((cache, ancestors, accounts_index)) = accounts_cache_and_ancestors {
                        if let Some(slot_cache) = cache.slot_cache(slot) {
                            if valid_slot
                                || ancestors.contains_key(&slot)
                                || accounts_index.is_root(slot)
                            {
                                let keys = slot_cache.get_all_pubkeys();
                                for key in keys {
                                    if let Some(cached_account) = slot_cache.get_cloned(&key) {
                                        let mut accessor = LoadedAccountAccessor::Cached(Some((
                                            key,
                                            Cow::Owned(cached_account),
                                        )));
                                        let account = accessor.get_loaded_account().unwrap();
                                        scan_func(account, &mut retval, slot);
                                    };
                                }
                            }
                        }
                    }
                }
                after_func(retval)
            })
            .collect()
    }

    fn calculate_accounts_hash_helper(
        &self,
        use_index: bool,
        slot: Slot,
        ancestors: &Ancestors,
        check_hash: bool,
        can_cached_slot_be_unflushed: bool,
    ) -> Result<(Hash, u64), BankHashVerificationError> {
        if !use_index {
            let accounts_cache_and_ancestors = if can_cached_slot_be_unflushed {
                Some((&self.accounts_cache, ancestors, &self.accounts_index))
            } else {
                None
            };

            let mut collect_time = Measure::start("collect");
            let (combined_maps, slots) = self.get_snapshot_storages(slot, Some(ancestors));
            collect_time.stop();

            let mut sort_time = Measure::start("sort_storages");
            let min_root = self.accounts_index.min_root();
            let storages = SortedStorages::new_with_slots(
                combined_maps.iter().zip(slots.iter()),
                min_root,
                Some(slot),
            );
            sort_time.stop();

            let timings = HashStats {
                collect_snapshots_us: collect_time.as_us(),
                storage_sort_us: sort_time.as_us(),
                ..HashStats::default()
            };

            Self::calculate_accounts_hash_without_index(
                &storages,
                Some(&self.thread_pool_clean),
                timings,
                check_hash,
                accounts_cache_and_ancestors,
            )
        } else {
            self.calculate_accounts_hash(slot, ancestors, check_hash)
        }
    }

    fn calculate_accounts_hash_helper_with_verify(
        &self,
        use_index: bool,
        debug_verify: bool,
        slot: Slot,
        ancestors: &Ancestors,
        expected_capitalization: Option<u64>,
        can_cached_slot_be_unflushed: bool,
        check_hash: bool,
    ) -> Result<(Hash, u64), BankHashVerificationError> {
        let (hash, total_lamports) = self.calculate_accounts_hash_helper(
            use_index,
            slot,
            ancestors,
            check_hash,
            can_cached_slot_be_unflushed,
        )?;
        if debug_verify {
            // calculate the other way (store or non-store) and verify results match.
            let (hash_other, total_lamports_other) = self.calculate_accounts_hash_helper(
                !use_index,
                slot,
                ancestors,
                check_hash,
                can_cached_slot_be_unflushed,
            )?;

            let success = hash == hash_other
                && total_lamports == total_lamports_other
                && total_lamports == expected_capitalization.unwrap_or(total_lamports);
            assert!(success, "update_accounts_hash_with_index_option mismatch. hashes: {}, {}; lamports: {}, {}; expected lamports: {:?}, using index: {}, slot: {}", hash, hash_other, total_lamports, total_lamports_other, expected_capitalization, use_index, slot);
        }
        Ok((hash, total_lamports))
    }

    pub fn update_accounts_hash_with_index_option(
        &self,
        use_index: bool,
        debug_verify: bool,
        slot: Slot,
        ancestors: &Ancestors,
        expected_capitalization: Option<u64>,
        can_cached_slot_be_unflushed: bool,
    ) -> (Hash, u64) {
        let check_hash = false;
        let (hash, total_lamports) = self
            .calculate_accounts_hash_helper_with_verify(
                use_index,
                debug_verify,
                slot,
                ancestors,
                expected_capitalization,
                can_cached_slot_be_unflushed,
                check_hash,
            )
            .unwrap(); // unwrap here will never fail since check_hash = false
        let mut bank_hashes = self.bank_hashes.write().unwrap();
        let mut bank_hash_info = bank_hashes.get_mut(&slot).unwrap();
        bank_hash_info.snapshot_hash = hash;
        (hash, total_lamports)
    }

    fn scan_snapshot_stores_with_cache(
        storage: &SortedStorages,
        mut stats: &mut crate::accounts_hash::HashStats,
        bins: usize,
        bin_range: &Range<usize>,
        check_hash: bool,
        accounts_cache_and_ancestors: Option<(
            &AccountsCache,
            &Ancestors,
            &AccountInfoAccountsIndex,
        )>,
    ) -> Result<Vec<Vec<Vec<CalculateHashIntermediate>>>, BankHashVerificationError> {
        let bin_calculator = PubkeyBinCalculator16::new(bins);
        assert!(bin_range.start < bins && bin_range.end <= bins && bin_range.start < bin_range.end);
        let mut time = Measure::start("scan all accounts");
        stats.num_snapshot_storage = storage.slot_count();
        let mismatch_found = AtomicU64::new(0);
        let range = bin_range.end - bin_range.start;
        let sort_time = AtomicU64::new(0);

        let result: Vec<Vec<Vec<CalculateHashIntermediate>>> = Self::scan_account_storage_no_bank(
            accounts_cache_and_ancestors,
            storage,
            |loaded_account: LoadedAccount,
             accum: &mut Vec<Vec<CalculateHashIntermediate>>,
             slot: Slot| {
                let pubkey = loaded_account.pubkey();
                let mut pubkey_to_bin_index = bin_calculator.bin_from_pubkey(pubkey);
                if !bin_range.contains(&pubkey_to_bin_index) {
                    return;
                }

                // when we are scanning with bin ranges, we don't need to use exact bin numbers. Subtract to make first bin we care about at index 0.
                pubkey_to_bin_index -= bin_range.start;

                let raw_lamports = loaded_account.lamports();
                let zero_raw_lamports = raw_lamports == 0;
                let balance = if zero_raw_lamports {
                    crate::accounts_hash::ZERO_RAW_LAMPORTS_SENTINEL
                } else {
                    raw_lamports
                };

                let source_item = CalculateHashIntermediate::new_without_slot(
                    loaded_account.loaded_hash(),
                    balance,
                    *pubkey,
                );

                if check_hash {
                    let computed_hash = loaded_account.compute_hash(slot, pubkey);
                    if computed_hash != source_item.hash {
                        info!(
                            "hash mismatch found: computed: {}, loaded: {}, pubkey: {}",
                            computed_hash, source_item.hash, pubkey
                        );
                        mismatch_found.fetch_add(1, Ordering::Relaxed);
                    }
                }

                let max = accum.len();
                if max == 0 {
                    accum.extend(vec![Vec::new(); range]);
                }
                accum[pubkey_to_bin_index].push(source_item);
            },
            |x| {
                let (result, timing) = Self::sort_slot_storage_scan(x);
                sort_time.fetch_add(timing, Ordering::Relaxed);
                result
            },
        );

        stats.sort_time_total_us += sort_time.load(Ordering::Relaxed);

        if check_hash && mismatch_found.load(Ordering::Relaxed) > 0 {
            warn!(
                "{} mismatched account hash(es) found",
                mismatch_found.load(Ordering::Relaxed)
            );
            return Err(BankHashVerificationError::MismatchedAccountHash);
        }

        time.stop();
        stats.scan_time_total_us += time.as_us();

        Ok(result)
    }

    fn sort_slot_storage_scan(
        accum: Vec<Vec<CalculateHashIntermediate>>,
    ) -> (Vec<Vec<CalculateHashIntermediate>>, u64) {
        let time = AtomicU64::new(0);
        (
            accum
                .into_par_iter()
                .map(|mut items| {
                    let mut sort_time = Measure::start("sort");
                    {
                        // sort_by vs unstable because slot and write_version are already in order
                        items.sort_by(AccountsHash::compare_two_hash_entries);
                    }
                    sort_time.stop();
                    time.fetch_add(sort_time.as_us(), Ordering::Relaxed);
                    items
                })
                .collect(),
            time.load(Ordering::Relaxed),
        )
    }

    // modeled after get_accounts_delta_hash
    // intended to be faster than calculate_accounts_hash
    pub fn calculate_accounts_hash_without_index(
        storages: &SortedStorages,
        thread_pool: Option<&ThreadPool>,
        mut stats: HashStats,
        check_hash: bool,
        accounts_cache_and_ancestors: Option<(
            &AccountsCache,
            &Ancestors,
            &AccountInfoAccountsIndex,
        )>,
    ) -> Result<(Hash, u64), BankHashVerificationError> {
        let mut scan_and_hash = move || {
            // When calculating hashes, it is helpful to break the pubkeys found into bins based on the pubkey value.
            // More bins means smaller vectors to sort, copy, etc.
            const PUBKEY_BINS_FOR_CALCULATING_HASHES: usize = 65536;

            // # of passes should be a function of the total # of accounts that are active.
            // higher passes = slower total time, lower dynamic memory usage
            // lower passes = faster total time, higher dynamic memory usage
            // passes=2 cuts dynamic memory usage in approximately half.
            let num_scan_passes: usize = 2;

            let bins_per_pass = PUBKEY_BINS_FOR_CALCULATING_HASHES / num_scan_passes;
            assert_eq!(
                bins_per_pass * num_scan_passes,
                PUBKEY_BINS_FOR_CALCULATING_HASHES
            ); // evenly divisible
            let mut previous_pass = PreviousPass::default();
            let mut final_result = (Hash::default(), 0);

            for pass in 0..num_scan_passes {
                let bounds = Range {
                    start: pass * bins_per_pass,
                    end: (pass + 1) * bins_per_pass,
                };

                let result = Self::scan_snapshot_stores_with_cache(
                    storages,
                    &mut stats,
                    PUBKEY_BINS_FOR_CALCULATING_HASHES,
                    &bounds,
                    check_hash,
                    accounts_cache_and_ancestors,
                )?;

                let (hash, lamports, for_next_pass) = AccountsHash::rest_of_hash_calculation(
                    result,
                    &mut stats,
                    pass == num_scan_passes - 1,
                    previous_pass,
                    bins_per_pass,
                );
                previous_pass = for_next_pass;
                final_result = (hash, lamports);
            }
            Ok(final_result)
        };
        if let Some(thread_pool) = thread_pool {
            thread_pool.install(scan_and_hash)
        } else {
            scan_and_hash()
        }
    }

    pub fn verify_bank_hash_and_lamports(
        &self,
        slot: Slot,
        ancestors: &Ancestors,
        total_lamports: u64,
        test_hash_calculation: bool,
    ) -> Result<(), BankHashVerificationError> {
        use BankHashVerificationError::*;

        let use_index = false;
        let check_hash = true;
        let can_cached_slot_be_unflushed = false;
        let (calculated_hash, calculated_lamports) = self
            .calculate_accounts_hash_helper_with_verify(
                use_index,
                test_hash_calculation,
                slot,
                ancestors,
                None,
                can_cached_slot_be_unflushed,
                check_hash,
            )?;

        if calculated_lamports != total_lamports {
            warn!(
                "Mismatched total lamports: {} calculated: {}",
                total_lamports, calculated_lamports
            );
            return Err(MismatchedTotalLamports(calculated_lamports, total_lamports));
        }

        let bank_hashes = self.bank_hashes.read().unwrap();
        if let Some(found_hash_info) = bank_hashes.get(&slot) {
            if calculated_hash == found_hash_info.snapshot_hash {
                Ok(())
            } else {
                warn!(
                    "mismatched bank hash for slot {}: {} (calculated) != {} (expected)",
                    slot, calculated_hash, found_hash_info.snapshot_hash
                );
                Err(MismatchedBankHash)
            }
        } else {
            Err(MissingBankHash)
        }
    }

    /// Perform the scan for pubkeys that were written to in a slot
    fn do_scan_slot_for_dirty_pubkeys(
        &self,
        slot: Slot,
    ) -> ScanStorageResult<Pubkey, DashSet<Pubkey>> {
        self.scan_account_storage(
            slot,
            |loaded_account: LoadedAccount| Some(*loaded_account.pubkey()),
            |accum: &DashSet<Pubkey>, loaded_account: LoadedAccount| {
                accum.insert(*loaded_account.pubkey());
            },
        )
    }

    /// Reduce the scan result of dirty pubkeys after calling `scan_account_storage()` into a
    /// single vec of Pubkeys.
    fn do_reduce_scan_slot_for_dirty_pubkeys(
        scan_result: ScanStorageResult<Pubkey, DashSet<Pubkey>>,
    ) -> Vec<Pubkey> {
        match scan_result {
            ScanStorageResult::Cached(cached_result) => cached_result,
            ScanStorageResult::Stored(stored_result) => {
                stored_result.into_iter().collect::<Vec<_>>()
            }
        }
    }

    /// Scan a slot for dirty pubkeys
    fn scan_slot_for_dirty_pubkeys(&self, slot: Slot) -> Vec<Pubkey> {
        let dirty_pubkeys = self.do_scan_slot_for_dirty_pubkeys(slot);
        Self::do_reduce_scan_slot_for_dirty_pubkeys(dirty_pubkeys)
    }

    /// Scan a slot in the account storage for dirty pubkeys and insert them into the list of
    /// uncleaned pubkeys
    ///
    /// This function is called in Bank::drop() when the bank is _not_ frozen, so that its pubkeys
    /// are considered for cleanup.
    pub fn scan_slot_and_insert_dirty_pubkeys_into_uncleaned_pubkeys(&self, slot: Slot) {
        let dirty_pubkeys = self.scan_slot_for_dirty_pubkeys(slot);
        self.uncleaned_pubkeys.insert(slot, dirty_pubkeys);
    }

    pub fn get_accounts_delta_hash(&self, slot: Slot) -> Hash {
        let mut scan = Measure::start("scan");

        let scan_result: ScanStorageResult<(Pubkey, Hash), DashMapVersionHash> = self
            .scan_account_storage(
                slot,
                |loaded_account: LoadedAccount| {
                    // Cache only has one version per key, don't need to worry about versioning
                    Some((*loaded_account.pubkey(), loaded_account.loaded_hash()))
                },
                |accum: &DashMap<Pubkey, (u64, Hash)>, loaded_account: LoadedAccount| {
                    let loaded_write_version = loaded_account.write_version();
                    let loaded_hash = loaded_account.loaded_hash();
                    let should_insert =
                        if let Some(existing_entry) = accum.get(loaded_account.pubkey()) {
                            loaded_write_version > existing_entry.value().version()
                        } else {
                            true
                        };
                    if should_insert {
                        // Detected insertion is necessary, grabs the write lock to commit the write,
                        match accum.entry(*loaded_account.pubkey()) {
                            // Double check in case another thread interleaved a write between the read + write.
                            Occupied(mut occupied_entry) => {
                                if loaded_write_version > occupied_entry.get().version() {
                                    occupied_entry.insert((loaded_write_version, loaded_hash));
                                }
                            }

                            Vacant(vacant_entry) => {
                                vacant_entry.insert((loaded_write_version, loaded_hash));
                            }
                        }
                    }
                },
            );
        scan.stop();

        let mut accumulate = Measure::start("accumulate");
        let hashes: Vec<_> = match scan_result {
            ScanStorageResult::Cached(cached_result) => cached_result,
            ScanStorageResult::Stored(stored_result) => stored_result
                .into_iter()
                .map(|(pubkey, (_latest_write_version, hash))| (pubkey, hash))
                .collect(),
        };
        let dirty_keys = hashes.iter().map(|(pubkey, _hash)| *pubkey).collect();

        let ret = AccountsHash::accumulate_account_hashes(hashes);
        accumulate.stop();
        let mut uncleaned_time = Measure::start("uncleaned_index");
        self.uncleaned_pubkeys.insert(slot, dirty_keys);
        uncleaned_time.stop();
        self.stats
            .store_uncleaned_update
            .fetch_add(uncleaned_time.as_us(), Ordering::Relaxed);

        self.stats
            .delta_hash_scan_time_total_us
            .fetch_add(scan.as_us(), Ordering::Relaxed);
        self.stats
            .delta_hash_accumulate_time_total_us
            .fetch_add(accumulate.as_us(), Ordering::Relaxed);
        self.stats.delta_hash_num.fetch_add(1, Ordering::Relaxed);
        ret
    }

    fn update_index(
        &self,
        slot: Slot,
        infos: Vec<AccountInfo>,
        accounts: &[(&Pubkey, &impl ReadableAccount)],
    ) -> SlotList<AccountInfo> {
        let mut reclaims = SlotList::<AccountInfo>::with_capacity(infos.len() * 2);
        for (info, pubkey_account) in infos.into_iter().zip(accounts.iter()) {
            let pubkey = pubkey_account.0;
            self.accounts_index.upsert(
                slot,
                pubkey,
                pubkey_account.1.owner(),
                pubkey_account.1.data(),
                &self.account_indexes,
                info,
                &mut reclaims,
            );
        }
        reclaims
    }

    fn should_not_shrink(aligned_bytes: u64, total_bytes: u64, num_stores: usize) -> bool {
        aligned_bytes + PAGE_SIZE > total_bytes && num_stores == 1
    }

    fn is_shrinking_productive(slot: Slot, stores: &[Arc<AccountStorageEntry>]) -> bool {
        let mut alive_count = 0;
        let mut stored_count = 0;
        let mut alive_bytes = 0;
        let mut total_bytes = 0;

        for store in stores {
            alive_count += store.count();
            stored_count += store.approx_stored_count();
            alive_bytes += store.alive_bytes();
            total_bytes += store.total_bytes();
        }

        let aligned_bytes = Self::page_align(alive_bytes as u64);
        if Self::should_not_shrink(aligned_bytes, total_bytes, stores.len()) {
            trace!(
                "shrink_slot_forced ({}, {}): not able to shrink at all: alive/stored: ({} / {}) ({}b / {}b) save: {}",
                slot,
                stores.len(),
                alive_count,
                stored_count,
                aligned_bytes,
                total_bytes,
                total_bytes.saturating_sub(aligned_bytes),
            );
            return false;
        }

        true
    }

    fn is_candidate_for_shrink(&self, store: &Arc<AccountStorageEntry>) -> bool {
        match self.shrink_ratio {
            AccountShrinkThreshold::TotalSpace { shrink_ratio: _ } => {
                Self::page_align(store.alive_bytes() as u64) < store.total_bytes()
            }
            AccountShrinkThreshold::IndividalStore { shrink_ratio } => {
                (Self::page_align(store.alive_bytes() as u64) as f64 / store.total_bytes() as f64)
                    < shrink_ratio
            }
        }
    }

    fn remove_dead_accounts(
        &self,
        reclaims: SlotSlice<AccountInfo>,
        expected_slot: Option<Slot>,
        mut reclaimed_offsets: Option<&mut AppendVecOffsets>,
        reset_accounts: bool,
    ) -> HashSet<Slot> {
        let mut dead_slots = HashSet::new();
        let mut new_shrink_candidates: ShrinkCandidates = HashMap::new();
        for (slot, account_info) in reclaims {
            // No cached accounts should make it here
            assert_ne!(account_info.store_id, CACHE_VIRTUAL_STORAGE_ID);
            if let Some(ref mut reclaimed_offsets) = reclaimed_offsets {
                reclaimed_offsets
                    .entry(account_info.store_id)
                    .or_default()
                    .insert(account_info.offset);
            }
            if let Some(expected_slot) = expected_slot {
                assert_eq!(*slot, expected_slot);
            }
            if let Some(store) = self
                .storage
                .get_account_storage_entry(*slot, account_info.store_id)
            {
                assert_eq!(
                    *slot, store.slot(),
                    "AccountDB::accounts_index corrupted. Storage pointed to: {}, expected: {}, should only point to one slot",
                    store.slot(), *slot
                );
                let count = store.remove_account(account_info.stored_size, reset_accounts);
                if count == 0 {
                    self.dirty_stores
                        .insert((*slot, store.append_vec_id()), store.clone());
                    dead_slots.insert(*slot);
                } else if self.caching_enabled
                    && Self::is_shrinking_productive(*slot, &[store.clone()])
                    && self.is_candidate_for_shrink(&store)
                {
                    // Checking that this single storage entry is ready for shrinking,
                    // should be a sufficient indication that the slot is ready to be shrunk
                    // because slots should only have one storage entry, namely the one that was
                    // created by `flush_slot_cache()`.
                    {
                        new_shrink_candidates
                            .entry(*slot)
                            .or_default()
                            .insert(store.append_vec_id(), store);
                    }
                }
            }
        }

        if self.caching_enabled {
            {
                let mut shrink_candidate_slots = self.shrink_candidate_slots.lock().unwrap();
                for (slot, slot_shrink_candidates) in new_shrink_candidates {
                    for (store_id, store) in slot_shrink_candidates {
                        // count could be == 0 if multiple accounts are removed
                        // at once
                        if store.count() != 0 {
                            debug!(
                                "adding: {} {} to shrink candidates: count: {}/{} bytes: {}/{}",
                                store_id,
                                slot,
                                store.approx_stored_count(),
                                store.count(),
                                store.alive_bytes(),
                                store.total_bytes()
                            );

                            shrink_candidate_slots
                                .entry(slot)
                                .or_default()
                                .insert(store_id, store);
                        }
                    }
                }
            }
        }

        dead_slots.retain(|slot| {
            if let Some(slot_stores) = self.storage.get_slot_stores(*slot) {
                for x in slot_stores.read().unwrap().values() {
                    if x.count() != 0 {
                        return false;
                    }
                }
            }
            true
        });

        dead_slots
    }

    fn remove_dead_slots_metadata<'a>(
        &'a self,
        dead_slots_iter: impl Iterator<Item = &'a Slot> + Clone,
        purged_slot_pubkeys: HashSet<(Slot, Pubkey)>,
        // Should only be `Some` for non-cached slots
        purged_stored_account_slots: Option<&mut AccountSlots>,
    ) {
        self.clean_dead_slots_from_accounts_index(
            dead_slots_iter.clone(),
            purged_slot_pubkeys,
            purged_stored_account_slots,
        );
        {
            let mut bank_hashes = self.bank_hashes.write().unwrap();
            for slot in dead_slots_iter {
                bank_hashes.remove(slot);
            }
        }
    }

    fn clean_dead_slots_from_accounts_index<'a>(
        &'a self,
        dead_slots_iter: impl Iterator<Item = &'a Slot> + Clone,
        purged_slot_pubkeys: HashSet<(Slot, Pubkey)>,
        // Should only be `Some` for non-cached slots
        purged_stored_account_slots: Option<&mut AccountSlots>,
    ) {
        if let Some(purged_stored_account_slots) = purged_stored_account_slots {
            for (slot, pubkey) in purged_slot_pubkeys {
                purged_stored_account_slots
                    .entry(pubkey)
                    .or_default()
                    .insert(slot);
                self.accounts_index.unref_from_storage(&pubkey);
            }
        }

        let mut accounts_index_root_stats = AccountsIndexRootsStats::default();
        let mut rooted_cleaned_count = 0;
        let mut unrooted_cleaned_count = 0;
        let dead_slots: Vec<_> = dead_slots_iter
            .map(|slot| {
                if let Some(latest) = self.accounts_index.clean_dead_slot(*slot) {
                    rooted_cleaned_count += 1;
                    accounts_index_root_stats = latest;
                } else {
                    unrooted_cleaned_count += 1;
                }
                *slot
            })
            .collect();
        info!("remove_dead_slots_metadata: slots {:?}", dead_slots);

        accounts_index_root_stats.rooted_cleaned_count += rooted_cleaned_count;
        accounts_index_root_stats.unrooted_cleaned_count += unrooted_cleaned_count;

        self.clean_accounts_stats
            .latest_accounts_index_roots_stats
            .update(&accounts_index_root_stats);
    }

    fn clean_stored_dead_slots(
        &self,
        dead_slots: &HashSet<Slot>,
        purged_account_slots: Option<&mut AccountSlots>,
    ) {
        let mut measure = Measure::start("clean_stored_dead_slots-ms");
        let mut stores: Vec<Arc<AccountStorageEntry>> = vec![];
        for slot in dead_slots.iter() {
            if let Some(slot_storage) = self.storage.get_slot_stores(*slot) {
                for store in slot_storage.read().unwrap().values() {
                    stores.push(store.clone());
                }
            }
        }
        let purged_slot_pubkeys: HashSet<(Slot, Pubkey)> = {
            self.thread_pool_clean.install(|| {
                stores
                    .into_par_iter()
                    .map(|store| {
                        let accounts = store.all_accounts();
                        accounts
                            .into_iter()
                            .map(|account| (store.slot(), account.meta.pubkey))
                            .collect::<HashSet<(Slot, Pubkey)>>()
                    })
                    .reduce(HashSet::new, |mut reduced, store_pubkeys| {
                        reduced.extend(store_pubkeys);
                        reduced
                    })
            })
        };
        self.remove_dead_slots_metadata(
            dead_slots.iter(),
            purged_slot_pubkeys,
            purged_account_slots,
        );
        measure.stop();
        inc_new_counter_info!("clean_stored_dead_slots-ms", measure.as_ms() as usize);
    }

    pub(crate) fn freeze_accounts(&mut self, ancestors: &Ancestors, account_pubkeys: &[Pubkey]) {
        for account_pubkey in account_pubkeys {
            if let Some((account, _slot)) = self.load_without_fixed_root(ancestors, account_pubkey)
            {
                let frozen_account_info = FrozenAccountInfo {
                    hash: Self::hash_frozen_account_data(&account),
                    lamports: account.lamports(),
                };
                warn!(
                    "Account {} is now frozen at lamports={}, hash={}",
                    account_pubkey, frozen_account_info.lamports, frozen_account_info.hash
                );
                self.frozen_accounts
                    .insert(*account_pubkey, frozen_account_info);
            } else {
                panic!(
                    "Unable to freeze an account that does not exist: {}",
                    account_pubkey
                );
            }
        }
    }

    /// Cause a panic if frozen accounts would be affected by data in `accounts`
    fn assert_frozen_accounts(&self, accounts: &[(&Pubkey, &AccountSharedData)]) {
        if self.frozen_accounts.is_empty() {
            return;
        }
        for (account_pubkey, account) in accounts.iter() {
            if let Some(frozen_account_info) = self.frozen_accounts.get(*account_pubkey) {
                if account.lamports() < frozen_account_info.lamports {
                    FROZEN_ACCOUNT_PANIC.store(true, Ordering::Relaxed);
                    panic!(
                        "Frozen account {} modified.  Lamports decreased from {} to {}",
                        account_pubkey,
                        frozen_account_info.lamports,
                        account.lamports(),
                    )
                }

                let hash = Self::hash_frozen_account_data(account);
                if hash != frozen_account_info.hash {
                    FROZEN_ACCOUNT_PANIC.store(true, Ordering::Relaxed);
                    panic!(
                        "Frozen account {} modified.  Hash changed from {} to {}",
                        account_pubkey, frozen_account_info.hash, hash,
                    )
                }
            }
        }
    }

    pub fn store_cached(&self, slot: Slot, accounts: &[(&Pubkey, &AccountSharedData)]) {
        self.store(slot, accounts, self.caching_enabled);
    }

    /// Store the account update.
    pub fn store_uncached(&self, slot: Slot, accounts: &[(&Pubkey, &AccountSharedData)]) {
        self.store(slot, accounts, false);
    }

    fn store(&self, slot: Slot, accounts: &[(&Pubkey, &AccountSharedData)], is_cached_store: bool) {
        // If all transactions in a batch are errored,
        // it's possible to get a store with no accounts.
        if accounts.is_empty() {
            return;
        }
        self.assert_frozen_accounts(accounts);

        let mut stats = BankHashStats::default();
        let mut total_data = 0;
        accounts.iter().for_each(|(_pubkey, account)| {
            total_data += account.data().len();
            stats.update(*account);
        });

        self.stats
            .store_total_data
            .fetch_add(total_data as u64, Ordering::Relaxed);

        let mut bank_hashes = self.bank_hashes.write().unwrap();
        let slot_info = bank_hashes
            .entry(slot)
            .or_insert_with(BankHashInfo::default);
        slot_info.stats.merge(&stats);

        // we use default hashes for now since the same account may be stored to the cache multiple times
        self.store_accounts_unfrozen(slot, accounts, None, is_cached_store);
        self.report_store_timings();
    }

    fn report_store_timings(&self) {
        let last = self.stats.last_store_report.load(Ordering::Relaxed);
        let now = solana_sdk::timing::timestamp();

        if now.saturating_sub(last) > 1000
            && self.stats.last_store_report.compare_exchange(
                last,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) == Ok(last)
        {
            let (read_only_cache_hits, read_only_cache_misses) =
                self.read_only_accounts_cache.get_and_reset_stats();
            datapoint_info!(
                "accounts_db_store_timings",
                (
                    "hash_accounts",
                    self.stats.store_hash_accounts.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "store_accounts",
                    self.stats.store_accounts.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "update_index",
                    self.stats.store_update_index.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "handle_reclaims",
                    self.stats.store_handle_reclaims.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "append_accounts",
                    self.stats.store_append_accounts.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "find_storage",
                    self.stats.store_find_store.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "num_accounts",
                    self.stats.store_num_accounts.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "total_data",
                    self.stats.store_total_data.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "read_only_accounts_cache_entries",
                    self.read_only_accounts_cache.cache_len(),
                    i64
                ),
                (
                    "read_only_accounts_cache_data_size",
                    self.read_only_accounts_cache.data_size(),
                    i64
                ),
                ("read_only_accounts_cache_hits", read_only_cache_hits, i64),
                (
                    "read_only_accounts_cache_misses",
                    read_only_cache_misses,
                    i64
                ),
                (
                    "calc_stored_meta_us",
                    self.stats.calc_stored_meta.swap(0, Ordering::Relaxed),
                    i64
                ),
            );

            let recycle_stores = self.recycle_stores.read().unwrap();
            datapoint_info!(
                "accounts_db_store_timings2",
                (
                    "recycle_store_count",
                    self.stats.recycle_store_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "current_recycle_store_count",
                    recycle_stores.entry_count(),
                    i64
                ),
                (
                    "current_recycle_store_bytes",
                    recycle_stores.total_bytes(),
                    i64
                ),
                (
                    "create_store_count",
                    self.stats.create_store_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "store_get_slot_store",
                    self.stats.store_get_slot_store.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "store_find_existing",
                    self.stats.store_find_existing.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "dropped_stores",
                    self.stats.dropped_stores.swap(0, Ordering::Relaxed),
                    i64
                ),
            );
        }
    }

    fn store_accounts_unfrozen(
        &self,
        slot: Slot,
        accounts: &[(&Pubkey, &AccountSharedData)],
        hashes: Option<&[&Hash]>,
        is_cached_store: bool,
    ) {
        // This path comes from a store to a non-frozen slot.
        // If a store is dead here, then a newer update for
        // each pubkey in the store must exist in another
        // store in the slot. Thus it is safe to reset the store and
        // re-use it for a future store op. The pubkey ref counts should still
        // hold just 1 ref from this slot.
        let reset_accounts = true;

        self.store_accounts_custom(
            slot,
            accounts,
            hashes,
            None::<StorageFinder>,
            None::<Box<dyn Iterator<Item = u64>>>,
            is_cached_store,
            reset_accounts,
        );
    }

    fn store_accounts_frozen<'a>(
        &'a self,
        slot: Slot,
        accounts: &[(&Pubkey, &impl ReadableAccount)],
        hashes: Option<&[impl Borrow<Hash>]>,
        storage_finder: Option<StorageFinder<'a>>,
        write_version_producer: Option<Box<dyn Iterator<Item = StoredMetaWriteVersion>>>,
    ) -> StoreAccountsTiming {
        // stores on a frozen slot should not reset
        // the append vec so that hashing could happen on the store
        // and accounts in the append_vec can be unrefed correctly
        let reset_accounts = false;
        let is_cached_store = false;
        self.store_accounts_custom(
            slot,
            accounts,
            hashes,
            storage_finder,
            write_version_producer,
            is_cached_store,
            reset_accounts,
        )
    }

    fn store_accounts_custom<'a>(
        &'a self,
        slot: Slot,
        accounts: &[(&Pubkey, &impl ReadableAccount)],
        hashes: Option<&[impl Borrow<Hash>]>,
        storage_finder: Option<StorageFinder<'a>>,
        write_version_producer: Option<Box<dyn Iterator<Item = u64>>>,
        is_cached_store: bool,
        reset_accounts: bool,
    ) -> StoreAccountsTiming {
        let storage_finder: StorageFinder<'a> = storage_finder
            .unwrap_or_else(|| Box::new(move |slot, size| self.find_storage_candidate(slot, size)));

        let write_version_producer: Box<dyn Iterator<Item = u64>> = write_version_producer
            .unwrap_or_else(|| {
                let mut current_version = self.bulk_assign_write_version(accounts.len());
                Box::new(std::iter::from_fn(move || {
                    let ret = current_version;
                    current_version += 1;
                    Some(ret)
                }))
            });

        self.stats
            .store_num_accounts
            .fetch_add(accounts.len() as u64, Ordering::Relaxed);
        let mut store_accounts_time = Measure::start("store_accounts");
        let infos = self.store_accounts_to(
            slot,
            accounts,
            hashes,
            storage_finder,
            write_version_producer,
            is_cached_store,
        );
        store_accounts_time.stop();
        self.stats
            .store_accounts
            .fetch_add(store_accounts_time.as_us(), Ordering::Relaxed);
        let mut update_index_time = Measure::start("update_index");

        // If the cache was flushed, then because `update_index` occurs
        // after the account are stored by the above `store_accounts_to`
        // call and all the accounts are stored, all reads after this point
        // will know to not check the cache anymore
        let mut reclaims = self.update_index(slot, infos, accounts);

        // For each updated account, `reclaims` should only have at most one
        // item (if the account was previously updated in this slot).
        // filter out the cached reclaims as those don't actually map
        // to anything that needs to be cleaned in the backing storage
        // entries
        if self.caching_enabled {
            reclaims.retain(|(_, r)| r.store_id != CACHE_VIRTUAL_STORAGE_ID);

            if is_cached_store {
                assert!(reclaims.is_empty());
            }
        }

        update_index_time.stop();
        self.stats
            .store_update_index
            .fetch_add(update_index_time.as_us(), Ordering::Relaxed);

        // A store for a single slot should:
        // 1) Only make "reclaims" for the same slot
        // 2) Should not cause any slots to be removed from the storage
        // database because
        //    a) this slot  has at least one account (the one being stored),
        //    b)From 1) we know no other slots are included in the "reclaims"
        //
        // From 1) and 2) we guarantee passing `no_purge_stats` == None, which is
        // equivalent to asserting there will be no dead slots, is safe.
        let no_purge_stats = None;
        let mut handle_reclaims_time = Measure::start("handle_reclaims");
        self.handle_reclaims(&reclaims, Some(slot), no_purge_stats, None, reset_accounts);
        handle_reclaims_time.stop();
        self.stats
            .store_handle_reclaims
            .fetch_add(handle_reclaims_time.as_us(), Ordering::Relaxed);

        StoreAccountsTiming {
            store_accounts_elapsed: store_accounts_time.as_us(),
            update_index_elapsed: update_index_time.as_us(),
            handle_reclaims_elapsed: handle_reclaims_time.as_us(),
        }
    }

    pub fn add_root(&self, slot: Slot) {
        self.accounts_index.add_root(slot, self.caching_enabled);
        if self.caching_enabled {
            self.accounts_cache.add_root(slot);
        }
        if let Some(slot_stores) = self.storage.get_slot_stores(slot) {
            for (store_id, store) in slot_stores.read().unwrap().iter() {
                self.dirty_stores.insert((slot, *store_id), store.clone());
            }
        }
    }

    pub fn get_snapshot_storages(
        &self,
        snapshot_slot: Slot,
        ancestors: Option<&Ancestors>,
    ) -> (SnapshotStorages, Vec<Slot>) {
        let mut m = Measure::start("get slots");
        let slots = self
            .storage
            .0
            .iter()
            .map(|k| *k.key() as Slot)
            .collect::<Vec<_>>();
        m.stop();
        let mut m2 = Measure::start("filter");

        let chunk_size = 5_000;
        let wide = self.thread_pool_clean.install(|| {
            slots
                .par_chunks(chunk_size)
                .map(|slots| {
                    slots
                        .iter()
                        .filter_map(|slot| {
                            if *slot <= snapshot_slot
                                && (self.accounts_index.is_root(*slot)
                                    || ancestors
                                        .map(|ancestors| ancestors.contains_key(slot))
                                        .unwrap_or_default())
                            {
                                self.storage.0.get(slot).map_or_else(
                                    || None,
                                    |item| {
                                        let storages = item
                                            .value()
                                            .read()
                                            .unwrap()
                                            .values()
                                            .filter(|x| x.has_accounts())
                                            .cloned()
                                            .collect::<Vec<_>>();
                                        if !storages.is_empty() {
                                            Some((storages, *slot))
                                        } else {
                                            None
                                        }
                                    },
                                )
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<(SnapshotStorage, Slot)>>()
                })
                .collect::<Vec<_>>()
        });
        m2.stop();
        let mut m3 = Measure::start("flatten");
        // some slots we found above may not have been a root or met the slot # constraint.
        // So the resulting 'slots' vector we return will be a subset of the raw keys we got initially.
        let mut slots = Vec::with_capacity(slots.len());
        let result = wide
            .into_iter()
            .flatten()
            .map(|(storage, slot)| {
                slots.push(slot);
                storage
            })
            .collect::<Vec<_>>();
        m3.stop();

        debug!(
            "hash_total: get slots: {}, filter: {}, flatten: {}",
            m.as_us(),
            m2.as_us(),
            m3.as_us()
        );
        (result, slots)
    }

    fn process_storage_slot(
        storage_maps: &[Arc<AccountStorageEntry>],
    ) -> GenerateIndexAccountsMap<'_> {
        let num_accounts = storage_maps
            .iter()
            .map(|storage| storage.approx_stored_count())
            .sum();
        let mut accounts_map = GenerateIndexAccountsMap::with_capacity(num_accounts);
        storage_maps.iter().for_each(|storage| {
            let accounts = storage.all_accounts();
            accounts.into_iter().for_each(|stored_account| {
                let this_version = stored_account.meta.write_version;
                match accounts_map.entry(stored_account.meta.pubkey) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert((this_version, storage.append_vec_id(), stored_account));
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        let occupied_version = entry.get().0;
                        if occupied_version < this_version {
                            entry.insert((this_version, storage.append_vec_id(), stored_account));
                        } else {
                            assert!(occupied_version != this_version);
                        }
                    }
                }
            })
        });
        accounts_map
    }

    fn generate_index_for_slot<'a>(
        &self,
        accounts_map: GenerateIndexAccountsMap<'a>,
        slot: &Slot,
    ) -> u64 {
        if accounts_map.is_empty() {
            return 0;
        }

        let secondary = !self.account_indexes.is_empty();

        let len = accounts_map.len();
        let items = accounts_map
            .into_iter()
            .map(|(pubkey, (_, store_id, stored_account))| {
                if secondary {
                    self.accounts_index.update_secondary_indexes(
                        &pubkey,
                        &stored_account.account_meta.owner,
                        stored_account.data,
                        &self.account_indexes,
                    );
                }

                (
                    pubkey,
                    AccountInfo {
                        store_id,
                        offset: stored_account.offset,
                        stored_size: stored_account.stored_size,
                        lamports: stored_account.account_meta.lamports,
                    },
                )
            });

        let (dirty_pubkeys, insert_us) = self
            .accounts_index
            .insert_new_if_missing_into_primary_index(*slot, len, items);

        // dirty_pubkeys will contain a pubkey if an item has multiple rooted entries for
        // a given pubkey. If there is just a single item, there is no cleaning to
        // be done on that pubkey. Use only those pubkeys with multiple updates.
        if !dirty_pubkeys.is_empty() {
            self.uncleaned_pubkeys.insert(*slot, dirty_pubkeys);
        }
        insert_us
    }

    #[allow(clippy::needless_collect)]
    pub fn generate_index(&self, limit_load_slot_count_from_snapshot: Option<usize>, verify: bool) {
        let mut slots = self.storage.all_slots();
        #[allow(clippy::stable_sort_primitive)]
        slots.sort();
        if let Some(limit) = limit_load_slot_count_from_snapshot {
            slots.truncate(limit); // get rid of the newer slots and keep just the older
        }
        // pass == 0 always runs and generates the index
        // pass == 1 only runs if verify == true.
        // verify checks that all the expected items are in the accounts index and measures how long it takes to look them all up
        let passes = if verify { 2 } else { 1 };
        for pass in 0..passes {
            let total_processed_slots_across_all_threads = AtomicU64::new(0);
            let outer_slots_len = slots.len();
            let chunk_size = (outer_slots_len / 7) + 1; // approximately 400k slots in a snapshot
            let mut index_time = Measure::start("index");
            let insertion_time_us = AtomicU64::new(0);
            let scan_time: u64 = slots
                .par_chunks(chunk_size)
                .map(|slots| {
                    let mut log_status = MultiThreadProgress::new(
                        &total_processed_slots_across_all_threads,
                        2,
                        outer_slots_len as u64,
                    );
                    let mut scan_time_sum = 0;
                    for (index, slot) in slots.iter().enumerate() {
                        let mut scan_time = Measure::start("scan");
                        log_status.report(index as u64);
                        let storage_maps: Vec<Arc<AccountStorageEntry>> = self
                            .storage
                            .get_slot_storage_entries(*slot)
                            .unwrap_or_default();
                        let accounts_map = Self::process_storage_slot(&storage_maps);
                        scan_time.stop();
                        scan_time_sum += scan_time.as_us();

                        let insert_us = if pass == 0 {
                            // generate index
                            self.generate_index_for_slot(accounts_map, slot)
                        } else {
                            // verify index matches expected and measure the time to get all items
                            assert!(verify);
                            let mut lookup_time = Measure::start("lookup_time");
                            for account in accounts_map.into_iter() {
                                let (key, account_info) = account;
                                let lock = self.accounts_index.get_account_maps_read_lock(&key);
                                let x = lock.get(&key).unwrap();
                                let sl = x.slot_list.read().unwrap();
                                let mut count = 0;
                                for (slot2, account_info2) in sl.iter() {
                                    if slot2 == slot {
                                        count += 1;
                                        let ai = AccountInfo {
                                            store_id: account_info.1,
                                            offset: account_info.2.offset,
                                            stored_size: account_info.2.stored_size,
                                            lamports: account_info.2.account_meta.lamports,
                                        };
                                        assert_eq!(&ai, account_info2);
                                    }
                                }
                                assert_eq!(1, count);
                            }
                            lookup_time.stop();
                            lookup_time.as_us()
                        };
                        insertion_time_us.fetch_add(insert_us, Ordering::Relaxed);
                    }
                    scan_time_sum
                })
                .sum();
            index_time.stop();

            let mut min_bin_size = usize::MAX;
            let mut max_bin_size = usize::MIN;
            let total_items = self
                .accounts_index
                .account_maps
                .iter()
                .map(|map_bin| {
                    let len = map_bin.read().unwrap().len();
                    min_bin_size = std::cmp::min(min_bin_size, len);
                    max_bin_size = std::cmp::max(max_bin_size, len);
                    len
                })
                .sum();

            let mut timings = GenerateIndexTimings {
                scan_time,
                index_time: index_time.as_us(),
                insertion_time_us: insertion_time_us.load(Ordering::Relaxed),
                min_bin_size,
                max_bin_size,
                total_items,
                ..GenerateIndexTimings::default()
            };

            if pass == 0 {
                // Need to add these last, otherwise older updates will be cleaned
                for slot in &slots {
                    self.accounts_index.add_root(*slot, false);
                }

                self.initialize_storage_count_and_alive_bytes(&mut timings);
            }
            timings.report();
        }
    }

    fn calculate_storage_count_and_alive_bytes(
        &self,
        timings: &mut GenerateIndexTimings,
    ) -> HashMap<usize, (usize, usize)> {
        // look at every account in the account index and calculate for each storage: stored_size and count
        let mut storage_size_accounts_map_time = Measure::start("storage_size_accounts_map");
        let mut maps = self
            .accounts_index
            .account_maps
            .par_iter()
            .map(|bin_map| {
                let mut stored_sizes_and_counts = HashMap::new();
                bin_map.read().unwrap().values().for_each(|entry| {
                    entry
                        .slot_list
                        .read()
                        .unwrap()
                        .iter()
                        .for_each(|(_slot, account_entry)| {
                            let storage_entry_meta = stored_sizes_and_counts
                                .entry(account_entry.store_id)
                                .or_insert((0, 0));
                            storage_entry_meta.0 += account_entry.stored_size;
                            storage_entry_meta.1 += 1;
                        })
                });
                stored_sizes_and_counts
            })
            .collect::<Vec<_>>();
        storage_size_accounts_map_time.stop();
        timings.storage_size_accounts_map_us = storage_size_accounts_map_time.as_us();

        // flatten/merge the HashMaps from the parallel iteration above
        let mut storage_size_accounts_map_flatten_time =
            Measure::start("storage_size_accounts_map_flatten_time");
        let mut stored_sizes_and_counts = maps.pop().unwrap_or_default();
        for map in maps {
            for (store_id, meta) in map.into_iter() {
                let storage_entry_meta = stored_sizes_and_counts.entry(store_id).or_insert((0, 0));
                storage_entry_meta.0 += meta.0;
                storage_entry_meta.1 += meta.1;
            }
        }
        storage_size_accounts_map_flatten_time.stop();
        timings.storage_size_accounts_map_flatten_us =
            storage_size_accounts_map_flatten_time.as_us();
        stored_sizes_and_counts
    }

    fn set_storage_count_and_alive_bytes(
        &self,
        stored_sizes_and_counts: HashMap<usize, (usize, usize)>,
        timings: &mut GenerateIndexTimings,
    ) {
        // store count and size for each storage
        let mut storage_size_storages_time = Measure::start("storage_size_storages");
        for slot_stores in self.storage.0.iter() {
            for (id, store) in slot_stores.value().read().unwrap().iter() {
                // Should be default at this point
                assert_eq!(store.alive_bytes(), 0);
                if let Some((stored_size, count)) = stored_sizes_and_counts.get(id) {
                    trace!("id: {} setting count: {} cur: {}", id, count, store.count(),);
                    store.count_and_status.write().unwrap().0 = *count;
                    store.alive_bytes.store(*stored_size, Ordering::SeqCst);
                } else {
                    trace!("id: {} clearing count", id);
                    store.count_and_status.write().unwrap().0 = 0;
                }
            }
        }
        storage_size_storages_time.stop();
        timings.storage_size_storages_us = storage_size_storages_time.as_us();
    }

    fn initialize_storage_count_and_alive_bytes(&self, timings: &mut GenerateIndexTimings) {
        let stored_sizes_and_counts = self.calculate_storage_count_and_alive_bytes(timings);
        self.set_storage_count_and_alive_bytes(stored_sizes_and_counts, timings);
    }

    pub(crate) fn print_accounts_stats(&self, label: &str) {
        self.print_index(label);
        self.print_count_and_status(label);
        info!("recycle_stores:");
        let recycle_stores = self.recycle_stores.read().unwrap();
        for (recycled_time, entry) in recycle_stores.iter() {
            info!(
                "  slot: {} id: {} count_and_status: {:?} approx_store_count: {} len: {} capacity: {} (recycled: {:?})",
                entry.slot(),
                entry.append_vec_id(),
                *entry.count_and_status.read().unwrap(),
                entry.approx_store_count.load(Ordering::Relaxed),
                entry.accounts.len(),
                entry.accounts.capacity(),
                recycled_time,
            );
        }
    }

    fn print_index(&self, label: &str) {
        let mut roots: Vec<_> = self.accounts_index.all_roots();
        #[allow(clippy::stable_sort_primitive)]
        roots.sort();
        info!("{}: accounts_index roots: {:?}", label, roots,);
        self.accounts_index.account_maps.iter().for_each(|i| {
            for (pubkey, account_entry) in i.read().unwrap().iter() {
                info!("  key: {} ref_count: {}", pubkey, account_entry.ref_count(),);
                info!(
                    "      slots: {:?}",
                    *account_entry.slot_list.read().unwrap()
                );
            }
        });
    }

    fn print_count_and_status(&self, label: &str) {
        let mut slots: Vec<_> = self.storage.all_slots();
        #[allow(clippy::stable_sort_primitive)]
        slots.sort();
        info!("{}: count_and status for {} slots:", label, slots.len());
        for slot in &slots {
            let slot_stores = self.storage.get_slot_stores(*slot).unwrap();
            let r_slot_stores = slot_stores.read().unwrap();
            let mut ids: Vec<_> = r_slot_stores.keys().cloned().collect();
            #[allow(clippy::stable_sort_primitive)]
            ids.sort();
            for id in &ids {
                let entry = r_slot_stores.get(id).unwrap();
                info!(
                    "  slot: {} id: {} count_and_status: {:?} approx_store_count: {} len: {} capacity: {}",
                    slot,
                    id,
                    *entry.count_and_status.read().unwrap(),
                    entry.approx_store_count.load(Ordering::Relaxed),
                    entry.accounts.len(),
                    entry.accounts.capacity(),
                );
            }
        }
    }
}

#[cfg(test)]
impl AccountsDb {
    pub fn new_sized(paths: Vec<PathBuf>, file_size: u64) -> Self {
        AccountsDb {
            file_size,
            ..AccountsDb::new(paths, &ClusterType::Development)
        }
    }

    pub fn new_sized_no_extra_stores(paths: Vec<PathBuf>, file_size: u64) -> Self {
        AccountsDb {
            file_size,
            min_num_stores: 0,
            ..AccountsDb::new(paths, &ClusterType::Development)
        }
    }

    pub fn get_append_vec_id(&self, pubkey: &Pubkey, slot: Slot) -> Option<AppendVecId> {
        let ancestors = vec![(slot, 1)].into_iter().collect();
        let result = self.accounts_index.get(pubkey, Some(&ancestors), None);
        result.map(|(list, index)| list.slot_list()[index].1.store_id)
    }

    pub fn alive_account_count_in_slot(&self, slot: Slot) -> usize {
        self.storage
            .get_slot_stores(slot)
            .map(|storages| storages.read().unwrap().values().map(|s| s.count()).sum())
            .unwrap_or(0)
    }
}

/// Legacy shrink functions to support non-cached path.
/// Should be able to be deleted after cache path is the only path.
impl AccountsDb {
    // Reads all accounts in given slot's AppendVecs and filter only to alive,
    // then create a minimum AppendVec filled with the alive.
    // v1 path shrinks all stores in the slot
    //
    // Requires all stores in the slot to be re-written otherwise the accounts_index
    // store ref count could become incorrect.
    fn do_shrink_slot_v1(&self, slot: Slot, forced: bool) -> usize {
        trace!("shrink_stale_slot: slot: {}", slot);

        if let Some(stores_lock) = self.storage.get_slot_stores(slot) {
            let stores: Vec<_> = stores_lock.read().unwrap().values().cloned().collect();
            let mut alive_count = 0;
            let mut stored_count = 0;
            let mut written_bytes = 0;
            let mut total_bytes = 0;
            for store in &stores {
                alive_count += store.count();
                stored_count += store.approx_stored_count();
                written_bytes += store.written_bytes();
                total_bytes += store.total_bytes();
            }
            if alive_count == stored_count && stores.len() == 1 {
                trace!(
                    "shrink_stale_slot ({}): not able to shrink at all: alive/stored: {} / {} {}",
                    slot,
                    alive_count,
                    stored_count,
                    if forced { " (forced)" } else { "" },
                );
                return 0;
            } else if !forced {
                let sparse_by_count = (alive_count as f32 / stored_count as f32) <= 0.8;
                let sparse_by_bytes = (written_bytes as f32 / total_bytes as f32) <= 0.8;
                let not_sparse = !sparse_by_count && !sparse_by_bytes;
                let too_small_to_shrink = total_bytes <= PAGE_SIZE;
                if not_sparse || too_small_to_shrink {
                    return 0;
                }
                info!(
                    "shrink_stale_slot ({}): not_sparse: {} count: {}/{} byte: {}/{}",
                    slot, not_sparse, alive_count, stored_count, written_bytes, total_bytes,
                );
            }

            self.do_shrink_slot_stores(slot, stores.iter(), false)
        } else {
            0
        }
    }

    fn do_reset_uncleaned_roots_v1(
        &self,
        candidates: &mut MutexGuard<Vec<Slot>>,
        max_clean_root: Option<Slot>,
    ) {
        let previous_roots = self.accounts_index.reset_uncleaned_roots(max_clean_root);
        candidates.extend(previous_roots);
    }

    #[cfg(test)]
    fn reset_uncleaned_roots_v1(&self) {
        self.do_reset_uncleaned_roots_v1(&mut self.shrink_candidate_slots_v1.lock().unwrap(), None);
    }

    fn do_shrink_stale_slot_v1(&self, slot: Slot) -> usize {
        self.do_shrink_slot_v1(slot, false)
    }
    fn do_shrink_slot_forced_v1(&self, slot: Slot) {
        self.do_shrink_slot_v1(slot, true);
    }

    fn shrink_stale_slot_v1(&self, candidates: &mut MutexGuard<Vec<Slot>>) -> usize {
        let mut shrunken_account_total = 0;
        let mut shrunk_slot_count = 0;
        let start = Instant::now();
        let num_roots = self.accounts_index.num_roots();
        loop {
            if let Some(slot) = self.do_next_shrink_slot_v1(candidates) {
                shrunken_account_total += self.do_shrink_stale_slot_v1(slot);
            } else {
                return 0;
            }
            if start.elapsed().as_millis() > 100 || shrunk_slot_count > num_roots / 10 {
                debug!(
                    "do_shrink_stale_slot_v1: {} {} {}us",
                    shrunk_slot_count,
                    candidates.len(),
                    start.elapsed().as_micros()
                );
                break;
            }
            shrunk_slot_count += 1;
        }
        shrunken_account_total
    }

    // Infinitely returns rooted roots in cyclic order
    fn do_next_shrink_slot_v1(&self, candidates: &mut MutexGuard<Vec<Slot>>) -> Option<Slot> {
        // At this point, a lock (= candidates) is ensured to be held to keep
        // do_reset_uncleaned_roots() (in clean_accounts()) from updating candidates.
        // Also, candidates in the lock may be swapped here if it's empty.
        let next = candidates.pop();

        if next.is_some() {
            next
        } else {
            let mut new_all_slots = self.all_root_slots_in_index();
            let next = new_all_slots.pop();
            // refresh candidates for later calls!
            **candidates = new_all_slots;

            next
        }
    }

    #[cfg(test)]
    fn next_shrink_slot_v1(&self) -> Option<Slot> {
        let mut candidates = self.shrink_candidate_slots_v1.lock().unwrap();
        self.do_next_shrink_slot_v1(&mut candidates)
    }

    pub fn process_stale_slot_v1(&self) -> usize {
        let mut measure = Measure::start("stale_slot_shrink-ms");
        let candidates = self.shrink_candidate_slots_v1.try_lock();
        if candidates.is_err() {
            // skip and return immediately if locked by clean_accounts()
            // the calling background thread will just retry later.
            return 0;
        }
        // hold this lock as long as this shrinking process is running to avoid conflicts
        // with clean_accounts().
        let mut candidates = candidates.unwrap();

        let count = self.shrink_stale_slot_v1(&mut candidates);
        measure.stop();
        inc_new_counter_info!("stale_slot_shrink-ms", measure.as_ms() as usize);

        count
    }

    #[cfg(test)]
    fn shrink_all_stale_slots_v1(&self) {
        for slot in self.all_slots_in_storage() {
            self.do_shrink_stale_slot_v1(slot);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        accounts_hash::MERKLE_FANOUT,
        accounts_index::RefCount,
        accounts_index::{tests::*, AccountSecondaryIndexesIncludeExclude},
        append_vec::{test_utils::TempFile, AccountMeta},
        inline_spl_token_v2_0,
    };
    use assert_matches::assert_matches;
    use rand::{thread_rng, Rng};
    use solana_sdk::{
        account::{accounts_equal, Account, AccountSharedData, ReadableAccount, WritableAccount},
        hash::HASH_BYTES,
        pubkey::PUBKEY_BYTES,
    };
    use std::{
        iter::FromIterator,
        str::FromStr,
        thread::{self, sleep, Builder, JoinHandle},
        time::Duration,
    };

    fn linear_ancestors(end_slot: u64) -> Ancestors {
        let mut ancestors: Ancestors = vec![(0, 0)].into_iter().collect();
        for i in 1..end_slot {
            ancestors.insert(i, (i - 1) as usize);
        }
        ancestors
    }

    fn empty_storages<'a>() -> SortedStorages<'a> {
        SortedStorages::new(&[])
    }

    impl AccountsDb {
        fn scan_snapshot_stores(
            storage: &SortedStorages,
            stats: &mut crate::accounts_hash::HashStats,
            bins: usize,
            bin_range: &Range<usize>,
            check_hash: bool,
        ) -> Result<Vec<Vec<Vec<CalculateHashIntermediate>>>, BankHashVerificationError> {
            Self::scan_snapshot_stores_with_cache(storage, stats, bins, bin_range, check_hash, None)
        }
    }

    #[test]
    #[should_panic(
        expected = "bin_range.start < bins && bin_range.end <= bins &&\\n    bin_range.start < bin_range.end"
    )]
    fn test_accountsdb_scan_snapshot_stores_illegal_range_start() {
        let mut stats = HashStats::default();
        let bounds = Range { start: 2, end: 2 };

        AccountsDb::scan_snapshot_stores(&empty_storages(), &mut stats, 2, &bounds, false).unwrap();
    }
    #[test]
    #[should_panic(
        expected = "bin_range.start < bins && bin_range.end <= bins &&\\n    bin_range.start < bin_range.end"
    )]
    fn test_accountsdb_scan_snapshot_stores_illegal_range_end() {
        let mut stats = HashStats::default();
        let bounds = Range { start: 1, end: 3 };

        AccountsDb::scan_snapshot_stores(&empty_storages(), &mut stats, 2, &bounds, false).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "bin_range.start < bins && bin_range.end <= bins &&\\n    bin_range.start < bin_range.end"
    )]
    fn test_accountsdb_scan_snapshot_stores_illegal_range_inverse() {
        let mut stats = HashStats::default();
        let bounds = Range { start: 1, end: 0 };

        AccountsDb::scan_snapshot_stores(&empty_storages(), &mut stats, 2, &bounds, false).unwrap();
    }

    fn sample_storages_and_account_in_slot(
        slot: Slot,
    ) -> (SnapshotStorages, Vec<CalculateHashIntermediate>) {
        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let pubkey0 = Pubkey::new(&[0u8; 32]);
        let pubkey127 = Pubkey::new(&[0x7fu8; 32]);
        let pubkey128 = Pubkey::new(&[0x80u8; 32]);
        let pubkey255 = Pubkey::new(&[0xffu8; 32]);

        let mut raw_expected = vec![
            CalculateHashIntermediate::new_without_slot(Hash::default(), 1, pubkey0),
            CalculateHashIntermediate::new_without_slot(Hash::default(), 128, pubkey127),
            CalculateHashIntermediate::new_without_slot(Hash::default(), 129, pubkey128),
            CalculateHashIntermediate::new_without_slot(Hash::default(), 256, pubkey255),
        ];

        let expected_hashes = vec![
            Hash::from_str("5K3NW73xFHwgTWVe4LyCg4QfQda8f88uZj2ypDx2kmmH").unwrap(),
            Hash::from_str("84ozw83MZ8oeSF4hRAg7SeW1Tqs9LMXagX1BrDRjtZEx").unwrap(),
            Hash::from_str("5XqtnEJ41CG2JWNp7MAg9nxkRUAnyjLxfsKsdrLxQUbC").unwrap(),
            Hash::from_str("DpvwJcznzwULYh19Zu5CuAA4AT6WTBe4H6n15prATmqj").unwrap(),
        ];

        let mut raw_accounts = Vec::default();

        for i in 0..raw_expected.len() {
            raw_accounts.push(AccountSharedData::new(
                raw_expected[i].lamports,
                1,
                AccountSharedData::default().owner(),
            ));
            let hash = AccountsDb::hash_account(slot, &raw_accounts[i], &raw_expected[i].pubkey);
            if slot == 1 {
                assert_eq!(hash, expected_hashes[i]);
            }
            raw_expected[i].hash = hash;
        }

        let to_store = raw_accounts
            .iter()
            .zip(raw_expected.iter())
            .map(|(account, intermediate)| (&intermediate.pubkey, account))
            .collect::<Vec<_>>();

        accounts.store_uncached(slot, &to_store[..]);
        accounts.add_root(slot);

        let (storages, slots) = accounts.get_snapshot_storages(slot, None);
        assert_eq!(storages.len(), slots.len());
        storages
            .iter()
            .zip(slots.iter())
            .for_each(|(storages, slot)| {
                for storage in storages {
                    assert_eq!(&storage.slot(), slot);
                }
            });
        (storages, raw_expected)
    }

    fn sample_storages_and_accounts() -> (SnapshotStorages, Vec<CalculateHashIntermediate>) {
        sample_storages_and_account_in_slot(1)
    }

    fn get_storage_refs(input: &[SnapshotStorage]) -> SortedStorages {
        SortedStorages::new(input)
    }

    #[test]
    fn test_accountsdb_scan_snapshot_stores() {
        solana_logger::setup();
        let (storages, raw_expected) = sample_storages_and_accounts();

        let bins = 1;
        let mut stats = HashStats::default();

        let result = AccountsDb::scan_snapshot_stores(
            &get_storage_refs(&storages),
            &mut stats,
            bins,
            &Range {
                start: 0,
                end: bins,
            },
            false,
        )
        .unwrap();
        assert_eq!(result, vec![vec![raw_expected.clone()]]);

        let bins = 2;
        let result = AccountsDb::scan_snapshot_stores(
            &get_storage_refs(&storages),
            &mut stats,
            bins,
            &Range {
                start: 0,
                end: bins,
            },
            false,
        )
        .unwrap();
        let mut expected = vec![Vec::new(); bins];
        expected[0].push(raw_expected[0].clone());
        expected[0].push(raw_expected[1].clone());
        expected[bins - 1].push(raw_expected[2].clone());
        expected[bins - 1].push(raw_expected[3].clone());
        assert_eq!(result, vec![expected]);

        let bins = 4;
        let result = AccountsDb::scan_snapshot_stores(
            &get_storage_refs(&storages),
            &mut stats,
            bins,
            &Range {
                start: 0,
                end: bins,
            },
            false,
        )
        .unwrap();
        let mut expected = vec![Vec::new(); bins];
        expected[0].push(raw_expected[0].clone());
        expected[1].push(raw_expected[1].clone());
        expected[2].push(raw_expected[2].clone());
        expected[bins - 1].push(raw_expected[3].clone());
        assert_eq!(result, vec![expected]);

        let bins = 256;
        let result = AccountsDb::scan_snapshot_stores(
            &get_storage_refs(&storages),
            &mut stats,
            bins,
            &Range {
                start: 0,
                end: bins,
            },
            false,
        )
        .unwrap();
        let mut expected = vec![Vec::new(); bins];
        expected[0].push(raw_expected[0].clone());
        expected[127].push(raw_expected[1].clone());
        expected[128].push(raw_expected[2].clone());
        expected[bins - 1].push(raw_expected.last().unwrap().clone());
        assert_eq!(result, vec![expected]);
    }

    #[test]
    fn test_accountsdb_scan_snapshot_stores_2nd_chunk() {
        // enough stores to get to 2nd chunk
        let bins = 1;
        const MAX_ITEMS_PER_CHUNK: usize = 5_000;
        let slot = MAX_ITEMS_PER_CHUNK as Slot;
        let (storages, raw_expected) = sample_storages_and_account_in_slot(slot);
        let storage_data = vec![(&storages[0], slot)];

        let sorted_storages =
            SortedStorages::new_debug(&storage_data[..], 0, MAX_ITEMS_PER_CHUNK + 1);

        let mut stats = HashStats::default();
        let result = AccountsDb::scan_snapshot_stores(
            &sorted_storages,
            &mut stats,
            bins,
            &Range {
                start: 0,
                end: bins,
            },
            false,
        )
        .unwrap();
        assert_eq!(result.len(), 2); // 2 chunks
        assert_eq!(result[0].len(), 0); // nothing found in first slots
        assert_eq!(result[1].len(), bins);
        assert_eq!(result[1], vec![raw_expected]);
    }

    #[test]
    fn test_accountsdb_scan_snapshot_stores_binning() {
        let mut stats = HashStats::default();
        let (storages, raw_expected) = sample_storages_and_accounts();

        // just the first bin of 2
        let bins = 2;
        let half_bins = bins / 2;
        let result = AccountsDb::scan_snapshot_stores(
            &get_storage_refs(&storages),
            &mut stats,
            bins,
            &Range {
                start: 0,
                end: half_bins,
            },
            false,
        )
        .unwrap();
        let mut expected = vec![Vec::new(); half_bins];
        expected[0].push(raw_expected[0].clone());
        expected[0].push(raw_expected[1].clone());
        assert_eq!(result, vec![expected]);

        // just the second bin of 2
        let result = AccountsDb::scan_snapshot_stores(
            &get_storage_refs(&storages),
            &mut stats,
            bins,
            &Range {
                start: 1,
                end: bins,
            },
            false,
        )
        .unwrap();

        let mut expected = vec![Vec::new(); half_bins];
        let starting_bin_index = 0;
        expected[starting_bin_index].push(raw_expected[2].clone());
        expected[starting_bin_index].push(raw_expected[3].clone());
        assert_eq!(result, vec![expected]);

        // 1 bin at a time of 4
        let bins = 4;
        for (bin, expected_item) in raw_expected.iter().enumerate().take(bins) {
            let result = AccountsDb::scan_snapshot_stores(
                &get_storage_refs(&storages),
                &mut stats,
                bins,
                &Range {
                    start: bin,
                    end: bin + 1,
                },
                false,
            )
            .unwrap();
            let mut expected = vec![Vec::new(); 1];
            expected[0].push(expected_item.clone());
            assert_eq!(result, vec![expected]);
        }

        let bins = 256;
        let bin_locations = vec![0, 127, 128, 255];
        for bin in 0..bins {
            let result = AccountsDb::scan_snapshot_stores(
                &get_storage_refs(&storages),
                &mut stats,
                bins,
                &Range {
                    start: bin,
                    end: bin + 1,
                },
                false,
            )
            .unwrap();
            let mut expected = vec![];
            if let Some(index) = bin_locations.iter().position(|&r| r == bin) {
                expected = vec![Vec::new(); 1];
                expected[0].push(raw_expected[index].clone());
            }
            assert_eq!(result, vec![expected]);
        }
    }

    #[test]
    fn test_accountsdb_scan_snapshot_stores_binning_2nd_chunk() {
        // enough stores to get to 2nd chunk
        // range is for only 1 bin out of 256.
        let bins = 256;
        const MAX_ITEMS_PER_CHUNK: usize = 5_000;
        let slot = MAX_ITEMS_PER_CHUNK as Slot;
        let (storages, raw_expected) = sample_storages_and_account_in_slot(slot);
        let storage_data = vec![(&storages[0], slot)];

        let sorted_storages =
            SortedStorages::new_debug(&storage_data[..], 0, MAX_ITEMS_PER_CHUNK + 1);

        let mut stats = HashStats::default();
        let result = AccountsDb::scan_snapshot_stores(
            &sorted_storages,
            &mut stats,
            bins,
            &Range {
                start: 127,
                end: 128,
            },
            false,
        )
        .unwrap();
        assert_eq!(result.len(), 2); // 2 chunks
        assert_eq!(result[0].len(), 0); // nothing found in first slots
        let mut expected = vec![Vec::new(); 1];
        expected[0].push(raw_expected[1].clone());
        assert_eq!(result[1].len(), 1);
        assert_eq!(result[1], expected);
    }

    #[test]
    fn test_accountsdb_calculate_accounts_hash_without_index_simple() {
        solana_logger::setup();

        let (storages, _size, _slot_expected) = sample_storage();
        let result = AccountsDb::calculate_accounts_hash_without_index(
            &get_storage_refs(&storages),
            None,
            HashStats::default(),
            false,
            None,
        )
        .unwrap();
        let expected_hash = Hash::from_str("GKot5hBsd81kMupNCXHaqbhv3huEbxAFMLnpcX2hniwn").unwrap();
        assert_eq!(result, (expected_hash, 0));
    }

    #[test]
    fn test_accountsdb_calculate_accounts_hash_without_index() {
        solana_logger::setup();

        let (storages, raw_expected) = sample_storages_and_accounts();
        let expected_hash =
            AccountsHash::compute_merkle_root_loop(raw_expected.clone(), MERKLE_FANOUT, |item| {
                item.hash
            });
        let sum = raw_expected.iter().map(|item| item.lamports).sum();
        let result = AccountsDb::calculate_accounts_hash_without_index(
            &get_storage_refs(&storages),
            None,
            HashStats::default(),
            false,
            None,
        )
        .unwrap();

        assert_eq!(result, (expected_hash, sum));
    }

    fn sample_storage() -> (SnapshotStorages, usize, Slot) {
        let (_temp_dirs, paths) = get_temp_accounts_paths(1).unwrap();
        let slot_expected: Slot = 0;
        let size: usize = 123;
        let data = AccountStorageEntry::new(&paths[0], slot_expected, 0, size as u64);

        let arc = Arc::new(data);
        let storages = vec![vec![arc]];
        (storages, size, slot_expected)
    }

    #[test]
    fn test_accountsdb_scan_account_storage_no_bank() {
        solana_logger::setup();

        let expected = 1;
        let tf = crate::append_vec::test_utils::get_append_vec_path(
            "test_accountsdb_scan_account_storage_no_bank",
        );
        let (_temp_dirs, paths) = get_temp_accounts_paths(1).unwrap();
        let slot_expected: Slot = 0;
        let size: usize = 123;
        let mut data = AccountStorageEntry::new(&paths[0], slot_expected, 0, size as u64);
        let av = AppendVec::new(&tf.path, true, 1024 * 1024);
        data.accounts = av;

        let arc = Arc::new(data);
        let storages = vec![vec![arc]];
        let pubkey = solana_sdk::pubkey::new_rand();
        let acc = AccountSharedData::new(1, 48, AccountSharedData::default().owner());
        let sm = StoredMeta {
            data_len: 1,
            pubkey,
            write_version: 1,
        };
        storages[0][0]
            .accounts
            .append_accounts(&[(sm, Some(&acc))], &[&Hash::default()]);

        let calls = AtomicU64::new(0);
        let result = AccountsDb::scan_account_storage_no_bank(
            None,
            &get_storage_refs(&storages),
            |loaded_account: LoadedAccount, accum: &mut Vec<u64>, slot: Slot| {
                calls.fetch_add(1, Ordering::Relaxed);
                assert_eq!(loaded_account.pubkey(), &pubkey);
                assert_eq!(slot_expected, slot);
                accum.push(expected);
            },
            |a| a,
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(result, vec![vec![expected]]);
    }

    #[test]
    fn test_accountsdb_scan_account_storage_no_bank_one_slot() {
        solana_logger::setup();

        let expected = 1;
        let tf = crate::append_vec::test_utils::get_append_vec_path(
            "test_accountsdb_scan_account_storage_no_bank",
        );
        let (_temp_dirs, paths) = get_temp_accounts_paths(1).unwrap();
        let slot_expected: Slot = 0;
        let size: usize = 123;
        let mut data = AccountStorageEntry::new(&paths[0], slot_expected, 0, size as u64);
        let av = AppendVec::new(&tf.path, true, 1024 * 1024);
        data.accounts = av;

        let arc = Arc::new(data);
        let storages = vec![vec![arc]];
        let pubkey = solana_sdk::pubkey::new_rand();
        let acc = AccountSharedData::new(1, 48, AccountSharedData::default().owner());
        let sm = StoredMeta {
            data_len: 1,
            pubkey,
            write_version: 1,
        };
        storages[0][0]
            .accounts
            .append_accounts(&[(sm, Some(&acc))], &[&Hash::default()]);

        let calls = AtomicU64::new(0);
        let mut accum = Vec::new();
        let scan_func = |loaded_account: LoadedAccount, accum: &mut Vec<u64>, slot: Slot| {
            calls.fetch_add(1, Ordering::Relaxed);
            assert_eq!(loaded_account.pubkey(), &pubkey);
            assert_eq!(slot_expected, slot);
            accum.push(expected);
        };
        AccountsDb::scan_multiple_account_storages_one_slot(
            &storages[0],
            &scan_func,
            slot_expected,
            &mut accum,
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(accum, vec![expected]);
    }

    fn sample_storage_with_entries(
        tf: &TempFile,
        write_version: StoredMetaWriteVersion,
        slot: Slot,
        pubkey: &Pubkey,
    ) -> SnapshotStorages {
        let (_temp_dirs, paths) = get_temp_accounts_paths(1).unwrap();
        let size: usize = 123;
        let mut data = AccountStorageEntry::new(&paths[0], slot, 0, size as u64);
        let av = AppendVec::new(&tf.path, true, 1024 * 1024);
        data.accounts = av;

        let arc = Arc::new(data);
        let storages = vec![vec![arc]];
        let acc = AccountSharedData::new(1, 48, AccountSharedData::default().owner());
        let sm = StoredMeta {
            data_len: 1,
            pubkey: *pubkey,
            write_version,
        };
        storages[0][0]
            .accounts
            .append_accounts(&[(sm, Some(&acc))], &[&Hash::default()]);
        storages
    }

    #[test]
    fn test_accountsdb_scan_multiple_account_storage_no_bank_one_slot() {
        solana_logger::setup();

        let slot_expected: Slot = 0;
        let tf = crate::append_vec::test_utils::get_append_vec_path(
            "test_accountsdb_scan_account_storage_no_bank",
        );
        let write_version1 = 0;
        let write_version2 = 1;
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let pubkey2 = solana_sdk::pubkey::new_rand();
        for swap in [false, true].iter() {
            let mut storages = [
                sample_storage_with_entries(&tf, write_version1, slot_expected, &pubkey1)
                    .remove(0)
                    .remove(0),
                sample_storage_with_entries(&tf, write_version2, slot_expected, &pubkey2)
                    .remove(0)
                    .remove(0),
            ];
            if *swap {
                storages[..].swap(0, 1);
            }
            let calls = AtomicU64::new(0);
            let scan_func = |loaded_account: LoadedAccount, accum: &mut Vec<u64>, slot: Slot| {
                calls.fetch_add(1, Ordering::Relaxed);
                let write_version = loaded_account.write_version();
                let first = loaded_account.pubkey() == &pubkey1 && write_version == write_version1;
                assert!(
                    first || loaded_account.pubkey() == &pubkey2 && write_version == write_version2
                );
                assert_eq!(slot_expected, slot);
                if first {
                    assert!(accum.is_empty());
                } else {
                    assert!(accum.len() == 1);
                }
                accum.push(write_version);
            };
            let mut accum = Vec::new();
            AccountsDb::scan_multiple_account_storages_one_slot(
                &storages,
                &scan_func,
                slot_expected,
                &mut accum,
            );
            assert_eq!(calls.load(Ordering::Relaxed), storages.len() as u64);
            assert_eq!(accum, vec![write_version1, write_version2]);
        }
    }

    #[test]
    fn test_accountsdb_add_root() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let key = Pubkey::default();
        let account0 = AccountSharedData::new(1, 0, &key);

        db.store_uncached(0, &[(&key, &account0)]);
        db.add_root(0);
        let ancestors = vec![(1, 1)].into_iter().collect();
        assert_eq!(
            db.load_without_fixed_root(&ancestors, &key),
            Some((account0, 0))
        );
    }

    #[test]
    fn test_accountsdb_latest_ancestor() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let key = Pubkey::default();
        let account0 = AccountSharedData::new(1, 0, &key);

        db.store_uncached(0, &[(&key, &account0)]);

        let account1 = AccountSharedData::new(0, 0, &key);
        db.store_uncached(1, &[(&key, &account1)]);

        let ancestors = vec![(1, 1)].into_iter().collect();
        assert_eq!(
            &db.load_without_fixed_root(&ancestors, &key).unwrap().0,
            &account1
        );

        let ancestors = vec![(1, 1), (0, 0)].into_iter().collect();
        assert_eq!(
            &db.load_without_fixed_root(&ancestors, &key).unwrap().0,
            &account1
        );

        let accounts: Vec<AccountSharedData> = db.unchecked_scan_accounts(
            "",
            &ancestors,
            |accounts: &mut Vec<AccountSharedData>, option| {
                accounts.push(option.1.take_account());
            },
        );
        assert_eq!(accounts, vec![account1]);
    }

    #[test]
    fn test_accountsdb_latest_ancestor_with_root() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let key = Pubkey::default();
        let account0 = AccountSharedData::new(1, 0, &key);

        db.store_uncached(0, &[(&key, &account0)]);

        let account1 = AccountSharedData::new(0, 0, &key);
        db.store_uncached(1, &[(&key, &account1)]);
        db.add_root(0);

        let ancestors = vec![(1, 1)].into_iter().collect();
        assert_eq!(
            &db.load_without_fixed_root(&ancestors, &key).unwrap().0,
            &account1
        );

        let ancestors = vec![(1, 1), (0, 0)].into_iter().collect();
        assert_eq!(
            &db.load_without_fixed_root(&ancestors, &key).unwrap().0,
            &account1
        );
    }

    #[test]
    fn test_accountsdb_root_one_slot() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = Pubkey::default();
        let account0 = AccountSharedData::new(1, 0, &key);

        // store value 1 in the "root", i.e. db zero
        db.store_uncached(0, &[(&key, &account0)]);

        // now we have:
        //
        //                       root0 -> key.lamports==1
        //                        / \
        //                       /   \
        //  key.lamports==0 <- slot1    \
        //                             slot2 -> key.lamports==1
        //                                       (via root0)

        // store value 0 in one child
        let account1 = AccountSharedData::new(0, 0, &key);
        db.store_uncached(1, &[(&key, &account1)]);

        // masking accounts is done at the Accounts level, at accountsDB we see
        // original account (but could also accept "None", which is implemented
        // at the Accounts level)
        let ancestors = vec![(0, 0), (1, 1)].into_iter().collect();
        assert_eq!(
            &db.load_without_fixed_root(&ancestors, &key).unwrap().0,
            &account1
        );

        // we should see 1 token in slot 2
        let ancestors = vec![(0, 0), (2, 2)].into_iter().collect();
        assert_eq!(
            &db.load_without_fixed_root(&ancestors, &key).unwrap().0,
            &account0
        );

        db.add_root(0);

        let ancestors = vec![(1, 1)].into_iter().collect();
        assert_eq!(
            db.load_without_fixed_root(&ancestors, &key),
            Some((account1, 1))
        );
        let ancestors = vec![(2, 2)].into_iter().collect();
        assert_eq!(
            db.load_without_fixed_root(&ancestors, &key),
            Some((account0, 0))
        ); // original value
    }

    #[test]
    fn test_accountsdb_add_root_many() {
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let mut pubkeys: Vec<Pubkey> = vec![];
        create_account(&db, &mut pubkeys, 0, 100, 0, 0);
        for _ in 1..100 {
            let idx = thread_rng().gen_range(0, 99);
            let ancestors = vec![(0, 0)].into_iter().collect();
            let account = db
                .load_without_fixed_root(&ancestors, &pubkeys[idx])
                .unwrap();
            let default_account = AccountSharedData::from(Account {
                lamports: (idx + 1) as u64,
                ..Account::default()
            });
            assert_eq!((default_account, 0), account);
        }

        db.add_root(0);

        // check that all the accounts appear with a new root
        for _ in 1..100 {
            let idx = thread_rng().gen_range(0, 99);
            let ancestors = vec![(0, 0)].into_iter().collect();
            let account0 = db
                .load_without_fixed_root(&ancestors, &pubkeys[idx])
                .unwrap();
            let ancestors = vec![(1, 1)].into_iter().collect();
            let account1 = db
                .load_without_fixed_root(&ancestors, &pubkeys[idx])
                .unwrap();
            let default_account = AccountSharedData::from(Account {
                lamports: (idx + 1) as u64,
                ..Account::default()
            });
            assert_eq!(&default_account, &account0.0);
            assert_eq!(&default_account, &account1.0);
        }
    }

    #[test]
    fn test_accountsdb_count_stores() {
        solana_logger::setup();
        let db = AccountsDb::new_single();

        let mut pubkeys: Vec<Pubkey> = vec![];
        create_account(&db, &mut pubkeys, 0, 2, DEFAULT_FILE_SIZE as usize / 3, 0);
        assert!(check_storage(&db, 0, 2));

        let pubkey = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(1, DEFAULT_FILE_SIZE as usize / 3, &pubkey);
        db.store_uncached(1, &[(&pubkey, &account)]);
        db.store_uncached(1, &[(&pubkeys[0], &account)]);
        {
            let slot_0_stores = &db.storage.get_slot_stores(0).unwrap();
            let slot_1_stores = &db.storage.get_slot_stores(1).unwrap();
            let r_slot_0_stores = slot_0_stores.read().unwrap();
            let r_slot_1_stores = slot_1_stores.read().unwrap();
            assert_eq!(r_slot_0_stores.len(), 1);
            assert_eq!(r_slot_1_stores.len(), 1);
            assert_eq!(r_slot_0_stores.get(&0).unwrap().count(), 2);
            assert_eq!(r_slot_1_stores[&1].count(), 2);
            assert_eq!(r_slot_0_stores.get(&0).unwrap().approx_stored_count(), 2);
            assert_eq!(r_slot_1_stores[&1].approx_stored_count(), 2);
        }

        // adding root doesn't change anything
        db.get_accounts_delta_hash(1);
        db.add_root(1);
        {
            let slot_0_stores = &db.storage.get_slot_stores(0).unwrap();
            let slot_1_stores = &db.storage.get_slot_stores(1).unwrap();
            let r_slot_0_stores = slot_0_stores.read().unwrap();
            let r_slot_1_stores = slot_1_stores.read().unwrap();
            assert_eq!(r_slot_0_stores.len(), 1);
            assert_eq!(r_slot_1_stores.len(), 1);
            assert_eq!(r_slot_0_stores.get(&0).unwrap().count(), 2);
            assert_eq!(r_slot_1_stores[&1].count(), 2);
            assert_eq!(r_slot_0_stores.get(&0).unwrap().approx_stored_count(), 2);
            assert_eq!(r_slot_1_stores[&1].approx_stored_count(), 2);
        }

        // overwrite old rooted account version; only the r_slot_0_stores.count() should be
        // decremented
        db.store_uncached(2, &[(&pubkeys[0], &account)]);
        db.clean_accounts(None, false);
        {
            let slot_0_stores = &db.storage.get_slot_stores(0).unwrap();
            let slot_1_stores = &db.storage.get_slot_stores(1).unwrap();
            let r_slot_0_stores = slot_0_stores.read().unwrap();
            let r_slot_1_stores = slot_1_stores.read().unwrap();
            assert_eq!(r_slot_0_stores.len(), 1);
            assert_eq!(r_slot_1_stores.len(), 1);
            assert_eq!(r_slot_0_stores.get(&0).unwrap().count(), 1);
            assert_eq!(r_slot_1_stores[&1].count(), 2);
            assert_eq!(r_slot_0_stores.get(&0).unwrap().approx_stored_count(), 2);
            assert_eq!(r_slot_1_stores[&1].approx_stored_count(), 2);
        }
    }

    #[test]
    fn test_accounts_unsquashed() {
        let key = Pubkey::default();

        // 1 token in the "root", i.e. db zero
        let db0 = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let account0 = AccountSharedData::new(1, 0, &key);
        db0.store_uncached(0, &[(&key, &account0)]);

        // 0 lamports in the child
        let account1 = AccountSharedData::new(0, 0, &key);
        db0.store_uncached(1, &[(&key, &account1)]);

        // masking accounts is done at the Accounts level, at accountsDB we see
        // original account
        let ancestors = vec![(0, 0), (1, 1)].into_iter().collect();
        assert_eq!(
            db0.load_without_fixed_root(&ancestors, &key),
            Some((account1, 1))
        );
        let ancestors = vec![(0, 0)].into_iter().collect();
        assert_eq!(
            db0.load_without_fixed_root(&ancestors, &key),
            Some((account0, 0))
        );
    }

    fn run_test_remove_unrooted_slot(is_cached: bool) {
        let unrooted_slot = 9;
        let unrooted_bank_id = 9;
        let mut db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        db.caching_enabled = true;
        let key = Pubkey::default();
        let account0 = AccountSharedData::new(1, 0, &key);
        let ancestors = vec![(unrooted_slot, 1)].into_iter().collect();
        if is_cached {
            db.store_cached(unrooted_slot, &[(&key, &account0)]);
        } else {
            db.store_uncached(unrooted_slot, &[(&key, &account0)]);
        }
        db.bank_hashes
            .write()
            .unwrap()
            .insert(unrooted_slot, BankHashInfo::default());
        assert!(db
            .accounts_index
            .get(&key, Some(&ancestors), None)
            .is_some());
        assert_load_account(&db, unrooted_slot, key, 1);

        // Purge the slot
        db.remove_unrooted_slots(&[(unrooted_slot, unrooted_bank_id)]);
        assert!(db.load_without_fixed_root(&ancestors, &key).is_none());
        assert!(db.bank_hashes.read().unwrap().get(&unrooted_slot).is_none());
        assert!(db.accounts_cache.slot_cache(unrooted_slot).is_none());
        assert!(db.storage.0.get(&unrooted_slot).is_none());
        assert!(db.accounts_index.get_account_read_entry(&key).is_none());
        assert!(db
            .accounts_index
            .get(&key, Some(&ancestors), None)
            .is_none());

        // Test we can store for the same slot again and get the right information
        let account0 = AccountSharedData::new(2, 0, &key);
        db.store_uncached(unrooted_slot, &[(&key, &account0)]);
        assert_load_account(&db, unrooted_slot, key, 2);
    }

    #[test]
    fn test_remove_unrooted_slot_cached() {
        run_test_remove_unrooted_slot(true);
    }

    #[test]
    fn test_remove_unrooted_slot_storage() {
        run_test_remove_unrooted_slot(false);
    }

    #[test]
    fn test_remove_unrooted_slot_snapshot() {
        solana_logger::setup();
        let unrooted_slot = 9;
        let unrooted_bank_id = 9;
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let key = solana_sdk::pubkey::new_rand();
        let account0 = AccountSharedData::new(1, 0, &key);
        db.store_uncached(unrooted_slot, &[(&key, &account0)]);

        // Purge the slot
        db.remove_unrooted_slots(&[(unrooted_slot, unrooted_bank_id)]);

        // Add a new root
        let key2 = solana_sdk::pubkey::new_rand();
        let new_root = unrooted_slot + 1;
        db.store_uncached(new_root, &[(&key2, &account0)]);
        db.add_root(new_root);

        // Simulate reconstruction from snapshot
        let db = reconstruct_accounts_db_via_serialization(&db, new_root);

        // Check root account exists
        assert_load_account(&db, new_root, key2, 1);

        // Check purged account stays gone
        let unrooted_slot_ancestors = vec![(unrooted_slot, 1)].into_iter().collect();
        assert!(db
            .load_without_fixed_root(&unrooted_slot_ancestors, &key)
            .is_none());
    }

    fn create_account(
        accounts: &AccountsDb,
        pubkeys: &mut Vec<Pubkey>,
        slot: Slot,
        num: usize,
        space: usize,
        num_vote: usize,
    ) {
        let ancestors = vec![(slot, 0)].into_iter().collect();
        for t in 0..num {
            let pubkey = solana_sdk::pubkey::new_rand();
            let account =
                AccountSharedData::new((t + 1) as u64, space, AccountSharedData::default().owner());
            pubkeys.push(pubkey);
            assert!(accounts
                .load_without_fixed_root(&ancestors, &pubkey)
                .is_none());
            accounts.store_uncached(slot, &[(&pubkey, &account)]);
        }
        for t in 0..num_vote {
            let pubkey = solana_sdk::pubkey::new_rand();
            let account =
                AccountSharedData::new((num + t + 1) as u64, space, &solana_vote_program::id());
            pubkeys.push(pubkey);
            let ancestors = vec![(slot, 0)].into_iter().collect();
            assert!(accounts
                .load_without_fixed_root(&ancestors, &pubkey)
                .is_none());
            accounts.store_uncached(slot, &[(&pubkey, &account)]);
        }
    }

    fn update_accounts(accounts: &AccountsDb, pubkeys: &[Pubkey], slot: Slot, range: usize) {
        for _ in 1..1000 {
            let idx = thread_rng().gen_range(0, range);
            let ancestors = vec![(slot, 0)].into_iter().collect();
            if let Some((mut account, _)) =
                accounts.load_without_fixed_root(&ancestors, &pubkeys[idx])
            {
                account.checked_add_lamports(1).unwrap();
                accounts.store_uncached(slot, &[(&pubkeys[idx], &account)]);
                if account.lamports() == 0 {
                    let ancestors = vec![(slot, 0)].into_iter().collect();
                    assert!(accounts
                        .load_without_fixed_root(&ancestors, &pubkeys[idx])
                        .is_none());
                } else {
                    let default_account = AccountSharedData::from(Account {
                        lamports: account.lamports(),
                        ..Account::default()
                    });
                    assert_eq!(default_account, account);
                }
            }
        }
    }

    fn check_storage(accounts: &AccountsDb, slot: Slot, count: usize) -> bool {
        assert_eq!(
            accounts
                .storage
                .get_slot_stores(slot)
                .unwrap()
                .read()
                .unwrap()
                .len(),
            1
        );
        let slot_storages = accounts.storage.get_slot_stores(slot).unwrap();
        let mut total_count: usize = 0;
        let r_slot_storages = slot_storages.read().unwrap();
        for store in r_slot_storages.values() {
            assert_eq!(store.status(), AccountStorageStatus::Available);
            total_count += store.count();
        }
        assert_eq!(total_count, count);
        let (expected_store_count, actual_store_count): (usize, usize) = (
            r_slot_storages
                .values()
                .map(|s| s.approx_stored_count())
                .sum(),
            r_slot_storages
                .values()
                .map(|s| s.all_accounts().len())
                .sum(),
        );
        assert_eq!(expected_store_count, actual_store_count);
        total_count == count
    }

    fn check_accounts(
        accounts: &AccountsDb,
        pubkeys: &[Pubkey],
        slot: Slot,
        num: usize,
        count: usize,
    ) {
        let ancestors = vec![(slot, 0)].into_iter().collect();
        for _ in 0..num {
            let idx = thread_rng().gen_range(0, num);
            let account = accounts.load_without_fixed_root(&ancestors, &pubkeys[idx]);
            let account1 = Some((
                AccountSharedData::new(
                    (idx + count) as u64,
                    0,
                    AccountSharedData::default().owner(),
                ),
                slot,
            ));
            assert_eq!(account, account1);
        }
    }

    #[allow(clippy::needless_range_loop)]
    fn modify_accounts(
        accounts: &AccountsDb,
        pubkeys: &[Pubkey],
        slot: Slot,
        num: usize,
        count: usize,
    ) {
        for idx in 0..num {
            let account = AccountSharedData::new(
                (idx + count) as u64,
                0,
                AccountSharedData::default().owner(),
            );
            accounts.store_uncached(slot, &[(&pubkeys[idx], &account)]);
        }
    }

    #[test]
    fn test_account_one() {
        let (_accounts_dirs, paths) = get_temp_accounts_paths(1).unwrap();
        let db = AccountsDb::new(paths, &ClusterType::Development);
        let mut pubkeys: Vec<Pubkey> = vec![];
        create_account(&db, &mut pubkeys, 0, 1, 0, 0);
        let ancestors = vec![(0, 0)].into_iter().collect();
        let account = db.load_without_fixed_root(&ancestors, &pubkeys[0]).unwrap();
        let default_account = AccountSharedData::from(Account {
            lamports: 1,
            ..Account::default()
        });
        assert_eq!((default_account, 0), account);
    }

    #[test]
    fn test_account_many() {
        let (_accounts_dirs, paths) = get_temp_accounts_paths(2).unwrap();
        let db = AccountsDb::new(paths, &ClusterType::Development);
        let mut pubkeys: Vec<Pubkey> = vec![];
        create_account(&db, &mut pubkeys, 0, 100, 0, 0);
        check_accounts(&db, &pubkeys, 0, 100, 1);
    }

    #[test]
    fn test_account_update() {
        let accounts = AccountsDb::new_single();
        let mut pubkeys: Vec<Pubkey> = vec![];
        create_account(&accounts, &mut pubkeys, 0, 100, 0, 0);
        update_accounts(&accounts, &pubkeys, 0, 99);
        assert!(check_storage(&accounts, 0, 100));
    }

    #[test]
    fn test_account_grow_many() {
        let (_accounts_dir, paths) = get_temp_accounts_paths(2).unwrap();
        let size = 4096;
        let accounts = AccountsDb::new_sized(paths, size);
        let mut keys = vec![];
        for i in 0..9 {
            let key = solana_sdk::pubkey::new_rand();
            let account = AccountSharedData::new(i + 1, size as usize / 4, &key);
            accounts.store_uncached(0, &[(&key, &account)]);
            keys.push(key);
        }
        let ancestors = vec![(0, 0)].into_iter().collect();
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                accounts
                    .load_without_fixed_root(&ancestors, key)
                    .unwrap()
                    .0
                    .lamports(),
                (i as u64) + 1
            );
        }

        let mut append_vec_histogram = HashMap::new();
        let mut all_storages = vec![];
        for slot_storage in accounts.storage.0.iter() {
            all_storages.extend(slot_storage.read().unwrap().values().cloned())
        }
        for storage in all_storages {
            *append_vec_histogram.entry(storage.slot()).or_insert(0) += 1;
        }
        for count in append_vec_histogram.values() {
            assert!(*count >= 2);
        }
    }

    #[test]
    fn test_account_grow() {
        let accounts = AccountsDb::new_single();

        let status = [AccountStorageStatus::Available, AccountStorageStatus::Full];
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let account1 = AccountSharedData::new(1, DEFAULT_FILE_SIZE as usize / 2, &pubkey1);
        accounts.store_uncached(0, &[(&pubkey1, &account1)]);
        {
            let stores = &accounts.storage.get_slot_stores(0).unwrap();
            let r_stores = stores.read().unwrap();
            assert_eq!(r_stores.len(), 1);
            assert_eq!(r_stores[&0].count(), 1);
            assert_eq!(r_stores[&0].status(), AccountStorageStatus::Available);
        }

        let pubkey2 = solana_sdk::pubkey::new_rand();
        let account2 = AccountSharedData::new(1, DEFAULT_FILE_SIZE as usize / 2, &pubkey2);
        accounts.store_uncached(0, &[(&pubkey2, &account2)]);
        {
            assert_eq!(accounts.storage.0.len(), 1);
            let stores = &accounts.storage.get_slot_stores(0).unwrap();
            let r_stores = stores.read().unwrap();
            assert_eq!(r_stores.len(), 2);
            assert_eq!(r_stores[&0].count(), 1);
            assert_eq!(r_stores[&0].status(), AccountStorageStatus::Full);
            assert_eq!(r_stores[&1].count(), 1);
            assert_eq!(r_stores[&1].status(), AccountStorageStatus::Available);
        }
        let ancestors = vec![(0, 0)].into_iter().collect();
        assert_eq!(
            accounts
                .load_without_fixed_root(&ancestors, &pubkey1)
                .unwrap()
                .0,
            account1
        );
        assert_eq!(
            accounts
                .load_without_fixed_root(&ancestors, &pubkey2)
                .unwrap()
                .0,
            account2
        );

        // lots of stores, but 7 storages should be enough for everything
        for _ in 0..25 {
            accounts.store_uncached(0, &[(&pubkey1, &account1)]);
            {
                assert_eq!(accounts.storage.0.len(), 1);
                let stores = &accounts.storage.get_slot_stores(0).unwrap();
                let r_stores = stores.read().unwrap();
                assert!(r_stores.len() <= 7);
                assert_eq!(r_stores[&0].status(), status[0]);
            }
            let ancestors = vec![(0, 0)].into_iter().collect();
            assert_eq!(
                accounts
                    .load_without_fixed_root(&ancestors, &pubkey1)
                    .unwrap()
                    .0,
                account1
            );
            assert_eq!(
                accounts
                    .load_without_fixed_root(&ancestors, &pubkey2)
                    .unwrap()
                    .0,
                account2
            );
        }
    }

    #[test]
    fn test_lazy_gc_slot() {
        solana_logger::setup();
        //This test is pedantic
        //A slot is purged when a non root bank is cleaned up.  If a slot is behind root but it is
        //not root, it means we are retaining dead banks.
        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let pubkey = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        //store an account
        accounts.store_uncached(0, &[(&pubkey, &account)]);
        let ancestors = vec![(0, 0)].into_iter().collect();
        let id = {
            let (lock, idx) = accounts
                .accounts_index
                .get(&pubkey, Some(&ancestors), None)
                .unwrap();
            lock.slot_list()[idx].1.store_id
        };
        accounts.get_accounts_delta_hash(0);
        accounts.add_root(1);

        //slot is still there, since gc is lazy
        assert!(accounts
            .storage
            .get_slot_stores(0)
            .unwrap()
            .read()
            .unwrap()
            .get(&id)
            .is_some());

        //store causes clean
        accounts.store_uncached(1, &[(&pubkey, &account)]);

        // generate delta state for slot 1, so clean operates on it.
        accounts.get_accounts_delta_hash(1);

        //slot is gone
        accounts.print_accounts_stats("pre-clean");
        accounts.clean_accounts(None, false);
        assert!(accounts.storage.0.get(&0).is_none());

        //new value is there
        let ancestors = vec![(1, 1)].into_iter().collect();
        assert_eq!(
            accounts.load_without_fixed_root(&ancestors, &pubkey),
            Some((account, 1))
        );
    }

    impl AccountsDb {
        fn all_account_count_in_append_vec(&self, slot: Slot) -> usize {
            let slot_storage = self.storage.get_slot_stores(slot);
            if let Some(slot_storage) = slot_storage {
                let r_slot_storage = slot_storage.read().unwrap();
                let count = r_slot_storage
                    .values()
                    .map(|store| store.all_accounts().len())
                    .sum();
                let stored_count: usize = r_slot_storage
                    .values()
                    .map(|store| store.approx_stored_count())
                    .sum();
                assert_eq!(stored_count, count);
                count
            } else {
                0
            }
        }

        fn ref_count_for_pubkey(&self, pubkey: &Pubkey) -> RefCount {
            self.accounts_index.ref_count_from_storage(pubkey)
        }
    }

    #[test]
    fn test_clean_zero_lamport_and_dead_slot() {
        solana_logger::setup();

        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let pubkey2 = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(1, 1, AccountSharedData::default().owner());
        let zero_lamport_account =
            AccountSharedData::new(0, 0, AccountSharedData::default().owner());

        // Store two accounts
        accounts.store_uncached(0, &[(&pubkey1, &account)]);
        accounts.store_uncached(0, &[(&pubkey2, &account)]);

        // Make sure both accounts are in the same AppendVec in slot 0, which
        // will prevent pubkey1 from being cleaned up later even when it's a
        // zero-lamport account
        let ancestors = vec![(0, 1)].into_iter().collect();
        let (slot1, account_info1) = accounts
            .accounts_index
            .get(&pubkey1, Some(&ancestors), None)
            .map(|(account_list1, index1)| account_list1.slot_list()[index1].clone())
            .unwrap();
        let (slot2, account_info2) = accounts
            .accounts_index
            .get(&pubkey2, Some(&ancestors), None)
            .map(|(account_list2, index2)| account_list2.slot_list()[index2].clone())
            .unwrap();
        assert_eq!(slot1, 0);
        assert_eq!(slot1, slot2);
        assert_eq!(account_info1.store_id, account_info2.store_id);

        // Update account 1 in slot 1
        accounts.store_uncached(1, &[(&pubkey1, &account)]);

        // Update account 1 as  zero lamports account
        accounts.store_uncached(2, &[(&pubkey1, &zero_lamport_account)]);

        // Pubkey 1 was the only account in slot 1, and it was updated in slot 2, so
        // slot 1 should be purged
        accounts.add_root(0);
        accounts.add_root(1);
        accounts.add_root(2);

        // Slot 1 should be removed, slot 0 cannot be removed because it still has
        // the latest update for pubkey 2
        accounts.clean_accounts(None, false);
        assert!(accounts.storage.get_slot_stores(0).is_some());
        assert!(accounts.storage.get_slot_stores(1).is_none());

        // Slot 1 should be cleaned because all it's accounts are
        // zero lamports, and are not present in any other slot's
        // storage entries
        assert_eq!(accounts.alive_account_count_in_slot(1), 0);
    }

    #[test]
    fn test_clean_multiple_zero_lamport_decrements_index_ref_count() {
        solana_logger::setup();

        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let pubkey2 = solana_sdk::pubkey::new_rand();
        let zero_lamport_account =
            AccountSharedData::new(0, 0, AccountSharedData::default().owner());

        // Store 2 accounts in slot 0, then update account 1 in two more slots
        accounts.store_uncached(0, &[(&pubkey1, &zero_lamport_account)]);
        accounts.store_uncached(0, &[(&pubkey2, &zero_lamport_account)]);
        accounts.store_uncached(1, &[(&pubkey1, &zero_lamport_account)]);
        accounts.store_uncached(2, &[(&pubkey1, &zero_lamport_account)]);
        // Root all slots
        accounts.add_root(0);
        accounts.add_root(1);
        accounts.add_root(2);

        // Account ref counts should match how many slots they were stored in
        // Account 1 = 3 slots; account 2 = 1 slot
        assert_eq!(accounts.accounts_index.ref_count_from_storage(&pubkey1), 3);
        assert_eq!(accounts.accounts_index.ref_count_from_storage(&pubkey2), 1);

        accounts.clean_accounts(None, false);
        // Slots 0 and 1 should each have been cleaned because all of their
        // accounts are zero lamports
        assert!(accounts.storage.get_slot_stores(0).is_none());
        assert!(accounts.storage.get_slot_stores(1).is_none());
        // Slot 2 only has a zero lamport account as well. But, calc_delete_dependencies()
        // should exclude slot 2 from the clean due to changes in other slots
        assert!(accounts.storage.get_slot_stores(2).is_some());
        // Index ref counts should be consistent with the slot stores. Account 1 ref count
        // should be 1 since slot 2 is the only alive slot; account 2 should have a ref
        // count of 0 due to slot 0 being dead
        assert_eq!(accounts.accounts_index.ref_count_from_storage(&pubkey1), 1);
        assert_eq!(accounts.accounts_index.ref_count_from_storage(&pubkey2), 0);

        accounts.clean_accounts(None, false);
        // Slot 2 will now be cleaned, which will leave account 1 with a ref count of 0
        assert!(accounts.storage.get_slot_stores(2).is_none());
        assert_eq!(accounts.accounts_index.ref_count_from_storage(&pubkey1), 0);
    }

    #[test]
    fn test_clean_zero_lamport_and_old_roots() {
        solana_logger::setup();

        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let pubkey = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        let zero_lamport_account =
            AccountSharedData::new(0, 0, AccountSharedData::default().owner());

        // Store a zero-lamport account
        accounts.store_uncached(0, &[(&pubkey, &account)]);
        accounts.store_uncached(1, &[(&pubkey, &zero_lamport_account)]);

        // Simulate rooting the zero-lamport account, should be a
        // candidate for cleaning
        accounts.add_root(0);
        accounts.add_root(1);

        // Slot 0 should be removed, and
        // zero-lamport account should be cleaned
        accounts.clean_accounts(None, false);

        assert!(accounts.storage.get_slot_stores(0).is_none());
        assert!(accounts.storage.get_slot_stores(1).is_none());

        // Slot 0 should be cleaned because all it's accounts have been
        // updated in the rooted slot 1
        assert_eq!(accounts.alive_account_count_in_slot(0), 0);

        // Slot 1 should be cleaned because all it's accounts are
        // zero lamports, and are not present in any other slot's
        // storage entries
        assert_eq!(accounts.alive_account_count_in_slot(1), 0);

        // zero lamport account, should no longer exist in accounts index
        // because it has been removed
        assert!(accounts.accounts_index.get(&pubkey, None, None).is_none());
    }

    #[test]
    fn test_clean_old_with_normal_account() {
        solana_logger::setup();

        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let pubkey = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        //store an account
        accounts.store_uncached(0, &[(&pubkey, &account)]);
        accounts.store_uncached(1, &[(&pubkey, &account)]);

        // simulate slots are rooted after while
        accounts.get_accounts_delta_hash(0);
        accounts.add_root(0);
        accounts.get_accounts_delta_hash(1);
        accounts.add_root(1);

        //even if rooted, old state isn't cleaned up
        assert_eq!(accounts.alive_account_count_in_slot(0), 1);
        assert_eq!(accounts.alive_account_count_in_slot(1), 1);

        accounts.clean_accounts(None, false);

        //now old state is cleaned up
        assert_eq!(accounts.alive_account_count_in_slot(0), 0);
        assert_eq!(accounts.alive_account_count_in_slot(1), 1);
    }

    #[test]
    fn test_clean_old_with_zero_lamport_account() {
        solana_logger::setup();

        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let pubkey2 = solana_sdk::pubkey::new_rand();
        let normal_account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        let zero_account = AccountSharedData::new(0, 0, AccountSharedData::default().owner());
        //store an account
        accounts.store_uncached(0, &[(&pubkey1, &normal_account)]);
        accounts.store_uncached(1, &[(&pubkey1, &zero_account)]);
        accounts.store_uncached(0, &[(&pubkey2, &normal_account)]);
        accounts.store_uncached(1, &[(&pubkey2, &normal_account)]);

        //simulate slots are rooted after while
        accounts.get_accounts_delta_hash(0);
        accounts.add_root(0);
        accounts.get_accounts_delta_hash(1);
        accounts.add_root(1);

        //even if rooted, old state isn't cleaned up
        assert_eq!(accounts.alive_account_count_in_slot(0), 2);
        assert_eq!(accounts.alive_account_count_in_slot(1), 2);

        accounts.print_accounts_stats("");

        accounts.clean_accounts(None, false);

        //Old state behind zero-lamport account is cleaned up
        assert_eq!(accounts.alive_account_count_in_slot(0), 0);
        assert_eq!(accounts.alive_account_count_in_slot(1), 2);
    }

    #[test]
    fn test_clean_old_with_both_normal_and_zero_lamport_accounts() {
        solana_logger::setup();

        let mut accounts = AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            spl_token_mint_index_enabled(),
            false,
            AccountShrinkThreshold::default(),
        );
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let pubkey2 = solana_sdk::pubkey::new_rand();

        // Set up account to be added to secondary index
        let mint_key = Pubkey::new_unique();
        let mut account_data_with_mint =
            vec![0; inline_spl_token_v2_0::state::Account::get_packed_len()];
        account_data_with_mint[..PUBKEY_BYTES].clone_from_slice(&(mint_key.to_bytes()));

        let mut normal_account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        normal_account.set_owner(inline_spl_token_v2_0::id());
        normal_account.set_data(account_data_with_mint.clone());
        let mut zero_account = AccountSharedData::new(0, 0, AccountSharedData::default().owner());
        zero_account.set_owner(inline_spl_token_v2_0::id());
        zero_account.set_data(account_data_with_mint);

        //store an account
        accounts.store_uncached(0, &[(&pubkey1, &normal_account)]);
        accounts.store_uncached(0, &[(&pubkey1, &normal_account)]);
        accounts.store_uncached(1, &[(&pubkey1, &zero_account)]);
        accounts.store_uncached(0, &[(&pubkey2, &normal_account)]);
        accounts.store_uncached(2, &[(&pubkey2, &normal_account)]);

        //simulate slots are rooted after while
        accounts.get_accounts_delta_hash(0);
        accounts.add_root(0);
        accounts.get_accounts_delta_hash(1);
        accounts.add_root(1);
        accounts.get_accounts_delta_hash(2);
        accounts.add_root(2);

        //even if rooted, old state isn't cleaned up
        assert_eq!(accounts.alive_account_count_in_slot(0), 2);
        assert_eq!(accounts.alive_account_count_in_slot(1), 1);
        assert_eq!(accounts.alive_account_count_in_slot(2), 1);

        // Secondary index should still find both pubkeys
        let mut found_accounts = HashSet::new();
        let index_key = IndexKey::SplTokenMint(mint_key);
        let bank_id = 0;
        accounts
            .accounts_index
            .index_scan_accounts(&Ancestors::default(), bank_id, index_key, |key, _| {
                found_accounts.insert(*key);
            })
            .unwrap();
        assert_eq!(found_accounts.len(), 2);
        assert!(found_accounts.contains(&pubkey1));
        assert!(found_accounts.contains(&pubkey2));

        {
            accounts.account_indexes.keys = Some(AccountSecondaryIndexesIncludeExclude {
                exclude: true,
                keys: [mint_key].iter().cloned().collect::<HashSet<Pubkey>>(),
            });
            // Secondary index can't be used - do normal scan: should still find both pubkeys
            let found_accounts = accounts
                .index_scan_accounts(
                    &Ancestors::default(),
                    bank_id,
                    index_key,
                    |collection: &mut HashSet<Pubkey>, account| {
                        collection.insert(*account.unwrap().0);
                    },
                )
                .unwrap();
            assert!(!found_accounts.1);
            assert_eq!(found_accounts.0.len(), 2);
            assert!(found_accounts.0.contains(&pubkey1));
            assert!(found_accounts.0.contains(&pubkey2));

            accounts.account_indexes.keys = None;

            // Secondary index can now be used since it isn't marked as excluded
            let found_accounts = accounts
                .index_scan_accounts(
                    &Ancestors::default(),
                    bank_id,
                    index_key,
                    |collection: &mut HashSet<Pubkey>, account| {
                        collection.insert(*account.unwrap().0);
                    },
                )
                .unwrap();
            assert!(found_accounts.1);
            assert_eq!(found_accounts.0.len(), 2);
            assert!(found_accounts.0.contains(&pubkey1));
            assert!(found_accounts.0.contains(&pubkey2));

            accounts.account_indexes.keys = None;
        }

        accounts.clean_accounts(None, false);

        //both zero lamport and normal accounts are cleaned up
        assert_eq!(accounts.alive_account_count_in_slot(0), 0);
        // The only store to slot 1 was a zero lamport account, should
        // be purged by zero-lamport cleaning logic because slot 1 is
        // rooted
        assert_eq!(accounts.alive_account_count_in_slot(1), 0);
        assert_eq!(accounts.alive_account_count_in_slot(2), 1);

        // `pubkey1`, a zero lamport account, should no longer exist in accounts index
        // because it has been removed by the clean
        assert!(accounts.accounts_index.get(&pubkey1, None, None).is_none());

        // Secondary index should have purged `pubkey1` as well
        let mut found_accounts = vec![];
        accounts
            .accounts_index
            .index_scan_accounts(
                &Ancestors::default(),
                bank_id,
                IndexKey::SplTokenMint(mint_key),
                |key, _| found_accounts.push(*key),
            )
            .unwrap();
        assert_eq!(found_accounts, vec![pubkey2]);
    }

    #[test]
    fn test_clean_max_slot_zero_lamport_account() {
        solana_logger::setup();

        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let pubkey = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        let zero_account = AccountSharedData::new(0, 0, AccountSharedData::default().owner());

        // store an account, make it a zero lamport account
        // in slot 1
        accounts.store_uncached(0, &[(&pubkey, &account)]);
        accounts.store_uncached(1, &[(&pubkey, &zero_account)]);

        // simulate slots are rooted after while
        accounts.add_root(0);
        accounts.add_root(1);

        // Only clean up to account 0, should not purge slot 0 based on
        // updates in later slots in slot 1
        assert_eq!(accounts.alive_account_count_in_slot(0), 1);
        assert_eq!(accounts.alive_account_count_in_slot(1), 1);
        accounts.clean_accounts(Some(0), false);
        assert_eq!(accounts.alive_account_count_in_slot(0), 1);
        assert_eq!(accounts.alive_account_count_in_slot(1), 1);
        assert!(accounts.accounts_index.get(&pubkey, None, None).is_some());

        // Now the account can be cleaned up
        accounts.clean_accounts(Some(1), false);
        assert_eq!(accounts.alive_account_count_in_slot(0), 0);
        assert_eq!(accounts.alive_account_count_in_slot(1), 0);

        // The zero lamport account, should no longer exist in accounts index
        // because it has been removed
        assert!(accounts.accounts_index.get(&pubkey, None, None).is_none());
    }

    #[test]
    fn test_uncleaned_roots_with_account() {
        solana_logger::setup();

        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let pubkey = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        //store an account
        accounts.store_uncached(0, &[(&pubkey, &account)]);
        assert_eq!(accounts.accounts_index.uncleaned_roots_len(), 0);

        // simulate slots are rooted after while
        accounts.add_root(0);
        assert_eq!(accounts.accounts_index.uncleaned_roots_len(), 1);

        //now uncleaned roots are cleaned up
        accounts.clean_accounts(None, false);
        assert_eq!(accounts.accounts_index.uncleaned_roots_len(), 0);
    }

    #[test]
    fn test_uncleaned_roots_with_no_account() {
        solana_logger::setup();

        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);

        assert_eq!(accounts.accounts_index.uncleaned_roots_len(), 0);

        // simulate slots are rooted after while
        accounts.add_root(0);
        assert_eq!(accounts.accounts_index.uncleaned_roots_len(), 1);

        //now uncleaned roots are cleaned up
        accounts.clean_accounts(None, false);
        assert_eq!(accounts.accounts_index.uncleaned_roots_len(), 0);
    }

    #[test]
    fn test_accounts_db_serialize1() {
        solana_logger::setup();
        let accounts = AccountsDb::new_single();
        let mut pubkeys: Vec<Pubkey> = vec![];

        // Create 100 accounts in slot 0
        create_account(&accounts, &mut pubkeys, 0, 100, 0, 0);
        accounts.clean_accounts(None, false);
        check_accounts(&accounts, &pubkeys, 0, 100, 1);

        // do some updates to those accounts and re-check
        modify_accounts(&accounts, &pubkeys, 0, 100, 2);
        assert!(check_storage(&accounts, 0, 100));
        check_accounts(&accounts, &pubkeys, 0, 100, 2);
        accounts.get_accounts_delta_hash(0);
        accounts.add_root(0);

        let mut pubkeys1: Vec<Pubkey> = vec![];

        // CREATE SLOT 1
        let latest_slot = 1;

        // Modify the first 10 of the accounts from slot 0 in slot 1
        modify_accounts(&accounts, &pubkeys, latest_slot, 10, 3);
        // Overwrite account 30 from slot 0 with lamports=0 into slot 1.
        // Slot 1 should now have 10 + 1 = 11 accounts
        let account = AccountSharedData::new(0, 0, AccountSharedData::default().owner());
        accounts.store_uncached(latest_slot, &[(&pubkeys[30], &account)]);

        // Create 10 new accounts in slot 1, should now have 11 + 10 = 21
        // accounts
        create_account(&accounts, &mut pubkeys1, latest_slot, 10, 0, 0);

        accounts.get_accounts_delta_hash(latest_slot);
        accounts.add_root(latest_slot);
        assert!(check_storage(&accounts, 1, 21));

        // CREATE SLOT 2
        let latest_slot = 2;
        let mut pubkeys2: Vec<Pubkey> = vec![];

        // Modify first 20 of the accounts from slot 0 in slot 2
        modify_accounts(&accounts, &pubkeys, latest_slot, 20, 4);
        accounts.clean_accounts(None, false);
        // Overwrite account 31 from slot 0 with lamports=0 into slot 2.
        // Slot 2 should now have 20 + 1 = 21 accounts
        let account = AccountSharedData::new(0, 0, AccountSharedData::default().owner());
        accounts.store_uncached(latest_slot, &[(&pubkeys[31], &account)]);

        // Create 10 new accounts in slot 2. Slot 2 should now have
        // 21 + 10 = 31 accounts
        create_account(&accounts, &mut pubkeys2, latest_slot, 10, 0, 0);

        accounts.get_accounts_delta_hash(latest_slot);
        accounts.add_root(latest_slot);
        assert!(check_storage(&accounts, 2, 31));

        accounts.clean_accounts(None, false);
        // The first 20 accounts of slot 0 have been updated in slot 2, as well as
        // accounts 30 and  31 (overwritten with zero-lamport accounts in slot 1 and
        // slot 2 respectively), so only 78 accounts are left in slot 0's storage entries.
        assert!(check_storage(&accounts, 0, 78));
        // 10 of the 21 accounts have been modified in slot 2, so only 11
        // accounts left in slot 1.
        assert!(check_storage(&accounts, 1, 11));
        assert!(check_storage(&accounts, 2, 31));

        let daccounts = reconstruct_accounts_db_via_serialization(&accounts, latest_slot);

        assert_eq!(
            daccounts.write_version.load(Ordering::Relaxed),
            accounts.write_version.load(Ordering::Relaxed)
        );

        assert_eq!(
            daccounts.next_id.load(Ordering::Relaxed),
            accounts.next_id.load(Ordering::Relaxed)
        );

        // Get the hash for the latest slot, which should be the only hash in the
        // bank_hashes map on the deserialized AccountsDb
        assert_eq!(daccounts.bank_hashes.read().unwrap().len(), 2);
        assert_eq!(
            daccounts.bank_hashes.read().unwrap().get(&latest_slot),
            accounts.bank_hashes.read().unwrap().get(&latest_slot)
        );

        daccounts.print_count_and_status("daccounts");

        // Don't check the first 35 accounts which have not been modified on slot 0
        check_accounts(&daccounts, &pubkeys[35..], 0, 65, 37);
        check_accounts(&daccounts, &pubkeys1, 1, 10, 1);
        assert!(check_storage(&daccounts, 0, 100));
        assert!(check_storage(&daccounts, 1, 21));
        assert!(check_storage(&daccounts, 2, 31));

        let ancestors = linear_ancestors(latest_slot);
        assert_eq!(
            daccounts.update_accounts_hash(latest_slot, &ancestors),
            accounts.update_accounts_hash(latest_slot, &ancestors)
        );
    }

    fn assert_load_account(
        accounts: &AccountsDb,
        slot: Slot,
        pubkey: Pubkey,
        expected_lamports: u64,
    ) {
        let ancestors = vec![(slot, 0)].into_iter().collect();
        let (account, slot) = accounts
            .load_without_fixed_root(&ancestors, &pubkey)
            .unwrap();
        assert_eq!((account.lamports(), slot), (expected_lamports, slot));
    }

    fn assert_not_load_account(accounts: &AccountsDb, slot: Slot, pubkey: Pubkey) {
        let ancestors = vec![(slot, 0)].into_iter().collect();
        assert!(accounts
            .load_without_fixed_root(&ancestors, &pubkey)
            .is_none());
    }

    fn reconstruct_accounts_db_via_serialization(accounts: &AccountsDb, slot: Slot) -> AccountsDb {
        let daccounts =
            crate::serde_snapshot::reconstruct_accounts_db_via_serialization(accounts, slot);
        daccounts.print_count_and_status("daccounts");
        daccounts
    }

    fn assert_no_stores(accounts: &AccountsDb, slot: Slot) {
        let slot_stores = accounts.storage.get_slot_stores(slot);
        let r_slot_stores = slot_stores.as_ref().map(|slot_stores| {
            let r_slot_stores = slot_stores.read().unwrap();
            info!("{:?}", *r_slot_stores);
            r_slot_stores
        });
        assert!(r_slot_stores.is_none() || r_slot_stores.unwrap().is_empty());
    }

    #[test]
    fn test_accounts_db_purge_keep_live() {
        solana_logger::setup();
        let some_lamport = 223;
        let zero_lamport = 0;
        let no_data = 0;
        let owner = *AccountSharedData::default().owner();

        let account = AccountSharedData::new(some_lamport, no_data, &owner);
        let pubkey = solana_sdk::pubkey::new_rand();

        let account2 = AccountSharedData::new(some_lamport, no_data, &owner);
        let pubkey2 = solana_sdk::pubkey::new_rand();

        let zero_lamport_account = AccountSharedData::new(zero_lamport, no_data, &owner);

        let accounts = AccountsDb::new_single();
        accounts.add_root(0);

        // Step A
        let mut current_slot = 1;
        accounts.store_uncached(current_slot, &[(&pubkey, &account)]);
        // Store another live account to slot 1 which will prevent any purge
        // since the store count will not be zero
        accounts.store_uncached(current_slot, &[(&pubkey2, &account2)]);
        accounts.add_root(current_slot);
        let (slot1, account_info1) = accounts
            .accounts_index
            .get(&pubkey, None, None)
            .map(|(account_list1, index1)| account_list1.slot_list()[index1].clone())
            .unwrap();
        let (slot2, account_info2) = accounts
            .accounts_index
            .get(&pubkey2, None, None)
            .map(|(account_list2, index2)| account_list2.slot_list()[index2].clone())
            .unwrap();
        assert_eq!(slot1, current_slot);
        assert_eq!(slot1, slot2);
        assert_eq!(account_info1.store_id, account_info2.store_id);

        // Step B
        current_slot += 1;
        let zero_lamport_slot = current_slot;
        accounts.store_uncached(current_slot, &[(&pubkey, &zero_lamport_account)]);
        accounts.add_root(current_slot);

        assert_load_account(&accounts, current_slot, pubkey, zero_lamport);

        current_slot += 1;
        accounts.add_root(current_slot);

        accounts.print_accounts_stats("pre_purge");

        accounts.clean_accounts(None, false);

        accounts.print_accounts_stats("post_purge");

        // The earlier entry for pubkey in the account index is purged,
        let (slot_list_len, index_slot) = {
            let account_entry = accounts
                .accounts_index
                .get_account_read_entry(&pubkey)
                .unwrap();
            let slot_list = account_entry.slot_list();
            (slot_list.len(), slot_list[0].0)
        };
        assert_eq!(slot_list_len, 1);
        // Zero lamport entry was not the one purged
        assert_eq!(index_slot, zero_lamport_slot);
        // The ref count should still be 2 because no slots were purged
        assert_eq!(accounts.ref_count_for_pubkey(&pubkey), 2);

        // storage for slot 1 had 2 accounts, now has 1 after pubkey 1
        // was reclaimed
        check_storage(&accounts, 1, 1);
        // storage for slot 2 had 1 accounts, now has 1
        check_storage(&accounts, 2, 1);
    }

    #[test]
    fn test_accounts_db_purge1() {
        solana_logger::setup();
        let some_lamport = 223;
        let zero_lamport = 0;
        let no_data = 0;
        let owner = *AccountSharedData::default().owner();

        let account = AccountSharedData::new(some_lamport, no_data, &owner);
        let pubkey = solana_sdk::pubkey::new_rand();

        let zero_lamport_account = AccountSharedData::new(zero_lamport, no_data, &owner);

        let accounts = AccountsDb::new_single();
        accounts.add_root(0);

        let mut current_slot = 1;
        accounts.set_hash(current_slot, current_slot - 1);
        accounts.store_uncached(current_slot, &[(&pubkey, &account)]);
        accounts.add_root(current_slot);

        current_slot += 1;
        accounts.set_hash(current_slot, current_slot - 1);
        accounts.store_uncached(current_slot, &[(&pubkey, &zero_lamport_account)]);
        accounts.add_root(current_slot);

        assert_load_account(&accounts, current_slot, pubkey, zero_lamport);

        // Otherwise slot 2 will not be removed
        current_slot += 1;
        accounts.set_hash(current_slot, current_slot - 1);
        accounts.add_root(current_slot);

        accounts.print_accounts_stats("pre_purge");

        let ancestors = linear_ancestors(current_slot);
        info!("ancestors: {:?}", ancestors);
        let hash = accounts.update_accounts_hash_test(current_slot, &ancestors);

        accounts.clean_accounts(None, false);

        assert_eq!(
            accounts.update_accounts_hash_test(current_slot, &ancestors),
            hash
        );

        accounts.print_accounts_stats("post_purge");

        // Make sure the index is for pubkey cleared
        assert!(accounts
            .accounts_index
            .get_account_read_entry(&pubkey)
            .is_none());

        // slot 1 & 2 should not have any stores
        assert_no_stores(&accounts, 1);
        assert_no_stores(&accounts, 2);
    }

    #[test]
    fn test_accounts_db_serialize_zero_and_free() {
        solana_logger::setup();

        let some_lamport = 223;
        let zero_lamport = 0;
        let no_data = 0;
        let owner = *AccountSharedData::default().owner();

        let account = AccountSharedData::new(some_lamport, no_data, &owner);
        let pubkey = solana_sdk::pubkey::new_rand();
        let zero_lamport_account = AccountSharedData::new(zero_lamport, no_data, &owner);

        let account2 = AccountSharedData::new(some_lamport + 1, no_data, &owner);
        let pubkey2 = solana_sdk::pubkey::new_rand();

        let filler_account = AccountSharedData::new(some_lamport, no_data, &owner);
        let filler_account_pubkey = solana_sdk::pubkey::new_rand();

        let accounts = AccountsDb::new_single();

        let mut current_slot = 1;
        accounts.store_uncached(current_slot, &[(&pubkey, &account)]);
        accounts.add_root(current_slot);

        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&pubkey, &zero_lamport_account)]);
        accounts.store_uncached(current_slot, &[(&pubkey2, &account2)]);

        // Store enough accounts such that an additional store for slot 2 is created.
        while accounts
            .storage
            .get_slot_stores(current_slot)
            .unwrap()
            .read()
            .unwrap()
            .len()
            < 2
        {
            accounts.store_uncached(current_slot, &[(&filler_account_pubkey, &filler_account)]);
        }
        accounts.add_root(current_slot);

        assert_load_account(&accounts, current_slot, pubkey, zero_lamport);

        accounts.print_accounts_stats("accounts");

        accounts.clean_accounts(None, false);

        accounts.print_accounts_stats("accounts_post_purge");
        let accounts = reconstruct_accounts_db_via_serialization(&accounts, current_slot);

        accounts.print_accounts_stats("reconstructed");

        assert_load_account(&accounts, current_slot, pubkey, zero_lamport);
    }

    fn with_chained_zero_lamport_accounts<F>(f: F)
    where
        F: Fn(AccountsDb, Slot) -> AccountsDb,
    {
        let some_lamport = 223;
        let zero_lamport = 0;
        let dummy_lamport = 999;
        let no_data = 0;
        let owner = *AccountSharedData::default().owner();

        let account = AccountSharedData::new(some_lamport, no_data, &owner);
        let account2 = AccountSharedData::new(some_lamport + 100_001, no_data, &owner);
        let account3 = AccountSharedData::new(some_lamport + 100_002, no_data, &owner);
        let zero_lamport_account = AccountSharedData::new(zero_lamport, no_data, &owner);

        let pubkey = solana_sdk::pubkey::new_rand();
        let purged_pubkey1 = solana_sdk::pubkey::new_rand();
        let purged_pubkey2 = solana_sdk::pubkey::new_rand();

        let dummy_account = AccountSharedData::new(dummy_lamport, no_data, &owner);
        let dummy_pubkey = Pubkey::default();

        let accounts = AccountsDb::new_single();

        let mut current_slot = 1;
        accounts.store_uncached(current_slot, &[(&pubkey, &account)]);
        accounts.store_uncached(current_slot, &[(&purged_pubkey1, &account2)]);
        accounts.add_root(current_slot);

        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&purged_pubkey1, &zero_lamport_account)]);
        accounts.store_uncached(current_slot, &[(&purged_pubkey2, &account3)]);
        accounts.add_root(current_slot);

        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&purged_pubkey2, &zero_lamport_account)]);
        accounts.add_root(current_slot);

        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&dummy_pubkey, &dummy_account)]);
        accounts.add_root(current_slot);

        accounts.print_accounts_stats("pre_f");
        accounts.update_accounts_hash(4, &Ancestors::default());

        let accounts = f(accounts, current_slot);

        accounts.print_accounts_stats("post_f");

        assert_load_account(&accounts, current_slot, pubkey, some_lamport);
        assert_load_account(&accounts, current_slot, purged_pubkey1, 0);
        assert_load_account(&accounts, current_slot, purged_pubkey2, 0);
        assert_load_account(&accounts, current_slot, dummy_pubkey, dummy_lamport);

        accounts
            .verify_bank_hash_and_lamports(4, &Ancestors::default(), 1222, true)
            .unwrap();
    }

    #[test]
    fn test_accounts_purge_chained_purge_before_snapshot_restore() {
        solana_logger::setup();
        with_chained_zero_lamport_accounts(|accounts, current_slot| {
            accounts.clean_accounts(None, false);
            reconstruct_accounts_db_via_serialization(&accounts, current_slot)
        });
    }

    #[test]
    fn test_accounts_purge_chained_purge_after_snapshot_restore() {
        solana_logger::setup();
        with_chained_zero_lamport_accounts(|accounts, current_slot| {
            let accounts = reconstruct_accounts_db_via_serialization(&accounts, current_slot);
            accounts.print_accounts_stats("after_reconstruct");
            accounts.clean_accounts(None, false);
            reconstruct_accounts_db_via_serialization(&accounts, current_slot)
        });
    }

    #[test]
    #[ignore]
    fn test_store_account_stress() {
        let slot = 42;
        let num_threads = 2;

        let min_file_bytes = std::mem::size_of::<StoredMeta>()
            + std::mem::size_of::<crate::append_vec::AccountMeta>();

        let db = Arc::new(AccountsDb::new_sized(Vec::new(), min_file_bytes as u64));

        db.add_root(slot);
        let thread_hdls: Vec<_> = (0..num_threads)
            .map(|_| {
                let db = db.clone();
                std::thread::Builder::new()
                    .name("account-writers".to_string())
                    .spawn(move || {
                        let pubkey = solana_sdk::pubkey::new_rand();
                        let mut account = AccountSharedData::new(1, 0, &pubkey);
                        let mut i = 0;
                        loop {
                            let account_bal = thread_rng().gen_range(1, 99);
                            account.set_lamports(account_bal);
                            db.store_uncached(slot, &[(&pubkey, &account)]);

                            let (account, slot) = db
                                .load_without_fixed_root(&Ancestors::default(), &pubkey)
                                .unwrap_or_else(|| {
                                    panic!("Could not fetch stored account {}, iter {}", pubkey, i)
                                });
                            assert_eq!(slot, slot);
                            assert_eq!(account.lamports(), account_bal);
                            i += 1;
                        }
                    })
                    .unwrap()
            })
            .collect();

        for t in thread_hdls {
            t.join().unwrap();
        }
    }

    #[test]
    fn test_accountsdb_scan_accounts() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let key = Pubkey::default();
        let key0 = solana_sdk::pubkey::new_rand();
        let account0 = AccountSharedData::new(1, 0, &key);

        db.store_uncached(0, &[(&key0, &account0)]);

        let key1 = solana_sdk::pubkey::new_rand();
        let account1 = AccountSharedData::new(2, 0, &key);
        db.store_uncached(1, &[(&key1, &account1)]);

        let ancestors = vec![(0, 0)].into_iter().collect();
        let accounts: Vec<AccountSharedData> = db.unchecked_scan_accounts(
            "",
            &ancestors,
            |accounts: &mut Vec<AccountSharedData>, option| {
                accounts.push(option.1.take_account());
            },
        );
        assert_eq!(accounts, vec![account0]);

        let ancestors = vec![(1, 1), (0, 0)].into_iter().collect();
        let accounts: Vec<AccountSharedData> = db.unchecked_scan_accounts(
            "",
            &ancestors,
            |accounts: &mut Vec<AccountSharedData>, option| {
                accounts.push(option.1.take_account());
            },
        );
        assert_eq!(accounts.len(), 2);
    }

    #[test]
    fn test_cleanup_key_not_removed() {
        solana_logger::setup();
        let db = AccountsDb::new_single();

        let key = Pubkey::default();
        let key0 = solana_sdk::pubkey::new_rand();
        let account0 = AccountSharedData::new(1, 0, &key);

        db.store_uncached(0, &[(&key0, &account0)]);

        let key1 = solana_sdk::pubkey::new_rand();
        let account1 = AccountSharedData::new(2, 0, &key);
        db.store_uncached(1, &[(&key1, &account1)]);

        db.print_accounts_stats("pre");

        let slots: HashSet<Slot> = vec![1].into_iter().collect();
        let purge_keys = vec![(key1, slots)];
        db.purge_keys_exact(purge_keys.iter());

        let account2 = AccountSharedData::new(3, 0, &key);
        db.store_uncached(2, &[(&key1, &account2)]);

        db.print_accounts_stats("post");
        let ancestors = vec![(2, 0)].into_iter().collect();
        assert_eq!(
            db.load_without_fixed_root(&ancestors, &key1)
                .unwrap()
                .0
                .lamports(),
            3
        );
    }

    #[test]
    fn test_store_large_account() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = Pubkey::default();
        let data_len = DEFAULT_FILE_SIZE as usize + 7;
        let account = AccountSharedData::new(1, data_len, &key);

        db.store_uncached(0, &[(&key, &account)]);

        let ancestors = vec![(0, 0)].into_iter().collect();
        let ret = db.load_without_fixed_root(&ancestors, &key).unwrap();
        assert_eq!(ret.0.data().len(), data_len);
    }

    #[test]
    fn test_hash_frozen_account_data() {
        let account = AccountSharedData::new(1, 42, &Pubkey::default());

        let hash = AccountsDb::hash_frozen_account_data(&account);
        assert_ne!(hash, Hash::default()); // Better not be the default Hash

        // Lamports changes to not affect the hash
        let mut account_modified = account.clone();
        account_modified.checked_sub_lamports(1).unwrap();
        assert_eq!(
            hash,
            AccountsDb::hash_frozen_account_data(&account_modified)
        );

        // Rent epoch may changes to not affect the hash
        let mut account_modified = account.clone();
        account_modified.set_rent_epoch(account_modified.rent_epoch() + 1);
        assert_eq!(
            hash,
            AccountsDb::hash_frozen_account_data(&account_modified)
        );

        // Account data may not be modified
        let mut account_modified = account.clone();
        account_modified.data_as_mut_slice()[0] = 42;
        assert_ne!(
            hash,
            AccountsDb::hash_frozen_account_data(&account_modified)
        );

        // Owner may not be modified
        let mut account_modified = account.clone();
        account_modified
            .set_owner(Pubkey::from_str("My11111111111111111111111111111111111111111").unwrap());
        assert_ne!(
            hash,
            AccountsDb::hash_frozen_account_data(&account_modified)
        );

        // Executable may not be modified
        let mut account_modified = account;
        account_modified.set_executable(true);
        assert_ne!(
            hash,
            AccountsDb::hash_frozen_account_data(&account_modified)
        );
    }

    #[test]
    fn test_frozen_account_lamport_increase() {
        let frozen_pubkey =
            Pubkey::from_str("My11111111111111111111111111111111111111111").unwrap();
        let mut db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let mut account = AccountSharedData::new(1, 42, &frozen_pubkey);
        db.store_uncached(0, &[(&frozen_pubkey, &account)]);

        let ancestors = vec![(0, 0)].into_iter().collect();
        db.freeze_accounts(&ancestors, &[frozen_pubkey]);

        // Store with no account changes is ok
        db.store_uncached(0, &[(&frozen_pubkey, &account)]);

        // Store with an increase in lamports is ok
        account.set_lamports(2);
        db.store_uncached(0, &[(&frozen_pubkey, &account)]);

        // Store with an decrease that does not go below the frozen amount of lamports is tolerated
        account.set_lamports(1);
        db.store_uncached(0, &[(&frozen_pubkey, &account)]);

        // A store of any value over the frozen value of '1' across different slots is also ok
        account.set_lamports(3);
        db.store_uncached(1, &[(&frozen_pubkey, &account)]);
        account.set_lamports(2);
        db.store_uncached(2, &[(&frozen_pubkey, &account)]);
        account.set_lamports(1);
        db.store_uncached(3, &[(&frozen_pubkey, &account)]);
    }

    #[test]
    #[should_panic(
        expected = "Frozen account My11111111111111111111111111111111111111111 modified.  Lamports decreased from 1 to 0"
    )]
    fn test_frozen_account_lamport_decrease() {
        let frozen_pubkey =
            Pubkey::from_str("My11111111111111111111111111111111111111111").unwrap();
        let mut db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let mut account = AccountSharedData::new(1, 42, &frozen_pubkey);
        db.store_uncached(0, &[(&frozen_pubkey, &account)]);

        let ancestors = vec![(0, 0)].into_iter().collect();
        db.freeze_accounts(&ancestors, &[frozen_pubkey]);

        // Store with a decrease below the frozen amount of lamports is not ok
        account.checked_sub_lamports(1).unwrap();
        db.store_uncached(0, &[(&frozen_pubkey, &account)]);
    }

    #[test]
    #[should_panic(
        expected = "Unable to freeze an account that does not exist: My11111111111111111111111111111111111111111"
    )]
    fn test_frozen_account_nonexistent() {
        let frozen_pubkey =
            Pubkey::from_str("My11111111111111111111111111111111111111111").unwrap();
        let mut db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let ancestors = vec![(0, 0)].into_iter().collect();
        db.freeze_accounts(&ancestors, &[frozen_pubkey]);
    }

    #[test]
    #[should_panic(
        expected = "Frozen account My11111111111111111111111111111111111111111 modified.  Hash changed from 8wHcxDkjiwdrkPAsDnmNrF1UDGJFAtZzPQBSVweY3yRA to JdscGYB1uczVssmYuJusDD1Bfe6wpNeeho8XjcH8inN"
    )]
    fn test_frozen_account_data_modified() {
        let frozen_pubkey =
            Pubkey::from_str("My11111111111111111111111111111111111111111").unwrap();
        let mut db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let mut account = AccountSharedData::new(1, 42, &frozen_pubkey);
        db.store_uncached(0, &[(&frozen_pubkey, &account)]);

        let ancestors = vec![(0, 0)].into_iter().collect();
        db.freeze_accounts(&ancestors, &[frozen_pubkey]);

        account.data_as_mut_slice()[0] = 42;
        db.store_uncached(0, &[(&frozen_pubkey, &account)]);
    }

    #[test]
    fn test_stored_readable_account() {
        let lamports = 1;
        let owner = Pubkey::new_unique();
        let executable = true;
        let rent_epoch = 2;
        let meta = StoredMeta {
            write_version: 5,
            pubkey: Pubkey::new_unique(),
            data_len: 7,
        };
        let account_meta = AccountMeta {
            lamports,
            owner,
            executable,
            rent_epoch,
        };
        let data = Vec::new();
        let account = Account {
            lamports,
            owner,
            executable,
            rent_epoch,
            data: data.clone(),
        };
        let offset = 99;
        let stored_size = 101;
        let hash = Hash::new_unique();
        let stored_account = StoredAccountMeta {
            meta: &meta,
            account_meta: &account_meta,
            data: &data,
            offset,
            stored_size,
            hash: &hash,
        };
        assert!(accounts_equal(&account, &stored_account));
    }

    #[test]
    fn test_hash_stored_account() {
        // This test uses some UNSAFE trick to detect most of account's field
        // addition and deletion without changing the hash code

        const ACCOUNT_DATA_LEN: usize = 3;
        // the type of InputTuple elements must not contain references;
        // they should be simple scalars or data blobs
        type InputTuple = (
            Slot,
            StoredMeta,
            AccountMeta,
            [u8; ACCOUNT_DATA_LEN],
            usize, // for StoredAccountMeta::offset
            Hash,
        );
        const INPUT_LEN: usize = std::mem::size_of::<InputTuple>();
        type InputBlob = [u8; INPUT_LEN];
        let mut blob: InputBlob = [0u8; INPUT_LEN];

        // spray memory with decreasing counts so that, data layout can be detected.
        for (i, byte) in blob.iter_mut().enumerate() {
            *byte = (INPUT_LEN - i) as u8;
        }

        //UNSAFE: forcibly cast the special byte pattern to actual account fields.
        let (slot, meta, account_meta, data, offset, hash): InputTuple =
            unsafe { std::mem::transmute::<InputBlob, InputTuple>(blob) };

        let stored_account = StoredAccountMeta {
            meta: &meta,
            account_meta: &account_meta,
            data: &data,
            offset,
            stored_size: CACHE_VIRTUAL_STORED_SIZE,
            hash: &hash,
        };
        let account = stored_account.clone_account();

        let expected_account_hash = if cfg!(debug_assertions) {
            Hash::from_str("4StuvYHFd7xuShVXB94uHHvpqGMCaacdZnYB74QQkPA1").unwrap()
        } else {
            Hash::from_str("33ruy7m3Xto7irYfsBSN74aAzQwCQxsfoZxXuZy2Rra3").unwrap()
        };

        assert_eq!(
            AccountsDb::hash_stored_account(slot, &stored_account),
            expected_account_hash,
            "StoredAccountMeta's data layout might be changed; update hashing if needed."
        );
        assert_eq!(
            AccountsDb::hash_account(slot, &account, &stored_account.meta.pubkey),
            expected_account_hash,
            "Account-based hashing must be consistent with StoredAccountMeta-based one."
        );
    }

    #[test]
    fn test_bank_hash_stats() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = Pubkey::default();
        let some_data_len = 5;
        let some_slot: Slot = 0;
        let account = AccountSharedData::new(1, some_data_len, &key);
        let ancestors = vec![(some_slot, 0)].into_iter().collect();

        db.store_uncached(some_slot, &[(&key, &account)]);
        let mut account = db.load_without_fixed_root(&ancestors, &key).unwrap().0;
        account.checked_sub_lamports(1).unwrap();
        account.set_executable(true);
        db.store_uncached(some_slot, &[(&key, &account)]);
        db.add_root(some_slot);

        let bank_hashes = db.bank_hashes.read().unwrap();
        let bank_hash = bank_hashes.get(&some_slot).unwrap();
        assert_eq!(bank_hash.stats.num_updated_accounts, 1);
        assert_eq!(bank_hash.stats.num_removed_accounts, 1);
        assert_eq!(bank_hash.stats.num_lamports_stored, 1);
        assert_eq!(bank_hash.stats.total_data_len, 2 * some_data_len as u64);
        assert_eq!(bank_hash.stats.num_executable_accounts, 1);
    }

    #[test]
    fn test_calculate_accounts_hash_check_hash_mismatch() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = solana_sdk::pubkey::new_rand();
        let some_data_len = 0;
        let some_slot: Slot = 0;
        let account = AccountSharedData::new(1, some_data_len, &key);

        let ancestors = vec![(some_slot, 0)].into_iter().collect();

        // put wrong hash value in store so we get a mismatch
        db.store_accounts_unfrozen(
            some_slot,
            &[(&key, &account)],
            Some(&[&Hash::default()]),
            false,
        );
        db.add_root(some_slot);
        let check_hash = true;
        assert!(db
            .calculate_accounts_hash_helper(false, some_slot, &ancestors, check_hash, false)
            .is_err());
        assert!(db
            .calculate_accounts_hash_helper(true, some_slot, &ancestors, check_hash, false)
            .is_err());
    }

    #[test]
    fn test_calculate_accounts_hash_check_hash() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = solana_sdk::pubkey::new_rand();
        let some_data_len = 0;
        let some_slot: Slot = 0;
        let account = AccountSharedData::new(1, some_data_len, &key);

        let ancestors = vec![(some_slot, 0)].into_iter().collect();

        db.store_uncached(some_slot, &[(&key, &account)]);
        db.add_root(some_slot);
        let check_hash = true;
        assert_eq!(
            db.calculate_accounts_hash_helper(false, some_slot, &ancestors, check_hash, false)
                .unwrap(),
            db.calculate_accounts_hash_helper(true, some_slot, &ancestors, check_hash, false)
                .unwrap(),
        );
    }

    #[test]
    fn test_verify_bank_hash() {
        use BankHashVerificationError::*;
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = solana_sdk::pubkey::new_rand();
        let some_data_len = 0;
        let some_slot: Slot = 0;
        let account = AccountSharedData::new(1, some_data_len, &key);
        let ancestors = vec![(some_slot, 0)].into_iter().collect();

        db.store_uncached(some_slot, &[(&key, &account)]);
        db.add_root(some_slot);
        db.update_accounts_hash_test(some_slot, &ancestors);
        assert_matches!(
            db.verify_bank_hash_and_lamports(some_slot, &ancestors, 1, true),
            Ok(_)
        );

        db.bank_hashes.write().unwrap().remove(&some_slot).unwrap();
        assert_matches!(
            db.verify_bank_hash_and_lamports(some_slot, &ancestors, 1, true),
            Err(MissingBankHash)
        );

        let some_bank_hash = Hash::new(&[0xca; HASH_BYTES]);
        let bank_hash_info = BankHashInfo {
            hash: some_bank_hash,
            snapshot_hash: Hash::new(&[0xca; HASH_BYTES]),
            stats: BankHashStats::default(),
        };
        db.bank_hashes
            .write()
            .unwrap()
            .insert(some_slot, bank_hash_info);
        assert_matches!(
            db.verify_bank_hash_and_lamports(some_slot, &ancestors, 1, true),
            Err(MismatchedBankHash)
        );
    }

    #[test]
    fn test_verify_bank_capitalization() {
        use BankHashVerificationError::*;
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = solana_sdk::pubkey::new_rand();
        let some_data_len = 0;
        let some_slot: Slot = 0;
        let account = AccountSharedData::new(1, some_data_len, &key);
        let ancestors = vec![(some_slot, 0)].into_iter().collect();

        db.store_uncached(some_slot, &[(&key, &account)]);
        db.add_root(some_slot);
        db.update_accounts_hash_test(some_slot, &ancestors);
        assert_matches!(
            db.verify_bank_hash_and_lamports(some_slot, &ancestors, 1, true),
            Ok(_)
        );

        let native_account_pubkey = solana_sdk::pubkey::new_rand();
        db.store_uncached(
            some_slot,
            &[(
                &native_account_pubkey,
                &solana_sdk::native_loader::create_loadable_account_for_test("foo"),
            )],
        );
        db.update_accounts_hash_test(some_slot, &ancestors);
        assert_matches!(
            db.verify_bank_hash_and_lamports(some_slot, &ancestors, 2, true),
            Ok(_)
        );

        assert_matches!(
            db.verify_bank_hash_and_lamports(some_slot, &ancestors, 10, true),
            Err(MismatchedTotalLamports(expected, actual)) if expected == 2 && actual == 10
        );
    }

    #[test]
    fn test_verify_bank_hash_no_account() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let some_slot: Slot = 0;
        let ancestors = vec![(some_slot, 0)].into_iter().collect();

        db.bank_hashes
            .write()
            .unwrap()
            .insert(some_slot, BankHashInfo::default());
        db.add_root(some_slot);
        db.update_accounts_hash_test(some_slot, &ancestors);
        assert_matches!(
            db.verify_bank_hash_and_lamports(some_slot, &ancestors, 0, true),
            Ok(_)
        );
    }

    #[test]
    fn test_verify_bank_hash_bad_account_hash() {
        use BankHashVerificationError::*;
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = Pubkey::default();
        let some_data_len = 0;
        let some_slot: Slot = 0;
        let account = AccountSharedData::new(1, some_data_len, &key);
        let ancestors = vec![(some_slot, 0)].into_iter().collect();

        let accounts = &[(&key, &account)];
        // update AccountsDb's bank hash
        {
            let mut bank_hashes = db.bank_hashes.write().unwrap();
            bank_hashes
                .entry(some_slot)
                .or_insert_with(BankHashInfo::default);
        }
        // provide bogus account hashes
        let some_hash = Hash::new(&[0xca; HASH_BYTES]);
        db.store_accounts_unfrozen(some_slot, accounts, Some(&[&some_hash]), false);
        db.add_root(some_slot);
        assert_matches!(
            db.verify_bank_hash_and_lamports(some_slot, &ancestors, 1, true),
            Err(MismatchedAccountHash)
        );
    }

    #[test]
    fn test_storage_finder() {
        solana_logger::setup();
        let db = AccountsDb::new_sized(Vec::new(), 16 * 1024);
        let key = solana_sdk::pubkey::new_rand();
        let lamports = 100;
        let data_len = 8190;
        let account = AccountSharedData::new(lamports, data_len, &solana_sdk::pubkey::new_rand());
        // pre-populate with a smaller empty store
        db.create_and_insert_store(1, 8192, "test_storage_finder");
        db.store_uncached(1, &[(&key, &account)]);
    }

    #[test]
    fn test_get_snapshot_storages_empty() {
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        assert!(db.get_snapshot_storages(0, None).0.is_empty());
    }

    #[test]
    fn test_get_snapshot_storages_only_older_than_or_equal_to_snapshot_slot() {
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = Pubkey::default();
        let account = AccountSharedData::new(1, 0, &key);
        let before_slot = 0;
        let base_slot = before_slot + 1;
        let after_slot = base_slot + 1;

        db.add_root(base_slot);
        db.store_uncached(base_slot, &[(&key, &account)]);
        assert!(db.get_snapshot_storages(before_slot, None).0.is_empty());

        assert_eq!(1, db.get_snapshot_storages(base_slot, None).0.len());
        assert_eq!(1, db.get_snapshot_storages(after_slot, None).0.len());
    }

    #[test]
    fn test_get_snapshot_storages_only_non_empty() {
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = Pubkey::default();
        let account = AccountSharedData::new(1, 0, &key);
        let base_slot = 0;
        let after_slot = base_slot + 1;

        db.store_uncached(base_slot, &[(&key, &account)]);
        db.storage
            .get_slot_stores(base_slot)
            .unwrap()
            .write()
            .unwrap()
            .clear();
        db.add_root(base_slot);
        assert!(db.get_snapshot_storages(after_slot, None).0.is_empty());

        db.store_uncached(base_slot, &[(&key, &account)]);
        assert_eq!(1, db.get_snapshot_storages(after_slot, None).0.len());
    }

    #[test]
    fn test_get_snapshot_storages_only_roots() {
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = Pubkey::default();
        let account = AccountSharedData::new(1, 0, &key);
        let base_slot = 0;
        let after_slot = base_slot + 1;

        db.store_uncached(base_slot, &[(&key, &account)]);
        assert!(db.get_snapshot_storages(after_slot, None).0.is_empty());

        db.add_root(base_slot);
        assert_eq!(1, db.get_snapshot_storages(after_slot, None).0.len());
    }

    #[test]
    fn test_get_snapshot_storages_exclude_empty() {
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let key = Pubkey::default();
        let account = AccountSharedData::new(1, 0, &key);
        let base_slot = 0;
        let after_slot = base_slot + 1;

        db.store_uncached(base_slot, &[(&key, &account)]);
        db.add_root(base_slot);
        assert_eq!(1, db.get_snapshot_storages(after_slot, None).0.len());

        db.storage
            .get_slot_stores(0)
            .unwrap()
            .read()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .remove_account(0, true);
        assert!(db.get_snapshot_storages(after_slot, None).0.is_empty());
    }

    #[test]
    #[should_panic(expected = "double remove of account in slot: 0/store: 0!!")]
    fn test_storage_remove_account_double_remove() {
        let accounts = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let pubkey = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        accounts.store_uncached(0, &[(&pubkey, &account)]);
        let storage_entry = accounts
            .storage
            .get_slot_stores(0)
            .unwrap()
            .read()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .clone();
        storage_entry.remove_account(0, true);
        storage_entry.remove_account(0, true);
    }

    #[test]
    fn test_accounts_purge_long_chained_after_snapshot_restore() {
        solana_logger::setup();
        let old_lamport = 223;
        let zero_lamport = 0;
        let no_data = 0;
        let owner = *AccountSharedData::default().owner();

        let account = AccountSharedData::new(old_lamport, no_data, &owner);
        let account2 = AccountSharedData::new(old_lamport + 100_001, no_data, &owner);
        let account3 = AccountSharedData::new(old_lamport + 100_002, no_data, &owner);
        let dummy_account = AccountSharedData::new(99_999_999, no_data, &owner);
        let zero_lamport_account = AccountSharedData::new(zero_lamport, no_data, &owner);

        let pubkey = solana_sdk::pubkey::new_rand();
        let dummy_pubkey = solana_sdk::pubkey::new_rand();
        let purged_pubkey1 = solana_sdk::pubkey::new_rand();
        let purged_pubkey2 = solana_sdk::pubkey::new_rand();

        let mut current_slot = 0;
        let accounts = AccountsDb::new_single();

        // create intermediate updates to purged_pubkey1 so that
        // generate_index must add slots as root last at once
        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&pubkey, &account)]);
        accounts.store_uncached(current_slot, &[(&purged_pubkey1, &account2)]);
        accounts.add_root(current_slot);

        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&purged_pubkey1, &account2)]);
        accounts.add_root(current_slot);

        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&purged_pubkey1, &account2)]);
        accounts.add_root(current_slot);

        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&purged_pubkey1, &zero_lamport_account)]);
        accounts.store_uncached(current_slot, &[(&purged_pubkey2, &account3)]);
        accounts.add_root(current_slot);

        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&purged_pubkey2, &zero_lamport_account)]);
        accounts.add_root(current_slot);

        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&dummy_pubkey, &dummy_account)]);
        accounts.add_root(current_slot);

        accounts.print_count_and_status("before reconstruct");
        let accounts = reconstruct_accounts_db_via_serialization(&accounts, current_slot);
        accounts.print_count_and_status("before purge zero");
        accounts.clean_accounts(None, false);
        accounts.print_count_and_status("after purge zero");

        assert_load_account(&accounts, current_slot, pubkey, old_lamport);
        assert_load_account(&accounts, current_slot, purged_pubkey1, 0);
        assert_load_account(&accounts, current_slot, purged_pubkey2, 0);
    }

    fn do_full_clean_refcount(store1_first: bool, store_size: u64) {
        let pubkey1 = Pubkey::from_str("My11111111111111111111111111111111111111111").unwrap();
        let pubkey2 = Pubkey::from_str("My22211111111111111111111111111111111111111").unwrap();
        let pubkey3 = Pubkey::from_str("My33311111111111111111111111111111111111111").unwrap();

        let old_lamport = 223;
        let zero_lamport = 0;
        let dummy_lamport = 999_999;

        // size data so only 1 fits in a 4k store
        let data_size = 2200;

        let owner = *AccountSharedData::default().owner();

        let account = AccountSharedData::new(old_lamport, data_size, &owner);
        let account2 = AccountSharedData::new(old_lamport + 100_001, data_size, &owner);
        let account3 = AccountSharedData::new(old_lamport + 100_002, data_size, &owner);
        let account4 = AccountSharedData::new(dummy_lamport, data_size, &owner);
        let zero_lamport_account = AccountSharedData::new(zero_lamport, data_size, &owner);

        let mut current_slot = 0;
        let accounts = AccountsDb::new_sized_no_extra_stores(Vec::new(), store_size);

        // A: Initialize AccountsDb with pubkey1 and pubkey2
        current_slot += 1;
        if store1_first {
            accounts.store_uncached(current_slot, &[(&pubkey1, &account)]);
            accounts.store_uncached(current_slot, &[(&pubkey2, &account)]);
        } else {
            accounts.store_uncached(current_slot, &[(&pubkey2, &account)]);
            accounts.store_uncached(current_slot, &[(&pubkey1, &account)]);
        }
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        info!("post A");
        accounts.print_accounts_stats("Post-A");

        // B: Test multiple updates to pubkey1 in a single slot/storage
        current_slot += 1;
        assert_eq!(0, accounts.alive_account_count_in_slot(current_slot));
        assert_eq!(1, accounts.ref_count_for_pubkey(&pubkey1));
        accounts.store_uncached(current_slot, &[(&pubkey1, &account2)]);
        accounts.store_uncached(current_slot, &[(&pubkey1, &account2)]);
        assert_eq!(1, accounts.alive_account_count_in_slot(current_slot));
        // Stores to same pubkey, same slot only count once towards the
        // ref count
        assert_eq!(2, accounts.ref_count_for_pubkey(&pubkey1));
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        accounts.print_accounts_stats("Post-B pre-clean");

        accounts.clean_accounts(None, false);

        info!("post B");
        accounts.print_accounts_stats("Post-B");

        // C: more updates to trigger clean of previous updates
        current_slot += 1;
        assert_eq!(2, accounts.ref_count_for_pubkey(&pubkey1));
        accounts.store_uncached(current_slot, &[(&pubkey1, &account3)]);
        accounts.store_uncached(current_slot, &[(&pubkey2, &account3)]);
        accounts.store_uncached(current_slot, &[(&pubkey3, &account4)]);
        assert_eq!(3, accounts.ref_count_for_pubkey(&pubkey1));
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        info!("post C");

        accounts.print_accounts_stats("Post-C");

        // D: Make all keys 0-lamport, cleans all keys
        current_slot += 1;
        assert_eq!(3, accounts.ref_count_for_pubkey(&pubkey1));
        accounts.store_uncached(current_slot, &[(&pubkey1, &zero_lamport_account)]);
        accounts.store_uncached(current_slot, &[(&pubkey2, &zero_lamport_account)]);
        accounts.store_uncached(current_slot, &[(&pubkey3, &zero_lamport_account)]);

        let snapshot_stores = accounts.get_snapshot_storages(current_slot, None).0;
        let total_accounts: usize = snapshot_stores
            .iter()
            .flatten()
            .map(|s| s.all_accounts().len())
            .sum();
        assert!(!snapshot_stores.is_empty());
        assert!(total_accounts > 0);

        info!("post D");
        accounts.print_accounts_stats("Post-D");

        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);
        accounts.clean_accounts(None, false);

        accounts.print_accounts_stats("Post-D clean");

        let total_accounts_post_clean: usize = snapshot_stores
            .iter()
            .flatten()
            .map(|s| s.all_accounts().len())
            .sum();
        assert_eq!(total_accounts, total_accounts_post_clean);

        // should clean all 3 pubkeys
        assert_eq!(accounts.ref_count_for_pubkey(&pubkey1), 0);
        assert_eq!(accounts.ref_count_for_pubkey(&pubkey2), 0);
        assert_eq!(accounts.ref_count_for_pubkey(&pubkey3), 0);
    }

    #[test]
    fn test_full_clean_refcount() {
        solana_logger::setup();

        // Setup 3 scenarios which try to differentiate between pubkey1 being in an
        // Available slot or a Full slot which would cause a different reset behavior
        // when pubkey1 is cleaned and therefor cause the ref count to be incorrect
        // preventing a removal of that key.
        //
        // do stores with a 4mb size so only 1 store is created per slot
        do_full_clean_refcount(false, 4 * 1024 * 1024);

        // do stores with a 4k size and store pubkey1 first
        do_full_clean_refcount(false, 4096);

        // do stores with a 4k size and store pubkey1 2nd
        do_full_clean_refcount(true, 4096);
    }

    #[test]
    fn test_accounts_clean_after_snapshot_restore_then_old_revives() {
        solana_logger::setup();
        let old_lamport = 223;
        let zero_lamport = 0;
        let no_data = 0;
        let dummy_lamport = 999_999;
        let owner = *AccountSharedData::default().owner();

        let account = AccountSharedData::new(old_lamport, no_data, &owner);
        let account2 = AccountSharedData::new(old_lamport + 100_001, no_data, &owner);
        let account3 = AccountSharedData::new(old_lamport + 100_002, no_data, &owner);
        let dummy_account = AccountSharedData::new(dummy_lamport, no_data, &owner);
        let zero_lamport_account = AccountSharedData::new(zero_lamport, no_data, &owner);

        let pubkey1 = solana_sdk::pubkey::new_rand();
        let pubkey2 = solana_sdk::pubkey::new_rand();
        let dummy_pubkey = solana_sdk::pubkey::new_rand();

        let mut current_slot = 0;
        let accounts = AccountsDb::new_single();

        // A: Initialize AccountsDb with pubkey1 and pubkey2
        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&pubkey1, &account)]);
        accounts.store_uncached(current_slot, &[(&pubkey2, &account)]);
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        // B: Test multiple updates to pubkey1 in a single slot/storage
        current_slot += 1;
        assert_eq!(0, accounts.alive_account_count_in_slot(current_slot));
        assert_eq!(1, accounts.ref_count_for_pubkey(&pubkey1));
        accounts.store_uncached(current_slot, &[(&pubkey1, &account2)]);
        accounts.store_uncached(current_slot, &[(&pubkey1, &account2)]);
        assert_eq!(1, accounts.alive_account_count_in_slot(current_slot));
        // Stores to same pubkey, same slot only count once towards the
        // ref count
        assert_eq!(2, accounts.ref_count_for_pubkey(&pubkey1));
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        // C: Yet more update to trigger lazy clean of step A
        current_slot += 1;
        assert_eq!(2, accounts.ref_count_for_pubkey(&pubkey1));
        accounts.store_uncached(current_slot, &[(&pubkey1, &account3)]);
        assert_eq!(3, accounts.ref_count_for_pubkey(&pubkey1));
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        // D: Make pubkey1 0-lamport; also triggers clean of step B
        current_slot += 1;
        assert_eq!(3, accounts.ref_count_for_pubkey(&pubkey1));
        accounts.store_uncached(current_slot, &[(&pubkey1, &zero_lamport_account)]);
        accounts.clean_accounts(None, false);

        assert_eq!(
            // Removed one reference from the dead slot (reference only counted once
            // even though there were two stores to the pubkey in that slot)
            3, /* == 3 - 1 + 1 */
            accounts.ref_count_for_pubkey(&pubkey1)
        );
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        // E: Avoid missing bank hash error
        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&dummy_pubkey, &dummy_account)]);
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        assert_load_account(&accounts, current_slot, pubkey1, zero_lamport);
        assert_load_account(&accounts, current_slot, pubkey2, old_lamport);
        assert_load_account(&accounts, current_slot, dummy_pubkey, dummy_lamport);

        // At this point, there is no index entries for A and B
        // If step C and step D should be purged, snapshot restore would cause
        // pubkey1 to be revived as the state of step A.
        // So, prevent that from happening by introducing refcount
        accounts.clean_accounts(None, false);
        let accounts = reconstruct_accounts_db_via_serialization(&accounts, current_slot);
        accounts.clean_accounts(None, false);

        info!("pubkey: {}", pubkey1);
        accounts.print_accounts_stats("pre_clean");
        assert_load_account(&accounts, current_slot, pubkey1, zero_lamport);
        assert_load_account(&accounts, current_slot, pubkey2, old_lamport);
        assert_load_account(&accounts, current_slot, dummy_pubkey, dummy_lamport);

        // F: Finally, make Step A cleanable
        current_slot += 1;
        accounts.store_uncached(current_slot, &[(&pubkey2, &account)]);
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        // Do clean
        accounts.clean_accounts(None, false);

        // 2nd clean needed to clean-up pubkey1
        accounts.clean_accounts(None, false);

        // Ensure pubkey2 is cleaned from the index finally
        assert_not_load_account(&accounts, current_slot, pubkey1);
        assert_load_account(&accounts, current_slot, pubkey2, old_lamport);
        assert_load_account(&accounts, current_slot, dummy_pubkey, dummy_lamport);
    }

    #[test]
    fn test_clean_stored_dead_slots_empty() {
        let accounts = AccountsDb::new_single();
        let mut dead_slots = HashSet::new();
        dead_slots.insert(10);
        accounts.clean_stored_dead_slots(&dead_slots, None);
    }

    #[test]
    fn test_shrink_all_slots_none() {
        for startup in &[false, true] {
            let accounts = AccountsDb::new_single();

            for _ in 0..10 {
                accounts.shrink_candidate_slots();
            }

            accounts.shrink_all_slots(*startup);
        }
    }

    #[test]
    fn test_shrink_next_slots() {
        let mut accounts = AccountsDb::new_single();
        accounts.caching_enabled = false;

        let mut current_slot = 7;

        assert_eq!(
            vec![None, None, None],
            (0..3)
                .map(|_| accounts.next_shrink_slot_v1())
                .collect::<Vec<_>>()
        );

        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        assert_eq!(
            vec![Some(7), Some(7), Some(7)],
            (0..3)
                .map(|_| accounts.next_shrink_slot_v1())
                .collect::<Vec<_>>()
        );

        current_slot += 1;
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        let slots = (0..6)
            .map(|_| accounts.next_shrink_slot_v1())
            .collect::<Vec<_>>();

        // Because the origin of this data is HashMap (not BTreeMap), key order is arbitrary per cycle.
        assert!(
            vec![Some(7), Some(8), Some(7), Some(8), Some(7), Some(8)] == slots
                || vec![Some(8), Some(7), Some(8), Some(7), Some(8), Some(7)] == slots
        );
    }

    #[test]
    fn test_shrink_reset_uncleaned_roots() {
        let mut accounts = AccountsDb::new_single();
        accounts.caching_enabled = false;

        accounts.reset_uncleaned_roots_v1();
        assert_eq!(
            *accounts.shrink_candidate_slots_v1.lock().unwrap(),
            vec![] as Vec<Slot>
        );

        accounts.get_accounts_delta_hash(0);
        accounts.add_root(0);
        accounts.get_accounts_delta_hash(1);
        accounts.add_root(1);
        accounts.get_accounts_delta_hash(2);
        accounts.add_root(2);

        accounts.reset_uncleaned_roots_v1();
        let actual_slots = accounts.shrink_candidate_slots_v1.lock().unwrap().clone();
        assert_eq!(actual_slots, vec![] as Vec<Slot>);

        accounts.reset_uncleaned_roots_v1();
        let mut actual_slots = accounts.shrink_candidate_slots_v1.lock().unwrap().clone();
        actual_slots.sort_unstable();
        assert_eq!(actual_slots, vec![0, 1, 2]);

        accounts.accounts_index.clear_roots();
        let mut actual_slots = (0..5)
            .map(|_| accounts.next_shrink_slot_v1())
            .collect::<Vec<_>>();
        actual_slots.sort();
        assert_eq!(actual_slots, vec![None, None, Some(0), Some(1), Some(2)],);
    }

    #[test]
    fn test_shrink_stale_slots_processed() {
        solana_logger::setup();

        for startup in &[false, true] {
            let accounts = AccountsDb::new_single();

            let pubkey_count = 100;
            let pubkeys: Vec<_> = (0..pubkey_count)
                .map(|_| solana_sdk::pubkey::new_rand())
                .collect();

            let some_lamport = 223;
            let no_data = 0;
            let owner = *AccountSharedData::default().owner();

            let account = AccountSharedData::new(some_lamport, no_data, &owner);

            let mut current_slot = 0;

            current_slot += 1;
            for pubkey in &pubkeys {
                accounts.store_uncached(current_slot, &[(pubkey, &account)]);
            }
            let shrink_slot = current_slot;
            accounts.get_accounts_delta_hash(current_slot);
            accounts.add_root(current_slot);

            current_slot += 1;
            let pubkey_count_after_shrink = 10;
            let updated_pubkeys = &pubkeys[0..pubkey_count - pubkey_count_after_shrink];

            for pubkey in updated_pubkeys {
                accounts.store_uncached(current_slot, &[(pubkey, &account)]);
            }
            accounts.get_accounts_delta_hash(current_slot);
            accounts.add_root(current_slot);

            accounts.clean_accounts(None, false);

            assert_eq!(
                pubkey_count,
                accounts.all_account_count_in_append_vec(shrink_slot)
            );
            accounts.shrink_all_slots(*startup);
            assert_eq!(
                pubkey_count_after_shrink,
                accounts.all_account_count_in_append_vec(shrink_slot)
            );

            let no_ancestors = Ancestors::default();
            accounts.update_accounts_hash(current_slot, &no_ancestors);
            accounts
                .verify_bank_hash_and_lamports(current_slot, &no_ancestors, 22300, true)
                .unwrap();

            let accounts = reconstruct_accounts_db_via_serialization(&accounts, current_slot);
            accounts
                .verify_bank_hash_and_lamports(current_slot, &no_ancestors, 22300, true)
                .unwrap();

            // repeating should be no-op
            accounts.shrink_all_slots(*startup);
            assert_eq!(
                pubkey_count_after_shrink,
                accounts.all_account_count_in_append_vec(shrink_slot)
            );
        }
    }

    #[test]
    fn test_shrink_candidate_slots() {
        solana_logger::setup();

        let accounts = AccountsDb::new_single();

        let pubkey_count = 30000;
        let pubkeys: Vec<_> = (0..pubkey_count)
            .map(|_| solana_sdk::pubkey::new_rand())
            .collect();

        let some_lamport = 223;
        let no_data = 0;
        let owner = *AccountSharedData::default().owner();

        let account = AccountSharedData::new(some_lamport, no_data, &owner);

        let mut current_slot = 0;

        current_slot += 1;
        for pubkey in &pubkeys {
            accounts.store_uncached(current_slot, &[(pubkey, &account)]);
        }
        let shrink_slot = current_slot;
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        current_slot += 1;
        let pubkey_count_after_shrink = 25000;
        let updated_pubkeys = &pubkeys[0..pubkey_count - pubkey_count_after_shrink];

        for pubkey in updated_pubkeys {
            accounts.store_uncached(current_slot, &[(pubkey, &account)]);
        }
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);
        accounts.clean_accounts(None, false);

        assert_eq!(
            pubkey_count,
            accounts.all_account_count_in_append_vec(shrink_slot)
        );

        // Only, try to shrink stale slots, nothing happens because 90/100
        // is not small enough to do a shrink
        accounts.shrink_candidate_slots();
        assert_eq!(
            pubkey_count,
            accounts.all_account_count_in_append_vec(shrink_slot)
        );

        // Now, do full-shrink.
        accounts.shrink_all_slots(false);
        assert_eq!(
            pubkey_count_after_shrink,
            accounts.all_account_count_in_append_vec(shrink_slot)
        );
    }

    #[test]
    fn test_select_candidates_by_total_usage() {
        solana_logger::setup();

        // case 1: no candidates
        let accounts = AccountsDb::new_single();

        let mut candidates: ShrinkCandidates = HashMap::new();
        let output_candidates =
            accounts.select_candidates_by_total_usage(&candidates, DEFAULT_ACCOUNTS_SHRINK_RATIO);

        assert_eq!(0, output_candidates.len());

        // case 2: two candidates, only one selected
        let dummy_path = Path::new("");
        let dummy_slot = 12;
        let dummy_size = 2 * PAGE_SIZE;

        let dummy_id1 = 22;
        let entry1 = Arc::new(AccountStorageEntry::new(
            dummy_path, dummy_slot, dummy_id1, dummy_size,
        ));
        entry1.alive_bytes.store(8000, Ordering::Relaxed);

        candidates
            .entry(dummy_slot)
            .or_default()
            .insert(entry1.append_vec_id(), entry1.clone());

        let dummy_id2 = 44;
        let entry2 = Arc::new(AccountStorageEntry::new(
            dummy_path, dummy_slot, dummy_id2, dummy_size,
        ));
        entry2.alive_bytes.store(3000, Ordering::Relaxed);
        candidates
            .entry(dummy_slot)
            .or_default()
            .insert(entry2.append_vec_id(), entry2.clone());

        let output_candidates =
            accounts.select_candidates_by_total_usage(&candidates, DEFAULT_ACCOUNTS_SHRINK_RATIO);
        assert_eq!(1, output_candidates.len());
        assert_eq!(1, output_candidates[&dummy_slot].len());
        assert!(output_candidates[&dummy_slot].contains(&entry2.append_vec_id()));

        // case 3: two candidates, both are selected
        candidates.clear();
        let dummy_size = 4 * PAGE_SIZE;
        let dummy_id1 = 22;
        let entry1 = Arc::new(AccountStorageEntry::new(
            dummy_path, dummy_slot, dummy_id1, dummy_size,
        ));
        entry1.alive_bytes.store(3500, Ordering::Relaxed);

        candidates
            .entry(dummy_slot)
            .or_default()
            .insert(entry1.append_vec_id(), entry1.clone());

        let dummy_id2 = 44;
        let dummy_slot2 = 44;
        let entry2 = Arc::new(AccountStorageEntry::new(
            dummy_path,
            dummy_slot2,
            dummy_id2,
            dummy_size,
        ));
        entry2.alive_bytes.store(3000, Ordering::Relaxed);

        candidates
            .entry(dummy_slot2)
            .or_default()
            .insert(entry2.append_vec_id(), entry2.clone());

        let output_candidates =
            accounts.select_candidates_by_total_usage(&candidates, DEFAULT_ACCOUNTS_SHRINK_RATIO);
        assert_eq!(2, output_candidates.len());
        assert_eq!(1, output_candidates[&dummy_slot].len());
        assert_eq!(1, output_candidates[&dummy_slot2].len());

        assert!(output_candidates[&dummy_slot].contains(&entry1.append_vec_id()));
        assert!(output_candidates[&dummy_slot2].contains(&entry2.append_vec_id()));
    }

    #[test]
    fn test_shrink_stale_slots_skipped() {
        solana_logger::setup();

        let mut accounts = AccountsDb::new_single();
        accounts.caching_enabled = false;

        let pubkey_count = 30000;
        let pubkeys: Vec<_> = (0..pubkey_count)
            .map(|_| solana_sdk::pubkey::new_rand())
            .collect();

        let some_lamport = 223;
        let no_data = 0;
        let owner = *AccountSharedData::default().owner();

        let account = AccountSharedData::new(some_lamport, no_data, &owner);

        let mut current_slot = 0;

        current_slot += 1;
        for pubkey in &pubkeys {
            accounts.store_uncached(current_slot, &[(pubkey, &account)]);
        }
        let shrink_slot = current_slot;
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        current_slot += 1;
        let pubkey_count_after_shrink = 25000;
        let updated_pubkeys = &pubkeys[0..pubkey_count - pubkey_count_after_shrink];

        for pubkey in updated_pubkeys {
            accounts.store_uncached(current_slot, &[(pubkey, &account)]);
        }
        accounts.get_accounts_delta_hash(current_slot);
        accounts.add_root(current_slot);

        accounts.clean_accounts(None, false);

        assert_eq!(
            pubkey_count,
            accounts.all_account_count_in_append_vec(shrink_slot)
        );

        // Only, try to shrink stale slots.
        accounts.shrink_all_stale_slots_v1();
        assert_eq!(
            pubkey_count,
            accounts.all_account_count_in_append_vec(shrink_slot)
        );

        // Now, do full-shrink.
        accounts.shrink_all_slots(false);
        assert_eq!(
            pubkey_count_after_shrink,
            accounts.all_account_count_in_append_vec(shrink_slot)
        );
    }

    #[test]
    fn test_delete_dependencies() {
        solana_logger::setup();
        let accounts_index = AccountsIndex::default();
        let key0 = Pubkey::new_from_array([0u8; 32]);
        let key1 = Pubkey::new_from_array([1u8; 32]);
        let key2 = Pubkey::new_from_array([2u8; 32]);
        let info0 = AccountInfo {
            store_id: 0,
            offset: 0,
            stored_size: 0,
            lamports: 0,
        };
        let info1 = AccountInfo {
            store_id: 1,
            offset: 0,
            stored_size: 0,
            lamports: 0,
        };
        let info2 = AccountInfo {
            store_id: 2,
            offset: 0,
            stored_size: 0,
            lamports: 0,
        };
        let info3 = AccountInfo {
            store_id: 3,
            offset: 0,
            stored_size: 0,
            lamports: 0,
        };
        let mut reclaims = vec![];
        accounts_index.upsert(
            0,
            &key0,
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            info0,
            &mut reclaims,
        );
        accounts_index.upsert(
            1,
            &key0,
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            info1.clone(),
            &mut reclaims,
        );
        accounts_index.upsert(
            1,
            &key1,
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            info1,
            &mut reclaims,
        );
        accounts_index.upsert(
            2,
            &key1,
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            info2.clone(),
            &mut reclaims,
        );
        accounts_index.upsert(
            2,
            &key2,
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            info2,
            &mut reclaims,
        );
        accounts_index.upsert(
            3,
            &key2,
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            info3,
            &mut reclaims,
        );
        accounts_index.add_root(0, false);
        accounts_index.add_root(1, false);
        accounts_index.add_root(2, false);
        accounts_index.add_root(3, false);
        let mut purges = HashMap::new();
        let (key0_entry, _) = accounts_index.get(&key0, None, None).unwrap();
        purges.insert(key0, accounts_index.roots_and_ref_count(&key0_entry, None));
        let (key1_entry, _) = accounts_index.get(&key1, None, None).unwrap();
        purges.insert(key1, accounts_index.roots_and_ref_count(&key1_entry, None));
        let (key2_entry, _) = accounts_index.get(&key2, None, None).unwrap();
        purges.insert(key2, accounts_index.roots_and_ref_count(&key2_entry, None));
        for (key, (list, ref_count)) in &purges {
            info!(" purge {} ref_count {} =>", key, ref_count);
            for x in list {
                info!("  {:?}", x);
            }
        }

        let mut store_counts = HashMap::new();
        store_counts.insert(0, (0, HashSet::from_iter(vec![key0])));
        store_counts.insert(1, (0, HashSet::from_iter(vec![key0, key1])));
        store_counts.insert(2, (0, HashSet::from_iter(vec![key1, key2])));
        store_counts.insert(3, (1, HashSet::from_iter(vec![key2])));
        AccountsDb::calc_delete_dependencies(&purges, &mut store_counts);
        let mut stores: Vec<_> = store_counts.keys().cloned().collect();
        stores.sort_unstable();
        for store in &stores {
            info!(
                "store: {:?} : {:?}",
                store,
                store_counts.get(store).unwrap()
            );
        }
        for x in 0..3 {
            assert!(store_counts[&x].0 >= 1);
        }
    }

    #[test]
    fn test_account_balance_for_capitalization_sysvar() {
        let normal_sysvar = solana_sdk::account::create_account_for_test(
            &solana_sdk::slot_history::SlotHistory::default(),
        );
        assert_eq!(normal_sysvar.lamports(), 1);
    }

    #[test]
    fn test_account_balance_for_capitalization_native_program() {
        let normal_native_program =
            solana_sdk::native_loader::create_loadable_account_for_test("foo");
        assert_eq!(normal_native_program.lamports(), 1);
    }

    #[test]
    fn test_checked_sum_for_capitalization_normal() {
        assert_eq!(
            AccountsDb::checked_sum_for_capitalization(vec![1, 2].into_iter()),
            3
        );
    }

    #[test]
    #[should_panic(expected = "overflow is detected while summing capitalization")]
    fn test_checked_sum_for_capitalization_overflow() {
        assert_eq!(
            AccountsDb::checked_sum_for_capitalization(vec![1, u64::max_value()].into_iter()),
            3
        );
    }

    #[test]
    fn test_store_overhead() {
        solana_logger::setup();
        let accounts = AccountsDb::new_single();
        let account = AccountSharedData::default();
        let pubkey = solana_sdk::pubkey::new_rand();
        accounts.store_uncached(0, &[(&pubkey, &account)]);
        let slot_stores = accounts.storage.get_slot_stores(0).unwrap();
        let mut total_len = 0;
        for (_id, store) in slot_stores.read().unwrap().iter() {
            total_len += store.accounts.len();
        }
        info!("total: {}", total_len);
        assert!(total_len < STORE_META_OVERHEAD);
    }

    #[test]
    fn test_store_clean_after_shrink() {
        solana_logger::setup();
        let accounts = AccountsDb::new_with_config(
            vec![],
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            true,
            AccountShrinkThreshold::default(),
        );

        let account = AccountSharedData::new(1, 16 * 4096, &Pubkey::default());
        let pubkey1 = solana_sdk::pubkey::new_rand();
        accounts.store_cached(0, &[(&pubkey1, &account)]);

        let pubkey2 = solana_sdk::pubkey::new_rand();
        accounts.store_cached(0, &[(&pubkey2, &account)]);

        let zero_account = AccountSharedData::new(0, 1, &Pubkey::default());
        accounts.store_cached(1, &[(&pubkey1, &zero_account)]);

        // Add root 0 and flush separately
        accounts.get_accounts_delta_hash(0);
        accounts.add_root(0);
        accounts.flush_accounts_cache(true, None);

        // clear out the dirty keys
        accounts.clean_accounts(None, false);

        // flush 1
        accounts.get_accounts_delta_hash(1);
        accounts.add_root(1);
        accounts.flush_accounts_cache(true, None);

        accounts.print_accounts_stats("pre-clean");

        // clean to remove pubkey1 from 0,
        // shrink to shrink pubkey1 from 0
        // then another clean to remove pubkey1 from slot 1
        accounts.clean_accounts(None, false);

        accounts.shrink_candidate_slots();

        accounts.clean_accounts(None, false);

        accounts.print_accounts_stats("post-clean");
        assert_eq!(accounts.accounts_index.ref_count_from_storage(&pubkey1), 0);
    }

    #[test]
    fn test_store_reuse() {
        solana_logger::setup();
        let accounts = AccountsDb::new_sized(vec![], 4096);

        let size = 100;
        let num_accounts: usize = 100;
        let mut keys = Vec::new();
        for i in 0..num_accounts {
            let account = AccountSharedData::new((i + 1) as u64, size, &Pubkey::default());
            let pubkey = solana_sdk::pubkey::new_rand();
            accounts.store_uncached(0, &[(&pubkey, &account)]);
            keys.push(pubkey);
        }
        accounts.add_root(0);

        for (i, key) in keys[1..].iter().enumerate() {
            let account =
                AccountSharedData::new((1 + i + num_accounts) as u64, size, &Pubkey::default());
            accounts.store_uncached(1, &[(key, &account)]);
        }
        accounts.add_root(1);
        accounts.clean_accounts(None, false);
        accounts.shrink_all_slots(false);

        // Clean again to flush the dirty stores
        // and allow them to be recycled in the next step
        accounts.clean_accounts(None, false);
        accounts.print_accounts_stats("post-shrink");
        let num_stores = accounts.recycle_stores.read().unwrap().entry_count();
        assert!(num_stores > 0);

        let mut account_refs = Vec::new();
        let num_to_store = 20;
        for (i, key) in keys[..num_to_store].iter().enumerate() {
            let account = AccountSharedData::new(
                (1 + i + 2 * num_accounts) as u64,
                i + 20,
                &Pubkey::default(),
            );
            accounts.store_uncached(2, &[(key, &account)]);
            account_refs.push(account);
        }
        assert!(accounts.recycle_stores.read().unwrap().entry_count() < num_stores);

        accounts.print_accounts_stats("post-store");

        let mut ancestors = Ancestors::default();
        ancestors.insert(1, 0);
        ancestors.insert(2, 1);
        for (key, account_ref) in keys[..num_to_store].iter().zip(account_refs) {
            assert_eq!(
                accounts.load_without_fixed_root(&ancestors, key).unwrap().0,
                account_ref
            );
        }
    }

    #[test]
    fn test_zero_lamport_new_root_not_cleaned() {
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let account_key = Pubkey::new_unique();
        let zero_lamport_account =
            AccountSharedData::new(0, 0, AccountSharedData::default().owner());

        // Store zero lamport account into slots 0 and 1, root both slots
        db.store_uncached(0, &[(&account_key, &zero_lamport_account)]);
        db.store_uncached(1, &[(&account_key, &zero_lamport_account)]);
        db.get_accounts_delta_hash(0);
        db.add_root(0);
        db.get_accounts_delta_hash(1);
        db.add_root(1);

        // Only clean zero lamport accounts up to slot 0
        db.clean_accounts(Some(0), false);

        // Should still be able to find zero lamport account in slot 1
        assert_eq!(
            db.load_without_fixed_root(&Ancestors::default(), &account_key),
            Some((zero_lamport_account, 1))
        );
    }

    #[test]
    fn test_store_load_cached() {
        let mut db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        db.caching_enabled = true;
        let key = Pubkey::default();
        let account0 = AccountSharedData::new(1, 0, &key);
        let slot = 0;
        db.store_cached(slot, &[(&key, &account0)]);

        // Load with no ancestors and no root will return nothing
        assert!(db
            .load_without_fixed_root(&Ancestors::default(), &key)
            .is_none());

        // Load with ancestors not equal to `slot` will return nothing
        let ancestors = vec![(slot + 1, 1)].into_iter().collect();
        assert!(db.load_without_fixed_root(&ancestors, &key).is_none());

        // Load with ancestors equal to `slot` will return the account
        let ancestors = vec![(slot, 1)].into_iter().collect();
        assert_eq!(
            db.load_without_fixed_root(&ancestors, &key),
            Some((account0.clone(), slot))
        );

        // Adding root will return the account even without ancestors
        db.add_root(slot);
        assert_eq!(
            db.load_without_fixed_root(&Ancestors::default(), &key),
            Some((account0, slot))
        );
    }

    #[test]
    fn test_store_flush_load_cached() {
        let mut db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        db.caching_enabled = true;
        let key = Pubkey::default();
        let account0 = AccountSharedData::new(1, 0, &key);
        let slot = 0;
        db.store_cached(slot, &[(&key, &account0)]);
        db.mark_slot_frozen(slot);

        // No root was added yet, requires an ancestor to find
        // the account
        db.flush_accounts_cache(true, None);
        let ancestors = vec![(slot, 1)].into_iter().collect();
        assert_eq!(
            db.load_without_fixed_root(&ancestors, &key),
            Some((account0.clone(), slot))
        );

        // Add root then flush
        db.add_root(slot);
        db.flush_accounts_cache(true, None);
        assert_eq!(
            db.load_without_fixed_root(&Ancestors::default(), &key),
            Some((account0, slot))
        );
    }

    #[test]
    fn test_flush_accounts_cache() {
        let mut db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        db.caching_enabled = true;
        let account0 = AccountSharedData::new(1, 0, &Pubkey::default());

        let unrooted_slot = 4;
        let root5 = 5;
        let root6 = 6;
        let unrooted_key = solana_sdk::pubkey::new_rand();
        let key5 = solana_sdk::pubkey::new_rand();
        let key6 = solana_sdk::pubkey::new_rand();
        db.store_cached(unrooted_slot, &[(&unrooted_key, &account0)]);
        db.store_cached(root5, &[(&key5, &account0)]);
        db.store_cached(root6, &[(&key6, &account0)]);
        for slot in &[unrooted_slot, root5, root6] {
            db.mark_slot_frozen(*slot);
        }
        db.add_root(root5);
        db.add_root(root6);

        // Unrooted slot should be able to be fetched before the flush
        let ancestors = vec![(unrooted_slot, 1)].into_iter().collect();
        assert_eq!(
            db.load_without_fixed_root(&ancestors, &unrooted_key),
            Some((account0.clone(), unrooted_slot))
        );
        db.flush_accounts_cache(true, None);

        // After the flush, the unrooted slot is still in the cache
        assert!(db
            .load_without_fixed_root(&ancestors, &unrooted_key)
            .is_some());
        assert!(db
            .accounts_index
            .get_account_read_entry(&unrooted_key)
            .is_some());
        assert_eq!(db.accounts_cache.num_slots(), 1);
        assert!(db.accounts_cache.slot_cache(unrooted_slot).is_some());
        assert_eq!(
            db.load_without_fixed_root(&Ancestors::default(), &key5),
            Some((account0.clone(), root5))
        );
        assert_eq!(
            db.load_without_fixed_root(&Ancestors::default(), &key6),
            Some((account0, root6))
        );
    }

    #[test]
    fn test_flush_accounts_cache_if_needed() {
        run_test_flush_accounts_cache_if_needed(0, 2 * MAX_CACHE_SLOTS);
        run_test_flush_accounts_cache_if_needed(2 * MAX_CACHE_SLOTS, 0);
        run_test_flush_accounts_cache_if_needed(MAX_CACHE_SLOTS - 1, 0);
        run_test_flush_accounts_cache_if_needed(0, MAX_CACHE_SLOTS - 1);
        run_test_flush_accounts_cache_if_needed(MAX_CACHE_SLOTS, 0);
        run_test_flush_accounts_cache_if_needed(0, MAX_CACHE_SLOTS);
        run_test_flush_accounts_cache_if_needed(2 * MAX_CACHE_SLOTS, 2 * MAX_CACHE_SLOTS);
        run_test_flush_accounts_cache_if_needed(MAX_CACHE_SLOTS - 1, MAX_CACHE_SLOTS - 1);
        run_test_flush_accounts_cache_if_needed(MAX_CACHE_SLOTS, MAX_CACHE_SLOTS);
    }

    fn run_test_flush_accounts_cache_if_needed(num_roots: usize, num_unrooted: usize) {
        let mut db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        db.caching_enabled = true;
        let account0 = AccountSharedData::new(1, 0, &Pubkey::default());
        let mut keys = vec![];
        let num_slots = 2 * MAX_CACHE_SLOTS;
        for i in 0..num_roots + num_unrooted {
            let key = Pubkey::new_unique();
            db.store_cached(i as Slot, &[(&key, &account0)]);
            keys.push(key);
            db.mark_slot_frozen(i as Slot);
            if i < num_roots {
                db.add_root(i as Slot);
            }
        }

        db.flush_accounts_cache(false, None);

        let total_slots = num_roots + num_unrooted;
        // If there's <= the max size, then nothing will be flushed from the slot
        if total_slots <= MAX_CACHE_SLOTS {
            assert_eq!(db.accounts_cache.num_slots(), total_slots);
        } else {
            // Otherwise, all the roots are flushed, and only at most MAX_CACHE_SLOTS
            // of the unrooted slots are kept in the cache
            let expected_size = std::cmp::min(num_unrooted, MAX_CACHE_SLOTS);
            if expected_size > 0 {
                for unrooted_slot in total_slots - expected_size..total_slots {
                    assert!(db
                        .accounts_cache
                        .slot_cache(unrooted_slot as Slot)
                        .is_some());
                }
            }
        }

        // Should still be able to fetch all the accounts after flush
        for (slot, key) in (0..num_slots as Slot).zip(keys) {
            let ancestors = if slot < num_roots as Slot {
                Ancestors::default()
            } else {
                vec![(slot, 1)].into_iter().collect()
            };
            assert_eq!(
                db.load_without_fixed_root(&ancestors, &key),
                Some((account0.clone(), slot))
            );
        }
    }

    fn slot_stores(db: &AccountsDb, slot: Slot) -> Vec<Arc<AccountStorageEntry>> {
        db.storage
            .get_slot_storage_entries(slot)
            .unwrap_or_default()
    }

    #[test]
    fn test_read_only_accounts_cache() {
        let caching_enabled = true;
        let db = Arc::new(AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        ));

        let account_key = Pubkey::new_unique();
        let zero_lamport_account =
            AccountSharedData::new(0, 0, AccountSharedData::default().owner());
        let slot1_account = AccountSharedData::new(1, 1, AccountSharedData::default().owner());
        db.store_cached(0, &[(&account_key, &zero_lamport_account)]);
        db.store_cached(1, &[(&account_key, &slot1_account)]);

        db.add_root(0);
        db.add_root(1);
        db.clean_accounts(None, false);
        db.flush_accounts_cache(true, None);
        db.clean_accounts(None, false);
        db.add_root(2);

        assert_eq!(db.read_only_accounts_cache.cache_len(), 0);
        let account = db
            .load_with_fixed_root(&Ancestors::default(), &account_key)
            .map(|(account, _)| account)
            .unwrap();
        assert_eq!(account.lamports(), 1);
        assert_eq!(db.read_only_accounts_cache.cache_len(), 1);
        let account = db
            .load_with_fixed_root(&Ancestors::default(), &account_key)
            .map(|(account, _)| account)
            .unwrap();
        assert_eq!(account.lamports(), 1);
        assert_eq!(db.read_only_accounts_cache.cache_len(), 1);
        db.store_cached(2, &[(&account_key, &zero_lamport_account)]);
        assert_eq!(db.read_only_accounts_cache.cache_len(), 1);
        let account = db
            .load_with_fixed_root(&Ancestors::default(), &account_key)
            .map(|(account, _)| account)
            .unwrap();
        assert_eq!(account.lamports(), 0);
        assert_eq!(db.read_only_accounts_cache.cache_len(), 1);
    }

    #[test]
    fn test_flush_cache_clean() {
        let caching_enabled = true;
        let db = Arc::new(AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        ));

        let account_key = Pubkey::new_unique();
        let zero_lamport_account =
            AccountSharedData::new(0, 0, AccountSharedData::default().owner());
        let slot1_account = AccountSharedData::new(1, 1, AccountSharedData::default().owner());
        db.store_cached(0, &[(&account_key, &zero_lamport_account)]);
        db.store_cached(1, &[(&account_key, &slot1_account)]);

        db.add_root(0);
        db.add_root(1);

        // Clean should not remove anything yet as nothing has been flushed
        db.clean_accounts(None, false);
        let account = db
            .do_load(
                &Ancestors::default(),
                &account_key,
                Some(0),
                LoadHint::Unspecified,
                false,
            )
            .unwrap();
        assert_eq!(account.0.lamports(), 0);
        // since this item is in the cache, it should not be in the read only cache
        assert_eq!(db.read_only_accounts_cache.cache_len(), 0);

        // Flush, then clean again. Should not need another root to initiate the cleaning
        // because `accounts_index.uncleaned_roots` should be correct
        db.flush_accounts_cache(true, None);
        db.clean_accounts(None, false);
        assert!(db
            .do_load(
                &Ancestors::default(),
                &account_key,
                Some(0),
                LoadHint::Unspecified,
                false,
            )
            .is_none());
    }

    #[test]
    fn test_flush_cache_dont_clean_zero_lamport_account() {
        let caching_enabled = true;
        let db = Arc::new(AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        ));

        let zero_lamport_account_key = Pubkey::new_unique();
        let other_account_key = Pubkey::new_unique();

        let original_lamports = 1;
        let slot0_account =
            AccountSharedData::new(original_lamports, 1, AccountSharedData::default().owner());
        let zero_lamport_account =
            AccountSharedData::new(0, 0, AccountSharedData::default().owner());

        // Store into slot 0, and then flush the slot to storage
        db.store_cached(0, &[(&zero_lamport_account_key, &slot0_account)]);
        // Second key keeps other lamport account entry for slot 0 alive,
        // preventing clean of the zero_lamport_account in slot 1.
        db.store_cached(0, &[(&other_account_key, &slot0_account)]);
        db.add_root(0);
        db.flush_accounts_cache(true, None);
        assert!(!db.storage.get_slot_storage_entries(0).unwrap().is_empty());

        // Store into slot 1, a dummy slot that will be dead and purged before flush
        db.store_cached(1, &[(&zero_lamport_account_key, &zero_lamport_account)]);

        // Store into slot 2, which makes all updates from slot 1 outdated.
        // This means slot 1 is a dead slot. Later, slot 1 will be cleaned/purged
        // before it even reaches storage, but this purge of slot 1should not affect
        // the refcount of `zero_lamport_account_key` because cached keys do not bump
        // the refcount in the index. This means clean should *not* remove
        // `zero_lamport_account_key` from slot 2
        db.store_cached(2, &[(&zero_lamport_account_key, &zero_lamport_account)]);
        db.add_root(1);
        db.add_root(2);

        // Flush, then clean. Should not need another root to initiate the cleaning
        // because `accounts_index.uncleaned_roots` should be correct
        db.flush_accounts_cache(true, None);
        db.clean_accounts(None, false);

        // The `zero_lamport_account_key` is still alive in slot 1, so refcount for the
        // pubkey should be 2
        assert_eq!(
            db.accounts_index
                .ref_count_from_storage(&zero_lamport_account_key),
            2
        );
        assert_eq!(
            db.accounts_index.ref_count_from_storage(&other_account_key),
            1
        );

        // The zero-lamport account in slot 2 should not be purged yet, because the
        // entry in slot 1 is blocking cleanup of the zero-lamport account.
        let max_root = None;
        // Fine to simulate a transaction load since we are not doing any out of band
        // removals, only using clean_accounts
        let load_hint = LoadHint::FixedMaxRoot;
        assert_eq!(
            db.do_load(
                &Ancestors::default(),
                &zero_lamport_account_key,
                max_root,
                load_hint,
                false,
            )
            .unwrap()
            .0
            .lamports(),
            0
        );
    }

    struct ScanTracker {
        t_scan: JoinHandle<()>,
        exit: Arc<AtomicBool>,
    }

    impl ScanTracker {
        fn exit(self) -> thread::Result<()> {
            self.exit.store(true, Ordering::Relaxed);
            self.t_scan.join()
        }
    }

    fn setup_scan(
        db: Arc<AccountsDb>,
        scan_ancestors: Arc<Ancestors>,
        bank_id: BankId,
        stall_key: Pubkey,
    ) -> ScanTracker {
        let exit = Arc::new(AtomicBool::new(false));
        let exit_ = exit.clone();
        let ready = Arc::new(AtomicBool::new(false));
        let ready_ = ready.clone();

        let t_scan = Builder::new()
            .name("scan".to_string())
            .spawn(move || {
                db.scan_accounts(
                    &scan_ancestors,
                    bank_id,
                    |_collector: &mut Vec<(Pubkey, AccountSharedData)>, maybe_account| {
                        ready_.store(true, Ordering::Relaxed);
                        if let Some((pubkey, _, _)) = maybe_account {
                            if *pubkey == stall_key {
                                loop {
                                    if exit_.load(Ordering::Relaxed) {
                                        break;
                                    } else {
                                        sleep(Duration::from_millis(10));
                                    }
                                }
                            }
                        }
                    },
                )
                .unwrap();
            })
            .unwrap();

        // Wait for scan to start
        while !ready.load(Ordering::Relaxed) {
            sleep(Duration::from_millis(10));
        }

        ScanTracker { t_scan, exit }
    }

    #[test]
    fn test_scan_flush_accounts_cache_then_clean_drop() {
        let caching_enabled = true;
        let db = Arc::new(AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        ));
        let account_key = Pubkey::new_unique();
        let account_key2 = Pubkey::new_unique();
        let zero_lamport_account =
            AccountSharedData::new(0, 0, AccountSharedData::default().owner());
        let slot1_account = AccountSharedData::new(1, 1, AccountSharedData::default().owner());
        let slot2_account = AccountSharedData::new(2, 1, AccountSharedData::default().owner());

        /*
            Store zero lamport account into slots 0, 1, 2 where
            root slots are 0, 2, and slot 1 is unrooted.
                                    0 (root)
                                /        \
                              1            2 (root)
        */
        db.store_cached(0, &[(&account_key, &zero_lamport_account)]);
        db.store_cached(1, &[(&account_key, &slot1_account)]);
        // Fodder for the scan so that the lock on `account_key` is not held
        db.store_cached(1, &[(&account_key2, &slot1_account)]);
        db.store_cached(2, &[(&account_key, &slot2_account)]);
        db.get_accounts_delta_hash(0);

        let max_scan_root = 0;
        db.add_root(max_scan_root);
        let scan_ancestors: Arc<Ancestors> = Arc::new(vec![(0, 1), (1, 1)].into_iter().collect());
        let bank_id = 0;
        let scan_tracker = setup_scan(db.clone(), scan_ancestors.clone(), bank_id, account_key2);

        // Add a new root 2
        let new_root = 2;
        db.get_accounts_delta_hash(new_root);
        db.add_root(new_root);

        // Check that the scan is properly set up
        assert_eq!(
            db.accounts_index.min_ongoing_scan_root().unwrap(),
            max_scan_root
        );

        // If we specify a requested_flush_root == 2, then `slot 2 <= max_flush_slot` will
        // be flushed even though `slot 2 > max_scan_root`. The unrooted slot 1 should
        // remain in the cache
        db.flush_accounts_cache(true, Some(new_root));
        assert_eq!(db.accounts_cache.num_slots(), 1);
        assert!(db.accounts_cache.slot_cache(1).is_some());

        // Intra cache cleaning should not clean the entry for `account_key` from slot 0,
        // even though it was updated in slot `2` because of the ongoing scan
        let account = db
            .do_load(
                &Ancestors::default(),
                &account_key,
                Some(0),
                LoadHint::Unspecified,
                false,
            )
            .unwrap();
        assert_eq!(account.0.lamports(), zero_lamport_account.lamports());

        // Run clean, unrooted slot 1 should not be purged, and still readable from the cache,
        // because we're still doing a scan on it.
        db.clean_accounts(None, false);
        let account = db
            .do_load(
                &scan_ancestors,
                &account_key,
                Some(max_scan_root),
                LoadHint::Unspecified,
                false,
            )
            .unwrap();
        assert_eq!(account.0.lamports(), slot1_account.lamports());

        // When the scan is over, clean should not panic and should not purge something
        // still in the cache.
        scan_tracker.exit().unwrap();
        db.clean_accounts(None, false);
        let account = db
            .do_load(
                &scan_ancestors,
                &account_key,
                Some(max_scan_root),
                LoadHint::Unspecified,
                false,
            )
            .unwrap();
        assert_eq!(account.0.lamports(), slot1_account.lamports());

        // Simulate dropping the bank, which finally removes the slot from the cache
        let bank_id = 1;
        db.purge_slot(1, bank_id, false);
        assert!(db
            .do_load(
                &scan_ancestors,
                &account_key,
                Some(max_scan_root),
                LoadHint::Unspecified,
                false,
            )
            .is_none());
    }

    #[test]
    fn test_alive_bytes() {
        let caching_enabled = true;
        let accounts_db = AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        );
        let slot: Slot = 0;
        let num_keys = 10;

        for data_size in 0..num_keys {
            let account = AccountSharedData::new(1, data_size, &Pubkey::default());
            accounts_db.store_cached(slot, &[(&Pubkey::new_unique(), &account)]);
        }

        accounts_db.add_root(slot);
        accounts_db.flush_accounts_cache(true, None);

        let mut storage_maps: Vec<Arc<AccountStorageEntry>> = accounts_db
            .storage
            .get_slot_storage_entries(slot)
            .unwrap_or_default();

        // Flushing cache should only create one storage entry
        assert_eq!(storage_maps.len(), 1);
        let storage0 = storage_maps.pop().unwrap();
        let accounts = storage0.all_accounts();

        for account in accounts {
            let before_size = storage0.alive_bytes.load(Ordering::Relaxed);
            let account_info = accounts_db
                .accounts_index
                .get_account_read_entry(&account.meta.pubkey)
                .map(|locked_entry| {
                    // Should only be one entry per key, since every key was only stored to slot 0
                    locked_entry.slot_list()[0].clone()
                })
                .unwrap();
            let removed_data_size = account_info.1.stored_size;
            // Fetching the account from storage should return the same
            // stored size as in the index.
            assert_eq!(removed_data_size, account.stored_size);
            assert_eq!(account_info.0, slot);
            let reclaims = vec![account_info];
            accounts_db.remove_dead_accounts(&reclaims, None, None, true);
            let after_size = storage0.alive_bytes.load(Ordering::Relaxed);
            assert_eq!(before_size, after_size + account.stored_size);
        }
    }

    fn setup_accounts_db_cache_clean(
        num_slots: usize,
        scan_slot: Option<Slot>,
    ) -> (Arc<AccountsDb>, Vec<Pubkey>, Vec<Slot>, Option<ScanTracker>) {
        let caching_enabled = true;
        let accounts_db = Arc::new(AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        ));
        let slots: Vec<_> = (0..num_slots as Slot).into_iter().collect();
        let stall_slot = num_slots as Slot;
        let scan_stall_key = Pubkey::new_unique();
        let keys: Vec<Pubkey> = std::iter::repeat_with(Pubkey::new_unique)
            .take(num_slots)
            .collect();
        if scan_slot.is_some() {
            accounts_db.store_cached(
                // Store it in a slot that isn't returned in `slots`
                stall_slot,
                &[(
                    &scan_stall_key,
                    &AccountSharedData::new(1, 0, &Pubkey::default()),
                )],
            );
        }

        // Store some subset of the keys in slots 0..num_slots
        let mut scan_tracker = None;
        for slot in &slots {
            for key in &keys[*slot as usize..] {
                accounts_db.store_cached(
                    *slot,
                    &[(key, &AccountSharedData::new(1, 0, &Pubkey::default()))],
                );
            }
            accounts_db.add_root(*slot as Slot);
            if Some(*slot) == scan_slot {
                let ancestors = Arc::new(vec![(stall_slot, 1), (*slot, 1)].into_iter().collect());
                let bank_id = 0;
                scan_tracker = Some(setup_scan(
                    accounts_db.clone(),
                    ancestors,
                    bank_id,
                    scan_stall_key,
                ));
                assert_eq!(
                    accounts_db.accounts_index.min_ongoing_scan_root().unwrap(),
                    *slot
                );
            }
        }

        accounts_db.accounts_cache.remove_slot(stall_slot);

        // If there's <= MAX_CACHE_SLOTS, no slots should be flushed
        if accounts_db.accounts_cache.num_slots() <= MAX_CACHE_SLOTS {
            accounts_db.flush_accounts_cache(false, None);
            assert_eq!(accounts_db.accounts_cache.num_slots(), num_slots);
        }

        (accounts_db, keys, slots, scan_tracker)
    }

    #[test]
    fn test_accounts_db_cache_clean_dead_slots() {
        let num_slots = 10;
        let (accounts_db, keys, mut slots, _) = setup_accounts_db_cache_clean(num_slots, None);
        let last_dead_slot = (num_slots - 1) as Slot;
        assert_eq!(*slots.last().unwrap(), last_dead_slot);
        let alive_slot = last_dead_slot as Slot + 1;
        slots.push(alive_slot);
        for key in &keys {
            // Store a slot that overwrites all previous keys, rendering all previous keys dead
            accounts_db.store_cached(
                alive_slot,
                &[(key, &AccountSharedData::new(1, 0, &Pubkey::default()))],
            );
            accounts_db.add_root(alive_slot);
        }

        // Before the flush, we can find entries in the database for slots < alive_slot if we specify
        // a smaller max root
        for key in &keys {
            assert!(accounts_db
                .do_load(
                    &Ancestors::default(),
                    key,
                    Some(last_dead_slot),
                    LoadHint::Unspecified,
                    false,
                )
                .is_some());
        }

        // If no `max_clean_root` is specified, cleaning should purge all flushed slots
        accounts_db.flush_accounts_cache(true, None);
        assert_eq!(accounts_db.accounts_cache.num_slots(), 0);
        let mut uncleaned_roots = accounts_db
            .accounts_index
            .clear_uncleaned_roots(None)
            .into_iter()
            .collect::<Vec<_>>();
        uncleaned_roots.sort_unstable();
        assert_eq!(uncleaned_roots, slots);
        assert_eq!(
            accounts_db.accounts_cache.fetch_max_flush_root(),
            alive_slot,
        );

        // Specifying a max_root < alive_slot, should not return any more entries,
        // as those have been purged from the accounts index for the dead slots.
        for key in &keys {
            assert!(accounts_db
                .do_load(
                    &Ancestors::default(),
                    key,
                    Some(last_dead_slot),
                    LoadHint::Unspecified,
                    false,
                )
                .is_none());
        }
        // Each slot should only have one entry in the storage, since all other accounts were
        // cleaned due to later updates
        for slot in &slots {
            if let ScanStorageResult::Stored(slot_accounts) = accounts_db.scan_account_storage(
                *slot as Slot,
                |_| Some(0),
                |slot_accounts: &DashSet<Pubkey>, loaded_account: LoadedAccount| {
                    slot_accounts.insert(*loaded_account.pubkey());
                },
            ) {
                if *slot == alive_slot {
                    assert_eq!(slot_accounts.len(), keys.len());
                } else {
                    assert!(slot_accounts.is_empty());
                }
            } else {
                panic!("Expected slot to be in storage, not cache");
            }
        }
    }

    #[test]
    fn test_accounts_db_cache_clean() {
        let (accounts_db, keys, slots, _) = setup_accounts_db_cache_clean(10, None);

        // If no `max_clean_root` is specified, cleaning should purge all flushed slots
        accounts_db.flush_accounts_cache(true, None);
        assert_eq!(accounts_db.accounts_cache.num_slots(), 0);
        let mut uncleaned_roots = accounts_db
            .accounts_index
            .clear_uncleaned_roots(None)
            .into_iter()
            .collect::<Vec<_>>();
        uncleaned_roots.sort_unstable();
        assert_eq!(uncleaned_roots, slots);
        assert_eq!(
            accounts_db.accounts_cache.fetch_max_flush_root(),
            *slots.last().unwrap()
        );

        // Each slot should only have one entry in the storage, since all other accounts were
        // cleaned due to later updates
        for slot in &slots {
            if let ScanStorageResult::Stored(slot_account) = accounts_db.scan_account_storage(
                *slot as Slot,
                |_| Some(0),
                |slot_account: &Arc<RwLock<Pubkey>>, loaded_account: LoadedAccount| {
                    *slot_account.write().unwrap() = *loaded_account.pubkey();
                },
            ) {
                assert_eq!(*slot_account.read().unwrap(), keys[*slot as usize]);
            } else {
                panic!("Everything should have been flushed")
            }
        }
    }

    fn run_test_accounts_db_cache_clean_max_root(
        num_slots: usize,
        requested_flush_root: Slot,
        scan_root: Option<Slot>,
    ) {
        assert!(requested_flush_root < (num_slots as Slot));
        let (accounts_db, keys, slots, scan_tracker) =
            setup_accounts_db_cache_clean(num_slots, scan_root);
        let is_cache_at_limit = num_slots - requested_flush_root as usize - 1 > MAX_CACHE_SLOTS;

        // If:
        // 1) `requested_flush_root` is specified,
        // 2) not at the cache limit, i.e. `is_cache_at_limit == false`, then
        // `flush_accounts_cache()` should clean and flush only slots <= requested_flush_root,
        accounts_db.flush_accounts_cache(true, Some(requested_flush_root));

        if !is_cache_at_limit {
            // Should flush all slots between 0..=requested_flush_root
            assert_eq!(
                accounts_db.accounts_cache.num_slots(),
                slots.len() - requested_flush_root as usize - 1
            );
        } else {
            // Otherwise, if we are at the cache limit, all roots will be flushed
            assert_eq!(accounts_db.accounts_cache.num_slots(), 0,);
        }

        let mut uncleaned_roots = accounts_db
            .accounts_index
            .clear_uncleaned_roots(None)
            .into_iter()
            .collect::<Vec<_>>();
        uncleaned_roots.sort_unstable();

        let expected_max_flushed_root = if !is_cache_at_limit {
            // Should flush all slots between 0..=requested_flush_root
            requested_flush_root
        } else {
            // Otherwise, if we are at the cache limit, all roots will be flushed
            num_slots as Slot - 1
        };

        assert_eq!(
            uncleaned_roots,
            slots[0..=expected_max_flushed_root as usize].to_vec()
        );
        assert_eq!(
            accounts_db.accounts_cache.fetch_max_flush_root(),
            expected_max_flushed_root,
        );

        for slot in &slots {
            let slot_accounts = accounts_db.scan_account_storage(
                *slot as Slot,
                |loaded_account: LoadedAccount| {
                    if is_cache_at_limit {
                        panic!(
                            "When cache is at limit, all roots should have been flushed to storage"
                        );
                    }
                    // All slots <= requested_flush_root should have been flushed, regardless
                    // of ongoing scans
                    assert!(*slot > requested_flush_root);
                    Some(*loaded_account.pubkey())
                },
                |slot_accounts: &DashSet<Pubkey>, loaded_account: LoadedAccount| {
                    slot_accounts.insert(*loaded_account.pubkey());
                    if !is_cache_at_limit {
                        // Only true when the limit hasn't been reached and there are still
                        // slots left in the cache
                        assert!(*slot <= requested_flush_root);
                    }
                },
            );

            let slot_accounts = match slot_accounts {
                ScanStorageResult::Cached(slot_accounts) => {
                    slot_accounts.into_iter().collect::<HashSet<Pubkey>>()
                }
                ScanStorageResult::Stored(slot_accounts) => {
                    slot_accounts.into_iter().collect::<HashSet<Pubkey>>()
                }
            };

            let expected_accounts =
                if *slot >= requested_flush_root || *slot >= scan_root.unwrap_or(Slot::MAX) {
                    // 1) If slot > `requested_flush_root`, then  either:
                    //   a) If `is_cache_at_limit == false`, still in the cache
                    //   b) if `is_cache_at_limit == true`, were not cleaned before being flushed to storage.
                    //
                    // In both cases all the *original* updates at index `slot` were uncleaned and thus
                    // should be discoverable by this scan.
                    //
                    // 2) If slot == `requested_flush_root`, the slot was not cleaned before being flushed to storage,
                    // so it also contains all the original updates.
                    //
                    // 3) If *slot >= scan_root, then we should not clean it either
                    keys[*slot as usize..]
                        .iter()
                        .cloned()
                        .collect::<HashSet<Pubkey>>()
                } else {
                    // Slots less than `requested_flush_root` and `scan_root` were cleaned in the cache before being flushed
                    // to storage, should only contain one account
                    std::iter::once(keys[*slot as usize])
                        .into_iter()
                        .collect::<HashSet<Pubkey>>()
                };

            assert_eq!(slot_accounts, expected_accounts);
        }

        if let Some(scan_tracker) = scan_tracker {
            scan_tracker.exit().unwrap();
        }
    }

    #[test]
    fn test_accounts_db_cache_clean_max_root() {
        let requested_flush_root = 5;
        run_test_accounts_db_cache_clean_max_root(10, requested_flush_root, None);
    }

    #[test]
    fn test_accounts_db_cache_clean_max_root_with_scan() {
        let requested_flush_root = 5;
        run_test_accounts_db_cache_clean_max_root(
            10,
            requested_flush_root,
            Some(requested_flush_root - 1),
        );
        run_test_accounts_db_cache_clean_max_root(
            10,
            requested_flush_root,
            Some(requested_flush_root + 1),
        );
    }

    #[test]
    fn test_accounts_db_cache_clean_max_root_with_cache_limit_hit() {
        let requested_flush_root = 5;
        // Test that if there are > MAX_CACHE_SLOTS in the cache after flush, then more roots
        // will be flushed
        run_test_accounts_db_cache_clean_max_root(
            MAX_CACHE_SLOTS + requested_flush_root as usize + 2,
            requested_flush_root,
            None,
        );
    }

    #[test]
    fn test_accounts_db_cache_clean_max_root_with_cache_limit_hit_and_scan() {
        let requested_flush_root = 5;
        // Test that if there are > MAX_CACHE_SLOTS in the cache after flush, then more roots
        // will be flushed
        run_test_accounts_db_cache_clean_max_root(
            MAX_CACHE_SLOTS + requested_flush_root as usize + 2,
            requested_flush_root,
            Some(requested_flush_root - 1),
        );
        run_test_accounts_db_cache_clean_max_root(
            MAX_CACHE_SLOTS + requested_flush_root as usize + 2,
            requested_flush_root,
            Some(requested_flush_root + 1),
        );
    }

    fn run_flush_rooted_accounts_cache(should_clean: bool) {
        let num_slots = 10;
        let (accounts_db, keys, slots, _) = setup_accounts_db_cache_clean(num_slots, None);
        let mut cleaned_bytes = 0;
        let mut cleaned_accounts = 0;
        let should_clean_tracker = if should_clean {
            Some((&mut cleaned_bytes, &mut cleaned_accounts))
        } else {
            None
        };

        // If no cleaning is specified, then flush everything
        accounts_db.flush_rooted_accounts_cache(None, should_clean_tracker);
        for slot in &slots {
            let slot_accounts = if let ScanStorageResult::Stored(slot_accounts) = accounts_db
                .scan_account_storage(
                    *slot as Slot,
                    |_| Some(0),
                    |slot_account: &DashSet<Pubkey>, loaded_account: LoadedAccount| {
                        slot_account.insert(*loaded_account.pubkey());
                    },
                ) {
                slot_accounts.into_iter().collect::<HashSet<Pubkey>>()
            } else {
                panic!("All roots should have been flushed to storage");
            };
            if !should_clean || slot == slots.last().unwrap() {
                // The slot was not cleaned before being flushed to storage,
                // so it also contains all the original updates.
                assert_eq!(
                    slot_accounts,
                    keys[*slot as usize..]
                        .iter()
                        .cloned()
                        .collect::<HashSet<Pubkey>>()
                );
            } else {
                // If clean was specified, only the latest slot should have all the updates.
                // All these other slots have been cleaned before flush
                assert_eq!(
                    slot_accounts,
                    std::iter::once(keys[*slot as usize])
                        .into_iter()
                        .collect::<HashSet<Pubkey>>()
                );
            }
        }
    }

    #[test]
    fn test_flush_rooted_accounts_cache_with_clean() {
        run_flush_rooted_accounts_cache(true);
    }

    #[test]
    fn test_flush_rooted_accounts_cache_without_clean() {
        run_flush_rooted_accounts_cache(false);
    }

    fn run_test_shrink_unref(do_intra_cache_clean: bool) {
        // Enable caching so that we use the straightforward implementation
        // of shrink that will shrink all candidate slots
        let caching_enabled = true;
        let db = AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        );
        let account_key1 = Pubkey::new_unique();
        let account_key2 = Pubkey::new_unique();
        let account1 = AccountSharedData::new(1, 0, AccountSharedData::default().owner());

        // Store into slot 0
        db.store_cached(0, &[(&account_key1, &account1)]);
        db.store_cached(0, &[(&account_key2, &account1)]);
        db.add_root(0);
        if !do_intra_cache_clean {
            // If we don't want the cache doing purges before flush,
            // then we cannot flush multiple roots at once, otherwise the later
            // roots will clean the earlier roots before they are stored.
            // Thus flush the roots individually
            db.flush_accounts_cache(true, None);

            // Add an additional ref within the same slot to pubkey 1
            db.store_uncached(0, &[(&account_key1, &account1)]);
        }

        // Make account_key1 in slot 0 outdated by updating in rooted slot 1
        db.store_cached(1, &[(&account_key1, &account1)]);
        db.add_root(1);
        // Flushes all roots
        db.flush_accounts_cache(true, None);
        db.get_accounts_delta_hash(0);
        db.get_accounts_delta_hash(1);

        // Clean to remove outdated entry from slot 0
        db.clean_accounts(Some(1), false);

        // Shrink Slot 0
        let mut slot0_stores = db.storage.get_slot_storage_entries(0).unwrap();
        assert_eq!(slot0_stores.len(), 1);
        let slot0_store = slot0_stores.pop().unwrap();
        {
            let mut shrink_candidate_slots = db.shrink_candidate_slots.lock().unwrap();
            shrink_candidate_slots
                .entry(0)
                .or_default()
                .insert(slot0_store.append_vec_id(), slot0_store);
        }
        db.shrink_candidate_slots();

        // Make slot 0 dead by updating the remaining key
        db.store_cached(2, &[(&account_key2, &account1)]);
        db.add_root(2);

        // Flushes all roots
        db.flush_accounts_cache(true, None);

        // Should be one store before clean for slot 0
        assert_eq!(db.storage.get_slot_storage_entries(0).unwrap().len(), 1);
        db.get_accounts_delta_hash(2);
        db.clean_accounts(Some(2), false);

        // No stores should exist for slot 0 after clean
        assert!(db.storage.get_slot_storage_entries(0).is_none());

        // Ref count for `account_key1` (account removed earlier by shrink)
        // should be 1, since it was only stored in slot 0 and 1, and slot 0
        // is now dead
        assert_eq!(db.accounts_index.ref_count_from_storage(&account_key1), 1);
    }

    #[test]
    fn test_shrink_unref() {
        run_test_shrink_unref(false)
    }

    #[test]
    fn test_shrink_unref_with_intra_slot_cleaning() {
        run_test_shrink_unref(true)
    }

    #[test]
    fn test_partial_clean() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);
        let account_key1 = Pubkey::new_unique();
        let account_key2 = Pubkey::new_unique();
        let account1 = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        let account2 = AccountSharedData::new(2, 0, AccountSharedData::default().owner());
        let account3 = AccountSharedData::new(3, 0, AccountSharedData::default().owner());
        let account4 = AccountSharedData::new(4, 0, AccountSharedData::default().owner());

        // Store accounts into slots 0 and 1
        db.store_uncached(0, &[(&account_key1, &account1)]);
        db.store_uncached(0, &[(&account_key2, &account1)]);
        db.store_uncached(1, &[(&account_key1, &account2)]);
        db.get_accounts_delta_hash(0);
        db.get_accounts_delta_hash(1);

        db.print_accounts_stats("pre-clean1");

        // clean accounts - no accounts should be cleaned, since no rooted slots
        //
        // Checking that the uncleaned_pubkeys are not pre-maturely removed
        // such that when the slots are rooted, and can actually be cleaned, then the
        // delta keys are still there.
        db.clean_accounts(None, false);

        db.print_accounts_stats("post-clean1");
        // Check stores > 0
        assert!(!slot_stores(&db, 0).is_empty());
        assert!(!slot_stores(&db, 1).is_empty());

        // root slot 0
        db.add_root(0);

        // store into slot 2
        db.store_uncached(2, &[(&account_key2, &account3)]);
        db.store_uncached(2, &[(&account_key1, &account3)]);
        db.get_accounts_delta_hash(2);

        db.clean_accounts(None, false);
        db.print_accounts_stats("post-clean2");

        // root slots 1
        db.add_root(1);
        db.clean_accounts(None, false);

        db.print_accounts_stats("post-clean3");

        db.store_uncached(3, &[(&account_key2, &account4)]);
        db.get_accounts_delta_hash(3);
        db.add_root(3);

        // Check that we can clean where max_root=3 and slot=2 is not rooted
        db.clean_accounts(None, false);

        assert!(db.uncleaned_pubkeys.is_empty());

        db.print_accounts_stats("post-clean4");

        assert!(slot_stores(&db, 0).is_empty());
        assert!(!slot_stores(&db, 1).is_empty());
    }

    #[test]
    fn test_recycle_stores_expiration() {
        solana_logger::setup();

        let dummy_path = Path::new("");
        let dummy_slot = 12;
        let dummy_size = 1000;

        let dummy_id1 = 22;
        let entry1 = Arc::new(AccountStorageEntry::new(
            dummy_path, dummy_slot, dummy_id1, dummy_size,
        ));

        let dummy_id2 = 44;
        let entry2 = Arc::new(AccountStorageEntry::new(
            dummy_path, dummy_slot, dummy_id2, dummy_size,
        ));

        let mut recycle_stores = RecycleStores::default();
        recycle_stores.add_entry(entry1);
        recycle_stores.add_entry(entry2);
        assert_eq!(recycle_stores.entry_count(), 2);

        // no expiration for newly added entries
        let expired = recycle_stores.expire_old_entries();
        assert_eq!(
            expired
                .iter()
                .map(|e| e.append_vec_id())
                .collect::<Vec<_>>(),
            Vec::<AppendVecId>::new()
        );
        assert_eq!(
            recycle_stores
                .iter()
                .map(|(_, e)| e.append_vec_id())
                .collect::<Vec<_>>(),
            vec![dummy_id1, dummy_id2]
        );
        assert_eq!(recycle_stores.entry_count(), 2);
        assert_eq!(recycle_stores.total_bytes(), dummy_size * 2);

        // expiration for only too old entries
        recycle_stores.entries[0].0 =
            Instant::now() - Duration::from_secs(EXPIRATION_TTL_SECONDS + 1);
        let expired = recycle_stores.expire_old_entries();
        assert_eq!(
            expired
                .iter()
                .map(|e| e.append_vec_id())
                .collect::<Vec<_>>(),
            vec![dummy_id1]
        );
        assert_eq!(
            recycle_stores
                .iter()
                .map(|(_, e)| e.append_vec_id())
                .collect::<Vec<_>>(),
            vec![dummy_id2]
        );
        assert_eq!(recycle_stores.entry_count(), 1);
        assert_eq!(recycle_stores.total_bytes(), dummy_size);
    }

    const RACY_SLEEP_MS: u64 = 10;
    const RACE_TIME: u64 = 5;

    fn start_load_thread(
        with_retry: bool,
        ancestors: Ancestors,
        db: Arc<AccountsDb>,
        exit: Arc<AtomicBool>,
        pubkey: Arc<Pubkey>,
        expected_lamports: impl Fn(&(AccountSharedData, Slot)) -> u64 + Send + 'static,
    ) -> JoinHandle<()> {
        let load_hint = if with_retry {
            LoadHint::FixedMaxRoot
        } else {
            LoadHint::Unspecified
        };

        std::thread::Builder::new()
            .name("account-do-load".to_string())
            .spawn(move || {
                loop {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    // Meddle load_limit to cover all branches of implementation.
                    // There should absolutely no behaviorial difference; the load_limit triggered
                    // slow branch should only affect the performance.
                    // Ordering::Relaxed is ok because of no data dependencies; the modified field is
                    // completely free-standing cfg(test) control-flow knob.
                    db.load_limit
                        .store(thread_rng().gen_range(0, 10) as u64, Ordering::Relaxed);

                    // Load should never be unable to find this key
                    let loaded_account = db
                        .do_load(&ancestors, &pubkey, None, load_hint, false)
                        .unwrap();
                    // slot + 1 == account.lamports because of the account-cache-flush thread
                    assert_eq!(
                        loaded_account.0.lamports(),
                        expected_lamports(&loaded_account)
                    );
                }
            })
            .unwrap()
    }

    fn do_test_load_account_and_cache_flush_race(with_retry: bool) {
        solana_logger::setup();

        let caching_enabled = true;
        let mut db = AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        );
        db.load_delay = RACY_SLEEP_MS;
        let db = Arc::new(db);
        let pubkey = Arc::new(Pubkey::new_unique());
        let exit = Arc::new(AtomicBool::new(false));
        db.store_cached(
            0,
            &[(
                &pubkey,
                &AccountSharedData::new(1, 0, AccountSharedData::default().owner()),
            )],
        );
        db.add_root(0);
        db.flush_accounts_cache(true, None);

        let t_flush_accounts_cache = {
            let db = db.clone();
            let exit = exit.clone();
            let pubkey = pubkey.clone();
            let mut account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
            std::thread::Builder::new()
                .name("account-cache-flush".to_string())
                .spawn(move || {
                    let mut slot = 1;
                    loop {
                        if exit.load(Ordering::Relaxed) {
                            return;
                        }
                        account.set_lamports(slot + 1);
                        db.store_cached(slot, &[(&pubkey, &account)]);
                        db.add_root(slot);
                        sleep(Duration::from_millis(RACY_SLEEP_MS));
                        db.flush_accounts_cache(true, None);
                        slot += 1;
                    }
                })
                .unwrap()
        };

        let t_do_load = start_load_thread(
            with_retry,
            Ancestors::default(),
            db,
            exit.clone(),
            pubkey,
            |(_, slot)| slot + 1,
        );

        sleep(Duration::from_secs(RACE_TIME));
        exit.store(true, Ordering::Relaxed);
        t_flush_accounts_cache.join().unwrap();
        t_do_load.join().map_err(std::panic::resume_unwind).unwrap()
    }

    #[test]
    fn test_load_account_and_cache_flush_race_with_retry() {
        do_test_load_account_and_cache_flush_race(true);
    }

    #[test]
    fn test_load_account_and_cache_flush_race_without_retry() {
        do_test_load_account_and_cache_flush_race(false);
    }

    fn do_test_load_account_and_shrink_race(with_retry: bool) {
        let caching_enabled = true;
        let mut db = AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        );
        db.load_delay = RACY_SLEEP_MS;
        let db = Arc::new(db);
        let pubkey = Arc::new(Pubkey::new_unique());
        let exit = Arc::new(AtomicBool::new(false));
        let slot = 1;

        // Store an account
        let lamports = 42;
        let mut account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        account.set_lamports(lamports);
        db.store_uncached(slot, &[(&pubkey, &account)]);

        // Set the slot as a root so account loads will see the contents of this slot
        db.add_root(slot);

        let t_shrink_accounts = {
            let db = db.clone();
            let exit = exit.clone();

            std::thread::Builder::new()
                .name("account-shrink".to_string())
                .spawn(move || loop {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    // Simulate adding shrink candidates from clean_accounts()
                    let stores = db.storage.get_slot_storage_entries(slot).unwrap();
                    assert_eq!(stores.len(), 1);
                    let store = &stores[0];
                    let store_id = store.append_vec_id();
                    db.shrink_candidate_slots
                        .lock()
                        .unwrap()
                        .entry(slot)
                        .or_default()
                        .insert(store_id, store.clone());
                    db.shrink_candidate_slots();
                })
                .unwrap()
        };

        let t_do_load = start_load_thread(
            with_retry,
            Ancestors::default(),
            db,
            exit.clone(),
            pubkey,
            move |_| lamports,
        );

        sleep(Duration::from_secs(RACE_TIME));
        exit.store(true, Ordering::Relaxed);
        t_shrink_accounts.join().unwrap();
        t_do_load.join().map_err(std::panic::resume_unwind).unwrap()
    }

    #[test]
    fn test_load_account_and_shrink_race_with_retry() {
        do_test_load_account_and_shrink_race(true);
    }

    #[test]
    fn test_load_account_and_shrink_race_without_retry() {
        do_test_load_account_and_shrink_race(false);
    }

    #[test]
    fn test_cache_flush_delayed_remove_unrooted_race() {
        let caching_enabled = true;
        let mut db = AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        );
        db.load_delay = RACY_SLEEP_MS;
        let db = Arc::new(db);
        let slot = 10;
        let bank_id = 10;

        let lamports = 42;
        let mut account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
        account.set_lamports(lamports);

        // Start up a thread to flush the accounts cache
        let (flush_trial_start_sender, flush_trial_start_receiver) = unbounded();
        let (flush_done_sender, flush_done_receiver) = unbounded();
        let t_flush_cache = {
            let db = db.clone();
            std::thread::Builder::new()
                .name("account-cache-flush".to_string())
                .spawn(move || loop {
                    // Wait for the signal to start a trial
                    if flush_trial_start_receiver.recv().is_err() {
                        return;
                    }
                    db.flush_slot_cache(10, None::<&mut fn(&_, &_) -> bool>);
                    flush_done_sender.send(()).unwrap();
                })
                .unwrap()
        };

        // Start up a thread remove the slot
        let (remove_trial_start_sender, remove_trial_start_receiver) = unbounded();
        let (remove_done_sender, remove_done_receiver) = unbounded();
        let t_remove = {
            let db = db.clone();
            std::thread::Builder::new()
                .name("account-remove".to_string())
                .spawn(move || loop {
                    // Wait for the signal to start a trial
                    if remove_trial_start_receiver.recv().is_err() {
                        return;
                    }
                    db.remove_unrooted_slots(&[(slot, bank_id)]);
                    remove_done_sender.send(()).unwrap();
                })
                .unwrap()
        };

        let num_trials = 10;
        for _ in 0..num_trials {
            let pubkey = Pubkey::new_unique();
            db.store_cached(slot, &[(&pubkey, &account)]);
            // Wait for both threads to finish
            flush_trial_start_sender.send(()).unwrap();
            remove_trial_start_sender.send(()).unwrap();
            let _ = flush_done_receiver.recv();
            let _ = remove_done_receiver.recv();
        }

        drop(flush_trial_start_sender);
        drop(remove_trial_start_sender);
        t_flush_cache.join().unwrap();
        t_remove.join().unwrap();
    }

    #[test]
    fn test_cache_flush_remove_unrooted_race_multiple_slots() {
        let caching_enabled = true;
        let db = AccountsDb::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            caching_enabled,
            AccountShrinkThreshold::default(),
        );
        let db = Arc::new(db);
        let num_cached_slots = 100;

        let num_trials = 100;
        let (new_trial_start_sender, new_trial_start_receiver) = unbounded();
        let (flush_done_sender, flush_done_receiver) = unbounded();
        // Start up a thread to flush the accounts cache
        let t_flush_cache = {
            let db = db.clone();

            std::thread::Builder::new()
                .name("account-cache-flush".to_string())
                .spawn(move || loop {
                    // Wait for the signal to start a trial
                    if new_trial_start_receiver.recv().is_err() {
                        return;
                    }
                    for slot in 0..num_cached_slots {
                        db.flush_slot_cache(slot, None::<&mut fn(&_, &_) -> bool>);
                    }
                    flush_done_sender.send(()).unwrap();
                })
                .unwrap()
        };

        let exit = Arc::new(AtomicBool::new(false));

        let t_spurious_signal = {
            let db = db.clone();
            let exit = exit.clone();
            std::thread::Builder::new()
                .name("account-cache-flush".to_string())
                .spawn(move || loop {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    // Simulate spurious wake-up that can happen, but is too rare to
                    // otherwise depend on in tests.
                    db.remove_unrooted_slots_synchronization.signal.notify_all();
                })
                .unwrap()
        };

        // Run multiple trials. Has the added benefit of rewriting the same slots after we've
        // dumped them in previous trials.
        for _ in 0..num_trials {
            // Store an account
            let lamports = 42;
            let mut account = AccountSharedData::new(1, 0, AccountSharedData::default().owner());
            account.set_lamports(lamports);

            // Pick random 50% of the slots to pass to `remove_unrooted_slots()`
            let mut all_slots: Vec<(Slot, BankId)> = (0..num_cached_slots)
                .map(|slot| {
                    let bank_id = slot + 1;
                    (slot, bank_id)
                })
                .collect();
            all_slots.shuffle(&mut rand::thread_rng());
            let slots_to_dump = &all_slots[0..num_cached_slots as usize / 2];
            let slots_to_keep = &all_slots[num_cached_slots as usize / 2..];

            // Set up a one account per slot across many different slots, track which
            // pubkey was stored in each slot.
            let slot_to_pubkey_map: HashMap<Slot, Pubkey> = (0..num_cached_slots)
                .map(|slot| {
                    let pubkey = Pubkey::new_unique();
                    db.store_cached(slot, &[(&pubkey, &account)]);
                    (slot, pubkey)
                })
                .collect();

            // Signal the flushing shred to start flushing
            new_trial_start_sender.send(()).unwrap();

            // Here we want to test both:
            // 1) Flush thread starts flushing a slot before we try dumping it.
            // 2) Flushing thread trying to flush while/after we're trying to dump the slot,
            // in which case flush should ignore/move past the slot to be dumped
            //
            // Hence, we split into chunks to get the dumping of each chunk to race with the
            // flushes. If we were to dump the entire chunk at once, then this reduces the possibility
            // of the flush occurring first since the dumping logic reserves all the slots it's about
            // to dump immediately.

            for chunks in slots_to_dump.chunks(slots_to_dump.len() / 2) {
                db.remove_unrooted_slots(chunks);
            }

            // Check that all the slots in `slots_to_dump` were completely removed from the
            // cache, storage, and index

            for (slot, _) in slots_to_dump {
                assert!(db.storage.get_slot_storage_entries(*slot).is_none());
                assert!(db.accounts_cache.slot_cache(*slot).is_none());
                let account_in_slot = slot_to_pubkey_map[slot];
                assert!(db
                    .accounts_index
                    .get_account_read_entry(&account_in_slot)
                    .is_none());
            }

            // Wait for flush to finish before starting next trial

            flush_done_receiver.recv().unwrap();

            for (slot, bank_id) in slots_to_keep {
                let account_in_slot = slot_to_pubkey_map[slot];
                assert!(db
                    .load(
                        &Ancestors::from(vec![(*slot, 0)]),
                        &account_in_slot,
                        LoadHint::FixedMaxRoot
                    )
                    .is_some());
                // Clear for next iteration so that `assert!(self.storage.get_slot_stores(purged_slot).is_none());`
                // in `purge_slot_pubkeys()` doesn't trigger
                db.remove_unrooted_slots(&[(*slot, *bank_id)]);
            }
        }

        exit.store(true, Ordering::Relaxed);
        drop(new_trial_start_sender);
        t_flush_cache.join().unwrap();

        t_spurious_signal.join().unwrap();
    }

    #[test]
    fn test_collect_uncleaned_slots_up_to_slot() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let slot1 = 11;
        let slot2 = 222;
        let slot3 = 3333;

        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();
        let pubkey3 = Pubkey::new_unique();

        db.uncleaned_pubkeys.insert(slot1, vec![pubkey1]);
        db.uncleaned_pubkeys.insert(slot2, vec![pubkey2]);
        db.uncleaned_pubkeys.insert(slot3, vec![pubkey3]);

        let mut uncleaned_slots1 = db.collect_uncleaned_slots_up_to_slot(slot1);
        let mut uncleaned_slots2 = db.collect_uncleaned_slots_up_to_slot(slot2);
        let mut uncleaned_slots3 = db.collect_uncleaned_slots_up_to_slot(slot3);

        uncleaned_slots1.sort_unstable();
        uncleaned_slots2.sort_unstable();
        uncleaned_slots3.sort_unstable();

        assert_eq!(uncleaned_slots1, [slot1]);
        assert_eq!(uncleaned_slots2, [slot1, slot2]);
        assert_eq!(uncleaned_slots3, [slot1, slot2, slot3]);
    }

    #[test]
    fn test_remove_uncleaned_slots_and_collect_pubkeys() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let slot1 = 11;
        let slot2 = 222;
        let slot3 = 3333;

        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();
        let pubkey3 = Pubkey::new_unique();

        let account1 = AccountSharedData::new(0, 0, &pubkey1);
        let account2 = AccountSharedData::new(0, 0, &pubkey2);
        let account3 = AccountSharedData::new(0, 0, &pubkey3);

        db.store_uncached(slot1, &[(&pubkey1, &account1)]);
        db.store_uncached(slot2, &[(&pubkey2, &account2)]);
        db.store_uncached(slot3, &[(&pubkey3, &account3)]);

        db.add_root(slot1);
        // slot 2 is _not_ a root on purpose
        db.add_root(slot3);

        db.uncleaned_pubkeys.insert(slot1, vec![pubkey1]);
        db.uncleaned_pubkeys.insert(slot2, vec![pubkey2]);
        db.uncleaned_pubkeys.insert(slot3, vec![pubkey3]);

        let uncleaned_pubkeys1 = db
            .remove_uncleaned_slots_and_collect_pubkeys(vec![slot1])
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let uncleaned_pubkeys2 = db
            .remove_uncleaned_slots_and_collect_pubkeys(vec![slot2])
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let uncleaned_pubkeys3 = db
            .remove_uncleaned_slots_and_collect_pubkeys(vec![slot3])
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert!(uncleaned_pubkeys1.contains(&pubkey1));
        assert!(!uncleaned_pubkeys1.contains(&pubkey2));
        assert!(!uncleaned_pubkeys1.contains(&pubkey3));

        assert!(!uncleaned_pubkeys2.contains(&pubkey1));
        assert!(uncleaned_pubkeys2.contains(&pubkey2));
        assert!(!uncleaned_pubkeys2.contains(&pubkey3));

        assert!(!uncleaned_pubkeys3.contains(&pubkey1));
        assert!(!uncleaned_pubkeys3.contains(&pubkey2));
        assert!(uncleaned_pubkeys3.contains(&pubkey3));
    }

    #[test]
    fn test_remove_uncleaned_slots_and_collect_pubkeys_up_to_slot() {
        solana_logger::setup();
        let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

        let slot1 = 11;
        let slot2 = 222;
        let slot3 = 3333;

        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();
        let pubkey3 = Pubkey::new_unique();

        let account1 = AccountSharedData::new(0, 0, &pubkey1);
        let account2 = AccountSharedData::new(0, 0, &pubkey2);
        let account3 = AccountSharedData::new(0, 0, &pubkey3);

        db.store_uncached(slot1, &[(&pubkey1, &account1)]);
        db.store_uncached(slot2, &[(&pubkey2, &account2)]);
        db.store_uncached(slot3, &[(&pubkey3, &account3)]);

        // slot 1 is _not_ a root on purpose
        db.add_root(slot2);
        db.add_root(slot3);

        db.uncleaned_pubkeys.insert(slot1, vec![pubkey1]);
        db.uncleaned_pubkeys.insert(slot2, vec![pubkey2]);
        db.uncleaned_pubkeys.insert(slot3, vec![pubkey3]);

        let uncleaned_pubkeys = db
            .remove_uncleaned_slots_and_collect_pubkeys_up_to_slot(slot3)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert!(uncleaned_pubkeys.contains(&pubkey1));
        assert!(uncleaned_pubkeys.contains(&pubkey2));
        assert!(uncleaned_pubkeys.contains(&pubkey3));
    }

    #[test]
    fn test_shrink_productive() {
        solana_logger::setup();
        let s1 = AccountStorageEntry::new(Path::new("."), 0, 0, 1024);
        let stores = vec![Arc::new(s1)];
        assert!(!AccountsDb::is_shrinking_productive(0, &stores));

        let s1 = AccountStorageEntry::new(Path::new("."), 0, 0, PAGE_SIZE * 4);
        let stores = vec![Arc::new(s1)];
        stores[0].add_account((3 * PAGE_SIZE as usize) - 1);
        stores[0].add_account(10);
        stores[0].remove_account(10, false);
        assert!(AccountsDb::is_shrinking_productive(0, &stores));

        stores[0].add_account(PAGE_SIZE as usize);
        assert!(!AccountsDb::is_shrinking_productive(0, &stores));

        let s1 = AccountStorageEntry::new(Path::new("."), 0, 0, PAGE_SIZE + 1);
        s1.add_account(PAGE_SIZE as usize);
        let s2 = AccountStorageEntry::new(Path::new("."), 0, 1, PAGE_SIZE + 1);
        s2.add_account(PAGE_SIZE as usize);
        let stores = vec![Arc::new(s1), Arc::new(s2)];
        assert!(AccountsDb::is_shrinking_productive(0, &stores));
    }

    #[test]
    fn test_is_candidate_for_shrink() {
        solana_logger::setup();

        let mut accounts = AccountsDb::new_single();
        let dummy_path = Path::new("");
        let dummy_size = 2 * PAGE_SIZE;
        let entry = Arc::new(AccountStorageEntry::new(dummy_path, 0, 1, dummy_size));
        match accounts.shrink_ratio {
            AccountShrinkThreshold::TotalSpace { shrink_ratio } => {
                assert_eq!(
                    (DEFAULT_ACCOUNTS_SHRINK_RATIO * 100.) as u64,
                    (shrink_ratio * 100.) as u64
                )
            }
            AccountShrinkThreshold::IndividalStore { shrink_ratio: _ } => {
                panic!("Expect the default to be TotalSpace")
            }
        }
        entry.alive_bytes.store(3000, Ordering::Relaxed);
        assert!(accounts.is_candidate_for_shrink(&entry));
        entry.alive_bytes.store(5000, Ordering::Relaxed);
        assert!(!accounts.is_candidate_for_shrink(&entry));
        accounts.shrink_ratio = AccountShrinkThreshold::TotalSpace { shrink_ratio: 0.3 };
        entry.alive_bytes.store(3000, Ordering::Relaxed);
        assert!(accounts.is_candidate_for_shrink(&entry));
        accounts.shrink_ratio = AccountShrinkThreshold::IndividalStore { shrink_ratio: 0.3 };
        assert!(!accounts.is_candidate_for_shrink(&entry));
    }

    #[test]
    fn test_calculate_storage_count_and_alive_bytes() {
        let accounts = AccountsDb::new_single();
        let shared_key = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(1, 1, AccountSharedData::default().owner());
        let slot0 = 0;
        accounts.store_uncached(slot0, &[(&shared_key, &account)]);

        let result =
            accounts.calculate_storage_count_and_alive_bytes(&mut GenerateIndexTimings::default());
        assert_eq!(result.len(), 1);
        for (k, v) in result.iter() {
            assert_eq!((k, v), (&0, &(144, 1)));
        }
    }

    #[test]
    fn test_calculate_storage_count_and_alive_bytes_0_accounts() {
        let accounts = AccountsDb::new_single();
        let result =
            accounts.calculate_storage_count_and_alive_bytes(&mut GenerateIndexTimings::default());
        assert!(result.is_empty());
    }

    #[test]
    fn test_calculate_storage_count_and_alive_bytes_2_accounts() {
        let accounts = AccountsDb::new_single();
        let keys = [
            solana_sdk::pubkey::Pubkey::new(&[0; 32]),
            solana_sdk::pubkey::Pubkey::new(&[255; 32]),
        ];
        // make sure accounts are in 2 different bins
        assert!(
            crate::accounts_index::get_bin_pubkey(&keys[0])
                != crate::accounts_index::get_bin_pubkey(&keys[1])
        );
        let account = AccountSharedData::new(1, 1, AccountSharedData::default().owner());
        let account_big = AccountSharedData::new(1, 1000, AccountSharedData::default().owner());
        let slot0 = 0;
        accounts.store_uncached(slot0, &[(&keys[0], &account)]);
        accounts.store_uncached(slot0, &[(&keys[1], &account_big)]);

        let result =
            accounts.calculate_storage_count_and_alive_bytes(&mut GenerateIndexTimings::default());
        assert_eq!(result.len(), 1);
        for (k, v) in result.iter() {
            assert_eq!((k, v), (&0, &(1280, 2)));
        }
    }

    #[test]
    fn test_set_storage_count_and_alive_bytes() {
        let accounts = AccountsDb::new_single();

        // make sure we have storage 0
        let shared_key = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(1, 1, AccountSharedData::default().owner());
        let slot0 = 0;
        accounts.store_uncached(slot0, &[(&shared_key, &account)]);

        // fake out the store count to avoid the assert
        for slot_stores in accounts.storage.0.iter() {
            for (_id, store) in slot_stores.value().read().unwrap().iter() {
                store.alive_bytes.store(0, Ordering::SeqCst);
            }
        }

        // populate based on made up hash data
        let mut hashmap = HashMap::default();
        hashmap.insert(0, (2, 3));
        accounts.set_storage_count_and_alive_bytes(hashmap, &mut GenerateIndexTimings::default());
        assert_eq!(accounts.storage.0.len(), 1);
        for slot_stores in accounts.storage.0.iter() {
            for (id, store) in slot_stores.value().read().unwrap().iter() {
                assert_eq!(id, &0);
                assert_eq!(store.count_and_status.read().unwrap().0, 3);
                assert_eq!(store.alive_bytes.load(Ordering::SeqCst), 2);
            }
        }
    }

    #[test]
    fn test_purge_alive_unrooted_slots_after_clean() {
        let accounts = AccountsDb::new_single();

        // Key shared between rooted and nonrooted slot
        let shared_key = solana_sdk::pubkey::new_rand();
        // Key to keep the storage entry for the unrooted slot alive
        let unrooted_key = solana_sdk::pubkey::new_rand();
        let slot0 = 0;
        let slot1 = 1;

        // Store accounts with greater than 0 lamports
        let account = AccountSharedData::new(1, 1, AccountSharedData::default().owner());
        accounts.store_uncached(slot0, &[(&shared_key, &account)]);
        accounts.store_uncached(slot0, &[(&unrooted_key, &account)]);

        // Simulate adding dirty pubkeys on bank freeze. Note this is
        // not a rooted slot
        accounts.get_accounts_delta_hash(slot0);

        // On the next *rooted* slot, update the `shared_key` account to zero lamports
        let zero_lamport_account =
            AccountSharedData::new(0, 0, AccountSharedData::default().owner());
        accounts.store_uncached(slot1, &[(&shared_key, &zero_lamport_account)]);

        // Simulate adding dirty pubkeys on bank freeze, set root
        accounts.get_accounts_delta_hash(slot1);
        accounts.add_root(slot1);

        // The later rooted zero-lamport update to `shared_key` cannot be cleaned
        // because it is kept alive by the unrooted slot.
        accounts.clean_accounts(None, false);
        assert!(accounts
            .accounts_index
            .get_account_read_entry(&shared_key)
            .is_some());

        // Simulate purge_slot() all from AccountsBackgroundService
        let is_from_abs = true;
        accounts.purge_slot(slot0, 0, is_from_abs);

        // Now clean should clean up the remaining key
        accounts.clean_accounts(None, false);
        assert!(accounts
            .accounts_index
            .get_account_read_entry(&shared_key)
            .is_none());
        assert!(accounts.storage.get_slot_storage_entries(slot0).is_none());
    }
}
