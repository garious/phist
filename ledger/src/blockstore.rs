//! The `blockstore` module provides functions for parallel verification of the
//! Proof of History ledger as well as iterative read, append write, and random
//! access read to a persistent file-based ledger.
use crate::{
    ancestor_iterator::AncestorIterator,
    blockstore_db::{
        columns as cf, AccessType, BlockstoreRecoveryMode, Column, Database, IteratorDirection,
        IteratorMode, LedgerColumn, Result, WriteBatch,
    },
    blockstore_meta::*,
    erasure::ErasureConfig,
    leader_schedule_cache::LeaderScheduleCache,
    next_slots_iterator::NextSlotsIterator,
    shred::{Result as ShredResult, Shred, Shredder, MAX_DATA_SHREDS_PER_FEC_BLOCK},
};
pub use crate::{blockstore_db::BlockstoreError, blockstore_meta::SlotMeta};
use bincode::deserialize;
use dashmap::DashMap;
use log::*;
use rayon::{
    iter::{IntoParallelRefIterator, ParallelIterator},
    ThreadPool,
};
use solana_entry::entry::{create_ticks, Entry};
use solana_measure::measure::Measure;
use solana_metrics::{datapoint_debug, datapoint_error};
use solana_rayon_threadlimit::get_thread_count;
use solana_runtime::hardened_unpack::{unpack_genesis_archive, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE};
use solana_sdk::{
    clock::{Slot, UnixTimestamp, DEFAULT_TICKS_PER_SECOND, MS_PER_TICK},
    genesis_config::{GenesisConfig, DEFAULT_GENESIS_ARCHIVE, DEFAULT_GENESIS_FILE},
    hash::Hash,
    pubkey::Pubkey,
    sanitize::Sanitize,
    signature::{Keypair, Signature, Signer},
    timing::timestamp,
    transaction::Transaction,
};
use solana_storage_proto::{StoredExtendedRewards, StoredTransactionStatusMeta};
use solana_transaction_status::{
    ConfirmedBlock, ConfirmedTransaction, ConfirmedTransactionStatusWithSignature, Rewards,
    TransactionStatusMeta, TransactionWithStatusMeta,
};
use std::{
    borrow::Cow,
    cell::RefCell,
    cmp,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    convert::TryInto,
    fs,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{sync_channel, Receiver, SyncSender, TrySendError},
        Arc, Mutex, RwLock, RwLockWriteGuard,
    },
    time::Instant,
};

use tempfile::TempDir;
use thiserror::Error;
use trees::{Tree, TreeWalk};

pub mod blockstore_purge;
mod blockstore_shreds;
use blockstore_shreds::{ShredWAL, DATA_SHRED_DIRECTORY, DEFAULT_MAX_WAL_SHREDS, SHRED_DIRECTORY};

pub const BLOCKSTORE_DIRECTORY: &str = "rocksdb";

thread_local!(static PAR_THREAD_POOL: RefCell<ThreadPool> = RefCell::new(rayon::ThreadPoolBuilder::new()
                    .num_threads(get_thread_count())
                    .thread_name(|ix| format!("blockstore_{}", ix))
                    .build()
                    .unwrap()));

thread_local!(static PAR_THREAD_POOL_ALL_CPUS: RefCell<ThreadPool> = RefCell::new(rayon::ThreadPoolBuilder::new()
                    .num_threads(num_cpus::get())
                    .thread_name(|ix| format!("blockstore_{}", ix))
                    .build()
                    .unwrap()));

pub const MAX_COMPLETED_SLOTS_IN_CHANNEL: usize = 100_000;
pub const MAX_TURBINE_PROPAGATION_IN_MS: u64 = 100;
pub const MAX_TURBINE_DELAY_IN_TICKS: u64 = MAX_TURBINE_PROPAGATION_IN_MS / MS_PER_TICK;

// An upper bound on maximum number of data shreds we can handle in a slot
// 32K shreds would allow ~320K peak TPS
// (32K shreds per slot * 4 TX per shred * 2.5 slots per sec)
pub const MAX_DATA_SHREDS_PER_SLOT: usize = 32_768;

pub type CompletedSlotsSender = SyncSender<Vec<Slot>>;
pub type CompletedSlotsReceiver = Receiver<Vec<Slot>>;
type CompletedRanges = Vec<(u32, u32)>;

#[derive(Clone, Copy)]
pub enum PurgeType {
    Exact,
    PrimaryIndex,
    CompactionFilter,
}

#[derive(Error, Debug)]
pub enum InsertDataShredError {
    Exists,
    InvalidShred,
    BlockstoreError(#[from] BlockstoreError),
}

impl std::fmt::Display for InsertDataShredError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "insert data shred error")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompletedDataSetInfo {
    pub slot: Slot,
    pub start_index: u32,
    pub end_index: u32,
}

pub struct BlockstoreSignals {
    pub blockstore: Blockstore,
    pub ledger_signal_receiver: Receiver<bool>,
    pub completed_slots_receiver: CompletedSlotsReceiver,
}

type ShredCache = BTreeMap<u64, Vec<u8>>;

// ledger window
pub struct Blockstore {
    ledger_path: PathBuf,
    db: Arc<Database>,
    meta_cf: LedgerColumn<cf::SlotMeta>,
    dead_slots_cf: LedgerColumn<cf::DeadSlots>,
    duplicate_slots_cf: LedgerColumn<cf::DuplicateSlots>,
    erasure_meta_cf: LedgerColumn<cf::ErasureMeta>,
    orphans_cf: LedgerColumn<cf::Orphans>,
    index_cf: LedgerColumn<cf::Index>,
    code_shred_cf: LedgerColumn<cf::ShredCode>,
    transaction_status_cf: LedgerColumn<cf::TransactionStatus>,
    address_signatures_cf: LedgerColumn<cf::AddressSignatures>,
    transaction_status_index_cf: LedgerColumn<cf::TransactionStatusIndex>,
    active_transaction_status_index: RwLock<u64>,
    rewards_cf: LedgerColumn<cf::Rewards>,
    blocktime_cf: LedgerColumn<cf::Blocktime>,
    perf_samples_cf: LedgerColumn<cf::PerfSamples>,
    block_height_cf: LedgerColumn<cf::BlockHeight>,
    program_costs_cf: LedgerColumn<cf::ProgramCosts>,
    bank_hash_cf: LedgerColumn<cf::BankHash>,
    last_root: Arc<RwLock<Slot>>,
    insert_shreds_lock: Arc<Mutex<()>>,
    pub new_shreds_signals: Vec<SyncSender<bool>>,
    pub completed_slots_senders: Vec<CompletedSlotsSender>,
    pub lowest_cleanup_slot: Arc<RwLock<Slot>>,
    no_compaction: bool,
    slots_stats: Arc<Mutex<SlotsStats>>,
    data_shred_path: PathBuf,
    data_shred_cache: DashMap<Slot, Arc<RwLock<ShredCache>>>,
    // TODO: probably don't want this pub, work in a helper function for ledger_cleanup
    pub data_shred_cache_slots: Mutex<BTreeSet<Slot>>,
    shred_wal: Mutex<ShredWAL>,
}

struct SlotsStats {
    last_cleanup_ts: Instant,
    stats: BTreeMap<Slot, SlotStats>,
}

impl Default for SlotsStats {
    fn default() -> Self {
        SlotsStats {
            last_cleanup_ts: Instant::now(),
            stats: BTreeMap::new(),
        }
    }
}

#[derive(Default)]
struct SlotStats {
    num_repaired: usize,
    num_recovered: usize,
}

pub struct IndexMetaWorkingSetEntry {
    index: Index,
    // true only if at least one shred for this Index was inserted since the time this
    // struct was created
    did_insert_occur: bool,
}

pub struct SlotMetaWorkingSetEntry {
    new_slot_meta: Rc<RefCell<SlotMeta>>,
    old_slot_meta: Option<SlotMeta>,
    // True only if at least one shred for this SlotMeta was inserted since the time this
    // struct was created.
    did_insert_occur: bool,
}

#[derive(PartialEq, Debug, Clone)]
enum ShredSource {
    Turbine,
    Repaired,
    Recovered,
}

#[derive(Default)]
pub struct BlockstoreInsertionMetrics {
    pub num_shreds: usize,
    pub insert_lock_elapsed: u64,
    pub insert_shreds_elapsed: u64,
    pub shred_recovery_elapsed: u64,
    pub chaining_elapsed: u64,
    pub commit_working_sets_elapsed: u64,
    pub write_batch_elapsed: u64,
    pub total_elapsed: u64,
    pub num_inserted: u64,
    pub num_repair: u64,
    pub num_recovered: usize,
    pub num_recovered_inserted: usize,
    pub num_recovered_failed_sig: usize,
    pub num_recovered_failed_invalid: usize,
    pub num_recovered_exists: usize,
    pub index_meta_time: u64,
}

impl SlotMetaWorkingSetEntry {
    fn new(new_slot_meta: Rc<RefCell<SlotMeta>>, old_slot_meta: Option<SlotMeta>) -> Self {
        Self {
            new_slot_meta,
            old_slot_meta,
            did_insert_occur: false,
        }
    }
}

impl BlockstoreInsertionMetrics {
    pub fn report_metrics(&self, metric_name: &'static str) {
        datapoint_info!(
            metric_name,
            ("num_shreds", self.num_shreds as i64, i64),
            ("total_elapsed", self.total_elapsed as i64, i64),
            ("insert_lock_elapsed", self.insert_lock_elapsed as i64, i64),
            (
                "insert_shreds_elapsed",
                self.insert_shreds_elapsed as i64,
                i64
            ),
            (
                "shred_recovery_elapsed",
                self.shred_recovery_elapsed as i64,
                i64
            ),
            ("chaining_elapsed", self.chaining_elapsed as i64, i64),
            (
                "commit_working_sets_elapsed",
                self.commit_working_sets_elapsed as i64,
                i64
            ),
            ("write_batch_elapsed", self.write_batch_elapsed as i64, i64),
            ("num_inserted", self.num_inserted as i64, i64),
            ("num_repair", self.num_repair as i64, i64),
            ("num_recovered", self.num_recovered as i64, i64),
            (
                "num_recovered_inserted",
                self.num_recovered_inserted as i64,
                i64
            ),
            (
                "num_recovered_failed_sig",
                self.num_recovered_failed_sig as i64,
                i64
            ),
            (
                "num_recovered_failed_invalid",
                self.num_recovered_failed_invalid as i64,
                i64
            ),
            (
                "num_recovered_exists",
                self.num_recovered_exists as i64,
                i64
            ),
        );
    }
}

impl Blockstore {
    pub fn db(self) -> Arc<Database> {
        self.db
    }

    pub fn ledger_path(&self) -> &Path {
        &self.ledger_path
    }

    /// Opens a Ledger in directory, provides "infinite" window of shreds
    pub fn open(ledger_path: &Path) -> Result<Blockstore> {
        Self::do_open(ledger_path, AccessType::PrimaryOnly, None, true)
    }

    pub fn open_with_access_type(
        ledger_path: &Path,
        access_type: AccessType,
        recovery_mode: Option<BlockstoreRecoveryMode>,
        enforce_ulimit_nofile: bool,
    ) -> Result<Blockstore> {
        Self::do_open(
            ledger_path,
            access_type,
            recovery_mode,
            enforce_ulimit_nofile,
        )
    }

    fn do_open(
        ledger_path: &Path,
        access_type: AccessType,
        recovery_mode: Option<BlockstoreRecoveryMode>,
        enforce_ulimit_nofile: bool,
    ) -> Result<Blockstore> {
        fs::create_dir_all(&ledger_path)?;
        let blockstore_path = ledger_path.join(BLOCKSTORE_DIRECTORY);
        let shred_db_path = ledger_path.join(SHRED_DIRECTORY);
        let data_shred_path = shred_db_path.join(DATA_SHRED_DIRECTORY);
        fs::create_dir_all(&data_shred_path)?;

        adjust_ulimit_nofile(enforce_ulimit_nofile)?;

        // Open the database
        let mut measure = Measure::start("open");
        info!("Opening database at {:?}", blockstore_path);
        let db = Database::open(&blockstore_path, access_type, recovery_mode)?;

        // Create the metadata column family
        let meta_cf = db.column();

        // Create the dead slots column family
        let dead_slots_cf = db.column();
        let duplicate_slots_cf = db.column();
        let erasure_meta_cf = db.column();

        // Create the orphans column family. An "orphan" is defined as
        // the head of a detached chain of slots, i.e. a slot with no
        // known parent
        let orphans_cf = db.column();
        let index_cf = db.column();

        let code_shred_cf = db.column();
        let transaction_status_cf = db.column();
        let address_signatures_cf = db.column();
        let transaction_status_index_cf = db.column();
        let rewards_cf = db.column();
        let blocktime_cf = db.column();
        let perf_samples_cf = db.column();
        let block_height_cf = db.column();
        let program_costs_cf = db.column();
        let bank_hash_cf = db.column();

        let db = Arc::new(db);

        // Get max root or 0 if it doesn't exist
        let max_root = db
            .iter::<cf::Root>(IteratorMode::End)?
            .next()
            .map(|(slot, _)| slot)
            .unwrap_or(0);
        let last_root = Arc::new(RwLock::new(max_root));

        // Get active transaction-status index or 0
        let active_transaction_status_index = db
            .iter::<cf::TransactionStatusIndex>(IteratorMode::Start)?
            .next();
        let initialize_transaction_status_index = active_transaction_status_index.is_none();
        let active_transaction_status_index = active_transaction_status_index
            .and_then(|(_, data)| {
                let index0: TransactionStatusIndexMeta = deserialize(&data).unwrap();
                if index0.frozen {
                    Some(1)
                } else {
                    None
                }
            })
            .unwrap_or(0);

        let shred_wal = ShredWAL::new(&shred_db_path, DEFAULT_MAX_WAL_SHREDS)?;
        let shred_wal = Mutex::new(shred_wal);

        measure.stop();
        info!("{:?} {}", blockstore_path, measure);
        let blockstore = Blockstore {
            ledger_path: ledger_path.to_path_buf(),
            db,
            meta_cf,
            dead_slots_cf,
            duplicate_slots_cf,
            erasure_meta_cf,
            orphans_cf,
            index_cf,
            code_shred_cf,
            transaction_status_cf,
            address_signatures_cf,
            transaction_status_index_cf,
            active_transaction_status_index: RwLock::new(active_transaction_status_index),
            rewards_cf,
            blocktime_cf,
            perf_samples_cf,
            block_height_cf,
            program_costs_cf,
            bank_hash_cf,
            new_shreds_signals: vec![],
            completed_slots_senders: vec![],
            insert_shreds_lock: Arc::new(Mutex::new(())),
            last_root,
            lowest_cleanup_slot: Arc::new(RwLock::new(0)),
            no_compaction: false,
            slots_stats: Arc::new(Mutex::new(SlotsStats::default())),
            data_shred_path,
            data_shred_cache: DashMap::new(),
            data_shred_cache_slots: Mutex::new(BTreeSet::new()),
            shred_wal,
        };
        if initialize_transaction_status_index {
            blockstore.initialize_transaction_status_index()?;
        }
        blockstore.recover()?;
        Ok(blockstore)
    }

    pub fn open_with_signal(
        ledger_path: &Path,
        recovery_mode: Option<BlockstoreRecoveryMode>,
        enforce_ulimit_nofile: bool,
    ) -> Result<BlockstoreSignals> {
        let mut blockstore = Self::open_with_access_type(
            ledger_path,
            AccessType::PrimaryOnly,
            recovery_mode,
            enforce_ulimit_nofile,
        )?;
        let (ledger_signal_sender, ledger_signal_receiver) = sync_channel(1);
        let (completed_slots_sender, completed_slots_receiver) =
            sync_channel(MAX_COMPLETED_SLOTS_IN_CHANNEL);

        blockstore.new_shreds_signals = vec![ledger_signal_sender];
        blockstore.completed_slots_senders = vec![completed_slots_sender];

        Ok(BlockstoreSignals {
            blockstore,
            ledger_signal_receiver,
            completed_slots_receiver,
        })
    }

    pub fn add_tree(
        &self,
        forks: Tree<Slot>,
        is_orphan: bool,
        is_slot_complete: bool,
        num_ticks: u64,
        starting_hash: Hash,
    ) {
        let mut walk = TreeWalk::from(forks);
        let mut blockhashes = HashMap::new();
        while let Some(visit) = walk.get() {
            let slot = *visit.node().data();
            if self.meta(slot).unwrap().is_some() && self.orphan(slot).unwrap().is_none() {
                // If slot exists in blockstore and is not an orphan, then skip it
                walk.forward();
                continue;
            }
            let parent = walk.get_parent().map(|n| *n.data());
            if parent.is_some() || !is_orphan {
                let parent_hash = parent
                    // parent won't exist for first node in a tree where
                    // `is_orphan == true`
                    .and_then(|parent| blockhashes.get(&parent))
                    .unwrap_or(&starting_hash);
                let mut entries = create_ticks(
                    num_ticks * (std::cmp::max(1, slot - parent.unwrap_or(slot))),
                    0,
                    *parent_hash,
                );
                blockhashes.insert(slot, entries.last().unwrap().hash);
                if !is_slot_complete {
                    entries.pop().unwrap();
                }
                let shreds = entries_to_test_shreds(
                    entries.clone(),
                    slot,
                    parent.unwrap_or(slot),
                    is_slot_complete,
                    0,
                );
                self.insert_shreds(shreds, None, false).unwrap();
            }
            walk.forward();
        }
    }

    pub fn set_no_compaction(&mut self, no_compaction: bool) {
        self.no_compaction = no_compaction;
    }

    pub fn destroy(ledger_path: &Path) -> Result<()> {
        // destroy() calls will fail if the path doesn't exist
        let blockstore_path = ledger_path.join(BLOCKSTORE_DIRECTORY);
        let shred_path = ledger_path.join(SHRED_DIRECTORY);
        fs::create_dir_all(&ledger_path)?;
        Database::destroy(&blockstore_path)?;
        Self::destroy_shreds(&shred_path)
    }

    pub fn meta(&self, slot: Slot) -> Result<Option<SlotMeta>> {
        self.meta_cf.get(slot)
    }

    pub fn is_full(&self, slot: Slot) -> bool {
        if let Ok(Some(meta)) = self.meta_cf.get(slot) {
            return meta.is_full();
        }
        false
    }

    pub fn erasure_meta(&self, slot: Slot, set_index: u64) -> Result<Option<ErasureMeta>> {
        self.erasure_meta_cf.get((slot, set_index))
    }

    pub fn orphan(&self, slot: Slot) -> Result<Option<bool>> {
        self.orphans_cf.get(slot)
    }

    // Get max root or 0 if it doesn't exist
    pub fn max_root(&self) -> Slot {
        self.db
            .iter::<cf::Root>(IteratorMode::End)
            .expect("Couldn't get rooted iterator for max_root()")
            .next()
            .map(|(slot, _)| slot)
            .unwrap_or(0)
    }

    pub fn slot_meta_iterator(
        &self,
        slot: Slot,
    ) -> Result<impl Iterator<Item = (Slot, SlotMeta)> + '_> {
        let meta_iter = self
            .db
            .iter::<cf::SlotMeta>(IteratorMode::From(slot, IteratorDirection::Forward))?;
        Ok(meta_iter.map(|(slot, slot_meta_bytes)| {
            (
                slot,
                deserialize(&slot_meta_bytes).unwrap_or_else(|e| {
                    panic!("Could not deserialize SlotMeta for slot {}: {:?}", slot, e)
                }),
            )
        }))
    }

    #[allow(dead_code)]
    pub fn live_slots_iterator(&self, root: Slot) -> impl Iterator<Item = (Slot, SlotMeta)> + '_ {
        let root_forks = NextSlotsIterator::new(root, self);

        let orphans_iter = self.orphans_iterator(root + 1).unwrap();
        root_forks.chain(orphans_iter.flat_map(move |orphan| NextSlotsIterator::new(orphan, self)))
    }

    pub fn slot_coding_iterator(
        &self,
        slot: Slot,
        index: u64,
    ) -> Result<impl Iterator<Item = ((u64, u64), Box<[u8]>)> + '_> {
        let slot_iterator = self.db.iter::<cf::ShredCode>(IteratorMode::From(
            (slot, index),
            IteratorDirection::Forward,
        ))?;
        Ok(slot_iterator.take_while(move |((shred_slot, _), _)| *shred_slot == slot))
    }

    pub fn rooted_slot_iterator(&self, slot: Slot) -> Result<impl Iterator<Item = u64> + '_> {
        let slot_iterator = self
            .db
            .iter::<cf::Root>(IteratorMode::From(slot, IteratorDirection::Forward))?;
        Ok(slot_iterator.map(move |(rooted_slot, _)| rooted_slot))
    }

    fn get_recovery_data_shreds(
        &self,
        index: &mut Index,
        set_index: u64,
        slot: Slot,
        erasure_meta: &ErasureMeta,
        available_shreds: &mut Vec<Shred>,
        prev_inserted_datas: &HashMap<(u64, u64), Shred>,
    ) {
        (set_index..set_index + erasure_meta.config.num_data() as u64).for_each(|i| {
            if index.data().is_present(i) {
                if let Some(shred) = match prev_inserted_datas.get(&(slot, i)) {
                    // prev_inserted_datas not yet in blockstore, so must copy it out
                    Some(shred_ref) => Some(shred_ref.clone()),
                    None => {
                        let some_data = self
                            .get_data_shred(slot, i)
                            .expect("Database failure, could not fetch data shred");
                        if let Some(data) = some_data {
                            Shred::new_from_serialized_shred(data).ok()
                        } else {
                            warn!("Data shred deleted while reading for recovery");
                            None
                        }
                    }
                } {
                    available_shreds.push(shred);
                }
            }
        });
    }

    fn get_recovery_coding_shreds(
        &self,
        index: &mut Index,
        slot: Slot,
        erasure_meta: &ErasureMeta,
        available_shreds: &mut Vec<Shred>,
        prev_inserted_codes: &mut HashMap<(u64, u64), Shred>,
    ) {
        (erasure_meta.set_index..erasure_meta.set_index + erasure_meta.config.num_coding() as u64)
            .for_each(|i| {
                if let Some(shred) = prev_inserted_codes
                    .remove(&(slot, i))
                    .map(|s| {
                        // Remove from the index so it doesn't get committed. We know
                        // this is safe to do because everything in
                        // `prev_inserted_codes` does not yet exist in blockstore
                        // (guaranteed by `check_cache_coding_shred`)
                        index.coding_mut().set_present(i, false);
                        s
                    })
                    .or_else(|| {
                        if index.coding().is_present(i) {
                            let some_code = self
                                .get_coding_shred(slot, i)
                                .expect("Database failure, could not fetch code shred");
                            if let Some(code) = some_code {
                                Shred::new_from_serialized_shred(code).ok()
                            } else {
                                warn!("Code shred deleted while reading for recovery");
                                None
                            }
                        } else {
                            None
                        }
                    })
                {
                    available_shreds.push(shred);
                }
            });
    }

    fn recover_shreds(
        &self,
        index: &mut Index,
        set_index: u64,
        erasure_meta: &ErasureMeta,
        prev_inserted_datas: &HashMap<(u64, u64), Shred>,
        prev_inserted_codes: &mut HashMap<(u64, u64), Shred>,
        recovered_data_shreds: &mut Vec<Shred>,
    ) {
        // Find shreds for this erasure set and try recovery
        let slot = index.slot;
        let mut available_shreds = vec![];

        self.get_recovery_data_shreds(
            index,
            set_index,
            slot,
            erasure_meta,
            &mut available_shreds,
            prev_inserted_datas,
        );

        self.get_recovery_coding_shreds(
            index,
            slot,
            erasure_meta,
            &mut available_shreds,
            prev_inserted_codes,
        );

        if let Ok(mut result) = Shredder::try_recovery(
            available_shreds,
            erasure_meta.config.num_data(),
            erasure_meta.config.num_coding(),
            set_index as usize,
            slot,
        ) {
            Self::submit_metrics(
                slot,
                set_index,
                erasure_meta,
                true,
                "complete".into(),
                result.len(),
            );
            recovered_data_shreds.append(&mut result);
        } else {
            Self::submit_metrics(slot, set_index, erasure_meta, true, "incomplete".into(), 0);
        }
    }

    fn submit_metrics(
        slot: Slot,
        set_index: u64,
        erasure_meta: &ErasureMeta,
        attempted: bool,
        status: String,
        recovered: usize,
    ) {
        datapoint_debug!(
            "blockstore-erasure",
            ("slot", slot as i64, i64),
            ("start_index", set_index as i64, i64),
            (
                "end_index",
                (erasure_meta.set_index + erasure_meta.config.num_data() as u64) as i64,
                i64
            ),
            ("recovery_attempted", attempted, bool),
            ("recovery_status", status, String),
            ("recovered", recovered as i64, i64),
        );
    }

    fn try_shred_recovery(
        &self,
        erasure_metas: &HashMap<(u64, u64), ErasureMeta>,
        index_working_set: &mut HashMap<u64, IndexMetaWorkingSetEntry>,
        prev_inserted_datas: &HashMap<(u64, u64), Shred>,
        prev_inserted_codes: &mut HashMap<(u64, u64), Shred>,
    ) -> Vec<Shred> {
        let mut recovered_data_shreds = vec![];
        // Recovery rules:
        // 1. Only try recovery around indexes for which new data or coding shreds are received
        // 2. For new data shreds, check if an erasure set exists. If not, don't try recovery
        // 3. Before trying recovery, check if enough number of shreds have been received
        // 3a. Enough number of shreds = (#data + #coding shreds) > erasure.num_data
        for (&(slot, set_index), erasure_meta) in erasure_metas.iter() {
            let index_meta_entry = index_working_set.get_mut(&slot).expect("Index");
            let index = &mut index_meta_entry.index;
            match erasure_meta.status(index) {
                ErasureMetaStatus::CanRecover => {
                    self.recover_shreds(
                        index,
                        set_index,
                        erasure_meta,
                        prev_inserted_datas,
                        prev_inserted_codes,
                        &mut recovered_data_shreds,
                    );
                }
                ErasureMetaStatus::DataFull => {
                    (set_index..set_index + erasure_meta.config.num_coding() as u64).for_each(
                        |i| {
                            // Remove saved coding shreds. We don't need these for future recovery.
                            if prev_inserted_codes.remove(&(slot, i)).is_some() {
                                // Remove from the index so it doesn't get committed. We know
                                // this is safe to do because everything in
                                // `prev_inserted_codes` does not yet exist in blockstore
                                // (guaranteed by `check_cache_coding_shred`)
                                index.coding_mut().set_present(i, false);
                            }
                        },
                    );
                    Self::submit_metrics(
                        slot,
                        set_index,
                        erasure_meta,
                        false,
                        "complete".into(),
                        0,
                    );
                }
                ErasureMetaStatus::StillNeed(needed) => {
                    Self::submit_metrics(
                        slot,
                        set_index,
                        erasure_meta,
                        false,
                        format!("still need: {}", needed),
                        0,
                    );
                }
            };
        }
        recovered_data_shreds
    }

    pub fn insert_shreds_handle_duplicate<F>(
        &self,
        shreds: Vec<Shred>,
        is_repaired: Vec<bool>,
        leader_schedule: Option<&Arc<LeaderScheduleCache>>,
        is_trusted: bool,
        handle_duplicate: &F,
        metrics: &mut BlockstoreInsertionMetrics,
    ) -> Result<(Vec<CompletedDataSetInfo>, Vec<usize>)>
    where
        F: Fn(Shred),
    {
        assert_eq!(shreds.len(), is_repaired.len());
        let mut total_start = Measure::start("Total elapsed");
        let mut start = Measure::start("Blockstore lock");
        let _lock = self.insert_shreds_lock.lock().unwrap();
        start.stop();
        let insert_lock_elapsed = start.as_us();

        let db = &*self.db;
        let mut write_batch = db.batch()?;

        let mut just_inserted_coding_shreds = HashMap::new();
        let mut just_inserted_data_shreds = HashMap::new();
        let mut erasure_metas = HashMap::new();
        let mut slot_meta_working_set = HashMap::new();
        let mut index_working_set = HashMap::new();

        let num_shreds = shreds.len();
        let mut start = Measure::start("Shred insertion");
        let mut num_inserted = 0;
        let mut index_meta_time = 0;
        let mut newly_completed_data_sets: Vec<CompletedDataSetInfo> = vec![];
        let mut inserted_indices = Vec::new();
        shreds
            .into_iter()
            .zip(is_repaired.into_iter())
            .enumerate()
            .for_each(|(i, (shred, is_repaired))| {
                if shred.is_data() {
                    let shred_slot = shred.slot();
                    let shred_source = if is_repaired {
                        ShredSource::Repaired
                    } else {
                        ShredSource::Turbine
                    };
                    if let Ok(completed_data_sets) = self.check_insert_data_shred(
                        shred,
                        &mut erasure_metas,
                        &mut index_working_set,
                        &mut slot_meta_working_set,
                        &mut write_batch,
                        &mut just_inserted_data_shreds,
                        &mut index_meta_time,
                        is_trusted,
                        handle_duplicate,
                        leader_schedule,
                        shred_source,
                    ) {
                        newly_completed_data_sets.extend(completed_data_sets.into_iter().map(
                            |(start_index, end_index)| CompletedDataSetInfo {
                                slot: shred_slot,
                                start_index,
                                end_index,
                            },
                        ));
                        inserted_indices.push(i);
                        num_inserted += 1;
                    }
                } else if shred.is_code() {
                    self.check_cache_coding_shred(
                        shred,
                        &mut erasure_metas,
                        &mut index_working_set,
                        &mut just_inserted_coding_shreds,
                        &mut index_meta_time,
                        handle_duplicate,
                        is_trusted,
                        is_repaired,
                    );
                } else {
                    panic!("There should be no other case");
                }
            });
        start.stop();

        let insert_shreds_elapsed = start.as_us();
        let mut start = Measure::start("Shred recovery");
        let mut num_recovered = 0;
        let mut num_recovered_inserted = 0;
        let mut num_recovered_failed_sig = 0;
        let mut num_recovered_failed_invalid = 0;
        let mut num_recovered_exists = 0;
        if let Some(leader_schedule_cache) = leader_schedule {
            let recovered_data = self.try_shred_recovery(
                &erasure_metas,
                &mut index_working_set,
                &just_inserted_data_shreds,
                &mut just_inserted_coding_shreds,
            );

            num_recovered = recovered_data.len();
            recovered_data.into_iter().for_each(|shred| {
                if let Some(leader) = leader_schedule_cache.slot_leader_at(shred.slot(), None) {
                    let shred_slot = shred.slot();
                    if shred.verify(&leader) {
                        match self.check_insert_data_shred(
                            shred,
                            &mut erasure_metas,
                            &mut index_working_set,
                            &mut slot_meta_working_set,
                            &mut write_batch,
                            &mut just_inserted_data_shreds,
                            &mut index_meta_time,
                            is_trusted,
                            &handle_duplicate,
                            leader_schedule,
                            ShredSource::Recovered,
                        ) {
                            Err(InsertDataShredError::Exists) => {
                                num_recovered_exists += 1;
                            }
                            Err(InsertDataShredError::InvalidShred) => {
                                num_recovered_failed_invalid += 1;
                            }
                            Err(InsertDataShredError::BlockstoreError(_)) => {}
                            Ok(completed_data_sets) => {
                                newly_completed_data_sets.extend(
                                    completed_data_sets.into_iter().map(
                                        |(start_index, end_index)| CompletedDataSetInfo {
                                            slot: shred_slot,
                                            start_index,
                                            end_index,
                                        },
                                    ),
                                );
                                num_recovered_inserted += 1;
                            }
                        }
                    } else {
                        num_recovered_failed_sig += 1;
                    }
                }
            });
        }
        start.stop();
        let shred_recovery_elapsed = start.as_us();

        just_inserted_coding_shreds
            .into_iter()
            .for_each(|((_, _), shred)| {
                self.check_insert_coding_shred(
                    shred,
                    &mut index_working_set,
                    &mut write_batch,
                    &mut index_meta_time,
                );
                num_inserted += 1;
            });

        self.shred_wal
            .lock()
            .unwrap()
            .write(&just_inserted_data_shreds)
            .expect("Couldn't write shreds to WAL");

        let mut start = Measure::start("Shred recovery");
        // Handle chaining for the members of the slot_meta_working_set that were inserted into,
        // drop the others
        handle_chaining(&self.db, &mut write_batch, &mut slot_meta_working_set)?;
        start.stop();
        let chaining_elapsed = start.as_us();

        let mut start = Measure::start("Commit Working Sets");
        let (should_signal, newly_completed_slots) = commit_slot_meta_working_set(
            &slot_meta_working_set,
            &self.completed_slots_senders,
            &mut write_batch,
        )?;

        for ((slot, set_index), erasure_meta) in erasure_metas {
            write_batch.put::<cf::ErasureMeta>((slot, set_index), &erasure_meta)?;
        }

        for (&slot, index_working_set_entry) in index_working_set.iter() {
            if index_working_set_entry.did_insert_occur {
                write_batch.put::<cf::Index>(slot, &index_working_set_entry.index)?;
            }
        }
        start.stop();
        let commit_working_sets_elapsed = start.as_us();

        let mut start = Measure::start("Write Batch");
        self.db.write(write_batch)?;
        start.stop();
        let write_batch_elapsed = start.as_us();

        send_signals(
            &self.new_shreds_signals,
            &self.completed_slots_senders,
            should_signal,
            newly_completed_slots,
        );

        total_start.stop();

        metrics.num_shreds += num_shreds;
        metrics.total_elapsed += total_start.as_us();
        metrics.insert_lock_elapsed += insert_lock_elapsed;
        metrics.insert_shreds_elapsed += insert_shreds_elapsed;
        metrics.shred_recovery_elapsed += shred_recovery_elapsed;
        metrics.chaining_elapsed += chaining_elapsed;
        metrics.commit_working_sets_elapsed += commit_working_sets_elapsed;
        metrics.write_batch_elapsed += write_batch_elapsed;
        metrics.num_inserted += num_inserted;
        metrics.num_recovered += num_recovered;
        metrics.num_recovered_inserted += num_recovered_inserted;
        metrics.num_recovered_failed_sig += num_recovered_failed_sig;
        metrics.num_recovered_failed_invalid = num_recovered_failed_invalid;
        metrics.num_recovered_exists = num_recovered_exists;
        metrics.index_meta_time += index_meta_time;

        Ok((newly_completed_data_sets, inserted_indices))
    }

    pub fn clear_unconfirmed_slot(&self, slot: Slot) {
        let _lock = self.insert_shreds_lock.lock().unwrap();
        if let Some(mut slot_meta) = self
            .meta(slot)
            .expect("Couldn't fetch from SlotMeta column family")
        {
            // Clear all slot related information
            self.run_purge(slot, slot, PurgeType::PrimaryIndex)
                .expect("Purge database operations failed");

            // Reinsert parts of `slot_meta` that are important to retain, like the `next_slots`
            // field.
            slot_meta.clear_unconfirmed_slot();
            self.meta_cf
                .put(slot, &slot_meta)
                .expect("Couldn't insert into SlotMeta column family");
        } else {
            error!(
                "clear_unconfirmed_slot() called on slot {} with no SlotMeta",
                slot
            );
        }
    }

    pub fn insert_shreds(
        &self,
        shreds: Vec<Shred>,
        leader_schedule: Option<&Arc<LeaderScheduleCache>>,
        is_trusted: bool,
    ) -> Result<(Vec<CompletedDataSetInfo>, Vec<usize>)> {
        let shreds_len = shreds.len();
        self.insert_shreds_handle_duplicate(
            shreds,
            vec![false; shreds_len],
            leader_schedule,
            is_trusted,
            &|_| {},
            &mut BlockstoreInsertionMetrics::default(),
        )
    }

    fn check_insert_coding_shred(
        &self,
        shred: Shred,
        index_working_set: &mut HashMap<u64, IndexMetaWorkingSetEntry>,
        write_batch: &mut WriteBatch,
        index_meta_time: &mut u64,
    ) -> bool {
        let slot = shred.slot();

        let index_meta_working_set_entry =
            get_index_meta_entry(&self.db, slot, index_working_set, index_meta_time);

        let index_meta = &mut index_meta_working_set_entry.index;
        // This gives the index of first coding shred in this FEC block
        // So, all coding shreds in a given FEC block will have the same set index
        self.insert_coding_shred(index_meta, &shred, write_batch)
            .map(|_| {
                index_meta_working_set_entry.did_insert_occur = true;
            })
            .is_ok()
    }

    fn erasure_mismatch(shred1: &Shred, shred2: &Shred) -> bool {
        shred1.coding_header.num_coding_shreds != shred2.coding_header.num_coding_shreds
            || shred1.coding_header.num_data_shreds != shred2.coding_header.num_data_shreds
    }

    fn check_cache_coding_shred<F>(
        &self,
        shred: Shred,
        erasure_metas: &mut HashMap<(u64, u64), ErasureMeta>,
        index_working_set: &mut HashMap<u64, IndexMetaWorkingSetEntry>,
        just_received_coding_shreds: &mut HashMap<(u64, u64), Shred>,
        index_meta_time: &mut u64,
        handle_duplicate: &F,
        is_trusted: bool,
        is_repaired: bool,
    ) -> bool
    where
        F: Fn(Shred),
    {
        let slot = shred.slot();
        let shred_index = u64::from(shred.index());

        let index_meta_working_set_entry =
            get_index_meta_entry(&self.db, slot, index_working_set, index_meta_time);

        let index_meta = &mut index_meta_working_set_entry.index;

        // This gives the index of first coding shred in this FEC block
        // So, all coding shreds in a given FEC block will have the same set index

        if !is_trusted {
            if index_meta.coding().is_present(shred_index) {
                handle_duplicate(shred);
                return false;
            }

            if !Blockstore::should_insert_coding_shred(&shred, &self.last_root) {
                return false;
            }
        }

        let set_index = u64::from(shred.common_header.fec_set_index);
        let erasure_config = ErasureConfig::new(
            shred.coding_header.num_data_shreds as usize,
            shred.coding_header.num_coding_shreds as usize,
        );

        let erasure_meta = erasure_metas.entry((slot, set_index)).or_insert_with(|| {
            self.erasure_meta_cf
                .get((slot, set_index))
                .expect("Expect database get to succeed")
                .unwrap_or_else(|| ErasureMeta::new(set_index, erasure_config))
        });

        if erasure_config != erasure_meta.config {
            let conflicting_shred = self.find_conflicting_coding_shred(
                &shred,
                slot,
                erasure_meta,
                just_received_coding_shreds,
            );
            if let Some(conflicting_shred) = conflicting_shred {
                if self
                    .store_duplicate_if_not_existing(slot, conflicting_shred, shred.payload.clone())
                    .is_err()
                {
                    warn!("bad duplicate store..");
                }
            } else {
                datapoint_info!("bad-conflict-shred", ("slot", slot, i64));
            }

            // ToDo: This is a potential slashing condition
            warn!("Received multiple erasure configs for the same erasure set!!!");
            warn!(
                "Slot: {}, shred index: {}, set_index: {}, is_duplicate: {}, stored config: {:#?}, new config: {:#?}",
                slot, shred.index(), set_index, self.has_duplicate_shreds_in_slot(slot), erasure_meta.config, erasure_config
            );

            return false;
        }

        if is_repaired {
            let mut slots_stats = self.slots_stats.lock().unwrap();
            let mut e = slots_stats.stats.entry(slot).or_default();
            e.num_repaired += 1;
        }

        // Should be safe to modify index_meta here. Two cases
        // 1) Recovery happens: Then all inserted erasure metas are removed
        // from just_received_coding_shreds, and nothing will be committed by
        // `check_insert_coding_shred`, so the coding index meta will not be
        // committed
        index_meta.coding_mut().set_present(shred_index, true);

        just_received_coding_shreds
            .entry((slot, shred_index))
            .or_insert_with(|| shred);

        true
    }

    fn find_conflicting_coding_shred(
        &self,
        shred: &Shred,
        slot: Slot,
        erasure_meta: &ErasureMeta,
        just_received_coding_shreds: &mut HashMap<(u64, u64), Shred>,
    ) -> Option<Vec<u8>> {
        // Search for the shred which set the initial erasure config, either inserted,
        // or in the current batch in just_received_coding_shreds.
        let coding_indices = erasure_meta.set_index
            ..erasure_meta.set_index + erasure_meta.config.num_coding() as u64;
        let mut conflicting_shred = None;
        for coding_index in coding_indices {
            let maybe_shred = self.get_coding_shred(slot, coding_index);
            if let Ok(Some(shred_data)) = maybe_shred {
                let potential_shred = Shred::new_from_serialized_shred(shred_data).unwrap();
                if Self::erasure_mismatch(&potential_shred, shred) {
                    conflicting_shred = Some(potential_shred.payload);
                }
                break;
            } else if let Some(potential_shred) =
                just_received_coding_shreds.get(&(slot, coding_index))
            {
                if Self::erasure_mismatch(potential_shred, shred) {
                    conflicting_shred = Some(potential_shred.payload.clone());
                }
                break;
            }
        }

        conflicting_shred
    }

    #[allow(clippy::too_many_arguments)]
    fn check_insert_data_shred<F>(
        &self,
        shred: Shred,
        erasure_metas: &mut HashMap<(u64, u64), ErasureMeta>,
        index_working_set: &mut HashMap<u64, IndexMetaWorkingSetEntry>,
        slot_meta_working_set: &mut HashMap<u64, SlotMetaWorkingSetEntry>,
        write_batch: &mut WriteBatch,
        just_inserted_data_shreds: &mut HashMap<(u64, u64), Shred>,
        index_meta_time: &mut u64,
        is_trusted: bool,
        handle_duplicate: &F,
        leader_schedule: Option<&Arc<LeaderScheduleCache>>,
        shred_source: ShredSource,
    ) -> std::result::Result<Vec<(u32, u32)>, InsertDataShredError>
    where
        F: Fn(Shred),
    {
        let slot = shred.slot();
        let shred_index = u64::from(shred.index());

        let index_meta_working_set_entry =
            get_index_meta_entry(&self.db, slot, index_working_set, index_meta_time);

        let index_meta = &mut index_meta_working_set_entry.index;
        let slot_meta_entry =
            get_slot_meta_entry(&self.db, slot_meta_working_set, slot, shred.parent());

        let slot_meta = &mut slot_meta_entry.new_slot_meta.borrow_mut();

        if !is_trusted {
            if Self::is_data_shred_present(&shred, slot_meta, index_meta.data()) {
                trace!(
                    "shred not inserted into slot {} and index {}, shred is already present",
                    shred.common_header.slot,
                    shred.common_header.index
                );
                handle_duplicate(shred);
                return Err(InsertDataShredError::Exists);
            }

            if shred.last_in_slot() && shred_index < slot_meta.received && !slot_meta.is_full() {
                // We got a last shred < slot_meta.received, which signals there's an alternative,
                // shorter version of the slot. Because also `!slot_meta.is_full()`, then this
                // means, for the current version of the slot, we might never get all the
                // shreds < the current last index, never replay this slot, and make no
                // progress (for instance if a leader sends an additional detached "last index"
                // shred with a very high index, but none of the intermediate shreds). Ideally, we would
                // just purge all shreds > the new last index slot, but because replay may have already
                // replayed entries past the newly detected "last" shred, then mark the slot as dead
                // and wait for replay to dump and repair the correct version.
                warn!("Received *last* shred index {} less than previous shred index {}, and slot {} is not full, marking slot dead", shred_index, slot_meta.received, slot);
                write_batch.put::<cf::DeadSlots>(slot, &true).unwrap();
            }

            if !self.should_insert_data_shred(
                &shred,
                slot_meta,
                just_inserted_data_shreds,
                &self.last_root,
                leader_schedule,
                shred_source.clone(),
            ) {
                return Err(InsertDataShredError::InvalidShred);
            }
        }

        let set_index = u64::from(shred.common_header.fec_set_index);
        let newly_completed_data_sets =
            self.insert_data_shred(slot_meta, index_meta.data_mut(), &shred, shred_source)?;
        just_inserted_data_shreds.insert((slot, shred_index), shred);
        index_meta_working_set_entry.did_insert_occur = true;
        slot_meta_entry.did_insert_occur = true;
        if let std::collections::hash_map::Entry::Vacant(_) = erasure_metas.entry((slot, set_index))
        {
            if let Some(meta) = self
                .erasure_meta_cf
                .get((slot, set_index))
                .expect("Expect database get to succeed")
            {
                erasure_metas.insert((slot, set_index), meta);
            }
        }
        Ok(newly_completed_data_sets)
    }

    fn should_insert_coding_shred(shred: &Shred, last_root: &RwLock<u64>) -> bool {
        let shred_index = shred.index();
        let fec_set_index = shred.common_header.fec_set_index;
        let num_coding_shreds = shred.coding_header.num_coding_shreds as u32;
        shred.is_code()
            && shred_index >= fec_set_index
            && shred_index - fec_set_index < num_coding_shreds
            && num_coding_shreds != 0
            && num_coding_shreds <= 8 * MAX_DATA_SHREDS_PER_FEC_BLOCK
            && num_coding_shreds - 1 <= u32::MAX - fec_set_index
            && shred.slot() > *last_root.read().unwrap()
    }

    fn insert_coding_shred(
        &self,
        index_meta: &mut Index,
        shred: &Shred,
        write_batch: &mut WriteBatch,
    ) -> Result<()> {
        let slot = shred.slot();
        let shred_index = u64::from(shred.index());

        // Assert guaranteed by integrity checks on the shred that happen before
        // `insert_coding_shred` is called
        assert!(shred.is_code() && shred_index >= shred.common_header.fec_set_index as u64);

        // Commit step: commit all changes to the mutable structures at once, or none at all.
        // We don't want only a subset of these changes going through.
        write_batch.put_bytes::<cf::ShredCode>((slot, shred_index), &shred.payload)?;
        index_meta.coding_mut().set_present(shred_index, true);

        Ok(())
    }

    fn is_data_shred_present(shred: &Shred, slot_meta: &SlotMeta, data_index: &ShredIndex) -> bool {
        let shred_index = u64::from(shred.index());
        // Check that the shred doesn't already exist in blockstore
        shred_index < slot_meta.consumed || data_index.is_present(shred_index)
    }

    fn get_data_shred_from_just_inserted_or_db<'a>(
        &'a self,
        just_inserted_data_shreds: &'a HashMap<(u64, u64), Shred>,
        slot: Slot,
        index: u64,
    ) -> Cow<'a, Vec<u8>> {
        if let Some(shred) = just_inserted_data_shreds.get(&(slot, index)) {
            Cow::Borrowed(&shred.payload)
        } else {
            // If it doesn't exist in the just inserted set, it must exist in
            // the backing store
            Cow::Owned(self.get_data_shred(slot, index).unwrap().unwrap())
        }
    }

    fn should_insert_data_shred(
        &self,
        shred: &Shred,
        slot_meta: &SlotMeta,
        just_inserted_data_shreds: &HashMap<(u64, u64), Shred>,
        last_root: &RwLock<u64>,
        leader_schedule: Option<&Arc<LeaderScheduleCache>>,
        shred_source: ShredSource,
    ) -> bool {
        use crate::shred::SHRED_PAYLOAD_SIZE;
        let shred_index = u64::from(shred.index());
        let slot = shred.slot();
        let last_in_slot = if shred.last_in_slot() {
            debug!("got last in slot");
            true
        } else {
            false
        };

        if shred.data_header.size == 0 {
            let leader_pubkey = leader_schedule
                .and_then(|leader_schedule| leader_schedule.slot_leader_at(slot, None));

            datapoint_error!(
                "blockstore_error",
                (
                    "error",
                    format!(
                        "Leader {:?}, slot {}: received index {} is empty",
                        leader_pubkey, slot, shred_index,
                    ),
                    String
                )
            );
            return false;
        }
        if shred.payload.len() > SHRED_PAYLOAD_SIZE {
            let leader_pubkey = leader_schedule
                .and_then(|leader_schedule| leader_schedule.slot_leader_at(slot, None));

            datapoint_error!(
                "blockstore_error",
                (
                    "error",
                    format!(
                        "Leader {:?}, slot {}: received index {} shred.payload.len() > SHRED_PAYLOAD_SIZE",
                        leader_pubkey, slot, shred_index,
                    ),
                    String
                )
            );
            return false;
        }

        // Check that we do not receive shred_index >= than the last_index
        // for the slot
        let last_index = slot_meta.last_index;
        if shred_index >= last_index {
            let leader_pubkey = leader_schedule
                .and_then(|leader_schedule| leader_schedule.slot_leader_at(slot, None));

            let ending_shred: Cow<Vec<u8>> = self.get_data_shred_from_just_inserted_or_db(
                just_inserted_data_shreds,
                slot,
                last_index,
            );

            if self
                .store_duplicate_if_not_existing(
                    slot,
                    ending_shred.into_owned(),
                    shred.payload.clone(),
                )
                .is_err()
            {
                warn!("store duplicate error");
            }

            datapoint_error!(
                "blockstore_error",
                (
                    "error",
                    format!(
                        "Leader {:?}, slot {}: received index {} >= slot.last_index {}, shred_source: {:?}",
                        leader_pubkey, slot, shred_index, last_index, shred_source
                    ),
                    String
                )
            );
            return false;
        }
        // Check that we do not receive a shred with "last_index" true, but shred_index
        // less than our current received
        if last_in_slot && shred_index < slot_meta.received {
            let leader_pubkey = leader_schedule
                .and_then(|leader_schedule| leader_schedule.slot_leader_at(slot, None));

            let ending_shred: Cow<Vec<u8>> = self.get_data_shred_from_just_inserted_or_db(
                just_inserted_data_shreds,
                slot,
                slot_meta.received - 1,
            );

            if self
                .store_duplicate_if_not_existing(
                    slot,
                    ending_shred.into_owned(),
                    shred.payload.clone(),
                )
                .is_err()
            {
                warn!("store duplicate error");
            }

            datapoint_error!(
                "blockstore_error",
                (
                    "error",
                    format!(
                        "Leader {:?}, slot {}: received shred_index {} < slot.received {}, shred_source: {:?}",
                        leader_pubkey, slot, shred_index, slot_meta.received, shred_source
                    ),
                    String
                )
            );
            return false;
        }

        let last_root = *last_root.read().unwrap();
        verify_shred_slots(slot, slot_meta.parent_slot, last_root)
    }

    fn insert_data_shred(
        &self,
        slot_meta: &mut SlotMeta,
        data_index: &mut ShredIndex,
        shred: &Shred,
        shred_source: ShredSource,
    ) -> Result<Vec<(u32, u32)>> {
        let slot = shred.slot();
        let index = u64::from(shred.index());

        let last_in_slot = if shred.last_in_slot() {
            debug!("got last in slot");
            true
        } else {
            false
        };

        let last_in_data = if shred.data_complete() {
            debug!("got last in data");
            true
        } else {
            false
        };

        // Parent for slot meta should have been set by this point
        assert!(!is_orphan(slot_meta));

        let new_consumed = if slot_meta.consumed == index {
            let mut current_index = index + 1;

            while data_index.is_present(current_index) {
                current_index += 1;
            }
            current_index
        } else {
            slot_meta.consumed
        };

        self.insert_data_shred_into_cache(slot, index, shred);

        data_index.set_present(index, true);
        let newly_completed_data_sets = update_slot_meta(
            last_in_slot,
            last_in_data,
            slot_meta,
            index as u32,
            new_consumed,
            shred.reference_tick(),
            data_index,
        );
        if shred_source == ShredSource::Repaired || shred_source == ShredSource::Recovered {
            let mut slots_stats = self.slots_stats.lock().unwrap();
            let mut e = slots_stats.stats.entry(slot_meta.slot).or_default();
            if shred_source == ShredSource::Repaired {
                e.num_repaired += 1;
            }
            if shred_source == ShredSource::Recovered {
                e.num_recovered += 1;
            }
        }
        if slot_meta.is_full() {
            let (num_repaired, num_recovered) = {
                let mut slots_stats = self.slots_stats.lock().unwrap();
                if let Some(e) = slots_stats.stats.remove(&slot_meta.slot) {
                    if slots_stats.last_cleanup_ts.elapsed().as_secs() > 30 {
                        let root = self.last_root();
                        slots_stats.stats = slots_stats.stats.split_off(&root);
                        slots_stats.last_cleanup_ts = Instant::now();
                    }
                    (e.num_repaired, e.num_recovered)
                } else {
                    (0, 0)
                }
            };
            datapoint_info!(
                "shred_insert_is_full",
                (
                    "total_time_ms",
                    solana_sdk::timing::timestamp() - slot_meta.first_shred_timestamp,
                    i64
                ),
                ("slot", slot_meta.slot, i64),
                ("last_index", slot_meta.last_index, i64),
                ("num_repaired", num_repaired, i64),
                ("num_recovered", num_recovered, i64),
            );
        }
        trace!("inserted shred into slot {:?} and index {:?}", slot, index);
        Ok(newly_completed_data_sets)
    }

    pub fn get_data_shred(&self, slot: Slot, index: u64) -> Result<Option<Vec<u8>>> {
        if let Ok(Some(shred)) = self.get_data_shred_from_cache(slot, index) {
            Ok(Some(shred))
        } else {
            // No luck in the cache, let's try the filesystem
            self.get_data_shred_from_fs(slot, index)
        }
    }

    pub fn get_data_shreds_for_slot(
        &self,
        slot: Slot,
        start_index: u64,
    ) -> ShredResult<Vec<Shred>> {
        if let Some(shreds) = self.get_data_shreds_for_slot_from_cache(slot, start_index) {
            shreds
        } else {
            self.get_data_shreds_for_slot_from_fs(slot, start_index)
                .unwrap_or_else(|| Ok(vec![]))
        }
    }

    pub fn get_data_shreds(
        &self,
        slot: Slot,
        from_index: u64,
        to_index: u64,
        buffer: &mut [u8],
    ) -> Result<(u64, usize)> {
        let _lock = self.check_lowest_cleanup_slot(slot)?;
        let meta_cf = self.db.column::<cf::SlotMeta>();
        let mut buffer_offset = 0;
        let mut last_index = 0;
        if let Some(meta) = meta_cf.get(slot)? {
            if !meta.is_full() {
                warn!("The slot is not yet full. Will not return any shreds");
                return Ok((last_index, buffer_offset));
            }
            let to_index = cmp::min(to_index, meta.consumed);
            for index in from_index..to_index {
                // TODO: this has two copies at the moment (copy out of cache and copy to existing buffer)
                // However, maybe not a high priority given that this appears to be used only for tests
                if let Some(shred_data) = self.get_data_shred(slot, index)? {
                    let shred_len = shred_data.len();
                    if buffer.len().saturating_sub(buffer_offset) >= shred_len {
                        buffer[buffer_offset..buffer_offset + shred_len]
                            .copy_from_slice(&shred_data[..shred_len]);
                        buffer_offset += shred_len;
                        last_index = index;
                        // All shreds are of the same length.
                        // Let's check if we have scope to accommodate another shred
                        // If not, let's break right away, as it'll save on 1 DB read
                        if buffer.len().saturating_sub(buffer_offset) < shred_len {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        Ok((last_index, buffer_offset))
    }

    pub fn get_coding_shred(&self, slot: Slot, index: u64) -> Result<Option<Vec<u8>>> {
        self.code_shred_cf.get_bytes((slot, index))
    }

    pub fn get_coding_shreds_for_slot(
        &self,
        slot: Slot,
        start_index: u64,
    ) -> ShredResult<Vec<Shred>> {
        self.slot_coding_iterator(slot, start_index)
            .expect("blockstore couldn't fetch iterator")
            .map(|code| Shred::new_from_serialized_shred(code.1.to_vec()))
            .collect()
    }

    // Only used by tests
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_entries(
        &self,
        start_slot: Slot,
        num_ticks_in_start_slot: u64,
        start_index: u32,
        ticks_per_slot: u64,
        parent: Option<u64>,
        is_full_slot: bool,
        keypair: &Arc<Keypair>,
        entries: Vec<Entry>,
        version: u16,
    ) -> Result<usize /*num of data shreds*/> {
        let mut parent_slot = parent.map_or(start_slot.saturating_sub(1), |v| v);
        let num_slots = (start_slot - parent_slot).max(1); // Note: slot 0 has parent slot 0
        assert!(num_ticks_in_start_slot < num_slots * ticks_per_slot);
        let mut remaining_ticks_in_slot = num_slots * ticks_per_slot - num_ticks_in_start_slot;

        let mut current_slot = start_slot;
        let mut shredder = Shredder::new(current_slot, parent_slot, 0, version).unwrap();
        let mut all_shreds = vec![];
        let mut slot_entries = vec![];
        // Find all the entries for start_slot
        for entry in entries.into_iter() {
            if remaining_ticks_in_slot == 0 {
                current_slot += 1;
                parent_slot = current_slot - 1;
                remaining_ticks_in_slot = ticks_per_slot;
                let mut current_entries = vec![];
                std::mem::swap(&mut slot_entries, &mut current_entries);
                let start_index = {
                    if all_shreds.is_empty() {
                        start_index
                    } else {
                        0
                    }
                };
                let (mut data_shreds, mut coding_shreds, _) =
                    shredder.entries_to_shreds(keypair, &current_entries, true, start_index);
                all_shreds.append(&mut data_shreds);
                all_shreds.append(&mut coding_shreds);
                shredder = Shredder::new(
                    current_slot,
                    parent_slot,
                    (ticks_per_slot - remaining_ticks_in_slot) as u8,
                    version,
                )
                .unwrap();
            }

            if entry.is_tick() {
                remaining_ticks_in_slot -= 1;
            }
            slot_entries.push(entry);
        }

        if !slot_entries.is_empty() {
            let (mut data_shreds, mut coding_shreds, _) =
                shredder.entries_to_shreds(keypair, &slot_entries, is_full_slot, 0);
            all_shreds.append(&mut data_shreds);
            all_shreds.append(&mut coding_shreds);
        }
        let num_data = all_shreds.iter().filter(|shred| shred.is_data()).count();
        self.insert_shreds(all_shreds, None, false)?;
        Ok(num_data)
    }

    pub fn get_index(&self, slot: Slot) -> Result<Option<Index>> {
        self.index_cf.get(slot)
    }

    /// Manually update the meta for a slot.
    /// Can interfere with automatic meta update and potentially break chaining.
    /// Dangerous. Use with care.
    pub fn put_meta_bytes(&self, slot: Slot, bytes: &[u8]) -> Result<()> {
        self.meta_cf.put_bytes(slot, bytes)
    }

    // Given a start and end entry index, find all the missing
    // indexes in the ledger in the range [start_index, end_index)
    // for the slot with the specified slot
    pub fn find_missing_data_indexes(
        &self,
        slot: Slot,
        first_timestamp: u64,
        start_index: u64,
        end_index: u64,
        max_missing: usize,
    ) -> Vec<u64> {
        if start_index >= end_index || max_missing == 0 {
            return vec![];
        }
        // TODO: need to adapt this to work with disk too
        let slot_cache = match self.data_slot_cache(slot) {
            Some(slot_cache) => slot_cache,
            None => {
                return (start_index..cmp::min(end_index, start_index + max_missing as u64))
                    .collect::<Vec<_>>()
            }
        };
        let slot_cache = slot_cache.read().unwrap();

        let mut missing_indexes = vec![];
        let ticks_since_first_insert =
            DEFAULT_TICKS_PER_SECOND * (timestamp() - first_timestamp) / 1000;

        let mut prev_index = start_index;
        'outer: for (index, shred) in slot_cache.iter() {
            if *index < start_index {
                continue;
            }
            // Get the tick that will be used to figure out the timeout for this hole
            let reference_tick = u64::from(Shred::reference_tick_from_data(shred));
            // Break out early if the higher index holes have not timed out yet
            if ticks_since_first_insert < reference_tick + MAX_TURBINE_DELAY_IN_TICKS {
                return missing_indexes;
            }
            // Insert any newly discovered holes
            for i in prev_index..cmp::min(*index, end_index) {
                missing_indexes.push(i);
                if missing_indexes.len() == max_missing {
                    break 'outer;
                }
            }
            // Update prev_index before the end-early check as we may use prev_index after
            prev_index = *index + 1;
            if *index >= end_index {
                break;
            }
        }
        // If prev_index < end_index, there could be holes within [start_index, end_index)
        // but that are greater than any shreds we have in the blockstore
        if missing_indexes.len() < max_missing && prev_index < end_index {
            for i in prev_index..end_index {
                missing_indexes.push(i);
                if missing_indexes.len() == max_missing {
                    break;
                }
            }
        }

        missing_indexes
    }

    pub fn get_block_time(&self, slot: Slot) -> Result<Option<UnixTimestamp>> {
        datapoint_info!(
            "blockstore-rpc-api",
            ("method", "get_block_time".to_string(), String)
        );
        let _lock = self.check_lowest_cleanup_slot(slot)?;
        self.blocktime_cf.get(slot)
    }

    pub fn cache_block_time(&self, slot: Slot, timestamp: UnixTimestamp) -> Result<()> {
        self.blocktime_cf.put(slot, &timestamp)
    }

    pub fn get_block_height(&self, slot: Slot) -> Result<Option<u64>> {
        datapoint_info!(
            "blockstore-rpc-api",
            ("method", "get_block_height".to_string(), String)
        );
        let _lock = self.check_lowest_cleanup_slot(slot)?;
        self.block_height_cf.get(slot)
    }

    pub fn cache_block_height(&self, slot: Slot, block_height: u64) -> Result<()> {
        self.block_height_cf.put(slot, &block_height)
    }

    pub fn get_first_available_block(&self) -> Result<Slot> {
        let mut root_iterator = self.rooted_slot_iterator(self.lowest_slot())?;
        Ok(root_iterator.next().unwrap_or_default())
    }

    pub fn get_rooted_block(
        &self,
        slot: Slot,
        require_previous_blockhash: bool,
    ) -> Result<ConfirmedBlock> {
        datapoint_info!(
            "blockstore-rpc-api",
            ("method", "get_rooted_block".to_string(), String)
        );
        let _lock = self.check_lowest_cleanup_slot(slot)?;

        if self.is_root(slot) {
            return self.get_complete_block(slot, require_previous_blockhash);
        }
        Err(BlockstoreError::SlotNotRooted)
    }

    pub fn get_complete_block(
        &self,
        slot: Slot,
        require_previous_blockhash: bool,
    ) -> Result<ConfirmedBlock> {
        let slot_meta_cf = self.db.column::<cf::SlotMeta>();
        let slot_meta = match slot_meta_cf.get(slot)? {
            Some(slot_meta) => slot_meta,
            None => {
                info!("SlotMeta not found for slot {}", slot);
                return Err(BlockstoreError::SlotUnavailable);
            }
        };
        if slot_meta.is_full() {
            let slot_entries = self.get_slot_entries(slot, 0)?;
            if !slot_entries.is_empty() {
                let slot_transaction_iterator = slot_entries
                    .iter()
                    .cloned()
                    .flat_map(|entry| entry.transactions)
                    .map(|transaction| {
                        if let Err(err) = transaction.sanitize() {
                            warn!(
                                "Blockstore::get_block sanitize failed: {:?}, \
                                slot: {:?}, \
                                {:?}",
                                err, slot, transaction,
                            );
                        }
                        transaction
                    });
                let parent_slot_entries = self
                    .get_slot_entries(slot_meta.parent_slot, 0)
                    .unwrap_or_default();
                if parent_slot_entries.is_empty() && require_previous_blockhash {
                    return Err(BlockstoreError::ParentEntriesUnavailable);
                }
                let previous_blockhash = if !parent_slot_entries.is_empty() {
                    get_last_hash(parent_slot_entries.iter()).unwrap()
                } else {
                    Hash::default()
                };

                let blockhash = get_last_hash(slot_entries.iter())
                    .unwrap_or_else(|| panic!("Rooted slot {:?} must have blockhash", slot));

                let rewards = self
                    .rewards_cf
                    .get_protobuf_or_bincode::<StoredExtendedRewards>(slot)?
                    .unwrap_or_default()
                    .into();

                // The Blocktime and BlockHeight column families are updated asynchronously; they
                // may not be written by the time the complete slot entries are available. In this
                // case, these fields will be `None`.
                let block_time = self.blocktime_cf.get(slot)?;
                let block_height = self.block_height_cf.get(slot)?;

                let block = ConfirmedBlock {
                    previous_blockhash: previous_blockhash.to_string(),
                    blockhash: blockhash.to_string(),
                    parent_slot: slot_meta.parent_slot,
                    transactions: self
                        .map_transactions_to_statuses(slot, slot_transaction_iterator),
                    rewards,
                    block_time,
                    block_height,
                };
                return Ok(block);
            }
        }
        Err(BlockstoreError::SlotUnavailable)
    }

    pub fn map_transactions_to_statuses<'a>(
        &self,
        slot: Slot,
        iterator: impl Iterator<Item = Transaction> + 'a,
    ) -> Vec<TransactionWithStatusMeta> {
        iterator
            .map(|transaction| {
                let signature = transaction.signatures[0];
                TransactionWithStatusMeta {
                    transaction,
                    meta: self
                        .read_transaction_status((signature, slot))
                        .ok()
                        .flatten(),
                }
            })
            .collect()
    }

    /// Initializes the TransactionStatusIndex column family with two records, `0` and `1`,
    /// which are used as the primary index for entries in the TransactionStatus and
    /// AddressSignatures columns. At any given time, one primary index is active (ie. new records
    /// are stored under this index), the other is frozen.
    fn initialize_transaction_status_index(&self) -> Result<()> {
        self.transaction_status_index_cf
            .put(0, &TransactionStatusIndexMeta::default())?;
        self.transaction_status_index_cf
            .put(1, &TransactionStatusIndexMeta::default())?;
        // This dummy status improves compaction performance
        let default_status = TransactionStatusMeta::default().into();
        self.transaction_status_cf
            .put_protobuf(cf::TransactionStatus::as_index(2), &default_status)?;
        self.address_signatures_cf.put(
            cf::AddressSignatures::as_index(2),
            &AddressSignatureMeta::default(),
        )
    }

    /// Toggles the active primary index between `0` and `1`, and clears the stored max-slot of the
    /// frozen index in preparation for pruning.
    fn toggle_transaction_status_index(
        &self,
        batch: &mut WriteBatch,
        w_active_transaction_status_index: &mut u64,
        to_slot: Slot,
    ) -> Result<Option<u64>> {
        let index0 = self.transaction_status_index_cf.get(0)?;
        if index0.is_none() {
            return Ok(None);
        }
        let mut index0 = index0.unwrap();
        let mut index1 = self.transaction_status_index_cf.get(1)?.unwrap();

        if !index0.frozen && !index1.frozen {
            index0.frozen = true;
            *w_active_transaction_status_index = 1;
            batch.put::<cf::TransactionStatusIndex>(0, &index0)?;
            Ok(None)
        } else {
            let purge_target_primary_index = if index0.frozen && to_slot > index0.max_slot {
                info!(
                    "Pruning expired primary index 0 up to slot {} (max requested: {})",
                    index0.max_slot, to_slot
                );
                Some(0)
            } else if index1.frozen && to_slot > index1.max_slot {
                info!(
                    "Pruning expired primary index 1 up to slot {} (max requested: {})",
                    index1.max_slot, to_slot
                );
                Some(1)
            } else {
                None
            };

            if let Some(purge_target_primary_index) = purge_target_primary_index {
                *w_active_transaction_status_index = purge_target_primary_index;
                if index0.frozen {
                    index0.max_slot = 0
                };
                index0.frozen = !index0.frozen;
                batch.put::<cf::TransactionStatusIndex>(0, &index0)?;
                if index1.frozen {
                    index1.max_slot = 0
                };
                index1.frozen = !index1.frozen;
                batch.put::<cf::TransactionStatusIndex>(1, &index1)?;
            }

            Ok(purge_target_primary_index)
        }
    }

    fn get_primary_index_to_write(
        &self,
        slot: Slot,
        // take WriteGuard to require critical section semantics at call site
        w_active_transaction_status_index: &RwLockWriteGuard<Slot>,
    ) -> Result<u64> {
        let i = **w_active_transaction_status_index;
        let mut index_meta = self.transaction_status_index_cf.get(i)?.unwrap();
        if slot > index_meta.max_slot {
            assert!(!index_meta.frozen);
            index_meta.max_slot = slot;
            self.transaction_status_index_cf.put(i, &index_meta)?;
        }
        Ok(i)
    }

    pub fn read_transaction_status(
        &self,
        index: (Signature, Slot),
    ) -> Result<Option<TransactionStatusMeta>> {
        let (signature, slot) = index;
        let result = self
            .transaction_status_cf
            .get_protobuf_or_bincode::<StoredTransactionStatusMeta>((0, signature, slot))?;
        if result.is_none() {
            Ok(self
                .transaction_status_cf
                .get_protobuf_or_bincode::<StoredTransactionStatusMeta>((1, signature, slot))?
                .and_then(|meta| meta.try_into().ok()))
        } else {
            Ok(result.and_then(|meta| meta.try_into().ok()))
        }
    }

    pub fn write_transaction_status(
        &self,
        slot: Slot,
        signature: Signature,
        writable_keys: Vec<&Pubkey>,
        readonly_keys: Vec<&Pubkey>,
        status: TransactionStatusMeta,
    ) -> Result<()> {
        let status = status.into();
        // This write lock prevents interleaving issues with the transaction_status_index_cf by gating
        // writes to that column
        let w_active_transaction_status_index =
            self.active_transaction_status_index.write().unwrap();
        let primary_index =
            self.get_primary_index_to_write(slot, &w_active_transaction_status_index)?;
        self.transaction_status_cf
            .put_protobuf((primary_index, signature, slot), &status)?;
        for address in writable_keys {
            self.address_signatures_cf.put(
                (primary_index, *address, slot, signature),
                &AddressSignatureMeta { writeable: true },
            )?;
        }
        for address in readonly_keys {
            self.address_signatures_cf.put(
                (primary_index, *address, slot, signature),
                &AddressSignatureMeta { writeable: false },
            )?;
        }
        Ok(())
    }

    fn check_lowest_cleanup_slot(&self, slot: Slot) -> Result<std::sync::RwLockReadGuard<Slot>> {
        // lowest_cleanup_slot is the last slot that was not cleaned up by LedgerCleanupService
        let lowest_cleanup_slot = self.lowest_cleanup_slot.read().unwrap();
        if *lowest_cleanup_slot > 0 && *lowest_cleanup_slot >= slot {
            return Err(BlockstoreError::SlotCleanedUp);
        }
        // Make caller hold this lock properly; otherwise LedgerCleanupService can purge/compact
        // needed slots here at any given moment
        Ok(lowest_cleanup_slot)
    }

    fn ensure_lowest_cleanup_slot(&self) -> (std::sync::RwLockReadGuard<Slot>, Slot) {
        // Ensures consistent result by using lowest_cleanup_slot as the lower bound
        // for reading columns that do not employ strong read consistency with slot-based
        // delete_range
        let lowest_cleanup_slot = self.lowest_cleanup_slot.read().unwrap();
        let lowest_available_slot = (*lowest_cleanup_slot)
            .checked_add(1)
            .expect("overflow from trusted value");

        // Make caller hold this lock properly; otherwise LedgerCleanupService can purge/compact
        // needed slots here at any given moment.
        // Blockstore callers, like rpc, can process concurrent read queries
        (lowest_cleanup_slot, lowest_available_slot)
    }

    // Returns a transaction status, as well as a loop counter for unit testing
    fn get_transaction_status_with_counter(
        &self,
        signature: Signature,
        confirmed_unrooted_slots: &[Slot],
    ) -> Result<(Option<(Slot, TransactionStatusMeta)>, u64)> {
        let mut counter = 0;
        let (lock, lowest_available_slot) = self.ensure_lowest_cleanup_slot();

        for transaction_status_cf_primary_index in 0..=1 {
            let index_iterator = self.transaction_status_cf.iter(IteratorMode::From(
                (
                    transaction_status_cf_primary_index,
                    signature,
                    lowest_available_slot,
                ),
                IteratorDirection::Forward,
            ))?;
            for ((i, sig, slot), _data) in index_iterator {
                counter += 1;
                if i != transaction_status_cf_primary_index || sig != signature {
                    break;
                }
                if !self.is_root(slot) && !confirmed_unrooted_slots.contains(&slot) {
                    continue;
                }
                let status = self
                    .transaction_status_cf
                    .get_protobuf_or_bincode::<StoredTransactionStatusMeta>((i, sig, slot))?
                    .and_then(|status| status.try_into().ok())
                    .map(|status| (slot, status));
                return Ok((status, counter));
            }
        }
        drop(lock);

        Ok((None, counter))
    }

    /// Returns a transaction status
    pub fn get_rooted_transaction_status(
        &self,
        signature: Signature,
    ) -> Result<Option<(Slot, TransactionStatusMeta)>> {
        datapoint_info!(
            "blockstore-rpc-api",
            (
                "method",
                "get_rooted_transaction_status".to_string(),
                String
            )
        );
        self.get_transaction_status(signature, &[])
    }

    /// Returns a transaction status
    pub fn get_transaction_status(
        &self,
        signature: Signature,
        confirmed_unrooted_slots: &[Slot],
    ) -> Result<Option<(Slot, TransactionStatusMeta)>> {
        datapoint_info!(
            "blockstore-rpc-api",
            ("method", "get_transaction_status".to_string(), String)
        );
        self.get_transaction_status_with_counter(signature, confirmed_unrooted_slots)
            .map(|(status, _)| status)
    }

    /// Returns a complete transaction if it was processed in a root
    pub fn get_rooted_transaction(
        &self,
        signature: Signature,
    ) -> Result<Option<ConfirmedTransaction>> {
        datapoint_info!(
            "blockstore-rpc-api",
            ("method", "get_rooted_transaction".to_string(), String)
        );
        self.get_transaction_with_status(signature, &[])
    }

    /// Returns a complete transaction
    pub fn get_complete_transaction(
        &self,
        signature: Signature,
        highest_confirmed_slot: Slot,
    ) -> Result<Option<ConfirmedTransaction>> {
        datapoint_info!(
            "blockstore-rpc-api",
            ("method", "get_complete_transaction".to_string(), String)
        );
        let last_root = self.last_root();
        let confirmed_unrooted_slots: Vec<_> =
            AncestorIterator::new_inclusive(highest_confirmed_slot, self)
                .take_while(|&slot| slot > last_root)
                .collect();
        self.get_transaction_with_status(signature, &confirmed_unrooted_slots)
    }

    fn get_transaction_with_status(
        &self,
        signature: Signature,
        confirmed_unrooted_slots: &[Slot],
    ) -> Result<Option<ConfirmedTransaction>> {
        if let Some((slot, status)) =
            self.get_transaction_status(signature, confirmed_unrooted_slots)?
        {
            let transaction = self
                .find_transaction_in_slot(slot, signature)?
                .ok_or(BlockstoreError::TransactionStatusSlotMismatch)?; // Should not happen
            let block_time = self.get_block_time(slot)?;
            Ok(Some(ConfirmedTransaction {
                slot,
                transaction: TransactionWithStatusMeta {
                    transaction,
                    meta: Some(status),
                },
                block_time,
            }))
        } else {
            Ok(None)
        }
    }

    fn find_transaction_in_slot(
        &self,
        slot: Slot,
        signature: Signature,
    ) -> Result<Option<Transaction>> {
        let slot_entries = self.get_slot_entries(slot, 0)?;
        Ok(slot_entries
            .iter()
            .cloned()
            .flat_map(|entry| entry.transactions)
            .map(|transaction| {
                if let Err(err) = transaction.sanitize() {
                    warn!(
                        "Blockstore::find_transaction_in_slot sanitize failed: {:?}, \
                        slot: {:?}, \
                        {:?}",
                        err, slot, transaction,
                    );
                }
                transaction
            })
            .find(|transaction| transaction.signatures[0] == signature))
    }

    // Returns all rooted signatures for an address, ordered by slot that the transaction was
    // processed in. Within each slot the transactions will be ordered by signature, and NOT by
    // the order in which the transactions exist in the block
    //
    // DEPRECATED
    fn find_address_signatures(
        &self,
        pubkey: Pubkey,
        start_slot: Slot,
        end_slot: Slot,
    ) -> Result<Vec<(Slot, Signature)>> {
        let (lock, lowest_available_slot) = self.ensure_lowest_cleanup_slot();

        let mut signatures: Vec<(Slot, Signature)> = vec![];
        for transaction_status_cf_primary_index in 0..=1 {
            let index_iterator = self.address_signatures_cf.iter(IteratorMode::From(
                (
                    transaction_status_cf_primary_index,
                    pubkey,
                    start_slot.max(lowest_available_slot),
                    Signature::default(),
                ),
                IteratorDirection::Forward,
            ))?;
            for ((i, address, slot, signature), _) in index_iterator {
                if i != transaction_status_cf_primary_index || slot > end_slot || address != pubkey
                {
                    break;
                }
                if self.is_root(slot) {
                    signatures.push((slot, signature));
                }
            }
        }
        drop(lock);
        signatures.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
        Ok(signatures)
    }

    // Returns all signatures for an address in a particular slot, regardless of whether that slot
    // has been rooted. The transactions will be ordered by signature, and NOT by the order in
    // which the transactions exist in the block
    fn find_address_signatures_for_slot(
        &self,
        pubkey: Pubkey,
        slot: Slot,
    ) -> Result<Vec<(Slot, Signature)>> {
        let (lock, lowest_available_slot) = self.ensure_lowest_cleanup_slot();
        let mut signatures: Vec<(Slot, Signature)> = vec![];
        for transaction_status_cf_primary_index in 0..=1 {
            let index_iterator = self.address_signatures_cf.iter(IteratorMode::From(
                (
                    transaction_status_cf_primary_index,
                    pubkey,
                    slot.max(lowest_available_slot),
                    Signature::default(),
                ),
                IteratorDirection::Forward,
            ))?;
            for ((i, address, transaction_slot, signature), _) in index_iterator {
                if i != transaction_status_cf_primary_index
                    || transaction_slot > slot
                    || address != pubkey
                {
                    break;
                }
                signatures.push((slot, signature));
            }
        }
        drop(lock);
        signatures.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
        Ok(signatures)
    }

    // DEPRECATED
    pub fn get_confirmed_signatures_for_address(
        &self,
        pubkey: Pubkey,
        start_slot: Slot,
        end_slot: Slot,
    ) -> Result<Vec<Signature>> {
        datapoint_info!(
            "blockstore-rpc-api",
            (
                "method",
                "get_confirmed_signatures_for_address".to_string(),
                String
            )
        );
        self.find_address_signatures(pubkey, start_slot, end_slot)
            .map(|signatures| signatures.iter().map(|(_, signature)| *signature).collect())
    }

    pub fn get_confirmed_signatures_for_address2(
        &self,
        address: Pubkey,
        highest_slot: Slot, // highest_confirmed_root or highest_confirmed_slot
        before: Option<Signature>,
        until: Option<Signature>,
        limit: usize,
    ) -> Result<Vec<ConfirmedTransactionStatusWithSignature>> {
        datapoint_info!(
            "blockstore-rpc-api",
            (
                "method",
                "get_confirmed_signatures_for_address2".to_string(),
                String
            )
        );
        let last_root = self.last_root();
        let confirmed_unrooted_slots: Vec<_> = AncestorIterator::new_inclusive(highest_slot, self)
            .take_while(|&slot| slot > last_root)
            .collect();

        // Figure the `slot` to start listing signatures at, based on the ledger location of the
        // `before` signature if present.  Also generate a HashSet of signatures that should
        // be excluded from the results.
        let mut get_before_slot_timer = Measure::start("get_before_slot_timer");
        let (slot, mut before_excluded_signatures) = match before {
            None => (highest_slot, None),
            Some(before) => {
                let transaction_status =
                    self.get_transaction_status(before, &confirmed_unrooted_slots)?;
                match transaction_status {
                    None => return Ok(vec![]),
                    Some((slot, _)) => {
                        let block = self.get_complete_block(slot, false).map_err(|err| {
                            BlockstoreError::Io(IoError::new(
                                ErrorKind::Other,
                                format!("Unable to get block: {}", err),
                            ))
                        })?;

                        // Load all signatures for the block
                        let mut slot_signatures: Vec<_> = block
                            .transactions
                            .into_iter()
                            .filter_map(|transaction_with_meta| {
                                transaction_with_meta
                                    .transaction
                                    .signatures
                                    .into_iter()
                                    .next()
                            })
                            .collect();

                        // Sort signatures as a way to entire a stable ordering within a slot, as
                        // the AddressSignatures column is ordered by signatures within a slot,
                        // not by block ordering
                        slot_signatures.sort();
                        slot_signatures.reverse();

                        if let Some(pos) = slot_signatures.iter().position(|&x| x == before) {
                            slot_signatures.truncate(pos + 1);
                        }

                        (
                            slot,
                            Some(slot_signatures.into_iter().collect::<HashSet<_>>()),
                        )
                    }
                }
            }
        };
        get_before_slot_timer.stop();

        // Generate a HashSet of signatures that should be excluded from the results based on
        // `until` signature
        let mut get_until_slot_timer = Measure::start("get_until_slot_timer");
        let (lowest_slot, until_excluded_signatures) = match until {
            None => (0, HashSet::new()),
            Some(until) => {
                let transaction_status =
                    self.get_transaction_status(until, &confirmed_unrooted_slots)?;
                match transaction_status {
                    None => (0, HashSet::new()),
                    Some((slot, _)) => {
                        let block = self.get_complete_block(slot, false).map_err(|err| {
                            BlockstoreError::Io(IoError::new(
                                ErrorKind::Other,
                                format!("Unable to get block: {}", err),
                            ))
                        })?;

                        // Load all signatures for the block
                        let mut slot_signatures: Vec<_> = block
                            .transactions
                            .into_iter()
                            .filter_map(|transaction_with_meta| {
                                transaction_with_meta
                                    .transaction
                                    .signatures
                                    .into_iter()
                                    .next()
                            })
                            .collect();

                        // Sort signatures as a way to entire a stable ordering within a slot, as
                        // the AddressSignatures column is ordered by signatures within a slot,
                        // not by block ordering
                        slot_signatures.sort();
                        slot_signatures.reverse();

                        if let Some(pos) = slot_signatures.iter().position(|&x| x == until) {
                            slot_signatures = slot_signatures.split_off(pos);
                        }

                        (slot, slot_signatures.into_iter().collect::<HashSet<_>>())
                    }
                }
            }
        };
        get_until_slot_timer.stop();

        // Fetch the list of signatures that affect the given address
        let first_available_block = self.get_first_available_block()?;
        let mut address_signatures = vec![];

        // Get signatures in `slot`
        let mut get_initial_slot_timer = Measure::start("get_initial_slot_timer");
        let mut signatures = self.find_address_signatures_for_slot(address, slot)?;
        signatures.reverse();
        if let Some(excluded_signatures) = before_excluded_signatures.take() {
            address_signatures.extend(
                signatures
                    .into_iter()
                    .filter(|(_, signature)| !excluded_signatures.contains(signature)),
            )
        } else {
            address_signatures.append(&mut signatures);
        }
        get_initial_slot_timer.stop();

        // Check the active_transaction_status_index to see if it contains slot. If so, start with
        // that index, as it will contain higher slots
        let starting_primary_index = *self.active_transaction_status_index.read().unwrap();
        let next_primary_index = if starting_primary_index == 0 { 1 } else { 0 };
        let next_max_slot = self
            .transaction_status_index_cf
            .get(next_primary_index)?
            .unwrap()
            .max_slot;

        let mut starting_primary_index_iter_timer = Measure::start("starting_primary_index_iter");
        if slot > next_max_slot {
            let mut starting_iterator = self.address_signatures_cf.iter(IteratorMode::From(
                (starting_primary_index, address, slot, Signature::default()),
                IteratorDirection::Reverse,
            ))?;

            // Iterate through starting_iterator until limit is reached
            while address_signatures.len() < limit {
                if let Some(((i, key_address, slot, signature), _)) = starting_iterator.next() {
                    if slot == next_max_slot || slot < lowest_slot {
                        break;
                    }
                    if i == starting_primary_index
                        && key_address == address
                        && slot >= first_available_block
                    {
                        if self.is_root(slot) || confirmed_unrooted_slots.contains(&slot) {
                            address_signatures.push((slot, signature));
                        }
                        continue;
                    }
                }
                break;
            }

            // Handle slots that cross primary indexes
            if next_max_slot >= lowest_slot {
                let mut signatures =
                    self.find_address_signatures_for_slot(address, next_max_slot)?;
                signatures.reverse();
                address_signatures.append(&mut signatures);
            }
        }
        starting_primary_index_iter_timer.stop();

        // Iterate through next_iterator until limit is reached
        let mut next_primary_index_iter_timer = Measure::start("next_primary_index_iter_timer");
        let mut next_iterator = self.address_signatures_cf.iter(IteratorMode::From(
            (next_primary_index, address, slot, Signature::default()),
            IteratorDirection::Reverse,
        ))?;
        while address_signatures.len() < limit {
            if let Some(((i, key_address, slot, signature), _)) = next_iterator.next() {
                // Skip next_max_slot, which is already included
                if slot == next_max_slot {
                    continue;
                }
                if slot < lowest_slot {
                    break;
                }
                if i == next_primary_index
                    && key_address == address
                    && slot >= first_available_block
                {
                    if self.is_root(slot) || confirmed_unrooted_slots.contains(&slot) {
                        address_signatures.push((slot, signature));
                    }
                    continue;
                }
            }
            break;
        }
        next_primary_index_iter_timer.stop();
        let mut address_signatures: Vec<(Slot, Signature)> = address_signatures
            .into_iter()
            .filter(|(_, signature)| !until_excluded_signatures.contains(signature))
            .collect();
        address_signatures.truncate(limit);

        // Fill in the status information for each found transaction
        let mut get_status_info_timer = Measure::start("get_status_info_timer");
        let mut infos = vec![];
        for (slot, signature) in address_signatures.into_iter() {
            let transaction_status =
                self.get_transaction_status(signature, &confirmed_unrooted_slots)?;
            let err = transaction_status.and_then(|(_slot, status)| status.status.err());
            let block_time = self.get_block_time(slot)?;
            infos.push(ConfirmedTransactionStatusWithSignature {
                signature,
                slot,
                err,
                memo: None,
                block_time,
            });
        }
        get_status_info_timer.stop();

        datapoint_info!(
            "blockstore-get-conf-sigs-for-addr-2",
            (
                "get_before_slot_us",
                get_before_slot_timer.as_us() as i64,
                i64
            ),
            (
                "get_initial_slot_us",
                get_initial_slot_timer.as_us() as i64,
                i64
            ),
            (
                "starting_primary_index_iter_us",
                starting_primary_index_iter_timer.as_us() as i64,
                i64
            ),
            (
                "next_primary_index_iter_us",
                next_primary_index_iter_timer.as_us() as i64,
                i64
            ),
            (
                "get_status_info_us",
                get_status_info_timer.as_us() as i64,
                i64
            ),
            (
                "get_until_slot_us",
                get_until_slot_timer.as_us() as i64,
                i64
            )
        );

        Ok(infos)
    }

    pub fn read_rewards(&self, index: Slot) -> Result<Option<Rewards>> {
        self.rewards_cf
            .get_protobuf_or_bincode::<Rewards>(index)
            .map(|result| result.map(|option| option.into()))
    }

    pub fn write_rewards(&self, index: Slot, rewards: Rewards) -> Result<()> {
        let rewards = rewards.into();
        self.rewards_cf.put_protobuf(index, &rewards)
    }

    pub fn get_recent_perf_samples(&self, num: usize) -> Result<Vec<(Slot, PerfSample)>> {
        Ok(self
            .db
            .iter::<cf::PerfSamples>(IteratorMode::End)?
            .take(num)
            .map(|(slot, data)| {
                let perf_sample = deserialize(&data).unwrap();
                (slot, perf_sample)
            })
            .collect())
    }

    pub fn write_perf_sample(&self, index: Slot, perf_sample: &PerfSample) -> Result<()> {
        self.perf_samples_cf.put(index, perf_sample)
    }

    pub fn read_program_costs(&self) -> Result<Vec<(Pubkey, u64)>> {
        Ok(self
            .db
            .iter::<cf::ProgramCosts>(IteratorMode::End)?
            .map(|(pubkey, data)| {
                let program_cost: ProgramCost = deserialize(&data).unwrap();
                (pubkey, program_cost.cost)
            })
            .collect())
    }

    pub fn write_program_cost(&self, key: &Pubkey, value: &u64) -> Result<()> {
        self.program_costs_cf
            .put(*key, &ProgramCost { cost: *value })
    }

    pub fn delete_program_cost(&self, key: &Pubkey) -> Result<()> {
        self.program_costs_cf.delete(*key)
    }

    /// Returns the entry vector for the slot starting with `shred_start_index`
    pub fn get_slot_entries(&self, slot: Slot, shred_start_index: u64) -> Result<Vec<Entry>> {
        self.get_slot_entries_with_shred_info(slot, shred_start_index, false)
            .map(|x| x.0)
    }

    /// Returns the entry vector for the slot starting with `shred_start_index`, the number of
    /// shreds that comprise the entry vector, and whether the slot is full (consumed all shreds).
    pub fn get_slot_entries_with_shred_info(
        &self,
        slot: Slot,
        start_index: u64,
        allow_dead_slots: bool,
    ) -> Result<(Vec<Entry>, u64, bool)> {
        let (completed_ranges, slot_meta) = self.get_completed_ranges(slot, start_index)?;

        // Check if the slot is dead *after* fetching completed ranges to avoid a race
        // where a slot is marked dead by another thread before the completed range query finishes.
        // This should be sufficient because full slots will never be marked dead from another thread,
        // this can only happen during entry processing during replay stage.
        if self.is_dead(slot) && !allow_dead_slots {
            return Err(BlockstoreError::DeadSlot);
        } else if completed_ranges.is_empty() {
            return Ok((vec![], 0, false));
        }

        let slot_meta = slot_meta.unwrap();
        let num_shreds = completed_ranges
            .last()
            .map(|(_, end_index)| u64::from(*end_index) - start_index + 1)
            .unwrap_or(0);

        let entries: Result<Vec<Vec<Entry>>> = PAR_THREAD_POOL.with(|thread_pool| {
            thread_pool.borrow().install(|| {
                completed_ranges
                    .par_iter()
                    .map(|(start_index, end_index)| {
                        self.get_entries_in_data_block(
                            slot,
                            *start_index,
                            *end_index,
                            Some(&slot_meta),
                        )
                    })
                    .collect()
            })
        });

        let entries: Vec<Entry> = entries?.into_iter().flatten().collect();
        Ok((entries, num_shreds, slot_meta.is_full()))
    }

    fn get_completed_ranges(
        &self,
        slot: Slot,
        start_index: u64,
    ) -> Result<(CompletedRanges, Option<SlotMeta>)> {
        let _lock = self.check_lowest_cleanup_slot(slot)?;

        let slot_meta_cf = self.db.column::<cf::SlotMeta>();
        let slot_meta = slot_meta_cf.get(slot)?;
        if slot_meta.is_none() {
            return Ok((vec![], slot_meta));
        }

        let slot_meta = slot_meta.unwrap();
        // Find all the ranges for the completed data blocks
        let completed_ranges = Self::get_completed_data_ranges(
            start_index as u32,
            &slot_meta.completed_data_indexes[..],
            slot_meta.consumed as u32,
        );

        Ok((completed_ranges, Some(slot_meta)))
    }

    // Get the range of indexes [start_index, end_index] of every completed data block
    fn get_completed_data_ranges(
        mut start_index: u32,
        completed_data_end_indexes: &[u32],
        consumed: u32,
    ) -> CompletedRanges {
        let mut completed_data_ranges = vec![];
        let floor = completed_data_end_indexes
            .iter()
            .position(|i| *i >= start_index)
            .unwrap_or_else(|| completed_data_end_indexes.len());

        for i in &completed_data_end_indexes[floor as usize..] {
            // `consumed` is the next missing shred index, but shred `i` existing in
            // completed_data_end_indexes implies it's not missing
            assert!(*i != consumed);

            if *i < consumed {
                completed_data_ranges.push((start_index, *i));
                start_index = *i + 1;
            }
        }

        completed_data_ranges
    }

    pub fn get_entries_in_data_block(
        &self,
        slot: Slot,
        start_index: u32,
        end_index: u32,
        slot_meta: Option<&SlotMeta>,
    ) -> Result<Vec<Entry>> {
        // Short circuit on first error
        let data_shreds: Result<Vec<Shred>> = (start_index..=end_index)
            .map(|i| {
                self.get_data_shred(slot, u64::from(i))
                    .and_then(|serialized_shred| {
                        if serialized_shred.is_none() {
                            if let Some(slot_meta) = slot_meta {
                                panic!(
                                    "Shred with
                                    slot: {},
                                    index: {},
                                    consumed: {},
                                    completed_indexes: {:?}
                                    must exist if shred index was included in a range: {} {}",
                                    slot,
                                    i,
                                    slot_meta.consumed,
                                    slot_meta.completed_data_indexes,
                                    start_index,
                                    end_index
                                );
                            } else {
                                return Err(BlockstoreError::InvalidShredData(Box::new(
                                    bincode::ErrorKind::Custom(format!(
                                        "Missing shred for slot {}, index {}",
                                        slot, i
                                    )),
                                )));
                            }
                        }

                        Shred::new_from_serialized_shred(serialized_shred.unwrap()).map_err(|err| {
                            BlockstoreError::InvalidShredData(Box::new(bincode::ErrorKind::Custom(
                                format!(
                                    "Could not reconstruct shred from shred payload: {:?}",
                                    err
                                ),
                            )))
                        })
                    })
            })
            .collect();

        let data_shreds = data_shreds?;
        let last_shred = data_shreds.last().unwrap();
        assert!(last_shred.data_complete() || last_shred.last_in_slot());

        let deshred_payload = Shredder::deshred(&data_shreds).map_err(|e| {
            BlockstoreError::InvalidShredData(Box::new(bincode::ErrorKind::Custom(format!(
                "Could not reconstruct data block from constituent shreds, error: {:?}",
                e
            ))))
        })?;

        debug!("{:?} shreds in last FEC set", data_shreds.len(),);
        bincode::deserialize::<Vec<Entry>>(&deshred_payload).map_err(|e| {
            BlockstoreError::InvalidShredData(Box::new(bincode::ErrorKind::Custom(format!(
                "could not reconstruct entries: {:?}",
                e
            ))))
        })
    }

    fn get_any_valid_slot_entries(&self, slot: Slot, start_index: u64) -> Vec<Entry> {
        let (completed_ranges, slot_meta) = self
            .get_completed_ranges(slot, start_index)
            .unwrap_or_default();
        if completed_ranges.is_empty() {
            return vec![];
        }
        let slot_meta = slot_meta.unwrap();

        let entries: Vec<Vec<Entry>> = PAR_THREAD_POOL_ALL_CPUS.with(|thread_pool| {
            thread_pool.borrow().install(|| {
                completed_ranges
                    .par_iter()
                    .map(|(start_index, end_index)| {
                        self.get_entries_in_data_block(
                            slot,
                            *start_index,
                            *end_index,
                            Some(&slot_meta),
                        )
                        .unwrap_or_default()
                    })
                    .collect()
            })
        });

        entries.into_iter().flatten().collect()
    }

    // Returns slots connecting to any element of the list `slots`.
    pub fn get_slots_since(&self, slots: &[u64]) -> Result<HashMap<u64, Vec<u64>>> {
        // Return error if there was a database error during lookup of any of the
        // slot indexes
        let slot_metas: Result<Vec<Option<SlotMeta>>> =
            slots.iter().map(|slot| self.meta(*slot)).collect();

        let slot_metas = slot_metas?;
        let result: HashMap<u64, Vec<u64>> = slots
            .iter()
            .zip(slot_metas)
            .filter_map(|(height, meta)| meta.map(|meta| (*height, meta.next_slots.to_vec())))
            .collect();

        Ok(result)
    }

    pub fn is_root(&self, slot: Slot) -> bool {
        matches!(self.db.get::<cf::Root>(slot), Ok(Some(true)))
    }

    /// Returns true if a slot is between the rooted slot bounds of the ledger, but has not itself
    /// been rooted. This is either because the slot was skipped, or due to a gap in ledger data,
    /// as when booting from a newer snapshot.
    pub fn is_skipped(&self, slot: Slot) -> bool {
        let lowest_root = self
            .rooted_slot_iterator(0)
            .ok()
            .and_then(|mut iter| iter.next())
            .unwrap_or_default();
        match self.db.get::<cf::Root>(slot).ok().flatten() {
            Some(_) => false,
            None => slot < self.max_root() && slot > lowest_root,
        }
    }

    pub fn insert_bank_hash(&self, slot: Slot, frozen_hash: Hash, is_duplicate_confirmed: bool) {
        if let Some(prev_value) = self.bank_hash_cf.get(slot).unwrap() {
            if prev_value.frozen_hash() == frozen_hash && prev_value.is_duplicate_confirmed() {
                // Don't overwrite is_duplicate_confirmed == true with is_duplicate_confirmed == false,
                // which may happen on startup when procesing from blockstore processor because the
                // blocks may not reflect earlier observed gossip votes from before the restart.
                return;
            }
        }
        let data = FrozenHashVersioned::Current(FrozenHashStatus {
            frozen_hash,
            is_duplicate_confirmed,
        });
        self.bank_hash_cf.put(slot, &data).unwrap()
    }

    pub fn get_bank_hash(&self, slot: Slot) -> Option<Hash> {
        self.bank_hash_cf
            .get(slot)
            .unwrap()
            .map(|versioned| versioned.frozen_hash())
    }

    pub fn is_duplicate_confirmed(&self, slot: Slot) -> bool {
        self.bank_hash_cf
            .get(slot)
            .unwrap()
            .map(|versioned| versioned.is_duplicate_confirmed())
            .unwrap_or(false)
    }

    pub fn set_duplicate_confirmed_slots_and_hashes(
        &self,
        duplicate_confirmed_slot_hashes: impl Iterator<Item = (Slot, Hash)>,
    ) -> Result<()> {
        let mut write_batch = self.db.batch()?;
        for (slot, frozen_hash) in duplicate_confirmed_slot_hashes {
            let data = FrozenHashVersioned::Current(FrozenHashStatus {
                frozen_hash,
                is_duplicate_confirmed: true,
            });
            write_batch.put::<cf::BankHash>(slot, &data)?;
        }

        self.db.write(write_batch)?;
        Ok(())
    }

    pub fn set_roots<'a>(&self, rooted_slots: impl Iterator<Item = &'a Slot>) -> Result<()> {
        let mut write_batch = self.db.batch()?;
        let mut max_new_rooted_slot = 0;
        for slot in rooted_slots {
            max_new_rooted_slot = std::cmp::max(max_new_rooted_slot, *slot);
            write_batch.put::<cf::Root>(*slot, &true)?;
        }

        self.db.write(write_batch)?;

        let mut last_root = self.last_root.write().unwrap();
        if *last_root == std::u64::MAX {
            *last_root = 0;
        }
        *last_root = cmp::max(max_new_rooted_slot, *last_root);
        Ok(())
    }

    pub fn is_dead(&self, slot: Slot) -> bool {
        matches!(
            self.db
                .get::<cf::DeadSlots>(slot)
                .expect("fetch from DeadSlots column family failed"),
            Some(true)
        )
    }

    pub fn set_dead_slot(&self, slot: Slot) -> Result<()> {
        self.dead_slots_cf.put(slot, &true)
    }

    pub fn store_duplicate_if_not_existing(
        &self,
        slot: Slot,
        shred1: Vec<u8>,
        shred2: Vec<u8>,
    ) -> Result<()> {
        if !self.has_duplicate_shreds_in_slot(slot) {
            self.store_duplicate_slot(slot, shred1, shred2)
        } else {
            Ok(())
        }
    }

    pub fn store_duplicate_slot(&self, slot: Slot, shred1: Vec<u8>, shred2: Vec<u8>) -> Result<()> {
        let duplicate_slot_proof = DuplicateSlotProof::new(shred1, shred2);
        self.duplicate_slots_cf.put(slot, &duplicate_slot_proof)
    }

    pub fn get_duplicate_slot(&self, slot: u64) -> Option<DuplicateSlotProof> {
        self.duplicate_slots_cf
            .get(slot)
            .expect("fetch from DuplicateSlots column family failed")
    }

    // `new_shred` is assumed to have slot and index equal to the given slot and index.
    // Returns the existing shred if `new_shred` is not equal to the existing shred at the
    // given slot and index as this implies the leader generated two different shreds with
    // the same slot and index
    pub fn is_shred_duplicate(
        &self,
        slot: u64,
        index: u32,
        new_shred_raw: &[u8],
        is_data: bool,
    ) -> Option<Vec<u8>> {
        let res = if is_data {
            self.get_data_shred(slot, index as u64)
                .expect("fetch from DuplicateSlots column family failed")
        } else {
            self.get_coding_shred(slot, index as u64)
                .expect("fetch from DuplicateSlots column family failed")
        };

        let mut payload = new_shred_raw.to_vec();
        payload.resize(
            std::cmp::max(new_shred_raw.len(), crate::shred::SHRED_PAYLOAD_SIZE),
            0,
        );
        let new_shred = Shred::new_from_serialized_shred(payload).unwrap();
        res.map(|existing_shred| {
            if existing_shred != new_shred.payload {
                Some(existing_shred)
            } else {
                None
            }
        })
        .unwrap_or(None)
    }

    pub fn has_duplicate_shreds_in_slot(&self, slot: Slot) -> bool {
        self.duplicate_slots_cf
            .get(slot)
            .expect("fetch from DuplicateSlots column family failed")
            .is_some()
    }

    pub fn orphans_iterator(&self, slot: Slot) -> Result<impl Iterator<Item = u64> + '_> {
        let orphans_iter = self
            .db
            .iter::<cf::Orphans>(IteratorMode::From(slot, IteratorDirection::Forward))?;
        Ok(orphans_iter.map(|(slot, _)| slot))
    }

    pub fn dead_slots_iterator(&self, slot: Slot) -> Result<impl Iterator<Item = Slot> + '_> {
        let dead_slots_iterator = self
            .db
            .iter::<cf::DeadSlots>(IteratorMode::From(slot, IteratorDirection::Forward))?;
        Ok(dead_slots_iterator.map(|(slot, _)| slot))
    }

    pub fn duplicate_slots_iterator(&self, slot: Slot) -> Result<impl Iterator<Item = Slot> + '_> {
        let duplicate_slots_iterator = self
            .db
            .iter::<cf::DuplicateSlots>(IteratorMode::From(slot, IteratorDirection::Forward))?;
        Ok(duplicate_slots_iterator.map(|(slot, _)| slot))
    }

    pub fn last_root(&self) -> Slot {
        *self.last_root.read().unwrap()
    }

    // find the first available slot in blockstore that has some data in it
    pub fn lowest_slot(&self) -> Slot {
        for (slot, meta) in self
            .slot_meta_iterator(0)
            .expect("unable to iterate over meta")
        {
            if slot > 0 && meta.received > 0 {
                return slot;
            }
        }
        // This means blockstore is empty, should never get here aside from right at boot.
        self.last_root()
    }

    pub fn lowest_cleanup_slot(&self) -> Slot {
        *self.lowest_cleanup_slot.read().unwrap()
    }

    pub fn storage_size(&self) -> Result<u64> {
        let db_size = self.db.storage_size()?;
        let shreds_size = self.shred_storage_size()?;
        Ok(db_size + shreds_size)
    }

    pub fn is_primary_access(&self) -> bool {
        self.db.is_primary_access()
    }

    pub fn scan_and_fix_roots(&self, exit: &Arc<AtomicBool>) -> Result<()> {
        let ancestor_iterator = AncestorIterator::new(self.last_root(), self)
            .take_while(|&slot| slot >= self.lowest_cleanup_slot());

        let mut find_missing_roots = Measure::start("find_missing_roots");
        let mut roots_to_fix = vec![];
        for slot in ancestor_iterator.filter(|slot| !self.is_root(*slot)) {
            if exit.load(Ordering::Relaxed) {
                return Ok(());
            }
            roots_to_fix.push(slot);
        }
        find_missing_roots.stop();
        let mut fix_roots = Measure::start("fix_roots");
        if !roots_to_fix.is_empty() {
            info!("{} slots to be rooted", roots_to_fix.len());
            for chunk in roots_to_fix.chunks(100) {
                if exit.load(Ordering::Relaxed) {
                    return Ok(());
                }
                trace!("{:?}", chunk);
                self.set_roots(chunk.iter())?;
            }
        } else {
            debug!(
                "No missing roots found in range {} to {}",
                self.lowest_cleanup_slot(),
                self.last_root()
            );
        }
        fix_roots.stop();
        datapoint_info!(
            "blockstore-scan_and_fix_roots",
            (
                "find_missing_roots_us",
                find_missing_roots.as_us() as i64,
                i64
            ),
            ("num_roots_to_fix", roots_to_fix.len() as i64, i64),
            ("fix_roots_us", fix_roots.as_us() as i64, i64),
        );
        Ok(())
    }
}

// Update the `completed_data_indexes` with a new shred `new_shred_index`. If a
// data set is complete, return the range of shred indexes [start_index, end_index]
// for that completed data set.
fn update_completed_data_indexes(
    is_last_in_data: bool,
    new_shred_index: u32,
    received_data_shreds: &ShredIndex,
    // Sorted array of shred indexes marked data complete
    completed_data_indexes: &mut Vec<u32>,
) -> Vec<(u32, u32)> {
    let mut first_greater_pos = None;
    let mut prev_completed_shred_index = None;
    // Find the first item in `completed_data_indexes > new_shred_index`
    for (i, completed_data_index) in completed_data_indexes.iter().enumerate() {
        // `completed_data_indexes` should be sorted from smallest to largest
        assert!(
            prev_completed_shred_index.is_none()
                || *completed_data_index > prev_completed_shred_index.unwrap()
        );
        if *completed_data_index > new_shred_index {
            first_greater_pos = Some(i);
            break;
        }
        prev_completed_shred_index = Some(*completed_data_index);
    }

    // Consecutive entries i, k, j in this vector represent potential ranges [i, k),
    // [k, j) that could be completed data ranges
    let mut check_ranges: Vec<u32> = vec![prev_completed_shred_index
        .map(|completed_data_shred_index| completed_data_shred_index + 1)
        .unwrap_or(0)];
    let mut first_greater_data_complete_index =
        first_greater_pos.map(|i| completed_data_indexes[i]);

    // `new_shred_index` is data complete, so need to insert here into the
    // `completed_data_indexes`
    if is_last_in_data {
        if first_greater_pos.is_some() {
            // If there exists a data complete shred greater than `new_shred_index`,
            // and the new shred is marked data complete, then the range
            // [new_shred_index + 1, completed_data_indexes[pos]] may be complete,
            // so add that range to check
            check_ranges.push(new_shred_index + 1);
        }
        completed_data_indexes.insert(
            first_greater_pos.unwrap_or_else(|| {
                // If `first_greater_pos` is none, then there was no greater
                // data complete index so mark this new shred's index as the latest data
                // complete index
                first_greater_data_complete_index = Some(new_shred_index);
                completed_data_indexes.len()
            }),
            new_shred_index,
        );
    }

    if first_greater_data_complete_index.is_none() {
        // That means new_shred_index > all known completed data indexes and
        // new shred not data complete, which means the data set of that new
        // shred is not data complete
        return vec![];
    }

    check_ranges.push(first_greater_data_complete_index.unwrap() + 1);
    let mut completed_data_ranges = vec![];
    for range in check_ranges.windows(2) {
        let mut is_complete = true;
        for shred_index in range[0]..range[1] {
            // If we're missing any shreds, the data set cannot be confirmed
            // to be completed, so check the next range
            if !received_data_shreds.is_present(shred_index as u64) {
                is_complete = false;
                break;
            }
        }
        if is_complete {
            completed_data_ranges.push((range[0], range[1] - 1));
        }
    }

    completed_data_ranges
}

fn update_slot_meta(
    is_last_in_slot: bool,
    is_last_in_data: bool,
    slot_meta: &mut SlotMeta,
    index: u32,
    new_consumed: u64,
    reference_tick: u8,
    received_data_shreds: &ShredIndex,
) -> Vec<(u32, u32)> {
    let maybe_first_insert = slot_meta.received == 0;
    // Index is zero-indexed, while the "received" height starts from 1,
    // so received = index + 1 for the same shred.
    slot_meta.received = cmp::max((u64::from(index) + 1) as u64, slot_meta.received);
    if maybe_first_insert && slot_meta.received > 0 {
        // predict the timestamp of what would have been the first shred in this slot
        let slot_time_elapsed = u64::from(reference_tick) * 1000 / DEFAULT_TICKS_PER_SECOND;
        slot_meta.first_shred_timestamp = timestamp() - slot_time_elapsed;
    }
    slot_meta.consumed = new_consumed;
    slot_meta.last_index = {
        // If the last index in the slot hasn't been set before, then
        // set it to this shred index
        if slot_meta.last_index == std::u64::MAX {
            if is_last_in_slot {
                u64::from(index)
            } else {
                std::u64::MAX
            }
        } else {
            slot_meta.last_index
        }
    };

    update_completed_data_indexes(
        is_last_in_slot || is_last_in_data,
        index,
        received_data_shreds,
        &mut slot_meta.completed_data_indexes,
    )
}

fn get_index_meta_entry<'a>(
    db: &Database,
    slot: Slot,
    index_working_set: &'a mut HashMap<u64, IndexMetaWorkingSetEntry>,
    index_meta_time: &mut u64,
) -> &'a mut IndexMetaWorkingSetEntry {
    let index_cf = db.column::<cf::Index>();
    let mut total_start = Measure::start("Total elapsed");
    let res = index_working_set.entry(slot).or_insert_with(|| {
        let newly_inserted_meta = index_cf
            .get(slot)
            .unwrap()
            .unwrap_or_else(|| Index::new(slot));
        IndexMetaWorkingSetEntry {
            index: newly_inserted_meta,
            did_insert_occur: false,
        }
    });
    total_start.stop();
    *index_meta_time += total_start.as_us();
    res
}

fn get_slot_meta_entry<'a>(
    db: &Database,
    slot_meta_working_set: &'a mut HashMap<u64, SlotMetaWorkingSetEntry>,
    slot: Slot,
    parent_slot: Slot,
) -> &'a mut SlotMetaWorkingSetEntry {
    let meta_cf = db.column::<cf::SlotMeta>();

    // Check if we've already inserted the slot metadata for this shred's slot
    slot_meta_working_set.entry(slot).or_insert_with(|| {
        // Store a 2-tuple of the metadata (working copy, backup copy)
        if let Some(mut meta) = meta_cf.get(slot).expect("Expect database get to succeed") {
            let backup = Some(meta.clone());
            // If parent_slot == std::u64::MAX, then this is one of the orphans inserted
            // during the chaining process, see the function find_slot_meta_in_cached_state()
            // for details. Slots that are orphans are missing a parent_slot, so we should
            // fill in the parent now that we know it.
            if is_orphan(&meta) {
                meta.parent_slot = parent_slot;
            }

            SlotMetaWorkingSetEntry::new(Rc::new(RefCell::new(meta)), backup)
        } else {
            SlotMetaWorkingSetEntry::new(
                Rc::new(RefCell::new(SlotMeta::new(slot, parent_slot))),
                None,
            )
        }
    })
}

fn get_last_hash<'a>(iterator: impl Iterator<Item = &'a Entry> + 'a) -> Option<Hash> {
    iterator.last().map(|entry| entry.hash)
}

fn is_valid_write_to_slot_0(slot_to_write: u64, parent_slot: Slot, last_root: u64) -> bool {
    slot_to_write == 0 && last_root == 0 && parent_slot == 0
}

fn send_signals(
    new_shreds_signals: &[SyncSender<bool>],
    completed_slots_senders: &[SyncSender<Vec<u64>>],
    should_signal: bool,
    newly_completed_slots: Vec<u64>,
) {
    if should_signal {
        for signal in new_shreds_signals {
            let _ = signal.try_send(true);
        }
    }

    if !completed_slots_senders.is_empty() && !newly_completed_slots.is_empty() {
        let mut slots: Vec<_> = (0..completed_slots_senders.len() - 1)
            .map(|_| newly_completed_slots.clone())
            .collect();

        slots.push(newly_completed_slots);

        for (signal, slots) in completed_slots_senders.iter().zip(slots.into_iter()) {
            let res = signal.try_send(slots);
            if let Err(TrySendError::Full(_)) = res {
                datapoint_error!(
                    "blockstore_error",
                    (
                        "error",
                        "Unable to send newly completed slot because channel is full".to_string(),
                        String
                    ),
                );
            }
        }
    }
}

fn commit_slot_meta_working_set(
    slot_meta_working_set: &HashMap<u64, SlotMetaWorkingSetEntry>,
    completed_slots_senders: &[SyncSender<Vec<u64>>],
    write_batch: &mut WriteBatch,
) -> Result<(bool, Vec<u64>)> {
    let mut should_signal = false;
    let mut newly_completed_slots = vec![];

    // Check if any metadata was changed, if so, insert the new version of the
    // metadata into the write batch
    for (slot, slot_meta_entry) in slot_meta_working_set.iter() {
        // Any slot that wasn't written to should have been filtered out by now.
        assert!(slot_meta_entry.did_insert_occur);
        let meta: &SlotMeta = &RefCell::borrow(&*slot_meta_entry.new_slot_meta);
        let meta_backup = &slot_meta_entry.old_slot_meta;
        if !completed_slots_senders.is_empty() && is_newly_completed_slot(meta, meta_backup) {
            newly_completed_slots.push(*slot);
        }
        // Check if the working copy of the metadata has changed
        if Some(meta) != meta_backup.as_ref() {
            should_signal = should_signal || slot_has_updates(meta, meta_backup);
            write_batch.put::<cf::SlotMeta>(*slot, meta)?;
        }
    }

    Ok((should_signal, newly_completed_slots))
}

// 1) Find the slot metadata in the cache of dirty slot metadata we've previously touched,
// else:
// 2) Search the database for that slot metadata. If still no luck, then:
// 3) Create a dummy orphan slot in the database
fn find_slot_meta_else_create<'a>(
    db: &Database,
    working_set: &'a HashMap<u64, SlotMetaWorkingSetEntry>,
    chained_slots: &'a mut HashMap<u64, Rc<RefCell<SlotMeta>>>,
    slot_index: u64,
) -> Result<Rc<RefCell<SlotMeta>>> {
    let result = find_slot_meta_in_cached_state(working_set, chained_slots, slot_index);
    if let Some(slot) = result {
        Ok(slot)
    } else {
        find_slot_meta_in_db_else_create(db, slot_index, chained_slots)
    }
}

// Search the database for that slot metadata. If still no luck, then
// create a dummy orphan slot in the database
fn find_slot_meta_in_db_else_create(
    db: &Database,
    slot: Slot,
    insert_map: &mut HashMap<u64, Rc<RefCell<SlotMeta>>>,
) -> Result<Rc<RefCell<SlotMeta>>> {
    if let Some(slot_meta) = db.column::<cf::SlotMeta>().get(slot)? {
        insert_map.insert(slot, Rc::new(RefCell::new(slot_meta)));
        Ok(insert_map.get(&slot).unwrap().clone())
    } else {
        // If this slot doesn't exist, make a orphan slot. This way we
        // remember which slots chained to this one when we eventually get a real shred
        // for this slot
        insert_map.insert(slot, Rc::new(RefCell::new(SlotMeta::new_orphan(slot))));
        Ok(insert_map.get(&slot).unwrap().clone())
    }
}

// Find the slot metadata in the cache of dirty slot metadata we've previously touched
fn find_slot_meta_in_cached_state<'a>(
    working_set: &'a HashMap<u64, SlotMetaWorkingSetEntry>,
    chained_slots: &'a HashMap<u64, Rc<RefCell<SlotMeta>>>,
    slot: Slot,
) -> Option<Rc<RefCell<SlotMeta>>> {
    if let Some(entry) = working_set.get(&slot) {
        Some(entry.new_slot_meta.clone())
    } else {
        chained_slots.get(&slot).cloned()
    }
}

// Chaining based on latest discussion here: https://github.com/solana-labs/solana/pull/2253
fn handle_chaining(
    db: &Database,
    write_batch: &mut WriteBatch,
    working_set: &mut HashMap<u64, SlotMetaWorkingSetEntry>,
) -> Result<()> {
    // Handle chaining for all the SlotMetas that were inserted into
    working_set.retain(|_, entry| entry.did_insert_occur);
    let mut new_chained_slots = HashMap::new();
    let working_set_slots: Vec<_> = working_set.keys().collect();
    for slot in working_set_slots {
        handle_chaining_for_slot(db, write_batch, working_set, &mut new_chained_slots, *slot)?;
    }

    // Write all the newly changed slots in new_chained_slots to the write_batch
    for (slot, meta) in new_chained_slots.iter() {
        let meta: &SlotMeta = &RefCell::borrow(&*meta);
        write_batch.put::<cf::SlotMeta>(*slot, meta)?;
    }
    Ok(())
}

fn handle_chaining_for_slot(
    db: &Database,
    write_batch: &mut WriteBatch,
    working_set: &HashMap<u64, SlotMetaWorkingSetEntry>,
    new_chained_slots: &mut HashMap<u64, Rc<RefCell<SlotMeta>>>,
    slot: Slot,
) -> Result<()> {
    let slot_meta_entry = working_set
        .get(&slot)
        .expect("Slot must exist in the working_set hashmap");

    let meta = &slot_meta_entry.new_slot_meta;
    let meta_backup = &slot_meta_entry.old_slot_meta;

    {
        let mut meta_mut = meta.borrow_mut();
        let was_orphan_slot = meta_backup.is_some() && is_orphan(meta_backup.as_ref().unwrap());

        // If:
        // 1) This is a new slot
        // 2) slot != 0
        // then try to chain this slot to a previous slot
        if slot != 0 {
            let prev_slot = meta_mut.parent_slot;

            // Check if the slot represented by meta_mut is either a new slot or a orphan.
            // In both cases we need to run the chaining logic b/c the parent on the slot was
            // previously unknown.
            if meta_backup.is_none() || was_orphan_slot {
                let prev_slot_meta =
                    find_slot_meta_else_create(db, working_set, new_chained_slots, prev_slot)?;

                // This is a newly inserted slot/orphan so run the chaining logic to link it to a
                // newly discovered parent
                chain_new_slot_to_prev_slot(&mut prev_slot_meta.borrow_mut(), slot, &mut meta_mut);

                // If the parent of `slot` is a newly inserted orphan, insert it into the orphans
                // column family
                if is_orphan(&RefCell::borrow(&*prev_slot_meta)) {
                    write_batch.put::<cf::Orphans>(prev_slot, &true)?;
                }
            }
        }

        // At this point this slot has received a parent, so it's no longer an orphan
        if was_orphan_slot {
            write_batch.delete::<cf::Orphans>(slot)?;
        }
    }

    // If this is a newly inserted slot, then we know the children of this slot were not previously
    // connected to the trunk of the ledger. Thus if slot.is_connected is now true, we need to
    // update all child slots with `is_connected` = true because these children are also now newly
    // connected to trunk of the ledger
    let should_propagate_is_connected =
        is_newly_completed_slot(&RefCell::borrow(&*meta), meta_backup)
            && RefCell::borrow(&*meta).is_connected;

    if should_propagate_is_connected {
        // slot_function returns a boolean indicating whether to explore the children
        // of the input slot
        let slot_function = |slot: &mut SlotMeta| {
            slot.is_connected = true;

            // We don't want to set the is_connected flag on the children of non-full
            // slots
            slot.is_full()
        };

        traverse_children_mut(
            db,
            slot,
            meta,
            working_set,
            new_chained_slots,
            slot_function,
        )?;
    }

    Ok(())
}

fn traverse_children_mut<F>(
    db: &Database,
    slot: Slot,
    slot_meta: &Rc<RefCell<SlotMeta>>,
    working_set: &HashMap<u64, SlotMetaWorkingSetEntry>,
    new_chained_slots: &mut HashMap<u64, Rc<RefCell<SlotMeta>>>,
    slot_function: F,
) -> Result<()>
where
    F: Fn(&mut SlotMeta) -> bool,
{
    let mut next_slots: Vec<(u64, Rc<RefCell<SlotMeta>>)> = vec![(slot, slot_meta.clone())];
    while !next_slots.is_empty() {
        let (_, current_slot) = next_slots.pop().unwrap();
        // Check whether we should explore the children of this slot
        if slot_function(&mut current_slot.borrow_mut()) {
            let current_slot = &RefCell::borrow(&*current_slot);
            for next_slot_index in current_slot.next_slots.iter() {
                let next_slot = find_slot_meta_else_create(
                    db,
                    working_set,
                    new_chained_slots,
                    *next_slot_index,
                )?;
                next_slots.push((*next_slot_index, next_slot));
            }
        }
    }

    Ok(())
}

fn is_orphan(meta: &SlotMeta) -> bool {
    // If we have no parent, then this is the head of a detached chain of
    // slots
    !meta.is_parent_set()
}

// 1) Chain current_slot to the previous slot defined by prev_slot_meta
// 2) Determine whether to set the is_connected flag
fn chain_new_slot_to_prev_slot(
    prev_slot_meta: &mut SlotMeta,
    current_slot: Slot,
    current_slot_meta: &mut SlotMeta,
) {
    prev_slot_meta.next_slots.push(current_slot);
    current_slot_meta.is_connected = prev_slot_meta.is_connected && prev_slot_meta.is_full();
}

fn is_newly_completed_slot(slot_meta: &SlotMeta, backup_slot_meta: &Option<SlotMeta>) -> bool {
    slot_meta.is_full()
        && (backup_slot_meta.is_none()
            || slot_meta.consumed != backup_slot_meta.as_ref().unwrap().consumed)
}

fn slot_has_updates(slot_meta: &SlotMeta, slot_meta_backup: &Option<SlotMeta>) -> bool {
    // We should signal that there are updates if we extended the chain of consecutive blocks starting
    // from block 0, which is true iff:
    // 1) The block with index prev_block_index is itself part of the trunk of consecutive blocks
    // starting from block 0,
    slot_meta.is_connected &&
        // AND either:
        // 1) The slot didn't exist in the database before, and now we have a consecutive
        // block for that slot
        ((slot_meta_backup.is_none() && slot_meta.consumed != 0) ||
        // OR
        // 2) The slot did exist, but now we have a new consecutive block for that slot
        (slot_meta_backup.is_some() && slot_meta_backup.as_ref().unwrap().consumed != slot_meta.consumed))
}

// Creates a new ledger with slot 0 full of ticks (and only ticks).
//
// Returns the blockhash that can be used to append entries with.
pub fn create_new_ledger(
    ledger_path: &Path,
    genesis_config: &GenesisConfig,
    max_genesis_archive_unpacked_size: u64,
    access_type: AccessType,
) -> Result<Hash> {
    Blockstore::destroy(ledger_path)?;
    genesis_config.write(ledger_path)?;

    // Fill slot 0 with ticks that link back to the genesis_config to bootstrap the ledger.
    let blockstore = Blockstore::open_with_access_type(ledger_path, access_type, None, false)?;
    let ticks_per_slot = genesis_config.ticks_per_slot;
    let hashes_per_tick = genesis_config.poh_config.hashes_per_tick.unwrap_or(0);
    let entries = create_ticks(ticks_per_slot, hashes_per_tick, genesis_config.hash());
    let last_hash = entries.last().unwrap().hash;
    let version = solana_sdk::shred_version::version_from_hash(&last_hash);

    let shredder = Shredder::new(0, 0, 0, version).unwrap();
    let shreds = shredder
        .entries_to_shreds(&Keypair::new(), &entries, true, 0)
        .0;
    assert!(shreds.last().unwrap().last_in_slot());

    blockstore.insert_shreds(shreds, None, false)?;
    blockstore.set_roots(std::iter::once(&0))?;
    // Explicitly close the blockstore before we create the archived genesis file
    drop(blockstore);

    let archive_path = ledger_path.join(DEFAULT_GENESIS_ARCHIVE);
    let args = vec![
        "jcfhS",
        archive_path.to_str().unwrap(),
        "-C",
        ledger_path.to_str().unwrap(),
        DEFAULT_GENESIS_FILE,
        "rocksdb",
    ];
    let output = std::process::Command::new("tar")
        .args(&args)
        .output()
        .unwrap();
    if !output.status.success() {
        use std::str::from_utf8;
        error!("tar stdout: {}", from_utf8(&output.stdout).unwrap_or("?"));
        error!("tar stderr: {}", from_utf8(&output.stderr).unwrap_or("?"));

        return Err(BlockstoreError::Io(IoError::new(
            ErrorKind::Other,
            format!(
                "Error trying to generate snapshot archive: {}",
                output.status
            ),
        )));
    }

    // ensure the genesis archive can be unpacked and it is under
    // max_genesis_archive_unpacked_size, immediately after creating it above.
    {
        let temp_dir = tempfile::tempdir_in(ledger_path).unwrap();
        // unpack into a temp dir, while completely discarding the unpacked files
        let unpack_check = unpack_genesis_archive(
            &archive_path,
            &temp_dir.into_path(),
            max_genesis_archive_unpacked_size,
        );
        if let Err(unpack_err) = unpack_check {
            // stash problematic original archived genesis related files to
            // examine them later and to prevent validator and ledger-tool from
            // naively consuming them
            let mut error_messages = String::new();

            fs::rename(
                &ledger_path.join(DEFAULT_GENESIS_ARCHIVE),
                ledger_path.join(format!("{}.failed", DEFAULT_GENESIS_ARCHIVE)),
            )
            .unwrap_or_else(|e| {
                error_messages += &format!(
                    "/failed to stash problematic {}: {}",
                    DEFAULT_GENESIS_ARCHIVE, e
                )
            });
            fs::rename(
                &ledger_path.join(DEFAULT_GENESIS_FILE),
                ledger_path.join(format!("{}.failed", DEFAULT_GENESIS_FILE)),
            )
            .unwrap_or_else(|e| {
                error_messages += &format!(
                    "/failed to stash problematic {}: {}",
                    DEFAULT_GENESIS_FILE, e
                )
            });
            fs::rename(
                &ledger_path.join("rocksdb"),
                ledger_path.join("rocksdb.failed"),
            )
            .unwrap_or_else(|e| {
                error_messages += &format!("/failed to stash problematic rocksdb: {}", e)
            });

            return Err(BlockstoreError::Io(IoError::new(
                ErrorKind::Other,
                format!(
                    "Error checking to unpack genesis archive: {}{}",
                    unpack_err, error_messages
                ),
            )));
        }
    }

    Ok(last_hash)
}

#[macro_export]
macro_rules! tmp_ledger_name {
    () => {
        &format!("{}-{}", file!(), line!())
    };
}

#[macro_export]
macro_rules! get_tmp_ledger_path {
    () => {
        $crate::blockstore::get_ledger_path_from_name($crate::tmp_ledger_name!())
    };
}

#[macro_export]
macro_rules! get_tmp_ledger_path_auto_delete {
    () => {
        $crate::blockstore::get_ledger_path_from_name_auto_delete($crate::tmp_ledger_name!())
    };
}

pub fn get_ledger_path_from_name_auto_delete(name: &str) -> TempDir {
    let path = get_ledger_path_from_name(name);
    fs::create_dir_all(&path).unwrap();
    TempDir::new_in(path).unwrap()
}

pub fn get_ledger_path_from_name(name: &str) -> PathBuf {
    use std::env;
    let out_dir = env::var("FARF_DIR").unwrap_or_else(|_| "farf".to_string());
    let keypair = Keypair::new();

    let path = [
        out_dir,
        "ledger".to_string(),
        format!("{}-{}", name, keypair.pubkey()),
    ]
    .iter()
    .collect();

    // whack any possible collision
    let _ignored = fs::remove_dir_all(&path);

    path
}

#[macro_export]
macro_rules! create_new_tmp_ledger {
    ($genesis_config:expr) => {
        $crate::blockstore::create_new_ledger_from_name(
            $crate::tmp_ledger_name!(),
            $genesis_config,
            $crate::blockstore_db::AccessType::PrimaryOnly,
        )
    };
}

pub fn verify_shred_slots(slot: Slot, parent_slot: Slot, last_root: Slot) -> bool {
    if !is_valid_write_to_slot_0(slot, parent_slot, last_root) {
        // Check that the parent_slot < slot
        if parent_slot >= slot {
            return false;
        }

        // Ignore shreds that chain to slots before the last root
        if parent_slot < last_root {
            return false;
        }

        // Above two checks guarantee that by this point, slot > last_root
    }

    true
}

// Same as `create_new_ledger()` but use a temporary ledger name based on the provided `name`
//
// Note: like `create_new_ledger` the returned ledger will have slot 0 full of ticks (and only
// ticks)
pub fn create_new_ledger_from_name(
    name: &str,
    genesis_config: &GenesisConfig,
    access_type: AccessType,
) -> (PathBuf, Hash) {
    let ledger_path = get_ledger_path_from_name(name);
    let blockhash = create_new_ledger(
        &ledger_path,
        genesis_config,
        MAX_GENESIS_ARCHIVE_UNPACKED_SIZE,
        access_type,
    )
    .unwrap();
    (ledger_path, blockhash)
}

pub fn entries_to_test_shreds(
    entries: Vec<Entry>,
    slot: Slot,
    parent_slot: Slot,
    is_full_slot: bool,
    version: u16,
) -> Vec<Shred> {
    Shredder::new(slot, parent_slot, 0, version)
        .unwrap()
        .entries_to_shreds(&Keypair::new(), &entries, is_full_slot, 0)
        .0
}

// used for tests only
pub fn make_slot_entries(
    slot: Slot,
    parent_slot: Slot,
    num_entries: u64,
) -> (Vec<Shred>, Vec<Entry>) {
    let entries = create_ticks(num_entries, 0, Hash::default());
    let shreds = entries_to_test_shreds(entries.clone(), slot, parent_slot, true, 0);
    (shreds, entries)
}

// used for tests only
pub fn make_many_slot_entries(
    start_slot: Slot,
    num_slots: u64,
    entries_per_slot: u64,
) -> (Vec<Shred>, Vec<Entry>) {
    let mut shreds = vec![];
    let mut entries = vec![];
    for slot in start_slot..start_slot + num_slots {
        let parent_slot = if slot == 0 { 0 } else { slot - 1 };

        let (slot_shreds, slot_entries) = make_slot_entries(slot, parent_slot, entries_per_slot);
        shreds.extend(slot_shreds);
        entries.extend(slot_entries);
    }

    (shreds, entries)
}

// Create shreds for slots that have a parent-child relationship defined by the input `chain`
// used for tests only
pub fn make_chaining_slot_entries(
    chain: &[u64],
    entries_per_slot: u64,
) -> Vec<(Vec<Shred>, Vec<Entry>)> {
    let mut slots_shreds_and_entries = vec![];
    for (i, slot) in chain.iter().enumerate() {
        let parent_slot = {
            if *slot == 0 || i == 0 {
                0
            } else {
                chain[i - 1]
            }
        };

        let result = make_slot_entries(*slot, parent_slot, entries_per_slot);
        slots_shreds_and_entries.push(result);
    }

    slots_shreds_and_entries
}

#[cfg(not(unix))]
fn adjust_ulimit_nofile(_enforce_ulimit_nofile: bool) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn adjust_ulimit_nofile(enforce_ulimit_nofile: bool) -> Result<()> {
    // Rocks DB likes to have many open files.  The default open file descriptor limit is
    // usually not enough
    let desired_nofile = 500000;

    fn get_nofile() -> libc::rlimit {
        let mut nofile = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut nofile) } != 0 {
            warn!("getrlimit(RLIMIT_NOFILE) failed");
        }
        nofile
    }

    let mut nofile = get_nofile();
    if nofile.rlim_cur < desired_nofile {
        nofile.rlim_cur = desired_nofile;
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &nofile) } != 0 {
            error!(
                "Unable to increase the maximum open file descriptor limit to {}",
                desired_nofile
            );

            if cfg!(target_os = "macos") {
                error!(
                    "On mac OS you may need to run |sudo launchctl limit maxfiles {} {}| first",
                    desired_nofile, desired_nofile,
                );
            }
            if enforce_ulimit_nofile {
                return Err(BlockstoreError::UnableToSetOpenFileDescriptorLimit);
            }
        }

        nofile = get_nofile();
    }
    info!("Maximum open file descriptors: {}", nofile.rlim_cur);
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        genesis_utils::{create_genesis_config, GenesisConfigInfo},
        leader_schedule::{FixedSchedule, LeaderSchedule},
        shred::{max_ticks_per_n_shreds, DataShredHeader},
    };
    use assert_matches::assert_matches;
    use bincode::serialize;
    use itertools::Itertools;
    use rand::{seq::SliceRandom, thread_rng};
    use solana_account_decoder::parse_token::UiTokenAmount;
    use solana_entry::entry::{next_entry, next_entry_mut};
    use solana_runtime::bank::{Bank, RewardType};
    use solana_sdk::{
        hash::{self, hash, Hash},
        instruction::CompiledInstruction,
        packet::PACKET_DATA_SIZE,
        pubkey::Pubkey,
        signature::Signature,
        transaction::TransactionError,
    };
    use solana_storage_proto::convert::generated;
    use solana_transaction_status::{InnerInstructions, Reward, Rewards, TransactionTokenBalance};
    use std::{sync::mpsc::channel, thread::Builder, time::Duration};

    // used for tests only
    pub(crate) fn make_slot_entries_with_transactions(num_entries: u64) -> Vec<Entry> {
        let mut entries: Vec<Entry> = Vec::new();
        for x in 0..num_entries {
            let transaction = Transaction::new_with_compiled_instructions(
                &[&Keypair::new()],
                &[solana_sdk::pubkey::new_rand()],
                Hash::default(),
                vec![solana_sdk::pubkey::new_rand()],
                vec![CompiledInstruction::new(1, &(), vec![0])],
            );
            entries.push(next_entry_mut(&mut Hash::default(), 0, vec![transaction]));
            let mut tick = create_ticks(1, 0, hash(&serialize(&x).unwrap()));
            entries.append(&mut tick);
        }
        entries
    }

    #[test]
    fn test_create_new_ledger() {
        solana_logger::setup();
        let mint_total = 1_000_000_000_000;
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(mint_total);
        let (ledger_path, _blockhash) = create_new_tmp_ledger!(&genesis_config);
        let ledger = Blockstore::open(&ledger_path).unwrap();

        let ticks = create_ticks(genesis_config.ticks_per_slot, 0, genesis_config.hash());
        let entries = ledger.get_slot_entries(0, 0).unwrap();

        assert_eq!(ticks, entries);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_double_destroy_ledger() {
        let ledger_path = get_tmp_ledger_path!();
        let ledger = Blockstore::open(&ledger_path).unwrap();

        // Create some entries; just want some data to be inserted
        let num_entries = max_ticks_per_n_shreds(1, None);
        assert!(num_entries > 0);

        let (shreds, _) = make_slot_entries(0, 0, num_entries);
        ledger.insert_shreds(shreds, None, false).unwrap();

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        // Destroy twice to ensure that destruction on non-existent blockstore is graceful
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_get_shred() {
        // Create enough entries to ensure there are at least two shreds created
        let num_entries = max_ticks_per_n_shreds(1, None) + 1;
        assert!(num_entries > 1);

        let (mut shreds, _) = make_slot_entries(0, 0, num_entries);

        let ledger_path = get_tmp_ledger_path!();
        let ledger = Blockstore::open(&ledger_path).unwrap();

        // Insert last shred, test we can retrieve it
        let last_shred = shreds.pop().unwrap();
        assert!(last_shred.index() > 0);
        ledger
            .insert_shreds(vec![last_shred.clone()], None, false)
            .unwrap();

        let serialized_shred = ledger
            .get_data_shred(0, last_shred.index() as u64)
            .unwrap()
            .unwrap();
        let deserialized_shred = Shred::new_from_serialized_shred(serialized_shred).unwrap();

        assert_eq!(last_shred, deserialized_shred);
        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_write_entries() {
        solana_logger::setup();
        let ledger_path = get_tmp_ledger_path!();
        {
            let ticks_per_slot = 10;
            let num_slots = 10;
            let ledger = Blockstore::open(&ledger_path).unwrap();
            let mut ticks = vec![];
            //let mut shreds_per_slot = 0 as u64;
            let mut shreds_per_slot = vec![];

            for i in 0..num_slots {
                let mut new_ticks = create_ticks(ticks_per_slot, 0, Hash::default());
                let num_shreds = ledger
                    .write_entries(
                        i,
                        0,
                        0,
                        ticks_per_slot,
                        Some(i.saturating_sub(1)),
                        true,
                        &Arc::new(Keypair::new()),
                        new_ticks.clone(),
                        0,
                    )
                    .unwrap() as u64;
                shreds_per_slot.push(num_shreds);
                ticks.append(&mut new_ticks);
            }

            for i in 0..num_slots {
                let meta = ledger.meta(i).unwrap().unwrap();
                let num_shreds = shreds_per_slot[i as usize];
                assert_eq!(meta.consumed, num_shreds);
                assert_eq!(meta.received, num_shreds);
                assert_eq!(meta.last_index, num_shreds - 1);
                if i == num_slots - 1 {
                    assert!(meta.next_slots.is_empty());
                } else {
                    assert_eq!(meta.next_slots, vec![i + 1]);
                }
                if i == 0 {
                    assert_eq!(meta.parent_slot, 0);
                } else {
                    assert_eq!(meta.parent_slot, i - 1);
                }

                assert_eq!(
                    &ticks[(i * ticks_per_slot) as usize..((i + 1) * ticks_per_slot) as usize],
                    &ledger.get_slot_entries(i, 0).unwrap()[..]
                );
            }

            /*
                        // Simulate writing to the end of a slot with existing ticks
                        ledger
                            .write_entries(
                                num_slots,
                                ticks_per_slot - 1,
                                ticks_per_slot - 2,
                                ticks_per_slot,
                                &ticks[0..2],
                            )
                            .unwrap();

                        let meta = ledger.meta(num_slots).unwrap().unwrap();
                        assert_eq!(meta.consumed, 0);
                        // received shred was ticks_per_slot - 2, so received should be ticks_per_slot - 2 + 1
                        assert_eq!(meta.received, ticks_per_slot - 1);
                        // last shred index ticks_per_slot - 2 because that's the shred that made tick_height == ticks_per_slot
                        // for the slot
                        assert_eq!(meta.last_index, ticks_per_slot - 2);
                        assert_eq!(meta.parent_slot, num_slots - 1);
                        assert_eq!(meta.next_slots, vec![num_slots + 1]);
                        assert_eq!(
                            &ticks[0..1],
                            &ledger
                                .get_slot_entries(num_slots, ticks_per_slot - 2)
                                .unwrap()[..]
                        );

                        // We wrote two entries, the second should spill into slot num_slots + 1
                        let meta = ledger.meta(num_slots + 1).unwrap().unwrap();
                        assert_eq!(meta.consumed, 1);
                        assert_eq!(meta.received, 1);
                        assert_eq!(meta.last_index, std::u64::MAX);
                        assert_eq!(meta.parent_slot, num_slots);
                        assert!(meta.next_slots.is_empty());

                        assert_eq!(
                            &ticks[1..2],
                            &ledger.get_slot_entries(num_slots + 1, 0).unwrap()[..]
                        );
            */
        }
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_put_get_simple() {
        let ledger_path = get_tmp_ledger_path!();
        let ledger = Blockstore::open(&ledger_path).unwrap();

        // Test meta column family
        let meta = SlotMeta::new(0, 1);
        ledger.meta_cf.put(0, &meta).unwrap();
        let result = ledger
            .meta_cf
            .get(0)
            .unwrap()
            .expect("Expected meta object to exist");

        assert_eq!(result, meta);

        // Test erasure column family
        let erasure = vec![1u8; 16];
        let erasure_key = (0, 0);
        ledger
            .code_shred_cf
            .put_bytes(erasure_key, &erasure)
            .unwrap();

        let result = ledger
            .code_shred_cf
            .get_bytes(erasure_key)
            .unwrap()
            .expect("Expected erasure object to exist");

        assert_eq!(result, erasure);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_read_shred_bytes() {
        let slot = 0;
        let (shreds, _) = make_slot_entries(slot, 0, 100);
        let num_shreds = shreds.len() as u64;
        let shred_bufs: Vec<_> = shreds.iter().map(|shred| shred.payload.clone()).collect();

        let ledger_path = get_tmp_ledger_path!();
        let ledger = Blockstore::open(&ledger_path).unwrap();
        ledger.insert_shreds(shreds, None, false).unwrap();

        let mut buf = [0; 4096];
        let (_, bytes) = ledger.get_data_shreds(slot, 0, 1, &mut buf).unwrap();
        assert_eq!(buf[..bytes], shred_bufs[0][..bytes]);

        let (last_index, bytes2) = ledger.get_data_shreds(slot, 0, 2, &mut buf).unwrap();
        assert_eq!(last_index, 1);
        assert!(bytes2 > bytes);
        {
            let shred_data_1 = &buf[..bytes];
            assert_eq!(shred_data_1, &shred_bufs[0][..bytes]);

            let shred_data_2 = &buf[bytes..bytes2];
            assert_eq!(shred_data_2, &shred_bufs[1][..bytes2 - bytes]);
        }

        // buf size part-way into shred[1], should just return shred[0]
        let mut buf = vec![0; bytes + 1];
        let (last_index, bytes3) = ledger.get_data_shreds(slot, 0, 2, &mut buf).unwrap();
        assert_eq!(last_index, 0);
        assert_eq!(bytes3, bytes);

        let mut buf = vec![0; bytes2 - 1];
        let (last_index, bytes4) = ledger.get_data_shreds(slot, 0, 2, &mut buf).unwrap();
        assert_eq!(last_index, 0);
        assert_eq!(bytes4, bytes);

        let mut buf = vec![0; bytes * 2];
        let (last_index, bytes6) = ledger
            .get_data_shreds(slot, num_shreds - 1, num_shreds, &mut buf)
            .unwrap();
        assert_eq!(last_index, num_shreds - 1);

        {
            let shred_data = &buf[..bytes6];
            assert_eq!(shred_data, &shred_bufs[(num_shreds - 1) as usize][..bytes6]);
        }

        // Read out of range
        let (last_index, bytes6) = ledger
            .get_data_shreds(slot, num_shreds, num_shreds + 2, &mut buf)
            .unwrap();
        assert_eq!(last_index, 0);
        assert_eq!(bytes6, 0);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_shred_cleanup_check() {
        let slot = 1;
        let (shreds, _) = make_slot_entries(slot, 0, 100);

        let ledger_path = get_tmp_ledger_path!();
        let ledger = Blockstore::open(&ledger_path).unwrap();
        ledger.insert_shreds(shreds, None, false).unwrap();

        let mut buf = [0; 4096];
        assert!(ledger.get_data_shreds(slot, 0, 1, &mut buf).is_ok());

        let max_purge_slot = 1;
        ledger
            .run_purge(0, max_purge_slot, PurgeType::PrimaryIndex)
            .unwrap();
        *ledger.lowest_cleanup_slot.write().unwrap() = max_purge_slot;

        let mut buf = [0; 4096];
        assert!(ledger.get_data_shreds(slot, 0, 1, &mut buf).is_err());
    }

    #[test]
    fn test_insert_data_shreds_basic() {
        // Create enough entries to ensure there are at least two shreds created
        let num_entries = max_ticks_per_n_shreds(1, None) + 1;
        assert!(num_entries > 1);

        let (mut shreds, entries) = make_slot_entries(0, 0, num_entries);
        let num_shreds = shreds.len() as u64;

        let ledger_path = get_tmp_ledger_path!();
        let ledger = Blockstore::open(&ledger_path).unwrap();

        // Insert last shred, we're missing the other shreds, so no consecutive
        // shreds starting from slot 0, index 0 should exist.
        assert!(shreds.len() > 1);
        let last_shred = shreds.pop().unwrap();
        ledger.insert_shreds(vec![last_shred], None, false).unwrap();
        assert!(ledger.get_slot_entries(0, 0).unwrap().is_empty());

        let meta = ledger
            .meta(0)
            .unwrap()
            .expect("Expected new metadata object to be created");
        assert!(meta.consumed == 0 && meta.received == num_shreds);

        // Insert the other shreds, check for consecutive returned entries
        ledger.insert_shreds(shreds, None, false).unwrap();
        let result = ledger.get_slot_entries(0, 0).unwrap();

        assert_eq!(result, entries);

        let meta = ledger
            .meta(0)
            .unwrap()
            .expect("Expected new metadata object to exist");
        assert_eq!(meta.consumed, num_shreds);
        assert_eq!(meta.received, num_shreds);
        assert_eq!(meta.parent_slot, 0);
        assert_eq!(meta.last_index, num_shreds - 1);
        assert!(meta.next_slots.is_empty());
        assert!(meta.is_connected);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_data_shreds_reverse() {
        let num_shreds = 10;
        let num_entries = max_ticks_per_n_shreds(num_shreds, None);
        let (mut shreds, entries) = make_slot_entries(0, 0, num_entries);
        let num_shreds = shreds.len() as u64;

        let ledger_path = get_tmp_ledger_path!();
        let ledger = Blockstore::open(&ledger_path).unwrap();

        // Insert shreds in reverse, check for consecutive returned shreds
        for i in (0..num_shreds).rev() {
            let shred = shreds.pop().unwrap();
            ledger.insert_shreds(vec![shred], None, false).unwrap();
            let result = ledger.get_slot_entries(0, 0).unwrap();

            let meta = ledger
                .meta(0)
                .unwrap()
                .expect("Expected metadata object to exist");
            assert_eq!(meta.last_index, num_shreds - 1);
            if i != 0 {
                assert_eq!(result.len(), 0);
                assert!(meta.consumed == 0 && meta.received == num_shreds as u64);
            } else {
                assert_eq!(meta.parent_slot, 0);
                assert_eq!(result, entries);
                assert!(meta.consumed == num_shreds as u64 && meta.received == num_shreds as u64);
            }
        }

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_slots() {
        test_insert_data_shreds_slots("test_insert_data_shreds_slots_single", false);
        test_insert_data_shreds_slots("test_insert_data_shreds_slots_bulk", true);
    }

    /*
        #[test]
        pub fn test_iteration_order() {
            let slot = 0;
            let blockstore_path = get_tmp_ledger_path!();
            {
                let blockstore = Blockstore::open(&blockstore_path).unwrap();

                // Write entries
                let num_entries = 8;
                let entries = make_tiny_test_entries(num_entries);
                let mut shreds = entries.to_single_entry_shreds();

                for (i, b) in shreds.iter_mut().enumerate() {
                    b.set_index(1 << (i * 8));
                    b.set_slot(0);
                }

                blockstore
                    .write_shreds(&shreds)
                    .expect("Expected successful write of shreds");

                let mut db_iterator = blockstore
                    .db
                    .cursor::<cf::Data>()
                    .expect("Expected to be able to open database iterator");

                db_iterator.seek((slot, 1));

                // Iterate through ledger
                for i in 0..num_entries {
                    assert!(db_iterator.valid());
                    let (_, current_index) = db_iterator.key().expect("Expected a valid key");
                    assert_eq!(current_index, (1 as u64) << (i * 8));
                    db_iterator.next();
                }
            }
            Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
        }
    */

    #[test]
    pub fn test_get_slot_entries1() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let entries = create_ticks(8, 0, Hash::default());
            let shreds = entries_to_test_shreds(entries[0..4].to_vec(), 1, 0, false, 0);
            blockstore
                .insert_shreds(shreds, None, false)
                .expect("Expected successful write of shreds");

            let mut shreds1 = entries_to_test_shreds(entries[4..].to_vec(), 1, 0, false, 0);
            for (i, b) in shreds1.iter_mut().enumerate() {
                b.set_index(8 + i as u32);
            }
            blockstore
                .insert_shreds(shreds1, None, false)
                .expect("Expected successful write of shreds");

            assert_eq!(
                blockstore.get_slot_entries(1, 0).unwrap()[2..4],
                entries[2..4],
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    // This test seems to be unnecessary with introduction of data shreds. There are no
    // guarantees that a particular shred index contains a complete entry
    #[test]
    #[ignore]
    pub fn test_get_slot_entries2() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            // Write entries
            let num_slots = 5_u64;
            let mut index = 0;
            for slot in 0..num_slots {
                let entries = create_ticks(slot + 1, 0, Hash::default());
                let last_entry = entries.last().unwrap().clone();
                let mut shreds =
                    entries_to_test_shreds(entries, slot, slot.saturating_sub(1), false, 0);
                for b in shreds.iter_mut() {
                    b.set_index(index);
                    b.set_slot(slot as u64);
                    index += 1;
                }
                blockstore
                    .insert_shreds(shreds, None, false)
                    .expect("Expected successful write of shreds");
                assert_eq!(
                    blockstore
                        .get_slot_entries(slot, u64::from(index - 1))
                        .unwrap(),
                    vec![last_entry],
                );
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_get_slot_entries3() {
        // Test inserting/fetching shreds which contain multiple entries per shred
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let num_slots = 5_u64;
            let shreds_per_slot = 5_u64;
            let entry_serialized_size =
                bincode::serialized_size(&create_ticks(1, 0, Hash::default())).unwrap();
            let entries_per_slot =
                (shreds_per_slot * PACKET_DATA_SIZE as u64) / entry_serialized_size;

            // Write entries
            for slot in 0..num_slots {
                let entries = create_ticks(entries_per_slot, 0, Hash::default());
                let shreds =
                    entries_to_test_shreds(entries.clone(), slot, slot.saturating_sub(1), false, 0);
                assert!(shreds.len() as u64 >= shreds_per_slot);
                blockstore
                    .insert_shreds(shreds, None, false)
                    .expect("Expected successful write of shreds");
                assert_eq!(blockstore.get_slot_entries(slot, 0).unwrap(), entries);
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_insert_data_shreds_consecutive() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            // Create enough entries to ensure there are at least two shreds created
            let min_entries = max_ticks_per_n_shreds(1, None) + 1;
            for i in 0..4 {
                let slot = i;
                let parent_slot = if i == 0 { 0 } else { i - 1 };
                // Write entries
                let num_entries = min_entries * (i + 1);
                let (shreds, original_entries) = make_slot_entries(slot, parent_slot, num_entries);

                let num_shreds = shreds.len() as u64;
                assert!(num_shreds > 1);
                let mut even_shreds = vec![];
                let mut odd_shreds = vec![];

                for (i, shred) in shreds.into_iter().enumerate() {
                    if i % 2 == 0 {
                        even_shreds.push(shred);
                    } else {
                        odd_shreds.push(shred);
                    }
                }

                blockstore.insert_shreds(odd_shreds, None, false).unwrap();

                assert_eq!(blockstore.get_slot_entries(slot, 0).unwrap(), vec![]);

                let meta = blockstore.meta(slot).unwrap().unwrap();
                if num_shreds % 2 == 0 {
                    assert_eq!(meta.received, num_shreds);
                } else {
                    trace!("got here");
                    assert_eq!(meta.received, num_shreds - 1);
                }
                assert_eq!(meta.consumed, 0);
                if num_shreds % 2 == 0 {
                    assert_eq!(meta.last_index, num_shreds - 1);
                } else {
                    assert_eq!(meta.last_index, std::u64::MAX);
                }

                blockstore.insert_shreds(even_shreds, None, false).unwrap();

                assert_eq!(
                    blockstore.get_slot_entries(slot, 0).unwrap(),
                    original_entries,
                );

                let meta = blockstore.meta(slot).unwrap().unwrap();
                assert_eq!(meta.received, num_shreds);
                assert_eq!(meta.consumed, num_shreds);
                assert_eq!(meta.parent_slot, parent_slot);
                assert_eq!(meta.last_index, num_shreds - 1);
            }
        }

        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_data_set_completed_on_insert() {
        let ledger_path = get_tmp_ledger_path!();
        let BlockstoreSignals { blockstore, .. } =
            Blockstore::open_with_signal(&ledger_path, None, true).unwrap();

        // Create enough entries to fill 2 shreds, only the later one is data complete
        let slot = 0;
        let num_entries = max_ticks_per_n_shreds(1, None) + 1;
        let entries = create_ticks(num_entries, slot, Hash::default());
        let shreds = entries_to_test_shreds(entries, slot, 0, true, 0);
        let num_shreds = shreds.len();
        assert!(num_shreds > 1);
        assert!(blockstore
            .insert_shreds(shreds[1..].to_vec(), None, false)
            .unwrap()
            .0
            .is_empty());
        assert_eq!(
            blockstore
                .insert_shreds(vec![shreds[0].clone()], None, false)
                .unwrap()
                .0,
            vec![CompletedDataSetInfo {
                slot,
                start_index: 0,
                end_index: num_shreds as u32 - 1
            }]
        );
        // Inserting shreds again doesn't trigger notification
        assert!(blockstore
            .insert_shreds(shreds, None, false)
            .unwrap()
            .0
            .is_empty());
    }

    #[test]
    pub fn test_new_shreds_signal() {
        // Initialize ledger
        let ledger_path = get_tmp_ledger_path!();
        let BlockstoreSignals {
            blockstore: ledger,
            ledger_signal_receiver: recvr,
            ..
        } = Blockstore::open_with_signal(&ledger_path, None, true).unwrap();
        let ledger = Arc::new(ledger);

        let entries_per_slot = 50;
        // Create entries for slot 0
        let (mut shreds, _) = make_slot_entries(0, 0, entries_per_slot);
        let shreds_per_slot = shreds.len() as u64;

        // Insert second shred, but we're missing the first shred, so no consecutive
        // shreds starting from slot 0, index 0 should exist.
        ledger
            .insert_shreds(vec![shreds.remove(1)], None, false)
            .unwrap();
        let timer = Duration::new(1, 0);
        assert!(recvr.recv_timeout(timer).is_err());
        // Insert first shred, now we've made a consecutive block
        ledger
            .insert_shreds(vec![shreds.remove(0)], None, false)
            .unwrap();
        // Wait to get notified of update, should only be one update
        assert!(recvr.recv_timeout(timer).is_ok());
        assert!(recvr.try_recv().is_err());
        // Insert the rest of the ticks
        ledger.insert_shreds(shreds, None, false).unwrap();
        // Wait to get notified of update, should only be one update
        assert!(recvr.recv_timeout(timer).is_ok());
        assert!(recvr.try_recv().is_err());

        // Create some other slots, and send batches of ticks for each slot such that each slot
        // is missing the tick at shred index == slot index - 1. Thus, no consecutive blocks
        // will be formed
        let num_slots = shreds_per_slot;
        let mut shreds = vec![];
        let mut missing_shreds = vec![];
        for slot in 1..num_slots + 1 {
            let (mut slot_shreds, _) = make_slot_entries(slot, slot - 1, entries_per_slot);
            let missing_shred = slot_shreds.remove(slot as usize - 1);
            shreds.extend(slot_shreds);
            missing_shreds.push(missing_shred);
        }

        // Should be no updates, since no new chains from block 0 were formed
        ledger.insert_shreds(shreds, None, false).unwrap();
        assert!(recvr.recv_timeout(timer).is_err());

        // Insert a shred for each slot that doesn't make a consecutive block, we
        // should get no updates
        let shreds: Vec<_> = (1..num_slots + 1)
            .flat_map(|slot| {
                let (mut shred, _) = make_slot_entries(slot, slot - 1, 1);
                shred[0].set_index(2 * num_slots as u32);
                shred
            })
            .collect();

        ledger.insert_shreds(shreds, None, false).unwrap();
        assert!(recvr.recv_timeout(timer).is_err());

        // For slots 1..num_slots/2, fill in the holes in one batch insertion,
        // so we should only get one signal
        let missing_shreds2 = missing_shreds
            .drain((num_slots / 2) as usize..)
            .collect_vec();
        ledger.insert_shreds(missing_shreds, None, false).unwrap();
        assert!(recvr.recv_timeout(timer).is_ok());
        assert!(recvr.try_recv().is_err());

        // Fill in the holes for each of the remaining slots, we should get a single update
        // for each
        ledger.insert_shreds(missing_shreds2, None, false).unwrap();

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_completed_shreds_signal() {
        // Initialize ledger
        let ledger_path = get_tmp_ledger_path!();
        let BlockstoreSignals {
            blockstore: ledger,
            completed_slots_receiver: recvr,
            ..
        } = Blockstore::open_with_signal(&ledger_path, None, true).unwrap();
        let ledger = Arc::new(ledger);

        let entries_per_slot = 10;

        // Create shreds for slot 0
        let (mut shreds, _) = make_slot_entries(0, 0, entries_per_slot);

        let shred0 = shreds.remove(0);
        // Insert all but the first shred in the slot, should not be considered complete
        ledger.insert_shreds(shreds, None, false).unwrap();
        assert!(recvr.try_recv().is_err());

        // Insert first shred, slot should now be considered complete
        ledger.insert_shreds(vec![shred0], None, false).unwrap();
        assert_eq!(recvr.try_recv().unwrap(), vec![0]);
    }

    #[test]
    pub fn test_completed_shreds_signal_orphans() {
        // Initialize ledger
        let ledger_path = get_tmp_ledger_path!();
        let BlockstoreSignals {
            blockstore: ledger,
            completed_slots_receiver: recvr,
            ..
        } = Blockstore::open_with_signal(&ledger_path, None, true).unwrap();
        let ledger = Arc::new(ledger);

        let entries_per_slot = 10;
        let slots = vec![2, 5, 10];
        let mut all_shreds = make_chaining_slot_entries(&slots[..], entries_per_slot);

        // Get the shreds for slot 10, chaining to slot 5
        let (mut orphan_child, _) = all_shreds.remove(2);

        // Get the shreds for slot 5 chaining to slot 2
        let (mut orphan_shreds, _) = all_shreds.remove(1);

        // Insert all but the first shred in the slot, should not be considered complete
        let orphan_child0 = orphan_child.remove(0);
        ledger.insert_shreds(orphan_child, None, false).unwrap();
        assert!(recvr.try_recv().is_err());

        // Insert first shred, slot should now be considered complete
        ledger
            .insert_shreds(vec![orphan_child0], None, false)
            .unwrap();
        assert_eq!(recvr.try_recv().unwrap(), vec![slots[2]]);

        // Insert the shreds for the orphan_slot
        let orphan_shred0 = orphan_shreds.remove(0);
        ledger.insert_shreds(orphan_shreds, None, false).unwrap();
        assert!(recvr.try_recv().is_err());

        // Insert first shred, slot should now be considered complete
        ledger
            .insert_shreds(vec![orphan_shred0], None, false)
            .unwrap();
        assert_eq!(recvr.try_recv().unwrap(), vec![slots[1]]);
    }

    #[test]
    pub fn test_completed_shreds_signal_many() {
        // Initialize ledger
        let ledger_path = get_tmp_ledger_path!();
        let BlockstoreSignals {
            blockstore: ledger,
            completed_slots_receiver: recvr,
            ..
        } = Blockstore::open_with_signal(&ledger_path, None, true).unwrap();
        let ledger = Arc::new(ledger);

        let entries_per_slot = 10;
        let mut slots = vec![2, 5, 10];
        let mut all_shreds = make_chaining_slot_entries(&slots[..], entries_per_slot);
        let disconnected_slot = 4;

        let (shreds0, _) = all_shreds.remove(0);
        let (shreds1, _) = all_shreds.remove(0);
        let (shreds2, _) = all_shreds.remove(0);
        let (shreds3, _) = make_slot_entries(disconnected_slot, 1, entries_per_slot);

        let mut all_shreds: Vec<_> = vec![shreds0, shreds1, shreds2, shreds3]
            .into_iter()
            .flatten()
            .collect();

        all_shreds.shuffle(&mut thread_rng());
        ledger.insert_shreds(all_shreds, None, false).unwrap();
        let mut result = recvr.try_recv().unwrap();
        result.sort_unstable();
        slots.push(disconnected_slot);
        slots.sort_unstable();
        assert_eq!(result, slots);
    }

    #[test]
    pub fn test_handle_chaining_basic() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let entries_per_slot = 5;
            let num_slots = 3;
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            // Construct the shreds
            let (mut shreds, _) = make_many_slot_entries(0, num_slots, entries_per_slot);
            let shreds_per_slot = shreds.len() / num_slots as usize;

            // 1) Write to the first slot
            let shreds1 = shreds
                .drain(shreds_per_slot..2 * shreds_per_slot)
                .collect_vec();
            blockstore.insert_shreds(shreds1, None, false).unwrap();
            let s1 = blockstore.meta(1).unwrap().unwrap();
            assert!(s1.next_slots.is_empty());
            // Slot 1 is not trunk because slot 0 hasn't been inserted yet
            assert!(!s1.is_connected);
            assert_eq!(s1.parent_slot, 0);
            assert_eq!(s1.last_index, shreds_per_slot as u64 - 1);

            // 2) Write to the second slot
            let shreds2 = shreds
                .drain(shreds_per_slot..2 * shreds_per_slot)
                .collect_vec();
            blockstore.insert_shreds(shreds2, None, false).unwrap();
            let s2 = blockstore.meta(2).unwrap().unwrap();
            assert!(s2.next_slots.is_empty());
            // Slot 2 is not trunk because slot 0 hasn't been inserted yet
            assert!(!s2.is_connected);
            assert_eq!(s2.parent_slot, 1);
            assert_eq!(s2.last_index, shreds_per_slot as u64 - 1);

            // Check the first slot again, it should chain to the second slot,
            // but still isn't part of the trunk
            let s1 = blockstore.meta(1).unwrap().unwrap();
            assert_eq!(s1.next_slots, vec![2]);
            assert!(!s1.is_connected);
            assert_eq!(s1.parent_slot, 0);
            assert_eq!(s1.last_index, shreds_per_slot as u64 - 1);

            // 3) Write to the zeroth slot, check that every slot
            // is now part of the trunk
            blockstore.insert_shreds(shreds, None, false).unwrap();
            for i in 0..3 {
                let s = blockstore.meta(i).unwrap().unwrap();
                // The last slot will not chain to any other slots
                if i != 2 {
                    assert_eq!(s.next_slots, vec![i + 1]);
                }
                if i == 0 {
                    assert_eq!(s.parent_slot, 0);
                } else {
                    assert_eq!(s.parent_slot, i - 1);
                }
                assert_eq!(s.last_index, shreds_per_slot as u64 - 1);
                assert!(s.is_connected);
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_handle_chaining_missing_slots() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let num_slots = 30;
            let entries_per_slot = 5;

            // Separate every other slot into two separate vectors
            let mut slots = vec![];
            let mut missing_slots = vec![];
            let mut shreds_per_slot = 2;
            for slot in 0..num_slots {
                let parent_slot = {
                    if slot == 0 {
                        0
                    } else {
                        slot - 1
                    }
                };
                let (slot_shreds, _) = make_slot_entries(slot, parent_slot, entries_per_slot);
                shreds_per_slot = slot_shreds.len();

                if slot % 2 == 1 {
                    slots.extend(slot_shreds);
                } else {
                    missing_slots.extend(slot_shreds);
                }
            }

            // Write the shreds for every other slot
            blockstore.insert_shreds(slots, None, false).unwrap();

            // Check metadata
            for i in 0..num_slots {
                // If "i" is the index of a slot we just inserted, then next_slots should be empty
                // for slot "i" because no slots chain to that slot, because slot i + 1 is missing.
                // However, if it's a slot we haven't inserted, aka one of the gaps, then one of the
                // slots we just inserted will chain to that gap, so next_slots for that orphan slot
                // won't be empty, but the parent slot is unknown so should equal std::u64::MAX.
                let s = blockstore.meta(i as u64).unwrap().unwrap();
                if i % 2 == 0 {
                    assert_eq!(s.next_slots, vec![i as u64 + 1]);
                    assert_eq!(s.parent_slot, std::u64::MAX);
                } else {
                    assert!(s.next_slots.is_empty());
                    assert_eq!(s.parent_slot, i - 1);
                }

                if i == 0 {
                    assert!(s.is_connected);
                } else {
                    assert!(!s.is_connected);
                }
            }

            // Write the shreds for the other half of the slots that we didn't insert earlier
            blockstore
                .insert_shreds(missing_slots, None, false)
                .unwrap();

            for i in 0..num_slots {
                // Check that all the slots chain correctly once the missing slots
                // have been filled
                let s = blockstore.meta(i as u64).unwrap().unwrap();
                if i != num_slots - 1 {
                    assert_eq!(s.next_slots, vec![i as u64 + 1]);
                } else {
                    assert!(s.next_slots.is_empty());
                }

                if i == 0 {
                    assert_eq!(s.parent_slot, 0);
                } else {
                    assert_eq!(s.parent_slot, i - 1);
                }
                assert_eq!(s.last_index, shreds_per_slot as u64 - 1);
                assert!(s.is_connected);
            }
        }

        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    pub fn test_forward_chaining_is_connected() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let num_slots = 15;
            // Create enough entries to ensure there are at least two shreds created
            let entries_per_slot = max_ticks_per_n_shreds(1, None) + 1;
            assert!(entries_per_slot > 1);

            let (mut shreds, _) = make_many_slot_entries(0, num_slots, entries_per_slot);
            let shreds_per_slot = shreds.len() / num_slots as usize;
            assert!(shreds_per_slot > 1);

            // Write the shreds such that every 3rd slot has a gap in the beginning
            let mut missing_shreds = vec![];
            for slot in 0..num_slots {
                let mut shreds_for_slot = shreds.drain(..shreds_per_slot).collect_vec();
                if slot % 3 == 0 {
                    let shred0 = shreds_for_slot.remove(0);
                    missing_shreds.push(shred0);
                }
                blockstore
                    .insert_shreds(shreds_for_slot, None, false)
                    .unwrap();
            }

            // Check metadata
            for i in 0..num_slots {
                let s = blockstore.meta(i as u64).unwrap().unwrap();
                // The last slot will not chain to any other slots
                if i as u64 != num_slots - 1 {
                    assert_eq!(s.next_slots, vec![i as u64 + 1]);
                } else {
                    assert!(s.next_slots.is_empty());
                }

                if i == 0 {
                    assert_eq!(s.parent_slot, 0);
                } else {
                    assert_eq!(s.parent_slot, i - 1);
                }

                assert_eq!(s.last_index, shreds_per_slot as u64 - 1);

                // Other than slot 0, no slots should be part of the trunk
                if i != 0 {
                    assert!(!s.is_connected);
                } else {
                    assert!(s.is_connected);
                }
            }

            // Iteratively finish every 3rd slot, and check that all slots up to and including
            // slot_index + 3 become part of the trunk
            for slot_index in 0..num_slots {
                if slot_index % 3 == 0 {
                    let shred = missing_shreds.remove(0);
                    blockstore.insert_shreds(vec![shred], None, false).unwrap();

                    for i in 0..num_slots {
                        let s = blockstore.meta(i as u64).unwrap().unwrap();
                        if i != num_slots - 1 {
                            assert_eq!(s.next_slots, vec![i as u64 + 1]);
                        } else {
                            assert!(s.next_slots.is_empty());
                        }
                        if i <= slot_index as u64 + 3 {
                            assert!(s.is_connected);
                        } else {
                            assert!(!s.is_connected);
                        }

                        if i == 0 {
                            assert_eq!(s.parent_slot, 0);
                        } else {
                            assert_eq!(s.parent_slot, i - 1);
                        }

                        assert_eq!(s.last_index, shreds_per_slot as u64 - 1);
                    }
                }
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }
    /*
        #[test]
        pub fn test_chaining_tree() {
            let blockstore_path = get_tmp_ledger_path!();
            {
                let blockstore = Blockstore::open(&blockstore_path).unwrap();
                let num_tree_levels = 6;
                assert!(num_tree_levels > 1);
                let branching_factor: u64 = 4;
                // Number of slots that will be in the tree
                let num_slots = (branching_factor.pow(num_tree_levels) - 1) / (branching_factor - 1);
                let erasure_config = ErasureConfig::default();
                let entries_per_slot = erasure_config.num_data() as u64;
                assert!(entries_per_slot > 1);

                let (mut shreds, _) = make_many_slot_entries(0, num_slots, entries_per_slot);

                // Insert tree one slot at a time in a random order
                let mut slots: Vec<_> = (0..num_slots).collect();

                // Get shreds for the slot
                slots.shuffle(&mut thread_rng());
                for slot in slots {
                    // Get shreds for the slot "slot"
                    let slot_shreds = &mut shreds
                        [(slot * entries_per_slot) as usize..((slot + 1) * entries_per_slot) as usize];
                    for shred in slot_shreds.iter_mut() {
                        // Get the parent slot of the slot in the tree
                        let slot_parent = {
                            if slot == 0 {
                                0
                            } else {
                                (slot - 1) / branching_factor
                            }
                        };
                        shred.set_parent(slot_parent);
                    }

                    let shared_shreds: Vec<_> = slot_shreds
                        .iter()
                        .cloned()
                        .map(|shred| Arc::new(RwLock::new(shred)))
                        .collect();
                    let mut coding_generator = CodingGenerator::new_from_config(&erasure_config);
                    let coding_shreds = coding_generator.next(&shared_shreds);
                    assert_eq!(coding_shreds.len(), erasure_config.num_coding());

                    let mut rng = thread_rng();

                    // Randomly pick whether to insert erasure or coding shreds first
                    if rng.gen_bool(0.5) {
                        blockstore.write_shreds(slot_shreds).unwrap();
                        blockstore.put_shared_coding_shreds(&coding_shreds).unwrap();
                    } else {
                        blockstore.put_shared_coding_shreds(&coding_shreds).unwrap();
                        blockstore.write_shreds(slot_shreds).unwrap();
                    }
                }

                // Make sure everything chains correctly
                let last_level =
                    (branching_factor.pow(num_tree_levels - 1) - 1) / (branching_factor - 1);
                for slot in 0..num_slots {
                    let slot_meta = blockstore.meta(slot).unwrap().unwrap();
                    assert_eq!(slot_meta.consumed, entries_per_slot);
                    assert_eq!(slot_meta.received, entries_per_slot);
                    assert!(slot_meta.is_connected);
                    let slot_parent = {
                        if slot == 0 {
                            0
                        } else {
                            (slot - 1) / branching_factor
                        }
                    };
                    assert_eq!(slot_meta.parent_slot, slot_parent);

                    let expected_children: HashSet<_> = {
                        if slot >= last_level {
                            HashSet::new()
                        } else {
                            let first_child_slot = min(num_slots - 1, slot * branching_factor + 1);
                            let last_child_slot = min(num_slots - 1, (slot + 1) * branching_factor);
                            (first_child_slot..last_child_slot + 1).collect()
                        }
                    };

                    let result: HashSet<_> = slot_meta.next_slots.iter().cloned().collect();
                    if expected_children.len() != 0 {
                        assert_eq!(slot_meta.next_slots.len(), branching_factor as usize);
                    } else {
                        assert_eq!(slot_meta.next_slots.len(), 0);
                    }
                    assert_eq!(expected_children, result);
                }

                // No orphan slots should exist
                assert!(blockstore.orphans_cf.is_empty().unwrap())
            }

            Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
        }
    */
    #[test]
    pub fn test_get_slots_since() {
        let blockstore_path = get_tmp_ledger_path!();

        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            // Slot doesn't exist
            assert!(blockstore.get_slots_since(&[0]).unwrap().is_empty());

            let mut meta0 = SlotMeta::new(0, 0);
            blockstore.meta_cf.put(0, &meta0).unwrap();

            // Slot exists, chains to nothing
            let expected: HashMap<u64, Vec<u64>> = vec![(0, vec![])].into_iter().collect();
            assert_eq!(blockstore.get_slots_since(&[0]).unwrap(), expected);
            meta0.next_slots = vec![1, 2];
            blockstore.meta_cf.put(0, &meta0).unwrap();

            // Slot exists, chains to some other slots
            let expected: HashMap<u64, Vec<u64>> = vec![(0, vec![1, 2])].into_iter().collect();
            assert_eq!(blockstore.get_slots_since(&[0]).unwrap(), expected);
            assert_eq!(blockstore.get_slots_since(&[0, 1]).unwrap(), expected);

            let mut meta3 = SlotMeta::new(3, 1);
            meta3.next_slots = vec![10, 5];
            blockstore.meta_cf.put(3, &meta3).unwrap();
            let expected: HashMap<u64, Vec<u64>> = vec![(0, vec![1, 2]), (3, vec![10, 5])]
                .into_iter()
                .collect();
            assert_eq!(blockstore.get_slots_since(&[0, 1, 3]).unwrap(), expected);
        }

        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_orphans() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            // Create shreds and entries
            let entries_per_slot = 1;
            let (mut shreds, _) = make_many_slot_entries(0, 3, entries_per_slot);
            let shreds_per_slot = shreds.len() / 3;

            // Write slot 2, which chains to slot 1. We're missing slot 0,
            // so slot 1 is the orphan
            let shreds_for_slot = shreds.drain((shreds_per_slot * 2)..).collect_vec();
            blockstore
                .insert_shreds(shreds_for_slot, None, false)
                .unwrap();
            let meta = blockstore
                .meta(1)
                .expect("Expect database get to succeed")
                .unwrap();
            assert!(is_orphan(&meta));
            assert_eq!(
                blockstore.orphans_iterator(0).unwrap().collect::<Vec<_>>(),
                vec![1]
            );

            // Write slot 1 which chains to slot 0, so now slot 0 is the
            // orphan, and slot 1 is no longer the orphan.
            let shreds_for_slot = shreds.drain(shreds_per_slot..).collect_vec();
            blockstore
                .insert_shreds(shreds_for_slot, None, false)
                .unwrap();
            let meta = blockstore
                .meta(1)
                .expect("Expect database get to succeed")
                .unwrap();
            assert!(!is_orphan(&meta));
            let meta = blockstore
                .meta(0)
                .expect("Expect database get to succeed")
                .unwrap();
            assert!(is_orphan(&meta));
            assert_eq!(
                blockstore.orphans_iterator(0).unwrap().collect::<Vec<_>>(),
                vec![0]
            );

            // Write some slot that also chains to existing slots and orphan,
            // nothing should change
            let (shred4, _) = make_slot_entries(4, 0, 1);
            let (shred5, _) = make_slot_entries(5, 1, 1);
            blockstore.insert_shreds(shred4, None, false).unwrap();
            blockstore.insert_shreds(shred5, None, false).unwrap();
            assert_eq!(
                blockstore.orphans_iterator(0).unwrap().collect::<Vec<_>>(),
                vec![0]
            );

            // Write zeroth slot, no more orphans
            blockstore.insert_shreds(shreds, None, false).unwrap();
            for i in 0..3 {
                let meta = blockstore
                    .meta(i)
                    .expect("Expect database get to succeed")
                    .unwrap();
                assert!(!is_orphan(&meta));
            }
            // Orphans cf is empty
            assert!(blockstore.orphans_cf.is_empty().unwrap())
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    fn test_insert_data_shreds_slots(name: &str, should_bulk_write: bool) {
        let blockstore_path = get_ledger_path_from_name(name);
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            // Create shreds and entries
            let num_entries = 20_u64;
            let mut entries = vec![];
            let mut shreds = vec![];
            let mut num_shreds_per_slot = 0;
            for slot in 0..num_entries {
                let parent_slot = {
                    if slot == 0 {
                        0
                    } else {
                        slot - 1
                    }
                };

                let (mut shred, entry) = make_slot_entries(slot, parent_slot, 1);
                num_shreds_per_slot = shred.len() as u64;
                shred
                    .iter_mut()
                    .enumerate()
                    .for_each(|(_, shred)| shred.set_index(0));
                shreds.extend(shred);
                entries.extend(entry);
            }

            let num_shreds = shreds.len();
            // Write shreds to the database
            if should_bulk_write {
                blockstore.insert_shreds(shreds, None, false).unwrap();
            } else {
                for _ in 0..num_shreds {
                    let shred = shreds.remove(0);
                    blockstore.insert_shreds(vec![shred], None, false).unwrap();
                }
            }

            for i in 0..num_entries - 1 {
                assert_eq!(
                    blockstore.get_slot_entries(i, 0).unwrap()[0],
                    entries[i as usize]
                );

                let meta = blockstore.meta(i).unwrap().unwrap();
                assert_eq!(meta.received, 1);
                assert_eq!(meta.last_index, 0);
                if i != 0 {
                    assert_eq!(meta.parent_slot, i - 1);
                    assert_eq!(meta.consumed, 1);
                } else {
                    assert_eq!(meta.parent_slot, 0);
                    assert_eq!(meta.consumed, num_shreds_per_slot);
                }
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_find_missing_data_indexes() {
        let slot = 0;
        let timestamp = 0;
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();

        // Write entries
        let gap: u64 = 10;
        assert!(gap > 3);
        // Create enough entries to ensure there are at least two shreds created
        let num_entries = max_ticks_per_n_shreds(1, None) + 1;
        let entries = create_ticks(num_entries, 0, Hash::default());
        let mut shreds = entries_to_test_shreds(entries, slot, 0, true, 0);
        let num_shreds = shreds.len();
        assert!(num_shreds > 1);
        for (i, s) in shreds.iter_mut().enumerate() {
            s.set_index(i as u32 * gap as u32);
            s.set_slot(slot);
        }
        blockstore.insert_shreds(shreds, None, false).unwrap();

        // Index of the first shred is 0
        // Index of the second shred is "gap"
        // Thus, the missing indexes should then be [1, gap - 1] for the input index
        // range of [0, gap)
        let expected: Vec<u64> = (1..gap).collect();
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, timestamp, 0, gap, gap as usize),
            expected
        );
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, timestamp, 1, gap, (gap - 1) as usize),
            expected,
        );
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, timestamp, 0, gap - 1, (gap - 1) as usize),
            &expected[..expected.len() - 1],
        );
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, timestamp, gap - 2, gap, gap as usize),
            vec![gap - 2, gap - 1],
        );
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, timestamp, gap - 2, gap, 1),
            vec![gap - 2],
        );
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, timestamp, 0, gap, 1),
            vec![1],
        );

        // Test with a range that encompasses a shred with index == gap which was
        // already inserted.
        let mut expected: Vec<u64> = (1..gap).collect();
        expected.push(gap + 1);
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, timestamp, 0, gap + 2, (gap + 2) as usize),
            expected,
        );
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, timestamp, 0, gap + 2, (gap - 1) as usize),
            &expected[..expected.len() - 1],
        );

        for i in 0..num_shreds as u64 {
            for j in 0..i {
                let expected: Vec<u64> = (j..i)
                    .flat_map(|k| {
                        let begin = k * gap + 1;
                        let end = (k + 1) * gap;
                        begin..end
                    })
                    .collect();
                assert_eq!(
                    blockstore.find_missing_data_indexes(
                        slot,
                        timestamp,
                        j * gap,
                        i * gap,
                        ((i - j) * gap) as usize
                    ),
                    expected,
                );
            }
        }

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_find_missing_data_indexes_timeout() {
        let slot = 0;
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();

        // Write entries
        let gap: u64 = 10;
        let shreds: Vec<_> = (0..64)
            .map(|i| {
                Shred::new_from_data(
                    slot,
                    (i * gap) as u32,
                    0,
                    None,
                    false,
                    false,
                    i as u8,
                    0,
                    (i * gap) as u32,
                )
            })
            .collect();
        blockstore.insert_shreds(shreds, None, false).unwrap();

        let empty: Vec<u64> = vec![];
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, timestamp(), 0, 50, 1),
            empty
        );
        let expected: Vec<_> = (1..=9).collect();
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, timestamp() - 400, 0, 50, 9),
            expected
        );

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_find_missing_data_indexes_sanity() {
        let slot = 0;

        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();

        // Early exit conditions
        let empty: Vec<u64> = vec![];
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, 0, 0, 0, 1),
            empty
        );
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, 0, 5, 5, 1),
            empty
        );
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, 0, 4, 3, 1),
            empty
        );
        assert_eq!(
            blockstore.find_missing_data_indexes(slot, 0, 1, 2, 0),
            empty
        );

        let entries = create_ticks(100, 0, Hash::default());
        let mut shreds = entries_to_test_shreds(entries, slot, 0, true, 0);
        assert!(shreds.len() > 2);
        shreds.drain(2..);

        const ONE: u64 = 1;
        const OTHER: u64 = 4;

        shreds[0].set_index(ONE as u32);
        shreds[1].set_index(OTHER as u32);

        // Insert one shred at index = first_index
        blockstore.insert_shreds(shreds, None, false).unwrap();

        const STARTS: u64 = OTHER * 2;
        const END: u64 = OTHER * 3;
        const MAX: usize = 10;
        // The first shred has index = first_index. Thus, for i < first_index,
        // given the input range of [i, first_index], the missing indexes should be
        // [i, first_index - 1]
        for start in 0..STARTS {
            let result = blockstore.find_missing_data_indexes(
                slot, 0, start, // start
                END,   //end
                MAX,   //max
            );
            let expected: Vec<u64> = (start..END).filter(|i| *i != ONE && *i != OTHER).collect();
            assert_eq!(result, expected);
        }

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_no_missing_shred_indexes() {
        let slot = 0;
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();

        // Write entries
        let num_entries = 10;
        let entries = create_ticks(num_entries, 0, Hash::default());
        let shreds = entries_to_test_shreds(entries, slot, 0, true, 0);
        let num_shreds = shreds.len();

        blockstore.insert_shreds(shreds, None, false).unwrap();

        let empty: Vec<u64> = vec![];
        for i in 0..num_shreds as u64 {
            for j in 0..i {
                assert_eq!(
                    blockstore.find_missing_data_indexes(slot, 0, j, i, (i - j) as usize),
                    empty
                );
            }
        }

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_should_insert_data_shred() {
        solana_logger::setup();
        let (mut shreds, _) = make_slot_entries(0, 0, 200);
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let last_root = RwLock::new(0);

            // Insert the first 5 shreds, we don't have a "is_last" shred yet
            blockstore
                .insert_shreds(shreds[0..5].to_vec(), None, false)
                .unwrap();

            let slot_meta = blockstore.meta(0).unwrap().unwrap();
            // Corrupt shred by making it too large
            let mut shred5 = shreds[5].clone();
            shred5.payload.push(10);
            shred5.data_header.size = shred5.payload.len() as u16;
            assert!(!blockstore.should_insert_data_shred(
                &shred5,
                &slot_meta,
                &HashMap::new(),
                &last_root,
                None,
                ShredSource::Turbine
            ));

            // Ensure that an empty shred (one with no data) would get inserted. Such shreds
            // may be used as signals (broadcast does so to indicate a slot was interrupted)
            // Reuse shred5's header values to avoid a false negative result
            let mut empty_shred = Shred::new_from_data(
                shred5.common_header.slot,
                shred5.common_header.index,
                shred5.data_header.parent_offset,
                None, // data
                true, // is_last_data
                true, // is_last_in_slot
                0,    // reference_tick
                shred5.common_header.version,
                shred5.common_header.fec_set_index,
            );
            assert!(blockstore.should_insert_data_shred(
                &empty_shred,
                &slot_meta,
                &HashMap::new(),
                &last_root,
                None,
                ShredSource::Repaired,
            ));
            empty_shred.data_header.size = 0;
            assert!(!blockstore.should_insert_data_shred(
                &empty_shred,
                &slot_meta,
                &HashMap::new(),
                &last_root,
                None,
                ShredSource::Recovered,
            ));

            // Trying to insert another "is_last" shred with index < the received index should fail
            // skip over shred 7
            blockstore
                .insert_shreds(shreds[8..9].to_vec(), None, false)
                .unwrap();
            let slot_meta = blockstore.meta(0).unwrap().unwrap();
            assert_eq!(slot_meta.received, 9);
            let shred7 = {
                if shreds[7].is_data() {
                    shreds[7].set_last_in_slot();
                    shreds[7].clone()
                } else {
                    panic!("Shred in unexpected format")
                }
            };
            assert!(!blockstore.should_insert_data_shred(
                &shred7,
                &slot_meta,
                &HashMap::new(),
                &last_root,
                None,
                ShredSource::Repaired,
            ));
            assert!(blockstore.has_duplicate_shreds_in_slot(0));

            // Insert all pending shreds
            let mut shred8 = shreds[8].clone();
            blockstore.insert_shreds(shreds, None, false).unwrap();
            let slot_meta = blockstore.meta(0).unwrap().unwrap();

            // Trying to insert a shred with index > the "is_last" shred should fail
            if shred8.is_data() {
                shred8.set_slot(slot_meta.last_index + 1);
            } else {
                panic!("Shred in unexpected format")
            }
            assert!(!blockstore.should_insert_data_shred(
                &shred7,
                &slot_meta,
                &HashMap::new(),
                &last_root,
                None,
                ShredSource::Repaired,
            ));
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_is_data_shred_present() {
        let (shreds, _) = make_slot_entries(0, 0, 200);
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let index_cf = blockstore.db.column::<cf::Index>();

            blockstore
                .insert_shreds(shreds[0..5].to_vec(), None, false)
                .unwrap();
            // Insert a shred less than `slot_meta.consumed`, check that
            // it already exists
            let slot_meta = blockstore.meta(0).unwrap().unwrap();
            let index = index_cf.get(0).unwrap().unwrap();
            assert_eq!(slot_meta.consumed, 5);
            assert!(Blockstore::is_data_shred_present(
                &shreds[1],
                &slot_meta,
                index.data(),
            ));

            // Insert a shred, check that it already exists
            blockstore
                .insert_shreds(shreds[6..7].to_vec(), None, false)
                .unwrap();
            let slot_meta = blockstore.meta(0).unwrap().unwrap();
            let index = index_cf.get(0).unwrap().unwrap();
            assert!(Blockstore::is_data_shred_present(
                &shreds[6],
                &slot_meta,
                index.data()
            ),);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_check_cache_coding_shred() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let slot = 1;
            let (shred, coding) = Shredder::new_coding_shred_header(slot, 11, 11, 11, 11, 0);
            let coding_shred =
                Shred::new_empty_from_header(shred, DataShredHeader::default(), coding);

            let mut erasure_metas = HashMap::new();
            let mut index_working_set = HashMap::new();
            let mut just_received_coding_shreds = HashMap::new();
            let mut index_meta_time = 0;
            assert!(blockstore.check_cache_coding_shred(
                coding_shred.clone(),
                &mut erasure_metas,
                &mut index_working_set,
                &mut just_received_coding_shreds,
                &mut index_meta_time,
                &|_shred| {
                    panic!("no dupes");
                },
                false,
                false,
            ));

            // insert again fails on dupe
            use std::sync::atomic::{AtomicUsize, Ordering};
            let counter = AtomicUsize::new(0);
            assert!(!blockstore.check_cache_coding_shred(
                coding_shred,
                &mut erasure_metas,
                &mut index_working_set,
                &mut just_received_coding_shreds,
                &mut index_meta_time,
                &|_shred| {
                    counter.fetch_add(1, Ordering::Relaxed);
                },
                false,
                false,
            ));
            assert_eq!(counter.load(Ordering::Relaxed), 1);
        }
    }

    #[test]
    pub fn test_should_insert_coding_shred() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let last_root = RwLock::new(0);

            let slot = 1;
            let (mut shred, coding) = Shredder::new_coding_shred_header(slot, 11, 11, 11, 11, 0);
            let coding_shred = Shred::new_empty_from_header(
                shred.clone(),
                DataShredHeader::default(),
                coding.clone(),
            );

            // Insert a good coding shred
            assert!(Blockstore::should_insert_coding_shred(
                &coding_shred,
                &last_root
            ));

            // Insertion should succeed
            blockstore
                .insert_shreds(vec![coding_shred.clone()], None, false)
                .unwrap();

            // Trying to insert the same shred again should pass since this doesn't check for
            // duplicate index
            {
                assert!(Blockstore::should_insert_coding_shred(
                    &coding_shred,
                    &last_root
                ));
            }

            shred.index += 1;

            // Establish a baseline that works
            {
                let coding_shred = Shred::new_empty_from_header(
                    shred.clone(),
                    DataShredHeader::default(),
                    coding.clone(),
                );
                assert!(Blockstore::should_insert_coding_shred(
                    &coding_shred,
                    &last_root
                ));
            }

            // Trying to insert a shred with index < position should fail
            {
                let mut coding_shred = Shred::new_empty_from_header(
                    shred.clone(),
                    DataShredHeader::default(),
                    coding.clone(),
                );
                let index = coding_shred.index() - coding_shred.common_header.fec_set_index - 1;
                coding_shred.set_index(index as u32);

                assert!(!Blockstore::should_insert_coding_shred(
                    &coding_shred,
                    &last_root
                ));
            }

            // Trying to insert shred with num_coding == 0 should fail
            {
                let mut coding_shred = Shred::new_empty_from_header(
                    shred.clone(),
                    DataShredHeader::default(),
                    coding.clone(),
                );
                coding_shred.coding_header.num_coding_shreds = 0;
                assert!(!Blockstore::should_insert_coding_shred(
                    &coding_shred,
                    &last_root
                ));
            }

            // Trying to insert shred with pos >= num_coding should fail
            {
                let mut coding_shred = Shred::new_empty_from_header(
                    shred.clone(),
                    DataShredHeader::default(),
                    coding.clone(),
                );
                let num_coding_shreds =
                    coding_shred.common_header.index - coding_shred.common_header.fec_set_index;
                coding_shred.coding_header.num_coding_shreds = num_coding_shreds as u16;
                assert!(!Blockstore::should_insert_coding_shred(
                    &coding_shred,
                    &last_root
                ));
            }

            // Trying to insert with set_index with num_coding that would imply the last shred
            // has index > u32::MAX should fail
            {
                let mut coding_shred = Shred::new_empty_from_header(
                    shred.clone(),
                    DataShredHeader::default(),
                    coding.clone(),
                );
                coding_shred.common_header.fec_set_index = std::u32::MAX - 1;
                coding_shred.coding_header.num_coding_shreds = 3;
                coding_shred.common_header.index = std::u32::MAX - 1;
                assert!(!Blockstore::should_insert_coding_shred(
                    &coding_shred,
                    &last_root
                ));

                coding_shred.coding_header.num_coding_shreds = 2000;
                assert!(!Blockstore::should_insert_coding_shred(
                    &coding_shred,
                    &last_root
                ));

                // Decreasing the number of num_coding_shreds will put it within the allowed limit
                coding_shred.coding_header.num_coding_shreds = 2;
                assert!(Blockstore::should_insert_coding_shred(
                    &coding_shred,
                    &last_root
                ));

                // Insertion should succeed
                blockstore
                    .insert_shreds(vec![coding_shred], None, false)
                    .unwrap();
            }

            // Trying to insert value into slot <= than last root should fail
            {
                let mut coding_shred =
                    Shred::new_empty_from_header(shred, DataShredHeader::default(), coding);
                coding_shred.set_slot(*last_root.read().unwrap());
                assert!(!Blockstore::should_insert_coding_shred(
                    &coding_shred,
                    &last_root
                ));
            }
        }

        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_insert_multiple_is_last() {
        solana_logger::setup();
        let (shreds, _) = make_slot_entries(0, 0, 20);
        let num_shreds = shreds.len() as u64;
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();

        blockstore.insert_shreds(shreds, None, false).unwrap();
        let slot_meta = blockstore.meta(0).unwrap().unwrap();

        assert_eq!(slot_meta.consumed, num_shreds);
        assert_eq!(slot_meta.received, num_shreds);
        assert_eq!(slot_meta.last_index, num_shreds - 1);
        assert!(slot_meta.is_full());

        let (shreds, _) = make_slot_entries(0, 0, 22);
        blockstore.insert_shreds(shreds, None, false).unwrap();
        let slot_meta = blockstore.meta(0).unwrap().unwrap();

        assert_eq!(slot_meta.consumed, num_shreds);
        assert_eq!(slot_meta.received, num_shreds);
        assert_eq!(slot_meta.last_index, num_shreds - 1);
        assert!(slot_meta.is_full());

        assert!(blockstore.has_duplicate_shreds_in_slot(0));

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_get_data_shreds_for_slot() {
        // Construct the shreds
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();
        let shreds_per_slot = 10;
        let slots = vec![2, 4, 8, 12];
        let all_shreds = make_chaining_slot_entries(&slots, shreds_per_slot);
        let slot_8_shreds = all_shreds[2].0.clone();
        for (slot_shreds, _) in all_shreds {
            blockstore.insert_shreds(slot_shreds, None, false).unwrap();
        }

        // Slot doesnt exist, shouldn't get any shreds back
        let result = blockstore.get_data_shreds_for_slot(5, 0).unwrap();
        assert_eq!(result, vec![]);

        // Slot does exist, test that we get back what was inserted earlier
        let result = blockstore.get_data_shreds_for_slot(8, 0).unwrap();
        assert_eq!(result.len(), slot_8_shreds.len());
        assert_eq!(result, slot_8_shreds);

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_set_roots() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();
        let chained_slots = vec![0, 2, 4, 7, 12, 15];
        assert_eq!(blockstore.last_root(), 0);

        blockstore.set_roots(chained_slots.iter()).unwrap();

        assert_eq!(blockstore.last_root(), 15);

        for i in chained_slots {
            assert!(blockstore.is_root(i));
        }

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_is_skipped() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();
        let roots = vec![2, 4, 7, 12, 15];
        blockstore.set_roots(roots.iter()).unwrap();

        for i in 0..20 {
            if i < 2 || roots.contains(&i) || i > 15 {
                assert!(!blockstore.is_skipped(i));
            } else {
                assert!(blockstore.is_skipped(i));
            }
        }

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_iter_bounds() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();

        // slot 5 does not exist, iter should be ok and should be a noop
        blockstore
            .slot_meta_iterator(5)
            .unwrap()
            .for_each(|_| panic!());

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_get_completed_data_ranges() {
        let completed_data_end_indexes = vec![2, 4, 9, 11];

        // Consumed is 1, which means we're missing shred with index 1, should return empty
        let start_index = 0;
        let consumed = 1;
        assert_eq!(
            Blockstore::get_completed_data_ranges(
                start_index,
                &completed_data_end_indexes[..],
                consumed
            ),
            vec![]
        );

        let start_index = 0;
        let consumed = 3;
        assert_eq!(
            Blockstore::get_completed_data_ranges(
                start_index,
                &completed_data_end_indexes[..],
                consumed
            ),
            vec![(0, 2)]
        );

        // Test all possible ranges:
        //
        // `consumed == completed_data_end_indexes[j] + 1`, means we have all the shreds up to index
        // `completed_data_end_indexes[j] + 1`. Thus the completed data blocks is everything in the
        // range:
        // [start_index, completed_data_end_indexes[j]] ==
        // [completed_data_end_indexes[i], completed_data_end_indexes[j]],
        for i in 0..completed_data_end_indexes.len() {
            for j in i..completed_data_end_indexes.len() {
                let start_index = completed_data_end_indexes[i];
                let consumed = completed_data_end_indexes[j] + 1;
                // When start_index == completed_data_end_indexes[i], then that means
                // the shred with index == start_index is a single-shred data block,
                // so the start index is the end index for that data block.
                let mut expected = vec![(start_index, start_index)];
                expected.extend(
                    completed_data_end_indexes[i..=j]
                        .windows(2)
                        .map(|end_indexes| (end_indexes[0] + 1, end_indexes[1])),
                );

                assert_eq!(
                    Blockstore::get_completed_data_ranges(
                        start_index,
                        &completed_data_end_indexes[..],
                        consumed
                    ),
                    expected
                );
            }
        }
    }

    #[test]
    fn test_get_slot_entries_with_shred_count_corruption() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let num_ticks = 8;
            let entries = create_ticks(num_ticks, 0, Hash::default());
            let slot = 1;
            let shreds = entries_to_test_shreds(entries, slot, 0, false, 0);
            let next_shred_index = shreds.len();
            blockstore
                .insert_shreds(shreds, None, false)
                .expect("Expected successful write of shreds");
            assert_eq!(
                blockstore.get_slot_entries(slot, 0).unwrap().len() as u64,
                num_ticks
            );

            // Insert an empty shred that won't deshred into entries
            let shreds = vec![Shred::new_from_data(
                slot,
                next_shred_index as u32,
                1,
                Some(&[1, 1, 1]),
                true,
                true,
                0,
                0,
                next_shred_index as u32,
            )];

            // With the corruption, nothing should be returned, even though an
            // earlier data block was valid
            blockstore
                .insert_shreds(shreds, None, false)
                .expect("Expected successful write of shreds");
            assert!(blockstore.get_slot_entries(slot, 0).is_err());
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_no_insert_but_modify_slot_meta() {
        // This tests correctness of the SlotMeta in various cases in which a shred
        // that gets filtered out by checks
        let (shreds0, _) = make_slot_entries(0, 0, 200);
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            // Insert the first 5 shreds, we don't have a "is_last" shred yet
            blockstore
                .insert_shreds(shreds0[0..5].to_vec(), None, false)
                .unwrap();

            // Insert a repetitive shred for slot 's', should get ignored, but also
            // insert shreds that chains to 's', should see the update in the SlotMeta
            // for 's'.
            let (mut shreds2, _) = make_slot_entries(2, 0, 200);
            let (mut shreds3, _) = make_slot_entries(3, 0, 200);
            shreds2.push(shreds0[1].clone());
            shreds3.insert(0, shreds0[1].clone());
            blockstore.insert_shreds(shreds2, None, false).unwrap();
            let slot_meta = blockstore.meta(0).unwrap().unwrap();
            assert_eq!(slot_meta.next_slots, vec![2]);
            blockstore.insert_shreds(shreds3, None, false).unwrap();
            let slot_meta = blockstore.meta(0).unwrap().unwrap();
            assert_eq!(slot_meta.next_slots, vec![2, 3]);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_trusted_insert_shreds() {
        // Make shred for slot 1
        let (shreds1, _) = make_slot_entries(1, 0, 1);
        let blockstore_path = get_tmp_ledger_path!();
        let last_root = 100;
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            blockstore.set_roots(std::iter::once(&last_root)).unwrap();

            // Insert will fail, slot < root
            blockstore
                .insert_shreds(shreds1[..].to_vec(), None, false)
                .unwrap();
            assert!(blockstore.get_data_shred(1, 0).unwrap().is_none());

            // Insert through trusted path will succeed
            blockstore
                .insert_shreds(shreds1[..].to_vec(), None, true)
                .unwrap();
            assert!(blockstore.get_data_shred(1, 0).unwrap().is_some());
        }
    }

    #[test]
    fn test_get_rooted_block() {
        let slot = 10;
        let entries = make_slot_entries_with_transactions(100);
        let blockhash = get_last_hash(entries.iter()).unwrap();
        let shreds = entries_to_test_shreds(entries.clone(), slot, slot - 1, true, 0);
        let more_shreds = entries_to_test_shreds(entries.clone(), slot + 1, slot, true, 0);
        let unrooted_shreds = entries_to_test_shreds(entries.clone(), slot + 2, slot + 1, true, 0);
        let ledger_path = get_tmp_ledger_path!();
        let ledger = Blockstore::open(&ledger_path).unwrap();
        ledger.insert_shreds(shreds, None, false).unwrap();
        ledger.insert_shreds(more_shreds, None, false).unwrap();
        ledger.insert_shreds(unrooted_shreds, None, false).unwrap();
        ledger
            .set_roots(vec![slot - 1, slot, slot + 1].iter())
            .unwrap();

        let parent_meta = SlotMeta {
            parent_slot: std::u64::MAX,
            ..SlotMeta::default()
        };
        ledger
            .put_meta_bytes(slot - 1, &serialize(&parent_meta).unwrap())
            .unwrap();

        let expected_transactions: Vec<TransactionWithStatusMeta> = entries
            .iter()
            .cloned()
            .filter(|entry| !entry.is_tick())
            .flat_map(|entry| entry.transactions)
            .map(|transaction| {
                let mut pre_balances: Vec<u64> = vec![];
                let mut post_balances: Vec<u64> = vec![];
                for (i, _account_key) in transaction.message.account_keys.iter().enumerate() {
                    pre_balances.push(i as u64 * 10);
                    post_balances.push(i as u64 * 11);
                }
                let signature = transaction.signatures[0];
                let status = TransactionStatusMeta {
                    status: Ok(()),
                    fee: 42,
                    pre_balances: pre_balances.clone(),
                    post_balances: post_balances.clone(),
                    inner_instructions: Some(vec![]),
                    log_messages: Some(vec![]),
                    pre_token_balances: Some(vec![]),
                    post_token_balances: Some(vec![]),
                    rewards: Some(vec![]),
                }
                .into();
                ledger
                    .transaction_status_cf
                    .put_protobuf((0, signature, slot), &status)
                    .unwrap();
                let status = TransactionStatusMeta {
                    status: Ok(()),
                    fee: 42,
                    pre_balances: pre_balances.clone(),
                    post_balances: post_balances.clone(),
                    inner_instructions: Some(vec![]),
                    log_messages: Some(vec![]),
                    pre_token_balances: Some(vec![]),
                    post_token_balances: Some(vec![]),
                    rewards: Some(vec![]),
                }
                .into();
                ledger
                    .transaction_status_cf
                    .put_protobuf((0, signature, slot + 1), &status)
                    .unwrap();
                let status = TransactionStatusMeta {
                    status: Ok(()),
                    fee: 42,
                    pre_balances: pre_balances.clone(),
                    post_balances: post_balances.clone(),
                    inner_instructions: Some(vec![]),
                    log_messages: Some(vec![]),
                    pre_token_balances: Some(vec![]),
                    post_token_balances: Some(vec![]),
                    rewards: Some(vec![]),
                }
                .into();
                ledger
                    .transaction_status_cf
                    .put_protobuf((0, signature, slot + 2), &status)
                    .unwrap();
                TransactionWithStatusMeta {
                    transaction,
                    meta: Some(TransactionStatusMeta {
                        status: Ok(()),
                        fee: 42,
                        pre_balances,
                        post_balances,
                        inner_instructions: Some(vec![]),
                        log_messages: Some(vec![]),
                        pre_token_balances: Some(vec![]),
                        post_token_balances: Some(vec![]),
                        rewards: Some(vec![]),
                    }),
                }
            })
            .collect();

        // Even if marked as root, a slot that is empty of entries should return an error
        let confirmed_block_err = ledger.get_rooted_block(slot - 1, true).unwrap_err();
        assert_matches!(confirmed_block_err, BlockstoreError::SlotUnavailable);

        // The previous_blockhash of `expected_block` is default because its parent slot is a root,
        // but empty of entries (eg. snapshot root slots). This now returns an error.
        let confirmed_block_err = ledger.get_rooted_block(slot, true).unwrap_err();
        assert_matches!(
            confirmed_block_err,
            BlockstoreError::ParentEntriesUnavailable
        );

        // Test if require_previous_blockhash is false
        let confirmed_block = ledger.get_rooted_block(slot, false).unwrap();
        assert_eq!(confirmed_block.transactions.len(), 100);
        let expected_block = ConfirmedBlock {
            transactions: expected_transactions.clone(),
            parent_slot: slot - 1,
            blockhash: blockhash.to_string(),
            previous_blockhash: Hash::default().to_string(),
            rewards: vec![],
            block_time: None,
            block_height: None,
        };
        assert_eq!(confirmed_block, expected_block);

        let confirmed_block = ledger.get_rooted_block(slot + 1, true).unwrap();
        assert_eq!(confirmed_block.transactions.len(), 100);

        let mut expected_block = ConfirmedBlock {
            transactions: expected_transactions.clone(),
            parent_slot: slot,
            blockhash: blockhash.to_string(),
            previous_blockhash: blockhash.to_string(),
            rewards: vec![],
            block_time: None,
            block_height: None,
        };
        assert_eq!(confirmed_block, expected_block);

        let not_root = ledger.get_rooted_block(slot + 2, true).unwrap_err();
        assert_matches!(not_root, BlockstoreError::SlotNotRooted);

        let complete_block = ledger.get_complete_block(slot + 2, true).unwrap();
        assert_eq!(complete_block.transactions.len(), 100);

        let mut expected_complete_block = ConfirmedBlock {
            transactions: expected_transactions,
            parent_slot: slot + 1,
            blockhash: blockhash.to_string(),
            previous_blockhash: blockhash.to_string(),
            rewards: vec![],
            block_time: None,
            block_height: None,
        };
        assert_eq!(complete_block, expected_complete_block);

        // Test block_time & block_height return, if available
        let timestamp = 1_576_183_541;
        ledger.blocktime_cf.put(slot + 1, &timestamp).unwrap();
        expected_block.block_time = Some(timestamp);
        let block_height = slot - 2;
        ledger.block_height_cf.put(slot + 1, &block_height).unwrap();
        expected_block.block_height = Some(block_height);

        let confirmed_block = ledger.get_rooted_block(slot + 1, true).unwrap();
        assert_eq!(confirmed_block, expected_block);

        let timestamp = 1_576_183_542;
        ledger.blocktime_cf.put(slot + 2, &timestamp).unwrap();
        expected_complete_block.block_time = Some(timestamp);
        let block_height = slot - 1;
        ledger.block_height_cf.put(slot + 2, &block_height).unwrap();
        expected_complete_block.block_height = Some(block_height);

        let complete_block = ledger.get_complete_block(slot + 2, true).unwrap();
        assert_eq!(complete_block, expected_complete_block);

        drop(ledger);
        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_persist_transaction_status() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let transaction_status_cf = blockstore.db.column::<cf::TransactionStatus>();

            let pre_balances_vec = vec![1, 2, 3];
            let post_balances_vec = vec![3, 2, 1];
            let inner_instructions_vec = vec![InnerInstructions {
                index: 0,
                instructions: vec![CompiledInstruction::new(1, &(), vec![0])],
            }];
            let log_messages_vec = vec![String::from("Test message\n")];
            let pre_token_balances_vec = vec![];
            let post_token_balances_vec = vec![];
            let rewards_vec = vec![];

            // result not found
            assert!(transaction_status_cf
                .get_protobuf_or_bincode::<StoredTransactionStatusMeta>((
                    0,
                    Signature::default(),
                    0
                ))
                .unwrap()
                .is_none());

            // insert value
            let status = TransactionStatusMeta {
                status: solana_sdk::transaction::Result::<()>::Err(
                    TransactionError::AccountNotFound,
                ),
                fee: 5u64,
                pre_balances: pre_balances_vec.clone(),
                post_balances: post_balances_vec.clone(),
                inner_instructions: Some(inner_instructions_vec.clone()),
                log_messages: Some(log_messages_vec.clone()),
                pre_token_balances: Some(pre_token_balances_vec.clone()),
                post_token_balances: Some(post_token_balances_vec.clone()),
                rewards: Some(rewards_vec.clone()),
            }
            .into();
            assert!(transaction_status_cf
                .put_protobuf((0, Signature::default(), 0), &status,)
                .is_ok());

            // result found
            let TransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
            } = transaction_status_cf
                .get_protobuf_or_bincode::<StoredTransactionStatusMeta>((
                    0,
                    Signature::default(),
                    0,
                ))
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap();
            assert_eq!(status, Err(TransactionError::AccountNotFound));
            assert_eq!(fee, 5u64);
            assert_eq!(pre_balances, pre_balances_vec);
            assert_eq!(post_balances, post_balances_vec);
            assert_eq!(inner_instructions.unwrap(), inner_instructions_vec);
            assert_eq!(log_messages.unwrap(), log_messages_vec);
            assert_eq!(pre_token_balances.unwrap(), pre_token_balances_vec);
            assert_eq!(post_token_balances.unwrap(), post_token_balances_vec);
            assert_eq!(rewards.unwrap(), rewards_vec);

            // insert value
            let status = TransactionStatusMeta {
                status: solana_sdk::transaction::Result::<()>::Ok(()),
                fee: 9u64,
                pre_balances: pre_balances_vec.clone(),
                post_balances: post_balances_vec.clone(),
                inner_instructions: Some(inner_instructions_vec.clone()),
                log_messages: Some(log_messages_vec.clone()),
                pre_token_balances: Some(pre_token_balances_vec.clone()),
                post_token_balances: Some(post_token_balances_vec.clone()),
                rewards: Some(rewards_vec.clone()),
            }
            .into();
            assert!(transaction_status_cf
                .put_protobuf((0, Signature::new(&[2u8; 64]), 9), &status,)
                .is_ok());

            // result found
            let TransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
            } = transaction_status_cf
                .get_protobuf_or_bincode::<StoredTransactionStatusMeta>((
                    0,
                    Signature::new(&[2u8; 64]),
                    9,
                ))
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap();

            // deserialize
            assert_eq!(status, Ok(()));
            assert_eq!(fee, 9u64);
            assert_eq!(pre_balances, pre_balances_vec);
            assert_eq!(post_balances, post_balances_vec);
            assert_eq!(inner_instructions.unwrap(), inner_instructions_vec);
            assert_eq!(log_messages.unwrap(), log_messages_vec);
            assert_eq!(pre_token_balances.unwrap(), pre_token_balances_vec);
            assert_eq!(post_token_balances.unwrap(), post_token_balances_vec);
            assert_eq!(rewards.unwrap(), rewards_vec);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_transaction_status_index() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let transaction_status_index_cf = blockstore.db.column::<cf::TransactionStatusIndex>();
            let slot0 = 10;

            // Primary index column is initialized on Blockstore::open
            assert!(transaction_status_index_cf.get(0).unwrap().is_some());
            assert!(transaction_status_index_cf.get(1).unwrap().is_some());

            for _ in 0..5 {
                let random_bytes: Vec<u8> = (0..64).map(|_| rand::random::<u8>()).collect();
                blockstore
                    .write_transaction_status(
                        slot0,
                        Signature::new(&random_bytes),
                        vec![&Pubkey::new(&random_bytes[0..32])],
                        vec![&Pubkey::new(&random_bytes[32..])],
                        TransactionStatusMeta::default(),
                    )
                    .unwrap();
            }

            // New statuses bump index 0 max_slot
            assert_eq!(
                transaction_status_index_cf.get(0).unwrap().unwrap(),
                TransactionStatusIndexMeta {
                    max_slot: slot0,
                    frozen: false,
                }
            );
            assert_eq!(
                transaction_status_index_cf.get(1).unwrap().unwrap(),
                TransactionStatusIndexMeta::default()
            );

            let first_status_entry = blockstore
                .db
                .iter::<cf::TransactionStatus>(IteratorMode::From(
                    cf::TransactionStatus::as_index(0),
                    IteratorDirection::Forward,
                ))
                .unwrap()
                .next()
                .unwrap()
                .0;
            assert_eq!(first_status_entry.0, 0);
            assert_eq!(first_status_entry.2, slot0);
            let first_address_entry = blockstore
                .db
                .iter::<cf::AddressSignatures>(IteratorMode::From(
                    cf::AddressSignatures::as_index(0),
                    IteratorDirection::Forward,
                ))
                .unwrap()
                .next()
                .unwrap()
                .0;
            assert_eq!(first_address_entry.0, 0);
            assert_eq!(first_address_entry.2, slot0);

            blockstore.run_purge(0, 8, PurgeType::PrimaryIndex).unwrap();
            // First successful prune freezes index 0
            assert_eq!(
                transaction_status_index_cf.get(0).unwrap().unwrap(),
                TransactionStatusIndexMeta {
                    max_slot: slot0,
                    frozen: true,
                }
            );
            assert_eq!(
                transaction_status_index_cf.get(1).unwrap().unwrap(),
                TransactionStatusIndexMeta::default()
            );

            let slot1 = 20;
            for _ in 0..5 {
                let random_bytes: Vec<u8> = (0..64).map(|_| rand::random::<u8>()).collect();
                blockstore
                    .write_transaction_status(
                        slot1,
                        Signature::new(&random_bytes),
                        vec![&Pubkey::new(&random_bytes[0..32])],
                        vec![&Pubkey::new(&random_bytes[32..])],
                        TransactionStatusMeta::default(),
                    )
                    .unwrap();
            }

            assert_eq!(
                transaction_status_index_cf.get(0).unwrap().unwrap(),
                TransactionStatusIndexMeta {
                    max_slot: slot0,
                    frozen: true,
                }
            );
            // Index 0 is frozen, so new statuses bump index 1 max_slot
            assert_eq!(
                transaction_status_index_cf.get(1).unwrap().unwrap(),
                TransactionStatusIndexMeta {
                    max_slot: slot1,
                    frozen: false,
                }
            );

            // Index 0 statuses and address records still exist
            let first_status_entry = blockstore
                .db
                .iter::<cf::TransactionStatus>(IteratorMode::From(
                    cf::TransactionStatus::as_index(0),
                    IteratorDirection::Forward,
                ))
                .unwrap()
                .next()
                .unwrap()
                .0;
            assert_eq!(first_status_entry.0, 0);
            assert_eq!(first_status_entry.2, 10);
            let first_address_entry = blockstore
                .db
                .iter::<cf::AddressSignatures>(IteratorMode::From(
                    cf::AddressSignatures::as_index(0),
                    IteratorDirection::Forward,
                ))
                .unwrap()
                .next()
                .unwrap()
                .0;
            assert_eq!(first_address_entry.0, 0);
            assert_eq!(first_address_entry.2, slot0);
            // New statuses and address records are stored in index 1
            let index1_first_status_entry = blockstore
                .db
                .iter::<cf::TransactionStatus>(IteratorMode::From(
                    cf::TransactionStatus::as_index(1),
                    IteratorDirection::Forward,
                ))
                .unwrap()
                .next()
                .unwrap()
                .0;
            assert_eq!(index1_first_status_entry.0, 1);
            assert_eq!(index1_first_status_entry.2, slot1);
            let index1_first_address_entry = blockstore
                .db
                .iter::<cf::AddressSignatures>(IteratorMode::From(
                    cf::AddressSignatures::as_index(1),
                    IteratorDirection::Forward,
                ))
                .unwrap()
                .next()
                .unwrap()
                .0;
            assert_eq!(index1_first_address_entry.0, 1);
            assert_eq!(index1_first_address_entry.2, slot1);

            blockstore
                .run_purge(0, 18, PurgeType::PrimaryIndex)
                .unwrap();
            // Successful prune toggles TransactionStatusIndex
            assert_eq!(
                transaction_status_index_cf.get(0).unwrap().unwrap(),
                TransactionStatusIndexMeta {
                    max_slot: 0,
                    frozen: false,
                }
            );
            assert_eq!(
                transaction_status_index_cf.get(1).unwrap().unwrap(),
                TransactionStatusIndexMeta {
                    max_slot: slot1,
                    frozen: true,
                }
            );

            // Index 0 has been pruned, so first status and address entries are now index 1
            let first_status_entry = blockstore
                .db
                .iter::<cf::TransactionStatus>(IteratorMode::From(
                    cf::TransactionStatus::as_index(0),
                    IteratorDirection::Forward,
                ))
                .unwrap()
                .next()
                .unwrap()
                .0;
            assert_eq!(first_status_entry.0, 1);
            assert_eq!(first_status_entry.2, slot1);
            let first_address_entry = blockstore
                .db
                .iter::<cf::AddressSignatures>(IteratorMode::From(
                    cf::AddressSignatures::as_index(0),
                    IteratorDirection::Forward,
                ))
                .unwrap()
                .next()
                .unwrap()
                .0;
            assert_eq!(first_address_entry.0, 1);
            assert_eq!(first_address_entry.2, slot1);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_get_transaction_status() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            // TransactionStatus column opens initialized with one entry at index 2
            let transaction_status_cf = blockstore.db.column::<cf::TransactionStatus>();

            let pre_balances_vec = vec![1, 2, 3];
            let post_balances_vec = vec![3, 2, 1];
            let status = TransactionStatusMeta {
                status: solana_sdk::transaction::Result::<()>::Ok(()),
                fee: 42u64,
                pre_balances: pre_balances_vec,
                post_balances: post_balances_vec,
                inner_instructions: Some(vec![]),
                log_messages: Some(vec![]),
                pre_token_balances: Some(vec![]),
                post_token_balances: Some(vec![]),
                rewards: Some(vec![]),
            }
            .into();

            let signature1 = Signature::new(&[1u8; 64]);
            let signature2 = Signature::new(&[2u8; 64]);
            let signature3 = Signature::new(&[3u8; 64]);
            let signature4 = Signature::new(&[4u8; 64]);
            let signature5 = Signature::new(&[5u8; 64]);
            let signature6 = Signature::new(&[6u8; 64]);
            let signature7 = Signature::new(&[7u8; 64]);

            // Insert slots with fork
            //   0 (root)
            //  / \
            // 1  |
            //    2 (root)
            //    |
            //    3
            let meta0 = SlotMeta::new(0, 0);
            blockstore.meta_cf.put(0, &meta0).unwrap();
            let meta1 = SlotMeta::new(1, 0);
            blockstore.meta_cf.put(1, &meta1).unwrap();
            let meta2 = SlotMeta::new(2, 0);
            blockstore.meta_cf.put(2, &meta2).unwrap();
            let meta3 = SlotMeta::new(3, 2);
            blockstore.meta_cf.put(3, &meta3).unwrap();

            blockstore.set_roots(vec![0, 2].iter()).unwrap();

            // Initialize index 0, including:
            //   signature2 in non-root and root,
            //   signature4 in non-root,
            //   signature5 in skipped slot and non-root,
            //   signature6 in skipped slot,
            transaction_status_cf
                .put_protobuf((0, signature2, 1), &status)
                .unwrap();

            transaction_status_cf
                .put_protobuf((0, signature2, 2), &status)
                .unwrap();

            transaction_status_cf
                .put_protobuf((0, signature4, 1), &status)
                .unwrap();

            transaction_status_cf
                .put_protobuf((0, signature5, 1), &status)
                .unwrap();

            transaction_status_cf
                .put_protobuf((0, signature5, 3), &status)
                .unwrap();

            transaction_status_cf
                .put_protobuf((0, signature6, 1), &status)
                .unwrap();

            // Initialize index 1, including:
            //   signature4 in root,
            //   signature6 in non-root,
            //   signature5 extra entries
            transaction_status_cf
                .put_protobuf((1, signature4, 2), &status)
                .unwrap();

            transaction_status_cf
                .put_protobuf((1, signature5, 4), &status)
                .unwrap();

            transaction_status_cf
                .put_protobuf((1, signature5, 5), &status)
                .unwrap();

            transaction_status_cf
                .put_protobuf((1, signature6, 3), &status)
                .unwrap();

            // Signature exists, root found in index 0
            if let (Some((slot, _status)), counter) = blockstore
                .get_transaction_status_with_counter(signature2, &[])
                .unwrap()
            {
                assert_eq!(slot, 2);
                assert_eq!(counter, 2);
            }

            // Signature exists, root found although not required
            if let (Some((slot, _status)), counter) = blockstore
                .get_transaction_status_with_counter(signature2, &[3])
                .unwrap()
            {
                assert_eq!(slot, 2);
                assert_eq!(counter, 2);
            }

            // Signature exists, root found in index 1
            if let (Some((slot, _status)), counter) = blockstore
                .get_transaction_status_with_counter(signature4, &[])
                .unwrap()
            {
                assert_eq!(slot, 2);
                assert_eq!(counter, 3);
            }

            // Signature exists, root found although not required, in index 1
            if let (Some((slot, _status)), counter) = blockstore
                .get_transaction_status_with_counter(signature4, &[3])
                .unwrap()
            {
                assert_eq!(slot, 2);
                assert_eq!(counter, 3);
            }

            // Signature exists, no root found
            let (status, counter) = blockstore
                .get_transaction_status_with_counter(signature5, &[])
                .unwrap();
            assert_eq!(status, None);
            assert_eq!(counter, 6);

            // Signature exists, root not required
            if let (Some((slot, _status)), counter) = blockstore
                .get_transaction_status_with_counter(signature5, &[3])
                .unwrap()
            {
                assert_eq!(slot, 3);
                assert_eq!(counter, 2);
            }

            // Signature does not exist, smaller than existing entries
            let (status, counter) = blockstore
                .get_transaction_status_with_counter(signature1, &[])
                .unwrap();
            assert_eq!(status, None);
            assert_eq!(counter, 2);

            let (status, counter) = blockstore
                .get_transaction_status_with_counter(signature1, &[3])
                .unwrap();
            assert_eq!(status, None);
            assert_eq!(counter, 2);

            // Signature does not exist, between existing entries
            let (status, counter) = blockstore
                .get_transaction_status_with_counter(signature3, &[])
                .unwrap();
            assert_eq!(status, None);
            assert_eq!(counter, 2);

            let (status, counter) = blockstore
                .get_transaction_status_with_counter(signature3, &[3])
                .unwrap();
            assert_eq!(status, None);
            assert_eq!(counter, 2);

            // Signature does not exist, larger than existing entries
            let (status, counter) = blockstore
                .get_transaction_status_with_counter(signature7, &[])
                .unwrap();
            assert_eq!(status, None);
            assert_eq!(counter, 2);

            let (status, counter) = blockstore
                .get_transaction_status_with_counter(signature7, &[3])
                .unwrap();
            assert_eq!(status, None);
            assert_eq!(counter, 2);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    fn do_test_lowest_cleanup_slot_and_special_cfs(
        simulate_compaction: bool,
        simulate_ledger_cleanup_service: bool,
    ) {
        solana_logger::setup();

        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            // TransactionStatus column opens initialized with one entry at index 2
            let transaction_status_cf = blockstore.db.column::<cf::TransactionStatus>();

            let pre_balances_vec = vec![1, 2, 3];
            let post_balances_vec = vec![3, 2, 1];
            let status = TransactionStatusMeta {
                status: solana_sdk::transaction::Result::<()>::Ok(()),
                fee: 42u64,
                pre_balances: pre_balances_vec,
                post_balances: post_balances_vec,
                inner_instructions: Some(vec![]),
                log_messages: Some(vec![]),
                pre_token_balances: Some(vec![]),
                post_token_balances: Some(vec![]),
                rewards: Some(vec![]),
            }
            .into();

            let signature1 = Signature::new(&[2u8; 64]);
            let signature2 = Signature::new(&[3u8; 64]);

            // Insert rooted slots 0..=3 with no fork
            let meta0 = SlotMeta::new(0, 0);
            blockstore.meta_cf.put(0, &meta0).unwrap();
            let meta1 = SlotMeta::new(1, 0);
            blockstore.meta_cf.put(1, &meta1).unwrap();
            let meta2 = SlotMeta::new(2, 1);
            blockstore.meta_cf.put(2, &meta2).unwrap();
            let meta3 = SlotMeta::new(3, 2);
            blockstore.meta_cf.put(3, &meta3).unwrap();

            blockstore.set_roots(vec![0, 1, 2, 3].iter()).unwrap();

            let lowest_cleanup_slot = 1;
            let lowest_available_slot = lowest_cleanup_slot + 1;

            transaction_status_cf
                .put_protobuf((0, signature1, lowest_cleanup_slot), &status)
                .unwrap();

            transaction_status_cf
                .put_protobuf((0, signature2, lowest_available_slot), &status)
                .unwrap();

            let address0 = solana_sdk::pubkey::new_rand();
            let address1 = solana_sdk::pubkey::new_rand();
            blockstore
                .write_transaction_status(
                    lowest_cleanup_slot,
                    signature1,
                    vec![&address0],
                    vec![],
                    TransactionStatusMeta::default(),
                )
                .unwrap();
            blockstore
                .write_transaction_status(
                    lowest_available_slot,
                    signature2,
                    vec![&address1],
                    vec![],
                    TransactionStatusMeta::default(),
                )
                .unwrap();

            let check_for_missing = || {
                (
                    blockstore
                        .get_transaction_status_with_counter(signature1, &[])
                        .unwrap()
                        .0
                        .is_none(),
                    blockstore
                        .find_address_signatures_for_slot(address0, lowest_cleanup_slot)
                        .unwrap()
                        .is_empty(),
                    blockstore
                        .find_address_signatures(address0, lowest_cleanup_slot, lowest_cleanup_slot)
                        .unwrap()
                        .is_empty(),
                )
            };

            let assert_existing_always = || {
                let are_existing_always = (
                    blockstore
                        .get_transaction_status_with_counter(signature2, &[])
                        .unwrap()
                        .0
                        .is_some(),
                    !blockstore
                        .find_address_signatures_for_slot(address1, lowest_available_slot)
                        .unwrap()
                        .is_empty(),
                    !blockstore
                        .find_address_signatures(
                            address1,
                            lowest_available_slot,
                            lowest_available_slot,
                        )
                        .unwrap()
                        .is_empty(),
                );
                assert_eq!(are_existing_always, (true, true, true));
            };

            let are_missing = check_for_missing();
            // should never be missing before the conditional compaction & simulation...
            assert_eq!(are_missing, (false, false, false));
            assert_existing_always();

            if simulate_compaction {
                blockstore.set_max_expired_slot(lowest_cleanup_slot);
                // force compaction filters to run across whole key range.
                blockstore
                    .compact_storage(Slot::min_value(), Slot::max_value())
                    .unwrap();
            }

            if simulate_ledger_cleanup_service {
                *blockstore.lowest_cleanup_slot.write().unwrap() = lowest_cleanup_slot;
            }

            let are_missing = check_for_missing();
            if simulate_compaction || simulate_ledger_cleanup_service {
                // ... when either simulation (or both) is effective, we should observe to be missing
                // consistently
                assert_eq!(are_missing, (true, true, true));
            } else {
                // ... otherwise, we should observe to be existing...
                assert_eq!(are_missing, (false, false, false));
            }
            assert_existing_always();
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_lowest_cleanup_slot_and_special_cfs_with_compact_with_ledger_cleanup_service_simulation(
    ) {
        do_test_lowest_cleanup_slot_and_special_cfs(true, true);
    }

    #[test]
    fn test_lowest_cleanup_slot_and_special_cfs_with_compact_without_ledger_cleanup_service_simulation(
    ) {
        do_test_lowest_cleanup_slot_and_special_cfs(true, false);
    }

    #[test]
    fn test_lowest_cleanup_slot_and_special_cfs_without_compact_with_ledger_cleanup_service_simulation(
    ) {
        do_test_lowest_cleanup_slot_and_special_cfs(false, true);
    }

    #[test]
    fn test_lowest_cleanup_slot_and_special_cfs_without_compact_without_ledger_cleanup_service_simulation(
    ) {
        do_test_lowest_cleanup_slot_and_special_cfs(false, false);
    }

    #[test]
    fn test_get_rooted_transaction() {
        let slot = 2;
        let entries = make_slot_entries_with_transactions(5);
        let shreds = entries_to_test_shreds(entries.clone(), slot, slot - 1, true, 0);
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&ledger_path).unwrap();
        blockstore.insert_shreds(shreds, None, false).unwrap();
        blockstore.set_roots(vec![slot - 1, slot].iter()).unwrap();

        let expected_transactions: Vec<TransactionWithStatusMeta> = entries
            .iter()
            .cloned()
            .filter(|entry| !entry.is_tick())
            .flat_map(|entry| entry.transactions)
            .map(|transaction| {
                let mut pre_balances: Vec<u64> = vec![];
                let mut post_balances: Vec<u64> = vec![];
                for (i, _account_key) in transaction.message.account_keys.iter().enumerate() {
                    pre_balances.push(i as u64 * 10);
                    post_balances.push(i as u64 * 11);
                }
                let inner_instructions = Some(vec![InnerInstructions {
                    index: 0,
                    instructions: vec![CompiledInstruction::new(1, &(), vec![0])],
                }]);
                let log_messages = Some(vec![String::from("Test message\n")]);
                let pre_token_balances = Some(vec![]);
                let post_token_balances = Some(vec![]);
                let rewards = Some(vec![]);
                let signature = transaction.signatures[0];
                let status = TransactionStatusMeta {
                    status: Ok(()),
                    fee: 42,
                    pre_balances: pre_balances.clone(),
                    post_balances: post_balances.clone(),
                    inner_instructions: inner_instructions.clone(),
                    log_messages: log_messages.clone(),
                    pre_token_balances: pre_token_balances.clone(),
                    post_token_balances: post_token_balances.clone(),
                    rewards: rewards.clone(),
                }
                .into();
                blockstore
                    .transaction_status_cf
                    .put_protobuf((0, signature, slot), &status)
                    .unwrap();
                TransactionWithStatusMeta {
                    transaction,
                    meta: Some(TransactionStatusMeta {
                        status: Ok(()),
                        fee: 42,
                        pre_balances,
                        post_balances,
                        inner_instructions,
                        log_messages,
                        pre_token_balances,
                        post_token_balances,
                        rewards,
                    }),
                }
            })
            .collect();

        for transaction in expected_transactions.clone() {
            let signature = transaction.transaction.signatures[0];
            assert_eq!(
                blockstore.get_rooted_transaction(signature).unwrap(),
                Some(ConfirmedTransaction {
                    slot,
                    transaction: transaction.clone(),
                    block_time: None
                })
            );
            assert_eq!(
                blockstore
                    .get_complete_transaction(signature, slot + 1)
                    .unwrap(),
                Some(ConfirmedTransaction {
                    slot,
                    transaction,
                    block_time: None
                })
            );
        }

        blockstore.run_purge(0, 2, PurgeType::PrimaryIndex).unwrap();
        *blockstore.lowest_cleanup_slot.write().unwrap() = slot;
        for TransactionWithStatusMeta { transaction, .. } in expected_transactions {
            let signature = transaction.signatures[0];
            assert_eq!(blockstore.get_rooted_transaction(signature).unwrap(), None,);
            assert_eq!(
                blockstore
                    .get_complete_transaction(signature, slot + 1)
                    .unwrap(),
                None,
            );
        }
    }

    #[test]
    fn test_get_complete_transaction() {
        let slot = 2;
        let entries = make_slot_entries_with_transactions(5);
        let shreds = entries_to_test_shreds(entries.clone(), slot, slot - 1, true, 0);
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&ledger_path).unwrap();
        blockstore.insert_shreds(shreds, None, false).unwrap();

        let expected_transactions: Vec<TransactionWithStatusMeta> = entries
            .iter()
            .cloned()
            .filter(|entry| !entry.is_tick())
            .flat_map(|entry| entry.transactions)
            .map(|transaction| {
                let mut pre_balances: Vec<u64> = vec![];
                let mut post_balances: Vec<u64> = vec![];
                for (i, _account_key) in transaction.message.account_keys.iter().enumerate() {
                    pre_balances.push(i as u64 * 10);
                    post_balances.push(i as u64 * 11);
                }
                let inner_instructions = Some(vec![InnerInstructions {
                    index: 0,
                    instructions: vec![CompiledInstruction::new(1, &(), vec![0])],
                }]);
                let log_messages = Some(vec![String::from("Test message\n")]);
                let pre_token_balances = Some(vec![]);
                let post_token_balances = Some(vec![]);
                let rewards = Some(vec![]);
                let signature = transaction.signatures[0];
                let status = TransactionStatusMeta {
                    status: Ok(()),
                    fee: 42,
                    pre_balances: pre_balances.clone(),
                    post_balances: post_balances.clone(),
                    inner_instructions: inner_instructions.clone(),
                    log_messages: log_messages.clone(),
                    pre_token_balances: pre_token_balances.clone(),
                    post_token_balances: post_token_balances.clone(),
                    rewards: rewards.clone(),
                }
                .into();
                blockstore
                    .transaction_status_cf
                    .put_protobuf((0, signature, slot), &status)
                    .unwrap();
                TransactionWithStatusMeta {
                    transaction,
                    meta: Some(TransactionStatusMeta {
                        status: Ok(()),
                        fee: 42,
                        pre_balances,
                        post_balances,
                        inner_instructions,
                        log_messages,
                        pre_token_balances,
                        post_token_balances,
                        rewards,
                    }),
                }
            })
            .collect();

        for transaction in expected_transactions.clone() {
            let signature = transaction.transaction.signatures[0];
            assert_eq!(
                blockstore
                    .get_complete_transaction(signature, slot)
                    .unwrap(),
                Some(ConfirmedTransaction {
                    slot,
                    transaction,
                    block_time: None
                })
            );
            assert_eq!(blockstore.get_rooted_transaction(signature).unwrap(), None);
        }

        blockstore.run_purge(0, 2, PurgeType::PrimaryIndex).unwrap();
        *blockstore.lowest_cleanup_slot.write().unwrap() = slot;
        for TransactionWithStatusMeta { transaction, .. } in expected_transactions {
            let signature = transaction.signatures[0];
            assert_eq!(
                blockstore
                    .get_complete_transaction(signature, slot)
                    .unwrap(),
                None,
            );
            assert_eq!(blockstore.get_rooted_transaction(signature).unwrap(), None,);
        }
    }

    #[test]
    fn test_empty_transaction_status() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();
        blockstore.set_roots(std::iter::once(&0)).unwrap();
        assert_eq!(
            blockstore
                .get_rooted_transaction(Signature::default())
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_get_confirmed_signatures_for_address() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let address0 = solana_sdk::pubkey::new_rand();
            let address1 = solana_sdk::pubkey::new_rand();

            let slot0 = 10;
            for x in 1..5 {
                let signature = Signature::new(&[x; 64]);
                blockstore
                    .write_transaction_status(
                        slot0,
                        signature,
                        vec![&address0],
                        vec![&address1],
                        TransactionStatusMeta::default(),
                    )
                    .unwrap();
            }
            // Purge to freeze index 0
            blockstore.run_purge(0, 1, PurgeType::PrimaryIndex).unwrap();
            let slot1 = 20;
            for x in 5..9 {
                let signature = Signature::new(&[x; 64]);
                blockstore
                    .write_transaction_status(
                        slot1,
                        signature,
                        vec![&address0],
                        vec![&address1],
                        TransactionStatusMeta::default(),
                    )
                    .unwrap();
            }
            blockstore.set_roots(vec![slot0, slot1].iter()).unwrap();

            let all0 = blockstore
                .get_confirmed_signatures_for_address(address0, 0, 50)
                .unwrap();
            assert_eq!(all0.len(), 8);
            for x in 1..9 {
                let expected_signature = Signature::new(&[x; 64]);
                assert_eq!(all0[x as usize - 1], expected_signature);
            }
            assert_eq!(
                blockstore
                    .get_confirmed_signatures_for_address(address0, 20, 50)
                    .unwrap()
                    .len(),
                4
            );
            assert_eq!(
                blockstore
                    .get_confirmed_signatures_for_address(address0, 0, 10)
                    .unwrap()
                    .len(),
                4
            );
            assert!(blockstore
                .get_confirmed_signatures_for_address(address0, 1, 5)
                .unwrap()
                .is_empty());
            assert_eq!(
                blockstore
                    .get_confirmed_signatures_for_address(address0, 1, 15)
                    .unwrap()
                    .len(),
                4
            );

            let all1 = blockstore
                .get_confirmed_signatures_for_address(address1, 0, 50)
                .unwrap();
            assert_eq!(all1.len(), 8);
            for x in 1..9 {
                let expected_signature = Signature::new(&[x; 64]);
                assert_eq!(all1[x as usize - 1], expected_signature);
            }

            // Purge index 0
            blockstore
                .run_purge(0, 10, PurgeType::PrimaryIndex)
                .unwrap();
            assert_eq!(
                blockstore
                    .get_confirmed_signatures_for_address(address0, 0, 50)
                    .unwrap()
                    .len(),
                4
            );
            assert_eq!(
                blockstore
                    .get_confirmed_signatures_for_address(address0, 20, 50)
                    .unwrap()
                    .len(),
                4
            );
            assert!(blockstore
                .get_confirmed_signatures_for_address(address0, 0, 10)
                .unwrap()
                .is_empty());
            assert!(blockstore
                .get_confirmed_signatures_for_address(address0, 1, 5)
                .unwrap()
                .is_empty());
            assert_eq!(
                blockstore
                    .get_confirmed_signatures_for_address(address0, 1, 25)
                    .unwrap()
                    .len(),
                4
            );

            // Test sort, regardless of entry order or signature value
            for slot in (21..25).rev() {
                let random_bytes: Vec<u8> = (0..64).map(|_| rand::random::<u8>()).collect();
                let signature = Signature::new(&random_bytes);
                blockstore
                    .write_transaction_status(
                        slot,
                        signature,
                        vec![&address0],
                        vec![&address1],
                        TransactionStatusMeta::default(),
                    )
                    .unwrap();
            }
            blockstore.set_roots(vec![21, 22, 23, 24].iter()).unwrap();
            let mut past_slot = 0;
            for (slot, _) in blockstore.find_address_signatures(address0, 1, 25).unwrap() {
                assert!(slot >= past_slot);
                past_slot = slot;
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_find_address_signatures_for_slot() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let address0 = solana_sdk::pubkey::new_rand();
            let address1 = solana_sdk::pubkey::new_rand();

            let slot1 = 1;
            for x in 1..5 {
                let signature = Signature::new(&[x; 64]);
                blockstore
                    .write_transaction_status(
                        slot1,
                        signature,
                        vec![&address0],
                        vec![&address1],
                        TransactionStatusMeta::default(),
                    )
                    .unwrap();
            }
            let slot2 = 2;
            for x in 5..7 {
                let signature = Signature::new(&[x; 64]);
                blockstore
                    .write_transaction_status(
                        slot2,
                        signature,
                        vec![&address0],
                        vec![&address1],
                        TransactionStatusMeta::default(),
                    )
                    .unwrap();
            }
            // Purge to freeze index 0
            blockstore.run_purge(0, 1, PurgeType::PrimaryIndex).unwrap();
            for x in 7..9 {
                let signature = Signature::new(&[x; 64]);
                blockstore
                    .write_transaction_status(
                        slot2,
                        signature,
                        vec![&address0],
                        vec![&address1],
                        TransactionStatusMeta::default(),
                    )
                    .unwrap();
            }
            let slot3 = 3;
            for x in 9..13 {
                let signature = Signature::new(&[x; 64]);
                blockstore
                    .write_transaction_status(
                        slot3,
                        signature,
                        vec![&address0],
                        vec![&address1],
                        TransactionStatusMeta::default(),
                    )
                    .unwrap();
            }
            blockstore.set_roots(std::iter::once(&slot1)).unwrap();

            let slot1_signatures = blockstore
                .find_address_signatures_for_slot(address0, 1)
                .unwrap();
            for (i, (slot, signature)) in slot1_signatures.iter().enumerate() {
                assert_eq!(*slot, slot1);
                assert_eq!(*signature, Signature::new(&[i as u8 + 1; 64]));
            }

            let slot2_signatures = blockstore
                .find_address_signatures_for_slot(address0, 2)
                .unwrap();
            for (i, (slot, signature)) in slot2_signatures.iter().enumerate() {
                assert_eq!(*slot, slot2);
                assert_eq!(*signature, Signature::new(&[i as u8 + 5; 64]));
            }

            let slot3_signatures = blockstore
                .find_address_signatures_for_slot(address0, 3)
                .unwrap();
            for (i, (slot, signature)) in slot3_signatures.iter().enumerate() {
                assert_eq!(*slot, slot3);
                assert_eq!(*signature, Signature::new(&[i as u8 + 9; 64]));
            }
        }
    }

    #[test]
    fn test_get_confirmed_signatures_for_address2() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            fn make_slot_entries_with_transaction_addresses(addresses: &[Pubkey]) -> Vec<Entry> {
                let mut entries: Vec<Entry> = Vec::new();
                for address in addresses {
                    let transaction = Transaction::new_with_compiled_instructions(
                        &[&Keypair::new()],
                        &[*address],
                        Hash::default(),
                        vec![solana_sdk::pubkey::new_rand()],
                        vec![CompiledInstruction::new(1, &(), vec![0])],
                    );
                    entries.push(next_entry_mut(&mut Hash::default(), 0, vec![transaction]));
                    let mut tick = create_ticks(1, 0, hash(&serialize(address).unwrap()));
                    entries.append(&mut tick);
                }
                entries
            }

            let address0 = solana_sdk::pubkey::new_rand();
            let address1 = solana_sdk::pubkey::new_rand();

            for slot in 2..=8 {
                let entries = make_slot_entries_with_transaction_addresses(&[
                    address0, address1, address0, address1,
                ]);
                let shreds = entries_to_test_shreds(entries.clone(), slot, slot - 1, true, 0);
                blockstore.insert_shreds(shreds, None, false).unwrap();

                for (i, entry) in entries.iter().enumerate() {
                    if slot == 4 && i == 2 {
                        // Purge to freeze index 0 and write address-signatures in new primary index
                        blockstore.run_purge(0, 1, PurgeType::PrimaryIndex).unwrap();
                    }
                    for transaction in &entry.transactions {
                        assert_eq!(transaction.signatures.len(), 1);
                        blockstore
                            .write_transaction_status(
                                slot,
                                transaction.signatures[0],
                                transaction.message.account_keys.iter().collect(),
                                vec![],
                                TransactionStatusMeta::default(),
                            )
                            .unwrap();
                    }
                }
            }

            // Add 2 slots that both descend from slot 8
            for slot in 9..=10 {
                let entries = make_slot_entries_with_transaction_addresses(&[
                    address0, address1, address0, address1,
                ]);
                let shreds = entries_to_test_shreds(entries.clone(), slot, 8, true, 0);
                blockstore.insert_shreds(shreds, None, false).unwrap();

                for entry in entries.iter() {
                    for transaction in &entry.transactions {
                        assert_eq!(transaction.signatures.len(), 1);
                        blockstore
                            .write_transaction_status(
                                slot,
                                transaction.signatures[0],
                                transaction.message.account_keys.iter().collect(),
                                vec![],
                                TransactionStatusMeta::default(),
                            )
                            .unwrap();
                    }
                }
            }

            // Leave one slot unrooted to test only returns confirmed signatures
            blockstore
                .set_roots(vec![1, 2, 4, 5, 6, 7, 8].iter())
                .unwrap();
            let highest_confirmed_root = 8;

            // Fetch all rooted signatures for address 0 at once...
            let all0 = blockstore
                .get_confirmed_signatures_for_address2(
                    address0,
                    highest_confirmed_root,
                    None,
                    None,
                    usize::MAX,
                )
                .unwrap();
            assert_eq!(all0.len(), 12);

            // Fetch all rooted signatures for address 1 at once...
            let all1 = blockstore
                .get_confirmed_signatures_for_address2(
                    address1,
                    highest_confirmed_root,
                    None,
                    None,
                    usize::MAX,
                )
                .unwrap();
            assert_eq!(all1.len(), 12);

            // Fetch all signatures for address 0 individually
            for i in 0..all0.len() {
                let results = blockstore
                    .get_confirmed_signatures_for_address2(
                        address0,
                        highest_confirmed_root,
                        if i == 0 {
                            None
                        } else {
                            Some(all0[i - 1].signature)
                        },
                        None,
                        1,
                    )
                    .unwrap();
                assert_eq!(results.len(), 1);
                assert_eq!(results[0], all0[i], "Unexpected result for {}", i);
            }
            // Fetch all signatures for address 0 individually using `until`
            for i in 0..all0.len() {
                let results = blockstore
                    .get_confirmed_signatures_for_address2(
                        address0,
                        highest_confirmed_root,
                        if i == 0 {
                            None
                        } else {
                            Some(all0[i - 1].signature)
                        },
                        if i == all0.len() - 1 || i == all0.len() {
                            None
                        } else {
                            Some(all0[i + 1].signature)
                        },
                        10,
                    )
                    .unwrap();
                assert_eq!(results.len(), 1);
                assert_eq!(results[0], all0[i], "Unexpected result for {}", i);
            }

            assert!(blockstore
                .get_confirmed_signatures_for_address2(
                    address0,
                    highest_confirmed_root,
                    Some(all0[all0.len() - 1].signature),
                    None,
                    1,
                )
                .unwrap()
                .is_empty());

            assert!(blockstore
                .get_confirmed_signatures_for_address2(
                    address0,
                    highest_confirmed_root,
                    None,
                    Some(all0[0].signature),
                    2,
                )
                .unwrap()
                .is_empty());

            // Fetch all signatures for address 0, three at a time
            assert!(all0.len() % 3 == 0);
            for i in (0..all0.len()).step_by(3) {
                let results = blockstore
                    .get_confirmed_signatures_for_address2(
                        address0,
                        highest_confirmed_root,
                        if i == 0 {
                            None
                        } else {
                            Some(all0[i - 1].signature)
                        },
                        None,
                        3,
                    )
                    .unwrap();
                assert_eq!(results.len(), 3);
                assert_eq!(results[0], all0[i]);
                assert_eq!(results[1], all0[i + 1]);
                assert_eq!(results[2], all0[i + 2]);
            }

            // Ensure that the signatures within a slot are reverse ordered by signature
            // (current limitation of the .get_confirmed_signatures_for_address2())
            for i in (0..all1.len()).step_by(2) {
                let results = blockstore
                    .get_confirmed_signatures_for_address2(
                        address1,
                        highest_confirmed_root,
                        if i == 0 {
                            None
                        } else {
                            Some(all1[i - 1].signature)
                        },
                        None,
                        2,
                    )
                    .unwrap();
                assert_eq!(results.len(), 2);
                assert_eq!(results[0].slot, results[1].slot);
                assert!(results[0].signature >= results[1].signature);
                assert_eq!(results[0], all1[i]);
                assert_eq!(results[1], all1[i + 1]);
            }

            // A search for address 0 with `before` and/or `until` signatures from address1 should also work
            let results = blockstore
                .get_confirmed_signatures_for_address2(
                    address0,
                    highest_confirmed_root,
                    Some(all1[0].signature),
                    None,
                    usize::MAX,
                )
                .unwrap();
            // The exact number of results returned is variable, based on the sort order of the
            // random signatures that are generated
            assert!(!results.is_empty());

            let results2 = blockstore
                .get_confirmed_signatures_for_address2(
                    address0,
                    highest_confirmed_root,
                    Some(all1[0].signature),
                    Some(all1[4].signature),
                    usize::MAX,
                )
                .unwrap();
            assert!(results2.len() < results.len());

            // Duplicate all tests using confirmed signatures
            let highest_confirmed_slot = 10;

            // Fetch all signatures for address 0 at once...
            let all0 = blockstore
                .get_confirmed_signatures_for_address2(
                    address0,
                    highest_confirmed_slot,
                    None,
                    None,
                    usize::MAX,
                )
                .unwrap();
            assert_eq!(all0.len(), 14);

            // Fetch all signatures for address 1 at once...
            let all1 = blockstore
                .get_confirmed_signatures_for_address2(
                    address1,
                    highest_confirmed_slot,
                    None,
                    None,
                    usize::MAX,
                )
                .unwrap();
            assert_eq!(all1.len(), 14);

            // Fetch all signatures for address 0 individually
            for i in 0..all0.len() {
                let results = blockstore
                    .get_confirmed_signatures_for_address2(
                        address0,
                        highest_confirmed_slot,
                        if i == 0 {
                            None
                        } else {
                            Some(all0[i - 1].signature)
                        },
                        None,
                        1,
                    )
                    .unwrap();
                assert_eq!(results.len(), 1);
                assert_eq!(results[0], all0[i], "Unexpected result for {}", i);
            }
            // Fetch all signatures for address 0 individually using `until`
            for i in 0..all0.len() {
                let results = blockstore
                    .get_confirmed_signatures_for_address2(
                        address0,
                        highest_confirmed_slot,
                        if i == 0 {
                            None
                        } else {
                            Some(all0[i - 1].signature)
                        },
                        if i == all0.len() - 1 || i == all0.len() {
                            None
                        } else {
                            Some(all0[i + 1].signature)
                        },
                        10,
                    )
                    .unwrap();
                assert_eq!(results.len(), 1);
                assert_eq!(results[0], all0[i], "Unexpected result for {}", i);
            }

            assert!(blockstore
                .get_confirmed_signatures_for_address2(
                    address0,
                    highest_confirmed_slot,
                    Some(all0[all0.len() - 1].signature),
                    None,
                    1,
                )
                .unwrap()
                .is_empty());

            assert!(blockstore
                .get_confirmed_signatures_for_address2(
                    address0,
                    highest_confirmed_slot,
                    None,
                    Some(all0[0].signature),
                    2,
                )
                .unwrap()
                .is_empty());

            // Fetch all signatures for address 0, three at a time
            assert!(all0.len() % 3 == 2);
            for i in (0..all0.len()).step_by(3) {
                let results = blockstore
                    .get_confirmed_signatures_for_address2(
                        address0,
                        highest_confirmed_slot,
                        if i == 0 {
                            None
                        } else {
                            Some(all0[i - 1].signature)
                        },
                        None,
                        3,
                    )
                    .unwrap();
                if i < 12 {
                    assert_eq!(results.len(), 3);
                    assert_eq!(results[2], all0[i + 2]);
                } else {
                    assert_eq!(results.len(), 2);
                }
                assert_eq!(results[0], all0[i]);
                assert_eq!(results[1], all0[i + 1]);
            }

            // Ensure that the signatures within a slot are reverse ordered by signature
            // (current limitation of the .get_confirmed_signatures_for_address2())
            for i in (0..all1.len()).step_by(2) {
                let results = blockstore
                    .get_confirmed_signatures_for_address2(
                        address1,
                        highest_confirmed_slot,
                        if i == 0 {
                            None
                        } else {
                            Some(all1[i - 1].signature)
                        },
                        None,
                        2,
                    )
                    .unwrap();
                assert_eq!(results.len(), 2);
                assert_eq!(results[0].slot, results[1].slot);
                assert!(results[0].signature >= results[1].signature);
                assert_eq!(results[0], all1[i]);
                assert_eq!(results[1], all1[i + 1]);
            }

            // A search for address 0 with `before` and/or `until` signatures from address1 should also work
            let results = blockstore
                .get_confirmed_signatures_for_address2(
                    address0,
                    highest_confirmed_slot,
                    Some(all1[0].signature),
                    None,
                    usize::MAX,
                )
                .unwrap();
            // The exact number of results returned is variable, based on the sort order of the
            // random signatures that are generated
            assert!(!results.is_empty());

            let results2 = blockstore
                .get_confirmed_signatures_for_address2(
                    address0,
                    highest_confirmed_slot,
                    Some(all1[0].signature),
                    Some(all1[4].signature),
                    usize::MAX,
                )
                .unwrap();
            assert!(results2.len() < results.len());
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    #[allow(clippy::same_item_push)]
    fn test_get_last_hash() {
        let mut entries: Vec<Entry> = vec![];
        let empty_entries_iterator = entries.iter();
        assert!(get_last_hash(empty_entries_iterator).is_none());

        let mut prev_hash = hash::hash(&[42u8]);
        for _ in 0..10 {
            let entry = next_entry(&prev_hash, 1, vec![]);
            prev_hash = entry.hash;
            entries.push(entry);
        }
        let entries_iterator = entries.iter();
        assert_eq!(get_last_hash(entries_iterator).unwrap(), entries[9].hash);
    }

    #[test]
    fn test_map_transactions_to_statuses() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let transaction_status_cf = blockstore.db.column::<cf::TransactionStatus>();

            let slot = 0;
            let mut transactions: Vec<Transaction> = vec![];
            for x in 0..4 {
                let transaction = Transaction::new_with_compiled_instructions(
                    &[&Keypair::new()],
                    &[solana_sdk::pubkey::new_rand()],
                    Hash::default(),
                    vec![solana_sdk::pubkey::new_rand()],
                    vec![CompiledInstruction::new(1, &(), vec![0])],
                );
                let status = TransactionStatusMeta {
                    status: solana_sdk::transaction::Result::<()>::Err(
                        TransactionError::AccountNotFound,
                    ),
                    fee: x,
                    pre_balances: vec![],
                    post_balances: vec![],
                    inner_instructions: Some(vec![]),
                    log_messages: Some(vec![]),
                    pre_token_balances: Some(vec![]),
                    post_token_balances: Some(vec![]),
                    rewards: Some(vec![]),
                }
                .into();
                transaction_status_cf
                    .put_protobuf((0, transaction.signatures[0], slot), &status)
                    .unwrap();
                transactions.push(transaction);
            }
            // Push transaction that will not have matching status, as a test case
            transactions.push(Transaction::new_with_compiled_instructions(
                &[&Keypair::new()],
                &[solana_sdk::pubkey::new_rand()],
                Hash::default(),
                vec![solana_sdk::pubkey::new_rand()],
                vec![CompiledInstruction::new(1, &(), vec![0])],
            ));

            let map = blockstore.map_transactions_to_statuses(slot, transactions.into_iter());
            assert_eq!(map.len(), 5);
            for (x, m) in map.iter().take(4).enumerate() {
                assert_eq!(m.meta.as_ref().unwrap().fee, x as u64);
            }
            assert_eq!(map[4].meta, None);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_write_get_perf_samples() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let num_entries: usize = 10;
            let mut perf_samples: Vec<(Slot, PerfSample)> = vec![];
            for x in 1..num_entries + 1 {
                perf_samples.push((
                    x as u64 * 50,
                    PerfSample {
                        num_transactions: 1000 + x as u64,
                        num_slots: 50,
                        sample_period_secs: 20,
                    },
                ));
            }
            for (slot, sample) in perf_samples.iter() {
                blockstore.write_perf_sample(*slot, sample).unwrap();
            }
            for x in 0..num_entries {
                let mut expected_samples = perf_samples[num_entries - 1 - x..].to_vec();
                expected_samples.sort_by(|a, b| b.0.cmp(&a.0));
                assert_eq!(
                    blockstore.get_recent_perf_samples(x + 1).unwrap(),
                    expected_samples
                );
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_lowest_slot() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            for i in 0..10 {
                let slot = i;
                let (shreds, _) = make_slot_entries(slot, 0, 1);
                blockstore.insert_shreds(shreds, None, false).unwrap();
            }
            assert_eq!(blockstore.lowest_slot(), 1);
            blockstore.run_purge(0, 5, PurgeType::PrimaryIndex).unwrap();
            assert_eq!(blockstore.lowest_slot(), 6);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_recovery() {
        let slot = 1;
        let (data_shreds, coding_shreds, leader_schedule_cache) =
            setup_erasure_shreds(slot, 0, 100);
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            blockstore
                .insert_shreds(coding_shreds, Some(&leader_schedule_cache), false)
                .unwrap();
            let shred_bufs: Vec<_> = data_shreds
                .iter()
                .map(|shred| shred.payload.clone())
                .collect();

            // Check all the data shreds were recovered
            for (s, buf) in data_shreds.iter().zip(shred_bufs) {
                assert_eq!(
                    blockstore
                        .get_data_shred(s.slot(), s.index() as u64)
                        .unwrap()
                        .unwrap(),
                    buf
                );
            }

            verify_index_integrity(&blockstore, slot);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_index_integrity() {
        let slot = 1;
        let num_entries = 100;
        let (data_shreds, coding_shreds, leader_schedule_cache) =
            setup_erasure_shreds(slot, 0, num_entries);
        assert!(data_shreds.len() > 3);
        assert!(coding_shreds.len() > 3);
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            // Test inserting all the shreds
            let all_shreds: Vec<_> = data_shreds
                .iter()
                .cloned()
                .chain(coding_shreds.iter().cloned())
                .collect();
            blockstore
                .insert_shreds(all_shreds, Some(&leader_schedule_cache), false)
                .unwrap();
            verify_index_integrity(&blockstore, slot);
            blockstore.purge_and_compact_slots(0, slot);

            // Test inserting just the codes, enough for recovery
            blockstore
                .insert_shreds(coding_shreds.clone(), Some(&leader_schedule_cache), false)
                .unwrap();
            verify_index_integrity(&blockstore, slot);
            blockstore.purge_and_compact_slots(0, slot);

            // Test inserting some codes, but not enough for recovery
            blockstore
                .insert_shreds(
                    coding_shreds[..coding_shreds.len() - 1].to_vec(),
                    Some(&leader_schedule_cache),
                    false,
                )
                .unwrap();
            verify_index_integrity(&blockstore, slot);
            blockstore.purge_and_compact_slots(0, slot);

            // Test inserting just the codes, and some data, enough for recovery
            let shreds: Vec<_> = data_shreds[..data_shreds.len() - 1]
                .iter()
                .cloned()
                .chain(coding_shreds[..coding_shreds.len() - 1].iter().cloned())
                .collect();
            blockstore
                .insert_shreds(shreds, Some(&leader_schedule_cache), false)
                .unwrap();
            verify_index_integrity(&blockstore, slot);
            blockstore.purge_and_compact_slots(0, slot);

            // Test inserting some codes, and some data, but enough for recovery
            let shreds: Vec<_> = data_shreds[..data_shreds.len() / 2 - 1]
                .iter()
                .cloned()
                .chain(coding_shreds[..coding_shreds.len() / 2 - 1].iter().cloned())
                .collect();
            blockstore
                .insert_shreds(shreds, Some(&leader_schedule_cache), false)
                .unwrap();
            verify_index_integrity(&blockstore, slot);
            blockstore.purge_and_compact_slots(0, slot);

            // Test inserting all shreds in 2 rounds, make sure nothing is lost
            let shreds1: Vec<_> = data_shreds[..data_shreds.len() / 2 - 1]
                .iter()
                .cloned()
                .chain(coding_shreds[..coding_shreds.len() / 2 - 1].iter().cloned())
                .collect();
            let shreds2: Vec<_> = data_shreds[data_shreds.len() / 2 - 1..]
                .iter()
                .cloned()
                .chain(coding_shreds[coding_shreds.len() / 2 - 1..].iter().cloned())
                .collect();
            blockstore
                .insert_shreds(shreds1, Some(&leader_schedule_cache), false)
                .unwrap();
            blockstore
                .insert_shreds(shreds2, Some(&leader_schedule_cache), false)
                .unwrap();
            verify_index_integrity(&blockstore, slot);
            blockstore.purge_and_compact_slots(0, slot);

            // Test not all, but enough data and coding shreds in 2 rounds to trigger recovery,
            // make sure nothing is lost
            let shreds1: Vec<_> = data_shreds[..data_shreds.len() / 2 - 1]
                .iter()
                .cloned()
                .chain(coding_shreds[..coding_shreds.len() / 2 - 1].iter().cloned())
                .collect();
            let shreds2: Vec<_> = data_shreds[data_shreds.len() / 2 - 1..data_shreds.len() / 2]
                .iter()
                .cloned()
                .chain(
                    coding_shreds[coding_shreds.len() / 2 - 1..coding_shreds.len() / 2]
                        .iter()
                        .cloned(),
                )
                .collect();
            blockstore
                .insert_shreds(shreds1, Some(&leader_schedule_cache), false)
                .unwrap();
            blockstore
                .insert_shreds(shreds2, Some(&leader_schedule_cache), false)
                .unwrap();
            verify_index_integrity(&blockstore, slot);
            blockstore.purge_and_compact_slots(0, slot);

            // Test insert shreds in 2 rounds, but not enough to trigger
            // recovery, make sure nothing is lost
            let shreds1: Vec<_> = data_shreds[..data_shreds.len() / 2 - 2]
                .iter()
                .cloned()
                .chain(coding_shreds[..coding_shreds.len() / 2 - 2].iter().cloned())
                .collect();
            let shreds2: Vec<_> = data_shreds[data_shreds.len() / 2 - 2..data_shreds.len() / 2 - 1]
                .iter()
                .cloned()
                .chain(
                    coding_shreds[coding_shreds.len() / 2 - 2..coding_shreds.len() / 2 - 1]
                        .iter()
                        .cloned(),
                )
                .collect();
            blockstore
                .insert_shreds(shreds1, Some(&leader_schedule_cache), false)
                .unwrap();
            blockstore
                .insert_shreds(shreds2, Some(&leader_schedule_cache), false)
                .unwrap();
            verify_index_integrity(&blockstore, slot);
            blockstore.purge_and_compact_slots(0, slot);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    fn setup_erasure_shreds(
        slot: u64,
        parent_slot: u64,
        num_entries: u64,
    ) -> (Vec<Shred>, Vec<Shred>, Arc<LeaderScheduleCache>) {
        let entries = make_slot_entries_with_transactions(num_entries);
        let leader_keypair = Arc::new(Keypair::new());
        let shredder = Shredder::new(slot, parent_slot, 0, 0).unwrap();
        let (data_shreds, coding_shreds, _) =
            shredder.entries_to_shreds(&leader_keypair, &entries, true, 0);

        let genesis_config = create_genesis_config(2).genesis_config;
        let bank = Arc::new(Bank::new(&genesis_config));
        let mut leader_schedule_cache = LeaderScheduleCache::new_from_bank(&bank);
        let fixed_schedule = FixedSchedule {
            leader_schedule: Arc::new(LeaderSchedule::new_from_schedule(vec![
                leader_keypair.pubkey()
            ])),
            start_epoch: 0,
        };
        leader_schedule_cache.set_fixed_leader_schedule(Some(fixed_schedule));

        (data_shreds, coding_shreds, Arc::new(leader_schedule_cache))
    }

    fn verify_index_integrity(blockstore: &Blockstore, slot: u64) {
        let shred_index = blockstore.get_index(slot).unwrap().unwrap();
        let data = blockstore.get_data_shreds_for_slot(slot, 0).unwrap();
        let mut num_data = 0;
        for shred in data.iter() {
            num_data += 1;
            // Test that by-slot and individual shred lookup yield same set
            assert!(blockstore
                .get_data_shred(shred.common_header.slot, shred.common_header.index.into())
                .unwrap()
                .is_some());
            // Test that the data index has current shred accounted for
            assert!(shred_index
                .data()
                .is_present(shred.common_header.index.into()));
        }

        // Test the data index doesn't have anything extra
        let num_data_in_index = shred_index.data().num_shreds();
        assert_eq!(num_data_in_index, num_data);

        let coding_iter = blockstore.slot_coding_iterator(slot, 0).unwrap();
        let mut num_coding = 0;
        for ((slot, index), _) in coding_iter {
            num_coding += 1;
            // Test that the iterator and individual shred lookup yield same set
            assert!(blockstore.get_coding_shred(slot, index).unwrap().is_some());
            // Test that the coding index has current shred accounted for
            assert!(shred_index.coding().is_present(index));
        }

        // Test the data index doesn't have anything extra
        let num_coding_in_index = shred_index.coding().num_shreds();
        assert_eq!(num_coding_in_index, num_coding);
    }

    #[test]
    fn test_duplicate_slot() {
        let slot = 0;
        let entries1 = make_slot_entries_with_transactions(1);
        let entries2 = make_slot_entries_with_transactions(1);
        let leader_keypair = Arc::new(Keypair::new());
        let shredder = Shredder::new(slot, 0, 0, 0).unwrap();
        let (shreds, _, _) = shredder.entries_to_shreds(&leader_keypair, &entries1, true, 0);
        let (duplicate_shreds, _, _) =
            shredder.entries_to_shreds(&leader_keypair, &entries2, true, 0);
        let shred = shreds[0].clone();
        let duplicate_shred = duplicate_shreds[0].clone();
        let non_duplicate_shred = shred.clone();

        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            blockstore
                .insert_shreds(vec![shred.clone()], None, false)
                .unwrap();

            // No duplicate shreds exist yet
            assert!(!blockstore.has_duplicate_shreds_in_slot(slot));

            // Check if shreds are duplicated
            assert_eq!(
                blockstore.is_shred_duplicate(
                    slot,
                    0,
                    &duplicate_shred.payload,
                    duplicate_shred.is_data()
                ),
                Some(shred.payload.to_vec())
            );
            assert!(blockstore
                .is_shred_duplicate(
                    slot,
                    0,
                    &non_duplicate_shred.payload,
                    duplicate_shred.is_data()
                )
                .is_none());

            // Store a duplicate shred
            blockstore
                .store_duplicate_slot(slot, shred.payload.clone(), duplicate_shred.payload.clone())
                .unwrap();

            // Slot is now marked as duplicate
            assert!(blockstore.has_duplicate_shreds_in_slot(slot));

            // Check ability to fetch the duplicates
            let duplicate_proof = blockstore.get_duplicate_slot(slot).unwrap();
            assert_eq!(duplicate_proof.shred1, shred.payload);
            assert_eq!(duplicate_proof.shred2, duplicate_shred.payload);
        }

        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_clear_unconfirmed_slot() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let unconfirmed_slot = 9;
            let unconfirmed_child_slot = 10;
            let slots = vec![2, unconfirmed_slot, unconfirmed_child_slot];

            // Insert into slot 9, mark it as dead
            let shreds: Vec<_> = make_chaining_slot_entries(&slots, 1)
                .into_iter()
                .flat_map(|x| x.0)
                .collect();
            blockstore.insert_shreds(shreds, None, false).unwrap();
            // Should only be one shred in slot 9
            assert!(blockstore
                .get_data_shred(unconfirmed_slot, 0)
                .unwrap()
                .is_some());
            assert!(blockstore
                .get_data_shred(unconfirmed_slot, 1)
                .unwrap()
                .is_none());
            blockstore.set_dead_slot(unconfirmed_slot).unwrap();

            // Purge the slot
            blockstore.clear_unconfirmed_slot(unconfirmed_slot);
            assert!(!blockstore.is_dead(unconfirmed_slot));
            assert_eq!(
                blockstore
                    .meta(unconfirmed_slot)
                    .unwrap()
                    .unwrap()
                    .next_slots,
                vec![unconfirmed_child_slot]
            );
            assert!(blockstore
                .get_data_shred(unconfirmed_slot, 0)
                .unwrap()
                .is_none());
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_update_completed_data_indexes() {
        let mut completed_data_indexes: Vec<u32> = vec![];
        let mut shred_index = ShredIndex::default();

        for i in 0..10 {
            shred_index.set_present(i as u64, true);
            assert_eq!(
                update_completed_data_indexes(true, i, &shred_index, &mut completed_data_indexes),
                vec![(i, i)]
            );
            assert_eq!(completed_data_indexes, (0..=i).collect::<Vec<u32>>());
        }
    }

    #[test]
    fn test_update_completed_data_indexes_out_of_order() {
        let mut completed_data_indexes = vec![];
        let mut shred_index = ShredIndex::default();

        shred_index.set_present(4, true);
        assert!(
            update_completed_data_indexes(false, 4, &shred_index, &mut completed_data_indexes)
                .is_empty()
        );
        assert!(completed_data_indexes.is_empty());

        shred_index.set_present(2, true);
        assert!(
            update_completed_data_indexes(false, 2, &shred_index, &mut completed_data_indexes)
                .is_empty()
        );
        assert!(completed_data_indexes.is_empty());

        shred_index.set_present(3, true);
        assert!(
            update_completed_data_indexes(true, 3, &shred_index, &mut completed_data_indexes)
                .is_empty()
        );
        assert_eq!(completed_data_indexes, vec![3]);

        // Inserting data complete shred 1 now confirms the range of shreds [2, 3]
        // is part of the same data set
        shred_index.set_present(1, true);
        assert_eq!(
            update_completed_data_indexes(true, 1, &shred_index, &mut completed_data_indexes),
            vec![(2, 3)]
        );
        assert_eq!(completed_data_indexes, vec![1, 3]);

        // Inserting data complete shred 0 now confirms the range of shreds [0]
        // is part of the same data set
        shred_index.set_present(0, true);
        assert_eq!(
            update_completed_data_indexes(true, 0, &shred_index, &mut completed_data_indexes),
            vec![(0, 0), (1, 1)]
        );
        assert_eq!(completed_data_indexes, vec![0, 1, 3]);
    }

    #[test]
    fn test_rewards_protobuf_backward_compatability() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let rewards: Rewards = (0..100)
                .map(|i| Reward {
                    pubkey: solana_sdk::pubkey::new_rand().to_string(),
                    lamports: 42 + i,
                    post_balance: std::u64::MAX,
                    reward_type: Some(RewardType::Fee),
                    commission: None,
                })
                .collect();
            let protobuf_rewards: generated::Rewards = rewards.into();

            let deprecated_rewards: StoredExtendedRewards = protobuf_rewards.clone().into();
            for slot in 0..2 {
                let data = serialize(&deprecated_rewards).unwrap();
                blockstore.rewards_cf.put_bytes(slot, &data).unwrap();
            }
            for slot in 2..4 {
                blockstore
                    .rewards_cf
                    .put_protobuf(slot, &protobuf_rewards)
                    .unwrap();
            }
            for slot in 0..4 {
                assert_eq!(
                    blockstore
                        .rewards_cf
                        .get_protobuf_or_bincode::<StoredExtendedRewards>(slot)
                        .unwrap()
                        .unwrap(),
                    protobuf_rewards
                );
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_transaction_status_protobuf_backward_compatability() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let status = TransactionStatusMeta {
                status: Ok(()),
                fee: 42,
                pre_balances: vec![1, 2, 3],
                post_balances: vec![1, 2, 3],
                inner_instructions: Some(vec![]),
                log_messages: Some(vec![]),
                pre_token_balances: Some(vec![TransactionTokenBalance {
                    account_index: 0,
                    mint: Pubkey::new_unique().to_string(),
                    ui_token_amount: UiTokenAmount {
                        ui_amount: Some(1.1),
                        decimals: 1,
                        amount: "11".to_string(),
                        ui_amount_string: "1.1".to_string(),
                    },
                }]),
                post_token_balances: Some(vec![TransactionTokenBalance {
                    account_index: 0,
                    mint: Pubkey::new_unique().to_string(),
                    ui_token_amount: UiTokenAmount {
                        ui_amount: None,
                        decimals: 1,
                        amount: "11".to_string(),
                        ui_amount_string: "1.1".to_string(),
                    },
                }]),
                rewards: Some(vec![Reward {
                    pubkey: "My11111111111111111111111111111111111111111".to_string(),
                    lamports: -42,
                    post_balance: 42,
                    reward_type: Some(RewardType::Rent),
                    commission: None,
                }]),
            };
            let deprecated_status: StoredTransactionStatusMeta = status.clone().into();
            let protobuf_status: generated::TransactionStatusMeta = status.into();

            for slot in 0..2 {
                let data = serialize(&deprecated_status).unwrap();
                blockstore
                    .transaction_status_cf
                    .put_bytes((0, Signature::default(), slot), &data)
                    .unwrap();
            }
            for slot in 2..4 {
                blockstore
                    .transaction_status_cf
                    .put_protobuf((0, Signature::default(), slot), &protobuf_status)
                    .unwrap();
            }
            for slot in 0..4 {
                assert_eq!(
                    blockstore
                        .transaction_status_cf
                        .get_protobuf_or_bincode::<StoredTransactionStatusMeta>((
                            0,
                            Signature::default(),
                            slot
                        ))
                        .unwrap()
                        .unwrap(),
                    protobuf_status
                );
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_remove_shred_data_complete_flag() {
        let (mut shreds, entries) = make_slot_entries(0, 0, 1);

        let ledger_path = get_tmp_ledger_path!();
        let ledger = Blockstore::open(&ledger_path).unwrap();

        // Remove the data complete flag from the last shred
        shreds[0].unset_data_complete();

        ledger.insert_shreds(shreds, None, false).unwrap();

        // Check that the `data_complete` flag was unset in the stored shred, but the
        // `last_in_slot` flag is set.
        let stored_shred = &ledger.get_data_shreds_for_slot(0, 0).unwrap()[0];
        assert!(!stored_shred.data_complete());
        assert!(stored_shred.last_in_slot());
        assert_eq!(entries, ledger.get_any_valid_slot_entries(0, 0));
    }

    fn make_large_tx_entry(num_txs: usize) -> Entry {
        let txs: Vec<_> = (0..num_txs)
            .into_iter()
            .map(|_| {
                let keypair0 = Keypair::new();
                let to = solana_sdk::pubkey::new_rand();
                solana_sdk::system_transaction::transfer(&keypair0, &to, 1, Hash::default())
            })
            .collect();

        Entry::new(&Hash::default(), 1, txs)
    }

    #[test]
    fn erasure_multiple_config() {
        solana_logger::setup();
        let slot = 1;
        let parent = 0;
        let num_txs = 20;
        let entry = make_large_tx_entry(num_txs);
        let shreds = entries_to_test_shreds(vec![entry], slot, parent, true, 0);
        assert!(shreds.len() > 1);

        let ledger_path = get_tmp_ledger_path!();
        let ledger = Blockstore::open(&ledger_path).unwrap();

        let coding1 = Shredder::generate_coding_shreds(&shreds, false);
        let coding2 = Shredder::generate_coding_shreds(&shreds, true);
        for shred in &shreds {
            info!("shred {:?}", shred);
        }
        for shred in &coding1 {
            info!("coding1 {:?}", shred);
        }
        for shred in &coding2 {
            info!("coding2 {:?}", shred);
        }
        ledger
            .insert_shreds(shreds[..shreds.len() - 2].to_vec(), None, false)
            .unwrap();
        ledger
            .insert_shreds(vec![coding1[0].clone(), coding2[1].clone()], None, false)
            .unwrap();
        assert!(ledger.has_duplicate_shreds_in_slot(slot));
    }

    #[test]
    fn test_large_num_coding() {
        solana_logger::setup();
        let slot = 1;
        let (_data_shreds, mut coding_shreds, leader_schedule_cache) =
            setup_erasure_shreds(slot, 0, 100);
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            coding_shreds[1].coding_header.num_coding_shreds = u16::MAX;
            blockstore
                .insert_shreds(
                    vec![coding_shreds[1].clone()],
                    Some(&leader_schedule_cache),
                    false,
                )
                .unwrap();

            // Check no coding shreds are inserted
            let res = blockstore.get_coding_shreds_for_slot(slot, 0).unwrap();
            assert!(res.is_empty());
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_insert_data_shreds_same_slot_last_index() {
        // Create RocksDb ledger
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            // Create enough entries to ensure there are at least two shreds created
            let num_unique_entries = max_ticks_per_n_shreds(1, None) + 1;
            let (mut original_shreds, original_entries) =
                make_slot_entries(0, 0, num_unique_entries);

            // Discard first shred, so that the slot is not full
            assert!(original_shreds.len() > 1);
            let last_index = original_shreds.last().unwrap().index() as u64;
            original_shreds.remove(0);

            // Insert the same shreds, including the last shred specifically, multiple
            // times
            for _ in 0..10 {
                blockstore
                    .insert_shreds(original_shreds.clone(), None, false)
                    .unwrap();
                let meta = blockstore.meta(0).unwrap().unwrap();
                assert!(!blockstore.is_dead(0));
                assert_eq!(blockstore.get_slot_entries(0, 0).unwrap(), vec![]);
                assert_eq!(meta.consumed, 0);
                assert_eq!(meta.received, last_index + 1);
                assert_eq!(meta.parent_slot, 0);
                assert_eq!(meta.last_index, last_index);
                assert!(!blockstore.is_full(0));
            }

            let duplicate_shreds = entries_to_test_shreds(original_entries.clone(), 0, 0, true, 0);
            let num_shreds = duplicate_shreds.len() as u64;
            blockstore
                .insert_shreds(duplicate_shreds, None, false)
                .unwrap();

            assert_eq!(blockstore.get_slot_entries(0, 0).unwrap(), original_entries);

            let meta = blockstore.meta(0).unwrap().unwrap();
            assert_eq!(meta.consumed, num_shreds);
            assert_eq!(meta.received, num_shreds);
            assert_eq!(meta.parent_slot, 0);
            assert_eq!(meta.last_index, num_shreds - 1);
            assert!(blockstore.is_full(0));
            assert!(!blockstore.is_dead(0));
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_duplicate_last_index() {
        let num_shreds = 2;
        let num_entries = max_ticks_per_n_shreds(num_shreds, None);
        let slot = 1;
        let (mut shreds, _) = make_slot_entries(slot, 0, num_entries);

        // Mark both as last shred
        shreds[0].set_last_in_slot();
        shreds[1].set_last_in_slot();
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            blockstore.insert_shreds(shreds, None, false).unwrap();

            assert!(blockstore.get_duplicate_slot(slot).is_some());
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_duplicate_last_index_mark_dead() {
        let num_shreds = 10;
        let smaller_last_shred_index = 5;
        let larger_last_shred_index = 8;

        let setup_test_shreds = |slot: Slot| -> Vec<Shred> {
            let num_entries = max_ticks_per_n_shreds(num_shreds, None);
            let (mut shreds, _) = make_slot_entries(slot, 0, num_entries);
            shreds[smaller_last_shred_index].set_last_in_slot();
            shreds[larger_last_shred_index].set_last_in_slot();
            shreds
        };

        let get_expected_slot_meta_and_index_meta =
            |blockstore: &Blockstore, shreds: Vec<Shred>| -> (SlotMeta, Index) {
                let slot = shreds[0].slot();
                blockstore
                    .insert_shreds(shreds.clone(), None, false)
                    .unwrap();
                let meta = blockstore.meta(slot).unwrap().unwrap();
                assert_eq!(meta.consumed, shreds.len() as u64);
                let shreds_index = blockstore.get_index(slot).unwrap().unwrap();
                for i in 0..shreds.len() as u64 {
                    assert!(shreds_index.data().is_present(i));
                }

                // Cleanup the slot
                blockstore
                    .run_purge(slot, slot, PurgeType::PrimaryIndex)
                    .expect("Purge database operations failed");
                assert!(blockstore.meta(slot).unwrap().is_none());

                (meta, shreds_index)
            };

        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let mut slot = 0;
            let shreds = setup_test_shreds(slot);

            // Case 1: Insert in the same batch. Since we're inserting the shreds in order,
            // any shreds > smaller_last_shred_index will not be inserted. Slot is not marked
            // as dead because no slots > the first "last" index shred are inserted before
            // the "last" index shred itself is inserted.
            let (expected_slot_meta, expected_index) = get_expected_slot_meta_and_index_meta(
                &blockstore,
                shreds[..=smaller_last_shred_index].to_vec(),
            );
            blockstore
                .insert_shreds(shreds.clone(), None, false)
                .unwrap();
            assert!(blockstore.get_duplicate_slot(slot).is_some());
            assert!(!blockstore.is_dead(slot));
            for i in 0..num_shreds {
                if i <= smaller_last_shred_index as u64 {
                    assert_eq!(
                        blockstore.get_data_shred(slot, i).unwrap().unwrap(),
                        shreds[i as usize].payload
                    );
                } else {
                    assert!(blockstore.get_data_shred(slot, i).unwrap().is_none());
                }
            }
            let mut meta = blockstore.meta(slot).unwrap().unwrap();
            meta.first_shred_timestamp = expected_slot_meta.first_shred_timestamp;
            assert_eq!(meta, expected_slot_meta);
            assert_eq!(blockstore.get_index(slot).unwrap().unwrap(), expected_index);

            // Case 2: Inserting a duplicate with an even smaller last shred index should not
            // mark the slot as dead since the Slotmeta is full.
            let mut even_smaller_last_shred_duplicate =
                shreds[smaller_last_shred_index - 1].clone();
            even_smaller_last_shred_duplicate.set_last_in_slot();
            // Flip a byte to create a duplicate shred
            even_smaller_last_shred_duplicate.payload[0] =
                std::u8::MAX - even_smaller_last_shred_duplicate.payload[0];
            assert!(blockstore
                .is_shred_duplicate(
                    slot,
                    even_smaller_last_shred_duplicate.index(),
                    &even_smaller_last_shred_duplicate.payload,
                    true
                )
                .is_some());
            blockstore
                .insert_shreds(vec![even_smaller_last_shred_duplicate], None, false)
                .unwrap();
            assert!(!blockstore.is_dead(slot));
            for i in 0..num_shreds {
                if i <= smaller_last_shred_index as u64 {
                    assert_eq!(
                        blockstore.get_data_shred(slot, i).unwrap().unwrap(),
                        shreds[i as usize].payload
                    );
                } else {
                    assert!(blockstore.get_data_shred(slot, i).unwrap().is_none());
                }
            }
            let mut meta = blockstore.meta(slot).unwrap().unwrap();
            meta.first_shred_timestamp = expected_slot_meta.first_shred_timestamp;
            assert_eq!(meta, expected_slot_meta);
            assert_eq!(blockstore.get_index(slot).unwrap().unwrap(), expected_index);

            // Case 3: Insert shreds in reverse so that consumed will not be updated. Now on insert, the
            // the slot should be marked as dead
            slot += 1;
            let mut shreds = setup_test_shreds(slot);
            shreds.reverse();
            blockstore
                .insert_shreds(shreds.clone(), None, false)
                .unwrap();
            assert!(blockstore.is_dead(slot));
            // All the shreds other than the two last index shreds because those two
            // are marked as last, but less than the first received index == 10.
            // The others will be inserted even after the slot is marked dead on attempted
            // insert of the first last_index shred since dead slots can still be
            // inserted into.
            for i in 0..num_shreds {
                let shred_to_check = &shreds[i as usize];
                let shred_index = shred_to_check.index() as u64;
                if shred_index != smaller_last_shred_index as u64
                    && shred_index != larger_last_shred_index as u64
                {
                    assert_eq!(
                        blockstore
                            .get_data_shred(slot, shred_index)
                            .unwrap()
                            .unwrap(),
                        shred_to_check.payload
                    );
                } else {
                    assert!(blockstore
                        .get_data_shred(slot, shred_index)
                        .unwrap()
                        .is_none());
                }
            }

            // Case 4: Same as Case 3, but this time insert the shreds one at a time to test that the clearing
            // of data shreds works even after they've been committed
            slot += 1;
            let mut shreds = setup_test_shreds(slot);
            shreds.reverse();
            for shred in shreds.clone() {
                blockstore.insert_shreds(vec![shred], None, false).unwrap();
            }
            assert!(blockstore.is_dead(slot));
            // All the shreds will be inserted since dead slots can still be inserted into.
            for i in 0..num_shreds {
                let shred_to_check = &shreds[i as usize];
                let shred_index = shred_to_check.index() as u64;
                if shred_index != smaller_last_shred_index as u64
                    && shred_index != larger_last_shred_index as u64
                {
                    assert_eq!(
                        blockstore
                            .get_data_shred(slot, shred_index)
                            .unwrap()
                            .unwrap(),
                        shred_to_check.payload
                    );
                } else {
                    assert!(blockstore
                        .get_data_shred(slot, shred_index)
                        .unwrap()
                        .is_none());
                }
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_get_slot_entries_dead_slot_race() {
        let setup_test_shreds = move |slot: Slot| -> Vec<Shred> {
            let num_shreds = 10;
            let middle_shred_index = 5;
            let num_entries = max_ticks_per_n_shreds(num_shreds, None);
            let (shreds, _) = make_slot_entries(slot, 0, num_entries);

            // Reverse shreds so that last shred gets inserted first and sets meta.received
            let mut shreds: Vec<Shred> = shreds.into_iter().rev().collect();

            // Push the real middle shred to the end of the shreds list
            shreds.push(shreds[middle_shred_index].clone());

            // Set the middle shred as a last shred to cause the slot to be marked dead
            shreds[middle_shred_index].set_last_in_slot();
            shreds
        };

        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Arc::new(Blockstore::open(&blockstore_path).unwrap());
            let (slot_sender, slot_receiver) = channel();
            let (shred_sender, shred_receiver) = channel::<Vec<Shred>>();
            let (signal_sender, signal_receiver) = channel();

            let t_entry_getter = {
                let blockstore = blockstore.clone();
                let signal_sender = signal_sender.clone();
                Builder::new()
                    .spawn(move || {
                        while let Ok(slot) = slot_receiver.recv() {
                            match blockstore.get_slot_entries_with_shred_info(slot, 0, false) {
                                Ok((_entries, _num_shreds, is_full)) => {
                                    if is_full {
                                        signal_sender
                                            .send(Err(IoError::new(
                                                ErrorKind::Other,
                                                "got full slot entries for dead slot",
                                            )))
                                            .unwrap();
                                    }
                                }
                                Err(err) => {
                                    assert_matches!(err, BlockstoreError::DeadSlot);
                                }
                            }
                            signal_sender.send(Ok(())).unwrap();
                        }
                    })
                    .unwrap()
            };

            let t_shred_inserter = Builder::new()
                .spawn(move || {
                    while let Ok(shreds) = shred_receiver.recv() {
                        let slot = shreds[0].slot();
                        // Grab this lock to block `get_slot_entries` before it fetches completed datasets
                        // and then mark the slot as dead, but full, by inserting carefully crafted shreds.
                        let _lowest_cleanup_slot = blockstore.lowest_cleanup_slot.write().unwrap();
                        blockstore.insert_shreds(shreds, None, false).unwrap();
                        assert!(blockstore.get_duplicate_slot(slot).is_some());
                        assert!(blockstore.is_dead(slot));
                        assert!(blockstore.meta(slot).unwrap().unwrap().is_full());
                        signal_sender.send(Ok(())).unwrap();
                    }
                })
                .unwrap();

            for slot in 0..100 {
                let shreds = setup_test_shreds(slot);

                // Start a task on each thread to trigger a race condition
                slot_sender.send(slot).unwrap();
                shred_sender.send(shreds).unwrap();

                // Check that each thread processed their task before continuing
                for _ in 1..=2 {
                    let res = signal_receiver.recv().unwrap();
                    assert!(res.is_ok(), "race condition: {:?}", res);
                }
            }

            drop(slot_sender);
            drop(shred_sender);

            let handles = vec![t_entry_getter, t_shred_inserter];
            for handle in handles {
                assert!(handle.join().is_ok());
            }
        }

        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_read_write_cost_table() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let num_entries: usize = 10;
            let mut cost_table: HashMap<Pubkey, u64> = HashMap::new();
            for x in 1..num_entries + 1 {
                cost_table.insert(Pubkey::new_unique(), (x + 100) as u64);
            }

            // write to db
            for (key, cost) in cost_table.iter() {
                blockstore
                    .write_program_cost(key, cost)
                    .expect("write a program");
            }

            // read back from db
            let read_back = blockstore.read_program_costs().expect("read programs");
            // verify
            assert_eq!(read_back.len(), cost_table.len());
            for (read_key, read_cost) in read_back {
                assert_eq!(read_cost, *cost_table.get(&read_key).unwrap());
            }

            // update value, write to db
            for val in cost_table.values_mut() {
                *val += 100;
            }
            for (key, cost) in cost_table.iter() {
                blockstore
                    .write_program_cost(key, cost)
                    .expect("write a program");
            }
            // add a new record
            let new_program_key = Pubkey::new_unique();
            let new_program_cost = 999;
            blockstore
                .write_program_cost(&new_program_key, &new_program_cost)
                .unwrap();

            // confirm value updated
            let read_back = blockstore.read_program_costs().expect("read programs");
            // verify
            assert_eq!(read_back.len(), cost_table.len() + 1);
            for (key, cost) in cost_table.iter() {
                assert_eq!(*cost, read_back.iter().find(|(k, _v)| k == key).unwrap().1);
            }
            assert_eq!(
                new_program_cost,
                read_back
                    .iter()
                    .find(|(k, _v)| *k == new_program_key)
                    .unwrap()
                    .1
            );

            // test delete
            blockstore
                .delete_program_cost(&new_program_key)
                .expect("delete a progrma");
            let read_back = blockstore.read_program_costs().expect("read programs");
            // verify
            assert_eq!(read_back.len(), cost_table.len());
            for (read_key, read_cost) in read_back {
                assert_eq!(read_cost, *cost_table.get(&read_key).unwrap());
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_delete_old_records_from_cost_table() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let num_entries: usize = 10;
            let mut cost_table: HashMap<Pubkey, u64> = HashMap::new();
            for x in 1..num_entries + 1 {
                cost_table.insert(Pubkey::new_unique(), (x + 100) as u64);
            }

            // write to db
            for (key, cost) in cost_table.iter() {
                blockstore
                    .write_program_cost(key, cost)
                    .expect("write a program");
            }

            // remove a record
            let mut removed_key = Pubkey::new_unique();
            for (key, cost) in cost_table.iter() {
                if *cost == 101_u64 {
                    removed_key = *key;
                    break;
                }
            }
            cost_table.remove(&removed_key);

            // delete records from blockstore if they are no longer in cost_table
            let db_records = blockstore.read_program_costs().expect("read programs");
            db_records.iter().for_each(|(pubkey, _)| {
                if !cost_table.iter().any(|(key, _)| key == pubkey) {
                    assert_eq!(*pubkey, removed_key);
                    blockstore
                        .delete_program_cost(pubkey)
                        .expect("delete old program");
                }
            });

            // read back from db
            let read_back = blockstore.read_program_costs().expect("read programs");
            // verify
            assert_eq!(read_back.len(), cost_table.len());
            for (read_key, read_cost) in read_back {
                assert_eq!(read_cost, *cost_table.get(&read_key).unwrap());
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }
}
