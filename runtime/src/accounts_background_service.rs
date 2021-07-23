// Service to clean up dead slots in accounts_db
//
// This can be expensive since we have to walk the append vecs being cleaned up.

use crate::{
    bank::{Bank, BankSlotDelta, DropCallback},
    bank_forks::BankForks,
    snapshot_config::SnapshotConfig,
    snapshot_package::AccountsPackageSender,
    snapshot_utils,
};
use crossbeam_channel::{Receiver, SendError, Sender};
use log::*;
use rand::{thread_rng, Rng};
use solana_measure::measure::Measure;
use solana_sdk::{
    clock::{BankId, Slot},
    hash::Hash,
};
use std::{
    boxed::Box,
    fmt::{Debug, Formatter},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{self, sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

const INTERVAL_MS: u64 = 100;
const SHRUNKEN_ACCOUNT_PER_SEC: usize = 250;
const SHRUNKEN_ACCOUNT_PER_INTERVAL: usize =
    SHRUNKEN_ACCOUNT_PER_SEC / (1000 / INTERVAL_MS as usize);
const CLEAN_INTERVAL_BLOCKS: u64 = 100;

// This value is chosen to spread the dropping cost over 3 expiration checks
// RecycleStores are fully populated almost all of its lifetime. So, otherwise
// this would drop MAX_RECYCLE_STORES mmaps at once in the worst case...
// (Anyway, the dropping part is outside the AccountsDb::recycle_stores lock
// and dropped in this AccountsBackgroundServe, so this shouldn't matter much)
const RECYCLE_STORE_EXPIRATION_INTERVAL_SECS: u64 = crate::accounts_db::EXPIRATION_TTL_SECONDS / 3;

pub type SnapshotRequestSender = Sender<SnapshotRequest>;
pub type SnapshotRequestReceiver = Receiver<SnapshotRequest>;
pub type DroppedSlotsSender = Sender<(Slot, BankId)>;
pub type DroppedSlotsReceiver = Receiver<(Slot, BankId)>;

#[derive(Clone)]
pub struct SendDroppedBankCallback {
    sender: DroppedSlotsSender,
}

impl DropCallback for SendDroppedBankCallback {
    fn callback(&self, bank: &Bank) {
        if let Err(e) = self.sender.send((bank.slot(), bank.bank_id())) {
            warn!("Error sending dropped banks: {:?}", e);
        }
    }

    fn clone_box(&self) -> Box<dyn DropCallback + Send + Sync> {
        Box::new(self.clone())
    }
}

impl Debug for SendDroppedBankCallback {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SendDroppedBankCallback({:p})", self)
    }
}

impl SendDroppedBankCallback {
    pub fn new(sender: DroppedSlotsSender) -> Self {
        Self { sender }
    }
}

pub struct SnapshotRequest {
    pub snapshot_root_bank: Arc<Bank>,
    pub status_cache_slot_deltas: Vec<BankSlotDelta>,
}

pub struct SnapshotRequestHandler {
    pub snapshot_config: SnapshotConfig,
    pub snapshot_request_receiver: SnapshotRequestReceiver,
    pub accounts_package_sender: AccountsPackageSender,
}

impl SnapshotRequestHandler {
    // Returns the latest requested snapshot slot, if one exists
    pub fn handle_snapshot_requests(
        &self,
        accounts_db_caching_enabled: bool,
        test_hash_calculation: bool,
        use_index_hash_calculation: bool,
        non_snapshot_time_us: u128,
    ) -> Option<u64> {
        self.snapshot_request_receiver
            .try_iter()
            .last()
            .map(|snapshot_request| {
                let mut total_time = Measure::start("snapshot_request_receiver_total_time");
                let SnapshotRequest {
                    snapshot_root_bank,
                    status_cache_slot_deltas,
                } = snapshot_request;

                let previous_hash = if test_hash_calculation {
                    // We have to use the index version here.
                    // We cannot calculate the non-index way because cache has not been flushed and stores don't match reality.
                    snapshot_root_bank.update_accounts_hash_with_index_option(true, false)
                } else {
                    Hash::default()
                };

                let mut shrink_time = Measure::start("shrink_time");
                if !accounts_db_caching_enabled {
                    snapshot_root_bank
                        .process_stale_slot_with_budget(0, SHRUNKEN_ACCOUNT_PER_INTERVAL);
                }
                shrink_time.stop();

                let mut flush_accounts_cache_time = Measure::start("flush_accounts_cache_time");
                if accounts_db_caching_enabled {
                    // Forced cache flushing MUST flush all roots <= snapshot_root_bank.slot().
                    // That's because `snapshot_root_bank.slot()` must be root at this point,
                    // and contains relevant updates because each bank has at least 1 account update due
                    // to sysvar maintenance. Otherwise, this would cause missing storages in the snapshot
                    snapshot_root_bank.force_flush_accounts_cache();
                    // Ensure all roots <= `self.slot()` have been flushed.
                    // Note `max_flush_root` could be larger than self.slot() if there are
                    // `> MAX_CACHE_SLOT` cached and rooted slots which triggered earlier flushes.
                    assert!(
                        snapshot_root_bank.slot()
                            <= snapshot_root_bank
                                .rc
                                .accounts
                                .accounts_db
                                .accounts_cache
                                .fetch_max_flush_root()
                    );
                }
                flush_accounts_cache_time.stop();

                let mut hash_time = Measure::start("hash_time");
                let this_hash = snapshot_root_bank.update_accounts_hash_with_index_option(
                    use_index_hash_calculation,
                    test_hash_calculation,
                );
                let hash_for_testing = if test_hash_calculation {
                    assert_eq!(previous_hash, this_hash);
                    Some(snapshot_root_bank.get_accounts_hash())
                } else {
                    None
                };
                hash_time.stop();

                let mut clean_time = Measure::start("clean_time");
                // Don't clean the slot we're snapshotting because it may have zero-lamport
                // accounts that were included in the bank delta hash when the bank was frozen,
                // and if we clean them here, the newly created snapshot's hash may not match
                // the frozen hash.
                // bprumo TODO: get and use real last_full_snapshot_slot
                snapshot_root_bank.clean_accounts(true, false, None);
                clean_time.stop();

                if accounts_db_caching_enabled {
                    shrink_time = Measure::start("shrink_time");
                    snapshot_root_bank.shrink_candidate_slots();
                    shrink_time.stop();
                }

                // Generate an accounts package
                let mut snapshot_time = Measure::start("snapshot_time");
                let r = snapshot_utils::snapshot_bank(
                    &snapshot_root_bank,
                    status_cache_slot_deltas,
                    &self.accounts_package_sender,
                    &self.snapshot_config.snapshot_path,
                    &self.snapshot_config.snapshot_package_output_path,
                    self.snapshot_config.snapshot_version,
                    &self.snapshot_config.archive_format,
                    hash_for_testing,
                );
                if r.is_err() {
                    warn!(
                        "Error generating snapshot for bank: {}, err: {:?}",
                        snapshot_root_bank.slot(),
                        r
                    );
                }
                snapshot_time.stop();

                // Cleanup outdated snapshots
                let mut purge_old_snapshots_time = Measure::start("purge_old_snapshots_time");
                snapshot_utils::purge_old_bank_snapshots(&self.snapshot_config.snapshot_path);
                purge_old_snapshots_time.stop();
                total_time.stop();

                datapoint_info!(
                    "handle_snapshot_requests-timing",
                    ("hash_time", hash_time.as_us(), i64),
                    (
                        "flush_accounts_cache_time",
                        flush_accounts_cache_time.as_us(),
                        i64
                    ),
                    ("shrink_time", shrink_time.as_us(), i64),
                    ("clean_time", clean_time.as_us(), i64),
                    ("snapshot_time", snapshot_time.as_us(), i64),
                    (
                        "purge_old_snapshots_time",
                        purge_old_snapshots_time.as_us(),
                        i64
                    ),
                    ("total_us", total_time.as_us(), i64),
                    ("non_snapshot_time_us", non_snapshot_time_us, i64),
                );
                snapshot_root_bank.block_height()
            })
    }
}

#[derive(Default)]
pub struct AbsRequestSender {
    snapshot_request_sender: Option<SnapshotRequestSender>,
}

impl AbsRequestSender {
    pub fn new(snapshot_request_sender: Option<SnapshotRequestSender>) -> Self {
        AbsRequestSender {
            snapshot_request_sender,
        }
    }

    pub fn is_snapshot_creation_enabled(&self) -> bool {
        self.snapshot_request_sender.is_some()
    }

    pub fn send_snapshot_request(
        &self,
        snapshot_request: SnapshotRequest,
    ) -> Result<(), SendError<SnapshotRequest>> {
        if let Some(ref snapshot_request_sender) = self.snapshot_request_sender {
            snapshot_request_sender.send(snapshot_request)
        } else {
            Ok(())
        }
    }
}

pub struct AbsRequestHandler {
    pub snapshot_request_handler: Option<SnapshotRequestHandler>,
    pub pruned_banks_receiver: DroppedSlotsReceiver,
}

impl AbsRequestHandler {
    // Returns the latest requested snapshot block height, if one exists
    pub fn handle_snapshot_requests(
        &self,
        accounts_db_caching_enabled: bool,
        test_hash_calculation: bool,
        use_index_hash_calculation: bool,
        non_snapshot_time_us: u128,
    ) -> Option<u64> {
        self.snapshot_request_handler
            .as_ref()
            .and_then(|snapshot_request_handler| {
                snapshot_request_handler.handle_snapshot_requests(
                    accounts_db_caching_enabled,
                    test_hash_calculation,
                    use_index_hash_calculation,
                    non_snapshot_time_us,
                )
            })
    }

    /// `is_from_abs` is true if the caller is the AccountsBackgroundService
    pub fn handle_pruned_banks(&self, bank: &Bank, is_from_abs: bool) -> usize {
        let mut count = 0;
        for (pruned_slot, pruned_bank_id) in self.pruned_banks_receiver.try_iter() {
            count += 1;
            bank.rc
                .accounts
                .purge_slot(pruned_slot, pruned_bank_id, is_from_abs);
        }

        count
    }
}

pub struct AccountsBackgroundService {
    t_background: JoinHandle<()>,
}

impl AccountsBackgroundService {
    pub fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        exit: &Arc<AtomicBool>,
        request_handler: AbsRequestHandler,
        accounts_db_caching_enabled: bool,
        test_hash_calculation: bool,
        use_index_hash_calculation: bool,
    ) -> Self {
        info!("AccountsBackgroundService active");
        let exit = exit.clone();
        let mut consumed_budget = 0;
        let mut last_cleaned_block_height = 0;
        let mut removed_slots_count = 0;
        let mut total_remove_slots_time = 0;
        let mut last_expiration_check_time = Instant::now();
        let t_background = Builder::new()
            .name("solana-bg-accounts".to_string())
            .spawn(move || {
                let mut last_snapshot_end_time = None;
                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }

                    // Grab the current root bank
                    let bank = bank_forks.read().unwrap().root_bank().clone();

                    // Purge accounts of any dead slots
                    Self::remove_dead_slots(
                        &bank,
                        &request_handler,
                        &mut removed_slots_count,
                        &mut total_remove_slots_time,
                    );

                    Self::expire_old_recycle_stores(&bank, &mut last_expiration_check_time);

                    let non_snapshot_time = last_snapshot_end_time
                        .map(|last_snapshot_end_time: Instant| {
                            last_snapshot_end_time.elapsed().as_micros()
                        })
                        .unwrap_or_default();

                    // Check to see if there were any requests for snapshotting banks
                    // < the current root bank `bank` above.

                    // Claim: Any snapshot request for slot `N` found here implies that the last cleanup
                    // slot `M` satisfies `M < N`
                    //
                    // Proof: Assume for contradiction that we find a snapshot request for slot `N` here,
                    // but cleanup has already happened on some slot `M >= N`. Because the call to
                    // `bank.clean_accounts(true)` (in the code below) implies we only clean slots `<= bank - 1`,
                    // then that means in some *previous* iteration of this loop, we must have gotten a root
                    // bank for slot some slot `R` where `R > N`, but did not see the snapshot for `N` in the
                    // snapshot request channel.
                    //
                    // However, this is impossible because BankForks.set_root() will always flush the snapshot
                    // request for `N` to the snapshot request channel before setting a root `R > N`, and
                    // snapshot_request_handler.handle_requests() will always look for the latest
                    // available snapshot in the channel.
                    let snapshot_block_height = request_handler.handle_snapshot_requests(
                        accounts_db_caching_enabled,
                        test_hash_calculation,
                        use_index_hash_calculation,
                        non_snapshot_time,
                    );
                    if snapshot_block_height.is_some() {
                        last_snapshot_end_time = Some(Instant::now());
                    }

                    if accounts_db_caching_enabled {
                        // Note that the flush will do an internal clean of the
                        // cache up to bank.slot(), so should be safe as long
                        // as any later snapshots that are taken are of
                        // slots >= bank.slot()
                        bank.flush_accounts_cache_if_needed();
                    }

                    if let Some(snapshot_block_height) = snapshot_block_height {
                        // Safe, see proof above
                        assert!(last_cleaned_block_height <= snapshot_block_height);
                        last_cleaned_block_height = snapshot_block_height;
                    } else {
                        if accounts_db_caching_enabled {
                            bank.shrink_candidate_slots();
                        } else {
                            // under sustained writes, shrink can lag behind so cap to
                            // SHRUNKEN_ACCOUNT_PER_INTERVAL (which is based on INTERVAL_MS,
                            // which in turn roughly associated block time)
                            consumed_budget = bank
                                .process_stale_slot_with_budget(
                                    consumed_budget,
                                    SHRUNKEN_ACCOUNT_PER_INTERVAL,
                                )
                                .min(SHRUNKEN_ACCOUNT_PER_INTERVAL);
                        }
                        if bank.block_height() - last_cleaned_block_height
                            > (CLEAN_INTERVAL_BLOCKS + thread_rng().gen_range(0, 10))
                        {
                            if accounts_db_caching_enabled {
                                // Note that the flush will do an internal clean of the
                                // cache up to bank.slot(), so should be safe as long
                                // as any later snapshots that are taken are of
                                // slots >= bank.slot()
                                bank.force_flush_accounts_cache();
                            }
                            // bprumo TODO: get and use real last_full_snapshot_slot
                            bank.clean_accounts(true, false, None);
                            last_cleaned_block_height = bank.block_height();
                        }
                    }
                    sleep(Duration::from_millis(INTERVAL_MS));
                }
            })
            .unwrap();
        Self { t_background }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_background.join()
    }

    fn remove_dead_slots(
        bank: &Bank,
        request_handler: &AbsRequestHandler,
        removed_slots_count: &mut usize,
        total_remove_slots_time: &mut u64,
    ) {
        let mut remove_slots_time = Measure::start("remove_slots_time");
        *removed_slots_count += request_handler.handle_pruned_banks(bank, true);
        remove_slots_time.stop();
        *total_remove_slots_time += remove_slots_time.as_us();

        if *removed_slots_count >= 100 {
            datapoint_info!(
                "remove_slots_timing",
                ("remove_slots_time", *total_remove_slots_time, i64),
                ("removed_slots_count", *removed_slots_count, i64),
            );
            *total_remove_slots_time = 0;
            *removed_slots_count = 0;
        }
    }

    fn expire_old_recycle_stores(bank: &Bank, last_expiration_check_time: &mut Instant) {
        let now = Instant::now();
        if now.duration_since(*last_expiration_check_time).as_secs()
            > RECYCLE_STORE_EXPIRATION_INTERVAL_SECS
        {
            bank.expire_old_recycle_stores();
            *last_expiration_check_time = now;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::genesis_utils::create_genesis_config;
    use crossbeam_channel::unbounded;
    use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};

    #[test]
    fn test_accounts_background_service_remove_dead_slots() {
        let genesis = create_genesis_config(10);
        let bank0 = Arc::new(Bank::new(&genesis.genesis_config));
        let (pruned_banks_sender, pruned_banks_receiver) = unbounded();
        let request_handler = AbsRequestHandler {
            snapshot_request_handler: None,
            pruned_banks_receiver,
        };

        // Store an account in slot 0
        let account_key = Pubkey::new_unique();
        bank0.store_account(
            &account_key,
            &AccountSharedData::new(264, 0, &Pubkey::default()),
        );
        assert!(bank0.get_account(&account_key).is_some());
        pruned_banks_sender.send((0, 0)).unwrap();

        assert!(!bank0.rc.accounts.scan_slot(0, |_| Some(())).is_empty());

        AccountsBackgroundService::remove_dead_slots(&bank0, &request_handler, &mut 0, &mut 0);

        assert!(bank0.rc.accounts.scan_slot(0, |_| Some(())).is_empty());
    }
}
