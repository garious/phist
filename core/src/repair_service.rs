//! The `repair_service` module implements the tools necessary to generate a thread which
//! regularly finds missing shreds in the ledger and sends repair requests for those shreds
use crate::{
    ancestor_hashes_service::AncestorHashesService,
    cluster_info_vote_listener::VerifiedVoteReceiver,
    cluster_slots::ClusterSlots,
    duplicate_repair_status::DuplicateSlotRepairStatus,
    outstanding_requests::OutstandingRequests,
    repair_weight::RepairWeight,
    result::Result,
    serve_repair::{ServeRepair, ShredRepairType, REPAIR_PEERS_CACHE_CAPACITY},
};
use crossbeam_channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender};
use lru::LruCache;
use solana_gossip::cluster_info::ClusterInfo;
use solana_ledger::{
    blockstore::{Blockstore, SlotMeta},
    shred::Nonce,
};
use solana_measure::measure::Measure;
use solana_runtime::{bank_forks::BankForks, contains::Contains};
use solana_sdk::{
    clock::Slot, epoch_schedule::EpochSchedule, hash::Hash, pubkey::Pubkey, timing::timestamp,
};
use std::{
    collections::{HashMap, HashSet},
    iter::Iterator,
    net::SocketAddr,
    net::UdpSocket,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, RwLock},
    thread::sleep,
    thread::{self, Builder, JoinHandle},
    time::{Duration, Instant},
};

pub type DuplicateSlotsResetSender = CrossbeamSender<Vec<(Slot, Hash)>>;
pub type DuplicateSlotsResetReceiver = CrossbeamReceiver<Vec<(Slot, Hash)>>;
pub type ConfirmedSlotsSender = CrossbeamSender<Vec<Slot>>;
pub type ConfirmedSlotsReceiver = CrossbeamReceiver<Vec<Slot>>;
pub type OutstandingShredRepairs = OutstandingRequests<ShredRepairType>;

#[derive(Default, Debug)]
pub struct SlotRepairs {
    highest_shred_index: u64,
    // map from pubkey to total number of requests
    pubkey_repairs: HashMap<Pubkey, u64>,
}

impl SlotRepairs {
    pub fn pubkey_repairs(&self) -> &HashMap<Pubkey, u64> {
        &self.pubkey_repairs
    }
}

#[derive(Default, Debug)]
pub struct RepairStatsGroup {
    pub count: u64,
    pub min: u64,
    pub max: u64,
    pub slot_pubkeys: HashMap<Slot, SlotRepairs>,
}

impl RepairStatsGroup {
    pub fn update(&mut self, repair_peer_id: &Pubkey, slot: Slot, shred_index: u64) {
        self.count += 1;
        let slot_repairs = self.slot_pubkeys.entry(slot).or_default();
        // Increment total number of repairs of this type for this pubkey by 1
        *slot_repairs
            .pubkey_repairs
            .entry(*repair_peer_id)
            .or_default() += 1;
        // Update the max requested shred index for this slot
        slot_repairs.highest_shred_index =
            std::cmp::max(slot_repairs.highest_shred_index, shred_index);
        self.min = std::cmp::min(self.min, slot);
        self.max = std::cmp::max(self.max, slot);
    }
}

#[derive(Default, Debug)]
pub struct RepairStats {
    pub shred: RepairStatsGroup,
    pub highest_shred: RepairStatsGroup,
    pub orphan: RepairStatsGroup,
    pub get_best_orphans_us: u64,
    pub get_best_shreds_us: u64,
}

#[derive(Default, Debug)]
pub struct RepairTiming {
    pub set_root_elapsed: u64,
    pub get_votes_elapsed: u64,
    pub add_votes_elapsed: u64,
    pub get_best_orphans_elapsed: u64,
    pub get_best_shreds_elapsed: u64,
    pub send_repairs_elapsed: u64,
}

impl RepairTiming {
    fn update(
        &mut self,
        set_root_elapsed: u64,
        get_votes_elapsed: u64,
        add_votes_elapsed: u64,
        send_repairs_elapsed: u64,
    ) {
        self.set_root_elapsed += set_root_elapsed;
        self.get_votes_elapsed += get_votes_elapsed;
        self.add_votes_elapsed += add_votes_elapsed;
        self.send_repairs_elapsed += send_repairs_elapsed;
    }
}

pub const MAX_REPAIR_LENGTH: usize = 512;
pub const MAX_REPAIR_PER_DUPLICATE: usize = 20;
pub const MAX_DUPLICATE_WAIT_MS: usize = 10_000;
pub const REPAIR_MS: u64 = 100;
pub const MAX_ORPHANS: usize = 5;

#[derive(Clone)]
pub struct RepairInfo {
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub cluster_info: Arc<ClusterInfo>,
    pub cluster_slots: Arc<ClusterSlots>,
    pub epoch_schedule: EpochSchedule,
    pub duplicate_slots_reset_sender: DuplicateSlotsResetSender,
    pub repair_validators: Option<HashSet<Pubkey>>,
}

pub struct RepairSlotRange {
    pub start: Slot,
    pub end: Slot,
}

impl Default for RepairSlotRange {
    fn default() -> Self {
        RepairSlotRange {
            start: 0,
            end: std::u64::MAX,
        }
    }
}

pub struct RepairService {
    t_repair: JoinHandle<()>,
    ancestor_hashes_service: AncestorHashesService,
}

impl RepairService {
    pub fn new(
        blockstore: Arc<Blockstore>,
        exit: Arc<AtomicBool>,
        repair_socket: Arc<UdpSocket>,
        repair_info: RepairInfo,
        verified_vote_receiver: VerifiedVoteReceiver,
        outstanding_requests: Arc<RwLock<OutstandingShredRepairs>>,
    ) -> Self {
        let t_repair = {
            let blockstore = blockstore.clone();
            let exit = exit.clone();
            let repair_info = repair_info.clone();
            Builder::new()
                .name("solana-repair-service".to_string())
                .spawn(move || {
                    Self::run(
                        &blockstore,
                        &exit,
                        &repair_socket,
                        repair_info,
                        verified_vote_receiver,
                        &outstanding_requests,
                    )
                })
                .unwrap()
        };

        let ancestor_hashes_request_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").unwrap());
        let ancestor_hashes_service = AncestorHashesService::new(
            exit,
            blockstore,
            ancestor_hashes_request_socket,
            repair_info,
        );

        RepairService {
            t_repair,
            ancestor_hashes_service,
        }
    }

    fn run(
        blockstore: &Blockstore,
        exit: &AtomicBool,
        repair_socket: &UdpSocket,
        repair_info: RepairInfo,
        verified_vote_receiver: VerifiedVoteReceiver,
        outstanding_requests: &RwLock<OutstandingShredRepairs>,
    ) {
        let mut repair_weight = RepairWeight::new(repair_info.bank_forks.read().unwrap().root());
        let serve_repair = ServeRepair::new(repair_info.cluster_info.clone());
        let id = repair_info.cluster_info.id();
        let mut repair_stats = RepairStats::default();
        let mut repair_timing = RepairTiming::default();
        let mut last_stats = Instant::now();
        let duplicate_slot_repair_statuses: HashMap<Slot, DuplicateSlotRepairStatus> =
            HashMap::new();
        let mut peers_cache = LruCache::new(REPAIR_PEERS_CACHE_CAPACITY);

        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            let mut set_root_elapsed;
            let mut get_votes_elapsed;
            let mut add_votes_elapsed;

            let repairs = {
                let root_bank = repair_info.bank_forks.read().unwrap().root_bank().clone();
                let new_root = root_bank.slot();

                // Purge outdated slots from the weighting heuristic
                set_root_elapsed = Measure::start("set_root_elapsed");
                repair_weight.set_root(new_root);
                set_root_elapsed.stop();

                // Add new votes to the weighting heuristic
                get_votes_elapsed = Measure::start("get_votes_elapsed");
                let mut slot_to_vote_pubkeys: HashMap<Slot, Vec<Pubkey>> = HashMap::new();
                verified_vote_receiver
                    .try_iter()
                    .for_each(|(vote_pubkey, vote_slots)| {
                        for slot in vote_slots {
                            slot_to_vote_pubkeys
                                .entry(slot)
                                .or_default()
                                .push(vote_pubkey);
                        }
                    });
                get_votes_elapsed.stop();

                add_votes_elapsed = Measure::start("add_votes");
                repair_weight.add_votes(
                    blockstore,
                    slot_to_vote_pubkeys.into_iter(),
                    root_bank.epoch_stakes_map(),
                    root_bank.epoch_schedule(),
                );
                add_votes_elapsed.stop();

                repair_weight.get_best_weighted_repairs(
                    blockstore,
                    root_bank.epoch_stakes_map(),
                    root_bank.epoch_schedule(),
                    MAX_ORPHANS,
                    MAX_REPAIR_LENGTH,
                    &duplicate_slot_repair_statuses,
                    Some(&mut repair_timing),
                )
            };

            let mut send_repairs_elapsed = Measure::start("send_repairs_elapsed");
            let mut outstanding_requests = outstanding_requests.write().unwrap();
            repairs.into_iter().for_each(|repair_request| {
                if let Ok((to, req)) = serve_repair.repair_request(
                    &repair_info.cluster_slots,
                    repair_request,
                    &mut peers_cache,
                    &mut repair_stats,
                    &repair_info.repair_validators,
                    &mut outstanding_requests,
                ) {
                    repair_socket.send_to(&req, to).unwrap_or_else(|e| {
                        info!("{} repair req send_to({}) error {:?}", id, to, e);
                        0
                    });
                }
            });
            send_repairs_elapsed.stop();
            repair_timing.update(
                set_root_elapsed.as_us(),
                get_votes_elapsed.as_us(),
                add_votes_elapsed.as_us(),
                send_repairs_elapsed.as_us(),
            );

            if last_stats.elapsed().as_secs() > 2 {
                let repair_total = repair_stats.shred.count
                    + repair_stats.highest_shred.count
                    + repair_stats.orphan.count;
                let slot_to_count: Vec<_> = repair_stats
                    .shred
                    .slot_pubkeys
                    .iter()
                    .chain(repair_stats.highest_shred.slot_pubkeys.iter())
                    .chain(repair_stats.orphan.slot_pubkeys.iter())
                    .map(|(slot, slot_repairs)| {
                        (
                            slot,
                            slot_repairs
                                .pubkey_repairs
                                .iter()
                                .map(|(_key, count)| count)
                                .sum::<u64>(),
                        )
                    })
                    .collect();
                info!("repair_stats: {:?}", slot_to_count);
                if repair_total > 0 {
                    datapoint_info!(
                        "serve_repair-repair",
                        ("repair-total", repair_total, i64),
                        ("shred-count", repair_stats.shred.count, i64),
                        ("highest-shred-count", repair_stats.highest_shred.count, i64),
                        ("orphan-count", repair_stats.orphan.count, i64),
                        ("repair-highest-slot", repair_stats.highest_shred.max, i64),
                        ("repair-orphan", repair_stats.orphan.max, i64),
                    );
                }
                datapoint_info!(
                    "serve_repair-repair-timing",
                    ("set-root-elapsed", repair_timing.set_root_elapsed, i64),
                    ("get-votes-elapsed", repair_timing.get_votes_elapsed, i64),
                    ("add-votes-elapsed", repair_timing.add_votes_elapsed, i64),
                    (
                        "get-best-orphans-elapsed",
                        repair_timing.get_best_orphans_elapsed,
                        i64
                    ),
                    (
                        "get-best-shreds-elapsed",
                        repair_timing.get_best_shreds_elapsed,
                        i64
                    ),
                    (
                        "send-repairs-elapsed",
                        repair_timing.send_repairs_elapsed,
                        i64
                    ),
                );
                repair_stats = RepairStats::default();
                repair_timing = RepairTiming::default();
                last_stats = Instant::now();
            }
            sleep(Duration::from_millis(REPAIR_MS));
        }
    }

    // Generate repairs for all slots `x` in the repair_range.start <= x <= repair_range.end
    pub fn generate_repairs_in_range(
        blockstore: &Blockstore,
        max_repairs: usize,
        repair_range: &RepairSlotRange,
    ) -> Result<Vec<ShredRepairType>> {
        // Slot height and shred indexes for shreds we want to repair
        let mut repairs: Vec<ShredRepairType> = vec![];
        for slot in repair_range.start..=repair_range.end {
            if repairs.len() >= max_repairs {
                break;
            }

            let meta = blockstore
                .meta(slot)
                .expect("Unable to lookup slot meta")
                .unwrap_or(SlotMeta {
                    slot,
                    ..SlotMeta::default()
                });

            let new_repairs = Self::generate_repairs_for_slot(
                blockstore,
                slot,
                &meta,
                max_repairs - repairs.len(),
            );
            repairs.extend(new_repairs);
        }

        Ok(repairs)
    }

    pub fn generate_repairs_for_slot(
        blockstore: &Blockstore,
        slot: Slot,
        slot_meta: &SlotMeta,
        max_repairs: usize,
    ) -> Vec<ShredRepairType> {
        if max_repairs == 0 || slot_meta.is_full() {
            vec![]
        } else if slot_meta.consumed == slot_meta.received {
            vec![ShredRepairType::HighestShred(slot, slot_meta.received)]
        } else {
            let reqs = blockstore.find_missing_data_indexes(
                slot,
                slot_meta.first_shred_timestamp,
                slot_meta.consumed,
                slot_meta.received,
                max_repairs,
            );
            reqs.into_iter()
                .map(|i| ShredRepairType::Shred(slot, i))
                .collect()
        }
    }

    /// Repairs any fork starting at the input slot
    pub fn generate_repairs_for_fork<'a>(
        blockstore: &Blockstore,
        repairs: &mut Vec<ShredRepairType>,
        max_repairs: usize,
        slot: Slot,
        duplicate_slot_repair_statuses: &impl Contains<'a, Slot>,
    ) {
        let mut pending_slots = vec![slot];
        while repairs.len() < max_repairs && !pending_slots.is_empty() {
            let slot = pending_slots.pop().unwrap();
            if duplicate_slot_repair_statuses.contains(&slot) {
                // These are repaired through a different path
                continue;
            }
            if let Some(slot_meta) = blockstore.meta(slot).unwrap() {
                let new_repairs = Self::generate_repairs_for_slot(
                    blockstore,
                    slot,
                    &slot_meta,
                    max_repairs - repairs.len(),
                );
                repairs.extend(new_repairs);
                let next_slots = slot_meta.next_slots;
                pending_slots.extend(next_slots);
            } else {
                break;
            }
        }
    }

    #[allow(dead_code)]
    fn generate_duplicate_repairs_for_slot(
        blockstore: &Blockstore,
        slot: Slot,
    ) -> Option<Vec<ShredRepairType>> {
        if let Some(slot_meta) = blockstore.meta(slot).unwrap() {
            if slot_meta.is_full() {
                // If the slot is full, no further need to repair this slot
                None
            } else {
                Some(Self::generate_repairs_for_slot(
                    blockstore,
                    slot,
                    &slot_meta,
                    MAX_REPAIR_PER_DUPLICATE,
                ))
            }
        } else {
            error!("Slot meta for duplicate slot does not exist, cannot generate repairs");
            // Filter out this slot from the set of duplicates to be repaired as
            // the SlotMeta has to exist for duplicates to be generated
            None
        }
    }

    #[allow(dead_code)]
    fn generate_and_send_duplicate_repairs(
        duplicate_slot_repair_statuses: &mut HashMap<Slot, DuplicateSlotRepairStatus>,
        cluster_slots: &ClusterSlots,
        blockstore: &Blockstore,
        serve_repair: &ServeRepair,
        repair_stats: &mut RepairStats,
        repair_socket: &UdpSocket,
        repair_validators: &Option<HashSet<Pubkey>>,
        outstanding_requests: &RwLock<OutstandingShredRepairs>,
    ) {
        duplicate_slot_repair_statuses.retain(|slot, status| {
            Self::update_duplicate_slot_repair_addr(
                *slot,
                status,
                cluster_slots,
                serve_repair,
                repair_validators,
            );
            if let Some((repair_pubkey, repair_addr)) = status.repair_pubkey_and_addr {
                let repairs = Self::generate_duplicate_repairs_for_slot(blockstore, *slot);

                if let Some(repairs) = repairs {
                    let mut outstanding_requests = outstanding_requests.write().unwrap();
                    for repair_type in repairs {
                        let nonce = outstanding_requests.add_request(repair_type, timestamp());
                        if let Err(e) = Self::serialize_and_send_request(
                            &repair_type,
                            repair_socket,
                            &repair_pubkey,
                            &repair_addr,
                            serve_repair,
                            repair_stats,
                            nonce,
                        ) {
                            info!(
                                "repair req send_to {} ({}) error {:?}",
                                repair_pubkey, repair_addr, e
                            );
                        }
                    }
                    true
                } else {
                    false
                }
            } else {
                true
            }
        })
    }

    #[allow(dead_code)]
    fn serialize_and_send_request(
        repair_type: &ShredRepairType,
        repair_socket: &UdpSocket,
        repair_pubkey: &Pubkey,
        to: &SocketAddr,
        serve_repair: &ServeRepair,
        repair_stats: &mut RepairStats,
        nonce: Nonce,
    ) -> Result<()> {
        let req =
            serve_repair.map_repair_request(repair_type, repair_pubkey, repair_stats, nonce)?;
        repair_socket.send_to(&req, to)?;
        Ok(())
    }

    #[allow(dead_code)]
    fn update_duplicate_slot_repair_addr(
        slot: Slot,
        status: &mut DuplicateSlotRepairStatus,
        cluster_slots: &ClusterSlots,
        serve_repair: &ServeRepair,
        repair_validators: &Option<HashSet<Pubkey>>,
    ) {
        let now = timestamp();
        if status.repair_pubkey_and_addr.is_none()
            || now.saturating_sub(status.start_ts) >= MAX_DUPLICATE_WAIT_MS as u64
        {
            let repair_pubkey_and_addr = serve_repair.repair_request_duplicate_compute_best_peer(
                slot,
                cluster_slots,
                repair_validators,
            );
            status.repair_pubkey_and_addr = repair_pubkey_and_addr.ok();
            status.start_ts = timestamp();
        }
    }

    #[allow(dead_code)]
    fn initiate_repair_for_duplicate_slot(
        slot: Slot,
        duplicate_slot_repair_statuses: &mut HashMap<Slot, DuplicateSlotRepairStatus>,
        cluster_slots: &ClusterSlots,
        serve_repair: &ServeRepair,
        repair_validators: &Option<HashSet<Pubkey>>,
    ) {
        // If we're already in the middle of repairing this, ignore the signal.
        if duplicate_slot_repair_statuses.contains_key(&slot) {
            return;
        }
        // Mark this slot as special repair, try to download from single
        // validator to avoid corruption
        let repair_pubkey_and_addr = serve_repair
            .repair_request_duplicate_compute_best_peer(slot, cluster_slots, repair_validators)
            .ok();
        let new_duplicate_slot_repair_status = DuplicateSlotRepairStatus {
            correct_ancestors_to_repair: vec![(slot, Hash::default())],
            repair_pubkey_and_addr,
            start_ts: timestamp(),
        };
        duplicate_slot_repair_statuses.insert(slot, new_duplicate_slot_repair_status);
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_repair.join()?;
        self.ancestor_hashes_service.join()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use solana_gossip::cluster_info::Node;
    use solana_ledger::blockstore::{
        make_chaining_slot_entries, make_many_slot_entries, make_slot_entries,
    };
    use solana_ledger::shred::max_ticks_per_n_shreds;
    use solana_ledger::{blockstore::Blockstore, get_tmp_ledger_path};
    use std::collections::HashSet;

    #[test]
    pub fn test_repair_orphan() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            // Create some orphan slots
            let (mut shreds, _) = make_slot_entries(1, 0, 1);
            let (shreds2, _) = make_slot_entries(5, 2, 1);
            shreds.extend(shreds2);
            blockstore.insert_shreds(shreds, None, false).unwrap();
            let mut repair_weight = RepairWeight::new(0);
            assert_eq!(
                repair_weight.get_best_weighted_repairs(
                    &blockstore,
                    &HashMap::new(),
                    &EpochSchedule::default(),
                    MAX_ORPHANS,
                    MAX_REPAIR_LENGTH,
                    &HashSet::default(),
                    None,
                ),
                vec![
                    ShredRepairType::Orphan(2),
                    ShredRepairType::HighestShred(0, 0)
                ]
            );
        }

        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_empty_slot() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let (shreds, _) = make_slot_entries(2, 0, 1);

            // Write this shred to slot 2, should chain to slot 0, which we haven't received
            // any shreds for
            blockstore.insert_shreds(shreds, None, false).unwrap();
            let mut repair_weight = RepairWeight::new(0);

            // Check that repair tries to patch the empty slot
            assert_eq!(
                repair_weight.get_best_weighted_repairs(
                    &blockstore,
                    &HashMap::new(),
                    &EpochSchedule::default(),
                    MAX_ORPHANS,
                    MAX_REPAIR_LENGTH,
                    &HashSet::default(),
                    None
                ),
                vec![ShredRepairType::HighestShred(0, 0)]
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_generate_repairs() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let nth = 3;
            let num_slots = 2;

            // Create some shreds
            let (mut shreds, _) = make_many_slot_entries(0, num_slots as u64, 150);
            let num_shreds = shreds.len() as u64;
            let num_shreds_per_slot = num_shreds / num_slots;

            // write every nth shred
            let mut shreds_to_write = vec![];
            let mut missing_indexes_per_slot = vec![];
            for i in (0..num_shreds).rev() {
                let index = i % num_shreds_per_slot;
                if index % nth == 0 {
                    shreds_to_write.insert(0, shreds.remove(i as usize));
                } else if i < num_shreds_per_slot {
                    missing_indexes_per_slot.insert(0, index);
                }
            }
            blockstore
                .insert_shreds(shreds_to_write, None, false)
                .unwrap();
            // sleep so that the holes are ready for repair
            sleep(Duration::from_secs(1));
            let expected: Vec<ShredRepairType> = (0..num_slots)
                .flat_map(|slot| {
                    missing_indexes_per_slot
                        .iter()
                        .map(move |shred_index| ShredRepairType::Shred(slot as u64, *shred_index))
                })
                .collect();

            let mut repair_weight = RepairWeight::new(0);
            assert_eq!(
                repair_weight.get_best_weighted_repairs(
                    &blockstore,
                    &HashMap::new(),
                    &EpochSchedule::default(),
                    MAX_ORPHANS,
                    MAX_REPAIR_LENGTH,
                    &HashSet::default(),
                    None
                ),
                expected
            );

            assert_eq!(
                repair_weight.get_best_weighted_repairs(
                    &blockstore,
                    &HashMap::new(),
                    &EpochSchedule::default(),
                    MAX_ORPHANS,
                    expected.len() - 2,
                    &HashSet::default(),
                    None
                )[..],
                expected[0..expected.len() - 2]
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_generate_highest_repair() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let num_entries_per_slot = 100;

            // Create some shreds
            let (mut shreds, _) = make_slot_entries(0, 0, num_entries_per_slot as u64);
            let num_shreds_per_slot = shreds.len() as u64;

            // Remove last shred (which is also last in slot) so that slot is not complete
            shreds.pop();

            blockstore.insert_shreds(shreds, None, false).unwrap();

            // We didn't get the last shred for this slot, so ask for the highest shred for that slot
            let expected: Vec<ShredRepairType> =
                vec![ShredRepairType::HighestShred(0, num_shreds_per_slot - 1)];

            let mut repair_weight = RepairWeight::new(0);
            assert_eq!(
                repair_weight.get_best_weighted_repairs(
                    &blockstore,
                    &HashMap::new(),
                    &EpochSchedule::default(),
                    MAX_ORPHANS,
                    MAX_REPAIR_LENGTH,
                    &HashSet::default(),
                    None
                ),
                expected
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_range() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let slots: Vec<u64> = vec![1, 3, 5, 7, 8];
            let num_entries_per_slot = max_ticks_per_n_shreds(1, None) + 1;

            let shreds = make_chaining_slot_entries(&slots, num_entries_per_slot);
            for (mut slot_shreds, _) in shreds.into_iter() {
                slot_shreds.remove(0);
                blockstore.insert_shreds(slot_shreds, None, false).unwrap();
            }
            // sleep to make slot eligible for repair
            sleep(Duration::from_secs(1));
            // Iterate through all possible combinations of start..end (inclusive on both
            // sides of the range)
            for start in 0..slots.len() {
                for end in start..slots.len() {
                    let repair_slot_range = RepairSlotRange {
                        start: slots[start],
                        end: slots[end],
                    };
                    let expected: Vec<ShredRepairType> = (repair_slot_range.start
                        ..=repair_slot_range.end)
                        .map(|slot_index| {
                            if slots.contains(&(slot_index as u64)) {
                                ShredRepairType::Shred(slot_index as u64, 0)
                            } else {
                                ShredRepairType::HighestShred(slot_index as u64, 0)
                            }
                        })
                        .collect();

                    assert_eq!(
                        RepairService::generate_repairs_in_range(
                            &blockstore,
                            std::usize::MAX,
                            &repair_slot_range,
                        )
                        .unwrap(),
                        expected
                    );
                }
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_range_highest() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let num_entries_per_slot = 10;

            let num_slots = 1;
            let start = 5;

            // Create some shreds in slots 0..num_slots
            for i in start..start + num_slots {
                let parent = if i > 0 { i - 1 } else { 0 };
                let (shreds, _) = make_slot_entries(i, parent, num_entries_per_slot as u64);

                blockstore.insert_shreds(shreds, None, false).unwrap();
            }

            let end = 4;
            let expected: Vec<ShredRepairType> = vec![
                ShredRepairType::HighestShred(end - 2, 0),
                ShredRepairType::HighestShred(end - 1, 0),
                ShredRepairType::HighestShred(end, 0),
            ];

            let repair_slot_range = RepairSlotRange { start: 2, end };

            assert_eq!(
                RepairService::generate_repairs_in_range(
                    &blockstore,
                    std::usize::MAX,
                    &repair_slot_range,
                )
                .unwrap(),
                expected
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_generate_duplicate_repairs_for_slot() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();
        let dead_slot = 9;

        // SlotMeta doesn't exist, should make no repairs
        assert!(
            RepairService::generate_duplicate_repairs_for_slot(&blockstore, dead_slot,).is_none()
        );

        // Insert some shreds to create a SlotMeta, should make repairs
        let num_entries_per_slot = max_ticks_per_n_shreds(1, None) + 1;
        let (mut shreds, _) = make_slot_entries(dead_slot, dead_slot - 1, num_entries_per_slot);
        blockstore
            .insert_shreds(shreds[..shreds.len() - 1].to_vec(), None, false)
            .unwrap();
        assert!(
            RepairService::generate_duplicate_repairs_for_slot(&blockstore, dead_slot,).is_some()
        );

        // SlotMeta is full, should make no repairs
        blockstore
            .insert_shreds(vec![shreds.pop().unwrap()], None, false)
            .unwrap();
        assert!(
            RepairService::generate_duplicate_repairs_for_slot(&blockstore, dead_slot,).is_none()
        );
    }

    #[test]
    pub fn test_generate_and_send_duplicate_repairs() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();
        let cluster_slots = ClusterSlots::default();
        let serve_repair = ServeRepair::new_with_invalid_keypair(Node::new_localhost().info);
        let mut duplicate_slot_repair_statuses = HashMap::new();
        let dead_slot = 9;
        let receive_socket = &UdpSocket::bind("0.0.0.0:0").unwrap();
        let duplicate_status = DuplicateSlotRepairStatus {
            correct_ancestors_to_repair: vec![(dead_slot, Hash::default())],
            start_ts: std::u64::MAX,
            repair_pubkey_and_addr: None,
        };

        // Insert some shreds to create a SlotMeta,
        let num_entries_per_slot = max_ticks_per_n_shreds(1, None) + 1;
        let (mut shreds, _) = make_slot_entries(dead_slot, dead_slot - 1, num_entries_per_slot);
        blockstore
            .insert_shreds(shreds[..shreds.len() - 1].to_vec(), None, false)
            .unwrap();

        duplicate_slot_repair_statuses.insert(dead_slot, duplicate_status);

        // There is no repair_addr, so should not get filtered because the timeout
        // `std::u64::MAX` has not expired
        RepairService::generate_and_send_duplicate_repairs(
            &mut duplicate_slot_repair_statuses,
            &cluster_slots,
            &blockstore,
            &serve_repair,
            &mut RepairStats::default(),
            &UdpSocket::bind("0.0.0.0:0").unwrap(),
            &None,
            &RwLock::new(OutstandingRequests::default()),
        );
        assert!(duplicate_slot_repair_statuses
            .get(&dead_slot)
            .unwrap()
            .repair_pubkey_and_addr
            .is_none());
        assert!(duplicate_slot_repair_statuses.get(&dead_slot).is_some());

        // Give the slot a repair address
        duplicate_slot_repair_statuses
            .get_mut(&dead_slot)
            .unwrap()
            .repair_pubkey_and_addr =
            Some((Pubkey::default(), receive_socket.local_addr().unwrap()));

        // Slot is not yet full, should not get filtered from `duplicate_slot_repair_statuses`
        RepairService::generate_and_send_duplicate_repairs(
            &mut duplicate_slot_repair_statuses,
            &cluster_slots,
            &blockstore,
            &serve_repair,
            &mut RepairStats::default(),
            &UdpSocket::bind("0.0.0.0:0").unwrap(),
            &None,
            &RwLock::new(OutstandingRequests::default()),
        );
        assert_eq!(duplicate_slot_repair_statuses.len(), 1);
        assert!(duplicate_slot_repair_statuses.get(&dead_slot).is_some());

        // Insert rest of shreds. Slot is full, should get filtered from
        // `duplicate_slot_repair_statuses`
        blockstore
            .insert_shreds(vec![shreds.pop().unwrap()], None, false)
            .unwrap();
        RepairService::generate_and_send_duplicate_repairs(
            &mut duplicate_slot_repair_statuses,
            &cluster_slots,
            &blockstore,
            &serve_repair,
            &mut RepairStats::default(),
            &UdpSocket::bind("0.0.0.0:0").unwrap(),
            &None,
            &RwLock::new(OutstandingRequests::default()),
        );
        assert!(duplicate_slot_repair_statuses.is_empty());
    }

    #[test]
    pub fn test_update_duplicate_slot_repair_addr() {
        let dummy_addr = Some((
            Pubkey::default(),
            UdpSocket::bind("0.0.0.0:0").unwrap().local_addr().unwrap(),
        ));
        let cluster_info = Arc::new(ClusterInfo::new_with_invalid_keypair(
            Node::new_localhost().info,
        ));
        let serve_repair = ServeRepair::new(cluster_info.clone());
        let valid_repair_peer = Node::new_localhost().info;

        // Signal that this peer has confirmed the dead slot, and is thus
        // a valid target for repair
        let dead_slot = 9;
        let cluster_slots = ClusterSlots::default();
        cluster_slots.insert_node_id(dead_slot, valid_repair_peer.id);
        cluster_info.insert_info(valid_repair_peer);

        // Not enough time has passed, should not update the
        // address
        let mut duplicate_status = DuplicateSlotRepairStatus {
            correct_ancestors_to_repair: vec![(dead_slot, Hash::default())],
            start_ts: std::u64::MAX,
            repair_pubkey_and_addr: dummy_addr,
        };
        RepairService::update_duplicate_slot_repair_addr(
            dead_slot,
            &mut duplicate_status,
            &cluster_slots,
            &serve_repair,
            &None,
        );
        assert_eq!(duplicate_status.repair_pubkey_and_addr, dummy_addr);

        // If the repair address is None, should try to update
        let mut duplicate_status = DuplicateSlotRepairStatus {
            correct_ancestors_to_repair: vec![(dead_slot, Hash::default())],
            start_ts: std::u64::MAX,
            repair_pubkey_and_addr: None,
        };
        RepairService::update_duplicate_slot_repair_addr(
            dead_slot,
            &mut duplicate_status,
            &cluster_slots,
            &serve_repair,
            &None,
        );
        assert!(duplicate_status.repair_pubkey_and_addr.is_some());

        // If sufficient time has passed, should try to update
        let mut duplicate_status = DuplicateSlotRepairStatus {
            correct_ancestors_to_repair: vec![(dead_slot, Hash::default())],
            start_ts: timestamp() - MAX_DUPLICATE_WAIT_MS as u64,
            repair_pubkey_and_addr: dummy_addr,
        };
        RepairService::update_duplicate_slot_repair_addr(
            dead_slot,
            &mut duplicate_status,
            &cluster_slots,
            &serve_repair,
            &None,
        );
        assert_ne!(duplicate_status.repair_pubkey_and_addr, dummy_addr);
    }
}
