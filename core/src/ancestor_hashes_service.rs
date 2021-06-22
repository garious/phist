use crate::{
    cluster_slots::ClusterSlots,
    duplicate_repair_status::DeadSlotAncestorRequestStatus,
    outstanding_requests::OutstandingRequests,
    repair_response,
    repair_service::{DuplicateSlotsResetSender, RepairInfo, RepairStatsGroup},
    replay_stage::DUPLICATE_THRESHOLD,
    result::{Error, Result},
    serve_repair::{AncestorHashesRepairType, ServeRepair},
};
use dashmap::DashMap;
use solana_ledger::blockstore::Blockstore;
use solana_measure::measure::Measure;
use solana_perf::{packet::limited_deserialize, recycler::Recycler};
use solana_runtime::bank::Bank;
use solana_sdk::{
    clock::{Slot, SLOT_MS},
    pubkey::Pubkey,
    timing::timestamp,
};
use solana_streamer::{
    packet::Packets,
    streamer::{self, PacketReceiver},
};
use std::{
    collections::HashSet,
    net::UdpSocket,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::channel,
        {Arc, RwLock},
    },
    thread::{self, sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

pub const MAX_ANCESTOR_HASHES_SLOT_REQUESTS_PER_SECOND: usize = 2;
type OutstandingAncestorHashesRepairs = OutstandingRequests<AncestorHashesRepairType>;

#[derive(Default)]
pub struct AncestorHashesResponsesStats {
    pub total_packets: usize,
    pub dropped_packets: usize,
    pub invalid_packets: usize,
    pub processed: usize,
}

impl AncestorHashesResponsesStats {
    fn report(&mut self) {
        inc_new_counter_info!(
            "ancestor_hashes_responses-total_packets",
            self.total_packets
        );
        inc_new_counter_info!("ancestor_hashes_responses-processed", self.processed);
        inc_new_counter_info!(
            "ancestor_hashes_responses-dropped_packets",
            self.dropped_packets
        );
        inc_new_counter_info!(
            "ancestor_hashes_responses-invalid_packets",
            self.invalid_packets
        );
        *self = AncestorHashesResponsesStats::default();
    }
}

pub struct AncestorRepairRequestsStats {
    pub ancestor_requests: RepairStatsGroup,
    last_report: Instant,
}

impl Default for AncestorRepairRequestsStats {
    fn default() -> Self {
        AncestorRepairRequestsStats {
            ancestor_requests: RepairStatsGroup::default(),
            last_report: Instant::now(),
        }
    }
}

impl AncestorRepairRequestsStats {
    fn report(&mut self) {
        let slot_to_count: Vec<_> = self
            .ancestor_requests
            .slot_pubkeys
            .iter()
            .map(|(slot, slot_repairs)| {
                (
                    slot,
                    slot_repairs
                        .pubkey_repairs()
                        .iter()
                        .map(|(_key, count)| count)
                        .sum::<u64>(),
                )
            })
            .collect();

        let repair_total = self.ancestor_requests.count;
        if self.last_report.elapsed().as_secs() > 2 && repair_total > 0 {
            info!("ancestor_repair_requests_stats: {:?}", slot_to_count);
            datapoint_info!(
                "ancestor-repair",
                ("ancestor-repair-count", self.ancestor_requests.count, i64)
            );

            *self = AncestorRepairRequestsStats::default();
        }
    }
}

pub struct AncestorHashesService {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl AncestorHashesService {
    pub fn new(
        exit: Arc<AtomicBool>,
        blockstore: Arc<Blockstore>,
        ancestor_hashes_request_socket: Arc<UdpSocket>,
        repair_info: RepairInfo,
    ) -> Self {
        let outstanding_requests: Arc<RwLock<OutstandingAncestorHashesRepairs>> =
            Arc::new(RwLock::new(OutstandingAncestorHashesRepairs::default()));
        let (response_sender, response_receiver) = channel();
        let t_receiver = streamer::receiver(
            ancestor_hashes_request_socket.clone(),
            &exit,
            response_sender,
            Recycler::default(),
            "ancestor_hashes_response_receiver",
            1,
            false,
        );

        let ancestor_hashes_request_statuses: Arc<DashMap<Slot, DeadSlotAncestorRequestStatus>> =
            Arc::new(DashMap::new());
        // Listen for responses to our ancestor requests
        let t_ancestor_hashes_responses = Self::run_responses_listener(
            ancestor_hashes_request_statuses.clone(),
            response_receiver,
            blockstore.clone(),
            outstanding_requests.clone(),
            exit.clone(),
            repair_info.duplicate_slots_reset_sender.clone(),
        );

        // Generate ancestor requests for dead slots that are repairable
        let t_ancestor_requests = Self::run_find_repairable_dead_slots(
            ancestor_hashes_request_statuses,
            ancestor_hashes_request_socket,
            blockstore,
            repair_info,
            outstanding_requests,
            exit,
        );
        let thread_hdls = vec![t_receiver, t_ancestor_hashes_responses, t_ancestor_requests];
        Self { thread_hdls }
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }

    /// Listen for responses to our ancestors hashes repair requests
    fn run_responses_listener(
        ancestor_hashes_request_statuses: Arc<DashMap<Slot, DeadSlotAncestorRequestStatus>>,
        response_receiver: PacketReceiver,
        blockstore: Arc<Blockstore>,
        outstanding_requests: Arc<RwLock<OutstandingAncestorHashesRepairs>>,
        exit: Arc<AtomicBool>,
        duplicate_slots_reset_sender: DuplicateSlotsResetSender,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("solana-ancestor-hashes-responses-service".to_string())
            .spawn(move || {
                let mut last_stats_report = Instant::now();
                let mut stats = AncestorHashesResponsesStats::default();
                let mut max_packets = 1024;
                loop {
                    let result = Self::process_new_responses(
                        &ancestor_hashes_request_statuses,
                        &response_receiver,
                        &blockstore,
                        &outstanding_requests,
                        &mut stats,
                        &mut max_packets,
                        &duplicate_slots_reset_sender,
                    );
                    match result {
                        Err(Error::RecvTimeout(_)) | Ok(_) => {}
                        Err(err) => info!("ancestors hashes reponses listener error: {:?}", err),
                    };
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    if last_stats_report.elapsed().as_secs() > 2 {
                        stats.report();
                        last_stats_report = Instant::now();
                    }
                }
            })
            .unwrap()
    }

    /// Process messages from the network
    fn process_new_responses(
        ancestor_hashes_request_statuses: &DashMap<Slot, DeadSlotAncestorRequestStatus>,
        response_receiver: &PacketReceiver,
        blockstore: &Blockstore,
        outstanding_requests: &RwLock<OutstandingAncestorHashesRepairs>,
        stats: &mut AncestorHashesResponsesStats,
        max_packets: &mut usize,
        duplicate_slots_reset_sender: &DuplicateSlotsResetSender,
    ) -> Result<()> {
        let timeout = Duration::new(1, 0);
        let mut responses = vec![response_receiver.recv_timeout(timeout)?];
        let mut total_packets = responses[0].packets.len();

        let mut dropped_packets = 0;
        while let Ok(more) = response_receiver.try_recv() {
            total_packets += more.packets.len();
            if total_packets < *max_packets {
                // Drop the rest in the channel in case of DOS
                responses.push(more);
            } else {
                dropped_packets += more.packets.len();
            }
        }

        stats.dropped_packets += dropped_packets;
        stats.total_packets += total_packets;

        let mut time = Measure::start("ancestor_hashes::handle_packets");
        for response in responses {
            Self::handle_packets(
                ancestor_hashes_request_statuses,
                response,
                stats,
                outstanding_requests,
                blockstore,
                duplicate_slots_reset_sender,
            );
        }
        time.stop();
        if total_packets >= *max_packets {
            if time.as_ms() > 1000 {
                *max_packets = (*max_packets * 9) / 10;
            } else {
                *max_packets = (*max_packets * 10) / 9;
            }
        }
        Ok(())
    }

    fn handle_packets(
        ancestor_hashes_request_statuses: &DashMap<Slot, DeadSlotAncestorRequestStatus>,
        packets: Packets,
        stats: &mut AncestorHashesResponsesStats,
        outstanding_requests: &RwLock<OutstandingAncestorHashesRepairs>,
        blockstore: &Blockstore,
        duplicate_slots_reset_sender: &DuplicateSlotsResetSender,
    ) {
        // iter over the packets
        packets.packets.iter().for_each(|packet| {
            let from_addr = packet.meta.addr();
            limited_deserialize(&packet.data[..packet.meta.size])
                .into_iter()
                .for_each(|ancestor_hashes_response| {
                    // Verify the response
                    let request_slot = repair_response::nonce(&packet.data).and_then(|nonce| {
                        outstanding_requests.write().unwrap().register_response(
                            nonce,
                            &ancestor_hashes_response,
                            timestamp(),
                            // If the response is valid, return the slot the request
                            // was for
                            |ancestor_hashes_request| ancestor_hashes_request.0,
                        )
                    });

                    if request_slot.is_none() {
                        stats.invalid_packets += 1;
                        return;
                    }

                    // If was a valid response, there must be a valid `request_slot`
                    let request_slot = request_slot.unwrap();
                    stats.processed += 1;

                    // Check if we can make any decisions.
                    if let Some(mut ancestor_hashes_status_ref) =
                        ancestor_hashes_request_statuses.get_mut(&request_slot)
                    {
                        if let Some(decision) = ancestor_hashes_status_ref.add_response(
                            &from_addr,
                            ancestor_hashes_response.into_slot_hashes(),
                            blockstore,
                        ) {
                            let potential_slots_to_dump = {
                                // TODO: In the case of DuplicateAncestorDecision::ContinueSearch
                                // This means all the ancestors were mismatched, which
                                // means the earliest mismatched ancestor has yet to be found.
                                //
                                // In the best case scenario, this means after ReplayStage dumps
                                // the earliest known ancestor `A` here, and then repairs `A`,
                                // because we may still have the incorrect version of some ancestor
                                // of `A`, we will mark `A` as dead and then continue the search
                                // protocol through another round of ancestor repairs.
                                //
                                // However this process is a bit slow, so in an ideal world, the
                                // protocol could be extended to keep searching by making
                                // another ancestor repair request from the earliest returned
                                // ancestor from this search.
                                decision
                                    .repair_status()
                                    .map(|status| status.correct_ancestors_to_repair.clone())
                            };

                            if let Some(potential_slots_to_dump) = potential_slots_to_dump {
                                // Signal ReplayStage to dump the fork that is descended from
                                // `earliest_mismatched_slot_to_dump`.
                                let _ = duplicate_slots_reset_sender.send(potential_slots_to_dump);
                            }
                        }
                    }

                    // TODO: move rest of duplicate repair logic here?
                });
        });
    }

    fn run_find_repairable_dead_slots(
        ancestor_hashes_request_statuses: Arc<DashMap<Slot, DeadSlotAncestorRequestStatus>>,
        ancestor_hashes_request_socket: Arc<UdpSocket>,
        blockstore: Arc<Blockstore>,
        repair_info: RepairInfo,
        outstanding_requests: Arc<RwLock<OutstandingAncestorHashesRepairs>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let serve_repair = ServeRepair::new(repair_info.cluster_info.clone());
        let id = repair_info.cluster_info.id();
        let mut repair_stats = AncestorRepairRequestsStats::default();

        // Sliding window that limits the number of slots repaired via AncestorRepair
        // to MAX_ANCESTOR_HASHES_SLOT_REQUESTS_PER_SECOND/second
        let mut request_throttle = vec![];
        Builder::new()
            .name("solana-find-repairable-dead-slots".to_string())
            .spawn(move || {
                loop {
                    let root_bank = repair_info.bank_forks.read().unwrap().root_bank().clone();

                    // Find if:
                    // 1) Any outstanding ancestor requests need to be retried, if so,
                    // purge those entries from the tracker. The next call to
                    // `find_new_repairable_dead_slots()` should then find these dead slots again
                    // for retry.
                    // 2) The `requested_slot` is no longer dead. If so, that means the slot
                    // was cleared and we won't discover this status again via `find_new_repairable_dead_slots`.
                    // (Risk of immediately clearing when the sampled validators reach agreement and
                    // BEFORE the dead flag is cleared is that `find_new_repairable_dead_slots()` will
                    // find the same dead slot again).
                    ancestor_hashes_request_statuses.retain(|requested_slot, status| {
                        !status.is_expired() || blockstore.is_dead(*requested_slot)
                    });

                    // Keep around the last second of requests
                    request_throttle.retain(|request_time| *request_time > (timestamp() - 1000));

                    let repairable_dead_slots = Self::find_new_repairable_dead_slots(
                        &id,
                        &ancestor_hashes_request_statuses,
                        &blockstore,
                        &repair_info.cluster_slots,
                        &root_bank,
                        Some(
                            MAX_ANCESTOR_HASHES_SLOT_REQUESTS_PER_SECOND
                                .saturating_sub(request_throttle.len()),
                        ),
                    );

                    // Find dead slots for which it's worthwhile to ask the network for their
                    // ancestors
                    for slot in repairable_dead_slots {
                        warn!(
                            "Cluster froze slot: {}, but we marked it as dead. 
                            Initiating protocol to sample cluster for dead slot ancestors.",
                            slot
                        );

                        Self::initiate_ancestor_hashes_requests_for_duplicate_slot(
                            &ancestor_hashes_request_statuses,
                            &ancestor_hashes_request_socket,
                            &repair_info.cluster_slots,
                            &serve_repair,
                            &repair_info.repair_validators,
                            slot,
                            &mut repair_stats,
                            &outstanding_requests,
                        );

                        request_throttle.push(timestamp());
                    }

                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    repair_stats.report();
                    sleep(Duration::from_millis(SLOT_MS));
                }
            })
            .unwrap()
    }

    fn find_new_repairable_dead_slots(
        pubkey: &Pubkey,
        ancestor_hashes_request_statuses: &DashMap<Slot, DeadSlotAncestorRequestStatus>,
        blockstore: &Blockstore,
        cluster_slots: &ClusterSlots,
        root_bank: &Bank,
        max_search_results: Option<usize>,
    ) -> Vec<Slot> {
        let dead_slots_iter = blockstore
            .dead_slots_iterator(root_bank.slot() + 1)
            .expect("Couldn't get dead slots iterator from blockstore");

        let iter = dead_slots_iter.filter_map(|dead_slot| {
            if ancestor_hashes_request_statuses.contains_key(&dead_slot) {
                // Still in the middle of repairing this slot, skip for now
                return None;
            }
            let status = cluster_slots.lookup(dead_slot);
            info!(
                "{} looking up status of dead slot {}, status {:?}",
                pubkey, dead_slot, status
            );
            status.and_then(|completed_dead_slot_pubkeys| {
                let epoch = root_bank.get_epoch_and_slot_index(dead_slot).0;
                if let Some(epoch_stakes) = root_bank.epoch_stakes(epoch) {
                    let total_stake = epoch_stakes.total_stake();
                    let node_id_to_vote_accounts = epoch_stakes.node_id_to_vote_accounts();
                    let total_completed_slot_stake: u64 = completed_dead_slot_pubkeys
                        .read()
                        .unwrap()
                        .iter()
                        .map(|(node_key, _)| {
                            node_id_to_vote_accounts
                                .get(node_key)
                                .map(|v| v.total_stake)
                                .unwrap_or(0)
                        })
                        .sum();
                    info!(
                        "{} checking frozen stake on dead slot {} {} {}",
                        pubkey, dead_slot, total_completed_slot_stake, total_stake
                    );
                    if total_completed_slot_stake as f64 / total_stake as f64 > DUPLICATE_THRESHOLD
                    {
                        // Begin the AncestorHashes repair procedure to figure out
                        // if we have the correct versions of all the ancestors
                        // leading up to this dead slot.
                        Some(dead_slot)
                    } else {
                        None
                    }
                } else {
                    error!(
                        "Dead slot {} is too far ahead of root bank {}",
                        dead_slot,
                        root_bank.slot()
                    );
                    None
                }
            })
        });

        if let Some(max_search_results) = max_search_results {
            iter.take(max_search_results).collect()
        } else {
            iter.collect()
        }
    }

    fn initiate_ancestor_hashes_requests_for_duplicate_slot(
        ancestor_hashes_request_statuses: &DashMap<Slot, DeadSlotAncestorRequestStatus>,
        ancestor_hashes_request_socket: &UdpSocket,
        cluster_slots: &ClusterSlots,
        serve_repair: &ServeRepair,
        repair_validators: &Option<HashSet<Pubkey>>,
        duplicate_slot: Slot,
        repair_stats: &mut AncestorRepairRequestsStats,
        outstanding_requests: &RwLock<OutstandingAncestorHashesRepairs>,
    ) {
        let sampled_validators = serve_repair.repair_request_ancestor_hashes_sample_peers(
            duplicate_slot,
            cluster_slots,
            repair_validators,
        );

        if let Ok(sampled_validators) = sampled_validators {
            for (pubkey, socket_addr) in sampled_validators.iter() {
                repair_stats
                    .ancestor_requests
                    .update(&pubkey, duplicate_slot, 0);
                let nonce = outstanding_requests
                    .write()
                    .unwrap()
                    .add_request(AncestorHashesRepairType(duplicate_slot), timestamp());
                let request_bytes =
                    serve_repair.ancestor_repair_request_bytes(duplicate_slot, nonce);
                if let Ok(request_bytes) = request_bytes {
                    let _ = ancestor_hashes_request_socket.send_to(&request_bytes, socket_addr);
                }
            }

            let ancestor_request_status = DeadSlotAncestorRequestStatus::new(
                sampled_validators
                    .into_iter()
                    .map(|(_pk, socket_addr)| socket_addr),
                duplicate_slot,
            );
            assert!(!ancestor_hashes_request_statuses.contains_key(&duplicate_slot));
            ancestor_hashes_request_statuses.insert(duplicate_slot, ancestor_request_status);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use solana_ledger::{blockstore::Blockstore, get_tmp_ledger_path};
    use solana_runtime::{
        bank::Bank,
        genesis_utils::{self, GenesisConfigInfo, ValidatorVoteKeypairs},
    };
    use solana_sdk::signature::Signer;

    #[test]
    pub fn test_find_new_repairable_dead_slots() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();
        let cluster_slots = ClusterSlots::default();
        let ancestor_hashes_request_statuses = DashMap::new();
        let keypairs = ValidatorVoteKeypairs::new_rand();
        let only_node_id = keypairs.node_keypair.pubkey();
        let GenesisConfigInfo { genesis_config, .. } =
            genesis_utils::create_genesis_config_with_vote_accounts(
                1_000_000_000,
                &[keypairs],
                vec![100],
            );
        let bank0 = Bank::new(&genesis_config);

        // Empty blockstore should have no duplicates
        assert!(AncestorHashesService::find_new_repairable_dead_slots(
            &Pubkey::default(),
            &ancestor_hashes_request_statuses,
            &blockstore,
            &cluster_slots,
            &bank0,
            None,
        )
        .is_empty());

        // Insert a dead slot, but is not confirmed by network so should not
        // be marked as duplicate
        let dead_slot = 9;
        blockstore.set_dead_slot(dead_slot).unwrap();
        assert!(AncestorHashesService::find_new_repairable_dead_slots(
            &Pubkey::default(),
            &ancestor_hashes_request_statuses,
            &blockstore,
            &cluster_slots,
            &bank0,
            None,
        )
        .is_empty());

        // If supermajority confirms the slot, then dead slot should be
        // marked as a duplicate that needs to be repaired
        cluster_slots.insert_node_id(dead_slot, only_node_id);
        assert_eq!(
            AncestorHashesService::find_new_repairable_dead_slots(
                &Pubkey::default(),
                &ancestor_hashes_request_statuses,
                &blockstore,
                &cluster_slots,
                &bank0,
                None,
            ),
            vec![dead_slot]
        );
    }
}
