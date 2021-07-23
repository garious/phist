use crate::{
    duplicate_repair_status::DeadSlotAncestorRequestStatus,
    outstanding_requests::OutstandingRequests,
    repair_response::{self},
    repair_service::{DuplicateSlotsResetSender, RepairInfo, RepairStatsGroup},
    result::{Error, Result},
    serve_repair::AncestorHashesRepairType,
};
use dashmap::{mapref::entry::Entry::Occupied, DashMap};
use solana_ledger::{blockstore::Blockstore, shred::SIZE_OF_NONCE};
use solana_measure::measure::Measure;
use solana_perf::{packet::limited_deserialize, recycler::Recycler};
use solana_sdk::{
    clock::{Slot, SLOT_MS},
    timing::timestamp,
};
use solana_streamer::{
    packet::Packets,
    streamer::{self, PacketReceiver},
};
use std::{
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

        // Listen for responses to our ancestor requests
        let ancestor_hashes_request_statuses: Arc<DashMap<Slot, DeadSlotAncestorRequestStatus>> =
            Arc::new(DashMap::new());
        let t_ancestor_hashes_responses = Self::run_responses_listener(
            ancestor_hashes_request_statuses.clone(),
            response_receiver,
            blockstore,
            outstanding_requests.clone(),
            exit.clone(),
            repair_info.duplicate_slots_reset_sender.clone(),
        );

        // Generate ancestor requests for dead slots that are repairable
        let t_ancestor_requests = Self::run_find_repairable_dead_slots(
            ancestor_hashes_request_statuses,
            ancestor_hashes_request_socket,
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
            if let Ok(ancestor_hashes_response) =
                limited_deserialize(&packet.data[..packet.meta.size - SIZE_OF_NONCE])
            {
                // Verify the response
                let request_slot = repair_response::nonce(&packet.data[..packet.meta.size])
                    .and_then(|nonce| {
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
                if let Occupied(mut ancestor_hashes_status_ref) =
                    ancestor_hashes_request_statuses.entry(request_slot)
                {
                    if let Some(decision) = ancestor_hashes_status_ref.get_mut().add_response(
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

                        let mut did_send_replay_correct_ancestors = false;

                        if let Some(potential_slots_to_dump) = potential_slots_to_dump {
                            // Signal ReplayStage to dump the fork that is descended from
                            // `earliest_mismatched_slot_to_dump`.
                            if !potential_slots_to_dump.is_empty() {
                                did_send_replay_correct_ancestors = true;
                                let _ = duplicate_slots_reset_sender.send(potential_slots_to_dump);
                            }
                        }

                        if !did_send_replay_correct_ancestors {
                            // If nothing is going to be dumped + repaired, then we can remove
                            // this slot from `ancestor_hashes_request_statuses` since the
                            // dead flag won't be cleared from blockstore, so the
                            // `ancestor_hashes_request_statuses.retain()` in
                            // `Self::run_find_repairable_dead_slots()` won't clear
                            // this slot
                            ancestor_hashes_status_ref.remove();
                        }
                    }
                }
            }
        });
    }

    fn run_find_repairable_dead_slots(
        _ancestor_hashes_request_statuses: Arc<DashMap<Slot, DeadSlotAncestorRequestStatus>>,
        _ancestor_hashes_request_socket: Arc<UdpSocket>,
        _repair_info: RepairInfo,
        _outstanding_requests: Arc<RwLock<OutstandingAncestorHashesRepairs>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let mut repair_stats = AncestorRepairRequestsStats::default();

        Builder::new()
            .name("solana-find-repairable-dead-slots".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    return;
                }
                repair_stats.report();
                sleep(Duration::from_millis(SLOT_MS));
            })
            .unwrap()
    }
}
