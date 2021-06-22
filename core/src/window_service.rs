//! `window_service` handles the data plane incoming shreds, storing them in
//!   blockstore and retransmitting where required
//!
use crate::{
    cluster_info_vote_listener::VerifiedVoteReceiver,
    completed_data_sets_service::CompletedDataSetsSender,
    outstanding_requests::OutstandingRequests,
    repair_response,
    repair_service::{OutstandingShredRepairs, RepairInfo, RepairService},
    result::{Error, Result},
};
use crossbeam_channel::{
    unbounded, Receiver as CrossbeamReceiver, RecvTimeoutError, Sender as CrossbeamSender,
};
use rayon::{prelude::*, ThreadPool};
use solana_gossip::cluster_info::ClusterInfo;
use solana_ledger::{
    blockstore::{self, Blockstore, BlockstoreInsertionMetrics, MAX_DATA_SHREDS_PER_SLOT},
    leader_schedule_cache::LeaderScheduleCache,
    shred::{Nonce, Shred},
};
use solana_metrics::{inc_new_counter_debug, inc_new_counter_error};
use solana_perf::packet::{Packet, Packets};
use solana_rayon_threadlimit::get_thread_count;
use solana_runtime::{bank::Bank, bank_forks::BankForks};
use solana_sdk::{clock::Slot, packet::PACKET_DATA_SIZE, pubkey::Pubkey, timing::duration_as_ms};
use solana_streamer::streamer::PacketSender;
use std::collections::HashSet;
use std::{
    net::{SocketAddr, UdpSocket},
    ops::Deref,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, RwLock},
    thread::{self, Builder, JoinHandle},
    time::{Duration, Instant},
};

pub type DuplicateSlotSender = CrossbeamSender<Slot>;
pub type DuplicateSlotReceiver = CrossbeamReceiver<Slot>;

fn verify_shred_slot(shred: &Shred, root: u64) -> bool {
    if shred.is_data() {
        // Only data shreds have parent information
        blockstore::verify_shred_slots(shred.slot(), shred.parent(), root)
    } else {
        // Filter out outdated coding shreds
        shred.slot() >= root
    }
}

/// drop shreds that are from myself or not from the correct leader for the
/// shred's slot
pub fn should_retransmit_and_persist(
    shred: &Shred,
    bank: Option<Arc<Bank>>,
    leader_schedule_cache: &Arc<LeaderScheduleCache>,
    my_pubkey: &Pubkey,
    root: u64,
    shred_version: u16,
) -> bool {
    let slot_leader_pubkey = match bank {
        None => leader_schedule_cache.slot_leader_at(shred.slot(), None),
        Some(bank) => leader_schedule_cache.slot_leader_at(shred.slot(), Some(&bank)),
    };
    if let Some(leader_id) = slot_leader_pubkey {
        if leader_id == *my_pubkey {
            inc_new_counter_debug!("streamer-recv_window-circular_transmission", 1);
            false
        } else if !verify_shred_slot(shred, root) {
            inc_new_counter_debug!("streamer-recv_window-outdated_transmission", 1);
            false
        } else if shred.version() != shred_version {
            inc_new_counter_debug!("streamer-recv_window-incorrect_shred_version", 1);
            false
        } else if shred.index() >= MAX_DATA_SHREDS_PER_SLOT as u32 {
            inc_new_counter_warn!("streamer-recv_window-shred_index_overrun", 1);
            false
        } else {
            true
        }
    } else {
        inc_new_counter_debug!("streamer-recv_window-unknown_leader", 1);
        false
    }
}

fn run_check_duplicate(
    cluster_info: &ClusterInfo,
    blockstore: &Blockstore,
    shred_receiver: &CrossbeamReceiver<Shred>,
    duplicate_slot_sender: &DuplicateSlotSender,
) -> Result<()> {
    let check_duplicate = |shred: Shred| -> Result<()> {
        let shred_slot = shred.slot();
        if !blockstore.has_duplicate_shreds_in_slot(shred_slot) {
            if let Some(existing_shred_payload) = blockstore.is_shred_duplicate(
                shred_slot,
                shred.index(),
                &shred.payload,
                shred.is_data(),
            ) {
                cluster_info.push_duplicate_shred(&shred, &existing_shred_payload)?;
                blockstore.store_duplicate_slot(
                    shred_slot,
                    existing_shred_payload,
                    shred.payload,
                )?;

                duplicate_slot_sender.send(shred_slot)?;
            }
        }

        Ok(())
    };
    let timer = Duration::from_millis(200);
    let shred = shred_receiver.recv_timeout(timer)?;
    check_duplicate(shred)?;
    while let Ok(shred) = shred_receiver.try_recv() {
        check_duplicate(shred)?;
    }

    Ok(())
}

fn verify_repair(
    outstanding_requests: &mut OutstandingShredRepairs,
    shred: &Shred,
    repair_meta: &Option<RepairMeta>,
) -> bool {
    repair_meta
        .as_ref()
        .map(|repair_meta| {
            outstanding_requests
                .register_response(
                    repair_meta.nonce,
                    shred,
                    solana_sdk::timing::timestamp(),
                    |_| (),
                )
                .is_some()
        })
        .unwrap_or(true)
}

fn prune_shreds_invalid_repair(
    shreds: &mut Vec<Shred>,
    repair_infos: &mut Vec<Option<RepairMeta>>,
    outstanding_requests: &Arc<RwLock<OutstandingShredRepairs>>,
) {
    assert_eq!(shreds.len(), repair_infos.len());
    let mut i = 0;
    let mut removed = HashSet::new();
    {
        let mut outstanding_requests = outstanding_requests.write().unwrap();
        shreds.retain(|shred| {
            let should_keep = (
                verify_repair(&mut outstanding_requests, shred, &repair_infos[i]),
                i += 1,
            )
                .0;
            if !should_keep {
                removed.insert(i - 1);
            }
            should_keep
        });
    }
    i = 0;
    repair_infos.retain(|_repair_info| (!removed.contains(&i), i += 1).0);
    assert_eq!(shreds.len(), repair_infos.len());
}

fn run_insert<F>(
    shred_receiver: &CrossbeamReceiver<(Vec<Shred>, Vec<Option<RepairMeta>>)>,
    blockstore: &Arc<Blockstore>,
    leader_schedule_cache: &Arc<LeaderScheduleCache>,
    handle_duplicate: F,
    metrics: &mut BlockstoreInsertionMetrics,
    completed_data_sets_sender: &CompletedDataSetsSender,
    outstanding_requests: &Arc<RwLock<OutstandingShredRepairs>>,
) -> Result<()>
where
    F: Fn(Shred),
{
    let timer = Duration::from_millis(200);
    let (mut shreds, mut repair_infos) = shred_receiver.recv_timeout(timer)?;
    while let Ok((more_shreds, more_repair_infos)) = shred_receiver.try_recv() {
        shreds.extend(more_shreds);
        repair_infos.extend(more_repair_infos);
    }

    prune_shreds_invalid_repair(&mut shreds, &mut repair_infos, outstanding_requests);
    let repairs: Vec<_> = repair_infos
        .iter()
        .map(|repair_info| repair_info.is_some())
        .collect();

    let (completed_data_sets, inserted_indices) = blockstore.insert_shreds_handle_duplicate(
        shreds,
        repairs,
        Some(leader_schedule_cache),
        false,
        &handle_duplicate,
        metrics,
    )?;
    for index in inserted_indices {
        if repair_infos[index].is_some() {
            metrics.num_repair += 1;
        }
    }

    completed_data_sets_sender.try_send(completed_data_sets)?;
    Ok(())
}

fn recv_window<F>(
    blockstore: &Arc<Blockstore>,
    leader_schedule_cache: &Arc<LeaderScheduleCache>,
    bank_forks: &Arc<RwLock<BankForks>>,
    insert_shred_sender: &CrossbeamSender<(Vec<Shred>, Vec<Option<RepairMeta>>)>,
    my_pubkey: &Pubkey,
    verified_receiver: &CrossbeamReceiver<Vec<Packets>>,
    retransmit: &PacketSender,
    shred_filter: F,
    thread_pool: &ThreadPool,
) -> Result<()>
where
    F: Fn(&Shred, u64) -> bool + Sync,
{
    let timer = Duration::from_millis(200);
    let mut packets = verified_receiver.recv_timeout(timer)?;
    packets.extend(verified_receiver.try_iter().flatten());
    let total_packets: usize = packets.iter().map(|p| p.packets.len()).sum();
    let now = Instant::now();
    inc_new_counter_debug!("streamer-recv_window-recv", total_packets);

    let root_bank = bank_forks.read().unwrap().root_bank();
    let last_root = blockstore.last_root();
    let handle_packet = |packet: &mut Packet| {
        if packet.meta.discard {
            inc_new_counter_debug!("streamer-recv_window-invalid_or_unnecessary_packet", 1);
            return None;
        }
        // shred fetch stage should be sending packets
        // with sufficiently large buffers. Needed to ensure
        // call to `new_from_serialized_shred` is safe.
        assert_eq!(packet.data.len(), PACKET_DATA_SIZE);
        let serialized_shred = packet.data.to_vec();
        let shred = match Shred::new_from_serialized_shred(serialized_shred) {
            Ok(shred) if shred_filter(&shred, last_root) => {
                let leader_pubkey =
                    leader_schedule_cache.slot_leader_at(shred.slot(), Some(root_bank.deref()));
                packet.meta.slot = shred.slot();
                packet.meta.seed = shred.seed(leader_pubkey, root_bank.deref());
                shred
            }
            Ok(_) | Err(_) => {
                packet.meta.discard = true;
                return None;
            }
        };
        if packet.meta.repair {
            let repair_info = RepairMeta {
                _from_addr: packet.meta.addr(),
                // If can't parse the nonce, dump the packet.
                nonce: repair_response::nonce(&packet.data)?,
            };
            Some((shred, Some(repair_info)))
        } else {
            Some((shred, None))
        }
    };
    let (shreds, repair_infos): (Vec<_>, Vec<_>) = thread_pool.install(|| {
        packets
            .par_iter_mut()
            .flat_map_iter(|packet| packet.packets.iter_mut().filter_map(handle_packet))
            .unzip()
    });

    trace!("{:?} shreds from packets", shreds.len());

    trace!("{} num total shreds received: {}", my_pubkey, total_packets);

    for packets in packets.into_iter() {
        if !packets.is_empty() {
            // Ignore the send error, as the retransmit is optional (e.g. archivers don't retransmit)
            let _ = retransmit.send(packets);
        }
    }

    insert_shred_sender.send((shreds, repair_infos))?;

    trace!(
        "Elapsed processing time in recv_window(): {}",
        duration_as_ms(&now.elapsed())
    );

    Ok(())
}

struct RepairMeta {
    _from_addr: SocketAddr,
    nonce: Nonce,
}

// Implement a destructor for the window_service thread to signal it exited
// even on panics
struct Finalizer {
    exit_sender: Arc<AtomicBool>,
}

impl Finalizer {
    fn new(exit_sender: Arc<AtomicBool>) -> Self {
        Finalizer { exit_sender }
    }
}
// Implement a destructor for Finalizer.
impl Drop for Finalizer {
    fn drop(&mut self) {
        self.exit_sender.clone().store(true, Ordering::Relaxed);
    }
}

pub struct WindowService {
    t_window: JoinHandle<()>,
    t_insert: JoinHandle<()>,
    t_check_duplicate: JoinHandle<()>,
    repair_service: RepairService,
}

impl WindowService {
    #[allow(clippy::too_many_arguments)]
    pub fn new<F>(
        blockstore: Arc<Blockstore>,
        verified_receiver: CrossbeamReceiver<Vec<Packets>>,
        retransmit: PacketSender,
        repair_socket: Arc<UdpSocket>,
        exit: &Arc<AtomicBool>,
        repair_info: RepairInfo,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        shred_filter: F,
        verified_vote_receiver: VerifiedVoteReceiver,
        completed_data_sets_sender: CompletedDataSetsSender,
        duplicate_slots_sender: DuplicateSlotSender,
    ) -> WindowService
    where
        F: 'static
            + Fn(&Pubkey, &Shred, Option<Arc<Bank>>, u64) -> bool
            + std::marker::Send
            + std::marker::Sync,
    {
        let outstanding_requests: Arc<RwLock<OutstandingShredRepairs>> =
            Arc::new(RwLock::new(OutstandingRequests::default()));

        let bank_forks = repair_info.bank_forks.clone();
        let cluster_info = repair_info.cluster_info.clone();
        let id = cluster_info.id();

        let repair_service = RepairService::new(
            blockstore.clone(),
            exit.clone(),
            repair_socket,
            repair_info,
            verified_vote_receiver,
            outstanding_requests.clone(),
        );

        let (insert_sender, insert_receiver) = unbounded();
        let (duplicate_sender, duplicate_receiver) = unbounded();

        let t_check_duplicate = Self::start_check_duplicate_thread(
            cluster_info,
            exit.clone(),
            blockstore.clone(),
            duplicate_receiver,
            duplicate_slots_sender,
        );

        let t_insert = Self::start_window_insert_thread(
            exit,
            &blockstore,
            leader_schedule_cache,
            insert_receiver,
            duplicate_sender,
            completed_data_sets_sender,
            outstanding_requests,
        );

        let t_window = Self::start_recv_window_thread(
            id,
            exit,
            &blockstore,
            insert_sender,
            verified_receiver,
            shred_filter,
            leader_schedule_cache,
            &bank_forks,
            retransmit,
        );

        WindowService {
            t_window,
            t_insert,
            t_check_duplicate,
            repair_service,
        }
    }

    fn start_check_duplicate_thread(
        cluster_info: Arc<ClusterInfo>,
        exit: Arc<AtomicBool>,
        blockstore: Arc<Blockstore>,
        duplicate_receiver: CrossbeamReceiver<Shred>,
        duplicate_slot_sender: DuplicateSlotSender,
    ) -> JoinHandle<()> {
        let handle_error = || {
            inc_new_counter_error!("solana-check-duplicate-error", 1, 1);
        };
        Builder::new()
            .name("solana-check-duplicate".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }

                let mut noop = || {};
                if let Err(e) = run_check_duplicate(
                    &cluster_info,
                    &blockstore,
                    &duplicate_receiver,
                    &duplicate_slot_sender,
                ) {
                    if Self::should_exit_on_error(e, &mut noop, &handle_error) {
                        break;
                    }
                }
            })
            .unwrap()
    }

    fn start_window_insert_thread(
        exit: &Arc<AtomicBool>,
        blockstore: &Arc<Blockstore>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        insert_receiver: CrossbeamReceiver<(Vec<Shred>, Vec<Option<RepairMeta>>)>,
        check_duplicate_sender: CrossbeamSender<Shred>,
        completed_data_sets_sender: CompletedDataSetsSender,
        outstanding_requests: Arc<RwLock<OutstandingShredRepairs>>,
    ) -> JoinHandle<()> {
        let exit = exit.clone();
        let blockstore = blockstore.clone();
        let leader_schedule_cache = leader_schedule_cache.clone();
        let mut handle_timeout = || {};
        let handle_error = || {
            inc_new_counter_error!("solana-window-insert-error", 1, 1);
        };

        Builder::new()
            .name("solana-window-insert".to_string())
            .spawn(move || {
                let handle_duplicate = |shred| {
                    let _ = check_duplicate_sender.send(shred);
                };
                let mut metrics = BlockstoreInsertionMetrics::default();
                let mut last_print = Instant::now();
                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }

                    if let Err(e) = run_insert(
                        &insert_receiver,
                        &blockstore,
                        &leader_schedule_cache,
                        &handle_duplicate,
                        &mut metrics,
                        &completed_data_sets_sender,
                        &outstanding_requests,
                    ) {
                        if Self::should_exit_on_error(e, &mut handle_timeout, &handle_error) {
                            break;
                        }
                    }

                    if last_print.elapsed().as_secs() > 2 {
                        metrics.report_metrics("recv-window-insert-shreds");
                        metrics = BlockstoreInsertionMetrics::default();
                        last_print = Instant::now();
                    }
                }
            })
            .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn start_recv_window_thread<F>(
        id: Pubkey,
        exit: &Arc<AtomicBool>,
        blockstore: &Arc<Blockstore>,
        insert_sender: CrossbeamSender<(Vec<Shred>, Vec<Option<RepairMeta>>)>,
        verified_receiver: CrossbeamReceiver<Vec<Packets>>,
        shred_filter: F,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        bank_forks: &Arc<RwLock<BankForks>>,
        retransmit: PacketSender,
    ) -> JoinHandle<()>
    where
        F: 'static
            + Fn(&Pubkey, &Shred, Option<Arc<Bank>>, u64) -> bool
            + std::marker::Send
            + std::marker::Sync,
    {
        let exit = exit.clone();
        let blockstore = blockstore.clone();
        let bank_forks = bank_forks.clone();
        let bank_forks_opt = Some(bank_forks.clone());
        let leader_schedule_cache = leader_schedule_cache.clone();
        Builder::new()
            .name("solana-window".to_string())
            .spawn(move || {
                let _exit = Finalizer::new(exit.clone());
                trace!("{}: RECV_WINDOW started", id);
                let thread_pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(get_thread_count())
                    .build()
                    .unwrap();
                let mut now = Instant::now();
                let handle_error = || {
                    inc_new_counter_error!("solana-window-error", 1, 1);
                };

                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }

                    let mut handle_timeout = || {
                        if now.elapsed() > Duration::from_secs(30) {
                            warn!("Window does not seem to be receiving data. Ensure port configuration is correct...");
                            now = Instant::now();
                        }
                    };
                    if let Err(e) = recv_window(
                        &blockstore,
                        &leader_schedule_cache,
                        &bank_forks,
                        &insert_sender,
                        &id,
                        &verified_receiver,
                        &retransmit,
                        |shred, last_root| {
                            shred_filter(
                                &id,
                                shred,
                                bank_forks_opt
                                    .as_ref()
                                    .map(|bank_forks| bank_forks.read().unwrap().working_bank()),
                                last_root,
                            )
                        },
                        &thread_pool,
                    ) {
                        if Self::should_exit_on_error(e, &mut handle_timeout, &handle_error) {
                            break;
                        }
                    } else {
                        now = Instant::now();
                    }
                }
            })
            .unwrap()
    }

    fn should_exit_on_error<F, H>(e: Error, handle_timeout: &mut F, handle_error: &H) -> bool
    where
        F: FnMut(),
        H: Fn(),
    {
        match e {
            Error::CrossbeamRecvTimeout(RecvTimeoutError::Disconnected) => true,
            Error::CrossbeamRecvTimeout(RecvTimeoutError::Timeout) => {
                handle_timeout();
                false
            }
            Error::CrossbeamSend => true,
            _ => {
                handle_error();
                error!("thread {:?} error {:?}", thread::current().name(), e);
                false
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_window.join()?;
        self.t_insert.join()?;
        self.t_check_duplicate.join()?;
        self.repair_service.join()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use solana_entry::entry::{create_ticks, Entry};
    use solana_gossip::contact_info::ContactInfo;
    use solana_ledger::{
        blockstore::{make_many_slot_entries, Blockstore},
        genesis_utils::create_genesis_config_with_leader,
        get_tmp_ledger_path,
        shred::{DataShredHeader, Shredder},
    };
    use solana_sdk::{
        epoch_schedule::MINIMUM_SLOTS_PER_EPOCH,
        hash::Hash,
        signature::{Keypair, Signer},
        timing::timestamp,
    };
    use std::sync::Arc;

    fn local_entries_to_shred(
        entries: &[Entry],
        slot: Slot,
        parent: Slot,
        keypair: &Keypair,
    ) -> Vec<Shred> {
        let shredder = Shredder::new(slot, parent, 0, 0).unwrap();
        shredder.entries_to_shreds(keypair, entries, true, 0).0
    }

    #[test]
    fn test_process_shred() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(Blockstore::open(&blockstore_path).unwrap());
        let num_entries = 10;
        let original_entries = create_ticks(num_entries, 0, Hash::default());
        let mut shreds = local_entries_to_shred(&original_entries, 0, 0, &Keypair::new());
        shreds.reverse();
        blockstore
            .insert_shreds(shreds, None, false)
            .expect("Expect successful processing of shred");

        assert_eq!(blockstore.get_slot_entries(0, 0).unwrap(), original_entries);

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_should_retransmit_and_persist() {
        let me_id = solana_sdk::pubkey::new_rand();
        let leader_keypair = Arc::new(Keypair::new());
        let leader_pubkey = leader_keypair.pubkey();
        let bank = Arc::new(Bank::new(
            &create_genesis_config_with_leader(100, &leader_pubkey, 10).genesis_config,
        ));
        let cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));

        let mut shreds = local_entries_to_shred(&[Entry::default()], 0, 0, &leader_keypair);

        // with a Bank for slot 0, shred continues
        assert!(should_retransmit_and_persist(
            &shreds[0],
            Some(bank.clone()),
            &cache,
            &me_id,
            0,
            0
        ));
        // with the wrong shred_version, shred gets thrown out
        assert!(!should_retransmit_and_persist(
            &shreds[0],
            Some(bank.clone()),
            &cache,
            &me_id,
            0,
            1
        ));

        // If it's a coding shred, test that slot >= root
        let (common, coding) = Shredder::new_coding_shred_header(5, 5, 5, 6, 6, 0);
        let mut coding_shred =
            Shred::new_empty_from_header(common, DataShredHeader::default(), coding);
        Shredder::sign_shred(&leader_keypair, &mut coding_shred);
        assert!(should_retransmit_and_persist(
            &coding_shred,
            Some(bank.clone()),
            &cache,
            &me_id,
            0,
            0
        ));
        assert!(should_retransmit_and_persist(
            &coding_shred,
            Some(bank.clone()),
            &cache,
            &me_id,
            5,
            0
        ));
        assert!(!should_retransmit_and_persist(
            &coding_shred,
            Some(bank.clone()),
            &cache,
            &me_id,
            6,
            0
        ));

        // with a Bank and no idea who leader is, shred gets thrown out
        shreds[0].set_slot(MINIMUM_SLOTS_PER_EPOCH as u64 * 3);
        assert!(!should_retransmit_and_persist(
            &shreds[0],
            Some(bank.clone()),
            &cache,
            &me_id,
            0,
            0
        ));

        // with a shred where shred.slot() == root, shred gets thrown out
        let slot = MINIMUM_SLOTS_PER_EPOCH as u64 * 3;
        let shreds = local_entries_to_shred(&[Entry::default()], slot, slot - 1, &leader_keypair);
        assert!(!should_retransmit_and_persist(
            &shreds[0],
            Some(bank.clone()),
            &cache,
            &me_id,
            slot,
            0
        ));

        // with a shred where shred.parent() < root, shred gets thrown out
        let slot = MINIMUM_SLOTS_PER_EPOCH as u64 * 3;
        let shreds =
            local_entries_to_shred(&[Entry::default()], slot + 1, slot - 1, &leader_keypair);
        assert!(!should_retransmit_and_persist(
            &shreds[0],
            Some(bank),
            &cache,
            &me_id,
            slot,
            0
        ));

        // if the shred came back from me, it doesn't continue, whether or not I have a bank
        assert!(!should_retransmit_and_persist(
            &shreds[0], None, &cache, &me_id, 0, 0
        ));
    }

    #[test]
    fn test_run_check_duplicate() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(Blockstore::open(&blockstore_path).unwrap());
        let (sender, receiver) = unbounded();
        let (duplicate_slot_sender, duplicate_slot_receiver) = unbounded();
        let (shreds, _) = make_many_slot_entries(5, 5, 10);
        blockstore
            .insert_shreds(shreds.clone(), None, false)
            .unwrap();
        let mut duplicate_shred = shreds[1].clone();
        duplicate_shred.set_slot(shreds[0].slot());
        let duplicate_shred_slot = duplicate_shred.slot();
        sender.send(duplicate_shred).unwrap();
        assert!(!blockstore.has_duplicate_shreds_in_slot(duplicate_shred_slot));
        let keypair = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), timestamp());
        let cluster_info = ClusterInfo::new(contact_info, Arc::new(keypair));
        run_check_duplicate(
            &cluster_info,
            &blockstore,
            &receiver,
            &duplicate_slot_sender,
        )
        .unwrap();
        assert!(blockstore.has_duplicate_shreds_in_slot(duplicate_shred_slot));
        assert_eq!(
            duplicate_slot_receiver.try_recv().unwrap(),
            duplicate_shred_slot
        );
    }

    #[test]
    fn test_prune_shreds() {
        use crate::serve_repair::ShredRepairType;
        use std::net::{IpAddr, Ipv4Addr};
        solana_logger::setup();
        let (common, coding) = Shredder::new_coding_shred_header(5, 5, 5, 6, 6, 0);
        let shred = Shred::new_empty_from_header(common, DataShredHeader::default(), coding);
        let mut shreds = vec![shred.clone(), shred.clone(), shred];
        let _from_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let repair_meta = RepairMeta {
            _from_addr,
            nonce: 0,
        };
        let outstanding_requests = Arc::new(RwLock::new(OutstandingShredRepairs::default()));
        let repair_type = ShredRepairType::Orphan(9);
        let nonce = outstanding_requests
            .write()
            .unwrap()
            .add_request(repair_type, timestamp());
        let repair_meta1 = RepairMeta { _from_addr, nonce };
        let mut repair_infos = vec![None, Some(repair_meta), Some(repair_meta1)];
        prune_shreds_invalid_repair(&mut shreds, &mut repair_infos, &outstanding_requests);
        assert_eq!(repair_infos.len(), 2);
        assert!(repair_infos[0].is_none());
        assert_eq!(repair_infos[1].as_ref().unwrap().nonce, nonce);
    }
}
