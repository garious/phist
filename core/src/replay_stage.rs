//! The `replay_stage` replays transactions broadcast by the leader.

use crate::{
    broadcast_stage::RetransmitSlotsSender,
    cache_block_meta_service::CacheBlockMetaSender,
    cluster_info_vote_listener::{
        GossipDuplicateConfirmedSlotsReceiver, GossipVerifiedVoteHashReceiver, VoteTracker,
    },
    cluster_slot_state_verifier::*,
    cluster_slots::ClusterSlots,
    cluster_slots_service::ClusterSlotsUpdateSender,
    commitment_service::{AggregateCommitmentService, CommitmentAggregationData},
    consensus::{
        ComputedBankState, SavedTower, Stake, SwitchForkDecision, Tower, TowerStorage, VotedStakes,
        SWITCH_FORK_THRESHOLD,
    },
    fork_choice::{ForkChoice, SelectVoteAndResetForkResult},
    heaviest_subtree_fork_choice::HeaviestSubtreeForkChoice,
    latest_validator_votes_for_frozen_banks::LatestValidatorVotesForFrozenBanks,
    progress_map::{ForkProgress, ProgressMap, PropagatedStats},
    repair_service::DuplicateSlotsResetReceiver,
    rewards_recorder_service::RewardsRecorderSender,
    unfrozen_gossip_verified_vote_hashes::UnfrozenGossipVerifiedVoteHashes,
    voting_service::VoteOp,
    window_service::DuplicateSlotReceiver,
};
use solana_client::rpc_response::SlotUpdate;
use solana_entry::entry::VerifyRecyclers;
use solana_gossip::cluster_info::ClusterInfo;
use solana_ledger::{
    block_error::BlockError,
    blockstore::Blockstore,
    blockstore_processor::{self, BlockstoreProcessorError, TransactionStatusSender},
    leader_schedule_cache::LeaderScheduleCache,
};
use solana_measure::measure::Measure;
use solana_metrics::inc_new_counter_info;
use solana_poh::poh_recorder::{PohRecorder, GRACE_TICKS_FACTOR, MAX_GRACE_SLOTS};
use solana_rpc::{
    optimistically_confirmed_bank_tracker::{BankNotification, BankNotificationSender},
    rpc_subscriptions::RpcSubscriptions,
};
use solana_runtime::{
    accounts_background_service::AbsRequestSender, bank::Bank, bank::ExecuteTimings,
    bank_forks::BankForks, commitment::BlockCommitmentCache, vote_sender_types::ReplayVoteSender,
};
use solana_sdk::{
    clock::{BankId, Slot, MAX_PROCESSING_AGE, NUM_CONSECUTIVE_LEADER_SLOTS},
    genesis_config::ClusterType,
    hash::Hash,
    pubkey::Pubkey,
    signature::Signature,
    signature::{Keypair, Signer},
    timing::timestamp,
    transaction::Transaction,
};
use solana_vote_program::vote_state::Vote;
use std::{
    collections::{HashMap, HashSet},
    result,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, RecvTimeoutError, Sender},
        Arc, Mutex, RwLock,
    },
    thread::{self, Builder, JoinHandle},
    time::{Duration, Instant},
};

pub const MAX_ENTRY_RECV_PER_ITER: usize = 512;
pub const SUPERMINORITY_THRESHOLD: f64 = 1f64 / 3f64;
pub const MAX_UNCONFIRMED_SLOTS: usize = 5;
pub const DUPLICATE_LIVENESS_THRESHOLD: f64 = 0.1;
pub const DUPLICATE_THRESHOLD: f64 = 1.0 - SWITCH_FORK_THRESHOLD - DUPLICATE_LIVENESS_THRESHOLD;
const MAX_VOTE_SIGNATURES: usize = 200;
const MAX_VOTE_REFRESH_INTERVAL_MILLIS: usize = 5000;

#[derive(PartialEq, Debug)]
pub enum HeaviestForkFailures {
    LockedOut(u64),
    FailedThreshold(u64),
    FailedSwitchThreshold(u64),
    NoPropagatedConfirmation(u64),
}

// Implement a destructor for the ReplayStage thread to signal it exited
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

struct LastVoteRefreshTime {
    last_refresh_time: Instant,
    last_print_time: Instant,
}

#[derive(Default)]
struct SkippedSlotsInfo {
    last_retransmit_slot: u64,
    last_skipped_slot: u64,
}

pub struct ReplayStageConfig {
    pub vote_account: Pubkey,
    pub authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
    pub exit: Arc<AtomicBool>,
    pub rpc_subscriptions: Arc<RpcSubscriptions>,
    pub leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub latest_root_senders: Vec<Sender<Slot>>,
    pub accounts_background_request_sender: AbsRequestSender,
    pub block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    pub transaction_status_sender: Option<TransactionStatusSender>,
    pub rewards_recorder_sender: Option<RewardsRecorderSender>,
    pub cache_block_meta_sender: Option<CacheBlockMetaSender>,
    pub bank_notification_sender: Option<BankNotificationSender>,
    pub wait_for_vote_to_start_leader: bool,
    pub tower_storage: Arc<dyn TowerStorage>,
}

#[derive(Default)]
pub struct ReplayTiming {
    last_print: u64,
    collect_frozen_banks_elapsed: u64,
    compute_bank_stats_elapsed: u64,
    select_vote_and_reset_forks_elapsed: u64,
    start_leader_elapsed: u64,
    reset_bank_elapsed: u64,
    voting_elapsed: u64,
    vote_push_us: u64,
    vote_send_us: u64,
    generate_vote_us: u64,
    update_commitment_cache_us: u64,
    select_forks_elapsed: u64,
    compute_slot_stats_elapsed: u64,
    generate_new_bank_forks_elapsed: u64,
    replay_active_banks_elapsed: u64,
    wait_receive_elapsed: u64,
    heaviest_fork_failures_elapsed: u64,
    bank_count: u64,
    process_gossip_duplicate_confirmed_slots_elapsed: u64,
    process_duplicate_slots_elapsed: u64,
    process_unfrozen_gossip_verified_vote_hashes_elapsed: u64,
    repair_correct_slots_elapsed: u64,
}
impl ReplayTiming {
    #[allow(clippy::too_many_arguments)]
    fn update(
        &mut self,
        collect_frozen_banks_elapsed: u64,
        compute_bank_stats_elapsed: u64,
        select_vote_and_reset_forks_elapsed: u64,
        start_leader_elapsed: u64,
        reset_bank_elapsed: u64,
        voting_elapsed: u64,
        select_forks_elapsed: u64,
        compute_slot_stats_elapsed: u64,
        generate_new_bank_forks_elapsed: u64,
        replay_active_banks_elapsed: u64,
        wait_receive_elapsed: u64,
        heaviest_fork_failures_elapsed: u64,
        bank_count: u64,
        process_gossip_duplicate_confirmed_slots_elapsed: u64,
        process_unfrozen_gossip_verified_vote_hashes_elapsed: u64,
        process_duplicate_slots_elapsed: u64,
        repair_correct_slots_elapsed: u64,
    ) {
        self.collect_frozen_banks_elapsed += collect_frozen_banks_elapsed;
        self.compute_bank_stats_elapsed += compute_bank_stats_elapsed;
        self.select_vote_and_reset_forks_elapsed += select_vote_and_reset_forks_elapsed;
        self.start_leader_elapsed += start_leader_elapsed;
        self.reset_bank_elapsed += reset_bank_elapsed;
        self.voting_elapsed += voting_elapsed;
        self.select_forks_elapsed += select_forks_elapsed;
        self.compute_slot_stats_elapsed += compute_slot_stats_elapsed;
        self.generate_new_bank_forks_elapsed += generate_new_bank_forks_elapsed;
        self.replay_active_banks_elapsed += replay_active_banks_elapsed;
        self.wait_receive_elapsed += wait_receive_elapsed;
        self.heaviest_fork_failures_elapsed += heaviest_fork_failures_elapsed;
        self.bank_count += bank_count;
        self.process_gossip_duplicate_confirmed_slots_elapsed +=
            process_gossip_duplicate_confirmed_slots_elapsed;
        self.process_unfrozen_gossip_verified_vote_hashes_elapsed +=
            process_unfrozen_gossip_verified_vote_hashes_elapsed;
        self.process_duplicate_slots_elapsed += process_duplicate_slots_elapsed;
        self.repair_correct_slots_elapsed += repair_correct_slots_elapsed;
        let now = timestamp();
        let elapsed_ms = now - self.last_print;
        if elapsed_ms > 1000 {
            datapoint_info!(
                "replay-loop-voting-stats",
                ("vote_push_us", self.vote_push_us, i64),
                ("vote_send_us", self.vote_send_us, i64),
                ("generate_vote_us", self.generate_vote_us, i64),
                (
                    "update_commitment_cache_us",
                    self.update_commitment_cache_us,
                    i64
                ),
            );
            datapoint_info!(
                "replay-loop-timing-stats",
                ("total_elapsed_us", elapsed_ms * 1000, i64),
                (
                    "collect_frozen_banks_elapsed",
                    self.collect_frozen_banks_elapsed as i64,
                    i64
                ),
                (
                    "compute_bank_stats_elapsed",
                    self.compute_bank_stats_elapsed as i64,
                    i64
                ),
                (
                    "select_vote_and_reset_forks_elapsed",
                    self.select_vote_and_reset_forks_elapsed as i64,
                    i64
                ),
                (
                    "start_leader_elapsed",
                    self.start_leader_elapsed as i64,
                    i64
                ),
                ("reset_bank_elapsed", self.reset_bank_elapsed as i64, i64),
                ("voting_elapsed", self.voting_elapsed as i64, i64),
                (
                    "select_forks_elapsed",
                    self.select_forks_elapsed as i64,
                    i64
                ),
                (
                    "compute_slot_stats_elapsed",
                    self.compute_slot_stats_elapsed as i64,
                    i64
                ),
                (
                    "generate_new_bank_forks_elapsed",
                    self.generate_new_bank_forks_elapsed as i64,
                    i64
                ),
                (
                    "replay_active_banks_elapsed",
                    self.replay_active_banks_elapsed as i64,
                    i64
                ),
                (
                    "process_gossip_duplicate_confirmed_slots_elapsed",
                    self.process_gossip_duplicate_confirmed_slots_elapsed as i64,
                    i64
                ),
                (
                    "process_unfrozen_gossip_verified_vote_hashes_elapsed",
                    self.process_unfrozen_gossip_verified_vote_hashes_elapsed as i64,
                    i64
                ),
                (
                    "wait_receive_elapsed",
                    self.wait_receive_elapsed as i64,
                    i64
                ),
                (
                    "heaviest_fork_failures_elapsed",
                    self.heaviest_fork_failures_elapsed as i64,
                    i64
                ),
                ("bank_count", self.bank_count as i64, i64),
                (
                    "process_duplicate_slots_elapsed",
                    self.process_duplicate_slots_elapsed as i64,
                    i64
                ),
                (
                    "repair_correct_slots_elapsed",
                    self.repair_correct_slots_elapsed as i64,
                    i64
                )
            );

            *self = ReplayTiming::default();
            self.last_print = now;
        }
    }
}

pub struct ReplayStage {
    t_replay: JoinHandle<()>,
    commitment_service: AggregateCommitmentService,
}

impl ReplayStage {
    #[allow(clippy::new_ret_no_self, clippy::too_many_arguments)]
    pub fn new(
        config: ReplayStageConfig,
        blockstore: Arc<Blockstore>,
        bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
        ledger_signal_receiver: Receiver<bool>,
        duplicate_slots_receiver: DuplicateSlotReceiver,
        poh_recorder: Arc<Mutex<PohRecorder>>,
        mut tower: Tower,
        vote_tracker: Arc<VoteTracker>,
        cluster_slots: Arc<ClusterSlots>,
        retransmit_slots_sender: RetransmitSlotsSender,
        _duplicate_slots_reset_receiver: DuplicateSlotsResetReceiver,
        replay_vote_sender: ReplayVoteSender,
        gossip_duplicate_confirmed_slots_receiver: GossipDuplicateConfirmedSlotsReceiver,
        gossip_verified_vote_hash_receiver: GossipVerifiedVoteHashReceiver,
        cluster_slots_update_sender: ClusterSlotsUpdateSender,
        cost_update_sender: Sender<ExecuteTimings>,
        voting_sender: Sender<VoteOp>,
    ) -> Self {
        let ReplayStageConfig {
            vote_account,
            authorized_voter_keypairs,
            exit,
            rpc_subscriptions,
            leader_schedule_cache,
            latest_root_senders,
            accounts_background_request_sender,
            block_commitment_cache,
            transaction_status_sender,
            rewards_recorder_sender,
            cache_block_meta_sender,
            bank_notification_sender,
            wait_for_vote_to_start_leader,
            tower_storage,
        } = config;

        trace!("replay stage");
        // Start the replay stage loop
        let (lockouts_sender, commitment_service) = AggregateCommitmentService::new(
            &exit,
            block_commitment_cache.clone(),
            rpc_subscriptions.clone(),
        );

        #[allow(clippy::cognitive_complexity)]
        let t_replay = Builder::new()
            .name("solana-replay-stage".to_string())
            .spawn(move || {
                let verify_recyclers = VerifyRecyclers::default();
                let _exit = Finalizer::new(exit.clone());
                let mut identity_keypair = cluster_info.keypair().clone();
                let mut my_pubkey = identity_keypair.pubkey();
                let (
                    mut progress,
                    mut heaviest_subtree_fork_choice,
                ) = Self::initialize_progress_and_fork_choice_with_locked_bank_forks(
                    &bank_forks,
                    &my_pubkey,
                    &vote_account,
                );
                let mut current_leader = None;
                let mut last_reset = Hash::default();
                let mut partition_exists = false;
                let mut skipped_slots_info = SkippedSlotsInfo::default();
                let mut replay_timing = ReplayTiming::default();
                let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
                let mut gossip_duplicate_confirmed_slots: GossipDuplicateConfirmedSlots = GossipDuplicateConfirmedSlots::default();
                let mut duplicate_slots_to_repair = HashSet::new();
                let mut unfrozen_gossip_verified_vote_hashes: UnfrozenGossipVerifiedVoteHashes = UnfrozenGossipVerifiedVoteHashes::default();
                let mut latest_validator_votes_for_frozen_banks: LatestValidatorVotesForFrozenBanks = LatestValidatorVotesForFrozenBanks::default();
                let mut voted_signatures = Vec::new();
                let mut has_new_vote_been_rooted = !wait_for_vote_to_start_leader;
                let mut last_vote_refresh_time = LastVoteRefreshTime {
                    last_refresh_time: Instant::now(),
                    last_print_time: Instant::now(),
                };
                loop {
                    // Stop getting entries if we get exit signal
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }

                    let mut generate_new_bank_forks_time =
                        Measure::start("generate_new_bank_forks_time");
                    Self::generate_new_bank_forks(
                        &blockstore,
                        &bank_forks,
                        &leader_schedule_cache,
                        &rpc_subscriptions,
                        &mut progress,
                    );
                    generate_new_bank_forks_time.stop();

                    let mut tpu_has_bank = poh_recorder.lock().unwrap().has_bank();

                    let mut replay_active_banks_time = Measure::start("replay_active_banks_time");
                    let mut ancestors = bank_forks.read().unwrap().ancestors();
                    let mut descendants = bank_forks.read().unwrap().descendants().clone();
                    let did_complete_bank = Self::replay_active_banks(
                        &blockstore,
                        &bank_forks,
                        &my_pubkey,
                        &vote_account,
                        &mut progress,
                        transaction_status_sender.as_ref(),
                        cache_block_meta_sender.as_ref(),
                        &verify_recyclers,
                        &mut heaviest_subtree_fork_choice,
                        &replay_vote_sender,
                        &bank_notification_sender,
                        &rewards_recorder_sender,
                        &rpc_subscriptions,
                        &mut duplicate_slots_tracker,
                        &gossip_duplicate_confirmed_slots,
                        &mut unfrozen_gossip_verified_vote_hashes,
                        &mut latest_validator_votes_for_frozen_banks,
                        &cluster_slots_update_sender,
                        &cost_update_sender,
                        &mut duplicate_slots_to_repair
                    );
                    replay_active_banks_time.stop();

                    let forks_root = bank_forks.read().unwrap().root();

                    // Check for any newly confirmed slots detected from gossip.
                    let mut process_gossip_duplicate_confirmed_slots_time = Measure::start("process_gossip_duplicate_confirmed_slots");
                    Self::process_gossip_duplicate_confirmed_slots(
                        &gossip_duplicate_confirmed_slots_receiver,
                        &blockstore,
                        &mut duplicate_slots_tracker,
                        &mut gossip_duplicate_confirmed_slots,
                        &bank_forks,
                        &mut progress,
                        &mut heaviest_subtree_fork_choice,
                        &mut duplicate_slots_to_repair
                    );
                    process_gossip_duplicate_confirmed_slots_time.stop();


                    // Ingest any new verified votes from gossip. Important for fork choice
                    // and switching proofs because these may be votes that haven't yet been
                    // included in a block, so we may not have yet observed these votes just
                    // by replaying blocks.
                    let mut process_unfrozen_gossip_verified_vote_hashes_time = Measure::start("process_gossip_duplicate_confirmed_slots");
                    Self::process_gossip_verified_vote_hashes(
                        &gossip_verified_vote_hash_receiver,
                        &mut unfrozen_gossip_verified_vote_hashes,
                        &heaviest_subtree_fork_choice,
                        &mut latest_validator_votes_for_frozen_banks,
                    );
                    for _ in gossip_verified_vote_hash_receiver.try_iter() {}
                    process_unfrozen_gossip_verified_vote_hashes_time.stop();

                    // Check to remove any duplicated slots from fork choice
                    let mut process_duplicate_slots_time = Measure::start("process_duplicate_slots");
                    if !tpu_has_bank {
                        Self::process_duplicate_slots(
                            &blockstore,
                            &duplicate_slots_receiver,
                            &mut duplicate_slots_tracker,
                            &gossip_duplicate_confirmed_slots,
                            &bank_forks,
                            &mut progress,
                            &mut heaviest_subtree_fork_choice,
                            &mut duplicate_slots_to_repair
                        );
                    }
                    process_duplicate_slots_time.stop();

                    let mut collect_frozen_banks_time = Measure::start("frozen_banks");
                    let mut frozen_banks: Vec<_> = bank_forks
                        .read()
                        .unwrap()
                        .frozen_banks()
                        .into_iter()
                        .filter(|(slot, _)| *slot >= forks_root)
                        .map(|(_, bank)| bank)
                        .collect();
                    collect_frozen_banks_time.stop();

                    let mut compute_bank_stats_time = Measure::start("compute_bank_stats");
                    let newly_computed_slot_stats = Self::compute_bank_stats(
                        &vote_account,
                        &ancestors,
                        &mut frozen_banks,
                        &tower,
                        &mut progress,
                        &vote_tracker,
                        &cluster_slots,
                        &bank_forks,
                        &mut heaviest_subtree_fork_choice,
                        &mut latest_validator_votes_for_frozen_banks,
                    );
                    compute_bank_stats_time.stop();

                    let mut compute_slot_stats_time = Measure::start("compute_slot_stats_time");
                    for slot in newly_computed_slot_stats {
                        let fork_stats = progress.get_fork_stats(slot).unwrap();
                        let confirmed_forks = Self::confirm_forks(
                            &tower,
                            &fork_stats.voted_stakes,
                            fork_stats.total_stake,
                            &progress,
                            &bank_forks,
                        );

                        Self::mark_slots_confirmed(&confirmed_forks, &blockstore, &bank_forks, &mut progress, &mut duplicate_slots_tracker, &mut heaviest_subtree_fork_choice,  &mut duplicate_slots_to_repair);
                    }
                    compute_slot_stats_time.stop();

                    let mut select_forks_time = Measure::start("select_forks_time");
                    let (heaviest_bank, heaviest_bank_on_same_voted_fork) = heaviest_subtree_fork_choice
                        .select_forks(&frozen_banks, &tower, &progress, &ancestors, &bank_forks);
                    select_forks_time.stop();

                    if let Some(heaviest_bank_on_same_voted_fork) = heaviest_bank_on_same_voted_fork.as_ref() {
                        if let Some(my_latest_landed_vote) = progress.my_latest_landed_vote(heaviest_bank_on_same_voted_fork.slot()) {
                            Self::refresh_last_vote(&mut tower,
                                                    heaviest_bank_on_same_voted_fork,
                                                    my_latest_landed_vote,
                                                    &vote_account,
                                                    &identity_keypair,
                                                    &authorized_voter_keypairs.read().unwrap(),
                                                    &mut voted_signatures,
                                                    has_new_vote_been_rooted, &mut
                                                    last_vote_refresh_time,
                                                    &voting_sender,
                                                    );
                        }
                    }

                    let mut select_vote_and_reset_forks_time =
                        Measure::start("select_vote_and_reset_forks");
                    let SelectVoteAndResetForkResult {
                        vote_bank,
                        reset_bank,
                        heaviest_fork_failures,
                    } = Self::select_vote_and_reset_forks(
                        &heaviest_bank,
                        heaviest_bank_on_same_voted_fork.as_ref(),
                        &ancestors,
                        &descendants,
                        &progress,
                        &mut tower,
                        &latest_validator_votes_for_frozen_banks,
                        &heaviest_subtree_fork_choice,
                    );
                    select_vote_and_reset_forks_time.stop();

                    let mut heaviest_fork_failures_time = Measure::start("heaviest_fork_failures_time");
                    if tower.is_recent(heaviest_bank.slot()) && !heaviest_fork_failures.is_empty() {
                        info!(
                            "Couldn't vote on heaviest fork: {:?}, heaviest_fork_failures: {:?}",
                            heaviest_bank.slot(),
                            heaviest_fork_failures
                        );

                        for r in heaviest_fork_failures {
                            if let HeaviestForkFailures::NoPropagatedConfirmation(slot) = r {
                                if let Some(latest_leader_slot) =
                                    progress.get_latest_leader_slot(slot)
                                {
                                    progress.log_propagated_stats(latest_leader_slot, &bank_forks);
                                }
                            }
                        }
                    }
                    heaviest_fork_failures_time.stop();

                    let mut voting_time = Measure::start("voting_time");
                    // Vote on a fork
                    if let Some((ref vote_bank, ref switch_fork_decision)) = vote_bank {
                        if let Some(votable_leader) =
                            leader_schedule_cache.slot_leader_at(vote_bank.slot(), Some(vote_bank))
                        {
                            Self::log_leader_change(
                                &my_pubkey,
                                vote_bank.slot(),
                                &mut current_leader,
                                &votable_leader,
                            );
                        }

                        Self::handle_votable_bank(
                            vote_bank,
                            switch_fork_decision,
                            &bank_forks,
                            &mut tower,
                            &mut progress,
                            &vote_account,
                            &identity_keypair,
                            &authorized_voter_keypairs.read().unwrap(),
                            &blockstore,
                            &leader_schedule_cache,
                            &lockouts_sender,
                            &accounts_background_request_sender,
                            &latest_root_senders,
                            &rpc_subscriptions,
                            &block_commitment_cache,
                            &mut heaviest_subtree_fork_choice,
                            &bank_notification_sender,
                            &mut duplicate_slots_tracker,
                            &mut gossip_duplicate_confirmed_slots,
                            &mut unfrozen_gossip_verified_vote_hashes,
                            &mut voted_signatures,
                            &mut has_new_vote_been_rooted,
                            &mut replay_timing,
                            &voting_sender,
                        );
                    };
                    voting_time.stop();

                    let mut reset_bank_time = Measure::start("reset_bank");
                    // Reset onto a fork
                    if let Some(reset_bank) = reset_bank {
                        if last_reset != reset_bank.last_blockhash() {
                            info!(
                                "vote bank: {:?} reset bank: {:?}",
                                vote_bank.as_ref().map(|(b, switch_fork_decision)| (
                                    b.slot(),
                                    switch_fork_decision
                                )),
                                reset_bank.slot(),
                            );
                            let fork_progress = progress
                                .get(&reset_bank.slot())
                                .expect("bank to reset to must exist in progress map");
                            datapoint_info!(
                                "blocks_produced",
                                ("num_blocks_on_fork", fork_progress.num_blocks_on_fork, i64),
                                (
                                    "num_dropped_blocks_on_fork",
                                    fork_progress.num_dropped_blocks_on_fork,
                                    i64
                                ),
                            );

                            if my_pubkey != cluster_info.id() {
                                identity_keypair = cluster_info.keypair().clone();
                                let my_old_pubkey = my_pubkey;
                                my_pubkey = identity_keypair.pubkey();

                                // Load the new identity's tower
                                tower = Tower::restore(tower_storage.as_ref(), &my_pubkey)
                                    .and_then(|restored_tower| {
                                        let root_bank = bank_forks.read().unwrap().root_bank();
                                        let slot_history = root_bank.get_slot_history();
                                        restored_tower.adjust_lockouts_after_replay(root_bank.slot(), &slot_history)
                                    }).
                                    unwrap_or_else(|err| {
                                        // It's a fatal error if the tower is not present.  This is
                                        // necessary to prevent the validator from violating
                                        // lockouts for its new identity
                                        error!("Failed to load tower for {}: {}", my_pubkey, err);
                                        std::process::exit(1);
                                    });

                                // Ensure the validator can land votes with the new identity before
                                // becoming leader
                                has_new_vote_been_rooted = !wait_for_vote_to_start_leader;
                                warn!("Identity changed from {} to {}", my_old_pubkey, my_pubkey);
                            }

                            Self::reset_poh_recorder(
                                &my_pubkey,
                                &blockstore,
                                &reset_bank,
                                &poh_recorder,
                                &leader_schedule_cache,
                            );
                            last_reset = reset_bank.last_blockhash();
                            tpu_has_bank = false;

                            if let Some(last_voted_slot) = tower.last_voted_slot() {
                                // If the current heaviest bank is not a descendant of the last voted slot,
                                // there must be a partition
                                let partition_detected = Self::is_partition_detected(&ancestors, last_voted_slot, heaviest_bank.slot());

                                if !partition_exists && partition_detected
                                {
                                    warn!(
                                        "PARTITION DETECTED waiting to join heaviest fork: {} last vote: {:?}, reset slot: {}",
                                        heaviest_bank.slot(),
                                        last_voted_slot,
                                        reset_bank.slot(),
                                    );
                                    inc_new_counter_info!("replay_stage-partition_detected", 1);
                                    datapoint_info!(
                                        "replay_stage-partition",
                                        ("slot", reset_bank.slot() as i64, i64)
                                    );
                                    partition_exists = true;
                                } else if partition_exists
                                    && !partition_detected
                                {
                                    warn!(
                                        "PARTITION resolved heaviest fork: {} last vote: {:?}, reset slot: {}",
                                        heaviest_bank.slot(),
                                        last_voted_slot,
                                        reset_bank.slot()
                                    );
                                    partition_exists = false;
                                    inc_new_counter_info!("replay_stage-partition_resolved", 1);
                                }
                            }
                        }
                    }
                    reset_bank_time.stop();

                    let mut start_leader_time = Measure::start("start_leader_time");
                    let mut dump_then_repair_correct_slots_time = Measure::start("dump_then_repair_correct_slots_time");
                    // Used for correctness check
                    let poh_bank = poh_recorder.lock().unwrap().bank();
                    // Dump any duplicate slots that have been confirmed by the network in
                    // anticipation of repairing the confirmed version of the slot.
                    //
                    // Has to be before `maybe_start_leader()`. Otherwise, `ancestors` and `descendants`
                    // will be outdated, and we cannot assume `poh_bank` will be in either of these maps.
                    Self::dump_then_repair_correct_slots(&mut duplicate_slots_to_repair, &mut ancestors, &mut descendants, &mut progress, &bank_forks, &blockstore, poh_bank.map(|bank| bank.slot()));
                    dump_then_repair_correct_slots_time.stop();

                    // From this point on, its not safe to use ancestors/descendants since maybe_start_leader
                    // may add a bank that will not included in either of these maps.
                    drop(ancestors);
                    drop(descendants);
                    if !tpu_has_bank {
                        Self::maybe_start_leader(
                            &my_pubkey,
                            &bank_forks,
                            &poh_recorder,
                            &leader_schedule_cache,
                            &rpc_subscriptions,
                            &progress,
                            &retransmit_slots_sender,
                            &mut skipped_slots_info,
                            has_new_vote_been_rooted,
                        );

                        let poh_bank = poh_recorder.lock().unwrap().bank();
                        if let Some(bank) = poh_bank {
                            Self::log_leader_change(
                                &my_pubkey,
                                bank.slot(),
                                &mut current_leader,
                                &my_pubkey,
                            );
                        }
                    }
                    start_leader_time.stop();

                    let mut wait_receive_time = Measure::start("wait_receive_time");
                    if !did_complete_bank {
                        // only wait for the signal if we did not just process a bank; maybe there are more slots available

                        let timer = Duration::from_millis(100);
                        let result = ledger_signal_receiver.recv_timeout(timer);
                        match result {
                            Err(RecvTimeoutError::Timeout) => (),
                            Err(_) => break,
                            Ok(_) => trace!("blockstore signal"),
                        };
                    }
                    wait_receive_time.stop();

                    replay_timing.update(
                        collect_frozen_banks_time.as_us(),
                        compute_bank_stats_time.as_us(),
                        select_vote_and_reset_forks_time.as_us(),
                        start_leader_time.as_us(),
                        reset_bank_time.as_us(),
                        voting_time.as_us(),
                        select_forks_time.as_us(),
                        compute_slot_stats_time.as_us(),
                        generate_new_bank_forks_time.as_us(),
                        replay_active_banks_time.as_us(),
                        wait_receive_time.as_us(),
                        heaviest_fork_failures_time.as_us(),
                        if did_complete_bank {1} else {0},
                        process_gossip_duplicate_confirmed_slots_time.as_us(),
                        process_unfrozen_gossip_verified_vote_hashes_time.as_us(),
                        process_duplicate_slots_time.as_us(),
                        dump_then_repair_correct_slots_time.as_us(),
                    );
                }
            })
            .unwrap();

        Self {
            t_replay,
            commitment_service,
        }
    }

    fn is_partition_detected(
        ancestors: &HashMap<Slot, HashSet<Slot>>,
        last_voted_slot: Slot,
        heaviest_slot: Slot,
    ) -> bool {
        last_voted_slot != heaviest_slot
            && !ancestors
                .get(&heaviest_slot)
                .map(|ancestors| ancestors.contains(&last_voted_slot))
                .unwrap_or(true)
    }

    fn initialize_progress_and_fork_choice_with_locked_bank_forks(
        bank_forks: &RwLock<BankForks>,
        my_pubkey: &Pubkey,
        vote_account: &Pubkey,
    ) -> (ProgressMap, HeaviestSubtreeForkChoice) {
        let (root_bank, frozen_banks) = {
            let bank_forks = bank_forks.read().unwrap();
            (
                bank_forks.root_bank(),
                bank_forks.frozen_banks().values().cloned().collect(),
            )
        };

        Self::initialize_progress_and_fork_choice(&root_bank, frozen_banks, my_pubkey, vote_account)
    }

    pub fn initialize_progress_and_fork_choice(
        root_bank: &Bank,
        mut frozen_banks: Vec<Arc<Bank>>,
        my_pubkey: &Pubkey,
        vote_account: &Pubkey,
    ) -> (ProgressMap, HeaviestSubtreeForkChoice) {
        let mut progress = ProgressMap::default();

        frozen_banks.sort_by_key(|bank| bank.slot());

        // Initialize progress map with any root banks
        for bank in &frozen_banks {
            let prev_leader_slot = progress.get_bank_prev_leader_slot(bank);
            progress.insert(
                bank.slot(),
                ForkProgress::new_from_bank(bank, my_pubkey, vote_account, prev_leader_slot, 0, 0),
            );
        }
        let root = root_bank.slot();
        let heaviest_subtree_fork_choice = HeaviestSubtreeForkChoice::new_from_frozen_banks(
            (root, root_bank.hash()),
            &frozen_banks,
        );

        (progress, heaviest_subtree_fork_choice)
    }

    fn dump_then_repair_correct_slots(
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
        ancestors: &mut HashMap<Slot, HashSet<Slot>>,
        descendants: &mut HashMap<Slot, HashSet<Slot>>,
        progress: &mut ProgressMap,
        bank_forks: &RwLock<BankForks>,
        blockstore: &Blockstore,
        poh_bank_slot: Option<Slot>,
    ) {
        if duplicate_slots_to_repair.is_empty() {
            return;
        }

        let root_bank = bank_forks.read().unwrap().root_bank();
        // TODO: handle if alternate version of descendant also got confirmed after ancestor was
        // confirmed, what happens then? Should probably keep track of purged list and skip things
        // in `duplicate_slots_to_repair` that have already been purged. Add test.
        duplicate_slots_to_repair.retain(|(duplicate_slot, correct_hash)| {
            // Should not purge duplicate slots if there is currently a poh bank building
            // on top of that slot, as BankingStage might still be referencing/touching that state
            // concurrently.
            // Luckily for us, because the fork choice rule removes duplicate slots from fork
            // choice, and this function is called after:
            // 1) We have picked a bank to reset to in `select_vote_and_reset_forks()`
            // 2) And also called `reset_poh_recorder()`
            // Then we should have reset to a fork that doesn't include the duplicate block,
            // which means any working bank in PohRecorder that was built on that duplicate fork
            // should have been cleared as well. However, if there is some violation of this guarantee,
            // then log here
            let is_poh_building_on_duplicate_fork = poh_bank_slot
                .map(|poh_bank_slot| {
                    ancestors
                        .get(&poh_bank_slot)
                        .expect("Poh bank should exist in BankForks and thus in ancestors map")
                        .contains(duplicate_slot)
                })
                .unwrap_or(false);

            let did_purge_repair = {
                if !is_poh_building_on_duplicate_fork {
                    let frozen_hash = bank_forks.read().unwrap().bank_hash(*duplicate_slot);
                    if let Some(frozen_hash) = frozen_hash {
                        if frozen_hash == *correct_hash {
                            warn!(
                                "Trying to purge slot {} with correct_hash {}",
                                *duplicate_slot, *correct_hash
                            );
                            return false;
                        } else if frozen_hash == Hash::default()
                            && !progress.is_dead(*duplicate_slot).expect(
                                "If slot exists in BankForks must exist in the progress map",
                            )
                        {
                            warn!(
                                "Trying to purge unfrozen slot {} that is not dead",
                                *duplicate_slot
                            );
                            return false;
                        }
                    } else {
                        warn!(
                            "Trying to purge slot {} which does not exist in bank forks",
                            *duplicate_slot
                        );
                        return false;
                    }

                    Self::purge_unconfirmed_duplicate_slot(
                        *duplicate_slot,
                        ancestors,
                        descendants,
                        progress,
                        &root_bank,
                        bank_forks,
                        blockstore,
                    );
                    true
                // TODO: Send signal to repair to repair the correct version of
                // `duplicate_slot` with hash == `correct_hash`
                } else {
                    warn!(
                        "PoH bank for slot {} is building on duplicate slot {}",
                        poh_bank_slot.unwrap(),
                        duplicate_slot
                    );
                    false
                }
            };

            // If we purged/repaired, then no need to keep the slot in the set of pending work
            !did_purge_repair
        });
    }

    fn purge_unconfirmed_duplicate_slot(
        duplicate_slot: Slot,
        ancestors: &mut HashMap<Slot, HashSet<Slot>>,
        descendants: &mut HashMap<Slot, HashSet<Slot>>,
        progress: &mut ProgressMap,
        root_bank: &Bank,
        bank_forks: &RwLock<BankForks>,
        blockstore: &Blockstore,
    ) {
        warn!("purging slot {}", duplicate_slot);

        // Doesn't need to be root bank, just needs a common bank to
        // access the status cache and accounts
        let slot_descendants = descendants.get(&duplicate_slot).cloned();
        if slot_descendants.is_none() {
            // Root has already moved past this slot, no need to purge it
            if root_bank.slot() <= duplicate_slot {
                blockstore.clear_unconfirmed_slot(duplicate_slot);
            }

            return;
        }

        // Clear the ancestors/descendants map to keep them
        // consistent
        let slot_descendants = slot_descendants.unwrap();
        Self::purge_ancestors_descendants(
            duplicate_slot,
            &slot_descendants,
            ancestors,
            descendants,
        );

        // Grab the Slot and BankId's of the banks we need to purge, then clear the banks
        // from BankForks
        let (slots_to_purge, removed_banks): (Vec<(Slot, BankId)>, Vec<Arc<Bank>>) = {
            let mut w_bank_forks = bank_forks.write().unwrap();
            slot_descendants
                .iter()
                .chain(std::iter::once(&duplicate_slot))
                .map(|slot| {
                    // Clear the duplicate banks from BankForks
                    let bank = w_bank_forks
                        .remove(*slot)
                        .expect("BankForks should not have been purged yet");
                    ((*slot, bank.bank_id()), bank)
                })
                .unzip()
        };

        // Clear the accounts for these slots so that any ongoing RPC scans fail.
        // These have to be atomically cleared together in the same batch, in order
        // to prevent RPC from seeing inconsistent results in scans.
        root_bank.remove_unrooted_slots(&slots_to_purge);

        // Once the slots above have been purged, now it's safe to remove the banks from
        // BankForks, allowing the Bank::drop() purging to run and not race with the
        // `remove_unrooted_slots()` call.
        drop(removed_banks);

        for (slot, slot_id) in slots_to_purge {
            warn!(
                "purging descendant: {} with slot_id {}, of slot {}",
                slot, slot_id, duplicate_slot
            );
            // Clear the slot signatures from status cache for this slot.
            // TODO: What about RPC queries that had already cloned the Bank for this slot
            // and are looking up the signature for this slot?
            root_bank.clear_slot_signatures(slot);

            // Clear the slot-related data in blockstore. This will:
            // 1) Clear old shreds allowing new ones to be inserted
            // 2) Clear the "dead" flag allowing ReplayStage to start replaying
            // this slot
            blockstore.clear_unconfirmed_slot(slot);

            // Clear the progress map of these forks
            let _ = progress.remove(&slot);
        }
    }

    // Purge given slot and all its descendants from the `ancestors` and
    // `descendants` structures so that they're consistent with `BankForks`
    // and the `progress` map.
    fn purge_ancestors_descendants(
        slot: Slot,
        slot_descendants: &HashSet<Slot>,
        ancestors: &mut HashMap<Slot, HashSet<Slot>>,
        descendants: &mut HashMap<Slot, HashSet<Slot>>,
    ) {
        if !ancestors.contains_key(&slot) {
            // Slot has already been purged
            return;
        }

        // Purge this slot from each of its ancestors' `descendants` maps
        for a in ancestors
            .get(&slot)
            .expect("must exist based on earlier check")
        {
            descendants
                .get_mut(a)
                .expect("If exists in ancestor map must exist in descendants map")
                .retain(|d| *d != slot && !slot_descendants.contains(d));
        }
        ancestors
            .remove(&slot)
            .expect("must exist based on earlier check");

        // Purge all the descendants of this slot from both maps
        for descendant in slot_descendants {
            ancestors.remove(descendant).expect("must exist");
            descendants
                .remove(descendant)
                .expect("must exist based on earlier check");
        }
        descendants
            .remove(&slot)
            .expect("must exist based on earlier check");
    }

    // Check for any newly confirmed slots by the cluster. This is only detects
    // optimistic and in the future, duplicate slot confirmations on the exact
    // single slots and does not account for votes on their descendants. Used solely
    // for duplicate slot recovery.
    fn process_gossip_duplicate_confirmed_slots(
        gossip_duplicate_confirmed_slots_receiver: &GossipDuplicateConfirmedSlotsReceiver,
        blockstore: &Blockstore,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        gossip_duplicate_confirmed_slots: &mut GossipDuplicateConfirmedSlots,
        bank_forks: &RwLock<BankForks>,
        progress: &mut ProgressMap,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
    ) {
        let root = bank_forks.read().unwrap().root();
        for new_confirmed_slots in gossip_duplicate_confirmed_slots_receiver.try_iter() {
            for (confirmed_slot, duplicate_confirmed_hash) in new_confirmed_slots {
                if confirmed_slot <= root {
                    continue;
                } else if let Some(prev_hash) = gossip_duplicate_confirmed_slots
                    .insert(confirmed_slot, duplicate_confirmed_hash)
                {
                    assert_eq!(prev_hash, duplicate_confirmed_hash);
                    // Already processed this signal
                    return;
                }

                let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
                    duplicate_confirmed_hash,
                    || progress.is_dead(confirmed_slot).unwrap_or(false),
                    || bank_forks.read().unwrap().bank_hash(confirmed_slot),
                );
                check_slot_agrees_with_cluster(
                    confirmed_slot,
                    root,
                    blockstore,
                    duplicate_slots_tracker,
                    fork_choice,
                    duplicate_slots_to_repair,
                    SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
                );
            }
        }
    }

    fn process_gossip_verified_vote_hashes(
        gossip_verified_vote_hash_receiver: &GossipVerifiedVoteHashReceiver,
        unfrozen_gossip_verified_vote_hashes: &mut UnfrozenGossipVerifiedVoteHashes,
        heaviest_subtree_fork_choice: &HeaviestSubtreeForkChoice,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
    ) {
        for (pubkey, slot, hash) in gossip_verified_vote_hash_receiver.try_iter() {
            let is_frozen = heaviest_subtree_fork_choice.contains_block(&(slot, hash));
            // cluster_info_vote_listener will ensure it doesn't push duplicates
            unfrozen_gossip_verified_vote_hashes.add_vote(
                pubkey,
                slot,
                hash,
                is_frozen,
                latest_validator_votes_for_frozen_banks,
            )
        }
    }

    // Checks for and handle forks with duplicate slots.
    fn process_duplicate_slots(
        blockstore: &Blockstore,
        duplicate_slots_receiver: &DuplicateSlotReceiver,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        gossip_duplicate_confirmed_slots: &GossipDuplicateConfirmedSlots,
        bank_forks: &RwLock<BankForks>,
        progress: &mut ProgressMap,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
    ) {
        let new_duplicate_slots: Vec<Slot> = duplicate_slots_receiver.try_iter().collect();
        let (root_slot, bank_hashes) = {
            let r_bank_forks = bank_forks.read().unwrap();
            let bank_hashes: Vec<Option<Hash>> = new_duplicate_slots
                .iter()
                .map(|duplicate_slot| r_bank_forks.bank_hash(*duplicate_slot))
                .collect();

            (r_bank_forks.root(), bank_hashes)
        };
        for (duplicate_slot, bank_hash) in
            new_duplicate_slots.into_iter().zip(bank_hashes.into_iter())
        {
            // WindowService should only send the signal once per slot
            let duplicate_state = DuplicateState::new_from_state(
                duplicate_slot,
                gossip_duplicate_confirmed_slots,
                fork_choice,
                || progress.is_dead(duplicate_slot).unwrap_or(false),
                || bank_hash,
            );
            check_slot_agrees_with_cluster(
                duplicate_slot,
                root_slot,
                blockstore,
                duplicate_slots_tracker,
                fork_choice,
                duplicate_slots_to_repair,
                SlotStateUpdate::Duplicate(duplicate_state),
            );
        }
    }

    fn log_leader_change(
        my_pubkey: &Pubkey,
        bank_slot: Slot,
        current_leader: &mut Option<Pubkey>,
        new_leader: &Pubkey,
    ) {
        if let Some(ref current_leader) = current_leader {
            if current_leader != new_leader {
                let msg = if current_leader == my_pubkey {
                    ". I am no longer the leader"
                } else if new_leader == my_pubkey {
                    ". I am now the leader"
                } else {
                    ""
                };
                info!(
                    "LEADER CHANGE at slot: {} leader: {}{}",
                    bank_slot, new_leader, msg
                );
            }
        }
        current_leader.replace(new_leader.to_owned());
    }

    fn check_propagation_for_start_leader(
        poh_slot: Slot,
        parent_slot: Slot,
        progress_map: &ProgressMap,
    ) -> bool {
        // Assume `NUM_CONSECUTIVE_LEADER_SLOTS` = 4. Then `skip_propagated_check`
        // below is true if `poh_slot` is within the same `NUM_CONSECUTIVE_LEADER_SLOTS`
        // set of blocks as `latest_leader_slot`.
        //
        // Example 1 (`poh_slot` directly descended from `latest_leader_slot`):
        //
        // [B B B B] [B B B latest_leader_slot] poh_slot
        //
        // Example 2:
        //
        // [B latest_leader_slot B poh_slot]
        //
        // In this example, even if there's a block `B` on another fork between
        // `poh_slot` and `parent_slot`, because they're in the same
        // `NUM_CONSECUTIVE_LEADER_SLOTS` block, we still skip the propagated
        // check because it's still within the propagation grace period.
        if let Some(latest_leader_slot) = progress_map.get_latest_leader_slot(parent_slot) {
            let skip_propagated_check =
                poh_slot - latest_leader_slot < NUM_CONSECUTIVE_LEADER_SLOTS;
            if skip_propagated_check {
                return true;
            }
        }

        // Note that `is_propagated(parent_slot)` doesn't necessarily check
        // propagation of `parent_slot`, it checks propagation of the latest ancestor
        // of `parent_slot` (hence the call to `get_latest_leader_slot()` in the
        // check above)
        progress_map.is_propagated(parent_slot)
    }

    fn should_retransmit(poh_slot: Slot, last_retransmit_slot: &mut Slot) -> bool {
        if poh_slot < *last_retransmit_slot
            || poh_slot >= *last_retransmit_slot + NUM_CONSECUTIVE_LEADER_SLOTS
        {
            *last_retransmit_slot = poh_slot;
            true
        } else {
            false
        }
    }

    fn maybe_start_leader(
        my_pubkey: &Pubkey,
        bank_forks: &Arc<RwLock<BankForks>>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        progress_map: &ProgressMap,
        retransmit_slots_sender: &RetransmitSlotsSender,
        skipped_slots_info: &mut SkippedSlotsInfo,
        has_new_vote_been_rooted: bool,
    ) {
        // all the individual calls to poh_recorder.lock() are designed to
        // increase granularity, decrease contention

        assert!(!poh_recorder.lock().unwrap().has_bank());

        let (reached_leader_slot, _grace_ticks, poh_slot, parent_slot) =
            poh_recorder.lock().unwrap().reached_leader_slot();

        if !reached_leader_slot {
            trace!("{} poh_recorder hasn't reached_leader_slot", my_pubkey);
            return;
        }
        trace!("{} reached_leader_slot", my_pubkey);

        let parent = bank_forks
            .read()
            .unwrap()
            .get(parent_slot)
            .expect("parent_slot doesn't exist in bank forks")
            .clone();

        assert!(parent.is_frozen());

        if bank_forks.read().unwrap().get(poh_slot).is_some() {
            warn!("{} already have bank in forks at {}?", my_pubkey, poh_slot);
            return;
        }
        trace!(
            "{} poh_slot {} parent_slot {}",
            my_pubkey,
            poh_slot,
            parent_slot
        );

        if let Some(next_leader) = leader_schedule_cache.slot_leader_at(poh_slot, Some(&parent)) {
            if !has_new_vote_been_rooted {
                info!("Haven't landed a vote, so skipping my leader slot");
                return;
            }

            trace!(
                "{} leader {} at poh slot: {}",
                my_pubkey,
                next_leader,
                poh_slot
            );

            // I guess I missed my slot
            if next_leader != *my_pubkey {
                return;
            }

            datapoint_info!(
                "replay_stage-new_leader",
                ("slot", poh_slot, i64),
                ("leader", next_leader.to_string(), String),
            );

            if !Self::check_propagation_for_start_leader(poh_slot, parent_slot, progress_map) {
                let latest_unconfirmed_leader_slot = progress_map.get_latest_leader_slot(parent_slot)
                    .expect("In order for propagated check to fail, latest leader must exist in progress map");
                if poh_slot != skipped_slots_info.last_skipped_slot {
                    datapoint_info!(
                        "replay_stage-skip_leader_slot",
                        ("slot", poh_slot, i64),
                        ("parent_slot", parent_slot, i64),
                        (
                            "latest_unconfirmed_leader_slot",
                            latest_unconfirmed_leader_slot,
                            i64
                        )
                    );
                    progress_map.log_propagated_stats(latest_unconfirmed_leader_slot, bank_forks);
                    skipped_slots_info.last_skipped_slot = poh_slot;
                }
                let bank = bank_forks
                    .read()
                    .unwrap()
                    .get(latest_unconfirmed_leader_slot)
                    .expect(
                        "In order for propagated check to fail, \
                            latest leader must exist in progress map, and thus also in BankForks",
                    )
                    .clone();

                // Signal retransmit
                if Self::should_retransmit(poh_slot, &mut skipped_slots_info.last_retransmit_slot) {
                    datapoint_info!("replay_stage-retransmit", ("slot", bank.slot(), i64),);
                    let _ = retransmit_slots_sender
                        .send(vec![(bank.slot(), bank.clone())].into_iter().collect());
                }
                return;
            }

            let root_slot = bank_forks.read().unwrap().root();
            datapoint_info!("replay_stage-my_leader_slot", ("slot", poh_slot, i64),);
            info!(
                "new fork:{} parent:{} (leader) root:{}",
                poh_slot, parent_slot, root_slot
            );

            let tpu_bank = Self::new_bank_from_parent_with_notify(
                &parent,
                poh_slot,
                root_slot,
                my_pubkey,
                rpc_subscriptions,
            );

            let tpu_bank = bank_forks.write().unwrap().insert(tpu_bank);
            poh_recorder.lock().unwrap().set_bank(&tpu_bank);
        } else {
            error!("{} No next leader found", my_pubkey);
        }
    }

    fn replay_blockstore_into_bank(
        bank: &Arc<Bank>,
        blockstore: &Blockstore,
        bank_progress: &mut ForkProgress,
        transaction_status_sender: Option<&TransactionStatusSender>,
        replay_vote_sender: &ReplayVoteSender,
        verify_recyclers: &VerifyRecyclers,
    ) -> result::Result<usize, BlockstoreProcessorError> {
        let tx_count_before = bank_progress.replay_progress.num_txs;
        let confirm_result = blockstore_processor::confirm_slot(
            blockstore,
            bank,
            &mut bank_progress.replay_stats,
            &mut bank_progress.replay_progress,
            false,
            transaction_status_sender,
            Some(replay_vote_sender),
            None,
            verify_recyclers,
            false,
        );
        let tx_count_after = bank_progress.replay_progress.num_txs;
        let tx_count = tx_count_after - tx_count_before;
        confirm_result.map_err(|err| {
            // All errors must lead to marking the slot as dead, otherwise,
            // the `check_slot_agrees_with_cluster()` called by `replay_active_banks()`
            // will break!
            err
        })?;

        Ok(tx_count)
    }

    #[allow(clippy::too_many_arguments)]
    fn mark_dead_slot(
        blockstore: &Blockstore,
        bank: &Bank,
        root: Slot,
        err: &BlockstoreProcessorError,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        gossip_duplicate_confirmed_slots: &GossipDuplicateConfirmedSlots,
        progress: &mut ProgressMap,
        heaviest_subtree_fork_choice: &mut HeaviestSubtreeForkChoice,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
    ) {
        // Do not remove from progress map when marking dead! Needed by
        // `process_gossip_duplicate_confirmed_slots()`

        // Block producer can abandon the block if it detects a better one
        // while producing. Somewhat common and expected in a
        // network with variable network/machine configuration.
        let is_serious = !matches!(
            err,
            BlockstoreProcessorError::InvalidBlock(BlockError::TooFewTicks)
        );
        let slot = bank.slot();
        if is_serious {
            datapoint_error!(
                "replay-stage-mark_dead_slot",
                ("error", format!("error: {:?}", err), String),
                ("slot", slot, i64)
            );
        } else {
            datapoint_info!(
                "replay-stage-mark_dead_slot",
                ("error", format!("error: {:?}", err), String),
                ("slot", slot, i64)
            );
        }
        progress.get_mut(&slot).unwrap().is_dead = true;
        blockstore
            .set_dead_slot(slot)
            .expect("Failed to mark slot as dead in blockstore");
        rpc_subscriptions.notify_slot_update(SlotUpdate::Dead {
            slot,
            err: format!("error: {:?}", err),
            timestamp: timestamp(),
        });
        let dead_state = DeadState::new_from_state(
            slot,
            duplicate_slots_tracker,
            gossip_duplicate_confirmed_slots,
            heaviest_subtree_fork_choice,
        );
        check_slot_agrees_with_cluster(
            slot,
            root,
            blockstore,
            duplicate_slots_tracker,
            heaviest_subtree_fork_choice,
            duplicate_slots_to_repair,
            SlotStateUpdate::Dead(dead_state),
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_votable_bank(
        bank: &Arc<Bank>,
        switch_fork_decision: &SwitchForkDecision,
        bank_forks: &Arc<RwLock<BankForks>>,
        tower: &mut Tower,
        progress: &mut ProgressMap,
        vote_account_pubkey: &Pubkey,
        identity_keypair: &Keypair,
        authorized_voter_keypairs: &[Arc<Keypair>],
        blockstore: &Arc<Blockstore>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        lockouts_sender: &Sender<CommitmentAggregationData>,
        accounts_background_request_sender: &AbsRequestSender,
        latest_root_senders: &[Sender<Slot>],
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        block_commitment_cache: &Arc<RwLock<BlockCommitmentCache>>,
        heaviest_subtree_fork_choice: &mut HeaviestSubtreeForkChoice,
        bank_notification_sender: &Option<BankNotificationSender>,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        gossip_duplicate_confirmed_slots: &mut GossipDuplicateConfirmedSlots,
        unfrozen_gossip_verified_vote_hashes: &mut UnfrozenGossipVerifiedVoteHashes,
        vote_signatures: &mut Vec<Signature>,
        has_new_vote_been_rooted: &mut bool,
        replay_timing: &mut ReplayTiming,
        voting_sender: &Sender<VoteOp>,
    ) {
        if bank.is_empty() {
            inc_new_counter_info!("replay_stage-voted_empty_bank", 1);
        }
        trace!("handle votable bank {}", bank.slot());
        let new_root = tower.record_bank_vote(bank, vote_account_pubkey);

        let saved_tower = SavedTower::new(&tower, identity_keypair).unwrap_or_else(|err| {
            error!("Unable to create saved tower: {:?}", err);
            std::process::exit(1);
        });

        if let Some(new_root) = new_root {
            // get the root bank before squash
            let root_bank = bank_forks
                .read()
                .unwrap()
                .get(new_root)
                .expect("Root bank doesn't exist")
                .clone();
            let mut rooted_banks = root_bank.parents();
            rooted_banks.push(root_bank.clone());
            let rooted_slots: Vec<_> = rooted_banks.iter().map(|bank| bank.slot()).collect();
            // Call leader schedule_cache.set_root() before blockstore.set_root() because
            // bank_forks.root is consumed by repair_service to update gossip, so we don't want to
            // get shreds for repair on gossip before we update leader schedule, otherwise they may
            // get dropped.
            leader_schedule_cache.set_root(rooted_banks.last().unwrap());
            blockstore
                .set_roots(rooted_slots.iter())
                .expect("Ledger set roots failed");
            let highest_confirmed_root = Some(
                block_commitment_cache
                    .read()
                    .unwrap()
                    .highest_confirmed_root(),
            );
            Self::handle_new_root(
                new_root,
                bank_forks,
                progress,
                accounts_background_request_sender,
                highest_confirmed_root,
                heaviest_subtree_fork_choice,
                duplicate_slots_tracker,
                gossip_duplicate_confirmed_slots,
                unfrozen_gossip_verified_vote_hashes,
                has_new_vote_been_rooted,
                vote_signatures,
            );
            rpc_subscriptions.notify_roots(rooted_slots);
            if let Some(sender) = bank_notification_sender {
                sender
                    .send(BankNotification::Root(root_bank))
                    .unwrap_or_else(|err| warn!("bank_notification_sender failed: {:?}", err));
            }
            latest_root_senders.iter().for_each(|s| {
                if let Err(e) = s.send(new_root) {
                    trace!("latest root send failed: {:?}", e);
                }
            });
            info!("new root {}", new_root);
        }

        let mut update_commitment_cache_time = Measure::start("update_commitment_cache");
        Self::update_commitment_cache(
            bank.clone(),
            bank_forks.read().unwrap().root(),
            progress.get_fork_stats(bank.slot()).unwrap().total_stake,
            lockouts_sender,
        );
        update_commitment_cache_time.stop();
        replay_timing.update_commitment_cache_us += update_commitment_cache_time.as_us();

        Self::push_vote(
            bank,
            vote_account_pubkey,
            identity_keypair,
            authorized_voter_keypairs,
            tower,
            saved_tower,
            switch_fork_decision,
            vote_signatures,
            *has_new_vote_been_rooted,
            replay_timing,
            voting_sender,
        );
    }

    fn generate_vote_tx(
        node_keypair: &Keypair,
        bank: &Bank,
        vote_account_pubkey: &Pubkey,
        authorized_voter_keypairs: &[Arc<Keypair>],
        vote: Vote,
        switch_fork_decision: &SwitchForkDecision,
        vote_signatures: &mut Vec<Signature>,
        has_new_vote_been_rooted: bool,
    ) -> Option<Transaction> {
        if authorized_voter_keypairs.is_empty() {
            return None;
        }
        let vote_account = match bank.get_vote_account(vote_account_pubkey) {
            None => {
                warn!(
                    "Vote account {} does not exist.  Unable to vote",
                    vote_account_pubkey,
                );
                return None;
            }
            Some((_stake, vote_account)) => vote_account,
        };
        let vote_state = vote_account.vote_state();
        let vote_state = match vote_state.as_ref() {
            Err(_) => {
                warn!(
                    "Vote account {} is unreadable.  Unable to vote",
                    vote_account_pubkey,
                );
                return None;
            }
            Ok(vote_state) => vote_state,
        };

        if vote_state.node_pubkey != node_keypair.pubkey() {
            info!(
                "Vote account node_pubkey mismatch: {} (expected: {}).  Unable to vote",
                vote_state.node_pubkey,
                node_keypair.pubkey()
            );
            return None;
        }

        let authorized_voter_pubkey =
            if let Some(authorized_voter_pubkey) = vote_state.get_authorized_voter(bank.epoch()) {
                authorized_voter_pubkey
            } else {
                warn!(
                    "Vote account {} has no authorized voter for epoch {}.  Unable to vote",
                    vote_account_pubkey,
                    bank.epoch()
                );
                return None;
            };

        let authorized_voter_keypair = match authorized_voter_keypairs
            .iter()
            .find(|keypair| keypair.pubkey() == authorized_voter_pubkey)
        {
            None => {
                warn!("The authorized keypair {} for vote account {} is not available.  Unable to vote",
                      authorized_voter_pubkey, vote_account_pubkey);
                return None;
            }
            Some(authorized_voter_keypair) => authorized_voter_keypair,
        };

        // Send our last few votes along with the new one
        let vote_ix = switch_fork_decision
            .to_vote_instruction(
                vote,
                vote_account_pubkey,
                &authorized_voter_keypair.pubkey(),
            )
            .expect("Switch threshold failure should not lead to voting");

        let mut vote_tx = Transaction::new_with_payer(&[vote_ix], Some(&node_keypair.pubkey()));

        let blockhash = bank.last_blockhash();
        vote_tx.partial_sign(&[node_keypair], blockhash);
        vote_tx.partial_sign(&[authorized_voter_keypair.as_ref()], blockhash);

        if !has_new_vote_been_rooted {
            vote_signatures.push(vote_tx.signatures[0]);
            if vote_signatures.len() > MAX_VOTE_SIGNATURES {
                vote_signatures.remove(0);
            }
        } else {
            vote_signatures.clear();
        }

        Some(vote_tx)
    }

    #[allow(clippy::too_many_arguments)]
    fn refresh_last_vote(
        tower: &mut Tower,
        heaviest_bank_on_same_fork: &Bank,
        my_latest_landed_vote: Slot,
        vote_account_pubkey: &Pubkey,
        identity_keypair: &Keypair,
        authorized_voter_keypairs: &[Arc<Keypair>],
        vote_signatures: &mut Vec<Signature>,
        has_new_vote_been_rooted: bool,
        last_vote_refresh_time: &mut LastVoteRefreshTime,
        voting_sender: &Sender<VoteOp>,
    ) {
        let last_voted_slot = tower.last_voted_slot();
        if last_voted_slot.is_none() {
            return;
        }

        // Refresh the vote if our latest vote hasn't landed, and the recent blockhash of the
        // last attempt at a vote transaction has expired
        let last_voted_slot = last_voted_slot.unwrap();
        if my_latest_landed_vote > last_voted_slot
            && last_vote_refresh_time.last_print_time.elapsed().as_secs() >= 1
        {
            last_vote_refresh_time.last_print_time = Instant::now();
            info!(
                "Last landed vote for slot {} in bank {} is greater than the current last vote for slot: {} tracked by Tower",
                my_latest_landed_vote,
                heaviest_bank_on_same_fork.slot(),
                last_voted_slot
            );
        }
        if my_latest_landed_vote >= last_voted_slot
            || heaviest_bank_on_same_fork
                .check_hash_age(&tower.last_vote_tx_blockhash(), MAX_PROCESSING_AGE)
                .unwrap_or(false)
            // In order to avoid voting on multiple forks all past MAX_PROCESSING_AGE that don't
            // include the last voted blockhash
            || last_vote_refresh_time.last_refresh_time.elapsed().as_millis() < MAX_VOTE_REFRESH_INTERVAL_MILLIS as u128
        {
            return;
        }

        // TODO: check the timestamp in this vote is correct, i.e. it shouldn't
        // have changed from the original timestamp of the vote.
        let vote_tx = Self::generate_vote_tx(
            identity_keypair,
            heaviest_bank_on_same_fork,
            vote_account_pubkey,
            authorized_voter_keypairs,
            tower.last_vote(),
            &SwitchForkDecision::SameFork,
            vote_signatures,
            has_new_vote_been_rooted,
        );

        if let Some(vote_tx) = vote_tx {
            let recent_blockhash = vote_tx.message.recent_blockhash;
            tower.refresh_last_vote_tx_blockhash(recent_blockhash);

            // Send the votes to the TPU and gossip for network propagation
            let hash_string = format!("{}", recent_blockhash);
            datapoint_info!(
                "refresh_vote",
                ("last_voted_slot", last_voted_slot, i64),
                ("target_bank_slot", heaviest_bank_on_same_fork.slot(), i64),
                ("target_bank_hash", hash_string, String),
            );
            voting_sender
                .send(VoteOp::RefreshVote {
                    tx: vote_tx,
                    last_voted_slot,
                })
                .unwrap_or_else(|err| warn!("Error: {:?}", err));
            last_vote_refresh_time.last_refresh_time = Instant::now();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push_vote(
        bank: &Bank,
        vote_account_pubkey: &Pubkey,
        identity_keypair: &Keypair,
        authorized_voter_keypairs: &[Arc<Keypair>],
        tower: &mut Tower,
        saved_tower: SavedTower,
        switch_fork_decision: &SwitchForkDecision,
        vote_signatures: &mut Vec<Signature>,
        has_new_vote_been_rooted: bool,
        replay_timing: &mut ReplayTiming,
        voting_sender: &Sender<VoteOp>,
    ) {
        let mut generate_time = Measure::start("generate_vote");
        let vote_tx = Self::generate_vote_tx(
            identity_keypair,
            bank,
            vote_account_pubkey,
            authorized_voter_keypairs,
            tower.last_vote(),
            switch_fork_decision,
            vote_signatures,
            has_new_vote_been_rooted,
        );
        generate_time.stop();
        replay_timing.generate_vote_us += generate_time.as_us();
        if let Some(vote_tx) = vote_tx {
            tower.refresh_last_vote_tx_blockhash(vote_tx.message.recent_blockhash);

            let tower_slots = tower.tower_slots();
            voting_sender
                .send(VoteOp::PushVote {
                    tx: vote_tx,
                    tower_slots,
                    saved_tower,
                })
                .unwrap_or_else(|err| warn!("Error: {:?}", err));
        }
    }

    fn update_commitment_cache(
        bank: Arc<Bank>,
        root: Slot,
        total_stake: Stake,
        lockouts_sender: &Sender<CommitmentAggregationData>,
    ) {
        if let Err(e) =
            lockouts_sender.send(CommitmentAggregationData::new(bank, root, total_stake))
        {
            trace!("lockouts_sender failed: {:?}", e);
        }
    }

    fn reset_poh_recorder(
        my_pubkey: &Pubkey,
        blockstore: &Blockstore,
        bank: &Arc<Bank>,
        poh_recorder: &Mutex<PohRecorder>,
        leader_schedule_cache: &LeaderScheduleCache,
    ) {
        let next_leader_slot = leader_schedule_cache.next_leader_slot(
            my_pubkey,
            bank.slot(),
            bank,
            Some(blockstore),
            GRACE_TICKS_FACTOR * MAX_GRACE_SLOTS,
        );
        poh_recorder
            .lock()
            .unwrap()
            .reset(bank.last_blockhash(), bank.slot(), next_leader_slot);

        let next_leader_msg = if let Some(next_leader_slot) = next_leader_slot {
            format!("My next leader slot is {}", next_leader_slot.0)
        } else {
            "I am not in the leader schedule yet".to_owned()
        };

        info!(
            "{} reset PoH to tick {} (within slot {}). {}",
            my_pubkey,
            bank.tick_height(),
            bank.slot(),
            next_leader_msg,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_active_banks(
        blockstore: &Blockstore,
        bank_forks: &RwLock<BankForks>,
        my_pubkey: &Pubkey,
        vote_account: &Pubkey,
        progress: &mut ProgressMap,
        transaction_status_sender: Option<&TransactionStatusSender>,
        cache_block_meta_sender: Option<&CacheBlockMetaSender>,
        verify_recyclers: &VerifyRecyclers,
        heaviest_subtree_fork_choice: &mut HeaviestSubtreeForkChoice,
        replay_vote_sender: &ReplayVoteSender,
        bank_notification_sender: &Option<BankNotificationSender>,
        rewards_recorder_sender: &Option<RewardsRecorderSender>,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        gossip_duplicate_confirmed_slots: &GossipDuplicateConfirmedSlots,
        unfrozen_gossip_verified_vote_hashes: &mut UnfrozenGossipVerifiedVoteHashes,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
        cluster_slots_update_sender: &ClusterSlotsUpdateSender,
        cost_update_sender: &Sender<ExecuteTimings>,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
    ) -> bool {
        let mut did_complete_bank = false;
        let mut tx_count = 0;
        let mut execute_timings = ExecuteTimings::default();
        let active_banks = bank_forks.read().unwrap().active_banks();
        trace!("active banks {:?}", active_banks);

        for bank_slot in &active_banks {
            // If the fork was marked as dead, don't replay it
            if progress.get(bank_slot).map(|p| p.is_dead).unwrap_or(false) {
                debug!("bank_slot {:?} is marked dead", *bank_slot);
                continue;
            }

            let bank = bank_forks.read().unwrap().get(*bank_slot).unwrap().clone();
            let parent_slot = bank.parent_slot();
            let prev_leader_slot = progress.get_bank_prev_leader_slot(&bank);
            let (num_blocks_on_fork, num_dropped_blocks_on_fork) = {
                let stats = progress
                    .get(&parent_slot)
                    .expect("parent of active bank must exist in progress map");
                let num_blocks_on_fork = stats.num_blocks_on_fork + 1;
                let new_dropped_blocks = bank.slot() - parent_slot - 1;
                let num_dropped_blocks_on_fork =
                    stats.num_dropped_blocks_on_fork + new_dropped_blocks;
                (num_blocks_on_fork, num_dropped_blocks_on_fork)
            };

            // Insert a progress entry even for slots this node is the leader for, so that
            // 1) confirm_forks can report confirmation, 2) we can cache computations about
            // this bank in `select_forks()`
            let bank_progress = &mut progress.entry(bank.slot()).or_insert_with(|| {
                ForkProgress::new_from_bank(
                    &bank,
                    my_pubkey,
                    vote_account,
                    prev_leader_slot,
                    num_blocks_on_fork,
                    num_dropped_blocks_on_fork,
                )
            });
            if bank.collector_id() != my_pubkey {
                let root_slot = bank_forks.read().unwrap().root();
                let replay_result = Self::replay_blockstore_into_bank(
                    &bank,
                    blockstore,
                    bank_progress,
                    transaction_status_sender,
                    replay_vote_sender,
                    verify_recyclers,
                );
                execute_timings.accumulate(&bank_progress.replay_stats.execute_timings);
                match replay_result {
                    Ok(replay_tx_count) => tx_count += replay_tx_count,
                    Err(err) => {
                        // Error means the slot needs to be marked as dead
                        Self::mark_dead_slot(
                            blockstore,
                            &bank,
                            root_slot,
                            &err,
                            rpc_subscriptions,
                            duplicate_slots_tracker,
                            gossip_duplicate_confirmed_slots,
                            progress,
                            heaviest_subtree_fork_choice,
                            duplicate_slots_to_repair,
                        );
                        // If the bank was corrupted, don't try to run the below logic to check if the
                        // bank is completed
                        continue;
                    }
                }
            }
            assert_eq!(*bank_slot, bank.slot());
            if bank.is_complete() {
                bank_progress.replay_stats.report_stats(
                    bank.slot(),
                    bank_progress.replay_progress.num_entries,
                    bank_progress.replay_progress.num_shreds,
                );
                did_complete_bank = true;
                info!("bank frozen: {}", bank.slot());
                let _ = cluster_slots_update_sender.send(vec![*bank_slot]);
                if let Some(transaction_status_sender) = transaction_status_sender {
                    transaction_status_sender.send_transaction_status_freeze_message(&bank);
                }
                bank.freeze();
                let bank_hash = bank.hash();
                assert_ne!(bank_hash, Hash::default());
                // Needs to be updated before `check_slot_agrees_with_cluster()` so that
                // any updates in `check_slot_agrees_with_cluster()` on fork choice take
                // effect
                heaviest_subtree_fork_choice.add_new_leaf_slot(
                    (bank.slot(), bank.hash()),
                    Some((bank.parent_slot(), bank.parent_hash())),
                );
                progress
                    .get_fork_stats_mut(bank.slot())
                    .expect("All frozen banks must exist in the Progress map")
                    .bank_hash = Some(bank.hash());
                let bank_frozen_state = BankFrozenState::new_from_state(
                    bank.slot(),
                    bank.hash(),
                    duplicate_slots_tracker,
                    gossip_duplicate_confirmed_slots,
                    heaviest_subtree_fork_choice,
                );
                check_slot_agrees_with_cluster(
                    bank.slot(),
                    bank_forks.read().unwrap().root(),
                    blockstore,
                    duplicate_slots_tracker,
                    heaviest_subtree_fork_choice,
                    duplicate_slots_to_repair,
                    SlotStateUpdate::BankFrozen(bank_frozen_state),
                );
                if let Some(sender) = bank_notification_sender {
                    sender
                        .send(BankNotification::Frozen(bank.clone()))
                        .unwrap_or_else(|err| warn!("bank_notification_sender failed: {:?}", err));
                }
                blockstore_processor::cache_block_meta(&bank, cache_block_meta_sender);

                let bank_hash = bank.hash();
                if let Some(new_frozen_voters) =
                    unfrozen_gossip_verified_vote_hashes.remove_slot_hash(bank.slot(), &bank_hash)
                {
                    for pubkey in new_frozen_voters {
                        latest_validator_votes_for_frozen_banks.check_add_vote(
                            pubkey,
                            bank.slot(),
                            Some(bank_hash),
                            false,
                        );
                    }
                }
                Self::record_rewards(&bank, rewards_recorder_sender);
            } else {
                trace!(
                    "bank {} not completed tick_height: {}, max_tick_height: {}",
                    bank.slot(),
                    bank.tick_height(),
                    bank.max_tick_height()
                );
            }
        }

        // send accumulated excute-timings to cost_update_service
        cost_update_sender
            .send(execute_timings)
            .unwrap_or_else(|err| warn!("cost_update_sender failed: {:?}", err));

        inc_new_counter_info!("replay_stage-replay_transactions", tx_count);
        did_complete_bank
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compute_bank_stats(
        my_vote_pubkey: &Pubkey,
        ancestors: &HashMap<u64, HashSet<u64>>,
        frozen_banks: &mut Vec<Arc<Bank>>,
        tower: &Tower,
        progress: &mut ProgressMap,
        vote_tracker: &VoteTracker,
        cluster_slots: &ClusterSlots,
        bank_forks: &RwLock<BankForks>,
        heaviest_subtree_fork_choice: &mut HeaviestSubtreeForkChoice,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
    ) -> Vec<Slot> {
        frozen_banks.sort_by_key(|bank| bank.slot());
        let mut new_stats = vec![];
        for bank in frozen_banks {
            let bank_slot = bank.slot();
            // Only time progress map should be missing a bank slot
            // is if this node was the leader for this slot as those banks
            // are not replayed in replay_active_banks()
            {
                let is_computed = progress
                    .get_fork_stats_mut(bank_slot)
                    .expect("All frozen banks must exist in the Progress map")
                    .computed;
                if !is_computed {
                    let computed_bank_state = Tower::collect_vote_lockouts(
                        my_vote_pubkey,
                        bank_slot,
                        bank.vote_accounts().into_iter(),
                        ancestors,
                        |slot| progress.get_hash(slot),
                        latest_validator_votes_for_frozen_banks,
                    );
                    // Notify any listeners of the votes found in this newly computed
                    // bank
                    heaviest_subtree_fork_choice.compute_bank_stats(
                        bank,
                        tower,
                        latest_validator_votes_for_frozen_banks,
                    );
                    let ComputedBankState {
                        voted_stakes,
                        total_stake,
                        lockout_intervals,
                        my_latest_landed_vote,
                        ..
                    } = computed_bank_state;
                    let stats = progress
                        .get_fork_stats_mut(bank_slot)
                        .expect("All frozen banks must exist in the Progress map");
                    stats.total_stake = total_stake;
                    stats.voted_stakes = voted_stakes;
                    stats.lockout_intervals = lockout_intervals;
                    stats.block_height = bank.block_height();
                    stats.my_latest_landed_vote = my_latest_landed_vote;
                    stats.computed = true;
                    new_stats.push(bank_slot);
                    datapoint_info!(
                        "bank_weight",
                        ("slot", bank_slot, i64),
                        // u128 too large for influx, convert to hex
                        ("weight", format!("{:X}", stats.weight), String),
                    );
                    info!(
                        "{} slot_weight: {} {} {} {}",
                        my_vote_pubkey,
                        bank_slot,
                        stats.weight,
                        stats.fork_weight,
                        bank.parent().map(|b| b.slot()).unwrap_or(0)
                    );
                }
            }

            Self::update_propagation_status(
                progress,
                bank_slot,
                bank_forks,
                vote_tracker,
                cluster_slots,
            );

            let stats = progress
                .get_fork_stats_mut(bank_slot)
                .expect("All frozen banks must exist in the Progress map");

            stats.vote_threshold =
                tower.check_vote_stake_threshold(bank_slot, &stats.voted_stakes, stats.total_stake);
            stats.is_locked_out = tower.is_locked_out(
                bank_slot,
                ancestors
                    .get(&bank_slot)
                    .expect("Ancestors map should contain slot for is_locked_out() check"),
            );
            stats.has_voted = tower.has_voted(bank_slot);
            stats.is_recent = tower.is_recent(bank_slot);
        }
        new_stats
    }

    fn update_propagation_status(
        progress: &mut ProgressMap,
        slot: Slot,
        bank_forks: &RwLock<BankForks>,
        vote_tracker: &VoteTracker,
        cluster_slots: &ClusterSlots,
    ) {
        // If propagation has already been confirmed, return
        if progress.is_propagated(slot) {
            return;
        }

        // Otherwise we have to check the votes for confirmation
        let mut slot_vote_tracker = progress
            .get_propagated_stats(slot)
            .expect("All frozen banks must exist in the Progress map")
            .slot_vote_tracker
            .clone();

        if slot_vote_tracker.is_none() {
            slot_vote_tracker = vote_tracker.get_slot_vote_tracker(slot);
            progress
                .get_propagated_stats_mut(slot)
                .expect("All frozen banks must exist in the Progress map")
                .slot_vote_tracker = slot_vote_tracker.clone();
        }

        let mut cluster_slot_pubkeys = progress
            .get_propagated_stats(slot)
            .expect("All frozen banks must exist in the Progress map")
            .cluster_slot_pubkeys
            .clone();

        if cluster_slot_pubkeys.is_none() {
            cluster_slot_pubkeys = cluster_slots.lookup(slot);
            progress
                .get_propagated_stats_mut(slot)
                .expect("All frozen banks must exist in the Progress map")
                .cluster_slot_pubkeys = cluster_slot_pubkeys.clone();
        }

        let newly_voted_pubkeys = slot_vote_tracker
            .as_ref()
            .and_then(|slot_vote_tracker| {
                slot_vote_tracker.write().unwrap().get_voted_slot_updates()
            })
            .unwrap_or_default();

        let cluster_slot_pubkeys = cluster_slot_pubkeys
            .map(|v| v.read().unwrap().keys().cloned().collect())
            .unwrap_or_default();

        Self::update_fork_propagated_threshold_from_votes(
            progress,
            newly_voted_pubkeys,
            cluster_slot_pubkeys,
            slot,
            bank_forks,
        );
    }

    // Given a heaviest bank, `heaviest_bank` and the next votable bank
    // `heaviest_bank_on_same_voted_fork` as the validator's last vote, return
    // a bank to vote on, a bank to reset to,
    pub fn select_vote_and_reset_forks(
        heaviest_bank: &Arc<Bank>,
        // Should only be None if there was no previous vote
        heaviest_bank_on_same_voted_fork: Option<&Arc<Bank>>,
        ancestors: &HashMap<u64, HashSet<u64>>,
        descendants: &HashMap<u64, HashSet<u64>>,
        progress: &ProgressMap,
        tower: &mut Tower,
        latest_validator_votes_for_frozen_banks: &LatestValidatorVotesForFrozenBanks,
        fork_choice: &HeaviestSubtreeForkChoice,
    ) -> SelectVoteAndResetForkResult {
        // Try to vote on the actual heaviest fork. If the heaviest bank is
        // locked out or fails the threshold check, the validator will:
        // 1) Not continue to vote on current fork, waiting for lockouts to expire/
        //    threshold check to pass
        // 2) Will reset PoH to heaviest fork in order to make sure the heaviest
        //    fork is propagated
        // This above behavior should ensure correct voting and resetting PoH
        // behavior under all cases:
        // 1) The best "selected" bank is on same fork
        // 2) The best "selected" bank is on a different fork,
        //    switch_threshold fails
        // 3) The best "selected" bank is on a different fork,
        //    switch_threshold succeeds
        let mut failure_reasons = vec![];
        let selected_fork = {
            let switch_fork_decision = tower.check_switch_threshold(
                heaviest_bank.slot(),
                ancestors,
                descendants,
                progress,
                heaviest_bank.total_epoch_stake(),
                heaviest_bank
                    .epoch_vote_accounts(heaviest_bank.epoch())
                    .expect("Bank epoch vote accounts must contain entry for the bank's own epoch"),
                latest_validator_votes_for_frozen_banks,
                fork_choice,
            );

            match switch_fork_decision {
                SwitchForkDecision::FailedSwitchThreshold(_, _) => {
                    let reset_bank = heaviest_bank_on_same_voted_fork;
                    // If we can't switch and our last vote was on a non-duplicate/confirmed slot, then
                    // reset to the the next votable bank on the same fork as our last vote,
                    // but don't vote.

                    // We don't just reset to the heaviest fork when switch threshold fails because
                    // a situation like this can occur:

                    /* Figure 1:
                                  slot 0
                                    |
                                  slot 1
                                /        \
                    slot 2 (last vote)     |
                                |      slot 8 (10%)
                        slot 4 (9%)
                    */

                    // Imagine 90% of validators voted on slot 4, but only 9% landed. If everybody that fails
                    // the switch theshold abandons slot 4 to build on slot 8 (because it's *currently* heavier),
                    // then there will be no blocks to include the votes for slot 4, and the network halts
                    // because 90% of validators can't vote
                    info!(
                        "Waiting to switch vote to {}, resetting to slot {:?} for now",
                        heaviest_bank.slot(),
                        reset_bank.as_ref().map(|b| b.slot()),
                    );
                    failure_reasons.push(HeaviestForkFailures::FailedSwitchThreshold(
                        heaviest_bank.slot(),
                    ));
                    reset_bank.map(|b| (b, switch_fork_decision))
                }
                SwitchForkDecision::FailedSwitchDuplicateRollback(latest_duplicate_ancestor) => {
                    // If we can't switch and our last vote was on an unconfirmed, duplicate slot,
                    // then we need to reset to the heaviest bank, even if the heaviest bank is not
                    // a descendant of the last vote (usually for switch threshold failures we reset
                    // to the heaviest descendant of the last vote, but in this case, the last vote
                    // was on a duplicate branch). This is because in the case of *unconfirmed* duplicate
                    // slots, somebody needs to generate an alternative branch to escape a situation
                    // like a 50-50 split  where both partitions have voted on different versions of the
                    // same duplicate slot.

                    // Unlike the situation described in `Figure 1` above, this is safe. To see why,
                    // imagine the same situation described in Figure 1 above occurs, but slot 2 is
                    // a duplicate block. There are now a few cases:
                    //
                    // Note first that DUPLICATE_THRESHOLD + SWITCH_FORK_THRESHOLD + DUPLICATE_LIVENESS_THRESHOLD = 1;
                    //
                    // 1) > DUPLICATE_THRESHOLD of the network voted on some version of slot 2. Because duplicate slots can be confirmed
                    // by gossip, unlike the situation described in `Figure 1`, we don't need those
                    // votes to land in a descendant to confirm slot 2. Once slot 2 is confirmed by
                    // gossip votes, that fork is added back to the fork choice set and falls back into
                    // normal fork choice, which is covered by the `FailedSwitchThreshold` case above
                    // (everyone will resume building on their last voted fork, slot 4, since slot 8
                    // doesn't have for switch threshold)
                    //
                    // 2) <= DUPLICATE_THRESHOLD of the network voted on some version of slot 2, > SWITCH_FORK_THRESHOLD of the network voted
                    // on slot 8. Then everybody abandons the duplicate fork from fork choice and both builds
                    // on slot 8's fork. They can also vote on slot 8's fork because it has sufficient weight
                    // to pass the switching threshold
                    //
                    // 3) <= DUPLICATE_THRESHOLD of the network voted on some version of slot 2, <= SWITCH_FORK_THRESHOLD of the network voted
                    // on slot 8. This means more than DUPLICATE_LIVENESS_THRESHOLD of the network is gone, so we cannot
                    // guarantee progress anyways

                    // Note the heaviest fork is never descended from a known unconfirmed duplicate slot
                    // because the fork choice rule ensures that (marks it as an invalid candidate),
                    // thus it's safe to use as the reset bank.
                    let reset_bank = Some(heaviest_bank);
                    info!(
                        "Waiting to switch vote to {}, resetting to slot {:?} for now, latest duplicate ancestor: {:?}",
                        heaviest_bank.slot(),
                        reset_bank.as_ref().map(|b| b.slot()),
                        latest_duplicate_ancestor,
                    );
                    failure_reasons.push(HeaviestForkFailures::FailedSwitchThreshold(
                        heaviest_bank.slot(),
                    ));
                    reset_bank.map(|b| (b, switch_fork_decision))
                }
                _ => Some((heaviest_bank, switch_fork_decision)),
            }
        };

        if let Some((bank, switch_fork_decision)) = selected_fork {
            let (is_locked_out, vote_threshold, is_leader_slot, fork_weight) = {
                let fork_stats = progress.get_fork_stats(bank.slot()).unwrap();
                let propagated_stats = &progress.get_propagated_stats(bank.slot()).unwrap();
                (
                    fork_stats.is_locked_out,
                    fork_stats.vote_threshold,
                    propagated_stats.is_leader_slot,
                    fork_stats.weight,
                )
            };

            let propagation_confirmed = is_leader_slot || progress.is_propagated(bank.slot());

            if is_locked_out {
                failure_reasons.push(HeaviestForkFailures::LockedOut(bank.slot()));
            }
            if !vote_threshold {
                failure_reasons.push(HeaviestForkFailures::FailedThreshold(bank.slot()));
            }
            if !propagation_confirmed {
                failure_reasons.push(HeaviestForkFailures::NoPropagatedConfirmation(bank.slot()));
            }

            if !is_locked_out
                && vote_threshold
                && propagation_confirmed
                && switch_fork_decision.can_vote()
            {
                info!("voting: {} {}", bank.slot(), fork_weight);
                SelectVoteAndResetForkResult {
                    vote_bank: Some((bank.clone(), switch_fork_decision)),
                    reset_bank: Some(bank.clone()),
                    heaviest_fork_failures: failure_reasons,
                }
            } else {
                SelectVoteAndResetForkResult {
                    vote_bank: None,
                    reset_bank: Some(bank.clone()),
                    heaviest_fork_failures: failure_reasons,
                }
            }
        } else {
            SelectVoteAndResetForkResult {
                vote_bank: None,
                reset_bank: None,
                heaviest_fork_failures: failure_reasons,
            }
        }
    }

    fn update_fork_propagated_threshold_from_votes(
        progress: &mut ProgressMap,
        mut newly_voted_pubkeys: Vec<Pubkey>,
        mut cluster_slot_pubkeys: Vec<Pubkey>,
        fork_tip: Slot,
        bank_forks: &RwLock<BankForks>,
    ) {
        let mut current_leader_slot = progress.get_latest_leader_slot(fork_tip);
        let mut did_newly_reach_threshold = false;
        let root = bank_forks.read().unwrap().root();
        loop {
            // These cases mean confirmation of propagation on any earlier
            // leader blocks must have been reached
            if current_leader_slot == None || current_leader_slot.unwrap() < root {
                break;
            }

            let leader_propagated_stats = progress
                .get_propagated_stats_mut(current_leader_slot.unwrap())
                .expect("current_leader_slot >= root, so must exist in the progress map");

            // If a descendant has reached propagation threshold, then
            // all its ancestor banks have also reached propagation
            // threshold as well (Validators can't have voted for a
            // descendant without also getting the ancestor block)
            if leader_propagated_stats.is_propagated ||
                // If there's no new validators to record, and there's no
                // newly achieved threshold, then there's no further
                // information to propagate backwards to past leader blocks
                (newly_voted_pubkeys.is_empty() && cluster_slot_pubkeys.is_empty() &&
                !did_newly_reach_threshold)
            {
                break;
            }

            // We only iterate through the list of leader slots by traversing
            // the linked list of 'prev_leader_slot`'s outlined in the
            // `progress` map
            assert!(leader_propagated_stats.is_leader_slot);
            let leader_bank = bank_forks
                .read()
                .unwrap()
                .get(current_leader_slot.unwrap())
                .expect("Entry in progress map must exist in BankForks")
                .clone();

            did_newly_reach_threshold = Self::update_slot_propagated_threshold_from_votes(
                &mut newly_voted_pubkeys,
                &mut cluster_slot_pubkeys,
                &leader_bank,
                leader_propagated_stats,
                did_newly_reach_threshold,
            ) || did_newly_reach_threshold;

            // Now jump to process the previous leader slot
            current_leader_slot = leader_propagated_stats.prev_leader_slot;
        }
    }

    fn update_slot_propagated_threshold_from_votes(
        newly_voted_pubkeys: &mut Vec<Pubkey>,
        cluster_slot_pubkeys: &mut Vec<Pubkey>,
        leader_bank: &Bank,
        leader_propagated_stats: &mut PropagatedStats,
        did_child_reach_threshold: bool,
    ) -> bool {
        // Track whether this slot newly confirm propagation
        // throughout the network (switched from is_propagated == false
        // to is_propagated == true)
        let mut did_newly_reach_threshold = false;

        // If a child of this slot confirmed propagation, then
        // we can return early as this implies this slot must also
        // be propagated
        if did_child_reach_threshold {
            if !leader_propagated_stats.is_propagated {
                leader_propagated_stats.is_propagated = true;
                return true;
            } else {
                return false;
            }
        }

        if leader_propagated_stats.is_propagated {
            return false;
        }

        // Remove the vote/node pubkeys that we already know voted for this
        // slot. These vote accounts/validator identities are safe to drop
        // because they don't to be ported back any further because earlier
        // parents must have:
        // 1) Also recorded these pubkeys already, or
        // 2) Already reached the propagation threshold, in which case
        //    they no longer need to track the set of propagated validators
        newly_voted_pubkeys.retain(|vote_pubkey| {
            let exists = leader_propagated_stats
                .propagated_validators
                .contains(vote_pubkey);
            leader_propagated_stats.add_vote_pubkey(
                *vote_pubkey,
                leader_bank.epoch_vote_account_stake(vote_pubkey),
            );
            !exists
        });

        cluster_slot_pubkeys.retain(|node_pubkey| {
            let exists = leader_propagated_stats
                .propagated_node_ids
                .contains(node_pubkey);
            leader_propagated_stats.add_node_pubkey(&*node_pubkey, leader_bank);
            !exists
        });

        if leader_propagated_stats.total_epoch_stake == 0
            || leader_propagated_stats.propagated_validators_stake as f64
                / leader_propagated_stats.total_epoch_stake as f64
                > SUPERMINORITY_THRESHOLD
        {
            leader_propagated_stats.is_propagated = true;
            did_newly_reach_threshold = true
        }

        did_newly_reach_threshold
    }

    fn mark_slots_confirmed(
        confirmed_forks: &[(Slot, Hash)],
        blockstore: &Blockstore,
        bank_forks: &RwLock<BankForks>,
        progress: &mut ProgressMap,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
    ) {
        let root_slot = bank_forks.read().unwrap().root();
        for (slot, frozen_hash) in confirmed_forks.iter() {
            // This case should be guaranteed as false by confirm_forks()
            if let Some(false) = progress.is_supermajority_confirmed(*slot) {
                // Because supermajority confirmation will iterate through and update the
                // subtree in fork choice, only incur this cost if the slot wasn't already
                // confirmed
                progress.set_supermajority_confirmed_slot(*slot);
                // If the slot was confirmed, then it must be frozen. Otherwise, we couldn't
                // have replayed any of its descendants and figured out it was confirmed.
                assert!(*frozen_hash != Hash::default());

                let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
                    *frozen_hash,
                    || false,
                    || Some(*frozen_hash),
                );
                check_slot_agrees_with_cluster(
                    *slot,
                    root_slot,
                    blockstore,
                    duplicate_slots_tracker,
                    fork_choice,
                    duplicate_slots_to_repair,
                    SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
                );
            }
        }
    }

    fn confirm_forks(
        tower: &Tower,
        voted_stakes: &VotedStakes,
        total_stake: Stake,
        progress: &ProgressMap,
        bank_forks: &RwLock<BankForks>,
    ) -> Vec<(Slot, Hash)> {
        let mut confirmed_forks = vec![];
        for (slot, prog) in progress.iter() {
            if !prog.fork_stats.is_supermajority_confirmed {
                let bank = bank_forks
                    .read()
                    .unwrap()
                    .get(*slot)
                    .expect("bank in progress must exist in BankForks")
                    .clone();
                let duration = prog.replay_stats.started.elapsed().as_millis();
                if bank.is_frozen() && tower.is_slot_confirmed(*slot, voted_stakes, total_stake) {
                    info!("validator fork confirmed {} {}ms", *slot, duration);
                    datapoint_info!("validator-confirmation", ("duration_ms", duration, i64));
                    confirmed_forks.push((*slot, bank.hash()));
                } else {
                    debug!(
                        "validator fork not confirmed {} {}ms {:?}",
                        *slot,
                        duration,
                        voted_stakes.get(slot)
                    );
                }
            }
        }
        confirmed_forks
    }

    #[allow(clippy::too_many_arguments)]
    pub fn handle_new_root(
        new_root: Slot,
        bank_forks: &RwLock<BankForks>,
        progress: &mut ProgressMap,
        accounts_background_request_sender: &AbsRequestSender,
        highest_confirmed_root: Option<Slot>,
        heaviest_subtree_fork_choice: &mut HeaviestSubtreeForkChoice,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        gossip_duplicate_confirmed_slots: &mut GossipDuplicateConfirmedSlots,
        unfrozen_gossip_verified_vote_hashes: &mut UnfrozenGossipVerifiedVoteHashes,
        has_new_vote_been_rooted: &mut bool,
        voted_signatures: &mut Vec<Signature>,
    ) {
        bank_forks.write().unwrap().set_root(
            new_root,
            accounts_background_request_sender,
            highest_confirmed_root,
        );
        let r_bank_forks = bank_forks.read().unwrap();
        let new_root_bank = &r_bank_forks[new_root];
        if !*has_new_vote_been_rooted {
            for signature in voted_signatures.iter() {
                if new_root_bank.get_signature_status(signature).is_some() {
                    *has_new_vote_been_rooted = true;
                    break;
                }
            }
            if *has_new_vote_been_rooted {
                std::mem::take(voted_signatures);
            }
        }
        progress.handle_new_root(&r_bank_forks);
        heaviest_subtree_fork_choice.set_root((new_root, r_bank_forks.root_bank().hash()));
        let mut slots_ge_root = duplicate_slots_tracker.split_off(&new_root);
        // duplicate_slots_tracker now only contains entries >= `new_root`
        std::mem::swap(duplicate_slots_tracker, &mut slots_ge_root);

        let mut slots_ge_root = gossip_duplicate_confirmed_slots.split_off(&new_root);
        // gossip_confirmed_slots now only contains entries >= `new_root`
        std::mem::swap(gossip_duplicate_confirmed_slots, &mut slots_ge_root);

        unfrozen_gossip_verified_vote_hashes.set_root(new_root);
    }

    fn generate_new_bank_forks(
        blockstore: &Blockstore,
        bank_forks: &RwLock<BankForks>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
        progress: &mut ProgressMap,
    ) {
        // Find the next slot that chains to the old slot
        let forks = bank_forks.read().unwrap();
        let frozen_banks = forks.frozen_banks();
        let frozen_bank_slots: Vec<u64> = frozen_banks
            .keys()
            .cloned()
            .filter(|s| *s >= forks.root())
            .collect();
        let next_slots = blockstore
            .get_slots_since(&frozen_bank_slots)
            .expect("Db error");
        // Filter out what we've already seen
        trace!("generate new forks {:?}", {
            let mut next_slots = next_slots.iter().collect::<Vec<_>>();
            next_slots.sort();
            next_slots
        });
        let mut new_banks = HashMap::new();
        for (parent_slot, children) in next_slots {
            let parent_bank = frozen_banks
                .get(&parent_slot)
                .expect("missing parent in bank forks")
                .clone();
            for child_slot in children {
                if forks.get(child_slot).is_some() || new_banks.get(&child_slot).is_some() {
                    trace!("child already active or frozen {}", child_slot);
                    continue;
                }
                let leader = leader_schedule_cache
                    .slot_leader_at(child_slot, Some(&parent_bank))
                    .unwrap();
                info!(
                    "new fork:{} parent:{} root:{}",
                    child_slot,
                    parent_slot,
                    forks.root()
                );
                let child_bank = Self::new_bank_from_parent_with_notify(
                    &parent_bank,
                    child_slot,
                    forks.root(),
                    &leader,
                    rpc_subscriptions,
                );
                let empty: Vec<Pubkey> = vec![];
                Self::update_fork_propagated_threshold_from_votes(
                    progress,
                    empty,
                    vec![leader],
                    parent_bank.slot(),
                    bank_forks,
                );
                new_banks.insert(child_slot, child_bank);
            }
        }
        drop(forks);

        let mut forks = bank_forks.write().unwrap();
        for (_, bank) in new_banks {
            forks.insert(bank);
        }
    }

    fn new_bank_from_parent_with_notify(
        parent: &Arc<Bank>,
        slot: u64,
        root_slot: u64,
        leader: &Pubkey,
        rpc_subscriptions: &Arc<RpcSubscriptions>,
    ) -> Bank {
        rpc_subscriptions.notify_slot(slot, parent.slot(), root_slot);
        Bank::new_from_parent(parent, leader, slot)
    }

    fn record_rewards(bank: &Bank, rewards_recorder_sender: &Option<RewardsRecorderSender>) {
        if let Some(rewards_recorder_sender) = rewards_recorder_sender {
            let rewards = bank.rewards.read().unwrap();
            if !rewards.is_empty() {
                rewards_recorder_sender
                    .send((bank.slot(), rewards.clone()))
                    .unwrap_or_else(|err| warn!("rewards_recorder_sender failed: {:?}", err));
            }
        }
    }

    pub fn get_unlock_switch_vote_slot(cluster_type: ClusterType) -> Slot {
        match cluster_type {
            ClusterType::Development => 0,
            ClusterType::Devnet => 0,
            // Epoch 63
            ClusterType::Testnet => 21_692_256,
            // 400_000 slots into epoch 61
            ClusterType::MainnetBeta => 26_752_000,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.commitment_service.join()?;
        self.t_replay.join().map(|_| ())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        consensus::{NullTowerStorage, Tower},
        progress_map::ValidatorStakeInfo,
        replay_stage::ReplayStage,
        tree_diff::TreeDiff,
        vote_simulator::{self, VoteSimulator},
    };
    use crossbeam_channel::unbounded;
    use solana_entry::entry::{self, Entry};
    use solana_gossip::{cluster_info::Node, crds::Cursor};
    use solana_ledger::{
        blockstore::make_slot_entries,
        blockstore::{entries_to_test_shreds, BlockstoreError},
        create_new_tmp_ledger,
        genesis_utils::{create_genesis_config, create_genesis_config_with_leader},
        get_tmp_ledger_path,
        shred::{
            CodingShredHeader, DataShredHeader, Shred, ShredCommonHeader, DATA_COMPLETE_SHRED,
            SIZE_OF_COMMON_SHRED_HEADER, SIZE_OF_DATA_SHRED_HEADER, SIZE_OF_DATA_SHRED_PAYLOAD,
        },
    };
    use solana_rpc::{
        optimistically_confirmed_bank_tracker::OptimisticallyConfirmedBank,
        rpc::create_test_transactions_and_populate_blockstore,
    };
    use solana_runtime::{
        accounts_background_service::AbsRequestSender,
        commitment::BlockCommitment,
        genesis_utils::{GenesisConfigInfo, ValidatorVoteKeypairs},
    };
    use solana_sdk::{
        clock::NUM_CONSECUTIVE_LEADER_SLOTS,
        genesis_config,
        hash::{hash, Hash},
        instruction::InstructionError,
        packet::PACKET_DATA_SIZE,
        poh_config::PohConfig,
        signature::{Keypair, Signer},
        system_transaction,
        transaction::TransactionError,
    };
    use solana_transaction_status::TransactionWithStatusMeta;
    use solana_vote_program::{
        vote_state::{VoteState, VoteStateVersions},
        vote_transaction,
    };
    use std::sync::mpsc::channel;
    use std::{
        fs::remove_dir_all,
        iter,
        sync::{atomic::AtomicU64, Arc, RwLock},
    };
    use trees::{tr, Tree};

    #[test]
    fn test_is_partition_detected() {
        let (VoteSimulator { bank_forks, .. }, _) = setup_default_forks(1, None::<GenerateVotes>);
        let ancestors = bank_forks.read().unwrap().ancestors();
        // Last vote 1 is an ancestor of the heaviest slot 3, no partition
        assert!(!ReplayStage::is_partition_detected(&ancestors, 1, 3));
        // Last vote 1 is an ancestor of the from heaviest slot 1, no partition
        assert!(!ReplayStage::is_partition_detected(&ancestors, 3, 3));
        // Last vote 2 is not an ancestor of the heaviest slot 3,
        // partition detected!
        assert!(ReplayStage::is_partition_detected(&ancestors, 2, 3));
        // Last vote 4 is not an ancestor of the heaviest slot 3,
        // partition detected!
        assert!(ReplayStage::is_partition_detected(&ancestors, 4, 3));
    }

    struct ReplayBlockstoreComponents {
        blockstore: Arc<Blockstore>,
        validator_node_to_vote_keys: HashMap<Pubkey, Pubkey>,
        my_pubkey: Pubkey,
        cluster_info: ClusterInfo,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        poh_recorder: Mutex<PohRecorder>,
        tower: Tower,
        rpc_subscriptions: Arc<RpcSubscriptions>,
        vote_simulator: VoteSimulator,
    }

    fn replay_blockstore_components(
        forks: Option<Tree<Slot>>,
        num_validators: usize,
        generate_votes: Option<GenerateVotes>,
    ) -> ReplayBlockstoreComponents {
        // Setup blockstore
        let (vote_simulator, blockstore) = setup_forks_from_tree(
            forks.unwrap_or_else(|| tr(0)),
            num_validators,
            generate_votes,
        );

        let VoteSimulator {
            ref validator_keypairs,
            ref bank_forks,
            ..
        } = vote_simulator;

        let blockstore = Arc::new(blockstore);
        let validator_node_to_vote_keys: HashMap<Pubkey, Pubkey> = validator_keypairs
            .iter()
            .map(|(_, keypairs)| {
                (
                    keypairs.node_keypair.pubkey(),
                    keypairs.vote_keypair.pubkey(),
                )
            })
            .collect();

        // ClusterInfo
        let my_keypairs = validator_keypairs.values().next().unwrap();
        let my_pubkey = my_keypairs.node_keypair.pubkey();
        let cluster_info = ClusterInfo::new(
            Node::new_localhost_with_pubkey(&my_pubkey).info,
            Arc::new(Keypair::from_bytes(&my_keypairs.node_keypair.to_bytes()).unwrap()),
        );
        assert_eq!(my_pubkey, cluster_info.id());

        // Leader schedule cache
        let root_bank = bank_forks.read().unwrap().root_bank();
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&root_bank));

        // PohRecorder
        let working_bank = bank_forks.read().unwrap().working_bank();
        let poh_recorder = Mutex::new(
            PohRecorder::new(
                working_bank.tick_height(),
                working_bank.last_blockhash(),
                working_bank.slot(),
                None,
                working_bank.ticks_per_slot(),
                &Pubkey::default(),
                &blockstore,
                &leader_schedule_cache,
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::new(false)),
            )
            .0,
        );

        // Tower
        let my_vote_pubkey = my_keypairs.vote_keypair.pubkey();
        let tower = Tower::new_from_bankforks(
            &bank_forks.read().unwrap(),
            &cluster_info.id(),
            &my_vote_pubkey,
        );

        // RpcSubscriptions
        let optimistically_confirmed_bank =
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(bank_forks);
        let exit = Arc::new(AtomicBool::new(false));
        let rpc_subscriptions = Arc::new(RpcSubscriptions::new(
            &exit,
            bank_forks.clone(),
            Arc::new(RwLock::new(BlockCommitmentCache::default())),
            optimistically_confirmed_bank,
        ));

        ReplayBlockstoreComponents {
            blockstore,
            validator_node_to_vote_keys,
            my_pubkey,
            cluster_info,
            leader_schedule_cache,
            poh_recorder,
            tower,
            rpc_subscriptions,
            vote_simulator,
        }
    }

    #[test]
    fn test_child_slots_of_same_parent() {
        let ReplayBlockstoreComponents {
            blockstore,
            validator_node_to_vote_keys,
            vote_simulator,
            leader_schedule_cache,
            rpc_subscriptions,
            ..
        } = replay_blockstore_components(None, 1, None::<GenerateVotes>);

        let VoteSimulator {
            mut progress,
            bank_forks,
            ..
        } = vote_simulator;

        // Insert a non-root bank so that the propagation logic will update this
        // bank
        let bank1 = Bank::new_from_parent(
            bank_forks.read().unwrap().get(0).unwrap(),
            &leader_schedule_cache.slot_leader_at(1, None).unwrap(),
            1,
        );
        progress.insert(
            1,
            ForkProgress::new_from_bank(
                &bank1,
                bank1.collector_id(),
                validator_node_to_vote_keys
                    .get(bank1.collector_id())
                    .unwrap(),
                Some(0),
                0,
                0,
            ),
        );
        assert!(progress.get_propagated_stats(1).unwrap().is_leader_slot);
        bank1.freeze();
        bank_forks.write().unwrap().insert(bank1);

        // Insert shreds for slot NUM_CONSECUTIVE_LEADER_SLOTS,
        // chaining to slot 1
        let (shreds, _) = make_slot_entries(NUM_CONSECUTIVE_LEADER_SLOTS, 1, 8);
        blockstore.insert_shreds(shreds, None, false).unwrap();
        assert!(bank_forks
            .read()
            .unwrap()
            .get(NUM_CONSECUTIVE_LEADER_SLOTS)
            .is_none());
        ReplayStage::generate_new_bank_forks(
            &blockstore,
            &bank_forks,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &mut progress,
        );
        assert!(bank_forks
            .read()
            .unwrap()
            .get(NUM_CONSECUTIVE_LEADER_SLOTS)
            .is_some());

        // Insert shreds for slot 2 * NUM_CONSECUTIVE_LEADER_SLOTS,
        // chaining to slot 1
        let (shreds, _) = make_slot_entries(2 * NUM_CONSECUTIVE_LEADER_SLOTS, 1, 8);
        blockstore.insert_shreds(shreds, None, false).unwrap();
        assert!(bank_forks
            .read()
            .unwrap()
            .get(2 * NUM_CONSECUTIVE_LEADER_SLOTS)
            .is_none());
        ReplayStage::generate_new_bank_forks(
            &blockstore,
            &bank_forks,
            &leader_schedule_cache,
            &rpc_subscriptions,
            &mut progress,
        );
        assert!(bank_forks
            .read()
            .unwrap()
            .get(NUM_CONSECUTIVE_LEADER_SLOTS)
            .is_some());
        assert!(bank_forks
            .read()
            .unwrap()
            .get(2 * NUM_CONSECUTIVE_LEADER_SLOTS)
            .is_some());

        // // There are 20 equally staked accounts, of which 3 have built
        // banks above or at bank 1. Because 3/20 < SUPERMINORITY_THRESHOLD,
        // we should see 3 validators in bank 1's propagated_validator set.
        let expected_leader_slots = vec![
            1,
            NUM_CONSECUTIVE_LEADER_SLOTS,
            2 * NUM_CONSECUTIVE_LEADER_SLOTS,
        ];
        for slot in expected_leader_slots {
            let leader = leader_schedule_cache.slot_leader_at(slot, None).unwrap();
            let vote_key = validator_node_to_vote_keys.get(&leader).unwrap();
            assert!(progress
                .get_propagated_stats(1)
                .unwrap()
                .propagated_validators
                .contains(vote_key));
        }
    }

    #[test]
    fn test_handle_new_root() {
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank0 = Bank::new(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank0)));

        let root = 3;
        let root_bank = Bank::new_from_parent(
            bank_forks.read().unwrap().get(0).unwrap(),
            &Pubkey::default(),
            root,
        );
        root_bank.freeze();
        let root_hash = root_bank.hash();
        bank_forks.write().unwrap().insert(root_bank);

        let mut heaviest_subtree_fork_choice = HeaviestSubtreeForkChoice::new((root, root_hash));

        let mut progress = ProgressMap::default();
        for i in 0..=root {
            progress.insert(i, ForkProgress::new(Hash::default(), None, None, 0, 0));
        }

        let mut duplicate_slots_tracker: DuplicateSlotsTracker =
            vec![root - 1, root, root + 1].into_iter().collect();
        let mut gossip_duplicate_confirmed_slots: GossipDuplicateConfirmedSlots =
            vec![root - 1, root, root + 1]
                .into_iter()
                .map(|s| (s, Hash::default()))
                .collect();
        let mut unfrozen_gossip_verified_vote_hashes: UnfrozenGossipVerifiedVoteHashes =
            UnfrozenGossipVerifiedVoteHashes {
                votes_per_slot: vec![root - 1, root, root + 1]
                    .into_iter()
                    .map(|s| (s, HashMap::new()))
                    .collect(),
            };
        ReplayStage::handle_new_root(
            root,
            &bank_forks,
            &mut progress,
            &AbsRequestSender::default(),
            None,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_tracker,
            &mut gossip_duplicate_confirmed_slots,
            &mut unfrozen_gossip_verified_vote_hashes,
            &mut true,
            &mut Vec::new(),
        );
        assert_eq!(bank_forks.read().unwrap().root(), root);
        assert_eq!(progress.len(), 1);
        assert!(progress.get(&root).is_some());
        // root - 1 is filtered out
        assert_eq!(
            duplicate_slots_tracker.into_iter().collect::<Vec<Slot>>(),
            vec![root, root + 1]
        );
        assert_eq!(
            gossip_duplicate_confirmed_slots
                .keys()
                .cloned()
                .collect::<Vec<Slot>>(),
            vec![root, root + 1]
        );
        assert_eq!(
            unfrozen_gossip_verified_vote_hashes
                .votes_per_slot
                .keys()
                .cloned()
                .collect::<Vec<Slot>>(),
            vec![root, root + 1]
        );
    }

    #[test]
    fn test_handle_new_root_ahead_of_highest_confirmed_root() {
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let bank0 = Bank::new(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank0)));
        let confirmed_root = 1;
        let fork = 2;
        let bank1 = Bank::new_from_parent(
            bank_forks.read().unwrap().get(0).unwrap(),
            &Pubkey::default(),
            confirmed_root,
        );
        bank_forks.write().unwrap().insert(bank1);
        let bank2 = Bank::new_from_parent(
            bank_forks.read().unwrap().get(confirmed_root).unwrap(),
            &Pubkey::default(),
            fork,
        );
        bank_forks.write().unwrap().insert(bank2);
        let root = 3;
        let root_bank = Bank::new_from_parent(
            bank_forks.read().unwrap().get(confirmed_root).unwrap(),
            &Pubkey::default(),
            root,
        );
        root_bank.freeze();
        let root_hash = root_bank.hash();
        bank_forks.write().unwrap().insert(root_bank);
        let mut heaviest_subtree_fork_choice = HeaviestSubtreeForkChoice::new((root, root_hash));
        let mut progress = ProgressMap::default();
        for i in 0..=root {
            progress.insert(i, ForkProgress::new(Hash::default(), None, None, 0, 0));
        }
        ReplayStage::handle_new_root(
            root,
            &bank_forks,
            &mut progress,
            &AbsRequestSender::default(),
            Some(confirmed_root),
            &mut heaviest_subtree_fork_choice,
            &mut DuplicateSlotsTracker::default(),
            &mut GossipDuplicateConfirmedSlots::default(),
            &mut UnfrozenGossipVerifiedVoteHashes::default(),
            &mut true,
            &mut Vec::new(),
        );
        assert_eq!(bank_forks.read().unwrap().root(), root);
        assert!(bank_forks.read().unwrap().get(confirmed_root).is_some());
        assert!(bank_forks.read().unwrap().get(fork).is_none());
        assert_eq!(progress.len(), 2);
        assert!(progress.get(&root).is_some());
        assert!(progress.get(&confirmed_root).is_some());
        assert!(progress.get(&fork).is_none());
    }

    #[test]
    fn test_dead_fork_transaction_error() {
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let missing_keypair = Keypair::new();
        let missing_keypair2 = Keypair::new();

        let res = check_dead_fork(|_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            let entry = entry::next_entry(
                &blockhash,
                hashes_per_tick.saturating_sub(1),
                vec![
                    system_transaction::transfer(&keypair1, &keypair2.pubkey(), 2, blockhash), // should be fine,
                    system_transaction::transfer(
                        &missing_keypair,
                        &missing_keypair2.pubkey(),
                        2,
                        blockhash,
                    ), // should cause AccountNotFound error
                ],
            );
            entries_to_test_shreds(vec![entry], slot, slot.saturating_sub(1), false, 0)
        });

        assert_matches!(
            res,
            Err(BlockstoreProcessorError::InvalidTransaction(
                TransactionError::AccountNotFound
            ))
        );
    }

    #[test]
    fn test_dead_fork_entry_verification_failure() {
        let keypair2 = Keypair::new();
        let res = check_dead_fork(|genesis_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let bad_hash = hash(&[2; 30]);
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            let entry = entry::next_entry(
                // Use wrong blockhash so that the entry causes an entry verification failure
                &bad_hash,
                hashes_per_tick.saturating_sub(1),
                vec![system_transaction::transfer(
                    genesis_keypair,
                    &keypair2.pubkey(),
                    2,
                    blockhash,
                )],
            );
            entries_to_test_shreds(vec![entry], slot, slot.saturating_sub(1), false, 0)
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::InvalidEntryHash);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_dead_fork_invalid_tick_hash_count() {
        let res = check_dead_fork(|_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            assert!(hashes_per_tick > 0);

            let too_few_hashes_tick = Entry::new(&blockhash, hashes_per_tick - 1, vec![]);
            entries_to_test_shreds(
                vec![too_few_hashes_tick],
                slot,
                slot.saturating_sub(1),
                false,
                0,
            )
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::InvalidTickHashCount);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_dead_fork_invalid_slot_tick_count() {
        solana_logger::setup();
        // Too many ticks per slot
        let res = check_dead_fork(|_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            entries_to_test_shreds(
                entry::create_ticks(bank.ticks_per_slot() + 1, hashes_per_tick, blockhash),
                slot,
                slot.saturating_sub(1),
                false,
                0,
            )
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::TooManyTicks);
        } else {
            panic!();
        }

        // Too few ticks per slot
        let res = check_dead_fork(|_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            entries_to_test_shreds(
                entry::create_ticks(bank.ticks_per_slot() - 1, hashes_per_tick, blockhash),
                slot,
                slot.saturating_sub(1),
                true,
                0,
            )
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::TooFewTicks);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_dead_fork_invalid_last_tick() {
        let res = check_dead_fork(|_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            entries_to_test_shreds(
                entry::create_ticks(bank.ticks_per_slot(), hashes_per_tick, blockhash),
                slot,
                slot.saturating_sub(1),
                false,
                0,
            )
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::InvalidLastTick);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_dead_fork_trailing_entry() {
        let keypair = Keypair::new();
        let res = check_dead_fork(|funded_keypair, bank| {
            let blockhash = bank.last_blockhash();
            let slot = bank.slot();
            let hashes_per_tick = bank.hashes_per_tick().unwrap_or(0);
            let mut entries =
                entry::create_ticks(bank.ticks_per_slot(), hashes_per_tick, blockhash);
            let last_entry_hash = entries.last().unwrap().hash;
            let tx = system_transaction::transfer(funded_keypair, &keypair.pubkey(), 2, blockhash);
            let trailing_entry = entry::next_entry(&last_entry_hash, 1, vec![tx]);
            entries.push(trailing_entry);
            entries_to_test_shreds(entries, slot, slot.saturating_sub(1), true, 0)
        });

        if let Err(BlockstoreProcessorError::InvalidBlock(block_error)) = res {
            assert_eq!(block_error, BlockError::TrailingEntry);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_dead_fork_entry_deserialize_failure() {
        // Insert entry that causes deserialization failure
        let res = check_dead_fork(|_, bank| {
            let gibberish = [0xa5u8; PACKET_DATA_SIZE];
            let mut data_header = DataShredHeader::default();
            data_header.flags |= DATA_COMPLETE_SHRED;
            // Need to provide the right size for Shredder::deshred.
            data_header.size = SIZE_OF_DATA_SHRED_PAYLOAD as u16;
            data_header.parent_offset = (bank.slot() - bank.parent_slot()) as u16;
            let shred_common_header = ShredCommonHeader {
                slot: bank.slot(),
                ..ShredCommonHeader::default()
            };
            let mut shred = Shred::new_empty_from_header(
                shred_common_header,
                data_header,
                CodingShredHeader::default(),
            );
            bincode::serialize_into(
                &mut shred.payload[SIZE_OF_COMMON_SHRED_HEADER + SIZE_OF_DATA_SHRED_HEADER..],
                &gibberish[..SIZE_OF_DATA_SHRED_PAYLOAD],
            )
            .unwrap();
            vec![shred]
        });

        assert_matches!(
            res,
            Err(BlockstoreProcessorError::FailedToLoadEntries(
                BlockstoreError::InvalidShredData(_)
            ),)
        );
    }

    // Given a shred and a fatal expected error, check that replaying that shred causes causes the fork to be
    // marked as dead. Returns the error for caller to verify.
    fn check_dead_fork<F>(shred_to_insert: F) -> result::Result<(), BlockstoreProcessorError>
    where
        F: Fn(&Keypair, Arc<Bank>) -> Vec<Shred>,
    {
        let ledger_path = get_tmp_ledger_path!();
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let res = {
            let ReplayBlockstoreComponents {
                blockstore,
                vote_simulator,
                ..
            } = replay_blockstore_components(Some(tr(0)), 1, None);
            let VoteSimulator {
                mut progress,
                bank_forks,
                mut heaviest_subtree_fork_choice,
                validator_keypairs,
                ..
            } = vote_simulator;

            let bank0 = bank_forks.read().unwrap().get(0).cloned().unwrap();
            assert!(bank0.is_frozen());
            assert_eq!(bank0.tick_height(), bank0.max_tick_height());
            let bank1 = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
            bank_forks.write().unwrap().insert(bank1);
            let bank1 = bank_forks.read().unwrap().get(1).cloned().unwrap();
            let mut bank1_progress = progress
                .entry(bank1.slot())
                .or_insert_with(|| ForkProgress::new(bank1.last_blockhash(), None, None, 0, 0));
            let shreds = shred_to_insert(
                &validator_keypairs.values().next().unwrap().node_keypair,
                bank1.clone(),
            );
            blockstore.insert_shreds(shreds, None, false).unwrap();
            let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
            let exit = Arc::new(AtomicBool::new(false));
            let res = ReplayStage::replay_blockstore_into_bank(
                &bank1,
                &blockstore,
                &mut bank1_progress,
                None,
                &replay_vote_sender,
                &VerifyRecyclers::default(),
            );
            let rpc_subscriptions = Arc::new(RpcSubscriptions::new(
                &exit,
                bank_forks.clone(),
                block_commitment_cache,
                OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
            ));
            if let Err(err) = &res {
                ReplayStage::mark_dead_slot(
                    &blockstore,
                    &bank1,
                    0,
                    err,
                    &rpc_subscriptions,
                    &mut DuplicateSlotsTracker::default(),
                    &GossipDuplicateConfirmedSlots::default(),
                    &mut progress,
                    &mut heaviest_subtree_fork_choice,
                    &mut DuplicateSlotsToRepair::default(),
                );
            }

            // Check that the erroring bank was marked as dead in the progress map
            assert!(progress
                .get(&bank1.slot())
                .map(|b| b.is_dead)
                .unwrap_or(false));

            // Check that the erroring bank was marked as dead in blockstore
            assert!(blockstore.is_dead(bank1.slot()));
            res.map(|_| ())
        };
        let _ignored = remove_dir_all(&ledger_path);
        res
    }

    #[test]
    fn test_replay_commitment_cache() {
        fn leader_vote(vote_slot: Slot, bank: &Arc<Bank>, pubkey: &Pubkey) {
            let mut leader_vote_account = bank.get_account(pubkey).unwrap();
            let mut vote_state = VoteState::from(&leader_vote_account).unwrap();
            vote_state.process_slot_vote_unchecked(vote_slot);
            let versioned = VoteStateVersions::new_current(vote_state);
            VoteState::to(&versioned, &mut leader_vote_account).unwrap();
            bank.store_account(pubkey, &leader_vote_account);
        }

        let leader_pubkey = solana_sdk::pubkey::new_rand();
        let leader_lamports = 3;
        let genesis_config_info =
            create_genesis_config_with_leader(50, &leader_pubkey, leader_lamports);
        let mut genesis_config = genesis_config_info.genesis_config;
        let leader_voting_pubkey = genesis_config_info.voting_keypair.pubkey();
        genesis_config.epoch_schedule.warmup = false;
        genesis_config.ticks_per_slot = 4;
        let bank0 = Bank::new(&genesis_config);
        for _ in 0..genesis_config.ticks_per_slot {
            bank0.register_tick(&Hash::default());
        }
        bank0.freeze();
        let arc_bank0 = Arc::new(bank0);
        let bank_forks = Arc::new(RwLock::new(BankForks::new_from_banks(&[arc_bank0], 0)));

        let exit = Arc::new(AtomicBool::new(false));
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
        let rpc_subscriptions = Arc::new(RpcSubscriptions::new(
            &exit,
            bank_forks.clone(),
            block_commitment_cache.clone(),
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
        ));
        let (lockouts_sender, _) = AggregateCommitmentService::new(
            &exit,
            block_commitment_cache.clone(),
            rpc_subscriptions,
        );

        assert!(block_commitment_cache
            .read()
            .unwrap()
            .get_block_commitment(0)
            .is_none());
        assert!(block_commitment_cache
            .read()
            .unwrap()
            .get_block_commitment(1)
            .is_none());

        for i in 1..=3 {
            let prev_bank = bank_forks.read().unwrap().get(i - 1).unwrap().clone();
            let bank = Bank::new_from_parent(&prev_bank, &Pubkey::default(), prev_bank.slot() + 1);
            let _res = bank.transfer(
                10,
                &genesis_config_info.mint_keypair,
                &solana_sdk::pubkey::new_rand(),
            );
            for _ in 0..genesis_config.ticks_per_slot {
                bank.register_tick(&Hash::default());
            }
            bank_forks.write().unwrap().insert(bank);
            let arc_bank = bank_forks.read().unwrap().get(i).unwrap().clone();
            leader_vote(i - 1, &arc_bank, &leader_voting_pubkey);
            ReplayStage::update_commitment_cache(
                arc_bank.clone(),
                0,
                leader_lamports,
                &lockouts_sender,
            );
            arc_bank.freeze();
        }

        for _ in 0..10 {
            let done = {
                let bcc = block_commitment_cache.read().unwrap();
                bcc.get_block_commitment(0).is_some()
                    && bcc.get_block_commitment(1).is_some()
                    && bcc.get_block_commitment(2).is_some()
            };
            if done {
                break;
            } else {
                thread::sleep(Duration::from_millis(200));
            }
        }

        let mut expected0 = BlockCommitment::default();
        expected0.increase_confirmation_stake(3, leader_lamports);
        assert_eq!(
            block_commitment_cache
                .read()
                .unwrap()
                .get_block_commitment(0)
                .unwrap(),
            &expected0,
        );
        let mut expected1 = BlockCommitment::default();
        expected1.increase_confirmation_stake(2, leader_lamports);
        assert_eq!(
            block_commitment_cache
                .read()
                .unwrap()
                .get_block_commitment(1)
                .unwrap(),
            &expected1
        );
        let mut expected2 = BlockCommitment::default();
        expected2.increase_confirmation_stake(1, leader_lamports);
        assert_eq!(
            block_commitment_cache
                .read()
                .unwrap()
                .get_block_commitment(2)
                .unwrap(),
            &expected2
        );
    }

    #[test]
    fn test_write_persist_transaction_status() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(1000);
        let (ledger_path, _) = create_new_tmp_ledger!(&genesis_config);
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to successfully open database ledger");
            let blockstore = Arc::new(blockstore);

            let keypair1 = Keypair::new();
            let keypair2 = Keypair::new();
            let keypair3 = Keypair::new();

            let bank0 = Arc::new(Bank::new(&genesis_config));
            bank0
                .transfer(4, &mint_keypair, &keypair2.pubkey())
                .unwrap();

            let bank1 = Arc::new(Bank::new_from_parent(&bank0, &Pubkey::default(), 1));
            let slot = bank1.slot();

            let signatures = create_test_transactions_and_populate_blockstore(
                vec![&mint_keypair, &keypair1, &keypair2, &keypair3],
                bank0.slot(),
                bank1,
                blockstore.clone(),
                Arc::new(AtomicU64::default()),
            );

            let confirmed_block = blockstore.get_rooted_block(slot, false).unwrap();
            assert_eq!(confirmed_block.transactions.len(), 3);

            for TransactionWithStatusMeta { transaction, meta } in
                confirmed_block.transactions.into_iter()
            {
                if transaction.signatures[0] == signatures[0] {
                    let meta = meta.unwrap();
                    assert_eq!(meta.status, Ok(()));
                } else if transaction.signatures[0] == signatures[1] {
                    let meta = meta.unwrap();
                    assert_eq!(
                        meta.status,
                        Err(TransactionError::InstructionError(
                            0,
                            InstructionError::Custom(1)
                        ))
                    );
                } else {
                    assert_eq!(meta, None);
                }
            }
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_compute_bank_stats_confirmed() {
        let vote_keypairs = ValidatorVoteKeypairs::new_rand();
        let my_node_pubkey = vote_keypairs.node_keypair.pubkey();
        let my_vote_pubkey = vote_keypairs.vote_keypair.pubkey();
        let keypairs: HashMap<_, _> = vec![(my_node_pubkey, vote_keypairs)].into_iter().collect();

        let (bank_forks, mut progress, mut heaviest_subtree_fork_choice) =
            vote_simulator::initialize_state(&keypairs, 10_000);
        let mut latest_validator_votes_for_frozen_banks =
            LatestValidatorVotesForFrozenBanks::default();
        let bank0 = bank_forks.get(0).unwrap().clone();
        let my_keypairs = keypairs.get(&my_node_pubkey).unwrap();
        let vote_tx = vote_transaction::new_vote_transaction(
            vec![0],
            bank0.hash(),
            bank0.last_blockhash(),
            &my_keypairs.node_keypair,
            &my_keypairs.vote_keypair,
            &my_keypairs.vote_keypair,
            None,
        );

        let bank_forks = RwLock::new(bank_forks);
        let bank1 = Bank::new_from_parent(&bank0, &my_node_pubkey, 1);
        bank1.process_transaction(&vote_tx).unwrap();
        bank1.freeze();

        // Test confirmations
        let ancestors = bank_forks.read().unwrap().ancestors();
        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let tower = Tower::new_for_tests(0, 0.67);
        let newly_computed = ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &ancestors,
            &mut frozen_banks,
            &tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        // bank 0 has no votes, should not send any votes on the channel
        assert_eq!(newly_computed, vec![0]);
        // The only vote is in bank 1, and bank_forks does not currently contain
        // bank 1, so no slot should be confirmed.
        {
            let fork_progress = progress.get(&0).unwrap();
            let confirmed_forks = ReplayStage::confirm_forks(
                &tower,
                &fork_progress.fork_stats.voted_stakes,
                fork_progress.fork_stats.total_stake,
                &progress,
                &bank_forks,
            );

            assert!(confirmed_forks.is_empty());
        }

        // Insert the bank that contains a vote for slot 0, which confirms slot 0
        bank_forks.write().unwrap().insert(bank1);
        progress.insert(
            1,
            ForkProgress::new(bank0.last_blockhash(), None, None, 0, 0),
        );
        let ancestors = bank_forks.read().unwrap().ancestors();
        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let newly_computed = ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &ancestors,
            &mut frozen_banks,
            &tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        // Bank 1 had one vote
        assert_eq!(newly_computed, vec![1]);
        {
            let fork_progress = progress.get(&1).unwrap();
            let confirmed_forks = ReplayStage::confirm_forks(
                &tower,
                &fork_progress.fork_stats.voted_stakes,
                fork_progress.fork_stats.total_stake,
                &progress,
                &bank_forks,
            );
            // No new stats should have been computed
            assert_eq!(confirmed_forks, vec![(0, bank0.hash())]);
        }

        let ancestors = bank_forks.read().unwrap().ancestors();
        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let newly_computed = ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &ancestors,
            &mut frozen_banks,
            &tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );
        // No new stats should have been computed
        assert!(newly_computed.is_empty());
    }

    #[test]
    fn test_same_weight_select_lower_slot() {
        // Init state
        let mut vote_simulator = VoteSimulator::new(1);
        let tower = Tower::default();

        // Create the tree of banks in a BankForks object
        let forks = tr(0) / (tr(1)) / (tr(2));
        vote_simulator.fill_bank_forks(forks, &HashMap::new());
        let mut frozen_banks: Vec<_> = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let mut heaviest_subtree_fork_choice = &mut vote_simulator.heaviest_subtree_fork_choice;
        let mut latest_validator_votes_for_frozen_banks =
            LatestValidatorVotesForFrozenBanks::default();
        let ancestors = vote_simulator.bank_forks.read().unwrap().ancestors();

        let my_vote_pubkey = vote_simulator.vote_pubkeys[0];
        ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &ancestors,
            &mut frozen_banks,
            &tower,
            &mut vote_simulator.progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &vote_simulator.bank_forks,
            &mut heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        let bank1 = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .get(1)
            .unwrap()
            .clone();
        let bank2 = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .get(2)
            .unwrap()
            .clone();
        assert_eq!(
            heaviest_subtree_fork_choice
                .stake_voted_subtree(&(1, bank1.hash()))
                .unwrap(),
            heaviest_subtree_fork_choice
                .stake_voted_subtree(&(2, bank2.hash()))
                .unwrap()
        );

        let (heaviest_bank, _) = heaviest_subtree_fork_choice.select_forks(
            &frozen_banks,
            &tower,
            &vote_simulator.progress,
            &ancestors,
            &vote_simulator.bank_forks,
        );

        // Should pick the lower of the two equally weighted banks
        assert_eq!(heaviest_bank.slot(), 1);
    }

    #[test]
    fn test_child_bank_heavier() {
        // Init state
        let mut vote_simulator = VoteSimulator::new(1);
        let my_node_pubkey = vote_simulator.node_pubkeys[0];
        let mut tower = Tower::default();

        // Create the tree of banks in a BankForks object
        let forks = tr(0) / (tr(1) / (tr(2) / (tr(3))));

        // Set the voting behavior
        let mut cluster_votes = HashMap::new();
        let votes = vec![0, 2];
        cluster_votes.insert(my_node_pubkey, votes.clone());
        vote_simulator.fill_bank_forks(forks, &cluster_votes);

        // Fill banks with votes
        for vote in votes {
            assert!(vote_simulator
                .simulate_vote(vote, &my_node_pubkey, &mut tower,)
                .is_empty());
        }

        let mut frozen_banks: Vec<_> = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();

        let my_vote_pubkey = vote_simulator.vote_pubkeys[0];
        ReplayStage::compute_bank_stats(
            &my_vote_pubkey,
            &vote_simulator.bank_forks.read().unwrap().ancestors(),
            &mut frozen_banks,
            &tower,
            &mut vote_simulator.progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &vote_simulator.bank_forks,
            &mut vote_simulator.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
        );

        frozen_banks.sort_by_key(|bank| bank.slot());
        for pair in frozen_banks.windows(2) {
            let first = vote_simulator
                .progress
                .get_fork_stats(pair[0].slot())
                .unwrap()
                .fork_weight;
            let second = vote_simulator
                .progress
                .get_fork_stats(pair[1].slot())
                .unwrap()
                .fork_weight;
            assert!(second >= first);
        }
        for bank in frozen_banks {
            // The only leaf should always be chosen over parents
            assert_eq!(
                vote_simulator
                    .heaviest_subtree_fork_choice
                    .best_slot(&(bank.slot(), bank.hash()))
                    .unwrap()
                    .0,
                3
            );
        }
    }

    #[test]
    fn test_should_retransmit() {
        let poh_slot = 4;
        let mut last_retransmit_slot = 4;
        // We retransmitted already at slot 4, shouldn't retransmit until
        // >= 4 + NUM_CONSECUTIVE_LEADER_SLOTS, or if we reset to < 4
        assert!(!ReplayStage::should_retransmit(
            poh_slot,
            &mut last_retransmit_slot
        ));
        assert_eq!(last_retransmit_slot, 4);

        for poh_slot in 4..4 + NUM_CONSECUTIVE_LEADER_SLOTS {
            assert!(!ReplayStage::should_retransmit(
                poh_slot,
                &mut last_retransmit_slot
            ));
            assert_eq!(last_retransmit_slot, 4);
        }

        let poh_slot = 4 + NUM_CONSECUTIVE_LEADER_SLOTS;
        last_retransmit_slot = 4;
        assert!(ReplayStage::should_retransmit(
            poh_slot,
            &mut last_retransmit_slot
        ));
        assert_eq!(last_retransmit_slot, poh_slot);

        let poh_slot = 3;
        last_retransmit_slot = 4;
        assert!(ReplayStage::should_retransmit(
            poh_slot,
            &mut last_retransmit_slot
        ));
        assert_eq!(last_retransmit_slot, poh_slot);
    }

    #[test]
    fn test_update_slot_propagated_threshold_from_votes() {
        let keypairs: HashMap<_, _> = iter::repeat_with(|| {
            let vote_keypairs = ValidatorVoteKeypairs::new_rand();
            (vote_keypairs.node_keypair.pubkey(), vote_keypairs)
        })
        .take(10)
        .collect();

        let new_vote_pubkeys: Vec<_> = keypairs
            .values()
            .map(|keys| keys.vote_keypair.pubkey())
            .collect();
        let new_node_pubkeys: Vec<_> = keypairs
            .values()
            .map(|keys| keys.node_keypair.pubkey())
            .collect();

        // Once 4/10 validators have voted, we have hit threshold
        run_test_update_slot_propagated_threshold_from_votes(&keypairs, &new_vote_pubkeys, &[], 4);
        // Adding the same node pubkey's instead of the corresponding
        // vote pubkeys should be equivalent
        run_test_update_slot_propagated_threshold_from_votes(&keypairs, &[], &new_node_pubkeys, 4);
        // Adding the same node pubkey's in the same order as their
        // corresponding vote accounts is redundant, so we don't
        // reach the threshold any sooner.
        run_test_update_slot_propagated_threshold_from_votes(
            &keypairs,
            &new_vote_pubkeys,
            &new_node_pubkeys,
            4,
        );
        // However, if we add different node pubkey's than the
        // vote accounts, we should hit threshold much faster
        // because now we are getting 2 new pubkeys on each
        // iteration instead of 1, so by the 2nd iteration
        // we should have 4/10 validators voting
        run_test_update_slot_propagated_threshold_from_votes(
            &keypairs,
            &new_vote_pubkeys[0..5],
            &new_node_pubkeys[5..],
            2,
        );
    }

    fn run_test_update_slot_propagated_threshold_from_votes(
        all_keypairs: &HashMap<Pubkey, ValidatorVoteKeypairs>,
        new_vote_pubkeys: &[Pubkey],
        new_node_pubkeys: &[Pubkey],
        success_index: usize,
    ) {
        let stake = 10_000;
        let (bank_forks, _, _) = vote_simulator::initialize_state(all_keypairs, stake);
        let root_bank = bank_forks.root_bank();
        let mut propagated_stats = PropagatedStats {
            total_epoch_stake: stake * all_keypairs.len() as u64,
            ..PropagatedStats::default()
        };

        let child_reached_threshold = false;
        for i in 0..std::cmp::max(new_vote_pubkeys.len(), new_node_pubkeys.len()) {
            propagated_stats.is_propagated = false;
            let len = std::cmp::min(i, new_vote_pubkeys.len());
            let mut voted_pubkeys = new_vote_pubkeys[..len].iter().copied().collect();
            let len = std::cmp::min(i, new_node_pubkeys.len());
            let mut node_pubkeys = new_node_pubkeys[..len].iter().copied().collect();
            let did_newly_reach_threshold =
                ReplayStage::update_slot_propagated_threshold_from_votes(
                    &mut voted_pubkeys,
                    &mut node_pubkeys,
                    &root_bank,
                    &mut propagated_stats,
                    child_reached_threshold,
                );

            // Only the i'th voted pubkey should be new (everything else was
            // inserted in previous iteration of the loop), so those redundant
            // pubkeys should have been filtered out
            let remaining_vote_pubkeys = {
                if i == 0 || i >= new_vote_pubkeys.len() {
                    vec![]
                } else {
                    vec![new_vote_pubkeys[i - 1]]
                }
            };
            let remaining_node_pubkeys = {
                if i == 0 || i >= new_node_pubkeys.len() {
                    vec![]
                } else {
                    vec![new_node_pubkeys[i - 1]]
                }
            };
            assert_eq!(voted_pubkeys, remaining_vote_pubkeys);
            assert_eq!(node_pubkeys, remaining_node_pubkeys);

            // If we crossed the superminority threshold, then
            // `did_newly_reach_threshold == true`, otherwise the
            // threshold has not been reached
            if i >= success_index {
                assert!(propagated_stats.is_propagated);
                assert!(did_newly_reach_threshold);
            } else {
                assert!(!propagated_stats.is_propagated);
                assert!(!did_newly_reach_threshold);
            }
        }
    }

    #[test]
    fn test_update_slot_propagated_threshold_from_votes2() {
        let mut empty: Vec<Pubkey> = vec![];
        let genesis_config = create_genesis_config(100_000_000).genesis_config;
        let root_bank = Bank::new(&genesis_config);
        let stake = 10_000;
        // Simulate a child slot seeing threshold (`child_reached_threshold` = true),
        // then the parent should also be marked as having reached threshold,
        // even if there are no new pubkeys to add (`newly_voted_pubkeys.is_empty()`)
        let mut propagated_stats = PropagatedStats {
            total_epoch_stake: stake * 10,
            ..PropagatedStats::default()
        };
        propagated_stats.total_epoch_stake = stake * 10;
        let child_reached_threshold = true;
        let mut newly_voted_pubkeys: Vec<Pubkey> = vec![];

        assert!(ReplayStage::update_slot_propagated_threshold_from_votes(
            &mut newly_voted_pubkeys,
            &mut empty,
            &root_bank,
            &mut propagated_stats,
            child_reached_threshold,
        ));

        // If propagation already happened (propagated_stats.is_propagated = true),
        // always returns false
        propagated_stats = PropagatedStats {
            total_epoch_stake: stake * 10,
            ..PropagatedStats::default()
        };
        propagated_stats.is_propagated = true;
        newly_voted_pubkeys = vec![];
        assert!(!ReplayStage::update_slot_propagated_threshold_from_votes(
            &mut newly_voted_pubkeys,
            &mut empty,
            &root_bank,
            &mut propagated_stats,
            child_reached_threshold,
        ));

        let child_reached_threshold = false;
        assert!(!ReplayStage::update_slot_propagated_threshold_from_votes(
            &mut newly_voted_pubkeys,
            &mut empty,
            &root_bank,
            &mut propagated_stats,
            child_reached_threshold,
        ));
    }

    #[test]
    fn test_update_propagation_status() {
        // Create genesis stakers
        let vote_keypairs = ValidatorVoteKeypairs::new_rand();
        let node_pubkey = vote_keypairs.node_keypair.pubkey();
        let vote_pubkey = vote_keypairs.vote_keypair.pubkey();
        let keypairs: HashMap<_, _> = vec![(node_pubkey, vote_keypairs)].into_iter().collect();
        let stake = 10_000;
        let (mut bank_forks, mut progress_map, _) =
            vote_simulator::initialize_state(&keypairs, stake);

        let bank0 = bank_forks.get(0).unwrap().clone();
        bank_forks.insert(Bank::new_from_parent(&bank0, &Pubkey::default(), 9));
        let bank9 = bank_forks.get(9).unwrap().clone();
        bank_forks.insert(Bank::new_from_parent(&bank9, &Pubkey::default(), 10));
        bank_forks.set_root(9, &AbsRequestSender::default(), None);
        let total_epoch_stake = bank0.total_epoch_stake();

        // Insert new ForkProgress for slot 10 and its
        // previous leader slot 9
        progress_map.insert(
            10,
            ForkProgress::new(
                Hash::default(),
                Some(9),
                Some(ValidatorStakeInfo {
                    total_epoch_stake,
                    ..ValidatorStakeInfo::default()
                }),
                0,
                0,
            ),
        );
        progress_map.insert(
            9,
            ForkProgress::new(
                Hash::default(),
                Some(8),
                Some(ValidatorStakeInfo {
                    total_epoch_stake,
                    ..ValidatorStakeInfo::default()
                }),
                0,
                0,
            ),
        );

        // Make sure is_propagated == false so that the propagation logic
        // runs in `update_propagation_status`
        assert!(!progress_map.is_propagated(10));

        let vote_tracker = VoteTracker::new(&bank_forks.root_bank());
        vote_tracker.insert_vote(10, vote_pubkey);
        ReplayStage::update_propagation_status(
            &mut progress_map,
            10,
            &RwLock::new(bank_forks),
            &vote_tracker,
            &ClusterSlots::default(),
        );

        let propagated_stats = &progress_map.get(&10).unwrap().propagated_stats;

        // There should now be a cached reference to the VoteTracker for
        // slot 10
        assert!(propagated_stats.slot_vote_tracker.is_some());

        // Updates should have been consumed
        assert!(propagated_stats
            .slot_vote_tracker
            .as_ref()
            .unwrap()
            .write()
            .unwrap()
            .get_voted_slot_updates()
            .is_none());

        // The voter should be recorded
        assert!(propagated_stats
            .propagated_validators
            .contains(&vote_pubkey));

        assert_eq!(propagated_stats.propagated_validators_stake, stake);
    }

    #[test]
    fn test_chain_update_propagation_status() {
        let keypairs: HashMap<_, _> = iter::repeat_with(|| {
            let vote_keypairs = ValidatorVoteKeypairs::new_rand();
            (vote_keypairs.node_keypair.pubkey(), vote_keypairs)
        })
        .take(10)
        .collect();

        let vote_pubkeys: Vec<_> = keypairs
            .values()
            .map(|keys| keys.vote_keypair.pubkey())
            .collect();

        let stake_per_validator = 10_000;
        let (mut bank_forks, mut progress_map, _) =
            vote_simulator::initialize_state(&keypairs, stake_per_validator);
        progress_map
            .get_propagated_stats_mut(0)
            .unwrap()
            .is_leader_slot = true;
        bank_forks.set_root(0, &AbsRequestSender::default(), None);
        let total_epoch_stake = bank_forks.root_bank().total_epoch_stake();

        // Insert new ForkProgress representing a slot for all slots 1..=num_banks. Only
        // make even numbered ones leader slots
        for i in 1..=10 {
            let parent_bank = bank_forks.get(i - 1).unwrap().clone();
            let prev_leader_slot = ((i - 1) / 2) * 2;
            bank_forks.insert(Bank::new_from_parent(&parent_bank, &Pubkey::default(), i));
            progress_map.insert(
                i,
                ForkProgress::new(
                    Hash::default(),
                    Some(prev_leader_slot),
                    {
                        if i % 2 == 0 {
                            Some(ValidatorStakeInfo {
                                total_epoch_stake,
                                ..ValidatorStakeInfo::default()
                            })
                        } else {
                            None
                        }
                    },
                    0,
                    0,
                ),
            );
        }

        let vote_tracker = VoteTracker::new(&bank_forks.root_bank());
        for vote_pubkey in &vote_pubkeys {
            // Insert a vote for the last bank for each voter
            vote_tracker.insert_vote(10, *vote_pubkey);
        }

        // The last bank should reach propagation threshold, and propagate it all
        // the way back through earlier leader banks
        ReplayStage::update_propagation_status(
            &mut progress_map,
            10,
            &RwLock::new(bank_forks),
            &vote_tracker,
            &ClusterSlots::default(),
        );

        for i in 1..=10 {
            let propagated_stats = &progress_map.get(&i).unwrap().propagated_stats;
            // Only the even numbered ones were leader banks, so only
            // those should have been updated
            if i % 2 == 0 {
                assert!(propagated_stats.is_propagated);
            } else {
                assert!(!propagated_stats.is_propagated);
            }
        }
    }

    #[test]
    fn test_chain_update_propagation_status2() {
        let num_validators = 6;
        let keypairs: HashMap<_, _> = iter::repeat_with(|| {
            let vote_keypairs = ValidatorVoteKeypairs::new_rand();
            (vote_keypairs.node_keypair.pubkey(), vote_keypairs)
        })
        .take(num_validators)
        .collect();

        let vote_pubkeys: Vec<_> = keypairs
            .values()
            .map(|keys| keys.vote_keypair.pubkey())
            .collect();

        let stake_per_validator = 10_000;
        let (mut bank_forks, mut progress_map, _) =
            vote_simulator::initialize_state(&keypairs, stake_per_validator);
        progress_map
            .get_propagated_stats_mut(0)
            .unwrap()
            .is_leader_slot = true;
        bank_forks.set_root(0, &AbsRequestSender::default(), None);

        let total_epoch_stake = num_validators as u64 * stake_per_validator;

        // Insert new ForkProgress representing a slot for all slots 1..=num_banks. Only
        // make even numbered ones leader slots
        for i in 1..=10 {
            let parent_bank = bank_forks.get(i - 1).unwrap().clone();
            let prev_leader_slot = i - 1;
            bank_forks.insert(Bank::new_from_parent(&parent_bank, &Pubkey::default(), i));
            let mut fork_progress = ForkProgress::new(
                Hash::default(),
                Some(prev_leader_slot),
                Some(ValidatorStakeInfo {
                    total_epoch_stake,
                    ..ValidatorStakeInfo::default()
                }),
                0,
                0,
            );

            let end_range = {
                // The earlier slots are one pubkey away from reaching confirmation
                if i < 5 {
                    2
                } else {
                    // The later slots are two pubkeys away from reaching confirmation
                    1
                }
            };
            fork_progress.propagated_stats.propagated_validators =
                vote_pubkeys[0..end_range].iter().copied().collect();
            fork_progress.propagated_stats.propagated_validators_stake =
                end_range as u64 * stake_per_validator;
            progress_map.insert(i, fork_progress);
        }

        let vote_tracker = VoteTracker::new(&bank_forks.root_bank());
        // Insert a new vote
        vote_tracker.insert_vote(10, vote_pubkeys[2]);

        // The last bank should reach propagation threshold, and propagate it all
        // the way back through earlier leader banks
        ReplayStage::update_propagation_status(
            &mut progress_map,
            10,
            &RwLock::new(bank_forks),
            &vote_tracker,
            &ClusterSlots::default(),
        );

        // Only the first 5 banks should have reached the threshold
        for i in 1..=10 {
            let propagated_stats = &progress_map.get(&i).unwrap().propagated_stats;
            if i < 5 {
                assert!(propagated_stats.is_propagated);
            } else {
                assert!(!propagated_stats.is_propagated);
            }
        }
    }

    #[test]
    fn test_check_propagation_for_start_leader() {
        let mut progress_map = ProgressMap::default();
        let poh_slot = 5;
        let parent_slot = poh_slot - NUM_CONSECUTIVE_LEADER_SLOTS;

        // If there is no previous leader slot (previous leader slot is None),
        // should succeed
        progress_map.insert(
            parent_slot,
            ForkProgress::new(Hash::default(), None, None, 0, 0),
        );
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // Now if we make the parent was itself the leader, then requires propagation
        // confirmation check because the parent is at least NUM_CONSECUTIVE_LEADER_SLOTS
        // slots from the `poh_slot`
        progress_map.insert(
            parent_slot,
            ForkProgress::new(
                Hash::default(),
                None,
                Some(ValidatorStakeInfo::default()),
                0,
                0,
            ),
        );
        assert!(!ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));
        progress_map
            .get_mut(&parent_slot)
            .unwrap()
            .propagated_stats
            .is_propagated = true;
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));
        // Now, set up the progress map to show that the `previous_leader_slot` of 5 is
        // `parent_slot - 1` (not equal to the actual parent!), so `parent_slot - 1` needs
        // to see propagation confirmation before we can start a leader for block 5
        let previous_leader_slot = parent_slot - 1;
        progress_map.insert(
            parent_slot,
            ForkProgress::new(Hash::default(), Some(previous_leader_slot), None, 0, 0),
        );
        progress_map.insert(
            previous_leader_slot,
            ForkProgress::new(
                Hash::default(),
                None,
                Some(ValidatorStakeInfo::default()),
                0,
                0,
            ),
        );

        // `previous_leader_slot` has not seen propagation threshold, so should fail
        assert!(!ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // If we set the is_propagated = true for the `previous_leader_slot`, should
        // allow the block to be generated
        progress_map
            .get_mut(&previous_leader_slot)
            .unwrap()
            .propagated_stats
            .is_propagated = true;
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // If the root is now set to `parent_slot`, this filters out `previous_leader_slot` from the progress map,
        // which implies confirmation
        let bank0 = Bank::new(&genesis_config::create_genesis_config(10000).0);
        let parent_slot_bank =
            Bank::new_from_parent(&Arc::new(bank0), &Pubkey::default(), parent_slot);
        let mut bank_forks = BankForks::new(parent_slot_bank);
        let bank5 =
            Bank::new_from_parent(bank_forks.get(parent_slot).unwrap(), &Pubkey::default(), 5);
        bank_forks.insert(bank5);

        // Should purge only `previous_leader_slot` from the progress map
        progress_map.handle_new_root(&bank_forks);

        // Should succeed
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));
    }

    #[test]
    fn test_check_propagation_skip_propagation_check() {
        let mut progress_map = ProgressMap::default();
        let poh_slot = 4;
        let mut parent_slot = poh_slot - 1;

        // Set up the progress map to show that the last leader slot of 4 is 3,
        // which means 3 and 4 are consecutive leader slots
        progress_map.insert(
            3,
            ForkProgress::new(
                Hash::default(),
                None,
                Some(ValidatorStakeInfo::default()),
                0,
                0,
            ),
        );

        // If the previous leader slot has not seen propagation threshold, but
        // was the direct parent (implying consecutive leader slots), create
        // the block regardless
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // If propagation threshold was achieved on parent, block should
        // also be created
        progress_map
            .get_mut(&3)
            .unwrap()
            .propagated_stats
            .is_propagated = true;
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // Now insert another parent slot 2 for which this validator is also the leader
        parent_slot = poh_slot - NUM_CONSECUTIVE_LEADER_SLOTS + 1;
        progress_map.insert(
            parent_slot,
            ForkProgress::new(
                Hash::default(),
                None,
                Some(ValidatorStakeInfo::default()),
                0,
                0,
            ),
        );

        // Even though `parent_slot` and `poh_slot` are separated by another block,
        // because they're within `NUM_CONSECUTIVE` blocks of each other, the propagation
        // check is still skipped
        assert!(ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));

        // Once the distance becomes >= NUM_CONSECUTIVE_LEADER_SLOTS, then we need to
        // enforce the propagation check
        parent_slot = poh_slot - NUM_CONSECUTIVE_LEADER_SLOTS;
        progress_map.insert(
            parent_slot,
            ForkProgress::new(
                Hash::default(),
                None,
                Some(ValidatorStakeInfo::default()),
                0,
                0,
            ),
        );
        assert!(!ReplayStage::check_propagation_for_start_leader(
            poh_slot,
            parent_slot,
            &progress_map,
        ));
    }

    #[test]
    fn test_purge_unconfirmed_duplicate_slot() {
        let (vote_simulator, blockstore) = setup_default_forks(2, None::<GenerateVotes>);
        let VoteSimulator {
            bank_forks,
            node_pubkeys,
            mut progress,
            validator_keypairs,
            ..
        } = vote_simulator;

        // Create bank 7
        let root_bank = bank_forks.read().unwrap().root_bank();
        let bank7 = Bank::new_from_parent(
            &bank_forks.read().unwrap().get(6).unwrap().clone(),
            &Pubkey::default(),
            7,
        );
        bank_forks.write().unwrap().insert(bank7);
        blockstore.add_tree(tr(6) / tr(7), false, false, 3, Hash::default());
        let bank7 = bank_forks.read().unwrap().get(7).unwrap().clone();
        let mut descendants = bank_forks.read().unwrap().descendants().clone();
        let mut ancestors = bank_forks.read().unwrap().ancestors();

        // Process a transfer on bank 7
        let sender = node_pubkeys[0];
        let receiver = node_pubkeys[1];
        let old_balance = bank7.get_balance(&sender);
        let transfer_amount = old_balance / 2;
        let transfer_sig = bank7
            .transfer(
                transfer_amount,
                &validator_keypairs.get(&sender).unwrap().node_keypair,
                &receiver,
            )
            .unwrap();

        // Process a vote for slot 0 in bank 5
        let validator0_keypairs = &validator_keypairs.get(&sender).unwrap();
        let bank0 = bank_forks.read().unwrap().get(0).unwrap().clone();
        let vote_tx = vote_transaction::new_vote_transaction(
            vec![0],
            bank0.hash(),
            bank0.last_blockhash(),
            &validator0_keypairs.node_keypair,
            &validator0_keypairs.vote_keypair,
            &validator0_keypairs.vote_keypair,
            None,
        );
        bank7.process_transaction(&vote_tx).unwrap();
        assert!(bank7.get_signature_status(&vote_tx.signatures[0]).is_some());

        // Both signatures should exist in status cache
        assert!(bank7.get_signature_status(&vote_tx.signatures[0]).is_some());
        assert!(bank7.get_signature_status(&transfer_sig).is_some());

        // Mark slot 7 dead
        blockstore
            .set_dead_slot(7)
            .expect("Failed to mark slot as dead in blockstore");

        // Purging slot 5 should purge only slots 5 and its descendants 6,7
        ReplayStage::purge_unconfirmed_duplicate_slot(
            5,
            &mut ancestors,
            &mut descendants,
            &mut progress,
            &root_bank,
            &bank_forks,
            &blockstore,
        );
        for i in 5..=7 {
            assert!(bank_forks.read().unwrap().get(i).is_none());
            assert!(progress.get(&i).is_none());
        }
        for i in 0..=4 {
            assert!(bank_forks.read().unwrap().get(i).is_some());
            assert!(progress.get(&i).is_some());
        }

        // Blockstore should have been cleared
        for slot in &[5, 6, 7] {
            assert!(!blockstore.is_full(*slot));
            // Slot 7 was marked dead before, should no longer be marked
            assert!(!blockstore.is_dead(*slot));
            assert!(blockstore.get_slot_entries(*slot, 0).unwrap().is_empty());
        }

        // Should not be able to find signature in slot 5 for previously
        // processed transactions
        assert!(bank7.get_signature_status(&vote_tx.signatures[0]).is_none());
        assert!(bank7.get_signature_status(&transfer_sig).is_none());

        // Getting balance should return the old balance (accounts were cleared)
        assert_eq!(bank7.get_balance(&sender), old_balance);

        // Purging slot 4 should purge only slot 4
        let mut descendants = bank_forks.read().unwrap().descendants().clone();
        let mut ancestors = bank_forks.read().unwrap().ancestors();
        ReplayStage::purge_unconfirmed_duplicate_slot(
            4,
            &mut ancestors,
            &mut descendants,
            &mut progress,
            &root_bank,
            &bank_forks,
            &blockstore,
        );
        for i in 4..=7 {
            assert!(bank_forks.read().unwrap().get(i).is_none());
            assert!(progress.get(&i).is_none());
            assert!(blockstore.get_slot_entries(i, 0).unwrap().is_empty());
        }
        for i in 0..=3 {
            assert!(bank_forks.read().unwrap().get(i).is_some());
            assert!(progress.get(&i).is_some());
            assert!(!blockstore.get_slot_entries(i, 0).unwrap().is_empty());
        }

        // Purging slot 1 should purge both forks 2 and 3
        let mut descendants = bank_forks.read().unwrap().descendants().clone();
        let mut ancestors = bank_forks.read().unwrap().ancestors();
        ReplayStage::purge_unconfirmed_duplicate_slot(
            1,
            &mut ancestors,
            &mut descendants,
            &mut progress,
            &root_bank,
            &bank_forks,
            &blockstore,
        );
        for i in 1..=7 {
            assert!(bank_forks.read().unwrap().get(i).is_none());
            assert!(progress.get(&i).is_none());
            assert!(blockstore.get_slot_entries(i, 0).unwrap().is_empty());
        }
        assert!(bank_forks.read().unwrap().get(0).is_some());
        assert!(progress.get(&0).is_some());
    }

    #[test]
    fn test_purge_ancestors_descendants() {
        let (VoteSimulator { bank_forks, .. }, _) = setup_default_forks(1, None::<GenerateVotes>);

        // Purge branch rooted at slot 2
        let mut descendants = bank_forks.read().unwrap().descendants().clone();
        let mut ancestors = bank_forks.read().unwrap().ancestors();
        let slot_2_descendants = descendants.get(&2).unwrap().clone();
        ReplayStage::purge_ancestors_descendants(
            2,
            &slot_2_descendants,
            &mut ancestors,
            &mut descendants,
        );

        // Result should be equivalent to removing slot from BankForks
        // and regenerating the `ancestor` `descendant` maps
        for d in slot_2_descendants {
            bank_forks.write().unwrap().remove(d);
        }
        bank_forks.write().unwrap().remove(2);
        assert!(check_map_eq(
            &ancestors,
            &bank_forks.read().unwrap().ancestors()
        ));
        assert!(check_map_eq(
            &descendants,
            bank_forks.read().unwrap().descendants()
        ));

        // Try to purge the root
        bank_forks
            .write()
            .unwrap()
            .set_root(3, &AbsRequestSender::default(), None);
        let mut descendants = bank_forks.read().unwrap().descendants().clone();
        let mut ancestors = bank_forks.read().unwrap().ancestors();
        let slot_3_descendants = descendants.get(&3).unwrap().clone();
        ReplayStage::purge_ancestors_descendants(
            3,
            &slot_3_descendants,
            &mut ancestors,
            &mut descendants,
        );

        assert!(ancestors.is_empty());
        // Only remaining keys should be ones < root
        for k in descendants.keys() {
            assert!(*k < 3);
        }
    }

    #[test]
    fn test_leader_snapshot_restart_propagation() {
        let ReplayBlockstoreComponents {
            validator_node_to_vote_keys,
            leader_schedule_cache,
            vote_simulator,
            ..
        } = replay_blockstore_components(None, 1, None::<GenerateVotes>);

        let VoteSimulator {
            mut progress,
            bank_forks,
            ..
        } = vote_simulator;

        let root_bank = bank_forks.read().unwrap().root_bank();
        let my_pubkey = leader_schedule_cache
            .slot_leader_at(root_bank.slot(), Some(&root_bank))
            .unwrap();

        // Check that we are the leader of the root bank
        assert!(
            progress
                .get_propagated_stats(root_bank.slot())
                .unwrap()
                .is_leader_slot
        );
        let ancestors = bank_forks.read().unwrap().ancestors();

        // Freeze bank so it shows up in frozen banks
        root_bank.freeze();
        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();

        // Compute bank stats, make sure vote is propagated back to starting root bank
        let vote_tracker = VoteTracker::default();

        // Add votes
        for vote_key in validator_node_to_vote_keys.values() {
            vote_tracker.insert_vote(root_bank.slot(), *vote_key);
        }

        assert!(!progress.is_propagated(root_bank.slot()));

        // Update propagation status
        let tower = Tower::new_for_tests(0, 0.67);
        ReplayStage::compute_bank_stats(
            &validator_node_to_vote_keys[&my_pubkey],
            &ancestors,
            &mut frozen_banks,
            &tower,
            &mut progress,
            &vote_tracker,
            &ClusterSlots::default(),
            &bank_forks,
            &mut HeaviestSubtreeForkChoice::new_from_bank_forks(&bank_forks.read().unwrap()),
            &mut LatestValidatorVotesForFrozenBanks::default(),
        );

        // Check status is true
        assert!(progress.is_propagated(root_bank.slot()));
    }

    #[test]
    fn test_unconfirmed_duplicate_slots_and_lockouts() {
        /*
            Build fork structure:

                 slot 0
                   |
                 slot 1
                 /    \
            slot 2    |
               |      |
            slot 3    |
               |      |
            slot 4    |
                    slot 5
                      |
                    slot 6
        */
        let forks = tr(0) / (tr(1) / (tr(2) / (tr(3) / (tr(4)))) / (tr(5) / (tr(6))));

        // Make enough validators for vote switch threshold later
        let mut vote_simulator = VoteSimulator::new(2);
        let validator_votes: HashMap<Pubkey, Vec<u64>> = vec![
            (vote_simulator.node_pubkeys[0], vec![5]),
            (vote_simulator.node_pubkeys[1], vec![2]),
        ]
        .into_iter()
        .collect();
        vote_simulator.fill_bank_forks(forks, &validator_votes);

        let (bank_forks, mut progress) = (vote_simulator.bank_forks, vote_simulator.progress);
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(
            Blockstore::open(&ledger_path).expect("Expected to be able to open database ledger"),
        );
        let mut tower = Tower::new_for_tests(8, 0.67);

        // All forks have same weight so heaviest bank to vote/reset on should be the tip of
        // the fork with the lower slot
        let (vote_fork, reset_fork) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
        );
        assert_eq!(vote_fork.unwrap(), 4);
        assert_eq!(reset_fork.unwrap(), 4);

        // Record the vote for 4
        tower.record_bank_vote(
            bank_forks.read().unwrap().get(4).unwrap(),
            &Pubkey::default(),
        );

        // Mark 4 as duplicate, 3 should be the heaviest slot, but should not be votable
        // because of lockout
        blockstore.store_duplicate_slot(4, vec![], vec![]).unwrap();
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();
        let bank4_hash = bank_forks.read().unwrap().bank_hash(4).unwrap();
        assert_ne!(bank4_hash, Hash::default());
        let duplicate_state = DuplicateState::new_from_state(
            4,
            &gossip_duplicate_confirmed_slots,
            &mut vote_simulator.heaviest_subtree_fork_choice,
            || progress.is_dead(4).unwrap_or(false),
            || Some(bank4_hash),
        );
        check_slot_agrees_with_cluster(
            4,
            bank_forks.read().unwrap().root(),
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut vote_simulator.heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            SlotStateUpdate::Duplicate(duplicate_state),
        );

        let (vote_fork, reset_fork) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
        );
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork.unwrap(), 3);

        // Now mark 2, an ancestor of 4, as duplicate
        blockstore.store_duplicate_slot(2, vec![], vec![]).unwrap();
        let bank2_hash = bank_forks.read().unwrap().bank_hash(2).unwrap();
        assert_ne!(bank2_hash, Hash::default());
        let duplicate_state = DuplicateState::new_from_state(
            2,
            &gossip_duplicate_confirmed_slots,
            &mut vote_simulator.heaviest_subtree_fork_choice,
            || progress.is_dead(2).unwrap_or(false),
            || Some(bank2_hash),
        );
        check_slot_agrees_with_cluster(
            2,
            bank_forks.read().unwrap().root(),
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut vote_simulator.heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            SlotStateUpdate::Duplicate(duplicate_state),
        );

        let (vote_fork, reset_fork) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
        );

        // Should now pick the next heaviest fork that is not a descendant of 2, which is 6.
        // However the lockout from vote 4 should still apply, so 6 should not be votable
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork.unwrap(), 6);

        // If slot 4 is marked as confirmed, then this confirms slot 2 and 4, and
        // then slot 4 is now the heaviest bank again
        let mut duplicate_slots_to_repair = HashSet::new();
        gossip_duplicate_confirmed_slots.insert(4, bank4_hash);
        let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
            bank4_hash,
            || progress.is_dead(4).unwrap_or(false),
            || Some(bank4_hash),
        );
        check_slot_agrees_with_cluster(
            4,
            bank_forks.read().unwrap().root(),
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut vote_simulator.heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
        );
        // The confirmed hash is detected in `progress`, which means
        // it's confirmation on the replayed block. This means we have
        // the right version of the block, so `duplicate_slots_to_repair`
        // should be empty
        assert!(duplicate_slots_to_repair.is_empty());
        let (vote_fork, reset_fork) = run_compute_and_select_forks(
            &bank_forks,
            &mut progress,
            &mut tower,
            &mut vote_simulator.heaviest_subtree_fork_choice,
            &mut vote_simulator.latest_validator_votes_for_frozen_banks,
        );
        // Should now pick the heaviest fork 4 again, but lockouts apply so fork 4
        // is not votable, which avoids voting for 4 again.
        assert!(vote_fork.is_none());
        assert_eq!(reset_fork.unwrap(), 4);
    }

    #[test]
    fn test_dump_then_repair_correct_slots() {
        // Create the tree of banks in a BankForks object
        let forks = tr(0) / (tr(1)) / (tr(2));

        let ReplayBlockstoreComponents {
            ref mut vote_simulator,
            ref blockstore,
            ..
        } = replay_blockstore_components(Some(forks), 1, None);

        let VoteSimulator {
            ref mut progress,
            ref bank_forks,
            ..
        } = vote_simulator;

        let (mut ancestors, mut descendants) = {
            let r_bank_forks = bank_forks.read().unwrap();
            (r_bank_forks.ancestors(), r_bank_forks.descendants().clone())
        };

        // Insert different versions of both 1 and 2. Both slots 1 and 2 should
        // then be purged
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        duplicate_slots_to_repair.insert((1, Hash::new_unique()));
        duplicate_slots_to_repair.insert((2, Hash::new_unique()));
        ReplayStage::dump_then_repair_correct_slots(
            &mut duplicate_slots_to_repair,
            &mut ancestors,
            &mut descendants,
            progress,
            bank_forks,
            blockstore,
            None,
        );

        let r_bank_forks = bank_forks.read().unwrap();
        for slot in 0..=2 {
            let bank = r_bank_forks.get(slot);
            let ancestor_result = ancestors.get(&slot);
            let descendants_result = descendants.get(&slot);
            if slot == 0 {
                assert!(bank.is_some());
                assert!(ancestor_result.is_some());
                assert!(descendants_result.is_some());
            } else {
                assert!(bank.is_none());
                assert!(ancestor_result.is_none());
                assert!(descendants_result.is_none());
            }
        }
    }

    fn setup_vote_then_rollback(
        first_vote: Slot,
        num_validators: usize,
        generate_votes: Option<GenerateVotes>,
    ) -> ReplayBlockstoreComponents {
        /*
            Build fork structure:

                 slot 0
                   |
                 slot 1
                 /    \
            slot 2    |
               |      |
            slot 3    |
               |      |
            slot 4    |
               |      |
            slot 5    |
                    slot 6
                      |
                    slot 7
        */
        let forks = tr(0) / (tr(1) / (tr(2) / (tr(3) / (tr(4) / (tr(5))))) / (tr(6) / (tr(7))));

        let mut replay_components =
            replay_blockstore_components(Some(forks), num_validators, generate_votes);

        let ReplayBlockstoreComponents {
            ref mut tower,
            ref blockstore,
            ref mut vote_simulator,
            ..
        } = replay_components;

        let VoteSimulator {
            ref mut progress,
            ref bank_forks,
            ref mut heaviest_subtree_fork_choice,
            ..
        } = vote_simulator;

        tower.record_bank_vote(
            bank_forks.read().unwrap().get(first_vote).unwrap(),
            &Pubkey::default(),
        );

        // Simulate another version of slot 2 was duplicate confirmed
        let our_bank2_hash = bank_forks.read().unwrap().bank_hash(2).unwrap();
        let duplicate_confirmed_bank2_hash = Hash::new_unique();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();
        gossip_duplicate_confirmed_slots.insert(2, duplicate_confirmed_bank2_hash);
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();

        // Mark fork choice branch as invalid so select forks below doesn't panic
        // on a nonexistent `heaviest_bank_on_same_fork` after we dump the duplciate fork.
        let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
            duplicate_confirmed_bank2_hash,
            || progress.is_dead(2).unwrap_or(false),
            || Some(our_bank2_hash),
        );
        check_slot_agrees_with_cluster(
            2,
            bank_forks.read().unwrap().root(),
            blockstore,
            &mut duplicate_slots_tracker,
            heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
        );
        assert!(duplicate_slots_to_repair.contains(&(2, duplicate_confirmed_bank2_hash)));
        let mut ancestors = bank_forks.read().unwrap().ancestors();
        let mut descendants = bank_forks.read().unwrap().descendants().clone();
        let old_descendants_of_2 = descendants.get(&2).unwrap().clone();

        ReplayStage::dump_then_repair_correct_slots(
            &mut duplicate_slots_to_repair,
            &mut ancestors,
            &mut descendants,
            progress,
            bank_forks,
            blockstore,
            None,
        );

        // Check everything was purged properly
        for purged_slot in std::iter::once(&2).chain(old_descendants_of_2.iter()) {
            assert!(!ancestors.contains_key(purged_slot));
            assert!(!descendants.contains_key(purged_slot));
        }

        replay_components
    }

    fn run_test_duplicate_rollback_then_vote(first_vote: Slot) -> SelectVoteAndResetForkResult {
        let replay_components = setup_vote_then_rollback(
            first_vote,
            2,
            Some(Box::new(|node_keys| {
                // Simulate everyone else voting on 6, so we have enough to
                // make a switch to the other fork
                node_keys.into_iter().map(|k| (k, vec![6])).collect()
            })),
        );

        let ReplayBlockstoreComponents {
            mut tower,
            vote_simulator,
            ..
        } = replay_components;

        let VoteSimulator {
            mut progress,
            bank_forks,
            mut heaviest_subtree_fork_choice,
            mut latest_validator_votes_for_frozen_banks,
            ..
        } = vote_simulator;

        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();

        let ancestors = bank_forks.read().unwrap().ancestors();
        let descendants = bank_forks.read().unwrap().descendants().clone();

        ReplayStage::compute_bank_stats(
            &Pubkey::new_unique(),
            &ancestors,
            &mut frozen_banks,
            &tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        // Try to switch to vote to the heaviest slot 6, then return the vote results
        let (heaviest_bank, heaviest_bank_on_same_fork) = heaviest_subtree_fork_choice
            .select_forks(&frozen_banks, &tower, &progress, &ancestors, &bank_forks);
        assert_eq!(heaviest_bank.slot(), 7);
        assert!(heaviest_bank_on_same_fork.is_none());
        ReplayStage::select_vote_and_reset_forks(
            &heaviest_bank,
            heaviest_bank_on_same_fork.as_ref(),
            &ancestors,
            &descendants,
            &progress,
            &mut tower,
            &latest_validator_votes_for_frozen_banks,
            &heaviest_subtree_fork_choice,
        )
    }

    #[test]
    fn test_duplicate_rollback_then_vote_locked_out() {
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = run_test_duplicate_rollback_then_vote(5);

        // If we vote on 5 first then try to vote on 7, we should be locked out,
        // despite the rollback
        assert!(vote_bank.is_none());
        assert_eq!(reset_bank.unwrap().slot(), 7);
        assert_eq!(
            heaviest_fork_failures,
            vec![HeaviestForkFailures::LockedOut(7)]
        );
    }

    #[test]
    fn test_duplicate_rollback_then_vote_success() {
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = run_test_duplicate_rollback_then_vote(4);

        // If we vote on 4 first then try to vote on 7, we should succeed
        assert_matches!(
            vote_bank
                .map(|(bank, switch_decision)| (bank.slot(), switch_decision))
                .unwrap(),
            (7, SwitchForkDecision::SwitchProof(_))
        );
        assert_eq!(reset_bank.unwrap().slot(), 7);
        assert!(heaviest_fork_failures.is_empty());
    }

    fn run_test_duplicate_rollback_then_vote_on_other_duplicate(
        first_vote: Slot,
    ) -> SelectVoteAndResetForkResult {
        let replay_components = setup_vote_then_rollback(first_vote, 10, None::<GenerateVotes>);

        let ReplayBlockstoreComponents {
            mut tower,
            mut vote_simulator,
            ..
        } = replay_components;

        // Simulate repairing an alternate version of slot 2, 3 and 4 that we just dumped. Because
        // we're including votes this time for slot 1, it should generate a different
        // version of 2.
        let cluster_votes: HashMap<Pubkey, Vec<Slot>> = vote_simulator
            .node_pubkeys
            .iter()
            .map(|k| (*k, vec![1, 2]))
            .collect();

        // Create new versions of slots 2, 3, 4, 5, with parent slot 1
        vote_simulator.create_and_vote_new_branch(
            1,
            5,
            &cluster_votes,
            &HashSet::new(),
            &Pubkey::new_unique(),
            &mut tower,
        );

        let VoteSimulator {
            mut progress,
            bank_forks,
            mut heaviest_subtree_fork_choice,
            mut latest_validator_votes_for_frozen_banks,
            ..
        } = vote_simulator;

        // Check that the new branch with slot 2 is different than the original version.
        let bank_1_hash = bank_forks.read().unwrap().bank_hash(1).unwrap();
        let children_of_1 = heaviest_subtree_fork_choice
            .children(&(1, bank_1_hash))
            .unwrap();
        let duplicate_versions_of_2 = children_of_1
            .iter()
            .filter(|(slot, _hash)| *slot == 2)
            .count();
        assert_eq!(duplicate_versions_of_2, 2);

        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();

        let ancestors = bank_forks.read().unwrap().ancestors();
        let descendants = bank_forks.read().unwrap().descendants().clone();

        ReplayStage::compute_bank_stats(
            &Pubkey::new_unique(),
            &ancestors,
            &mut frozen_banks,
            &tower,
            &mut progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            &bank_forks,
            &mut heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        // Try to switch to vote to the heaviest slot 5, then return the vote results
        let (heaviest_bank, heaviest_bank_on_same_fork) = heaviest_subtree_fork_choice
            .select_forks(&frozen_banks, &tower, &progress, &ancestors, &bank_forks);
        assert_eq!(heaviest_bank.slot(), 5);
        assert!(heaviest_bank_on_same_fork.is_none());
        ReplayStage::select_vote_and_reset_forks(
            &heaviest_bank,
            heaviest_bank_on_same_fork.as_ref(),
            &ancestors,
            &descendants,
            &progress,
            &mut tower,
            &latest_validator_votes_for_frozen_banks,
            &heaviest_subtree_fork_choice,
        )
    }

    #[test]
    fn test_duplicate_rollback_then_vote_on_other_duplicate_success() {
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = run_test_duplicate_rollback_then_vote_on_other_duplicate(3);

        // If we vote on 2 first then try to vote on 5, we should succeed
        assert_matches!(
            vote_bank
                .map(|(bank, switch_decision)| (bank.slot(), switch_decision))
                .unwrap(),
            (5, SwitchForkDecision::SwitchProof(_))
        );
        assert_eq!(reset_bank.unwrap().slot(), 5);
        assert!(heaviest_fork_failures.is_empty());
    }

    #[test]
    fn test_duplicate_rollback_then_vote_on_other_duplicate_same_slot_locked_out() {
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = run_test_duplicate_rollback_then_vote_on_other_duplicate(5);

        // If we vote on 5 first then try to vote on another version of 5,
        // lockout should fail
        assert!(vote_bank.is_none());
        assert_eq!(reset_bank.unwrap().slot(), 5);
        assert_eq!(
            heaviest_fork_failures,
            vec![HeaviestForkFailures::LockedOut(5)]
        );
    }

    #[test]
    #[ignore]
    fn test_duplicate_rollback_then_vote_on_other_duplicate_different_slot_locked_out() {
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            heaviest_fork_failures,
        } = run_test_duplicate_rollback_then_vote_on_other_duplicate(4);

        // If we vote on 4 first then try to vote on 5 descended from another version
        // of 4, lockout should fail
        assert!(vote_bank.is_none());
        assert_eq!(reset_bank.unwrap().slot(), 5);
        assert_eq!(
            heaviest_fork_failures,
            vec![HeaviestForkFailures::LockedOut(5)]
        );
    }

    #[test]
    fn test_gossip_vote_doesnt_affect_fork_choice() {
        let (
            VoteSimulator {
                bank_forks,
                mut heaviest_subtree_fork_choice,
                mut latest_validator_votes_for_frozen_banks,
                vote_pubkeys,
                ..
            },
            _,
        ) = setup_default_forks(1, None::<GenerateVotes>);

        let vote_pubkey = vote_pubkeys[0];
        let mut unfrozen_gossip_verified_vote_hashes = UnfrozenGossipVerifiedVoteHashes::default();
        let (gossip_verified_vote_hash_sender, gossip_verified_vote_hash_receiver) = unbounded();

        // Best slot is 4
        assert_eq!(heaviest_subtree_fork_choice.best_overall_slot().0, 4);

        // Cast a vote for slot 3 on one fork
        let vote_slot = 3;
        let vote_bank = bank_forks.read().unwrap().get(vote_slot).unwrap().clone();
        gossip_verified_vote_hash_sender
            .send((vote_pubkey, vote_slot, vote_bank.hash()))
            .expect("Send should succeed");
        ReplayStage::process_gossip_verified_vote_hashes(
            &gossip_verified_vote_hash_receiver,
            &mut unfrozen_gossip_verified_vote_hashes,
            &heaviest_subtree_fork_choice,
            &mut latest_validator_votes_for_frozen_banks,
        );

        // Pick the best fork. Gossip votes shouldn't affect fork choice
        heaviest_subtree_fork_choice.compute_bank_stats(
            &vote_bank,
            &Tower::default(),
            &mut latest_validator_votes_for_frozen_banks,
        );

        // Best slot is still 4
        assert_eq!(heaviest_subtree_fork_choice.best_overall_slot().0, 4);
    }

    #[test]
    fn test_replay_stage_refresh_last_vote() {
        let ReplayBlockstoreComponents {
            cluster_info,
            poh_recorder,
            mut tower,
            my_pubkey,
            vote_simulator,
            ..
        } = replay_blockstore_components(None, 10, None::<GenerateVotes>);
        let tower_storage = NullTowerStorage::default();

        let VoteSimulator {
            mut validator_keypairs,
            bank_forks,
            ..
        } = vote_simulator;

        let mut last_vote_refresh_time = LastVoteRefreshTime {
            last_refresh_time: Instant::now(),
            last_print_time: Instant::now(),
        };
        let has_new_vote_been_rooted = false;
        let mut voted_signatures = vec![];

        let identity_keypair = cluster_info.keypair().clone();
        let my_vote_keypair = vec![Arc::new(
            validator_keypairs.remove(&my_pubkey).unwrap().vote_keypair,
        )];
        let my_vote_pubkey = my_vote_keypair[0].pubkey();
        let bank0 = bank_forks.read().unwrap().get(0).unwrap().clone();

        fn fill_bank_with_ticks(bank: &Bank) {
            let parent_distance = bank.slot() - bank.parent_slot();
            for _ in 0..parent_distance {
                let last_blockhash = bank.last_blockhash();
                while bank.last_blockhash() == last_blockhash {
                    bank.register_tick(&Hash::new_unique())
                }
            }
        }
        let (voting_sender, voting_receiver) = channel();

        // Simulate landing a vote for slot 0 landing in slot 1
        let bank1 = Arc::new(Bank::new_from_parent(&bank0, &Pubkey::default(), 1));
        fill_bank_with_ticks(&bank1);
        tower.record_bank_vote(&bank0, &my_vote_pubkey);
        ReplayStage::push_vote(
            &bank0,
            &my_vote_pubkey,
            &identity_keypair,
            &my_vote_keypair,
            &mut tower,
            SavedTower::default(),
            &SwitchForkDecision::SameFork,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &mut ReplayTiming::default(),
            &voting_sender,
        );
        let vote_info = voting_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        crate::voting_service::VotingService::handle_vote(
            &cluster_info,
            &poh_recorder,
            &tower_storage,
            vote_info,
        );

        let mut cursor = Cursor::default();
        let (_, votes) = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes.len(), 1);
        let vote_tx = &votes[0];
        assert_eq!(vote_tx.message.recent_blockhash, bank0.last_blockhash());
        assert_eq!(tower.last_vote_tx_blockhash(), bank0.last_blockhash());
        assert_eq!(tower.last_voted_slot().unwrap(), 0);
        bank1.process_transaction(vote_tx).unwrap();
        bank1.freeze();

        // Trying to refresh the vote for bank 0 in bank 1 or bank 2 won't succeed because
        // the last vote has landed already
        let bank2 = Arc::new(Bank::new_from_parent(&bank1, &Pubkey::default(), 2));
        fill_bank_with_ticks(&bank2);
        bank2.freeze();
        for refresh_bank in &[&bank1, &bank2] {
            ReplayStage::refresh_last_vote(
                &mut tower,
                refresh_bank,
                Tower::last_voted_slot_in_bank(refresh_bank, &my_vote_pubkey).unwrap(),
                &my_vote_pubkey,
                &identity_keypair,
                &my_vote_keypair,
                &mut voted_signatures,
                has_new_vote_been_rooted,
                &mut last_vote_refresh_time,
                &voting_sender,
            );

            // No new votes have been submitted to gossip
            let (_, votes) = cluster_info.get_votes(&mut cursor);
            assert!(votes.is_empty());
            // Tower's latest vote tx blockhash hasn't changed either
            assert_eq!(tower.last_vote_tx_blockhash(), bank0.last_blockhash());
            assert_eq!(tower.last_voted_slot().unwrap(), 0);
        }

        // Simulate submitting a new vote for bank 1 to the network, but the vote
        // not landing
        tower.record_bank_vote(&bank1, &my_vote_pubkey);
        ReplayStage::push_vote(
            &bank1,
            &my_vote_pubkey,
            &identity_keypair,
            &my_vote_keypair,
            &mut tower,
            SavedTower::default(),
            &SwitchForkDecision::SameFork,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &mut ReplayTiming::default(),
            &voting_sender,
        );
        let vote_info = voting_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        crate::voting_service::VotingService::handle_vote(
            &cluster_info,
            &poh_recorder,
            &tower_storage,
            vote_info,
        );
        let (_, votes) = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes.len(), 1);
        let vote_tx = &votes[0];
        assert_eq!(vote_tx.message.recent_blockhash, bank1.last_blockhash());
        assert_eq!(tower.last_vote_tx_blockhash(), bank1.last_blockhash());
        assert_eq!(tower.last_voted_slot().unwrap(), 1);

        // Trying to refresh the vote for bank 1 in bank 2 won't succeed because
        // the last vote has not expired yet
        ReplayStage::refresh_last_vote(
            &mut tower,
            &bank2,
            Tower::last_voted_slot_in_bank(&bank2, &my_vote_pubkey).unwrap(),
            &my_vote_pubkey,
            &identity_keypair,
            &my_vote_keypair,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &mut last_vote_refresh_time,
            &voting_sender,
        );

        // No new votes have been submitted to gossip
        let (_, votes) = cluster_info.get_votes(&mut cursor);
        assert!(votes.is_empty());
        assert_eq!(tower.last_vote_tx_blockhash(), bank1.last_blockhash());
        assert_eq!(tower.last_voted_slot().unwrap(), 1);

        // Create a bank where the last vote transaction will have expired
        let expired_bank = Arc::new(Bank::new_from_parent(
            &bank2,
            &Pubkey::default(),
            bank2.slot() + MAX_PROCESSING_AGE as Slot,
        ));
        fill_bank_with_ticks(&expired_bank);
        expired_bank.freeze();

        // Now trying to refresh the vote for slot 1 will succeed because the recent blockhash
        // of the last vote transaction has expired
        last_vote_refresh_time.last_refresh_time = last_vote_refresh_time
            .last_refresh_time
            .checked_sub(Duration::from_millis(
                MAX_VOTE_REFRESH_INTERVAL_MILLIS as u64 + 1,
            ))
            .unwrap();
        let clone_refresh_time = last_vote_refresh_time.last_refresh_time;
        ReplayStage::refresh_last_vote(
            &mut tower,
            &expired_bank,
            Tower::last_voted_slot_in_bank(&expired_bank, &my_vote_pubkey).unwrap(),
            &my_vote_pubkey,
            &identity_keypair,
            &my_vote_keypair,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &mut last_vote_refresh_time,
            &voting_sender,
        );
        let vote_info = voting_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        crate::voting_service::VotingService::handle_vote(
            &cluster_info,
            &poh_recorder,
            &tower_storage,
            vote_info,
        );

        assert!(last_vote_refresh_time.last_refresh_time > clone_refresh_time);
        let (_, votes) = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes.len(), 1);
        let vote_tx = &votes[0];
        assert_eq!(
            vote_tx.message.recent_blockhash,
            expired_bank.last_blockhash()
        );
        assert_eq!(
            tower.last_vote_tx_blockhash(),
            expired_bank.last_blockhash()
        );
        assert_eq!(tower.last_voted_slot().unwrap(), 1);

        // Processing the vote transaction should be valid
        let expired_bank_child = Arc::new(Bank::new_from_parent(
            &expired_bank,
            &Pubkey::default(),
            expired_bank.slot() + 1,
        ));
        expired_bank_child.process_transaction(vote_tx).unwrap();
        let (_stake, vote_account) = expired_bank_child
            .get_vote_account(&my_vote_pubkey)
            .unwrap();
        assert_eq!(
            vote_account.vote_state().as_ref().unwrap().tower(),
            vec![0, 1]
        );
        fill_bank_with_ticks(&expired_bank_child);
        expired_bank_child.freeze();

        // Trying to refresh the vote on a sibling bank where:
        // 1) The vote for slot 1 hasn't landed
        // 2) The latest refresh vote transaction's recent blockhash (the sibling's hash) doesn't exist
        // This will still not refresh because `MAX_VOTE_REFRESH_INTERVAL_MILLIS` has not expired yet
        let expired_bank_sibling = Arc::new(Bank::new_from_parent(
            &bank2,
            &Pubkey::default(),
            expired_bank_child.slot() + 1,
        ));
        fill_bank_with_ticks(&expired_bank_sibling);
        expired_bank_sibling.freeze();
        // Set the last refresh to now, shouldn't refresh because the last refresh just happened.
        last_vote_refresh_time.last_refresh_time = Instant::now();
        ReplayStage::refresh_last_vote(
            &mut tower,
            &expired_bank_sibling,
            Tower::last_voted_slot_in_bank(&expired_bank_sibling, &my_vote_pubkey).unwrap(),
            &my_vote_pubkey,
            &identity_keypair,
            &my_vote_keypair,
            &mut voted_signatures,
            has_new_vote_been_rooted,
            &mut last_vote_refresh_time,
            &voting_sender,
        );

        let (_, votes) = cluster_info.get_votes(&mut cursor);
        assert!(votes.is_empty());
        assert_eq!(
            vote_tx.message.recent_blockhash,
            expired_bank.last_blockhash()
        );
        assert_eq!(
            tower.last_vote_tx_blockhash(),
            expired_bank.last_blockhash()
        );
        assert_eq!(tower.last_voted_slot().unwrap(), 1);
    }

    fn run_compute_and_select_forks(
        bank_forks: &RwLock<BankForks>,
        progress: &mut ProgressMap,
        tower: &mut Tower,
        heaviest_subtree_fork_choice: &mut HeaviestSubtreeForkChoice,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
    ) -> (Option<Slot>, Option<Slot>) {
        let mut frozen_banks: Vec<_> = bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .values()
            .cloned()
            .collect();
        let ancestors = &bank_forks.read().unwrap().ancestors();
        let descendants = &bank_forks.read().unwrap().descendants().clone();
        ReplayStage::compute_bank_stats(
            &Pubkey::default(),
            &bank_forks.read().unwrap().ancestors(),
            &mut frozen_banks,
            tower,
            progress,
            &VoteTracker::default(),
            &ClusterSlots::default(),
            bank_forks,
            heaviest_subtree_fork_choice,
            latest_validator_votes_for_frozen_banks,
        );
        let (heaviest_bank, heaviest_bank_on_same_fork) = heaviest_subtree_fork_choice
            .select_forks(&frozen_banks, tower, progress, ancestors, bank_forks);
        assert!(heaviest_bank_on_same_fork.is_none());
        let SelectVoteAndResetForkResult {
            vote_bank,
            reset_bank,
            ..
        } = ReplayStage::select_vote_and_reset_forks(
            &heaviest_bank,
            heaviest_bank_on_same_fork.as_ref(),
            ancestors,
            descendants,
            progress,
            tower,
            latest_validator_votes_for_frozen_banks,
            heaviest_subtree_fork_choice,
        );
        (
            vote_bank.map(|(b, _)| b.slot()),
            reset_bank.map(|b| b.slot()),
        )
    }

    type GenerateVotes = Box<dyn Fn(Vec<Pubkey>) -> HashMap<Pubkey, Vec<Slot>>>;

    pub fn setup_forks_from_tree(
        tree: Tree<Slot>,
        num_keys: usize,
        generate_votes: Option<GenerateVotes>,
    ) -> (VoteSimulator, Blockstore) {
        let mut vote_simulator = VoteSimulator::new(num_keys);
        let pubkeys: Vec<Pubkey> = vote_simulator
            .validator_keypairs
            .values()
            .map(|k| k.node_keypair.pubkey())
            .collect();
        let cluster_votes = generate_votes
            .map(|generate_votes| generate_votes(pubkeys))
            .unwrap_or_default();
        vote_simulator.fill_bank_forks(tree.clone(), &cluster_votes);
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&ledger_path).unwrap();
        blockstore.add_tree(tree, false, true, 2, Hash::default());
        (vote_simulator, blockstore)
    }

    fn setup_default_forks(
        num_keys: usize,
        generate_votes: Option<GenerateVotes>,
    ) -> (VoteSimulator, Blockstore) {
        /*
            Build fork structure:

                 slot 0
                   |
                 slot 1
                 /    \
            slot 2    |
               |    slot 3
            slot 4    |
                    slot 5
                      |
                    slot 6
        */

        let tree = tr(0) / (tr(1) / (tr(2) / (tr(4))) / (tr(3) / (tr(5) / (tr(6)))));
        setup_forks_from_tree(tree, num_keys, generate_votes)
    }

    fn check_map_eq<K: Eq + std::hash::Hash + std::fmt::Debug, T: PartialEq + std::fmt::Debug>(
        map1: &HashMap<K, T>,
        map2: &HashMap<K, T>,
    ) -> bool {
        map1.len() == map2.len() && map1.iter().all(|(k, v)| map2.get(k).unwrap() == v)
    }
}
