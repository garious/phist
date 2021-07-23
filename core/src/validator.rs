//! The `validator` module hosts all the validator microservices.

use crate::{
    broadcast_stage::BroadcastStageType,
    cache_block_meta_service::{CacheBlockMetaSender, CacheBlockMetaService},
    cluster_info_vote_listener::VoteTracker,
    completed_data_sets_service::CompletedDataSetsService,
    consensus::{reconcile_blockstore_roots_with_tower, FileTowerStorage, Tower, TowerStorage},
    cost_model::CostModel,
    rewards_recorder_service::{RewardsRecorderSender, RewardsRecorderService},
    sample_performance_service::SamplePerformanceService,
    serve_repair::ServeRepair,
    serve_repair_service::ServeRepairService,
    sigverify,
    snapshot_packager_service::{PendingSnapshotPackage, SnapshotPackagerService},
    tpu::{Tpu, DEFAULT_TPU_COALESCE_MS},
    tvu::{Sockets, Tvu, TvuConfig},
};
use crossbeam_channel::{bounded, unbounded};
use rand::{thread_rng, Rng};
use solana_entry::poh::compute_hash_time_ns;
use solana_gossip::{
    cluster_info::{
        ClusterInfo, Node, DEFAULT_CONTACT_DEBUG_INTERVAL_MILLIS,
        DEFAULT_CONTACT_SAVE_INTERVAL_MILLIS,
    },
    contact_info::ContactInfo,
    gossip_service::GossipService,
};
use solana_ledger::{
    bank_forks_utils,
    blockstore::{Blockstore, BlockstoreSignals, CompletedSlotsReceiver, PurgeType},
    blockstore_db::BlockstoreRecoveryMode,
    blockstore_processor::{self, TransactionStatusSender},
    leader_schedule::FixedSchedule,
    leader_schedule_cache::LeaderScheduleCache,
};
use solana_measure::measure::Measure;
use solana_metrics::datapoint_info;
use solana_poh::{
    poh_recorder::{PohRecorder, GRACE_TICKS_FACTOR, MAX_GRACE_SLOTS},
    poh_service::{self, PohService},
};
use solana_rpc::{
    max_slots::MaxSlots,
    optimistically_confirmed_bank_tracker::{
        OptimisticallyConfirmedBank, OptimisticallyConfirmedBankTracker,
    },
    rpc::JsonRpcConfig,
    rpc_completed_slots_service::RpcCompletedSlotsService,
    rpc_pubsub_service::{PubSubConfig, PubSubService},
    rpc_service::JsonRpcService,
    rpc_subscriptions::RpcSubscriptions,
    transaction_status_service::TransactionStatusService,
};
use solana_runtime::{
    accounts_db::AccountShrinkThreshold,
    accounts_index::AccountSecondaryIndexes,
    bank::Bank,
    bank_forks::BankForks,
    commitment::BlockCommitmentCache,
    hardened_unpack::{open_genesis_config, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE},
    snapshot_config::SnapshotConfig,
};
use solana_sdk::{
    clock::Slot,
    epoch_schedule::MAX_LEADER_SCHEDULE_EPOCH_OFFSET,
    exit::Exit,
    genesis_config::GenesisConfig,
    hash::Hash,
    pubkey::Pubkey,
    shred_version::compute_shred_version,
    signature::{Keypair, Signer},
    timing::timestamp,
};
use solana_vote_program::vote_state::VoteState;
use std::{
    collections::HashSet,
    net::SocketAddr,
    ops::Deref,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::mpsc::Receiver,
    sync::{Arc, Mutex, RwLock},
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

const MAX_COMPLETED_DATA_SETS_IN_CHANNEL: usize = 100_000;
const WAIT_FOR_SUPERMAJORITY_THRESHOLD_PERCENT: u64 = 90;

pub struct ValidatorConfig {
    pub dev_halt_at_slot: Option<Slot>,
    pub expected_genesis_hash: Option<Hash>,
    pub expected_bank_hash: Option<Hash>,
    pub expected_shred_version: Option<u16>,
    pub voting_disabled: bool,
    pub account_paths: Vec<PathBuf>,
    pub account_shrink_paths: Option<Vec<PathBuf>>,
    pub rpc_config: JsonRpcConfig,
    pub rpc_addrs: Option<(SocketAddr, SocketAddr)>, // (JsonRpc, JsonRpcPubSub)
    pub pubsub_config: PubSubConfig,
    pub snapshot_config: Option<SnapshotConfig>,
    pub max_ledger_shreds: Option<u64>,
    pub broadcast_stage_type: BroadcastStageType,
    pub enable_partition: Option<Arc<AtomicBool>>,
    pub enforce_ulimit_nofile: bool,
    pub fixed_leader_schedule: Option<FixedSchedule>,
    pub wait_for_supermajority: Option<Slot>,
    pub new_hard_forks: Option<Vec<Slot>>,
    pub trusted_validators: Option<HashSet<Pubkey>>, // None = trust all
    pub repair_validators: Option<HashSet<Pubkey>>,  // None = repair from all
    pub gossip_validators: Option<HashSet<Pubkey>>,  // None = gossip with all
    pub halt_on_trusted_validators_accounts_hash_mismatch: bool,
    pub accounts_hash_fault_injection_slots: u64, // 0 = no fault injection
    pub frozen_accounts: Vec<Pubkey>,
    pub no_rocksdb_compaction: bool,
    pub rocksdb_compaction_interval: Option<u64>,
    pub rocksdb_max_compaction_jitter: Option<u64>,
    pub accounts_hash_interval_slots: u64,
    pub max_genesis_archive_unpacked_size: u64,
    pub wal_recovery_mode: Option<BlockstoreRecoveryMode>,
    pub poh_verify: bool, // Perform PoH verification during blockstore processing at boo
    pub cuda: bool,
    pub require_tower: bool,
    pub tower_storage: Arc<dyn TowerStorage>,
    pub debug_keys: Option<Arc<HashSet<Pubkey>>>,
    pub contact_debug_interval: u64,
    pub contact_save_interval: u64,
    pub bpf_jit: bool,
    pub send_transaction_retry_ms: u64,
    pub send_transaction_leader_forward_count: u64,
    pub no_poh_speed_test: bool,
    pub poh_pinned_cpu_core: usize,
    pub poh_hashes_per_batch: u64,
    pub account_indexes: AccountSecondaryIndexes,
    pub accounts_db_caching_enabled: bool,
    pub warp_slot: Option<Slot>,
    pub accounts_db_test_hash_calculation: bool,
    pub accounts_db_use_index_hash_calculation: bool,
    pub tpu_coalesce_ms: u64,
    pub validator_exit: Arc<RwLock<Exit>>,
    pub no_wait_for_vote_to_start_leader: bool,
    pub accounts_shrink_ratio: AccountShrinkThreshold,
}

impl Default for ValidatorConfig {
    fn default() -> Self {
        Self {
            dev_halt_at_slot: None,
            expected_genesis_hash: None,
            expected_bank_hash: None,
            expected_shred_version: None,
            voting_disabled: false,
            max_ledger_shreds: None,
            account_paths: Vec::new(),
            account_shrink_paths: None,
            rpc_config: JsonRpcConfig::default(),
            rpc_addrs: None,
            pubsub_config: PubSubConfig::default(),
            snapshot_config: None,
            broadcast_stage_type: BroadcastStageType::Standard,
            enable_partition: None,
            enforce_ulimit_nofile: true,
            fixed_leader_schedule: None,
            wait_for_supermajority: None,
            new_hard_forks: None,
            trusted_validators: None,
            repair_validators: None,
            gossip_validators: None,
            halt_on_trusted_validators_accounts_hash_mismatch: false,
            accounts_hash_fault_injection_slots: 0,
            frozen_accounts: vec![],
            no_rocksdb_compaction: false,
            rocksdb_compaction_interval: None,
            rocksdb_max_compaction_jitter: None,
            accounts_hash_interval_slots: std::u64::MAX,
            max_genesis_archive_unpacked_size: MAX_GENESIS_ARCHIVE_UNPACKED_SIZE,
            wal_recovery_mode: None,
            poh_verify: true,
            cuda: false,
            require_tower: false,
            tower_storage: Arc::new(FileTowerStorage::new(PathBuf::default())),
            debug_keys: None,
            contact_debug_interval: DEFAULT_CONTACT_DEBUG_INTERVAL_MILLIS,
            contact_save_interval: DEFAULT_CONTACT_SAVE_INTERVAL_MILLIS,
            bpf_jit: false,
            send_transaction_retry_ms: 2000,
            send_transaction_leader_forward_count: 2,
            no_poh_speed_test: true,
            poh_pinned_cpu_core: poh_service::DEFAULT_PINNED_CPU_CORE,
            poh_hashes_per_batch: poh_service::DEFAULT_HASHES_PER_BATCH,
            account_indexes: AccountSecondaryIndexes::default(),
            accounts_db_caching_enabled: false,
            warp_slot: None,
            accounts_db_test_hash_calculation: false,
            accounts_db_use_index_hash_calculation: true,
            tpu_coalesce_ms: DEFAULT_TPU_COALESCE_MS,
            validator_exit: Arc::new(RwLock::new(Exit::default())),
            no_wait_for_vote_to_start_leader: true,
            accounts_shrink_ratio: AccountShrinkThreshold::default(),
        }
    }
}

// `ValidatorStartProgress` contains status information that is surfaced to the node operator over
// the admin RPC channel to help them to follow the general progress of node startup without
// having to watch log messages.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ValidatorStartProgress {
    Initializing, // Catch all, default state
    SearchingForRpcService,
    DownloadingSnapshot { slot: Slot, rpc_addr: SocketAddr },
    CleaningBlockStore,
    CleaningAccounts,
    LoadingLedger,
    StartingServices,
    Halted, // Validator halted due to `--dev-halt-at-slot` argument
    WaitingForSupermajority,

    // `Running` is the terminal state once the validator fully starts and all services are
    // operational
    Running,
}

impl Default for ValidatorStartProgress {
    fn default() -> Self {
        Self::Initializing
    }
}

#[derive(Default)]
struct TransactionHistoryServices {
    transaction_status_sender: Option<TransactionStatusSender>,
    transaction_status_service: Option<TransactionStatusService>,
    max_complete_transaction_status_slot: Arc<AtomicU64>,
    rewards_recorder_sender: Option<RewardsRecorderSender>,
    rewards_recorder_service: Option<RewardsRecorderService>,
    cache_block_meta_sender: Option<CacheBlockMetaSender>,
    cache_block_meta_service: Option<CacheBlockMetaService>,
}

pub struct Validator {
    validator_exit: Arc<RwLock<Exit>>,
    json_rpc_service: Option<JsonRpcService>,
    pubsub_service: Option<PubSubService>,
    rpc_completed_slots_service: JoinHandle<()>,
    optimistically_confirmed_bank_tracker: Option<OptimisticallyConfirmedBankTracker>,
    transaction_status_service: Option<TransactionStatusService>,
    rewards_recorder_service: Option<RewardsRecorderService>,
    cache_block_meta_service: Option<CacheBlockMetaService>,
    sample_performance_service: Option<SamplePerformanceService>,
    gossip_service: GossipService,
    serve_repair_service: ServeRepairService,
    completed_data_sets_service: CompletedDataSetsService,
    snapshot_packager_service: Option<SnapshotPackagerService>,
    poh_recorder: Arc<Mutex<PohRecorder>>,
    poh_service: PohService,
    tpu: Tpu,
    tvu: Tvu,
    ip_echo_server: Option<solana_net_utils::IpEchoServer>,
    pub cluster_info: Arc<ClusterInfo>,
}

// in the distant future, get rid of ::new()/exit() and use Result properly...
pub(crate) fn abort() -> ! {
    #[cfg(not(test))]
    {
        // standard error is usually redirected to a log file, cry for help on standard output as
        // well
        println!("Validator process aborted. The validator log may contain further details");
        std::process::exit(1);
    }

    #[cfg(test)]
    panic!("process::exit(1) is intercepted for friendly test failure...");
}

impl Validator {
    pub fn new(
        mut node: Node,
        identity_keypair: Arc<Keypair>,
        ledger_path: &Path,
        vote_account: &Pubkey,
        authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
        cluster_entrypoints: Vec<ContactInfo>,
        config: &ValidatorConfig,
        should_check_duplicate_instance: bool,
        start_progress: Arc<RwLock<ValidatorStartProgress>>,
    ) -> Self {
        let id = identity_keypair.pubkey();
        assert_eq!(id, node.info.id);

        warn!("identity: {}", id);
        warn!("vote account: {}", vote_account);

        if config.voting_disabled {
            warn!("voting disabled");
            authorized_voter_keypairs.write().unwrap().clear();
        } else {
            for authorized_voter_keypair in authorized_voter_keypairs.read().unwrap().iter() {
                warn!("authorized voter: {}", authorized_voter_keypair.pubkey());
            }
        }

        report_target_features();

        for cluster_entrypoint in &cluster_entrypoints {
            info!("entrypoint: {:?}", cluster_entrypoint);
        }

        if solana_perf::perf_libs::api().is_some() {
            info!("Initializing sigverify, this could take a while...");
        } else {
            info!("Initializing sigverify...");
        }
        sigverify::init();
        info!("Done.");

        if !ledger_path.is_dir() {
            error!(
                "ledger directory does not exist or is not accessible: {:?}",
                ledger_path
            );
            abort();
        }

        if let Some(shred_version) = config.expected_shred_version {
            if let Some(wait_for_supermajority_slot) = config.wait_for_supermajority {
                *start_progress.write().unwrap() = ValidatorStartProgress::CleaningBlockStore;
                backup_and_clear_blockstore(
                    ledger_path,
                    wait_for_supermajority_slot + 1,
                    shred_version,
                );
            }
        }

        info!("Cleaning accounts paths..");
        *start_progress.write().unwrap() = ValidatorStartProgress::CleaningAccounts;
        let mut start = Measure::start("clean_accounts_paths");
        for accounts_path in &config.account_paths {
            cleanup_accounts_path(accounts_path);
        }
        if let Some(ref shrink_paths) = config.account_shrink_paths {
            for accounts_path in shrink_paths {
                cleanup_accounts_path(accounts_path);
            }
        }
        start.stop();
        info!("done. {}", start);

        let exit = Arc::new(AtomicBool::new(false));
        {
            let exit = exit.clone();
            config
                .validator_exit
                .write()
                .unwrap()
                .register_exit(Box::new(move || exit.store(true, Ordering::Relaxed)));
        }

        let (replay_vote_sender, replay_vote_receiver) = unbounded();
        let (
            genesis_config,
            bank_forks,
            blockstore,
            ledger_signal_receiver,
            completed_slots_receiver,
            leader_schedule_cache,
            snapshot_hash,
            TransactionHistoryServices {
                transaction_status_sender,
                transaction_status_service,
                max_complete_transaction_status_slot,
                rewards_recorder_sender,
                rewards_recorder_service,
                cache_block_meta_sender,
                cache_block_meta_service,
            },
            tower,
        ) = new_banks_from_ledger(
            &id,
            vote_account,
            config,
            ledger_path,
            config.poh_verify,
            &exit,
            config.enforce_ulimit_nofile,
            &start_progress,
            config.no_poh_speed_test,
        );

        *start_progress.write().unwrap() = ValidatorStartProgress::StartingServices;

        let leader_schedule_cache = Arc::new(leader_schedule_cache);
        let bank = bank_forks.working_bank();
        if let Some(ref shrink_paths) = config.account_shrink_paths {
            bank.set_shrink_paths(shrink_paths.clone());
        }
        let bank_forks = Arc::new(RwLock::new(bank_forks));

        let sample_performance_service =
            if config.rpc_addrs.is_some() && config.rpc_config.enable_rpc_transaction_history {
                Some(SamplePerformanceService::new(
                    &bank_forks,
                    &blockstore,
                    &exit,
                ))
            } else {
                None
            };

        info!("Starting validator with working bank slot {}", bank.slot());
        {
            let hard_forks: Vec<_> = bank.hard_forks().read().unwrap().iter().copied().collect();
            if !hard_forks.is_empty() {
                info!("Hard forks: {:?}", hard_forks);
            }
        }

        node.info.wallclock = timestamp();
        node.info.shred_version = compute_shred_version(
            &genesis_config.hash(),
            Some(&bank.hard_forks().read().unwrap()),
        );

        Self::print_node_info(&node);

        if let Some(expected_shred_version) = config.expected_shred_version {
            if expected_shred_version != node.info.shred_version {
                error!(
                    "shred version mismatch: expected {} found: {}",
                    expected_shred_version, node.info.shred_version,
                );
                abort();
            }
        }

        let mut cluster_info = ClusterInfo::new(node.info.clone(), identity_keypair);
        cluster_info.set_contact_debug_interval(config.contact_debug_interval);
        cluster_info.set_entrypoints(cluster_entrypoints);
        cluster_info.restore_contact_info(ledger_path, config.contact_save_interval);
        let cluster_info = Arc::new(cluster_info);
        let mut block_commitment_cache = BlockCommitmentCache::default();
        block_commitment_cache.initialize_slots(bank.slot());
        let block_commitment_cache = Arc::new(RwLock::new(block_commitment_cache));

        let optimistically_confirmed_bank =
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);

        let rpc_subscriptions = Arc::new(RpcSubscriptions::new_with_vote_subscription(
            &exit,
            bank_forks.clone(),
            block_commitment_cache.clone(),
            optimistically_confirmed_bank.clone(),
            config.pubsub_config.enable_vote_subscription,
        ));

        let max_slots = Arc::new(MaxSlots::default());
        let (completed_data_sets_sender, completed_data_sets_receiver) =
            bounded(MAX_COMPLETED_DATA_SETS_IN_CHANNEL);
        let completed_data_sets_service = CompletedDataSetsService::new(
            completed_data_sets_receiver,
            blockstore.clone(),
            rpc_subscriptions.clone(),
            &exit,
            max_slots.clone(),
        );

        info!(
            "Starting PoH: epoch={} slot={} tick_height={} blockhash={} leader={:?}",
            bank.epoch(),
            bank.slot(),
            bank.tick_height(),
            bank.last_blockhash(),
            leader_schedule_cache.slot_leader_at(bank.slot(), Some(&bank))
        );

        let poh_config = Arc::new(genesis_config.poh_config.clone());
        let (mut poh_recorder, entry_receiver, record_receiver) =
            PohRecorder::new_with_clear_signal(
                bank.tick_height(),
                bank.last_blockhash(),
                bank.slot(),
                leader_schedule_cache.next_leader_slot(
                    &id,
                    bank.slot(),
                    &bank,
                    Some(&blockstore),
                    GRACE_TICKS_FACTOR * MAX_GRACE_SLOTS,
                ),
                bank.ticks_per_slot(),
                &id,
                &blockstore,
                blockstore.new_shreds_signals.first().cloned(),
                &leader_schedule_cache,
                &poh_config,
                exit.clone(),
            );
        if config.snapshot_config.is_some() {
            poh_recorder.set_bank(&bank);
        }
        let poh_recorder = Arc::new(Mutex::new(poh_recorder));

        let rpc_override_health_check = Arc::new(AtomicBool::new(false));
        let (
            json_rpc_service,
            pubsub_service,
            optimistically_confirmed_bank_tracker,
            bank_notification_sender,
        ) = if let Some((rpc_addr, rpc_pubsub_addr)) = config.rpc_addrs {
            if ContactInfo::is_valid_address(&node.info.rpc) {
                assert!(ContactInfo::is_valid_address(&node.info.rpc_pubsub));
            } else {
                assert!(!ContactInfo::is_valid_address(&node.info.rpc_pubsub));
            }
            let (bank_notification_sender, bank_notification_receiver) = unbounded();
            (
                Some(JsonRpcService::new(
                    rpc_addr,
                    config.rpc_config.clone(),
                    config.snapshot_config.clone(),
                    bank_forks.clone(),
                    block_commitment_cache.clone(),
                    blockstore.clone(),
                    cluster_info.clone(),
                    Some(poh_recorder.clone()),
                    genesis_config.hash(),
                    ledger_path,
                    config.validator_exit.clone(),
                    config.trusted_validators.clone(),
                    rpc_override_health_check.clone(),
                    optimistically_confirmed_bank.clone(),
                    config.send_transaction_retry_ms,
                    config.send_transaction_leader_forward_count,
                    max_slots.clone(),
                    leader_schedule_cache.clone(),
                    max_complete_transaction_status_slot,
                )),
                if config.rpc_config.minimal_api {
                    None
                } else {
                    Some(PubSubService::new(
                        config.pubsub_config.clone(),
                        &rpc_subscriptions,
                        rpc_pubsub_addr,
                        &exit,
                    ))
                },
                Some(OptimisticallyConfirmedBankTracker::new(
                    bank_notification_receiver,
                    &exit,
                    bank_forks.clone(),
                    optimistically_confirmed_bank,
                    rpc_subscriptions.clone(),
                )),
                Some(bank_notification_sender),
            )
        } else {
            (None, None, None, None)
        };

        if config.dev_halt_at_slot.is_some() {
            // Simulate a confirmed root to avoid RPC errors with CommitmentConfig::finalized() and
            // to ensure RPC endpoints like getConfirmedBlock, which require a confirmed root, work
            block_commitment_cache
                .write()
                .unwrap()
                .set_highest_confirmed_root(bank_forks.read().unwrap().root());

            // Park with the RPC service running, ready for inspection!
            warn!("Validator halted");
            *start_progress.write().unwrap() = ValidatorStartProgress::Halted;
            std::thread::park();
        }
        let ip_echo_server = match node.sockets.ip_echo {
            None => None,
            Some(tcp_listener) => Some(solana_net_utils::ip_echo_server(
                tcp_listener,
                Some(node.info.shred_version),
            )),
        };
        let gossip_service = GossipService::new(
            &cluster_info,
            Some(bank_forks.clone()),
            node.sockets.gossip,
            config.gossip_validators.clone(),
            should_check_duplicate_instance,
            &exit,
        );
        let serve_repair = Arc::new(RwLock::new(ServeRepair::new(cluster_info.clone())));
        let serve_repair_service = ServeRepairService::new(
            &serve_repair,
            Some(blockstore.clone()),
            node.sockets.serve_repair,
            &exit,
        );

        let (snapshot_packager_service, snapshot_config_and_pending_package) =
            if let Some(snapshot_config) = config.snapshot_config.clone() {
                if is_snapshot_config_invalid(
                    snapshot_config.snapshot_interval_slots,
                    config.accounts_hash_interval_slots,
                ) {
                    error!("Snapshot config is invalid");
                }

                // Start a snapshot packaging service
                let pending_snapshot_package = PendingSnapshotPackage::default();

                let snapshot_packager_service = SnapshotPackagerService::new(
                    pending_snapshot_package.clone(),
                    snapshot_hash,
                    &exit,
                    &cluster_info,
                    snapshot_config.maximum_snapshots_to_retain,
                );
                (
                    Some(snapshot_packager_service),
                    Some((snapshot_config, pending_snapshot_package)),
                )
            } else {
                (None, None)
            };

        let waited_for_supermajority = if let Ok(waited) = wait_for_supermajority(
            config,
            &bank,
            &cluster_info,
            rpc_override_health_check,
            &start_progress,
        ) {
            waited
        } else {
            abort();
        };

        let wait_for_vote_to_start_leader =
            !waited_for_supermajority && !config.no_wait_for_vote_to_start_leader;

        let poh_service = PohService::new(
            poh_recorder.clone(),
            &poh_config,
            &exit,
            bank.ticks_per_slot(),
            config.poh_pinned_cpu_core,
            config.poh_hashes_per_batch,
            record_receiver,
        );
        assert_eq!(
            blockstore.new_shreds_signals.len(),
            1,
            "New shred signal for the TVU should be the same as the clear bank signal."
        );

        let vote_tracker = Arc::new(VoteTracker::new(
            bank_forks.read().unwrap().root_bank().deref(),
        ));

        let mut cost_model = CostModel::default();
        cost_model.initialize_cost_table(&blockstore.read_program_costs().unwrap());
        let cost_model = Arc::new(RwLock::new(cost_model));

        let (retransmit_slots_sender, retransmit_slots_receiver) = unbounded();
        let (verified_vote_sender, verified_vote_receiver) = unbounded();
        let (gossip_verified_vote_hash_sender, gossip_verified_vote_hash_receiver) = unbounded();
        let (cluster_confirmed_slot_sender, cluster_confirmed_slot_receiver) = unbounded();

        let rpc_completed_slots_service =
            RpcCompletedSlotsService::spawn(completed_slots_receiver, rpc_subscriptions.clone());

        let tvu = Tvu::new(
            vote_account,
            authorized_voter_keypairs,
            &bank_forks,
            &cluster_info,
            Sockets {
                repair: node
                    .sockets
                    .repair
                    .try_clone()
                    .expect("Failed to clone repair socket"),
                retransmit: node
                    .sockets
                    .retransmit_sockets
                    .iter()
                    .map(|s| s.try_clone().expect("Failed to clone retransmit socket"))
                    .collect(),
                fetch: node
                    .sockets
                    .tvu
                    .iter()
                    .map(|s| s.try_clone().expect("Failed to clone TVU Sockets"))
                    .collect(),
                forwards: node
                    .sockets
                    .tvu_forwards
                    .iter()
                    .map(|s| s.try_clone().expect("Failed to clone TVU forwards Sockets"))
                    .collect(),
            },
            blockstore.clone(),
            ledger_signal_receiver,
            &rpc_subscriptions,
            &poh_recorder,
            tower,
            config.tower_storage.clone(),
            &leader_schedule_cache,
            &exit,
            block_commitment_cache,
            config.enable_partition.clone(),
            transaction_status_sender.clone(),
            rewards_recorder_sender,
            cache_block_meta_sender,
            snapshot_config_and_pending_package,
            vote_tracker.clone(),
            retransmit_slots_sender,
            gossip_verified_vote_hash_receiver,
            verified_vote_receiver,
            replay_vote_sender.clone(),
            completed_data_sets_sender,
            bank_notification_sender.clone(),
            cluster_confirmed_slot_receiver,
            TvuConfig {
                max_ledger_shreds: config.max_ledger_shreds,
                halt_on_trusted_validators_accounts_hash_mismatch: config
                    .halt_on_trusted_validators_accounts_hash_mismatch,
                shred_version: node.info.shred_version,
                trusted_validators: config.trusted_validators.clone(),
                repair_validators: config.repair_validators.clone(),
                accounts_hash_fault_injection_slots: config.accounts_hash_fault_injection_slots,
                accounts_db_caching_enabled: config.accounts_db_caching_enabled,
                test_hash_calculation: config.accounts_db_test_hash_calculation,
                use_index_hash_calculation: config.accounts_db_use_index_hash_calculation,
                rocksdb_compaction_interval: config.rocksdb_compaction_interval,
                rocksdb_max_compaction_jitter: config.rocksdb_compaction_interval,
                wait_for_vote_to_start_leader,
                accounts_shrink_ratio: config.accounts_shrink_ratio,
            },
            &max_slots,
            &cost_model,
        );

        let tpu = Tpu::new(
            &cluster_info,
            &poh_recorder,
            entry_receiver,
            retransmit_slots_receiver,
            node.sockets.tpu,
            node.sockets.tpu_forwards,
            node.sockets.broadcast,
            &rpc_subscriptions,
            transaction_status_sender,
            &blockstore,
            &config.broadcast_stage_type,
            &exit,
            node.info.shred_version,
            vote_tracker,
            bank_forks,
            verified_vote_sender,
            gossip_verified_vote_hash_sender,
            replay_vote_receiver,
            replay_vote_sender,
            bank_notification_sender,
            config.tpu_coalesce_ms,
            cluster_confirmed_slot_sender,
            &cost_model,
        );

        datapoint_info!("validator-new", ("id", id.to_string(), String));
        *start_progress.write().unwrap() = ValidatorStartProgress::Running;
        Self {
            gossip_service,
            serve_repair_service,
            json_rpc_service,
            pubsub_service,
            rpc_completed_slots_service,
            optimistically_confirmed_bank_tracker,
            transaction_status_service,
            rewards_recorder_service,
            cache_block_meta_service,
            sample_performance_service,
            snapshot_packager_service,
            completed_data_sets_service,
            tpu,
            tvu,
            poh_service,
            poh_recorder,
            ip_echo_server,
            validator_exit: config.validator_exit.clone(),
            cluster_info,
        }
    }

    // Used for notifying many nodes in parallel to exit
    pub fn exit(&mut self) {
        self.validator_exit.write().unwrap().exit();
    }

    pub fn close(mut self) {
        self.exit();
        self.join();
    }

    fn print_node_info(node: &Node) {
        info!("{:?}", node.info);
        info!(
            "local gossip address: {}",
            node.sockets.gossip.local_addr().unwrap()
        );
        info!(
            "local broadcast address: {}",
            node.sockets
                .broadcast
                .first()
                .unwrap()
                .local_addr()
                .unwrap()
        );
        info!(
            "local repair address: {}",
            node.sockets.repair.local_addr().unwrap()
        );
        info!(
            "local retransmit address: {}",
            node.sockets.retransmit_sockets[0].local_addr().unwrap()
        );
    }

    pub fn join(self) {
        drop(self.cluster_info);

        self.poh_service.join().expect("poh_service");
        drop(self.poh_recorder);

        if let Some(json_rpc_service) = self.json_rpc_service {
            json_rpc_service.join().expect("rpc_service");
        }

        if let Some(pubsub_service) = self.pubsub_service {
            pubsub_service.join().expect("pubsub_service");
        }

        self.rpc_completed_slots_service
            .join()
            .expect("rpc_completed_slots_service");

        if let Some(optimistically_confirmed_bank_tracker) =
            self.optimistically_confirmed_bank_tracker
        {
            optimistically_confirmed_bank_tracker
                .join()
                .expect("optimistically_confirmed_bank_tracker");
        }

        if let Some(transaction_status_service) = self.transaction_status_service {
            transaction_status_service
                .join()
                .expect("transaction_status_service");
        }

        if let Some(rewards_recorder_service) = self.rewards_recorder_service {
            rewards_recorder_service
                .join()
                .expect("rewards_recorder_service");
        }

        if let Some(cache_block_meta_service) = self.cache_block_meta_service {
            cache_block_meta_service
                .join()
                .expect("cache_block_meta_service");
        }

        if let Some(sample_performance_service) = self.sample_performance_service {
            sample_performance_service
                .join()
                .expect("sample_performance_service");
        }

        if let Some(s) = self.snapshot_packager_service {
            s.join().expect("snapshot_packager_service");
        }

        self.gossip_service.join().expect("gossip_service");
        self.serve_repair_service
            .join()
            .expect("serve_repair_service");
        self.tpu.join().expect("tpu");
        self.tvu.join().expect("tvu");
        self.completed_data_sets_service
            .join()
            .expect("completed_data_sets_service");
        if let Some(ip_echo_server) = self.ip_echo_server {
            ip_echo_server.shutdown_background();
        }
    }
}

fn active_vote_account_exists_in_bank(bank: &Arc<Bank>, vote_account: &Pubkey) -> bool {
    if let Some(account) = &bank.get_account(vote_account) {
        if let Some(vote_state) = VoteState::from(account) {
            return !vote_state.votes.is_empty();
        }
    }
    false
}

fn check_poh_speed(genesis_config: &GenesisConfig, maybe_hash_samples: Option<u64>) {
    if let Some(hashes_per_tick) = genesis_config.hashes_per_tick() {
        let ticks_per_slot = genesis_config.ticks_per_slot();
        let hashes_per_slot = hashes_per_tick * ticks_per_slot;

        let hash_samples = maybe_hash_samples.unwrap_or(hashes_per_slot);
        let hash_time_ns = compute_hash_time_ns(hash_samples);

        let my_ns_per_slot = (hash_time_ns * hashes_per_slot) / hash_samples;
        debug!("computed: ns_per_slot: {}", my_ns_per_slot);
        let target_ns_per_slot = genesis_config.ns_per_slot() as u64;
        debug!(
            "cluster ns_per_hash: {}ns ns_per_slot: {}",
            target_ns_per_slot / hashes_per_slot,
            target_ns_per_slot
        );
        if my_ns_per_slot < target_ns_per_slot {
            let extra_ns = target_ns_per_slot - my_ns_per_slot;
            info!("PoH speed check: Will sleep {}ns per slot.", extra_ns);
        } else {
            error!(
                "PoH is slower than cluster target tick rate! mine: {} cluster: {}. If you wish to continue, try --no-poh-speed-test",
                my_ns_per_slot, target_ns_per_slot,
            );
            abort();
        }
    }
}

fn post_process_restored_tower(
    restored_tower: crate::consensus::Result<Tower>,
    validator_identity: &Pubkey,
    vote_account: &Pubkey,
    config: &ValidatorConfig,
    bank_forks: &BankForks,
) -> Tower {
    let mut should_require_tower = config.require_tower;

    restored_tower
        .and_then(|tower| {
            let root_bank = bank_forks.root_bank();
            let slot_history = root_bank.get_slot_history();
            let tower = tower.adjust_lockouts_after_replay(root_bank.slot(), &slot_history);

            if let Some(wait_slot_for_supermajority) = config.wait_for_supermajority {
                if root_bank.slot() == wait_slot_for_supermajority {
                    // intentionally fail to restore tower; we're supposedly in a new hard fork; past
                    // out-of-chain vote state doesn't make sense at all
                    // what if --wait-for-supermajority again if the validator restarted?
                    let message = format!("Hardfork is detected; discarding tower restoration result: {:?}", tower);
                    datapoint_error!(
                        "tower_error",
                        (
                            "error",
                            message,
                            String
                        ),
                    );
                    error!("{}", message);

                    // unconditionally relax tower requirement so that we can always restore tower
                    // from root bank.
                    should_require_tower = false;
                    return Err(crate::consensus::TowerError::HardFork(wait_slot_for_supermajority));
                }
            }

            if let Some(warp_slot) = config.warp_slot {
                // unconditionally relax tower requirement so that we can always restore tower
                // from root bank after the warp
                should_require_tower = false;
                return Err(crate::consensus::TowerError::HardFork(warp_slot));
            }

            tower
        })
        .unwrap_or_else(|err| {
            let voting_has_been_active =
                active_vote_account_exists_in_bank(&bank_forks.working_bank(), vote_account);
            if !err.is_file_missing() {
                datapoint_error!(
                    "tower_error",
                    (
                        "error",
                        format!("Unable to restore tower: {}", err),
                        String
                    ),
                );
            }
            if should_require_tower && voting_has_been_active {
                error!("Requested mandatory tower restore failed: {}", err);
                error!(
                    "And there is an existing vote_account containing actual votes. \
                     Aborting due to possible conflicting duplicate votes",
                );
                abort();
            }
            if err.is_file_missing() && !voting_has_been_active {
                // Currently, don't protect against spoofed snapshots with no tower at all
                info!(
                    "Ignoring expected failed tower restore because this is the initial \
                      validator start with the vote account..."
                );
            } else {
                error!(
                    "Rebuilding a new tower from the latest vote account due to failed tower restore: {}",
                    err
                );
            }

            Tower::new_from_bankforks(
                bank_forks,
                validator_identity,
                vote_account,
            )
        })
}

#[allow(clippy::type_complexity)]
fn new_banks_from_ledger(
    validator_identity: &Pubkey,
    vote_account: &Pubkey,
    config: &ValidatorConfig,
    ledger_path: &Path,
    poh_verify: bool,
    exit: &Arc<AtomicBool>,
    enforce_ulimit_nofile: bool,
    start_progress: &Arc<RwLock<ValidatorStartProgress>>,
    no_poh_speed_test: bool,
) -> (
    GenesisConfig,
    BankForks,
    Arc<Blockstore>,
    Receiver<bool>,
    CompletedSlotsReceiver,
    LeaderScheduleCache,
    Option<(Slot, Hash)>,
    TransactionHistoryServices,
    Tower,
) {
    info!("loading ledger from {:?}...", ledger_path);
    *start_progress.write().unwrap() = ValidatorStartProgress::LoadingLedger;
    let genesis_config = open_genesis_config(ledger_path, config.max_genesis_archive_unpacked_size);

    // This needs to be limited otherwise the state in the VoteAccount data
    // grows too large
    let leader_schedule_slot_offset = genesis_config.epoch_schedule.leader_schedule_slot_offset;
    let slots_per_epoch = genesis_config.epoch_schedule.slots_per_epoch;
    let leader_epoch_offset = (leader_schedule_slot_offset + slots_per_epoch - 1) / slots_per_epoch;
    assert!(leader_epoch_offset <= MAX_LEADER_SCHEDULE_EPOCH_OFFSET);

    let genesis_hash = genesis_config.hash();
    info!("genesis hash: {}", genesis_hash);

    if let Some(expected_genesis_hash) = config.expected_genesis_hash {
        if genesis_hash != expected_genesis_hash {
            error!("genesis hash mismatch: expected {}", expected_genesis_hash);
            error!("Delete the ledger directory to continue: {:?}", ledger_path);
            abort();
        }
    }

    if !no_poh_speed_test {
        check_poh_speed(&genesis_config, None);
    }

    let BlockstoreSignals {
        mut blockstore,
        ledger_signal_receiver,
        completed_slots_receiver,
        ..
    } = Blockstore::open_with_signal(
        ledger_path,
        config.wal_recovery_mode.clone(),
        enforce_ulimit_nofile,
    )
    .expect("Failed to open ledger database");
    blockstore.set_no_compaction(config.no_rocksdb_compaction);

    let restored_tower = Tower::restore(config.tower_storage.as_ref(), validator_identity);
    if let Ok(tower) = &restored_tower {
        reconcile_blockstore_roots_with_tower(tower, &blockstore).unwrap_or_else(|err| {
            error!("Failed to reconcile blockstore with tower: {:?}", err);
            abort()
        });
    }

    let blockstore = Arc::new(blockstore);
    let blockstore_root_scan = if config.rpc_addrs.is_some()
        && config.rpc_config.enable_rpc_transaction_history
        && config.rpc_config.rpc_scan_and_fix_roots
    {
        let blockstore = blockstore.clone();
        let exit = exit.clone();
        Some(
            Builder::new()
                .name("blockstore-root-scan".to_string())
                .spawn(move || blockstore.scan_and_fix_roots(&exit))
                .unwrap(),
        )
    } else {
        None
    };

    let process_options = blockstore_processor::ProcessOptions {
        bpf_jit: config.bpf_jit,
        poh_verify,
        dev_halt_at_slot: config.dev_halt_at_slot,
        new_hard_forks: config.new_hard_forks.clone(),
        frozen_accounts: config.frozen_accounts.clone(),
        debug_keys: config.debug_keys.clone(),
        account_indexes: config.account_indexes.clone(),
        accounts_db_caching_enabled: config.accounts_db_caching_enabled,
        shrink_ratio: config.accounts_shrink_ratio,
        ..blockstore_processor::ProcessOptions::default()
    };

    let transaction_history_services =
        if config.rpc_addrs.is_some() && config.rpc_config.enable_rpc_transaction_history {
            initialize_rpc_transaction_history_services(
                blockstore.clone(),
                exit,
                config.rpc_config.enable_cpi_and_log_storage,
            )
        } else {
            TransactionHistoryServices::default()
        };

    let (mut bank_forks, mut leader_schedule_cache, snapshot_hash) = bank_forks_utils::load(
        &genesis_config,
        &blockstore,
        config.account_paths.clone(),
        config.account_shrink_paths.clone(),
        config.snapshot_config.as_ref(),
        process_options,
        transaction_history_services
            .transaction_status_sender
            .as_ref(),
        transaction_history_services
            .cache_block_meta_sender
            .as_ref(),
    )
    .unwrap_or_else(|err| {
        error!("Failed to load ledger: {:?}", err);
        abort()
    });

    if let Some(warp_slot) = config.warp_slot {
        let snapshot_config = config.snapshot_config.as_ref().unwrap_or_else(|| {
            error!("warp slot requires a snapshot config");
            abort();
        });

        let working_bank = bank_forks.working_bank();

        if warp_slot <= working_bank.slot() {
            error!(
                "warp slot ({}) cannot be less than the working bank slot ({})",
                warp_slot,
                working_bank.slot()
            );
            abort();
        }
        info!("warping to slot {}", warp_slot);

        bank_forks.insert(Bank::warp_from_parent(
            &bank_forks.root_bank(),
            &Pubkey::default(),
            warp_slot,
        ));
        bank_forks.set_root(
            warp_slot,
            &solana_runtime::accounts_background_service::AbsRequestSender::default(),
            Some(warp_slot),
        );
        leader_schedule_cache.set_root(&bank_forks.root_bank());

        let archive_file = solana_runtime::snapshot_utils::bank_to_full_snapshot_archive(
            ledger_path,
            &bank_forks.root_bank(),
            None,
            &snapshot_config.snapshot_package_output_path,
            snapshot_config.archive_format,
            Some(bank_forks.root_bank().get_thread_pool()),
            snapshot_config.maximum_snapshots_to_retain,
        )
        .unwrap_or_else(|err| {
            error!("Unable to create snapshot: {}", err);
            abort();
        });
        info!("created snapshot: {}", archive_file.display());
    }

    let tower = post_process_restored_tower(
        restored_tower,
        validator_identity,
        vote_account,
        config,
        &bank_forks,
    );

    info!("Tower state: {:?}", tower);

    leader_schedule_cache.set_fixed_leader_schedule(config.fixed_leader_schedule.clone());

    bank_forks.set_snapshot_config(config.snapshot_config.clone());
    bank_forks.set_accounts_hash_interval_slots(config.accounts_hash_interval_slots);

    if let Some(blockstore_root_scan) = blockstore_root_scan {
        if let Err(err) = blockstore_root_scan.join() {
            warn!("blockstore_root_scan failed to join {:?}", err);
        }
    }

    (
        genesis_config,
        bank_forks,
        blockstore,
        ledger_signal_receiver,
        completed_slots_receiver,
        leader_schedule_cache,
        snapshot_hash,
        transaction_history_services,
        tower,
    )
}

fn blockstore_contains_bad_shred_version(
    blockstore: &Blockstore,
    start_slot: Slot,
    shred_version: u16,
) -> bool {
    let now = Instant::now();
    // Search for shreds with incompatible version in blockstore
    if let Ok(slot_meta_iterator) = blockstore.slot_meta_iterator(start_slot) {
        info!("Searching for incorrect shreds..");
        for (slot, _meta) in slot_meta_iterator {
            if let Ok(shreds) = blockstore.get_data_shreds_for_slot(slot, 0) {
                for shred in &shreds {
                    if shred.version() != shred_version {
                        return true;
                    }
                }
            }
            if now.elapsed().as_secs() > 60 {
                info!("Didn't find incorrect shreds after 60 seconds, aborting");
                return false;
            }
        }
    }
    false
}

fn backup_and_clear_blockstore(ledger_path: &Path, start_slot: Slot, shred_version: u16) {
    let blockstore = Blockstore::open(ledger_path).unwrap();
    let do_copy_and_clear =
        blockstore_contains_bad_shred_version(&blockstore, start_slot, shred_version);

    // If found, then copy shreds to another db and clear from start_slot
    if do_copy_and_clear {
        let folder_name = format!("backup_rocksdb_{}", thread_rng().gen_range(0, 99999));
        let backup_blockstore = Blockstore::open(&ledger_path.join(folder_name));
        let mut last_print = Instant::now();
        let mut copied = 0;
        let mut last_slot = None;
        let slot_meta_iterator = blockstore.slot_meta_iterator(start_slot).unwrap();
        for (slot, _meta) in slot_meta_iterator {
            if let Ok(shreds) = blockstore.get_data_shreds_for_slot(slot, 0) {
                if let Ok(ref backup_blockstore) = backup_blockstore {
                    copied += shreds.len();
                    let _ = backup_blockstore.insert_shreds(shreds, None, true);
                }
            }
            if last_print.elapsed().as_millis() > 3000 {
                info!(
                    "Copying shreds from slot {} copied {} so far.",
                    start_slot, copied
                );
                last_print = Instant::now();
            }
            last_slot = Some(slot);
        }

        let end_slot = last_slot.unwrap();
        info!("Purging slots {} to {}", start_slot, end_slot);
        blockstore.purge_from_next_slots(start_slot, end_slot);
        blockstore.purge_slots(start_slot, end_slot, PurgeType::Exact);
        info!("Purging done, compacting db..");
        if let Err(e) = blockstore.compact_storage(start_slot, end_slot) {
            warn!(
                "Error from compacting storage from {} to {}: {:?}",
                start_slot, end_slot, e
            );
        }
        info!("done");
    }
    drop(blockstore);
}

fn initialize_rpc_transaction_history_services(
    blockstore: Arc<Blockstore>,
    exit: &Arc<AtomicBool>,
    enable_cpi_and_log_storage: bool,
) -> TransactionHistoryServices {
    let max_complete_transaction_status_slot = Arc::new(AtomicU64::new(blockstore.max_root()));
    let (transaction_status_sender, transaction_status_receiver) = unbounded();
    let transaction_status_sender = Some(TransactionStatusSender {
        sender: transaction_status_sender,
        enable_cpi_and_log_storage,
    });
    let transaction_status_service = Some(TransactionStatusService::new(
        transaction_status_receiver,
        max_complete_transaction_status_slot.clone(),
        blockstore.clone(),
        exit,
    ));

    let (rewards_recorder_sender, rewards_receiver) = unbounded();
    let rewards_recorder_sender = Some(rewards_recorder_sender);
    let rewards_recorder_service = Some(RewardsRecorderService::new(
        rewards_receiver,
        blockstore.clone(),
        exit,
    ));

    let (cache_block_meta_sender, cache_block_meta_receiver) = unbounded();
    let cache_block_meta_sender = Some(cache_block_meta_sender);
    let cache_block_meta_service = Some(CacheBlockMetaService::new(
        cache_block_meta_receiver,
        blockstore,
        exit,
    ));
    TransactionHistoryServices {
        transaction_status_sender,
        transaction_status_service,
        max_complete_transaction_status_slot,
        rewards_recorder_sender,
        rewards_recorder_service,
        cache_block_meta_sender,
        cache_block_meta_service,
    }
}

#[derive(Debug, PartialEq)]
enum ValidatorError {
    BadExpectedBankHash,
    NotEnoughLedgerData,
}

// Return if the validator waited on other nodes to start. In this case
// it should not wait for one of it's votes to land to produce blocks
// because if the whole network is waiting, then it will stall.
//
// Error indicates that a bad hash was encountered or another condition
// that is unrecoverable and the validator should exit.
fn wait_for_supermajority(
    config: &ValidatorConfig,
    bank: &Bank,
    cluster_info: &ClusterInfo,
    rpc_override_health_check: Arc<AtomicBool>,
    start_progress: &Arc<RwLock<ValidatorStartProgress>>,
) -> Result<bool, ValidatorError> {
    if let Some(wait_for_supermajority) = config.wait_for_supermajority {
        match wait_for_supermajority.cmp(&bank.slot()) {
            std::cmp::Ordering::Less => return Ok(false),
            std::cmp::Ordering::Greater => {
                error!(
                    "Ledger does not have enough data to wait for supermajority, \
                    please enable snapshot fetch. Has {} needs {}",
                    bank.slot(),
                    wait_for_supermajority
                );
                return Err(ValidatorError::NotEnoughLedgerData);
            }
            _ => {}
        }
    } else {
        return Ok(false);
    }

    if let Some(expected_bank_hash) = config.expected_bank_hash {
        if bank.hash() != expected_bank_hash {
            error!(
                "Bank hash({}) does not match expected value: {}",
                bank.hash(),
                expected_bank_hash
            );
            return Err(ValidatorError::BadExpectedBankHash);
        }
    }

    *start_progress.write().unwrap() = ValidatorStartProgress::WaitingForSupermajority;
    for i in 1.. {
        if i % 10 == 1 {
            info!(
                "Waiting for {}% of activated stake at slot {} to be in gossip...",
                WAIT_FOR_SUPERMAJORITY_THRESHOLD_PERCENT,
                bank.slot()
            );
        }

        let gossip_stake_percent = get_stake_percent_in_gossip(bank, cluster_info, i % 10 == 0);

        if gossip_stake_percent >= WAIT_FOR_SUPERMAJORITY_THRESHOLD_PERCENT {
            break;
        }
        // The normal RPC health checks don't apply as the node is waiting, so feign health to
        // prevent load balancers from removing the node from their list of candidates during a
        // manual restart.
        rpc_override_health_check.store(true, Ordering::Relaxed);
        sleep(Duration::new(1, 0));
    }
    rpc_override_health_check.store(false, Ordering::Relaxed);
    Ok(true)
}

fn report_target_features() {
    warn!(
        "CUDA is {}abled",
        if solana_perf::perf_libs::api().is_some() {
            "en"
        } else {
            "dis"
        }
    );

    // We exclude Mac OS here to be compatible with computers that have Mac M1 chips.
    // For these computers, one must install rust/cargo/brew etc. using Rosetta 2,
    // which allows them to run software targeted for x86_64 on an aarch64.
    // Hence the code below will run on these machines (target_arch="x86_64")
    // if we don't exclude with target_os="macos".
    //
    // It's going to require more more work to get Solana building
    // on Mac M1's without Rosetta,
    // and when that happens we should remove this
    // (the feature flag for code targeting that is target_arch="aarch64")
    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        not(target_os = "macos")
    ))]
    {
        unsafe { check_avx() };
    }
}

// Validator binaries built on a machine with AVX support will generate invalid opcodes
// when run on machines without AVX causing a non-obvious process abort.  Instead detect
// the mismatch and error cleanly.
#[target_feature(enable = "avx")]
unsafe fn check_avx() {
    if is_x86_feature_detected!("avx") {
        info!("AVX detected");
    } else {
        error!(
            "Your machine does not have AVX support, please rebuild from source on your machine"
        );
        abort();
    }
}

// Get the activated stake percentage (based on the provided bank) that is visible in gossip
fn get_stake_percent_in_gossip(bank: &Bank, cluster_info: &ClusterInfo, log: bool) -> u64 {
    let mut online_stake = 0;
    let mut wrong_shred_stake = 0;
    let mut wrong_shred_nodes = vec![];
    let mut offline_stake = 0;
    let mut offline_nodes = vec![];

    let mut total_activated_stake = 0;
    let all_tvu_peers = cluster_info.all_tvu_peers();
    let my_shred_version = cluster_info.my_shred_version();
    let my_id = cluster_info.id();

    for (_, (activated_stake, vote_account)) in bank.vote_accounts() {
        total_activated_stake += activated_stake;

        if activated_stake == 0 {
            continue;
        }
        let vote_state_node_pubkey = vote_account
            .vote_state()
            .as_ref()
            .map(|vote_state| vote_state.node_pubkey)
            .unwrap_or_default();

        if let Some(peer) = all_tvu_peers
            .iter()
            .find(|peer| peer.id == vote_state_node_pubkey)
        {
            if peer.shred_version == my_shred_version {
                trace!(
                    "observed {} in gossip, (activated_stake={})",
                    vote_state_node_pubkey,
                    activated_stake
                );
                online_stake += activated_stake;
            } else {
                wrong_shred_stake += activated_stake;
                wrong_shred_nodes.push((activated_stake, vote_state_node_pubkey));
            }
        } else if vote_state_node_pubkey == my_id {
            online_stake += activated_stake; // This node is online
        } else {
            offline_stake += activated_stake;
            offline_nodes.push((activated_stake, vote_state_node_pubkey));
        }
    }

    if log {
        info!(
            "{}% of active stake visible in gossip",
            online_stake * 100 / total_activated_stake
        );

        if !wrong_shred_nodes.is_empty() {
            info!(
                "{}% of active stake has the wrong shred version in gossip",
                wrong_shred_stake * 100 / total_activated_stake,
            );
            for (stake, identity) in wrong_shred_nodes {
                info!(
                    "    {}% - {}",
                    stake * 100 / total_activated_stake,
                    identity
                );
            }
        }

        if !offline_nodes.is_empty() {
            info!(
                "{}% of active stake is not visible in gossip",
                offline_stake * 100 / total_activated_stake
            );
            for (stake, identity) in offline_nodes {
                info!(
                    "    {}% - {}",
                    stake * 100 / total_activated_stake,
                    identity
                );
            }
        }
    }

    online_stake * 100 / total_activated_stake
}

// Cleanup anything that looks like an accounts append-vec
fn cleanup_accounts_path(account_path: &std::path::Path) {
    if let Err(e) = std::fs::remove_dir_all(account_path) {
        warn!(
            "encountered error removing accounts path: {:?}: {}",
            account_path, e
        );
    }
}

pub fn is_snapshot_config_invalid(
    snapshot_interval_slots: u64,
    accounts_hash_interval_slots: u64,
) -> bool {
    snapshot_interval_slots != 0
        && (snapshot_interval_slots < accounts_hash_interval_slots
            || snapshot_interval_slots % accounts_hash_interval_slots != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_ledger::{create_new_tmp_ledger, genesis_utils::create_genesis_config_with_leader};
    use solana_sdk::genesis_config::create_genesis_config;
    use solana_sdk::poh_config::PohConfig;
    use std::fs::remove_dir_all;

    #[test]
    fn validator_exit() {
        solana_logger::setup();
        let leader_keypair = Keypair::new();
        let leader_node = Node::new_localhost_with_pubkey(&leader_keypair.pubkey());

        let validator_keypair = Keypair::new();
        let validator_node = Node::new_localhost_with_pubkey(&validator_keypair.pubkey());
        let genesis_config =
            create_genesis_config_with_leader(10_000, &leader_keypair.pubkey(), 1000)
                .genesis_config;
        let (validator_ledger_path, _blockhash) = create_new_tmp_ledger!(&genesis_config);

        let voting_keypair = Arc::new(Keypair::new());
        let config = ValidatorConfig {
            rpc_addrs: Some((validator_node.info.rpc, validator_node.info.rpc_pubsub)),
            ..ValidatorConfig::default()
        };
        let start_progress = Arc::new(RwLock::new(ValidatorStartProgress::default()));
        let validator = Validator::new(
            validator_node,
            Arc::new(validator_keypair),
            &validator_ledger_path,
            &voting_keypair.pubkey(),
            Arc::new(RwLock::new(vec![voting_keypair.clone()])),
            vec![leader_node.info],
            &config,
            true, // should_check_duplicate_instance
            start_progress.clone(),
        );
        assert_eq!(
            *start_progress.read().unwrap(),
            ValidatorStartProgress::Running
        );
        validator.close();
        remove_dir_all(validator_ledger_path).unwrap();
    }

    #[test]
    fn test_backup_and_clear_blockstore() {
        use std::time::Instant;
        solana_logger::setup();
        use solana_entry::entry;
        use solana_ledger::{blockstore, get_tmp_ledger_path};
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let entries = entry::create_ticks(1, 0, Hash::default());

            info!("creating shreds");
            let mut last_print = Instant::now();
            for i in 1..10 {
                let shreds = blockstore::entries_to_test_shreds(entries.clone(), i, i - 1, true, 1);
                blockstore.insert_shreds(shreds, None, true).unwrap();
                if last_print.elapsed().as_millis() > 5000 {
                    info!("inserted {}", i);
                    last_print = Instant::now();
                }
            }
            drop(blockstore);

            // this purges and compacts all slots greater than or equal to 5
            backup_and_clear_blockstore(&blockstore_path, 5, 2);

            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            // assert that slots less than 5 aren't affected
            assert!(blockstore.meta(4).unwrap().unwrap().next_slots.is_empty());
            for i in 5..10 {
                assert!(blockstore
                    .get_data_shreds_for_slot(i, 0)
                    .unwrap()
                    .is_empty());
            }
        }
    }

    #[test]
    fn validator_parallel_exit() {
        let leader_keypair = Keypair::new();
        let leader_node = Node::new_localhost_with_pubkey(&leader_keypair.pubkey());

        let mut ledger_paths = vec![];
        let mut validators: Vec<Validator> = (0..2)
            .map(|_| {
                let validator_keypair = Keypair::new();
                let validator_node = Node::new_localhost_with_pubkey(&validator_keypair.pubkey());
                let genesis_config =
                    create_genesis_config_with_leader(10_000, &leader_keypair.pubkey(), 1000)
                        .genesis_config;
                let (validator_ledger_path, _blockhash) = create_new_tmp_ledger!(&genesis_config);
                ledger_paths.push(validator_ledger_path.clone());
                let vote_account_keypair = Keypair::new();
                let config = ValidatorConfig {
                    rpc_addrs: Some((validator_node.info.rpc, validator_node.info.rpc_pubsub)),
                    ..ValidatorConfig::default()
                };
                Validator::new(
                    validator_node,
                    Arc::new(validator_keypair),
                    &validator_ledger_path,
                    &vote_account_keypair.pubkey(),
                    Arc::new(RwLock::new(vec![Arc::new(vote_account_keypair)])),
                    vec![leader_node.info.clone()],
                    &config,
                    true, // should_check_duplicate_instance
                    Arc::new(RwLock::new(ValidatorStartProgress::default())),
                )
            })
            .collect();

        // Each validator can exit in parallel to speed many sequential calls to join`
        validators.iter_mut().for_each(|v| v.exit());
        // While join is called sequentially, the above exit call notified all the
        // validators to exit from all their threads
        validators.into_iter().for_each(|validator| {
            validator.join();
        });

        for path in ledger_paths {
            remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn test_wait_for_supermajority() {
        solana_logger::setup();
        use solana_sdk::hash::hash;
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
        );

        let (genesis_config, _mint_keypair) = create_genesis_config(1);
        let bank = Arc::new(Bank::new(&genesis_config));
        let mut config = ValidatorConfig::default();
        let rpc_override_health_check = Arc::new(AtomicBool::new(false));
        let start_progress = Arc::new(RwLock::new(ValidatorStartProgress::default()));

        assert!(!wait_for_supermajority(
            &config,
            &bank,
            &cluster_info,
            rpc_override_health_check.clone(),
            &start_progress,
        )
        .unwrap());

        // bank=0, wait=1, should fail
        config.wait_for_supermajority = Some(1);
        assert_eq!(
            wait_for_supermajority(
                &config,
                &bank,
                &cluster_info,
                rpc_override_health_check.clone(),
                &start_progress,
            ),
            Err(ValidatorError::NotEnoughLedgerData)
        );

        // bank=1, wait=0, should pass, bank is past the wait slot
        let bank = Bank::new_from_parent(&bank, &Pubkey::default(), 1);
        config.wait_for_supermajority = Some(0);
        assert!(!wait_for_supermajority(
            &config,
            &bank,
            &cluster_info,
            rpc_override_health_check.clone(),
            &start_progress,
        )
        .unwrap());

        // bank=1, wait=1, equal, but bad hash provided
        config.wait_for_supermajority = Some(1);
        config.expected_bank_hash = Some(hash(&[1]));
        assert_eq!(
            wait_for_supermajority(
                &config,
                &bank,
                &cluster_info,
                rpc_override_health_check,
                &start_progress,
            ),
            Err(ValidatorError::BadExpectedBankHash)
        );
    }

    #[test]
    fn test_interval_check() {
        assert!(!is_snapshot_config_invalid(0, 100));
        assert!(is_snapshot_config_invalid(1, 100));
        assert!(is_snapshot_config_invalid(230, 100));
        assert!(!is_snapshot_config_invalid(500, 100));
        assert!(!is_snapshot_config_invalid(5, 5));
    }

    #[test]
    #[should_panic]
    fn test_poh_speed() {
        solana_logger::setup();
        let poh_config = PohConfig {
            target_tick_duration: Duration::from_millis(solana_sdk::clock::MS_PER_TICK),
            // make PoH rate really fast to cause the panic condition
            hashes_per_tick: Some(100 * solana_sdk::clock::DEFAULT_HASHES_PER_TICK),
            ..PohConfig::default()
        };
        let genesis_config = GenesisConfig {
            poh_config,
            ..GenesisConfig::default()
        };
        check_poh_speed(&genesis_config, Some(10_000));
    }

    #[test]
    fn test_poh_speed_no_hashes_per_tick() {
        let poh_config = PohConfig {
            target_tick_duration: Duration::from_millis(solana_sdk::clock::MS_PER_TICK),
            hashes_per_tick: None,
            ..PohConfig::default()
        };
        let genesis_config = GenesisConfig {
            poh_config,
            ..GenesisConfig::default()
        };
        check_poh_speed(&genesis_config, Some(10_000));
    }
}
