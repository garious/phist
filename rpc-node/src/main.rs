#![allow(clippy::integer_arithmetic)]
use crossbeam_channel::unbounded;
use {
    clap::{crate_description, crate_name, value_t, values_t, App, AppSettings, Arg},
    log::*,
    solana_clap_utils::keypair::SKIP_SEED_PHRASE_VALIDATION_ARG,
    solana_core::{
        cluster_info::ClusterInfo,
        contact_info::ContactInfo,
        max_slots::MaxSlots,
        optimistically_confirmed_bank_tracker::{
            OptimisticallyConfirmedBank, OptimisticallyConfirmedBankTracker,
        },
        rpc_pubsub_service::PubSubService,
        rpc_service::JsonRpcService,
        rpc_subscriptions::RpcSubscriptions,
        validator::ValidatorConfig,
    },
    solana_download_utils::download_snapshot,
    solana_genesis_utils::download_then_check_genesis_hash,
    solana_ledger::{
        blockstore::Blockstore, blockstore_db::AccessType, blockstore_processor,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_runtime::{
        bank_forks::{BankForks, SnapshotConfig},
        commitment::BlockCommitmentCache,
        hardened_unpack::MAX_GENESIS_ARCHIVE_UNPACKED_SIZE,
        snapshot_utils,
    },
    solana_sdk::hash::Hash,
    std::{
        env,
        io::Read,
        net::TcpListener,
        path::PathBuf,
        process::exit,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, RwLock,
        },
    },
};

pub fn main() {
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .setting(AppSettings::VersionlessSubcommands)
        .setting(AppSettings::InferSubcommands)
        .arg(
            Arg::with_name(SKIP_SEED_PHRASE_VALIDATION_ARG.name)
                .long(SKIP_SEED_PHRASE_VALIDATION_ARG.long)
                .help(SKIP_SEED_PHRASE_VALIDATION_ARG.help),
        )
        .arg(
            Arg::with_name("ledger_path")
                .short("l")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .default_value("ledger")
                .help("Use DIR as ledger location"),
        )
        .arg(
            Arg::with_name("target_validator")
                .long("target-validator")
                .value_name("IP:PORT")
                .takes_value(true)
                .required(true)
                .help("RPC to download from"),
        )
        .arg(
            Arg::with_name("snapshot_hash")
                .long("snapshot-hash")
                .value_name("HASH")
                .takes_value(true)
                .required(true)
                .help("Snapshot hash to download"),
        )
        .arg(
            Arg::with_name("account_paths")
                .long("accounts")
                .value_name("PATHS")
                .takes_value(true)
                .multiple(true)
                .help("Comma separated persistent accounts location"),
        )
        .get_matches();

    let snapshot_hash = value_t!(matches, "snapshot_hash", Hash).unwrap();
    let snapshot_slot = value_t!(matches, "snapshot_slot", u64).unwrap();
    let ledger_path = PathBuf::from(matches.value_of("ledger_path").unwrap());
    let snapshot_output_dir = if matches.is_present("snapshots") {
        PathBuf::from(matches.value_of("snapshots").unwrap())
    } else {
        ledger_path.clone()
    };
    let snapshot_path = snapshot_output_dir.join("snapshot");

    let account_paths: Vec<PathBuf> =
        if let Ok(account_paths) = values_t!(matches, "account_paths", String) {
            account_paths
                .join(",")
                .split(',')
                .map(PathBuf::from)
                .collect()
        } else {
            vec![ledger_path.join("accounts")]
        };

    let rpc_source_addr = solana_net_utils::parse_host_port(
        matches.value_of("rpc_source").unwrap(),
    )
    .unwrap_or_else(|e| {
        eprintln!("failed to parse entrypoint address: {}", e);
        exit(1);
    });

    let rpc_addr = solana_net_utils::parse_host_port(matches.value_of("rpc_port").unwrap())
        .unwrap_or_else(|e| {
            eprintln!("failed to parse entrypoint address: {}", e);
            exit(1);
        });

    let rpc_pubsub_addr = solana_net_utils::parse_host_port(
        matches.value_of("rpc_pubsub").unwrap(),
    )
    .unwrap_or_else(|e| {
        eprintln!("failed to parse entrypoint address: {}", e);
        exit(1);
    });

    let genesis_config = download_then_check_genesis_hash(
        &rpc_source_addr,
        &ledger_path,
        None,
        MAX_GENESIS_ARCHIVE_UNPACKED_SIZE,
        false,
        true,
    )
    .unwrap();

    let mut config = ValidatorConfig::default();
    config.rpc_addrs = Some((rpc_addr, rpc_pubsub_addr));

    let snapshot_config = SnapshotConfig {
        snapshot_interval_slots: std::u64::MAX,
        snapshot_path,
        snapshot_package_output_path: snapshot_output_dir.clone(),
        ..SnapshotConfig::default()
    };

    download_snapshot(
        &rpc_source_addr,
        &snapshot_output_dir,
        (snapshot_slot, snapshot_hash),
        false,
        snapshot_config.maximum_snapshots_to_retain,
    )
    .unwrap();

    let (_highest_snapshot_hash, (snapshot_slot, snapshot_hash, archive_format)) =
        snapshot_utils::get_highest_snapshot_archive_path(snapshot_output_dir.clone()).unwrap();
    let snapshot_slot_hash = (snapshot_slot, snapshot_hash);
    let archive_filename = snapshot_utils::get_snapshot_archive_path(
        snapshot_output_dir,
        &snapshot_slot_hash,
        archive_format,
    );

    let process_options = blockstore_processor::ProcessOptions {
        account_indexes: config.account_indexes.clone(),
        accounts_db_caching_enabled: config.accounts_db_caching_enabled,
        ..blockstore_processor::ProcessOptions::default()
    };

    let bank0 = snapshot_utils::bank_from_archive(
        &account_paths,
        &[],
        &snapshot_config.snapshot_path,
        &archive_filename,
        archive_format,
        &genesis_config,
        process_options.debug_keys.clone(),
        None,
        process_options.account_indexes.clone(),
        process_options.accounts_db_caching_enabled,
    )
    .expect("Load from snapshot failed");

    let bank0_slot = bank0.slot();
    let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank0));

    let bank_forks = Arc::new(RwLock::new(BankForks::new(bank0)));

    let optimistically_confirmed_bank =
        OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);

    let blockstore = Arc::new(
        Blockstore::open_with_access_type(&ledger_path, AccessType::PrimaryOnly, None, false)
            .unwrap(),
    );

    let mut block_commitment_cache = BlockCommitmentCache::default();
    block_commitment_cache.initialize_slots(bank0_slot);
    let block_commitment_cache = Arc::new(RwLock::new(block_commitment_cache));
    let max_complete_transaction_status_slot = Arc::new(AtomicU64::new(0));

    let cluster_info = Arc::new(ClusterInfo::default());

    let max_slots = Arc::new(MaxSlots::default());
    let exit = Arc::new(AtomicBool::new(false));

    let subscriptions = Arc::new(RpcSubscriptions::new(
        &exit,
        bank_forks.clone(),
        block_commitment_cache.clone(),
        optimistically_confirmed_bank.clone(),
    ));

    let rpc_override_health_check = Arc::new(AtomicBool::new(false));
    let (
        json_rpc_service,
        pubsub_service,
        _optimistically_confirmed_bank_tracker,
        _bank_notification_sender,
    ) = if let Some((rpc_addr, rpc_pubsub_addr)) = config.rpc_addrs {
        if ContactInfo::is_valid_address(&rpc_addr) {
            assert!(ContactInfo::is_valid_address(&rpc_pubsub_addr));
        } else {
            assert!(!ContactInfo::is_valid_address(&rpc_pubsub_addr));
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
                None,
                genesis_config.hash(),
                &ledger_path,
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
            Some(PubSubService::new(
                config.pubsub_config.clone(),
                &subscriptions,
                rpc_pubsub_addr,
                &exit,
            )),
            Some(OptimisticallyConfirmedBankTracker::new(
                bank_notification_receiver,
                &exit,
                bank_forks.clone(),
                optimistically_confirmed_bank,
                subscriptions.clone(),
            )),
            Some(bank_notification_sender),
        )
    } else {
        (None, None, None, None)
    };

    let listener = TcpListener::bind("127.0.0.1:80").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => loop {
                let mut buffer = [0; 1024];
                if let Ok(x) = stream.read(&mut buffer) {
                    info!("connection: {:?}", x);
                }
            },
            Err(e) => {
                info!("tcp connection failed {:?}", e);
            }
        }
    }
    info!("Validator exiting..");
    exit.store(true, Ordering::Relaxed);
    json_rpc_service.map(|t| t.join().unwrap());
    pubsub_service.map(|t| t.join().unwrap());
}
