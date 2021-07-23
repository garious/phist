//! The `rpc_service` module implements the Solana JSON RPC service.

use {
    crate::{
        max_slots::MaxSlots,
        optimistically_confirmed_bank_tracker::OptimisticallyConfirmedBank,
        rpc::{
            rpc_accounts::*, rpc_bank::*, rpc_deprecated_v1_7::*, rpc_full::*, rpc_minimal::*,
            rpc_obsolete_v1_7::*, *,
        },
        rpc_health::*,
        send_transaction_service::{LeaderInfo, SendTransactionService},
    },
    jsonrpc_core::{futures::prelude::*, MetaIoHandler},
    jsonrpc_http_server::{
        hyper, AccessControlAllowOrigin, CloseHandle, DomainsValidation, RequestMiddleware,
        RequestMiddlewareAction, ServerBuilder,
    },
    regex::Regex,
    solana_client::rpc_cache::LargestAccountsCache,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        bigtable_upload_service::BigTableUploadService, blockstore::Blockstore,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_metrics::inc_new_counter_info,
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::{
        bank_forks::BankForks, commitment::BlockCommitmentCache, snapshot_config::SnapshotConfig,
        snapshot_utils,
    },
    solana_sdk::{
        exit::Exit, genesis_config::DEFAULT_GENESIS_DOWNLOAD_PATH, hash::Hash,
        native_token::lamports_to_sol, pubkey::Pubkey,
    },
    std::{
        collections::HashSet,
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
        sync::{mpsc::channel, Arc, Mutex, RwLock},
        thread::{self, Builder, JoinHandle},
    },
    tokio_util::codec::{BytesCodec, FramedRead},
};

const LARGEST_ACCOUNTS_CACHE_DURATION: u64 = 60 * 60 * 2;

pub struct JsonRpcService {
    thread_hdl: JoinHandle<()>,

    #[cfg(test)]
    pub request_processor: JsonRpcRequestProcessor, // Used only by test_rpc_new()...

    close_handle: Option<CloseHandle>,
}

struct RpcRequestMiddleware {
    ledger_path: PathBuf,
    snapshot_archive_path_regex: Regex,
    snapshot_config: Option<SnapshotConfig>,
    bank_forks: Arc<RwLock<BankForks>>,
    health: Arc<RpcHealth>,
}

impl RpcRequestMiddleware {
    pub fn new(
        ledger_path: PathBuf,
        snapshot_config: Option<SnapshotConfig>,
        bank_forks: Arc<RwLock<BankForks>>,
        health: Arc<RpcHealth>,
    ) -> Self {
        Self {
            ledger_path,
            snapshot_archive_path_regex: Regex::new(
                r"^/snapshot-\d+-[[:alnum:]]+\.(tar|tar\.bz2|tar\.zst|tar\.gz)$",
            )
            .unwrap(),
            snapshot_config,
            bank_forks,
            health,
        }
    }

    fn redirect(location: &str) -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::SEE_OTHER)
            .header(hyper::header::LOCATION, location)
            .body(hyper::Body::from(String::from(location)))
            .unwrap()
    }

    fn not_found() -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::NOT_FOUND)
            .body(hyper::Body::empty())
            .unwrap()
    }

    #[allow(dead_code)]
    fn internal_server_error() -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
            .body(hyper::Body::empty())
            .unwrap()
    }

    fn is_file_get_path(&self, path: &str) -> bool {
        match path {
            DEFAULT_GENESIS_DOWNLOAD_PATH => true,
            _ => {
                if self.snapshot_config.is_some() {
                    self.snapshot_archive_path_regex.is_match(path)
                } else {
                    false
                }
            }
        }
    }

    #[cfg(unix)]
    async fn open_no_follow(path: impl AsRef<Path>) -> std::io::Result<tokio::fs::File> {
        tokio::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .await
    }

    #[cfg(not(unix))]
    async fn open_no_follow(path: impl AsRef<Path>) -> std::io::Result<tokio::fs::File> {
        // TODO: Is there any way to achieve the same on Windows?
        tokio::fs::File::open(path).await
    }

    fn process_file_get(&self, path: &str) -> RequestMiddlewareAction {
        let stem = path.split_at(1).1; // Drop leading '/' from path
        let filename = {
            match path {
                DEFAULT_GENESIS_DOWNLOAD_PATH => {
                    inc_new_counter_info!("rpc-get_genesis", 1);
                    self.ledger_path.join(stem)
                }
                _ => {
                    inc_new_counter_info!("rpc-get_snapshot", 1);
                    self.snapshot_config
                        .as_ref()
                        .unwrap()
                        .snapshot_package_output_path
                        .join(stem)
                }
            }
        };

        let file_length = std::fs::metadata(&filename)
            .map(|m| m.len())
            .unwrap_or(0)
            .to_string();
        info!("get {} -> {:?} ({} bytes)", path, filename, file_length);
        RequestMiddlewareAction::Respond {
            should_validate_hosts: true,
            response: Box::pin(async {
                match Self::open_no_follow(filename).await {
                    Err(err) => Ok(if err.kind() == std::io::ErrorKind::NotFound {
                        Self::not_found()
                    } else {
                        Self::internal_server_error()
                    }),
                    Ok(file) => {
                        let stream =
                            FramedRead::new(file, BytesCodec::new()).map_ok(|b| b.freeze());
                        let body = hyper::Body::wrap_stream(stream);

                        Ok(hyper::Response::builder()
                            .header(hyper::header::CONTENT_LENGTH, file_length)
                            .body(body)
                            .unwrap())
                    }
                }
            }),
        }
    }

    fn health_check(&self) -> &'static str {
        let response = match self.health.check() {
            RpcHealthStatus::Ok => "ok",
            RpcHealthStatus::Behind { .. } => "behind",
            RpcHealthStatus::Unknown => "unknown",
        };
        info!("health check: {}", response);
        response
    }
}

impl RequestMiddleware for RpcRequestMiddleware {
    fn on_request(&self, request: hyper::Request<hyper::Body>) -> RequestMiddlewareAction {
        trace!("request uri: {}", request.uri());

        if let Some(ref snapshot_config) = self.snapshot_config {
            if request.uri().path() == "/snapshot.tar.bz2" {
                // Convenience redirect to the latest snapshot
                return if let Some(full_snapshot_archive_info) =
                    snapshot_utils::get_highest_full_snapshot_archive_info(
                        &snapshot_config.snapshot_package_output_path,
                    ) {
                    RpcRequestMiddleware::redirect(&format!(
                        "/{}",
                        full_snapshot_archive_info
                            .path()
                            .file_name()
                            .unwrap_or_else(|| std::ffi::OsStr::new(""))
                            .to_str()
                            .unwrap_or("")
                    ))
                } else {
                    RpcRequestMiddleware::not_found()
                }
                .into();
            }
        }

        if let Some(result) = process_rest(&self.bank_forks, request.uri().path()) {
            hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .body(hyper::Body::from(result))
                .unwrap()
                .into()
        } else if self.is_file_get_path(request.uri().path()) {
            self.process_file_get(request.uri().path())
        } else if request.uri().path() == "/health" {
            hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .body(hyper::Body::from(self.health_check()))
                .unwrap()
                .into()
        } else {
            request.into()
        }
    }
}

fn process_rest(bank_forks: &Arc<RwLock<BankForks>>, path: &str) -> Option<String> {
    match path {
        "/v0/circulating-supply" => {
            let r_bank_forks = bank_forks.read().unwrap();
            let bank = r_bank_forks.root_bank();
            let total_supply = bank.capitalization();
            let non_circulating_supply =
                solana_runtime::non_circulating_supply::calculate_non_circulating_supply(&bank)
                    .expect("Scan should not error on root banks")
                    .lamports;
            Some(format!(
                "{}",
                lamports_to_sol(total_supply - non_circulating_supply)
            ))
        }
        "/v0/total-supply" => {
            let r_bank_forks = bank_forks.read().unwrap();
            let bank = r_bank_forks.root_bank();
            let total_supply = bank.capitalization();
            Some(format!("{}", lamports_to_sol(total_supply)))
        }
        _ => None,
    }
}

impl JsonRpcService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc_addr: SocketAddr,
        config: JsonRpcConfig,
        snapshot_config: Option<SnapshotConfig>,
        bank_forks: Arc<RwLock<BankForks>>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        blockstore: Arc<Blockstore>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Option<Arc<Mutex<PohRecorder>>>,
        genesis_hash: Hash,
        ledger_path: &Path,
        validator_exit: Arc<RwLock<Exit>>,
        trusted_validators: Option<HashSet<Pubkey>>,
        override_health_check: Arc<AtomicBool>,
        optimistically_confirmed_bank: Arc<RwLock<OptimisticallyConfirmedBank>>,
        send_transaction_retry_ms: u64,
        send_transaction_leader_forward_count: u64,
        max_slots: Arc<MaxSlots>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        current_transaction_status_slot: Arc<AtomicU64>,
    ) -> Self {
        info!("rpc bound to {:?}", rpc_addr);
        info!("rpc configuration: {:?}", config);
        let rpc_threads = 1.max(config.rpc_threads);

        let health = Arc::new(RpcHealth::new(
            cluster_info.clone(),
            trusted_validators,
            config.health_check_slot_distance,
            override_health_check,
        ));

        let largest_accounts_cache = Arc::new(RwLock::new(LargestAccountsCache::new(
            LARGEST_ACCOUNTS_CACHE_DURATION,
        )));

        let tpu_address = cluster_info.my_contact_info().tpu;

        // sadly, some parts of our current rpc implemention block the jsonrpc's
        // _socket-listening_ event loop for too long, due to (blocking) long IO or intesive CPU,
        // causing no further processing of incoming requests and ultimatily innocent clients timing-out.
        // So create a (shared) multi-threaded event_loop for jsonrpc and set its .threads() to 1,
        // so that we avoid the single-threaded event loops from being created automatically by
        // jsonrpc for threads when .threads(N > 1) is given.
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(rpc_threads)
                .thread_name("sol-rpc-el")
                .enable_all()
                .build()
                .expect("Runtime"),
        );

        let exit_bigtable_ledger_upload_service = Arc::new(AtomicBool::new(false));

        let (bigtable_ledger_storage, _bigtable_ledger_upload_service) =
            if config.enable_bigtable_ledger_storage || config.enable_bigtable_ledger_upload {
                runtime
                    .block_on(solana_storage_bigtable::LedgerStorage::new(
                        !config.enable_bigtable_ledger_upload,
                        config.rpc_bigtable_timeout,
                    ))
                    .map(|bigtable_ledger_storage| {
                        info!("BigTable ledger storage initialized");

                        let bigtable_ledger_upload_service = if config.enable_bigtable_ledger_upload
                        {
                            Some(Arc::new(BigTableUploadService::new(
                                runtime.clone(),
                                bigtable_ledger_storage.clone(),
                                blockstore.clone(),
                                block_commitment_cache.clone(),
                                exit_bigtable_ledger_upload_service.clone(),
                            )))
                        } else {
                            None
                        };

                        (
                            Some(bigtable_ledger_storage),
                            bigtable_ledger_upload_service,
                        )
                    })
                    .unwrap_or_else(|err| {
                        error!("Failed to initialize BigTable ledger storage: {:?}", err);
                        (None, None)
                    })
            } else {
                (None, None)
            };

        let minimal_api = config.minimal_api;
        let obsolete_v1_7_api = config.obsolete_v1_7_api;
        let (request_processor, receiver) = JsonRpcRequestProcessor::new(
            config,
            snapshot_config.clone(),
            bank_forks.clone(),
            block_commitment_cache,
            blockstore,
            validator_exit.clone(),
            health.clone(),
            cluster_info.clone(),
            genesis_hash,
            bigtable_ledger_storage,
            optimistically_confirmed_bank,
            largest_accounts_cache,
            max_slots,
            leader_schedule_cache,
            current_transaction_status_slot,
        );

        let leader_info =
            poh_recorder.map(|recorder| LeaderInfo::new(cluster_info.clone(), recorder));
        let _send_transaction_service = Arc::new(SendTransactionService::new(
            tpu_address,
            &bank_forks,
            leader_info,
            receiver,
            send_transaction_retry_ms,
            send_transaction_leader_forward_count,
        ));

        #[cfg(test)]
        let test_request_processor = request_processor.clone();

        let ledger_path = ledger_path.to_path_buf();

        let (close_handle_sender, close_handle_receiver) = channel();
        let thread_hdl = Builder::new()
            .name("solana-jsonrpc".to_string())
            .spawn(move || {
                let mut io = MetaIoHandler::default();

                io.extend_with(rpc_minimal::MinimalImpl.to_delegate());
                if !minimal_api {
                    io.extend_with(rpc_bank::BankDataImpl.to_delegate());
                    io.extend_with(rpc_accounts::AccountsDataImpl.to_delegate());
                    io.extend_with(rpc_full::FullImpl.to_delegate());
                    io.extend_with(rpc_deprecated_v1_7::DeprecatedV1_7Impl.to_delegate());
                }
                if obsolete_v1_7_api {
                    io.extend_with(rpc_obsolete_v1_7::ObsoleteV1_7Impl.to_delegate());
                }

                let request_middleware = RpcRequestMiddleware::new(
                    ledger_path,
                    snapshot_config,
                    bank_forks.clone(),
                    health.clone(),
                );
                let server = ServerBuilder::with_meta_extractor(
                    io,
                    move |_req: &hyper::Request<hyper::Body>| request_processor.clone(),
                )
                .event_loop_executor(runtime.handle().clone())
                .threads(1)
                .cors(DomainsValidation::AllowOnly(vec![
                    AccessControlAllowOrigin::Any,
                ]))
                .cors_max_age(86400)
                .request_middleware(request_middleware)
                .max_request_body_size(MAX_REQUEST_PAYLOAD_SIZE)
                .start_http(&rpc_addr);

                if let Err(e) = server {
                    warn!(
                        "JSON RPC service unavailable error: {:?}. \n\
                           Also, check that port {} is not already in use by another application",
                        e,
                        rpc_addr.port()
                    );
                    return;
                }

                let server = server.unwrap();
                close_handle_sender.send(server.close_handle()).unwrap();
                server.wait();
                exit_bigtable_ledger_upload_service.store(true, Ordering::Relaxed);
            })
            .unwrap();

        let close_handle = close_handle_receiver.recv().unwrap();
        let close_handle_ = close_handle.clone();
        validator_exit
            .write()
            .unwrap()
            .register_exit(Box::new(move || close_handle_.close()));
        Self {
            thread_hdl,
            #[cfg(test)]
            request_processor: test_request_processor,
            close_handle: Some(close_handle),
        }
    }

    pub fn exit(&mut self) {
        if let Some(c) = self.close_handle.take() {
            c.close()
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::rpc::create_validator_exit,
        solana_gossip::{
            contact_info::ContactInfo,
            crds_value::{CrdsData, CrdsValue, SnapshotHash},
        },
        solana_ledger::{
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
            get_tmp_ledger_path,
        },
        solana_runtime::{
            bank::Bank,
            snapshot_utils::{
                ArchiveFormat, SnapshotVersion, DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            },
        },
        solana_sdk::{
            genesis_config::{ClusterType, DEFAULT_GENESIS_ARCHIVE},
            signature::Signer,
            signer::keypair::Keypair,
        },
        solana_streamer::socket::SocketAddrSpace,
        std::{
            io::Write,
            net::{IpAddr, Ipv4Addr},
        },
        tokio::runtime::Runtime,
    };

    #[test]
    fn test_rpc_new() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let exit = Arc::new(AtomicBool::new(false));
        let validator_exit = create_validator_exit(&exit);
        let bank = Bank::new(&genesis_config);
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::default(),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        let ip_addr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
        let rpc_addr = SocketAddr::new(
            ip_addr,
            solana_net_utils::find_available_port_in_range(ip_addr, (10000, 65535)).unwrap(),
        );
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(Blockstore::open(&ledger_path).unwrap());
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
        let optimistically_confirmed_bank =
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);
        let mut rpc_service = JsonRpcService::new(
            rpc_addr,
            JsonRpcConfig::default(),
            None,
            bank_forks,
            block_commitment_cache,
            blockstore,
            cluster_info,
            None,
            Hash::default(),
            &PathBuf::from("farf"),
            validator_exit,
            None,
            Arc::new(AtomicBool::new(false)),
            optimistically_confirmed_bank,
            1000,
            1,
            Arc::new(MaxSlots::default()),
            Arc::new(LeaderScheduleCache::default()),
            Arc::new(AtomicU64::default()),
        );
        let thread = rpc_service.thread_hdl.thread();
        assert_eq!(thread.name().unwrap(), "solana-jsonrpc");

        assert_eq!(
            10_000,
            rpc_service
                .request_processor
                .get_balance(&mint_keypair.pubkey(), None)
                .value
        );
        rpc_service.exit();
        rpc_service.join().unwrap();
    }

    fn create_bank_forks() -> Arc<RwLock<BankForks>> {
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config(10_000);
        genesis_config.cluster_type = ClusterType::MainnetBeta;
        let bank = Bank::new(&genesis_config);
        Arc::new(RwLock::new(BankForks::new(bank)))
    }

    #[test]
    fn test_process_rest_api() {
        let bank_forks = create_bank_forks();

        assert_eq!(None, process_rest(&bank_forks, "not-a-supported-rest-api"));
        assert_eq!(
            process_rest(&bank_forks, "/v0/circulating-supply"),
            process_rest(&bank_forks, "/v0/total-supply")
        );
    }

    #[test]
    fn test_is_file_get_path() {
        let bank_forks = create_bank_forks();
        let rrm = RpcRequestMiddleware::new(
            PathBuf::from("/"),
            None,
            bank_forks.clone(),
            RpcHealth::stub(),
        );
        let rrm_with_snapshot_config = RpcRequestMiddleware::new(
            PathBuf::from("/"),
            Some(SnapshotConfig {
                snapshot_interval_slots: 0,
                snapshot_package_output_path: PathBuf::from("/"),
                snapshot_path: PathBuf::from("/"),
                archive_format: ArchiveFormat::TarBzip2,
                snapshot_version: SnapshotVersion::default(),
                maximum_snapshots_to_retain: DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            }),
            bank_forks,
            RpcHealth::stub(),
        );

        assert!(rrm.is_file_get_path(DEFAULT_GENESIS_DOWNLOAD_PATH));
        assert!(!rrm.is_file_get_path(DEFAULT_GENESIS_ARCHIVE));

        assert!(!rrm.is_file_get_path("/snapshot.tar.bz2")); // This is a redirect

        assert!(!rrm.is_file_get_path(
            "/snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
        ));
        assert!(rrm_with_snapshot_config.is_file_get_path(
            "/snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
        ));
        assert!(rrm_with_snapshot_config.is_file_get_path(
            "/snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"
        ));
        assert!(rrm_with_snapshot_config
            .is_file_get_path("/snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.gz"));
        assert!(rrm_with_snapshot_config
            .is_file_get_path("/snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar"));

        assert!(!rrm_with_snapshot_config.is_file_get_path(
            "/snapshot-notaslotnumber-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
        ));

        assert!(!rrm_with_snapshot_config.is_file_get_path("../../../test/snapshot-123-xxx.tar"));

        assert!(!rrm.is_file_get_path("/"));
        assert!(!rrm.is_file_get_path(".."));
        assert!(!rrm.is_file_get_path("🎣"));
    }

    #[test]
    fn test_process_file_get() {
        let runtime = Runtime::new().unwrap();

        let ledger_path = get_tmp_ledger_path!();
        std::fs::create_dir(&ledger_path).unwrap();

        let genesis_path = ledger_path.join(DEFAULT_GENESIS_ARCHIVE);
        let rrm = RpcRequestMiddleware::new(
            ledger_path.clone(),
            None,
            create_bank_forks(),
            RpcHealth::stub(),
        );

        // File does not exist => request should fail.
        let action = rrm.process_file_get(DEFAULT_GENESIS_DOWNLOAD_PATH);
        if let RequestMiddlewareAction::Respond { response, .. } = action {
            let response = runtime.block_on(response);
            let response = response.unwrap();
            assert_ne!(response.status(), 200);
        } else {
            panic!("Unexpected RequestMiddlewareAction variant");
        }

        {
            let mut file = std::fs::File::create(&genesis_path).unwrap();
            file.write_all(b"should be ok").unwrap();
        }

        // Normal file exist => request should succeed.
        let action = rrm.process_file_get(DEFAULT_GENESIS_DOWNLOAD_PATH);
        if let RequestMiddlewareAction::Respond { response, .. } = action {
            let response = runtime.block_on(response);
            let response = response.unwrap();
            assert_eq!(response.status(), 200);
        } else {
            panic!("Unexpected RequestMiddlewareAction variant");
        }

        #[cfg(unix)]
        {
            std::fs::remove_file(&genesis_path).unwrap();
            {
                let mut file = std::fs::File::create(ledger_path.join("wrong")).unwrap();
                file.write_all(b"wrong file").unwrap();
            }
            symlink::symlink_file("wrong", &genesis_path).unwrap();

            // File is a symbolic link => request should fail.
            let action = rrm.process_file_get(DEFAULT_GENESIS_DOWNLOAD_PATH);
            if let RequestMiddlewareAction::Respond { response, .. } = action {
                let response = runtime.block_on(response);
                let response = response.unwrap();
                assert_ne!(response.status(), 200);
            } else {
                panic!("Unexpected RequestMiddlewareAction variant");
            }
        }
    }

    #[test]
    fn test_health_check_with_no_trusted_validators() {
        let rm = RpcRequestMiddleware::new(
            PathBuf::from("/"),
            None,
            create_bank_forks(),
            RpcHealth::stub(),
        );
        assert_eq!(rm.health_check(), "ok");
    }

    #[test]
    fn test_health_check_with_trusted_validators() {
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::default(),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        let health_check_slot_distance = 123;
        let override_health_check = Arc::new(AtomicBool::new(false));
        let trusted_validators = vec![
            solana_sdk::pubkey::new_rand(),
            solana_sdk::pubkey::new_rand(),
            solana_sdk::pubkey::new_rand(),
        ];

        let health = Arc::new(RpcHealth::new(
            cluster_info.clone(),
            Some(trusted_validators.clone().into_iter().collect()),
            health_check_slot_distance,
            override_health_check.clone(),
        ));

        let rm = RpcRequestMiddleware::new(PathBuf::from("/"), None, create_bank_forks(), health);

        // No account hashes for this node or any trusted validators
        assert_eq!(rm.health_check(), "unknown");

        // No account hashes for any trusted validators
        cluster_info.push_accounts_hashes(vec![(1000, Hash::default()), (900, Hash::default())]);
        cluster_info.flush_push_queue();
        assert_eq!(rm.health_check(), "unknown");

        // Override health check
        override_health_check.store(true, Ordering::Relaxed);
        assert_eq!(rm.health_check(), "ok");
        override_health_check.store(false, Ordering::Relaxed);

        // This node is ahead of the trusted validators
        cluster_info
            .gossip
            .crds
            .write()
            .unwrap()
            .insert(
                CrdsValue::new_unsigned(CrdsData::AccountsHashes(SnapshotHash::new(
                    trusted_validators[0],
                    vec![
                        (1, Hash::default()),
                        (1001, Hash::default()),
                        (2, Hash::default()),
                    ],
                ))),
                1,
            )
            .unwrap();
        assert_eq!(rm.health_check(), "ok");

        // Node is slightly behind the trusted validators
        cluster_info
            .gossip
            .crds
            .write()
            .unwrap()
            .insert(
                CrdsValue::new_unsigned(CrdsData::AccountsHashes(SnapshotHash::new(
                    trusted_validators[1],
                    vec![(1000 + health_check_slot_distance - 1, Hash::default())],
                ))),
                1,
            )
            .unwrap();
        assert_eq!(rm.health_check(), "ok");

        // Node is far behind the trusted validators
        cluster_info
            .gossip
            .crds
            .write()
            .unwrap()
            .insert(
                CrdsValue::new_unsigned(CrdsData::AccountsHashes(SnapshotHash::new(
                    trusted_validators[2],
                    vec![(1000 + health_check_slot_distance, Hash::default())],
                ))),
                1,
            )
            .unwrap();
        assert_eq!(rm.health_check(), "behind");
    }
}
