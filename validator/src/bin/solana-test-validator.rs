use {
    clap::{value_t, value_t_or_exit, App, Arg},
    solana_clap_utils::{
        input_parsers::{pubkey_of, pubkeys_of, value_of},
        input_validators::{
            is_pubkey, is_pubkey_or_keypair, is_slot, is_url_or_moniker,
            normalize_to_url_if_moniker,
        },
    },
    solana_client::rpc_client::RpcClient,
    solana_core::consensus::FileTowerStorage,
    solana_faucet::faucet::{run_local_faucet_with_port, FAUCET_PORT},
    solana_rpc::rpc::JsonRpcConfig,
    solana_sdk::{
        account::AccountSharedData,
        clock::Slot,
        epoch_schedule::{EpochSchedule, MINIMUM_SLOTS_PER_EPOCH},
        native_token::sol_to_lamports,
        pubkey::Pubkey,
        rent::Rent,
        rpc_port,
        signature::{read_keypair_file, write_keypair_file, Keypair, Signer},
        system_program,
    },
    solana_validator::{
        admin_rpc_service, dashboard::Dashboard, println_name_value, redirect_stderr_to_file,
        test_validator::*,
    },
    std::{
        collections::HashSet,
        fs, io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::{Path, PathBuf},
        process::exit,
        sync::{mpsc::channel, Arc, RwLock},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

/* 10,000 was derived empirically by watching the size
 * of the rocksdb/ directory self-limit itself to the
 * 40MB-150MB range when running `solana-test-validator`
 */
const DEFAULT_MAX_LEDGER_SHREDS: u64 = 10_000;

const DEFAULT_FAUCET_SOL: f64 = 1_000_000.;

#[derive(PartialEq)]
enum Output {
    None,
    Log,
    Dashboard,
}

fn main() {
    let default_rpc_port = rpc_port::DEFAULT_RPC_PORT.to_string();
    let default_faucet_port = FAUCET_PORT.to_string();
    let default_limit_ledger_size = DEFAULT_MAX_LEDGER_SHREDS.to_string();
    let default_faucet_sol = DEFAULT_FAUCET_SOL.to_string();

    let matches = App::new("solana-test-validator")
        .about("Test Validator")
        .version(solana_version::version!())
        .arg({
            let arg = Arg::with_name("config_file")
                .short("C")
                .long("config")
                .value_name("PATH")
                .takes_value(true)
                .help("Configuration file to use");
            if let Some(ref config_file) = *solana_cli_config::CONFIG_FILE {
                arg.default_value(config_file)
            } else {
                arg
            }
        })
        .arg(
            Arg::with_name("json_rpc_url")
                .short("u")
                .long("url")
                .value_name("URL_OR_MONIKER")
                .takes_value(true)
                .validator(is_url_or_moniker)
                .help(
                    "URL for Solana's JSON RPC or moniker (or their first letter): \
                   [mainnet-beta, testnet, devnet, localhost]",
                ),
        )
        .arg(
            Arg::with_name("mint_address")
                .long("mint")
                .value_name("PUBKEY")
                .validator(is_pubkey)
                .takes_value(true)
                .help(
                    "Address of the mint account that will receive tokens \
                       created at genesis.  If the ledger already exists then \
                       this parameter is silently ignored [default: client keypair]",
                ),
        )
        .arg(
            Arg::with_name("ledger_path")
                .short("l")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .default_value("test-ledger")
                .help("Use DIR as ledger location"),
        )
        .arg(
            Arg::with_name("reset")
                .short("r")
                .long("reset")
                .takes_value(false)
                .help(
                    "Reset the ledger to genesis if it exists. \
                       By default the validator will resume an existing ledger (if present)",
                ),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .takes_value(false)
                .conflicts_with("log")
                .help("Quiet mode: suppress normal output"),
        )
        .arg(
            Arg::with_name("log")
                .long("log")
                .takes_value(false)
                .conflicts_with("quiet")
                .help("Log mode: stream the validator log"),
        )
        .arg(
            Arg::with_name("faucet_port")
                .long("faucet-port")
                .value_name("PORT")
                .takes_value(true)
                .default_value(&default_faucet_port)
                .validator(solana_validator::port_validator)
                .help("Enable the faucet on this port"),
        )
        .arg(
            Arg::with_name("rpc_port")
                .long("rpc-port")
                .value_name("PORT")
                .takes_value(true)
                .default_value(&default_rpc_port)
                .validator(solana_validator::port_validator)
                .help("Enable JSON RPC on this port, and the next port for the RPC websocket"),
        )
        .arg(
            Arg::with_name("bpf_program")
                .long("bpf-program")
                .value_name("ADDRESS BPF_PROGRAM.SO")
                .takes_value(true)
                .number_of_values(2)
                .multiple(true)
                .help(
                    "Add a BPF program to the genesis configuration. \
                       If the ledger already exists then this parameter is silently ignored",
                ),
        )
        .arg(
            Arg::with_name("no_bpf_jit")
                .long("no-bpf-jit")
                .takes_value(false)
                .help("Disable the just-in-time compiler and instead use the interpreter for BPF"),
        )
        .arg(
            Arg::with_name("slots_per_epoch")
                .long("slots-per-epoch")
                .value_name("SLOTS")
                .validator(|value| {
                    value
                        .parse::<Slot>()
                        .map_err(|err| format!("error parsing '{}': {}", value, err))
                        .and_then(|slot| {
                            if slot < MINIMUM_SLOTS_PER_EPOCH {
                                Err(format!("value must be >= {}", MINIMUM_SLOTS_PER_EPOCH))
                            } else {
                                Ok(())
                            }
                        })
                })
                .takes_value(true)
                .help(
                    "Override the number of slots in an epoch. \
                       If the ledger already exists then this parameter is silently ignored",
                ),
        )
        .arg(
            Arg::with_name("gossip_port")
                .long("gossip-port")
                .value_name("PORT")
                .takes_value(true)
                .help("Gossip port number for the validator"),
        )
        .arg(
            Arg::with_name("gossip_host")
                .long("gossip-host")
                .value_name("HOST")
                .takes_value(true)
                .validator(solana_net_utils::is_host)
                .help(
                    "Gossip DNS name or IP address for the validator to advertise in gossip \
                       [default: 127.0.0.1]",
                ),
        )
        .arg(
            Arg::with_name("dynamic_port_range")
                .long("dynamic-port-range")
                .value_name("MIN_PORT-MAX_PORT")
                .takes_value(true)
                .validator(solana_validator::port_range_validator)
                .help(
                    "Range to use for dynamically assigned ports \
                    [default: 1024-65535]",
                ),
        )
        .arg(
            Arg::with_name("bind_address")
                .long("bind-address")
                .value_name("HOST")
                .takes_value(true)
                .validator(solana_net_utils::is_host)
                .default_value("0.0.0.0")
                .help("IP address to bind the validator ports [default: 0.0.0.0]"),
        )
        .arg(
            Arg::with_name("clone_account")
                .long("clone")
                .short("c")
                .value_name("ADDRESS")
                .takes_value(true)
                .validator(is_pubkey_or_keypair)
                .multiple(true)
                .requires("json_rpc_url")
                .help(
                    "Copy an account from the cluster referenced by the --url argument the \
                     genesis configuration. \
                     If the ledger already exists then this parameter is silently ignored",
                ),
        )
        .arg(
            Arg::with_name("warp_slot")
                .required(false)
                .long("warp-slot")
                .short("w")
                .takes_value(true)
                .value_name("WARP_SLOT")
                .validator(is_slot)
                .min_values(0)
                .max_values(1)
                .help(
                    "Warp the ledger to WARP_SLOT after starting the validator. \
                        If no slot is provided then the current slot of the cluster \
                        referenced by the --url argument will be used",
                ),
        )
        .arg(
            Arg::with_name("limit_ledger_size")
                .long("limit-ledger-size")
                .value_name("SHRED_COUNT")
                .takes_value(true)
                .default_value(default_limit_ledger_size.as_str())
                .help("Keep this amount of shreds in root slots."),
        )
        .arg(
            Arg::with_name("faucet_sol")
                .long("faucet-sol")
                .takes_value(true)
                .value_name("SOL")
                .default_value(default_faucet_sol.as_str())
                .help(
                    "Give the faucet address this much SOL in genesis. \
                     If the ledger already exists then this parameter is silently ignored",
                ),
        )
        .get_matches();

    let cli_config = if let Some(config_file) = matches.value_of("config_file") {
        solana_cli_config::Config::load(config_file).unwrap_or_default()
    } else {
        solana_cli_config::Config::default()
    };

    let cluster_rpc_client = value_t!(matches, "json_rpc_url", String)
        .map(normalize_to_url_if_moniker)
        .map(RpcClient::new);

    let (mint_address, random_mint) = pubkey_of(&matches, "mint_address")
        .map(|pk| (pk, false))
        .unwrap_or_else(|| {
            read_keypair_file(&cli_config.keypair_path)
                .map(|kp| (kp.pubkey(), false))
                .unwrap_or_else(|_| (Keypair::new().pubkey(), true))
        });

    let ledger_path = value_t_or_exit!(matches, "ledger_path", PathBuf);
    let reset_ledger = matches.is_present("reset");
    let output = if matches.is_present("quiet") {
        Output::None
    } else if matches.is_present("log") {
        Output::Log
    } else {
        Output::Dashboard
    };
    let rpc_port = value_t_or_exit!(matches, "rpc_port", u16);
    let faucet_port = value_t_or_exit!(matches, "faucet_port", u16);
    let slots_per_epoch = value_t!(matches, "slots_per_epoch", Slot).ok();
    let gossip_host = matches.value_of("gossip_host").map(|gossip_host| {
        solana_net_utils::parse_host(gossip_host).unwrap_or_else(|err| {
            eprintln!("Failed to parse --gossip-host: {}", err);
            exit(1);
        })
    });
    let gossip_port = value_t!(matches, "gossip_port", u16).ok();
    let dynamic_port_range = matches.value_of("dynamic_port_range").map(|port_range| {
        solana_net_utils::parse_port_range(port_range).unwrap_or_else(|| {
            eprintln!("Failed to parse --dynamic-port-range");
            exit(1);
        })
    });
    let bind_address = matches.value_of("bind_address").map(|bind_address| {
        solana_net_utils::parse_host(bind_address).unwrap_or_else(|err| {
            eprintln!("Failed to parse --bind-address: {}", err);
            exit(1);
        })
    });

    let faucet_addr = Some(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        faucet_port,
    ));
    let bpf_jit = !matches.is_present("no_bpf_jit");

    let mut programs = vec![];
    if let Some(values) = matches.values_of("bpf_program") {
        let values: Vec<&str> = values.collect::<Vec<_>>();
        for address_program in values.chunks(2) {
            match address_program {
                [address, program] => {
                    let address = address.parse::<Pubkey>().unwrap_or_else(|err| {
                        println!("Error: invalid address {}: {}", address, err);
                        exit(1);
                    });

                    let program_path = PathBuf::from(program);
                    if !program_path.exists() {
                        println!(
                            "Error: program file does not exist: {}",
                            program_path.display()
                        );
                        exit(1);
                    }

                    programs.push(ProgramInfo {
                        program_id: address,
                        loader: solana_sdk::bpf_loader::id(),
                        program_path,
                    });
                }
                _ => unreachable!(),
            }
        }
    }

    let clone_accounts: HashSet<_> = pubkeys_of(&matches, "clone_account")
        .map(|v| v.into_iter().collect())
        .unwrap_or_default();

    let warp_slot = if matches.is_present("warp_slot") {
        Some(match matches.value_of("warp_slot") {
            Some(_) => value_t_or_exit!(matches, "warp_slot", Slot),
            None => {
                cluster_rpc_client.as_ref().unwrap_or_else(|_| {
                        println!("The --url argument must be provided if --warp-slot/-w is used without an explicit slot");
                        exit(1);

                }).get_slot()
                    .unwrap_or_else(|err| {
                        println!("Unable to get current cluster slot: {}", err);
                        exit(1);
                    })
            }
        })
    } else {
        None
    };

    if !ledger_path.exists() {
        fs::create_dir(&ledger_path).unwrap_or_else(|err| {
            println!(
                "Error: Unable to create directory {}: {}",
                ledger_path.display(),
                err
            );
            exit(1);
        });
    }

    let mut ledger_fd_lock = fd_lock::RwLock::new(fs::File::open(&ledger_path).unwrap());
    let _ledger_lock = ledger_fd_lock.try_write().unwrap_or_else(|_| {
        println!(
            "Error: Unable to lock {} directory. Check if another validator is running",
            ledger_path.display()
        );
        exit(1);
    });

    if reset_ledger {
        remove_directory_contents(&ledger_path).unwrap_or_else(|err| {
            println!("Error: Unable to remove {}: {}", ledger_path.display(), err);
            exit(1);
        })
    }
    solana_runtime::snapshot_utils::remove_tmp_snapshot_archives(&ledger_path);

    let validator_log_symlink = ledger_path.join("validator.log");
    let logfile = if output != Output::Log {
        let validator_log_with_timestamp = format!(
            "validator-{}.log",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let _ = fs::remove_file(&validator_log_symlink);
        symlink::symlink_file(&validator_log_with_timestamp, &validator_log_symlink).unwrap();

        Some(
            ledger_path
                .join(validator_log_with_timestamp)
                .into_os_string()
                .into_string()
                .unwrap(),
        )
    } else {
        None
    };
    let _logger_thread = redirect_stderr_to_file(logfile);

    let faucet_lamports = sol_to_lamports(value_of(&matches, "faucet_sol").unwrap());
    let faucet_keypair_file = ledger_path.join("faucet-keypair.json");
    if !faucet_keypair_file.exists() {
        write_keypair_file(&Keypair::new(), faucet_keypair_file.to_str().unwrap()).unwrap_or_else(
            |err| {
                println!(
                    "Error: Failed to write {}: {}",
                    faucet_keypair_file.display(),
                    err
                );
                exit(1);
            },
        );
    }

    let faucet_keypair =
        read_keypair_file(faucet_keypair_file.to_str().unwrap()).unwrap_or_else(|err| {
            println!(
                "Error: Failed to read {}: {}",
                faucet_keypair_file.display(),
                err
            );
            exit(1);
        });
    let faucet_pubkey = faucet_keypair.pubkey();

    if let Some(faucet_addr) = &faucet_addr {
        let (sender, receiver) = channel();
        run_local_faucet_with_port(faucet_keypair, sender, None, faucet_addr.port());
        let _ = receiver.recv().expect("run faucet").unwrap_or_else(|err| {
            println!("Error: failed to start faucet: {}", err);
            exit(1);
        });
    }

    if TestValidatorGenesis::ledger_exists(&ledger_path) {
        for (name, long) in &[
            ("bpf_program", "--bpf-program"),
            ("clone_account", "--clone"),
            ("mint_address", "--mint"),
            ("slots_per_epoch", "--slots-per-epoch"),
            ("faucet_sol", "--faucet-sol"),
        ] {
            if matches.is_present(name) {
                println!("{} argument ignored, ledger already exists", long);
            }
        }
    } else if random_mint {
        println_name_value(
            "\nNotice!",
            "No wallet available. `solana airdrop` localnet SOL after creating one\n",
        );
    }

    let mut genesis = TestValidatorGenesis::default();
    genesis.max_ledger_shreds = value_of(&matches, "limit_ledger_size");

    let admin_service_cluster_info = Arc::new(RwLock::new(None));
    admin_rpc_service::run(
        &ledger_path,
        admin_rpc_service::AdminRpcRequestMetadata {
            rpc_addr: Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                rpc_port,
            )),
            start_progress: genesis.start_progress.clone(),
            start_time: std::time::SystemTime::now(),
            validator_exit: genesis.validator_exit.clone(),
            authorized_voter_keypairs: genesis.authorized_voter_keypairs.clone(),
            cluster_info: admin_service_cluster_info.clone(),
            tower_storage: Arc::new(FileTowerStorage::new(ledger_path.clone())),
        },
    );
    let dashboard = if output == Output::Dashboard {
        Some(
            Dashboard::new(
                &ledger_path,
                Some(&validator_log_symlink),
                Some(&mut genesis.validator_exit.write().unwrap()),
            )
            .unwrap(),
        )
    } else {
        None
    };

    genesis
        .ledger_path(&ledger_path)
        .add_account(
            faucet_pubkey,
            AccountSharedData::new(faucet_lamports, 0, &system_program::id()),
        )
        .rpc_config(JsonRpcConfig {
            enable_rpc_transaction_history: true,
            enable_cpi_and_log_storage: true,
            faucet_addr,
            ..JsonRpcConfig::default()
        })
        .bpf_jit(bpf_jit)
        .rpc_port(rpc_port)
        .add_programs_with_path(&programs);

    if !clone_accounts.is_empty() {
        genesis.clone_accounts(
            clone_accounts,
            cluster_rpc_client
                .as_ref()
                .expect("bug: --url argument missing?"),
        );
    }

    if let Some(warp_slot) = warp_slot {
        genesis.warp_slot(warp_slot);
    }

    if let Some(slots_per_epoch) = slots_per_epoch {
        genesis.epoch_schedule(EpochSchedule::custom(
            slots_per_epoch,
            slots_per_epoch,
            /* enable_warmup_epochs = */ false,
        ));

        genesis.rent = Rent::with_slots_per_epoch(slots_per_epoch);
    }

    if let Some(gossip_host) = gossip_host {
        genesis.gossip_host(gossip_host);
    }

    if let Some(gossip_port) = gossip_port {
        genesis.gossip_port(gossip_port);
    }

    if let Some(dynamic_port_range) = dynamic_port_range {
        genesis.port_range(dynamic_port_range);
    }

    if let Some(bind_address) = bind_address {
        genesis.bind_ip_addr(bind_address);
    }

    match genesis.start_with_mint_address(mint_address) {
        Ok(test_validator) => {
            *admin_service_cluster_info.write().unwrap() = Some(test_validator.cluster_info());
            if let Some(dashboard) = dashboard {
                dashboard.run(Duration::from_millis(250));
            }
            test_validator.join();
        }
        Err(err) => {
            drop(dashboard);
            println!("Error: failed to start validator: {}", err);
            exit(1);
        }
    }
}

fn remove_directory_contents(ledger_path: &Path) -> Result<(), io::Error> {
    for entry in fs::read_dir(&ledger_path)? {
        let entry = entry?;
        if entry.metadata()?.is_dir() {
            fs::remove_dir_all(&entry.path())?
        } else {
            fs::remove_file(&entry.path())?
        }
    }
    Ok(())
}
