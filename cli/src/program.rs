use crate::{
    checks::*,
    cli::{
        log_instruction_custom_error, CliCommand, CliCommandInfo, CliConfig, CliError,
        ProcessResult,
    },
};
use bip39::{Language, Mnemonic, MnemonicType, Seed};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use log::*;
use solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig};
use solana_bpf_loader_program::{syscalls::register_syscalls, BpfError, ThisInstructionMeter};
use solana_clap_utils::{self, input_parsers::*, input_validators::*, keypair::*};
use solana_cli_output::{
    display::new_spinner_progress_bar, CliProgram, CliProgramAccountType, CliProgramAuthority,
    CliProgramBuffer, CliProgramId, CliUpgradeableBuffer, CliUpgradeableBuffers,
    CliUpgradeableProgram,
};
use solana_client::{
    client_error::ClientErrorKind,
    rpc_client::RpcClient,
    rpc_config::RpcSendTransactionConfig,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
    rpc_request::MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS,
    tpu_client::{TpuClient, TpuClientConfig},
};
use solana_rbpf::{
    verifier,
    vm::{Config, Executable},
};
use solana_remote_wallet::remote_wallet::RemoteWalletManager;
use solana_sdk::{
    account::Account,
    account_utils::StateMut,
    bpf_loader, bpf_loader_deprecated,
    bpf_loader_upgradeable::{self, UpgradeableLoaderState},
    clock::Slot,
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    instruction::InstructionError,
    loader_instruction,
    message::Message,
    native_token::Sol,
    process_instruction::MockInvokeContext,
    pubkey::Pubkey,
    signature::{keypair_from_seed, read_keypair_file, Keypair, Signer},
    signers::Signers,
    system_instruction::{self, SystemError},
    system_program,
    transaction::Transaction,
    transaction::TransactionError,
};
use solana_transaction_status::TransactionConfirmationStatus;
use std::{
    collections::HashMap,
    error,
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
    thread::sleep,
    time::Duration,
};

const DATA_CHUNK_SIZE: usize = 229; // Keep program chunks under PACKET_DATA_SIZE

#[derive(Debug, PartialEq)]
pub enum ProgramCliCommand {
    Deploy {
        program_location: Option<String>,
        program_signer_index: Option<SignerIndex>,
        program_pubkey: Option<Pubkey>,
        buffer_signer_index: Option<SignerIndex>,
        buffer_pubkey: Option<Pubkey>,
        upgrade_authority_signer_index: SignerIndex,
        is_final: bool,
        max_len: Option<usize>,
        allow_excessive_balance: bool,
    },
    WriteBuffer {
        program_location: String,
        buffer_signer_index: Option<SignerIndex>,
        buffer_pubkey: Option<Pubkey>,
        buffer_authority_signer_index: Option<SignerIndex>,
        max_len: Option<usize>,
    },
    SetBufferAuthority {
        buffer_pubkey: Pubkey,
        buffer_authority_index: Option<SignerIndex>,
        new_buffer_authority: Pubkey,
    },
    SetUpgradeAuthority {
        program_pubkey: Pubkey,
        upgrade_authority_index: Option<SignerIndex>,
        new_upgrade_authority: Option<Pubkey>,
    },
    Show {
        account_pubkey: Option<Pubkey>,
        authority_pubkey: Pubkey,
        all: bool,
        use_lamports_unit: bool,
    },
    Dump {
        account_pubkey: Option<Pubkey>,
        output_location: String,
    },
    Close {
        account_pubkey: Option<Pubkey>,
        recipient_pubkey: Pubkey,
        authority_index: SignerIndex,
        use_lamports_unit: bool,
    },
}

pub trait ProgramSubCommands {
    fn program_subcommands(self) -> Self;
}

impl ProgramSubCommands for App<'_, '_> {
    fn program_subcommands(self) -> Self {
        self.subcommand(
            SubCommand::with_name("program")
                .about("Program management")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(
                    SubCommand::with_name("deploy")
                        .about("Deploy a program")
                        .arg(
                            Arg::with_name("program_location")
                                .index(1)
                                .value_name("PROGRAM_FILEPATH")
                                .takes_value(true)
                                .help("/path/to/program.so"),
                        )
                        .arg(
                            Arg::with_name("buffer")
                                .long("buffer")
                                .value_name("BUFFER_SIGNER")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Intermediate buffer account to write data to, which can be used to resume a failed deploy \
                                      [default: random address]")
                        )
                        .arg(
                            Arg::with_name("upgrade_authority")
                                .long("upgrade-authority")
                                .value_name("UPGRADE_AUTHORITY_SIGNER")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Upgrade authority [default: the default configured keypair]")
                        )
                        .arg(
                            pubkey!(Arg::with_name("program_id")
                                .long("program-id")
                                .value_name("PROGRAM_ID"),
                                "Executable program's address, must be a keypair for initial deploys, can be a pubkey for upgrades \
                                [default: address of keypair at /path/to/program-keypair.json if present, otherwise a random address]"),
                        )
                        .arg(
                            Arg::with_name("final")
                                .long("final")
                                .help("The program will not be upgradeable")
                        )
                        .arg(
                            Arg::with_name("max_len")
                                .long("max-len")
                                .value_name("max_len")
                                .takes_value(true)
                                .required(false)
                                .help("Maximum length of the upgradeable program \
                                      [default: twice the length of the original deployed program]")
                        )
                        .arg(
                            Arg::with_name("allow_excessive_balance")
                                .long("allow-excessive-deploy-account-balance")
                                .takes_value(false)
                                .help("Use the designated program id even if the account already holds a large balance of SOL")
                        ),
                )
                .subcommand(
                    SubCommand::with_name("write-buffer")
                        .about("Writes a program into a buffer account")
                        .arg(
                            Arg::with_name("program_location")
                                .index(1)
                                .value_name("PROGRAM_FILEPATH")
                                .takes_value(true)
                                .required(true)
                                .help("/path/to/program.so"),
                        )
                        .arg(
                            Arg::with_name("buffer")
                                .long("buffer")
                                .value_name("BUFFER_SIGNER")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Buffer account to write data into [default: random address]")
                        )
                        .arg(
                            Arg::with_name("buffer_authority")
                                .long("buffer-authority")
                                .value_name("BUFFER_AUTHORITY_SIGNER")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Buffer authority [default: the default configured keypair]")
                        )
                        .arg(
                            Arg::with_name("max_len")
                                .long("max-len")
                                .value_name("max_len")
                                .takes_value(true)
                                .required(false)
                                .help("Maximum length of the upgradeable program \
                                      [default: twice the length of the original deployed program]")
                        ),
                )
                .subcommand(
                    SubCommand::with_name("set-buffer-authority")
                        .about("Set a new buffer authority")
                        .arg(
                            Arg::with_name("buffer")
                                .index(1)
                                .value_name("BUFFER_PUBKEY")
                                .takes_value(true)
                                .required(true)
                                .help("Public key of the buffer")
                        )
                        .arg(
                            Arg::with_name("buffer_authority")
                                .long("buffer-authority")
                                .value_name("BUFFER_AUTHORITY_SIGNER")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Buffer authority [default: the default configured keypair]")
                        )
                        .arg(
                            pubkey!(Arg::with_name("new_buffer_authority")
                                .long("new-buffer-authority")
                                .value_name("NEW_BUFFER_AUTHORITY")
                                .required(true),
                                "Address of the new buffer authority"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("set-upgrade-authority")
                        .about("Set a new program authority")
                        .arg(
                            Arg::with_name("program_id")
                                .index(1)
                                .value_name("PROGRAM_ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .help("Address of the program to upgrade")
                        )
                        .arg(
                            Arg::with_name("upgrade_authority")
                                .long("upgrade-authority")
                                .value_name("UPGRADE_AUTHORITY_SIGNER")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Upgrade authority [default: the default configured keypair]")
                        )
                        .arg(
                            pubkey!(Arg::with_name("new_upgrade_authority")
                                .long("new-upgrade-authority")
                                .required_unless("final")
                                .value_name("NEW_UPGRADE_AUTHORITY"),
                                "Address of the new upgrade authority"),
                        )
                        .arg(
                            Arg::with_name("final")
                                .long("final")
                                .conflicts_with("new_upgrade_authority")
                                .help("The program will not be upgradeable")
                        )
                )
                .subcommand(
                    SubCommand::with_name("show")
                        .about("Display information about a buffer or program")
                        .arg(
                            Arg::with_name("account")
                                .index(1)
                                .value_name("ACCOUNT_ADDRESS")
                                .takes_value(true)
                                .help("Address of the buffer or program to show")
                        )
                        .arg(
                            Arg::with_name("buffers")
                                .long("buffers")
                                .conflicts_with("account")
                                .required_unless("account")
                                .help("Show every buffer account that matches the authority")
                        )
                        .arg(
                            Arg::with_name("all")
                                .long("all")
                                .conflicts_with("account")
                                .help("Show accounts for all authorities")
                        )
                        .arg(
                            pubkey!(Arg::with_name("buffer_authority")
                                .long("buffer-authority")
                                .value_name("AUTHORITY")
                                .conflicts_with("all"),
                                "Authority [default: the default configured keypair]"),
                        )
                        .arg(
                            Arg::with_name("lamports")
                                .long("lamports")
                                .takes_value(false)
                                .help("Display balance in lamports instead of SOL"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("dump")
                        .about("Write the program data to a file")
                        .arg(
                            Arg::with_name("account")
                                .index(1)
                                .value_name("ACCOUNT_ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .help("Address of the buffer or program")
                        )
                        .arg(
                            Arg::with_name("output_location")
                                .index(2)
                                .value_name("OUTPUT_FILEPATH")
                                .takes_value(true)
                                .required(true)
                                .help("/path/to/program.so"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("close")
                        .about("Close an acount and withdraw all lamports")
                        .arg(
                            Arg::with_name("account")
                                .index(1)
                                .value_name("BUFFER_ACCOUNT_ADDRESS")
                                .takes_value(true)
                                .help("Address of the buffer account to close"),
                        )
                        .arg(
                            Arg::with_name("buffers")
                                .long("buffers")
                                .conflicts_with("account")
                                .required_unless("account")
                                .help("Close every buffer accounts that match the authority")
                        )
                        .arg(
                            Arg::with_name("buffer_authority")
                                .long("buffer-authority")
                                .value_name("AUTHORITY_SIGNER")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Authority [default: the default configured keypair]")
                        )
                        .arg(
                            pubkey!(Arg::with_name("recipient_account")
                                .long("recipient")
                                .value_name("RECIPIENT_ADDRESS"),
                                "Address of the account to deposit the closed account's lamports [default: the default configured keypair]"),
                        )
                        .arg(
                            Arg::with_name("lamports")
                                .long("lamports")
                                .takes_value(false)
                                .help("Display balance in lamports instead of SOL"),
                        ),
                )
        )
    }
}

pub fn parse_program_subcommand(
    matches: &ArgMatches<'_>,
    default_signer: &DefaultSigner,
    wallet_manager: &mut Option<Arc<RemoteWalletManager>>,
) -> Result<CliCommandInfo, CliError> {
    let response = match matches.subcommand() {
        ("deploy", Some(matches)) => {
            let mut bulk_signers = vec![Some(
                default_signer.signer_from_path(matches, wallet_manager)?,
            )];

            let program_location = matches
                .value_of("program_location")
                .map(|location| location.to_string());

            let buffer_pubkey = if let Ok((buffer_signer, Some(buffer_pubkey))) =
                signer_of(matches, "buffer", wallet_manager)
            {
                bulk_signers.push(buffer_signer);
                Some(buffer_pubkey)
            } else {
                pubkey_of_signer(matches, "buffer", wallet_manager)?
            };

            let program_pubkey = if let Ok((program_signer, Some(program_pubkey))) =
                signer_of(matches, "program_id", wallet_manager)
            {
                bulk_signers.push(program_signer);
                Some(program_pubkey)
            } else {
                pubkey_of_signer(matches, "program_id", wallet_manager)?
            };

            let upgrade_authority_pubkey =
                if let Ok((upgrade_authority_signer, Some(upgrade_authority_pubkey))) =
                    signer_of(matches, "upgrade_authority", wallet_manager)
                {
                    bulk_signers.push(upgrade_authority_signer);
                    Some(upgrade_authority_pubkey)
                } else {
                    Some(
                        default_signer
                            .signer_from_path(matches, wallet_manager)?
                            .pubkey(),
                    )
                };

            let max_len = value_of(matches, "max_len");

            let signer_info =
                default_signer.generate_unique_signers(bulk_signers, matches, wallet_manager)?;

            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Deploy {
                    program_location,
                    program_signer_index: signer_info.index_of_or_none(program_pubkey),
                    program_pubkey,
                    buffer_signer_index: signer_info.index_of_or_none(buffer_pubkey),
                    buffer_pubkey,
                    upgrade_authority_signer_index: signer_info
                        .index_of(upgrade_authority_pubkey)
                        .unwrap(),
                    is_final: matches.is_present("final"),
                    max_len,
                    allow_excessive_balance: matches.is_present("allow_excessive_balance"),
                }),
                signers: signer_info.signers,
            }
        }
        ("write-buffer", Some(matches)) => {
            let mut bulk_signers = vec![Some(
                default_signer.signer_from_path(matches, wallet_manager)?,
            )];

            let buffer_pubkey = if let Ok((buffer_signer, Some(buffer_pubkey))) =
                signer_of(matches, "buffer", wallet_manager)
            {
                bulk_signers.push(buffer_signer);
                Some(buffer_pubkey)
            } else {
                pubkey_of_signer(matches, "buffer", wallet_manager)?
            };

            let buffer_authority_pubkey =
                if let Ok((buffer_authority_signer, Some(buffer_authority_pubkey))) =
                    signer_of(matches, "buffer_authority", wallet_manager)
                {
                    bulk_signers.push(buffer_authority_signer);
                    Some(buffer_authority_pubkey)
                } else {
                    Some(
                        default_signer
                            .signer_from_path(matches, wallet_manager)?
                            .pubkey(),
                    )
                };

            let max_len = value_of(matches, "max_len");

            let signer_info =
                default_signer.generate_unique_signers(bulk_signers, matches, wallet_manager)?;

            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::WriteBuffer {
                    program_location: matches.value_of("program_location").unwrap().to_string(),
                    buffer_signer_index: signer_info.index_of_or_none(buffer_pubkey),
                    buffer_pubkey,
                    buffer_authority_signer_index: signer_info
                        .index_of_or_none(buffer_authority_pubkey),
                    max_len,
                }),
                signers: signer_info.signers,
            }
        }
        ("set-buffer-authority", Some(matches)) => {
            let buffer_pubkey = pubkey_of(matches, "buffer").unwrap();

            let (buffer_authority_signer, buffer_authority_pubkey) =
                signer_of(matches, "buffer_authority", wallet_manager)?;
            let new_buffer_authority =
                pubkey_of_signer(matches, "new_buffer_authority", wallet_manager)?.unwrap();

            let signer_info = default_signer.generate_unique_signers(
                vec![
                    Some(default_signer.signer_from_path(matches, wallet_manager)?),
                    buffer_authority_signer,
                ],
                matches,
                wallet_manager,
            )?;

            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::SetBufferAuthority {
                    buffer_pubkey,
                    buffer_authority_index: signer_info.index_of(buffer_authority_pubkey),
                    new_buffer_authority,
                }),
                signers: signer_info.signers,
            }
        }
        ("set-upgrade-authority", Some(matches)) => {
            let (upgrade_authority_signer, upgrade_authority_pubkey) =
                signer_of(matches, "upgrade_authority", wallet_manager)?;
            let program_pubkey = pubkey_of(matches, "program_id").unwrap();
            let new_upgrade_authority = if matches.is_present("final") {
                None
            } else {
                pubkey_of_signer(matches, "new_upgrade_authority", wallet_manager)?
            };

            let signer_info = default_signer.generate_unique_signers(
                vec![
                    Some(default_signer.signer_from_path(matches, wallet_manager)?),
                    upgrade_authority_signer,
                ],
                matches,
                wallet_manager,
            )?;

            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::SetUpgradeAuthority {
                    program_pubkey,
                    upgrade_authority_index: signer_info.index_of(upgrade_authority_pubkey),
                    new_upgrade_authority,
                }),
                signers: signer_info.signers,
            }
        }
        ("show", Some(matches)) => {
            let account_pubkey = if matches.is_present("buffers") {
                None
            } else {
                pubkey_of(matches, "account")
            };

            let authority_pubkey = if let Some(authority_pubkey) =
                pubkey_of_signer(matches, "buffer_authority", wallet_manager)?
            {
                authority_pubkey
            } else {
                default_signer
                    .signer_from_path(matches, wallet_manager)?
                    .pubkey()
            };

            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Show {
                    account_pubkey,
                    authority_pubkey,
                    all: matches.is_present("all"),
                    use_lamports_unit: matches.is_present("lamports"),
                }),
                signers: vec![],
            }
        }
        ("dump", Some(matches)) => CliCommandInfo {
            command: CliCommand::Program(ProgramCliCommand::Dump {
                account_pubkey: pubkey_of(matches, "account"),
                output_location: matches.value_of("output_location").unwrap().to_string(),
            }),
            signers: vec![],
        },
        ("close", Some(matches)) => {
            let account_pubkey = if matches.is_present("buffers") {
                None
            } else {
                pubkey_of(matches, "account")
            };

            let recipient_pubkey = if let Some(recipient_pubkey) =
                pubkey_of_signer(matches, "recipient_account", wallet_manager)?
            {
                recipient_pubkey
            } else {
                default_signer
                    .signer_from_path(matches, wallet_manager)?
                    .pubkey()
            };

            let (authority_signer, authority_pubkey) =
                signer_of(matches, "buffer_authority", wallet_manager)?;

            let signer_info = default_signer.generate_unique_signers(
                vec![
                    Some(default_signer.signer_from_path(matches, wallet_manager)?),
                    authority_signer,
                ],
                matches,
                wallet_manager,
            )?;

            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Close {
                    account_pubkey,
                    recipient_pubkey,
                    authority_index: signer_info.index_of(authority_pubkey).unwrap(),
                    use_lamports_unit: matches.is_present("lamports"),
                }),
                signers: signer_info.signers,
            }
        }
        _ => unreachable!(),
    };
    Ok(response)
}

pub fn process_program_subcommand(
    rpc_client: Arc<RpcClient>,
    config: &CliConfig,
    program_subcommand: &ProgramCliCommand,
) -> ProcessResult {
    match program_subcommand {
        ProgramCliCommand::Deploy {
            program_location,
            program_signer_index,
            program_pubkey,
            buffer_signer_index,
            buffer_pubkey,
            upgrade_authority_signer_index,
            is_final,
            max_len,
            allow_excessive_balance,
        } => process_program_deploy(
            rpc_client,
            config,
            program_location,
            *program_signer_index,
            *program_pubkey,
            *buffer_signer_index,
            *buffer_pubkey,
            *upgrade_authority_signer_index,
            *is_final,
            *max_len,
            *allow_excessive_balance,
        ),
        ProgramCliCommand::WriteBuffer {
            program_location,
            buffer_signer_index,
            buffer_pubkey,
            buffer_authority_signer_index,
            max_len,
        } => process_write_buffer(
            rpc_client,
            config,
            program_location,
            *buffer_signer_index,
            *buffer_pubkey,
            *buffer_authority_signer_index,
            *max_len,
        ),
        ProgramCliCommand::SetBufferAuthority {
            buffer_pubkey,
            buffer_authority_index,
            new_buffer_authority,
        } => process_set_authority(
            &rpc_client,
            config,
            None,
            Some(*buffer_pubkey),
            *buffer_authority_index,
            Some(*new_buffer_authority),
        ),
        ProgramCliCommand::SetUpgradeAuthority {
            program_pubkey,
            upgrade_authority_index,
            new_upgrade_authority,
        } => process_set_authority(
            &rpc_client,
            config,
            Some(*program_pubkey),
            None,
            *upgrade_authority_index,
            *new_upgrade_authority,
        ),
        ProgramCliCommand::Show {
            account_pubkey,
            authority_pubkey,
            all,
            use_lamports_unit,
        } => process_show(
            &rpc_client,
            config,
            *account_pubkey,
            *authority_pubkey,
            *all,
            *use_lamports_unit,
        ),
        ProgramCliCommand::Dump {
            account_pubkey,
            output_location,
        } => process_dump(&rpc_client, config, *account_pubkey, output_location),
        ProgramCliCommand::Close {
            account_pubkey,
            recipient_pubkey,
            authority_index,
            use_lamports_unit,
        } => process_close(
            &rpc_client,
            config,
            *account_pubkey,
            *recipient_pubkey,
            *authority_index,
            *use_lamports_unit,
        ),
    }
}

fn get_default_program_keypair(program_location: &Option<String>) -> Keypair {
    let program_keypair = {
        if let Some(program_location) = program_location {
            let mut keypair_file = PathBuf::new();
            keypair_file.push(program_location);
            let mut filename = keypair_file.file_stem().unwrap().to_os_string();
            filename.push("-keypair");
            keypair_file.set_file_name(filename);
            keypair_file.set_extension("json");
            if let Ok(keypair) = read_keypair_file(&keypair_file.to_str().unwrap()) {
                keypair
            } else {
                Keypair::new()
            }
        } else {
            Keypair::new()
        }
    };
    program_keypair
}

/// Deploy using upgradeable loader
#[allow(clippy::too_many_arguments)]
fn process_program_deploy(
    rpc_client: Arc<RpcClient>,
    config: &CliConfig,
    program_location: &Option<String>,
    program_signer_index: Option<SignerIndex>,
    program_pubkey: Option<Pubkey>,
    buffer_signer_index: Option<SignerIndex>,
    buffer_pubkey: Option<Pubkey>,
    upgrade_authority_signer_index: SignerIndex,
    is_final: bool,
    max_len: Option<usize>,
    allow_excessive_balance: bool,
) -> ProcessResult {
    let (words, mnemonic, buffer_keypair) = create_ephemeral_keypair()?;
    let (buffer_provided, buffer_signer, buffer_pubkey) = if let Some(i) = buffer_signer_index {
        (true, Some(config.signers[i]), config.signers[i].pubkey())
    } else if let Some(pubkey) = buffer_pubkey {
        (true, None, pubkey)
    } else {
        (
            false,
            Some(&buffer_keypair as &dyn Signer),
            buffer_keypair.pubkey(),
        )
    };
    let upgrade_authority_signer = config.signers[upgrade_authority_signer_index];

    let default_program_keypair = get_default_program_keypair(program_location);
    let (program_signer, program_pubkey) = if let Some(i) = program_signer_index {
        (Some(config.signers[i]), config.signers[i].pubkey())
    } else if let Some(program_pubkey) = program_pubkey {
        (None, program_pubkey)
    } else {
        (
            Some(&default_program_keypair as &dyn Signer),
            default_program_keypair.pubkey(),
        )
    };

    let do_deploy = if let Some(account) = rpc_client
        .get_account_with_commitment(&program_pubkey, config.commitment)?
        .value
    {
        if account.owner != bpf_loader_upgradeable::id() {
            return Err(format!(
                "Account {} is not an upgradeable program or already in use",
                program_pubkey
            )
            .into());
        }

        if !account.executable {
            // Continue an initial deploy
            true
        } else if let Ok(UpgradeableLoaderState::Program {
            programdata_address,
        }) = account.state()
        {
            if let Some(account) = rpc_client
                .get_account_with_commitment(&programdata_address, config.commitment)?
                .value
            {
                if let Ok(UpgradeableLoaderState::ProgramData {
                    slot: _,
                    upgrade_authority_address: program_authority_pubkey,
                }) = account.state()
                {
                    if program_authority_pubkey.is_none() {
                        return Err(
                            format!("Program {} is no longer upgradeable", program_pubkey).into(),
                        );
                    }
                    if program_authority_pubkey != Some(upgrade_authority_signer.pubkey()) {
                        return Err(format!(
                            "Program's authority {:?} does not match authority provided {:?}",
                            program_authority_pubkey,
                            upgrade_authority_signer.pubkey(),
                        )
                        .into());
                    }
                    // Do upgrade
                    false
                } else {
                    return Err(format!(
                        "{} is not an upgradeable loader ProgramData account",
                        programdata_address
                    )
                    .into());
                }
            } else {
                return Err(
                    format!("ProgramData account {} does not exist", programdata_address).into(),
                );
            }
        } else {
            return Err(format!("{} is not an upgradeable program", program_pubkey).into());
        }
    } else {
        // do new deploy
        true
    };

    let (program_data, program_len) = if let Some(program_location) = program_location {
        let program_data = read_and_verify_elf(program_location)?;
        let program_len = program_data.len();
        (program_data, program_len)
    } else if buffer_provided {
        // Check supplied buffer account
        if let Some(account) = rpc_client
            .get_account_with_commitment(&buffer_pubkey, config.commitment)?
            .value
        {
            if let Ok(UpgradeableLoaderState::Buffer {
                authority_address: _,
            }) = account.state()
            {
            } else {
                return Err(format!("Buffer account {} is not initialized", buffer_pubkey).into());
            }
            (vec![], account.data.len())
        } else {
            return Err(format!(
                "Buffer account {} not found, was it already consumed?",
                buffer_pubkey
            )
            .into());
        }
    } else {
        return Err("Program location required if buffer not supplied".into());
    };
    let buffer_data_len = program_len;
    let programdata_len = if let Some(len) = max_len {
        if program_len > len {
            return Err("Max length specified not large enough".into());
        }
        len
    } else if is_final {
        program_len
    } else {
        program_len * 2
    };
    let minimum_balance = rpc_client.get_minimum_balance_for_rent_exemption(
        UpgradeableLoaderState::programdata_len(buffer_data_len)?,
    )?;

    let result = if do_deploy {
        if program_signer.is_none() {
            return Err(
                "Initial deployments require a keypair be provided for the program id".into(),
            );
        }
        do_process_program_write_and_deploy(
            rpc_client.clone(),
            config,
            &program_data,
            buffer_data_len,
            programdata_len,
            minimum_balance,
            &bpf_loader_upgradeable::id(),
            Some(&[program_signer.unwrap(), upgrade_authority_signer]),
            buffer_signer,
            &buffer_pubkey,
            Some(upgrade_authority_signer),
            allow_excessive_balance,
        )
    } else {
        do_process_program_upgrade(
            rpc_client.clone(),
            config,
            &program_data,
            &program_pubkey,
            config.signers[upgrade_authority_signer_index],
            &buffer_pubkey,
            buffer_signer,
        )
    };
    if result.is_ok() && is_final {
        process_set_authority(
            &rpc_client,
            config,
            Some(program_pubkey),
            None,
            Some(upgrade_authority_signer_index),
            None,
        )?;
    }
    if result.is_err() && buffer_signer_index.is_none() {
        report_ephemeral_mnemonic(words, mnemonic);
    }
    result
}

fn process_write_buffer(
    rpc_client: Arc<RpcClient>,
    config: &CliConfig,
    program_location: &str,
    buffer_signer_index: Option<SignerIndex>,
    buffer_pubkey: Option<Pubkey>,
    buffer_authority_signer_index: Option<SignerIndex>,
    max_len: Option<usize>,
) -> ProcessResult {
    // Create ephemeral keypair to use for Buffer account, if not provided
    let (words, mnemonic, buffer_keypair) = create_ephemeral_keypair()?;
    let (buffer_signer, buffer_pubkey) = if let Some(i) = buffer_signer_index {
        (Some(config.signers[i]), config.signers[i].pubkey())
    } else if let Some(pubkey) = buffer_pubkey {
        (None, pubkey)
    } else {
        (
            Some(&buffer_keypair as &dyn Signer),
            buffer_keypair.pubkey(),
        )
    };
    let buffer_authority = if let Some(i) = buffer_authority_signer_index {
        config.signers[i]
    } else {
        config.signers[0]
    };

    if let Some(account) = rpc_client
        .get_account_with_commitment(&buffer_pubkey, config.commitment)?
        .value
    {
        if let Ok(UpgradeableLoaderState::Buffer { authority_address }) = account.state() {
            if authority_address.is_none() {
                return Err(format!("Buffer {} is immutable", buffer_pubkey).into());
            }
            if authority_address != Some(buffer_authority.pubkey()) {
                return Err(format!(
                    "Buffer's authority {:?} does not match authority provided {}",
                    authority_address,
                    buffer_authority.pubkey()
                )
                .into());
            }
        } else {
            return Err(format!(
                "{} is not an upgradeable loader buffer account",
                buffer_pubkey
            )
            .into());
        }
    }

    let program_data = read_and_verify_elf(program_location)?;
    let buffer_data_len = if let Some(len) = max_len {
        len
    } else {
        program_data.len()
    };
    let minimum_balance = rpc_client.get_minimum_balance_for_rent_exemption(
        UpgradeableLoaderState::programdata_len(buffer_data_len)?,
    )?;

    let result = do_process_program_write_and_deploy(
        rpc_client,
        config,
        &program_data,
        program_data.len(),
        program_data.len(),
        minimum_balance,
        &bpf_loader_upgradeable::id(),
        None,
        buffer_signer,
        &buffer_pubkey,
        Some(buffer_authority),
        true,
    );

    if result.is_err() && buffer_signer_index.is_none() && buffer_signer.is_some() {
        report_ephemeral_mnemonic(words, mnemonic);
    }
    result
}

fn process_set_authority(
    rpc_client: &RpcClient,
    config: &CliConfig,
    program_pubkey: Option<Pubkey>,
    buffer_pubkey: Option<Pubkey>,
    authority: Option<SignerIndex>,
    new_authority: Option<Pubkey>,
) -> ProcessResult {
    let authority_signer = if let Some(index) = authority {
        config.signers[index]
    } else {
        return Err("Set authority requires the current authority".into());
    };

    trace!("Set a new authority");
    let (blockhash, _) = rpc_client.get_recent_blockhash()?;

    let mut tx = if let Some(ref pubkey) = program_pubkey {
        Transaction::new_unsigned(Message::new(
            &[bpf_loader_upgradeable::set_upgrade_authority(
                pubkey,
                &authority_signer.pubkey(),
                new_authority.as_ref(),
            )],
            Some(&config.signers[0].pubkey()),
        ))
    } else if let Some(pubkey) = buffer_pubkey {
        if let Some(ref new_authority) = new_authority {
            Transaction::new_unsigned(Message::new(
                &[bpf_loader_upgradeable::set_buffer_authority(
                    &pubkey,
                    &authority_signer.pubkey(),
                    new_authority,
                )],
                Some(&config.signers[0].pubkey()),
            ))
        } else {
            return Err("Buffer authority cannot be None".into());
        }
    } else {
        return Err("Program or Buffer not provided".into());
    };

    tx.try_sign(&[config.signers[0], authority_signer], blockhash)?;
    rpc_client
        .send_and_confirm_transaction_with_spinner_and_config(
            &tx,
            config.commitment,
            RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(config.commitment.commitment),
                ..RpcSendTransactionConfig::default()
            },
        )
        .map_err(|e| format!("Setting authority failed: {}", e))?;

    let authority = CliProgramAuthority {
        authority: new_authority
            .map(|pubkey| pubkey.to_string())
            .unwrap_or_else(|| "none".to_string()),
        account_type: if program_pubkey.is_some() {
            CliProgramAccountType::Program
        } else {
            CliProgramAccountType::Buffer
        },
    };
    Ok(config.output_format.formatted_string(&authority))
}

fn get_buffers(
    rpc_client: &RpcClient,
    authority_pubkey: Option<Pubkey>,
) -> Result<Vec<(Pubkey, Account)>, Box<dyn std::error::Error>> {
    let mut bytes = vec![1, 0, 0, 0, 1];
    let length = bytes.len() + 32; // Pubkey length
    if let Some(authority_pubkey) = authority_pubkey {
        bytes.extend_from_slice(authority_pubkey.as_ref());
    }

    let results = rpc_client.get_program_accounts_with_config(
        &bpf_loader_upgradeable::id(),
        RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp {
                offset: 0,
                bytes: MemcmpEncodedBytes::Binary(bs58::encode(bytes).into_string()),
                encoding: None,
            })]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                data_slice: Some(UiDataSliceConfig { offset: 0, length }),
                ..RpcAccountInfoConfig::default()
            },
            ..RpcProgramAccountsConfig::default()
        },
    )?;
    Ok(results)
}

fn process_show(
    rpc_client: &RpcClient,
    config: &CliConfig,
    account_pubkey: Option<Pubkey>,
    authority_pubkey: Pubkey,
    all: bool,
    use_lamports_unit: bool,
) -> ProcessResult {
    if let Some(account_pubkey) = account_pubkey {
        if let Some(account) = rpc_client
            .get_account_with_commitment(&account_pubkey, config.commitment)?
            .value
        {
            if account.owner == bpf_loader::id() || account.owner == bpf_loader_deprecated::id() {
                Ok(config.output_format.formatted_string(&CliProgram {
                    program_id: account_pubkey.to_string(),
                    owner: account.owner.to_string(),
                    data_len: account.data.len(),
                }))
            } else if account.owner == bpf_loader_upgradeable::id() {
                if let Ok(UpgradeableLoaderState::Program {
                    programdata_address,
                }) = account.state()
                {
                    if let Some(programdata_account) = rpc_client
                        .get_account_with_commitment(&programdata_address, config.commitment)?
                        .value
                    {
                        if let Ok(UpgradeableLoaderState::ProgramData {
                            upgrade_authority_address,
                            slot,
                        }) = programdata_account.state()
                        {
                            Ok(config
                                .output_format
                                .formatted_string(&CliUpgradeableProgram {
                                    program_id: account_pubkey.to_string(),
                                    owner: account.owner.to_string(),
                                    programdata_address: programdata_address.to_string(),
                                    authority: upgrade_authority_address
                                        .map(|pubkey| pubkey.to_string())
                                        .unwrap_or_else(|| "none".to_string()),
                                    last_deploy_slot: slot,
                                    data_len: programdata_account.data.len()
                                        - UpgradeableLoaderState::programdata_data_offset()?,
                                }))
                        } else {
                            Err(format!("Invalid associated ProgramData account {} found for the program {}",
                                        programdata_address, account_pubkey)
                                    .into(),
                            )
                        }
                    } else {
                        Err(format!(
                            "Failed to find associated ProgramData account {} for the program {}",
                            programdata_address, account_pubkey
                        )
                        .into())
                    }
                } else if let Ok(UpgradeableLoaderState::Buffer { authority_address }) =
                    account.state()
                {
                    Ok(config
                        .output_format
                        .formatted_string(&CliUpgradeableBuffer {
                            address: account_pubkey.to_string(),
                            authority: authority_address
                                .map(|pubkey| pubkey.to_string())
                                .unwrap_or_else(|| "none".to_string()),
                            data_len: account.data.len()
                                - UpgradeableLoaderState::buffer_data_offset()?,
                            lamports: account.lamports,
                            use_lamports_unit,
                        }))
                } else {
                    Err(format!(
                        "{} is not an upgradeble loader buffer or program account",
                        account_pubkey
                    )
                    .into())
                }
            } else {
                Err(format!("{} is not a BPF program", account_pubkey).into())
            }
        } else {
            Err(format!("Unable to find the account {}", account_pubkey).into())
        }
    } else {
        let authority_pubkey = if all { None } else { Some(authority_pubkey) };
        let mut buffers = vec![];
        let results = get_buffers(rpc_client, authority_pubkey)?;
        for (address, account) in results.iter() {
            if let Ok(UpgradeableLoaderState::Buffer { authority_address }) = account.state() {
                buffers.push(CliUpgradeableBuffer {
                    address: address.to_string(),
                    authority: authority_address
                        .map(|pubkey| pubkey.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    data_len: 0,
                    lamports: account.lamports,
                    use_lamports_unit,
                });
            } else {
                return Err(format!("Error parsing account {}", address).into());
            }
        }
        Ok(config
            .output_format
            .formatted_string(&CliUpgradeableBuffers {
                buffers,
                use_lamports_unit,
            }))
    }
}

fn process_dump(
    rpc_client: &RpcClient,
    config: &CliConfig,
    account_pubkey: Option<Pubkey>,
    output_location: &str,
) -> ProcessResult {
    if let Some(account_pubkey) = account_pubkey {
        if let Some(account) = rpc_client
            .get_account_with_commitment(&account_pubkey, config.commitment)?
            .value
        {
            if account.owner == bpf_loader::id() || account.owner == bpf_loader_deprecated::id() {
                let mut f = File::create(output_location)?;
                f.write_all(&account.data)?;
                Ok(format!("Wrote program to {}", output_location))
            } else if account.owner == bpf_loader_upgradeable::id() {
                if let Ok(UpgradeableLoaderState::Program {
                    programdata_address,
                }) = account.state()
                {
                    if let Some(programdata_account) = rpc_client
                        .get_account_with_commitment(&programdata_address, config.commitment)?
                        .value
                    {
                        if let Ok(UpgradeableLoaderState::ProgramData { .. }) =
                            programdata_account.state()
                        {
                            let offset =
                                UpgradeableLoaderState::programdata_data_offset().unwrap_or(0);
                            let program_data = &programdata_account.data[offset..];
                            let mut f = File::create(output_location)?;
                            f.write_all(program_data)?;
                            Ok(format!("Wrote program to {}", output_location))
                        } else {
                            Err(
                                format!("Invalid associated ProgramData account {} found for the program {}",
                                        programdata_address, account_pubkey)
                                    .into(),
                            )
                        }
                    } else {
                        Err(format!(
                            "Failed to find associated ProgramData account {} for the program {}",
                            programdata_address, account_pubkey
                        )
                        .into())
                    }
                } else if let Ok(UpgradeableLoaderState::Buffer { .. }) = account.state() {
                    let offset = UpgradeableLoaderState::buffer_data_offset().unwrap_or(0);
                    let program_data = &account.data[offset..];
                    let mut f = File::create(output_location)?;
                    f.write_all(program_data)?;
                    Ok(format!("Wrote program to {}", output_location))
                } else {
                    Err(format!(
                        "{} is not an upgradeble loader buffer or program account",
                        account_pubkey
                    )
                    .into())
                }
            } else {
                Err(format!("{} is not a BPF program", account_pubkey).into())
            }
        } else {
            Err(format!("Unable to find the account {}", account_pubkey).into())
        }
    } else {
        Err("No account specified".into())
    }
}

fn close(
    rpc_client: &RpcClient,
    config: &CliConfig,
    account_pubkey: &Pubkey,
    recipient_pubkey: &Pubkey,
    authority_signer: &dyn Signer,
) -> Result<(), Box<dyn std::error::Error>> {
    let (blockhash, _) = rpc_client.get_recent_blockhash()?;

    let mut tx = Transaction::new_unsigned(Message::new(
        &[bpf_loader_upgradeable::close(
            account_pubkey,
            recipient_pubkey,
            &authority_signer.pubkey(),
        )],
        Some(&config.signers[0].pubkey()),
    ));

    tx.try_sign(&[config.signers[0], authority_signer], blockhash)?;
    let result = rpc_client.send_and_confirm_transaction_with_spinner_and_config(
        &tx,
        config.commitment,
        RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(config.commitment.commitment),
            ..RpcSendTransactionConfig::default()
        },
    );
    if let Err(err) = result {
        if let ClientErrorKind::TransactionError(TransactionError::InstructionError(
            _,
            InstructionError::InvalidInstructionData,
        )) = err.kind()
        {
            return Err("Closing a buffer account is not supported by the cluster".into());
        } else {
            return Err(format!("Close failed: {}", err).into());
        }
    }
    Ok(())
}

fn process_close(
    rpc_client: &RpcClient,
    config: &CliConfig,
    account_pubkey: Option<Pubkey>,
    recipient_pubkey: Pubkey,
    authority_index: SignerIndex,
    use_lamports_unit: bool,
) -> ProcessResult {
    let authority_signer = config.signers[authority_index];
    let mut buffers = vec![];

    if let Some(account_pubkey) = account_pubkey {
        if let Some(account) = rpc_client
            .get_account_with_commitment(&account_pubkey, config.commitment)?
            .value
        {
            if let Ok(UpgradeableLoaderState::Buffer { authority_address }) = account.state() {
                if authority_address != Some(authority_signer.pubkey()) {
                    return Err(format!(
                        "Buffer account authority {:?} does not match {:?}",
                        authority_address,
                        Some(authority_signer.pubkey())
                    )
                    .into());
                } else {
                    close(
                        rpc_client,
                        config,
                        &account_pubkey,
                        &recipient_pubkey,
                        authority_signer,
                    )?;

                    buffers.push(CliUpgradeableBuffer {
                        address: account_pubkey.to_string(),
                        authority: authority_address
                            .map(|pubkey| pubkey.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        data_len: 0,
                        lamports: account.lamports,
                        use_lamports_unit,
                    });
                }
            } else {
                return Err(format!(
                    "{} is not an upgradeble loader buffer account",
                    account_pubkey
                )
                .into());
            }
        } else {
            return Err(format!("Unable to find the account {}", account_pubkey).into());
        }
    } else {
        let mut bytes = vec![1, 0, 0, 0, 1];
        bytes.extend_from_slice(authority_signer.pubkey().as_ref());
        let length = bytes.len();

        let results = rpc_client.get_program_accounts_with_config(
            &bpf_loader_upgradeable::id(),
            RpcProgramAccountsConfig {
                filters: Some(vec![RpcFilterType::Memcmp(Memcmp {
                    offset: 0,
                    bytes: MemcmpEncodedBytes::Binary(bs58::encode(bytes).into_string()),
                    encoding: None,
                })]),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    data_slice: Some(UiDataSliceConfig { offset: 0, length }),
                    ..RpcAccountInfoConfig::default()
                },
                ..RpcProgramAccountsConfig::default()
            },
        )?;

        for (address, account) in results.iter() {
            if close(
                rpc_client,
                config,
                address,
                &recipient_pubkey,
                authority_signer,
            )
            .is_ok()
            {
                if let Ok(UpgradeableLoaderState::Buffer { authority_address }) = account.state() {
                    buffers.push(CliUpgradeableBuffer {
                        address: address.to_string(),
                        authority: authority_address
                            .map(|address| address.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        data_len: 0,
                        lamports: account.lamports,
                        use_lamports_unit,
                    });
                } else {
                    return Err(format!("Error parsing account {}", address).into());
                }
            }
        }
    }
    Ok(config
        .output_format
        .formatted_string(&CliUpgradeableBuffers {
            buffers,
            use_lamports_unit,
        }))
}

/// Deploy using non-upgradeable loader
pub fn process_deploy(
    rpc_client: Arc<RpcClient>,
    config: &CliConfig,
    program_location: &str,
    buffer_signer_index: Option<SignerIndex>,
    use_deprecated_loader: bool,
    allow_excessive_balance: bool,
) -> ProcessResult {
    // Create ephemeral keypair to use for Buffer account, if not provided
    let (words, mnemonic, buffer_keypair) = create_ephemeral_keypair()?;
    let buffer_signer = if let Some(i) = buffer_signer_index {
        config.signers[i]
    } else {
        &buffer_keypair
    };

    let program_data = read_and_verify_elf(program_location)?;
    let minimum_balance = rpc_client.get_minimum_balance_for_rent_exemption(program_data.len())?;
    let loader_id = if use_deprecated_loader {
        bpf_loader_deprecated::id()
    } else {
        bpf_loader::id()
    };

    let result = do_process_program_write_and_deploy(
        rpc_client,
        config,
        &program_data,
        program_data.len(),
        program_data.len(),
        minimum_balance,
        &loader_id,
        Some(&[buffer_signer]),
        Some(buffer_signer),
        &buffer_signer.pubkey(),
        Some(buffer_signer),
        allow_excessive_balance,
    );
    if result.is_err() && buffer_signer_index.is_none() {
        report_ephemeral_mnemonic(words, mnemonic);
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn do_process_program_write_and_deploy(
    rpc_client: Arc<RpcClient>,
    config: &CliConfig,
    program_data: &[u8],
    buffer_data_len: usize,
    programdata_len: usize,
    minimum_balance: u64,
    loader_id: &Pubkey,
    program_signers: Option<&[&dyn Signer]>,
    buffer_signer: Option<&dyn Signer>,
    buffer_pubkey: &Pubkey,
    buffer_authority_signer: Option<&dyn Signer>,
    allow_excessive_balance: bool,
) -> ProcessResult {
    // Build messages to calculate fees
    let mut messages: Vec<&Message> = Vec::new();

    // Initialize buffer account or complete if already partially initialized
    let (initial_message, write_messages, balance_needed) =
        if let Some(buffer_authority_signer) = buffer_authority_signer {
            let (initial_instructions, balance_needed) = if let Some(account) = rpc_client
                .get_account_with_commitment(buffer_pubkey, config.commitment)?
                .value
            {
                complete_partial_program_init(
                    loader_id,
                    &config.signers[0].pubkey(),
                    buffer_pubkey,
                    &account,
                    if loader_id == &bpf_loader_upgradeable::id() {
                        UpgradeableLoaderState::buffer_len(buffer_data_len)?
                    } else {
                        buffer_data_len
                    },
                    minimum_balance,
                    allow_excessive_balance,
                )?
            } else if loader_id == &bpf_loader_upgradeable::id() {
                (
                    bpf_loader_upgradeable::create_buffer(
                        &config.signers[0].pubkey(),
                        buffer_pubkey,
                        &buffer_authority_signer.pubkey(),
                        minimum_balance,
                        buffer_data_len,
                    )?,
                    minimum_balance,
                )
            } else {
                (
                    vec![system_instruction::create_account(
                        &config.signers[0].pubkey(),
                        buffer_pubkey,
                        minimum_balance,
                        buffer_data_len as u64,
                        loader_id,
                    )],
                    minimum_balance,
                )
            };
            let initial_message = if !initial_instructions.is_empty() {
                Some(Message::new(
                    &initial_instructions,
                    Some(&config.signers[0].pubkey()),
                ))
            } else {
                None
            };

            // Create and add write messages

            let mut write_messages = vec![];
            for (chunk, i) in program_data.chunks(DATA_CHUNK_SIZE).zip(0..) {
                let instruction = if loader_id == &bpf_loader_upgradeable::id() {
                    bpf_loader_upgradeable::write(
                        buffer_pubkey,
                        &buffer_authority_signer.pubkey(),
                        (i * DATA_CHUNK_SIZE) as u32,
                        chunk.to_vec(),
                    )
                } else {
                    loader_instruction::write(
                        buffer_pubkey,
                        loader_id,
                        (i * DATA_CHUNK_SIZE) as u32,
                        chunk.to_vec(),
                    )
                };
                let message = Message::new(&[instruction], Some(&config.signers[0].pubkey()));
                write_messages.push(message);
            }

            (initial_message, Some(write_messages), balance_needed)
        } else {
            (None, None, 0)
        };

    if let Some(ref initial_message) = initial_message {
        messages.push(initial_message);
    }
    if let Some(ref write_messages) = write_messages {
        let mut write_message_refs = vec![];
        for message in write_messages.iter() {
            write_message_refs.push(message);
        }
        messages.append(&mut write_message_refs);
    }

    // Create and add final message

    let final_message = if let Some(program_signers) = program_signers {
        let message = if loader_id == &bpf_loader_upgradeable::id() {
            Message::new(
                &bpf_loader_upgradeable::deploy_with_max_program_len(
                    &config.signers[0].pubkey(),
                    &program_signers[0].pubkey(),
                    buffer_pubkey,
                    &program_signers[1].pubkey(),
                    rpc_client.get_minimum_balance_for_rent_exemption(
                        UpgradeableLoaderState::program_len()?,
                    )?,
                    programdata_len,
                )?,
                Some(&config.signers[0].pubkey()),
            )
        } else {
            Message::new(
                &[loader_instruction::finalize(buffer_pubkey, loader_id)],
                Some(&config.signers[0].pubkey()),
            )
        };
        Some(message)
    } else {
        None
    };
    if let Some(ref message) = final_message {
        messages.push(message);
    }

    check_payer(&rpc_client, config, balance_needed, &messages)?;

    send_deploy_messages(
        rpc_client,
        config,
        &initial_message,
        &write_messages,
        &final_message,
        buffer_signer,
        buffer_authority_signer,
        program_signers,
    )?;

    if let Some(program_signers) = program_signers {
        let program_id = CliProgramId {
            program_id: program_signers[0].pubkey().to_string(),
        };
        Ok(config.output_format.formatted_string(&program_id))
    } else {
        let buffer = CliProgramBuffer {
            buffer: buffer_pubkey.to_string(),
        };
        Ok(config.output_format.formatted_string(&buffer))
    }
}

fn do_process_program_upgrade(
    rpc_client: Arc<RpcClient>,
    config: &CliConfig,
    program_data: &[u8],
    program_id: &Pubkey,
    upgrade_authority: &dyn Signer,
    buffer_pubkey: &Pubkey,
    buffer_signer: Option<&dyn Signer>,
) -> ProcessResult {
    let loader_id = bpf_loader_upgradeable::id();
    let data_len = program_data.len();
    let minimum_balance = rpc_client.get_minimum_balance_for_rent_exemption(
        UpgradeableLoaderState::programdata_len(data_len)?,
    )?;

    // Build messages to calculate fees
    let mut messages: Vec<&Message> = Vec::new();

    let (initial_message, write_messages, balance_needed) =
        if let Some(buffer_signer) = buffer_signer {
            // Check Buffer account to see if partial initialization has occurred
            let (initial_instructions, balance_needed) = if let Some(account) = rpc_client
                .get_account_with_commitment(&buffer_signer.pubkey(), config.commitment)?
                .value
            {
                complete_partial_program_init(
                    &loader_id,
                    &config.signers[0].pubkey(),
                    &buffer_signer.pubkey(),
                    &account,
                    UpgradeableLoaderState::buffer_len(data_len)?,
                    minimum_balance,
                    true,
                )?
            } else {
                (
                    bpf_loader_upgradeable::create_buffer(
                        &config.signers[0].pubkey(),
                        buffer_pubkey,
                        &upgrade_authority.pubkey(),
                        minimum_balance,
                        data_len,
                    )?,
                    minimum_balance,
                )
            };

            let initial_message = if !initial_instructions.is_empty() {
                Some(Message::new(
                    &initial_instructions,
                    Some(&config.signers[0].pubkey()),
                ))
            } else {
                None
            };

            // Create and add write messages
            let mut write_messages = vec![];
            for (chunk, i) in program_data.chunks(DATA_CHUNK_SIZE).zip(0..) {
                let instruction = bpf_loader_upgradeable::write(
                    &buffer_signer.pubkey(),
                    &upgrade_authority.pubkey(),
                    (i * DATA_CHUNK_SIZE) as u32,
                    chunk.to_vec(),
                );
                let message = Message::new(&[instruction], Some(&config.signers[0].pubkey()));
                write_messages.push(message);
            }

            (initial_message, Some(write_messages), balance_needed)
        } else {
            (None, None, 0)
        };

    if let Some(ref message) = initial_message {
        messages.push(message);
    }
    if let Some(ref write_messages) = write_messages {
        let mut write_message_refs = vec![];
        for message in write_messages.iter() {
            write_message_refs.push(message);
        }
        messages.append(&mut write_message_refs);
    }

    // Create and add final message
    let final_message = Message::new(
        &[bpf_loader_upgradeable::upgrade(
            program_id,
            buffer_pubkey,
            &upgrade_authority.pubkey(),
            &config.signers[0].pubkey(),
        )],
        Some(&config.signers[0].pubkey()),
    );
    messages.push(&final_message);

    check_payer(&rpc_client, config, balance_needed, &messages)?;
    send_deploy_messages(
        rpc_client,
        config,
        &initial_message,
        &write_messages,
        &Some(final_message),
        buffer_signer,
        Some(upgrade_authority),
        Some(&[upgrade_authority]),
    )?;

    let program_id = CliProgramId {
        program_id: program_id.to_string(),
    };
    Ok(config.output_format.formatted_string(&program_id))
}

fn read_and_verify_elf(program_location: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut file = File::open(program_location)
        .map_err(|err| format!("Unable to open program file: {}", err))?;
    let mut program_data = Vec::new();
    file.read_to_end(&mut program_data)
        .map_err(|err| format!("Unable to read program file: {}", err))?;
    let mut invoke_context = MockInvokeContext::new(vec![]);

    // Verify the program
    <dyn Executable<BpfError, ThisInstructionMeter>>::from_elf(
        &program_data,
        Some(verifier::check),
        Config {
            reject_unresolved_syscalls: true,
            verify_mul64_imm_nonzero: true, // TODO: Remove me after feature gate
            ..Config::default()
        },
        register_syscalls(&mut invoke_context).unwrap(),
    )
    .map_err(|err| format!("ELF error: {}", err))?;

    Ok(program_data)
}

fn complete_partial_program_init(
    loader_id: &Pubkey,
    payer_pubkey: &Pubkey,
    elf_pubkey: &Pubkey,
    account: &Account,
    account_data_len: usize,
    minimum_balance: u64,
    allow_excessive_balance: bool,
) -> Result<(Vec<Instruction>, u64), Box<dyn std::error::Error>> {
    let mut instructions: Vec<Instruction> = vec![];
    let mut balance_needed = 0;
    if account.executable {
        return Err("Buffer account is already executable".into());
    }
    if account.owner != *loader_id && !system_program::check_id(&account.owner) {
        return Err("Buffer account is already owned by another account".into());
    }

    if account.data.is_empty() && system_program::check_id(&account.owner) {
        instructions.push(system_instruction::allocate(
            elf_pubkey,
            account_data_len as u64,
        ));
        if account.owner != *loader_id {
            instructions.push(system_instruction::assign(elf_pubkey, loader_id));
        }
    }
    if account.lamports < minimum_balance {
        let balance = minimum_balance - account.lamports;
        instructions.push(system_instruction::transfer(
            payer_pubkey,
            elf_pubkey,
            balance,
        ));
        balance_needed = balance;
    } else if account.lamports > minimum_balance
        && system_program::check_id(&account.owner)
        && !allow_excessive_balance
    {
        return Err(format!(
            "Buffer account has a balance: {:?}; it may already be in use",
            Sol(account.lamports)
        )
        .into());
    }
    Ok((instructions, balance_needed))
}

fn check_payer(
    rpc_client: &RpcClient,
    config: &CliConfig,
    balance_needed: u64,
    messages: &[&Message],
) -> Result<(), Box<dyn std::error::Error>> {
    let (_, fee_calculator) = rpc_client.get_recent_blockhash()?;

    // Does the payer have enough?
    check_account_for_spend_multiple_fees_with_commitment(
        rpc_client,
        &config.signers[0].pubkey(),
        balance_needed,
        &fee_calculator,
        messages,
        config.commitment,
    )?;
    Ok(())
}

fn send_deploy_messages(
    rpc_client: Arc<RpcClient>,
    config: &CliConfig,
    initial_message: &Option<Message>,
    write_messages: &Option<Vec<Message>>,
    final_message: &Option<Message>,
    initial_signer: Option<&dyn Signer>,
    write_signer: Option<&dyn Signer>,
    final_signers: Option<&[&dyn Signer]>,
) -> Result<(), Box<dyn std::error::Error>> {
    let payer_signer = config.signers[0];

    if let Some(message) = initial_message {
        if let Some(initial_signer) = initial_signer {
            trace!("Preparing the required accounts");
            let (blockhash, _) = rpc_client.get_recent_blockhash()?;

            let mut initial_transaction = Transaction::new_unsigned(message.clone());
            // Most of the initial_transaction combinations require both the fee-payer and new program
            // account to sign the transaction. One (transfer) only requires the fee-payer signature.
            // This check is to ensure signing does not fail on a KeypairPubkeyMismatch error from an
            // extraneous signature.
            if message.header.num_required_signatures == 2 {
                initial_transaction.try_sign(&[payer_signer, initial_signer], blockhash)?;
            } else {
                initial_transaction.try_sign(&[payer_signer], blockhash)?;
            }
            let result = rpc_client.send_and_confirm_transaction_with_spinner(&initial_transaction);
            log_instruction_custom_error::<SystemError>(result, config)
                .map_err(|err| format!("Account allocation failed: {}", err))?;
        } else {
            return Err("Buffer account not created yet, must provide a key pair".into());
        }
    }

    if let Some(write_messages) = write_messages {
        if let Some(write_signer) = write_signer {
            trace!("Writing program data");
            let (blockhash, _, last_valid_slot) = rpc_client
                .get_recent_blockhash_with_commitment(config.commitment)?
                .value;
            let mut write_transactions = vec![];
            for message in write_messages.iter() {
                let mut tx = Transaction::new_unsigned(message.clone());
                tx.try_sign(&[payer_signer, write_signer], blockhash)?;
                write_transactions.push(tx);
            }

            send_and_confirm_transactions_with_spinner(
                rpc_client.clone(),
                &config.websocket_url,
                write_transactions,
                &[payer_signer, write_signer],
                config.commitment,
                last_valid_slot,
            )
            .map_err(|err| format!("Data writes to account failed: {}", err))?;
        }
    }

    if let Some(message) = final_message {
        if let Some(final_signers) = final_signers {
            trace!("Deploying program");
            let (blockhash, _) = rpc_client.get_recent_blockhash()?;

            let mut final_tx = Transaction::new_unsigned(message.clone());
            let mut signers = final_signers.to_vec();
            signers.push(payer_signer);
            final_tx.try_sign(&signers, blockhash)?;
            rpc_client
                .send_and_confirm_transaction_with_spinner_and_config(
                    &final_tx,
                    config.commitment,
                    RpcSendTransactionConfig {
                        skip_preflight: true,
                        preflight_commitment: Some(config.commitment.commitment),
                        ..RpcSendTransactionConfig::default()
                    },
                )
                .map_err(|e| format!("Deploying program failed: {}", e))?;
        }
    }

    Ok(())
}

fn create_ephemeral_keypair(
) -> Result<(usize, bip39::Mnemonic, Keypair), Box<dyn std::error::Error>> {
    const WORDS: usize = 12;
    let mnemonic = Mnemonic::new(MnemonicType::for_word_count(WORDS)?, Language::English);
    let seed = Seed::new(&mnemonic, "");
    let new_keypair = keypair_from_seed(seed.as_bytes())?;

    Ok((WORDS, mnemonic, new_keypair))
}

fn report_ephemeral_mnemonic(words: usize, mnemonic: bip39::Mnemonic) {
    let phrase: &str = mnemonic.phrase();
    let divider = String::from_utf8(vec![b'='; phrase.len()]).unwrap();
    eprintln!(
        "{}\nRecover the intermediate account's ephemeral keypair file with",
        divider
    );
    eprintln!(
        "`solana-keygen recover` and the following {}-word seed phrase:",
        words
    );
    eprintln!("{}\n{}\n{}", divider, phrase, divider);
    eprintln!("To resume a deploy, pass the recovered keypair as");
    eprintln!("the [PROGRAM_ADDRESS_SIGNER] argument to `solana deploy` or");
    eprintln!("as the [BUFFER_SIGNER] to `solana program deploy` or `solana write-buffer'.");
    eprintln!("Or to recover the account's lamports, pass it as the");
    eprintln!(
        "[BUFFER_ACCOUNT_ADDRESS] argument to `solana program close`.\n{}",
        divider
    );
}

fn send_and_confirm_transactions_with_spinner<T: Signers>(
    rpc_client: Arc<RpcClient>,
    websocket_url: &str,
    mut transactions: Vec<Transaction>,
    signer_keys: &T,
    commitment: CommitmentConfig,
    mut last_valid_slot: Slot,
) -> Result<(), Box<dyn error::Error>> {
    let progress_bar = new_spinner_progress_bar();
    let mut send_retries = 5;

    progress_bar.set_message("Finding leader nodes...");
    let tpu_client = TpuClient::new(
        rpc_client.clone(),
        websocket_url,
        TpuClientConfig::default(),
    )?;
    loop {
        // Send all transactions
        let mut pending_transactions = HashMap::new();
        let num_transactions = transactions.len();
        for transaction in transactions {
            if !tpu_client.send_transaction(&transaction) {
                let _result = rpc_client
                    .send_transaction_with_config(
                        &transaction,
                        RpcSendTransactionConfig {
                            preflight_commitment: Some(commitment.commitment),
                            ..RpcSendTransactionConfig::default()
                        },
                    )
                    .ok();
            }
            pending_transactions.insert(transaction.signatures[0], transaction);
            progress_bar.set_message(format!(
                "[{}/{}] Transactions sent",
                pending_transactions.len(),
                num_transactions
            ));

            // Throttle transactions to about 100 TPS
            sleep(Duration::from_millis(10));
        }

        // Collect statuses for all the transactions, drop those that are confirmed
        loop {
            let mut slot = 0;
            let pending_signatures = pending_transactions.keys().cloned().collect::<Vec<_>>();
            for pending_signatures_chunk in
                pending_signatures.chunks(MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS)
            {
                if let Ok(result) = rpc_client.get_signature_statuses(pending_signatures_chunk) {
                    let statuses = result.value;
                    for (signature, status) in
                        pending_signatures_chunk.iter().zip(statuses.into_iter())
                    {
                        if let Some(status) = status {
                            if let Some(confirmation_status) = &status.confirmation_status {
                                if *confirmation_status != TransactionConfirmationStatus::Processed
                                {
                                    let _ = pending_transactions.remove(signature);
                                }
                            } else if status.confirmations.is_none()
                                || status.confirmations.unwrap() > 1
                            {
                                let _ = pending_transactions.remove(signature);
                            }
                        }
                    }
                }

                slot = rpc_client.get_slot()?;
                progress_bar.set_message(format!(
                    "[{}/{}] Transactions confirmed. Retrying in {} slots",
                    num_transactions - pending_transactions.len(),
                    num_transactions,
                    last_valid_slot.saturating_sub(slot)
                ));
            }

            if pending_transactions.is_empty() {
                return Ok(());
            }

            if slot > last_valid_slot {
                break;
            }

            for transaction in pending_transactions.values() {
                if !tpu_client.send_transaction(transaction) {
                    let _result = rpc_client
                        .send_transaction_with_config(
                            transaction,
                            RpcSendTransactionConfig {
                                preflight_commitment: Some(commitment.commitment),
                                ..RpcSendTransactionConfig::default()
                            },
                        )
                        .ok();
                }
            }

            if cfg!(not(test)) {
                // Retry twice a second
                sleep(Duration::from_millis(500));
            }
        }

        if send_retries == 0 {
            return Err("Transactions failed".into());
        }
        send_retries -= 1;

        // Re-sign any failed transactions with a new blockhash and retry
        let (blockhash, _fee_calculator, new_last_valid_slot) = rpc_client
            .get_recent_blockhash_with_commitment(commitment)?
            .value;
        last_valid_slot = new_last_valid_slot;
        transactions = vec![];
        for (_, mut transaction) in pending_transactions.into_iter() {
            transaction.try_sign(signer_keys, blockhash)?;
            transactions.push(transaction);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{app, parse_command, process_command};
    use serde_json::Value;
    use solana_cli_output::OutputFormat;
    use solana_sdk::signature::write_keypair_file;

    fn make_tmp_path(name: &str) -> String {
        let out_dir = std::env::var("FARF_DIR").unwrap_or_else(|_| "farf".to_string());
        let keypair = Keypair::new();

        let path = format!("{}/tmp/{}-{}", out_dir, name, keypair.pubkey());

        // whack any possible collision
        let _ignored = std::fs::remove_dir_all(&path);
        // whack any possible collision
        let _ignored = std::fs::remove_file(&path);

        path
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_cli_parse_deploy() {
        let test_commands = app("test", "desc", "version");

        let default_keypair = Keypair::new();
        let keypair_file = make_tmp_path("keypair_file");
        write_keypair_file(&default_keypair, &keypair_file).unwrap();
        let default_signer = DefaultSigner::new("", &keypair_file);

        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "deploy",
            "/Users/test/program.so",
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Deploy {
                    program_location: Some("/Users/test/program.so".to_string()),
                    buffer_signer_index: None,
                    buffer_pubkey: None,
                    program_signer_index: None,
                    program_pubkey: None,
                    upgrade_authority_signer_index: 0,
                    is_final: false,
                    max_len: None,
                    allow_excessive_balance: false,
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "deploy",
            "/Users/test/program.so",
            "--max-len",
            "42",
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Deploy {
                    program_location: Some("/Users/test/program.so".to_string()),
                    buffer_signer_index: None,
                    buffer_pubkey: None,
                    program_signer_index: None,
                    program_pubkey: None,
                    upgrade_authority_signer_index: 0,
                    is_final: false,
                    max_len: Some(42),
                    allow_excessive_balance: false,
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        let buffer_keypair = Keypair::new();
        let buffer_keypair_file = make_tmp_path("buffer_keypair_file");
        write_keypair_file(&buffer_keypair, &buffer_keypair_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "deploy",
            "--buffer",
            &buffer_keypair_file,
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Deploy {
                    program_location: None,
                    buffer_signer_index: Some(1),
                    buffer_pubkey: Some(buffer_keypair.pubkey()),
                    program_signer_index: None,
                    program_pubkey: None,
                    upgrade_authority_signer_index: 0,
                    is_final: false,
                    max_len: None,
                    allow_excessive_balance: false,
                }),
                signers: vec![
                    read_keypair_file(&keypair_file).unwrap().into(),
                    read_keypair_file(&buffer_keypair_file).unwrap().into(),
                ],
            }
        );

        let program_pubkey = Pubkey::new_unique();
        let test = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "deploy",
            "/Users/test/program.so",
            "--program-id",
            &program_pubkey.to_string(),
        ]);
        assert_eq!(
            parse_command(&test, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Deploy {
                    program_location: Some("/Users/test/program.so".to_string()),
                    buffer_signer_index: None,
                    buffer_pubkey: None,
                    program_signer_index: None,
                    program_pubkey: Some(program_pubkey),
                    upgrade_authority_signer_index: 0,
                    is_final: false,
                    max_len: None,
                    allow_excessive_balance: false,
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        let program_keypair = Keypair::new();
        let program_keypair_file = make_tmp_path("program_keypair_file");
        write_keypair_file(&program_keypair, &program_keypair_file).unwrap();
        let test = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "deploy",
            "/Users/test/program.so",
            "--program-id",
            &program_keypair_file,
        ]);
        assert_eq!(
            parse_command(&test, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Deploy {
                    program_location: Some("/Users/test/program.so".to_string()),
                    buffer_signer_index: None,
                    buffer_pubkey: None,
                    program_signer_index: Some(1),
                    program_pubkey: Some(program_keypair.pubkey()),
                    upgrade_authority_signer_index: 0,
                    is_final: false,
                    max_len: None,
                    allow_excessive_balance: false,
                }),
                signers: vec![
                    read_keypair_file(&keypair_file).unwrap().into(),
                    read_keypair_file(&program_keypair_file).unwrap().into(),
                ],
            }
        );

        let authority_keypair = Keypair::new();
        let authority_keypair_file = make_tmp_path("authority_keypair_file");
        write_keypair_file(&authority_keypair, &authority_keypair_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "deploy",
            "/Users/test/program.so",
            "--upgrade-authority",
            &authority_keypair_file,
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Deploy {
                    program_location: Some("/Users/test/program.so".to_string()),
                    buffer_signer_index: None,
                    buffer_pubkey: None,
                    program_signer_index: None,
                    program_pubkey: None,
                    upgrade_authority_signer_index: 1,
                    is_final: false,
                    max_len: None,
                    allow_excessive_balance: false,
                }),
                signers: vec![
                    read_keypair_file(&keypair_file).unwrap().into(),
                    read_keypair_file(&authority_keypair_file).unwrap().into(),
                ],
            }
        );

        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "deploy",
            "/Users/test/program.so",
            "--final",
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Deploy {
                    program_location: Some("/Users/test/program.so".to_string()),
                    buffer_signer_index: None,
                    buffer_pubkey: None,
                    program_signer_index: None,
                    program_pubkey: None,
                    upgrade_authority_signer_index: 0,
                    is_final: true,
                    max_len: None,
                    allow_excessive_balance: false,
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_cli_parse_write_buffer() {
        let test_commands = app("test", "desc", "version");

        let default_keypair = Keypair::new();
        let keypair_file = make_tmp_path("keypair_file");
        write_keypair_file(&default_keypair, &keypair_file).unwrap();
        let default_signer = DefaultSigner::new("", &keypair_file);

        // defaults
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "write-buffer",
            "/Users/test/program.so",
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::WriteBuffer {
                    program_location: "/Users/test/program.so".to_string(),
                    buffer_signer_index: None,
                    buffer_pubkey: None,
                    buffer_authority_signer_index: Some(0),
                    max_len: None,
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        // specify max len
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "write-buffer",
            "/Users/test/program.so",
            "--max-len",
            "42",
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::WriteBuffer {
                    program_location: "/Users/test/program.so".to_string(),
                    buffer_signer_index: None,
                    buffer_pubkey: None,
                    buffer_authority_signer_index: Some(0),
                    max_len: Some(42),
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        // specify buffer
        let buffer_keypair = Keypair::new();
        let buffer_keypair_file = make_tmp_path("buffer_keypair_file");
        write_keypair_file(&buffer_keypair, &buffer_keypair_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "write-buffer",
            "/Users/test/program.so",
            "--buffer",
            &buffer_keypair_file,
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::WriteBuffer {
                    program_location: "/Users/test/program.so".to_string(),
                    buffer_signer_index: Some(1),
                    buffer_pubkey: Some(buffer_keypair.pubkey()),
                    buffer_authority_signer_index: Some(0),
                    max_len: None,
                }),
                signers: vec![
                    read_keypair_file(&keypair_file).unwrap().into(),
                    read_keypair_file(&buffer_keypair_file).unwrap().into(),
                ],
            }
        );

        // specify authority
        let authority_keypair = Keypair::new();
        let authority_keypair_file = make_tmp_path("authority_keypair_file");
        write_keypair_file(&authority_keypair, &authority_keypair_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "write-buffer",
            "/Users/test/program.so",
            "--buffer-authority",
            &authority_keypair_file,
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::WriteBuffer {
                    program_location: "/Users/test/program.so".to_string(),
                    buffer_signer_index: None,
                    buffer_pubkey: None,
                    buffer_authority_signer_index: Some(1),
                    max_len: None,
                }),
                signers: vec![
                    read_keypair_file(&keypair_file).unwrap().into(),
                    read_keypair_file(&authority_keypair_file).unwrap().into(),
                ],
            }
        );

        // specify both buffer and authority
        let buffer_keypair = Keypair::new();
        let buffer_keypair_file = make_tmp_path("buffer_keypair_file");
        write_keypair_file(&buffer_keypair, &buffer_keypair_file).unwrap();
        let authority_keypair = Keypair::new();
        let authority_keypair_file = make_tmp_path("authority_keypair_file");
        write_keypair_file(&authority_keypair, &authority_keypair_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "write-buffer",
            "/Users/test/program.so",
            "--buffer",
            &buffer_keypair_file,
            "--buffer-authority",
            &authority_keypair_file,
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::WriteBuffer {
                    program_location: "/Users/test/program.so".to_string(),
                    buffer_signer_index: Some(1),
                    buffer_pubkey: Some(buffer_keypair.pubkey()),
                    buffer_authority_signer_index: Some(2),
                    max_len: None,
                }),
                signers: vec![
                    read_keypair_file(&keypair_file).unwrap().into(),
                    read_keypair_file(&buffer_keypair_file).unwrap().into(),
                    read_keypair_file(&authority_keypair_file).unwrap().into(),
                ],
            }
        );
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_cli_parse_set_upgrade_authority() {
        let test_commands = app("test", "desc", "version");

        let default_keypair = Keypair::new();
        let keypair_file = make_tmp_path("keypair_file");
        write_keypair_file(&default_keypair, &keypair_file).unwrap();
        let default_signer = DefaultSigner::new("", &keypair_file);

        let program_pubkey = Pubkey::new_unique();
        let new_authority_pubkey = Pubkey::new_unique();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "set-upgrade-authority",
            &program_pubkey.to_string(),
            "--new-upgrade-authority",
            &new_authority_pubkey.to_string(),
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::SetUpgradeAuthority {
                    program_pubkey,
                    upgrade_authority_index: Some(0),
                    new_upgrade_authority: Some(new_authority_pubkey),
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        let program_pubkey = Pubkey::new_unique();
        let new_authority_pubkey = Keypair::new();
        let new_authority_pubkey_file = make_tmp_path("authority_keypair_file");
        write_keypair_file(&new_authority_pubkey, &new_authority_pubkey_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "set-upgrade-authority",
            &program_pubkey.to_string(),
            "--new-upgrade-authority",
            &new_authority_pubkey_file,
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::SetUpgradeAuthority {
                    program_pubkey,
                    upgrade_authority_index: Some(0),
                    new_upgrade_authority: Some(new_authority_pubkey.pubkey()),
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        let program_pubkey = Pubkey::new_unique();
        let new_authority_pubkey = Keypair::new();
        let new_authority_pubkey_file = make_tmp_path("authority_keypair_file");
        write_keypair_file(&new_authority_pubkey, &new_authority_pubkey_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "set-upgrade-authority",
            &program_pubkey.to_string(),
            "--final",
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::SetUpgradeAuthority {
                    program_pubkey,
                    upgrade_authority_index: Some(0),
                    new_upgrade_authority: None,
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        let program_pubkey = Pubkey::new_unique();
        let authority = Keypair::new();
        let authority_keypair_file = make_tmp_path("authority_keypair_file");
        write_keypair_file(&authority, &authority_keypair_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "set-upgrade-authority",
            &program_pubkey.to_string(),
            "--upgrade-authority",
            &authority_keypair_file,
            "--final",
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::SetUpgradeAuthority {
                    program_pubkey,
                    upgrade_authority_index: Some(1),
                    new_upgrade_authority: None,
                }),
                signers: vec![
                    read_keypair_file(&keypair_file).unwrap().into(),
                    read_keypair_file(&authority_keypair_file).unwrap().into(),
                ],
            }
        );
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_cli_parse_set_buffer_authority() {
        let test_commands = app("test", "desc", "version");

        let default_keypair = Keypair::new();
        let keypair_file = make_tmp_path("keypair_file");
        write_keypair_file(&default_keypair, &keypair_file).unwrap();
        let default_signer = DefaultSigner::new("", &keypair_file);

        let buffer_pubkey = Pubkey::new_unique();
        let new_authority_pubkey = Pubkey::new_unique();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "set-buffer-authority",
            &buffer_pubkey.to_string(),
            "--new-buffer-authority",
            &new_authority_pubkey.to_string(),
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::SetBufferAuthority {
                    buffer_pubkey,
                    buffer_authority_index: Some(0),
                    new_buffer_authority: new_authority_pubkey,
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        let buffer_pubkey = Pubkey::new_unique();
        let new_authority_keypair = Keypair::new();
        let new_authority_keypair_file = make_tmp_path("authority_keypair_file");
        write_keypair_file(&new_authority_keypair, &new_authority_keypair_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "set-buffer-authority",
            &buffer_pubkey.to_string(),
            "--new-buffer-authority",
            &new_authority_keypair_file,
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::SetBufferAuthority {
                    buffer_pubkey,
                    buffer_authority_index: Some(0),
                    new_buffer_authority: new_authority_keypair.pubkey(),
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_cli_parse_show() {
        let test_commands = app("test", "desc", "version");

        let default_keypair = Keypair::new();
        let keypair_file = make_tmp_path("keypair_file");
        write_keypair_file(&default_keypair, &keypair_file).unwrap();
        let default_signer = DefaultSigner::new("", &keypair_file);

        // defaults
        let buffer_pubkey = Pubkey::new_unique();
        let authority_keypair = Keypair::new();
        let authority_keypair_file = make_tmp_path("authority_keypair_file");
        write_keypair_file(&authority_keypair, &authority_keypair_file).unwrap();

        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "show",
            &buffer_pubkey.to_string(),
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Show {
                    account_pubkey: Some(buffer_pubkey),
                    authority_pubkey: default_keypair.pubkey(),
                    all: false,
                    use_lamports_unit: false,
                }),
                signers: vec![],
            }
        );

        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "show",
            "--buffers",
            "--all",
            "--lamports",
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Show {
                    account_pubkey: None,
                    authority_pubkey: default_keypair.pubkey(),
                    all: true,
                    use_lamports_unit: true,
                }),
                signers: vec![],
            }
        );

        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "show",
            "--buffers",
            "--buffer-authority",
            &authority_keypair.pubkey().to_string(),
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Show {
                    account_pubkey: None,
                    authority_pubkey: authority_keypair.pubkey(),
                    all: false,
                    use_lamports_unit: false,
                }),
                signers: vec![],
            }
        );

        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "show",
            "--buffers",
            "--buffer-authority",
            &authority_keypair_file,
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Show {
                    account_pubkey: None,
                    authority_pubkey: authority_keypair.pubkey(),
                    all: false,
                    use_lamports_unit: false,
                }),
                signers: vec![],
            }
        );
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_cli_parse_close() {
        let test_commands = app("test", "desc", "version");

        let default_keypair = Keypair::new();
        let keypair_file = make_tmp_path("keypair_file");
        write_keypair_file(&default_keypair, &keypair_file).unwrap();
        let default_signer = DefaultSigner::new("", &keypair_file);

        // defaults
        let buffer_pubkey = Pubkey::new_unique();
        let recipient_pubkey = Pubkey::new_unique();
        let authority_keypair = Keypair::new();
        let authority_keypair_file = make_tmp_path("authority_keypair_file");

        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "close",
            &buffer_pubkey.to_string(),
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Close {
                    account_pubkey: Some(buffer_pubkey),
                    recipient_pubkey: default_keypair.pubkey(),
                    authority_index: 0,
                    use_lamports_unit: false,
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        // with authority
        write_keypair_file(&authority_keypair, &authority_keypair_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "close",
            &buffer_pubkey.to_string(),
            "--buffer-authority",
            &authority_keypair_file,
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Close {
                    account_pubkey: Some(buffer_pubkey),
                    recipient_pubkey: default_keypair.pubkey(),
                    authority_index: 1,
                    use_lamports_unit: false,
                }),
                signers: vec![
                    read_keypair_file(&keypair_file).unwrap().into(),
                    read_keypair_file(&authority_keypair_file).unwrap().into(),
                ],
            }
        );

        // with recipient
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "close",
            &buffer_pubkey.to_string(),
            "--recipient",
            &recipient_pubkey.to_string(),
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Close {
                    account_pubkey: Some(buffer_pubkey),
                    recipient_pubkey,
                    authority_index: 0,
                    use_lamports_unit: false,
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into(),],
            }
        );

        // --buffers and lamports
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "program",
            "close",
            "--buffers",
            "--lamports",
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Program(ProgramCliCommand::Close {
                    account_pubkey: None,
                    recipient_pubkey: default_keypair.pubkey(),
                    authority_index: 0,
                    use_lamports_unit: true,
                }),
                signers: vec![read_keypair_file(&keypair_file).unwrap().into(),],
            }
        );
    }

    #[test]
    fn test_cli_keypair_file() {
        solana_logger::setup();

        let default_keypair = Keypair::new();
        let program_pubkey = Keypair::new();
        let deploy_path = make_tmp_path("deploy");
        let mut program_location = PathBuf::from(deploy_path.clone());
        program_location.push("noop");
        program_location.set_extension("so");
        let mut pathbuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pathbuf.push("tests");
        pathbuf.push("fixtures");
        pathbuf.push("noop");
        pathbuf.set_extension("so");
        let program_keypair_location = program_location.with_file_name("noop-keypair.json");
        std::fs::create_dir_all(deploy_path).unwrap();
        std::fs::copy(pathbuf, program_location.as_os_str()).unwrap();
        write_keypair_file(&program_pubkey, &program_keypair_location).unwrap();

        let config = CliConfig {
            rpc_client: Some(Arc::new(RpcClient::new_mock("".to_string()))),
            command: CliCommand::Program(ProgramCliCommand::Deploy {
                program_location: Some(program_location.to_str().unwrap().to_string()),
                buffer_signer_index: None,
                buffer_pubkey: None,
                program_signer_index: None,
                program_pubkey: None,
                upgrade_authority_signer_index: 0,
                is_final: false,
                max_len: None,
                allow_excessive_balance: false,
            }),
            signers: vec![&default_keypair],
            output_format: OutputFormat::JsonCompact,
            ..CliConfig::default()
        };

        let result = process_command(&config);
        let json: Value = serde_json::from_str(&result.unwrap()).unwrap();
        let program_id = json
            .as_object()
            .unwrap()
            .get("programId")
            .unwrap()
            .as_str()
            .unwrap();

        assert_eq!(
            program_id.parse::<Pubkey>().unwrap(),
            program_pubkey.pubkey()
        );
    }
}
