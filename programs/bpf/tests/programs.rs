#![cfg(any(feature = "bpf_c", feature = "bpf_rust"))]

#[macro_use]
extern crate solana_bpf_loader_program;

use itertools::izip;
use log::{log_enabled, trace, Level::Trace};
use solana_account_decoder::parse_bpf_loader::{
    parse_bpf_upgradeable_loader, BpfUpgradeableLoaderAccountType,
};
use solana_bpf_loader_program::{
    create_vm,
    serialization::{deserialize_parameters, serialize_parameters},
    syscalls::register_syscalls,
    BpfError, ThisInstructionMeter,
};
use solana_cli_output::display::println_transaction;
use solana_rbpf::{
    static_analysis::Analysis,
    vm::{Config, Executable, Tracer},
};
use solana_runtime::{
    bank::{Bank, ExecuteTimings, NonceRollbackInfo, TransactionBalancesSet, TransactionResults},
    bank_client::BankClient,
    genesis_utils::{create_genesis_config, GenesisConfigInfo},
    loader_utils::{
        load_buffer_account, load_program, load_upgradeable_program, set_upgrade_authority,
        upgrade_program,
    },
};
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    account_utils::StateMut,
    bpf_loader, bpf_loader_deprecated, bpf_loader_upgradeable,
    client::SyncClient,
    clock::MAX_PROCESSING_AGE,
    entrypoint::{MAX_PERMITTED_DATA_INCREASE, SUCCESS},
    instruction::{AccountMeta, CompiledInstruction, Instruction, InstructionError},
    keyed_account::KeyedAccount,
    message::Message,
    process_instruction::{InvokeContext, MockInvokeContext},
    pubkey::Pubkey,
    signature::{keypair_from_seed, Keypair, Signer},
    system_instruction,
    sysvar::{clock, fees, rent},
    transaction::{Transaction, TransactionError},
};
use solana_transaction_status::{
    token_balances::collect_token_balances, ConfirmedTransaction, InnerInstructions,
    TransactionStatusMeta, TransactionWithStatusMeta, UiTransactionEncoding,
};
use std::{
    cell::RefCell, collections::HashMap, env, fs::File, io::Read, path::PathBuf, str::FromStr,
    sync::Arc,
};

/// BPF program file extension
const PLATFORM_FILE_EXTENSION_BPF: &str = "so";

/// Create a BPF program file name
fn create_bpf_path(name: &str) -> PathBuf {
    let mut pathbuf = {
        let current_exe = env::current_exe().unwrap();
        PathBuf::from(current_exe.parent().unwrap().parent().unwrap())
    };
    pathbuf.push("bpf/");
    pathbuf.push(name);
    pathbuf.set_extension(PLATFORM_FILE_EXTENSION_BPF);
    pathbuf
}

fn load_bpf_program(
    bank_client: &BankClient,
    loader_id: &Pubkey,
    payer_keypair: &Keypair,
    name: &str,
) -> Pubkey {
    let elf = read_bpf_program(name);
    load_program(bank_client, payer_keypair, loader_id, elf)
}

fn read_bpf_program(name: &str) -> Vec<u8> {
    let path = create_bpf_path(name);
    let mut file = File::open(&path).unwrap_or_else(|err| {
        panic!("Failed to open {}: {}", path.display(), err);
    });
    let mut elf = Vec::new();
    file.read_to_end(&mut elf).unwrap();

    elf
}

#[cfg(feature = "bpf_rust")]
fn write_bpf_program(
    bank_client: &BankClient,
    loader_id: &Pubkey,
    payer_keypair: &Keypair,
    program_keypair: &Keypair,
    elf: &[u8],
) {
    use solana_sdk::loader_instruction;

    let chunk_size = 256; // Size of chunk just needs to fit into tx
    let mut offset = 0;
    for chunk in elf.chunks(chunk_size) {
        let instruction =
            loader_instruction::write(&program_keypair.pubkey(), loader_id, offset, chunk.to_vec());
        let message = Message::new(&[instruction], Some(&payer_keypair.pubkey()));

        bank_client
            .send_and_confirm_message(&[payer_keypair, &program_keypair], message)
            .unwrap();

        offset += chunk_size as u32;
    }
}

fn load_upgradeable_bpf_program(
    bank_client: &BankClient,
    payer_keypair: &Keypair,
    buffer_keypair: &Keypair,
    executable_keypair: &Keypair,
    authority_keypair: &Keypair,
    name: &str,
) {
    let path = create_bpf_path(name);
    let mut file = File::open(&path).unwrap_or_else(|err| {
        panic!("Failed to open {}: {}", path.display(), err);
    });
    let mut elf = Vec::new();
    file.read_to_end(&mut elf).unwrap();
    load_upgradeable_program(
        bank_client,
        payer_keypair,
        buffer_keypair,
        executable_keypair,
        authority_keypair,
        elf,
    );
}

fn load_upgradeable_buffer(
    bank_client: &BankClient,
    payer_keypair: &Keypair,
    buffer_keypair: &Keypair,
    buffer_authority_keypair: &Keypair,
    name: &str,
) {
    let path = create_bpf_path(name);
    let mut file = File::open(&path).unwrap_or_else(|err| {
        panic!("Failed to open {}: {}", path.display(), err);
    });
    let mut elf = Vec::new();
    file.read_to_end(&mut elf).unwrap();
    load_buffer_account(
        bank_client,
        payer_keypair,
        &buffer_keypair,
        buffer_authority_keypair,
        &elf,
    );
}

fn upgrade_bpf_program(
    bank_client: &BankClient,
    payer_keypair: &Keypair,
    buffer_keypair: &Keypair,
    executable_pubkey: &Pubkey,
    authority_keypair: &Keypair,
    name: &str,
) {
    load_upgradeable_buffer(
        bank_client,
        payer_keypair,
        buffer_keypair,
        authority_keypair,
        name,
    );
    upgrade_program(
        bank_client,
        payer_keypair,
        executable_pubkey,
        &buffer_keypair.pubkey(),
        &authority_keypair,
        &payer_keypair.pubkey(),
    );
}

fn run_program(
    name: &str,
    program_id: &Pubkey,
    parameter_accounts: Vec<KeyedAccount>,
    instruction_data: &[u8],
) -> Result<u64, InstructionError> {
    let path = create_bpf_path(name);
    let mut file = File::open(path).unwrap();

    let mut data = vec![];
    file.read_to_end(&mut data).unwrap();
    let loader_id = bpf_loader::id();
    let parameter_bytes = serialize_parameters(
        &bpf_loader::id(),
        program_id,
        &parameter_accounts,
        &instruction_data,
    )
    .unwrap();
    let mut invoke_context = MockInvokeContext::new(parameter_accounts);
    let compute_meter = invoke_context.get_compute_meter();
    let mut instruction_meter = ThisInstructionMeter { compute_meter };

    let config = Config {
        enable_instruction_tracing: true,
        ..Config::default()
    };
    let mut executable = <dyn Executable<BpfError, ThisInstructionMeter>>::from_elf(
        &data,
        None,
        config,
        register_syscalls(&mut invoke_context).unwrap(),
    )
    .unwrap();
    executable.jit_compile().unwrap();

    let mut instruction_count = 0;
    let mut tracer = None;
    for i in 0..2 {
        let mut parameter_bytes = parameter_bytes.clone();
        {
            let mut vm = create_vm(
                &loader_id,
                executable.as_ref(),
                parameter_bytes.as_slice_mut(),
                &mut invoke_context,
            )
            .unwrap();
            let result = if i == 0 {
                vm.execute_program_interpreted(&mut instruction_meter)
            } else {
                vm.execute_program_jit(&mut instruction_meter)
            };
            assert_eq!(SUCCESS, result.unwrap());
            if i == 1 {
                assert_eq!(instruction_count, vm.get_total_instruction_count());
            }
            instruction_count = vm.get_total_instruction_count();
            if config.enable_instruction_tracing {
                if i == 1 {
                    if !Tracer::compare(tracer.as_ref().unwrap(), vm.get_tracer()) {
                        let analysis = Analysis::from_executable(executable.as_ref());
                        let stdout = std::io::stdout();
                        println!("TRACE (interpreted):");
                        tracer
                            .as_ref()
                            .unwrap()
                            .write(&mut stdout.lock(), &analysis)
                            .unwrap();
                        println!("TRACE (jit):");
                        vm.get_tracer()
                            .write(&mut stdout.lock(), &analysis)
                            .unwrap();
                        assert!(false);
                    } else if log_enabled!(Trace) {
                        let analysis = Analysis::from_executable(executable.as_ref());
                        let mut trace_buffer = Vec::<u8>::new();
                        tracer
                            .as_ref()
                            .unwrap()
                            .write(&mut trace_buffer, &analysis)
                            .unwrap();
                        let trace_string = String::from_utf8(trace_buffer).unwrap();
                        trace!("BPF Program Instruction Trace:\n{}", trace_string);
                    }
                }
                tracer = Some(vm.get_tracer().clone());
            }
        }
        let parameter_accounts = invoke_context.get_keyed_accounts().unwrap();
        deserialize_parameters(
            &bpf_loader::id(),
            parameter_accounts,
            parameter_bytes.as_slice(),
        )
        .unwrap();
    }

    Ok(instruction_count)
}

fn process_transaction_and_record_inner(
    bank: &Bank,
    tx: Transaction,
) -> (Result<(), TransactionError>, Vec<Vec<CompiledInstruction>>) {
    let signature = tx.signatures.get(0).unwrap().clone();
    let txs = vec![tx];
    let tx_batch = bank.prepare_batch(txs.iter()).unwrap();
    let (mut results, _, mut inner_instructions, _transaction_logs) = bank
        .load_execute_and_commit_transactions(
            &tx_batch,
            MAX_PROCESSING_AGE,
            false,
            true,
            false,
            &mut ExecuteTimings::default(),
        );
    let result = results
        .fee_collection_results
        .swap_remove(0)
        .and_then(|_| bank.get_signature_status(&signature).unwrap());
    (
        result,
        inner_instructions
            .swap_remove(0)
            .expect("cpi recording should be enabled"),
    )
}

fn execute_transactions(bank: &Bank, txs: &[Transaction]) -> Vec<ConfirmedTransaction> {
    let batch = bank.prepare_batch(txs.iter()).unwrap();
    let mut timings = ExecuteTimings::default();
    let mut mint_decimals = HashMap::new();
    let tx_pre_token_balances = collect_token_balances(&bank, &batch, &mut mint_decimals);
    let (
        TransactionResults {
            execution_results, ..
        },
        TransactionBalancesSet {
            pre_balances,
            post_balances,
            ..
        },
        inner_instructions,
        transaction_logs,
    ) = bank.load_execute_and_commit_transactions(
        &batch,
        std::usize::MAX,
        true,
        true,
        true,
        &mut timings,
    );
    let tx_post_token_balances = collect_token_balances(&bank, &batch, &mut mint_decimals);

    izip!(
        txs.iter(),
        execution_results.into_iter(),
        inner_instructions.into_iter(),
        pre_balances.into_iter(),
        post_balances.into_iter(),
        tx_pre_token_balances.into_iter(),
        tx_post_token_balances.into_iter(),
        transaction_logs.into_iter(),
    )
    .map(
        |(
            tx,
            (execute_result, nonce_rollback),
            inner_instructions,
            pre_balances,
            post_balances,
            pre_token_balances,
            post_token_balances,
            log_messages,
        )| {
            let fee_calculator = nonce_rollback
                .map(|nonce_rollback| nonce_rollback.fee_calculator())
                .unwrap_or_else(|| bank.get_fee_calculator(&tx.message().recent_blockhash))
                .expect("FeeCalculator must exist");
            let fee = fee_calculator.calculate_fee(tx.message());

            let inner_instructions = inner_instructions.map(|inner_instructions| {
                inner_instructions
                    .into_iter()
                    .enumerate()
                    .map(|(index, instructions)| InnerInstructions {
                        index: index as u8,
                        instructions,
                    })
                    .filter(|i| !i.instructions.is_empty())
                    .collect()
            });

            let tx_status_meta = TransactionStatusMeta {
                status: execute_result,
                fee,
                pre_balances,
                post_balances,
                pre_token_balances: Some(pre_token_balances),
                post_token_balances: Some(post_token_balances),
                inner_instructions,
                log_messages,
                rewards: None,
            };

            ConfirmedTransaction {
                slot: bank.slot(),
                transaction: TransactionWithStatusMeta {
                    transaction: tx.clone(),
                    meta: Some(tx_status_meta),
                },
                block_time: None,
            }
        },
    )
    .collect()
}

fn print_confirmed_tx(name: &str, confirmed_tx: ConfirmedTransaction) {
    let block_time = confirmed_tx.block_time;
    let tx = confirmed_tx.transaction.transaction.clone();
    let encoded = confirmed_tx.encode(UiTransactionEncoding::JsonParsed);
    println!("EXECUTE {} (slot {})", name, encoded.slot);
    println_transaction(&tx, &encoded.transaction.meta, "  ", None, block_time);
}

#[test]
#[cfg(any(feature = "bpf_c", feature = "bpf_rust"))]
fn test_program_bpf_sanity() {
    solana_logger::setup();

    let mut programs = Vec::new();
    #[cfg(feature = "bpf_c")]
    {
        programs.extend_from_slice(&[
            ("alloc", true),
            ("bpf_to_bpf", true),
            ("float", true),
            ("multiple_static", true),
            ("noop", true),
            ("noop++", true),
            ("panic", false),
            ("relative_call", true),
            ("sanity", true),
            ("sanity++", true),
            ("secp256k1_recover", true),
            ("sha", true),
            ("struct_pass", true),
            ("struct_ret", true),
        ]);
    }
    #[cfg(feature = "bpf_rust")]
    {
        programs.extend_from_slice(&[
            ("solana_bpf_rust_128bit", true),
            ("solana_bpf_rust_alloc", true),
            ("solana_bpf_rust_custom_heap", true),
            ("solana_bpf_rust_dep_crate", true),
            ("solana_bpf_rust_external_spend", false),
            ("solana_bpf_rust_iter", true),
            ("solana_bpf_rust_many_args", true),
            ("solana_bpf_rust_membuiltins", true),
            ("solana_bpf_rust_noop", true),
            ("solana_bpf_rust_panic", false),
            ("solana_bpf_rust_param_passing", true),
            ("solana_bpf_rust_rand", true),
            ("solana_bpf_rust_sanity", true),
            ("solana_bpf_rust_secp256k1_recover", true),
            ("solana_bpf_rust_sha", true),
        ]);
    }

    for program in programs.iter() {
        println!("Test program: {:?}", program.0);

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(50);

        let mut bank = Bank::new(&genesis_config);
        let (name, id, entrypoint) = solana_bpf_loader_program!();
        bank.add_builtin(&name, id, entrypoint);
        let bank_client = BankClient::new(bank);

        // Call user program
        let program_id =
            load_bpf_program(&bank_client, &bpf_loader::id(), &mint_keypair, program.0);
        let account_metas = vec![
            AccountMeta::new(mint_keypair.pubkey(), true),
            AccountMeta::new(Keypair::new().pubkey(), false),
        ];
        let instruction = Instruction::new_with_bytes(program_id, &[1], account_metas);
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        if program.1 {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
    }
}

#[test]
#[cfg(any(feature = "bpf_c", feature = "bpf_rust"))]
fn test_program_bpf_loader_deprecated() {
    solana_logger::setup();

    let mut programs = Vec::new();
    #[cfg(feature = "bpf_c")]
    {
        programs.extend_from_slice(&[("deprecated_loader")]);
    }
    #[cfg(feature = "bpf_rust")]
    {
        programs.extend_from_slice(&[("solana_bpf_rust_deprecated_loader")]);
    }

    for program in programs.iter() {
        println!("Test program: {:?}", program);

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(50);
        let mut bank = Bank::new(&genesis_config);
        let (name, id, entrypoint) = solana_bpf_loader_deprecated_program!();
        bank.add_builtin(&name, id, entrypoint);
        let bank_client = BankClient::new(bank);

        let program_id = load_bpf_program(
            &bank_client,
            &bpf_loader_deprecated::id(),
            &mint_keypair,
            program,
        );
        let account_metas = vec![AccountMeta::new(mint_keypair.pubkey(), true)];
        let instruction = Instruction::new_with_bytes(program_id, &[1], account_metas);
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        assert!(result.is_ok());
    }
}

#[test]
fn test_program_bpf_duplicate_accounts() {
    solana_logger::setup();

    let mut programs = Vec::new();
    #[cfg(feature = "bpf_c")]
    {
        programs.extend_from_slice(&[("dup_accounts")]);
    }
    #[cfg(feature = "bpf_rust")]
    {
        programs.extend_from_slice(&[("solana_bpf_rust_dup_accounts")]);
    }

    for program in programs.iter() {
        println!("Test program: {:?}", program);

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(50);
        let mut bank = Bank::new(&genesis_config);
        let (name, id, entrypoint) = solana_bpf_loader_program!();
        bank.add_builtin(&name, id, entrypoint);
        let bank = Arc::new(bank);
        let bank_client = BankClient::new_shared(&bank);
        let program_id = load_bpf_program(&bank_client, &bpf_loader::id(), &mint_keypair, program);
        let payee_account = AccountSharedData::new(10, 1, &program_id);
        let payee_pubkey = solana_sdk::pubkey::new_rand();
        bank.store_account(&payee_pubkey, &payee_account);
        let account = AccountSharedData::new(10, 1, &program_id);

        let pubkey = solana_sdk::pubkey::new_rand();
        let account_metas = vec![
            AccountMeta::new(mint_keypair.pubkey(), true),
            AccountMeta::new(payee_pubkey, false),
            AccountMeta::new(pubkey, false),
            AccountMeta::new(pubkey, false),
        ];

        bank.store_account(&pubkey, &account);
        let instruction = Instruction::new_with_bytes(program_id, &[1], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        let data = bank_client.get_account_data(&pubkey).unwrap().unwrap();
        assert!(result.is_ok());
        assert_eq!(data[0], 1);

        bank.store_account(&pubkey, &account);
        let instruction = Instruction::new_with_bytes(program_id, &[2], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        let data = bank_client.get_account_data(&pubkey).unwrap().unwrap();
        assert!(result.is_ok());
        assert_eq!(data[0], 2);

        bank.store_account(&pubkey, &account);
        let instruction = Instruction::new_with_bytes(program_id, &[3], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        let data = bank_client.get_account_data(&pubkey).unwrap().unwrap();
        assert!(result.is_ok());
        assert_eq!(data[0], 3);

        bank.store_account(&pubkey, &account);
        let instruction = Instruction::new_with_bytes(program_id, &[4], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        let lamports = bank_client.get_balance(&pubkey).unwrap();
        assert!(result.is_ok());
        assert_eq!(lamports, 11);

        bank.store_account(&pubkey, &account);
        let instruction = Instruction::new_with_bytes(program_id, &[5], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        let lamports = bank_client.get_balance(&pubkey).unwrap();
        assert!(result.is_ok());
        assert_eq!(lamports, 12);

        bank.store_account(&pubkey, &account);
        let instruction = Instruction::new_with_bytes(program_id, &[6], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        let lamports = bank_client.get_balance(&pubkey).unwrap();
        assert!(result.is_ok());
        assert_eq!(lamports, 13);

        let keypair = Keypair::new();
        let pubkey = keypair.pubkey();
        let account_metas = vec![
            AccountMeta::new(mint_keypair.pubkey(), true),
            AccountMeta::new(payee_pubkey, false),
            AccountMeta::new(pubkey, false),
            AccountMeta::new_readonly(pubkey, true),
            AccountMeta::new_readonly(program_id, false),
        ];
        bank.store_account(&pubkey, &account);
        let instruction = Instruction::new_with_bytes(program_id, &[7], account_metas.clone());
        let message = Message::new(&[instruction], Some(&mint_keypair.pubkey()));
        let result = bank_client.send_and_confirm_message(&[&mint_keypair, &keypair], message);
        assert!(result.is_ok());
    }
}

#[test]
fn test_program_bpf_error_handling() {
    solana_logger::setup();

    let mut programs = Vec::new();
    #[cfg(feature = "bpf_c")]
    {
        programs.extend_from_slice(&[("error_handling")]);
    }
    #[cfg(feature = "bpf_rust")]
    {
        programs.extend_from_slice(&[("solana_bpf_rust_error_handling")]);
    }

    for program in programs.iter() {
        println!("Test program: {:?}", program);

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(50);
        let mut bank = Bank::new(&genesis_config);
        let (name, id, entrypoint) = solana_bpf_loader_program!();
        bank.add_builtin(&name, id, entrypoint);
        let bank_client = BankClient::new(bank);
        let program_id = load_bpf_program(&bank_client, &bpf_loader::id(), &mint_keypair, program);
        let account_metas = vec![AccountMeta::new(mint_keypair.pubkey(), true)];

        let instruction = Instruction::new_with_bytes(program_id, &[1], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        assert!(result.is_ok());

        let instruction = Instruction::new_with_bytes(program_id, &[2], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        assert_eq!(
            result.unwrap_err().unwrap(),
            TransactionError::InstructionError(0, InstructionError::InvalidAccountData)
        );

        let instruction = Instruction::new_with_bytes(program_id, &[3], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        assert_eq!(
            result.unwrap_err().unwrap(),
            TransactionError::InstructionError(0, InstructionError::Custom(0))
        );

        let instruction = Instruction::new_with_bytes(program_id, &[4], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        assert_eq!(
            result.unwrap_err().unwrap(),
            TransactionError::InstructionError(0, InstructionError::Custom(42))
        );

        let instruction = Instruction::new_with_bytes(program_id, &[5], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        let result = result.unwrap_err().unwrap();
        if TransactionError::InstructionError(0, InstructionError::InvalidInstructionData) != result
        {
            assert_eq!(
                result,
                TransactionError::InstructionError(0, InstructionError::InvalidError)
            );
        }

        let instruction = Instruction::new_with_bytes(program_id, &[6], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        let result = result.unwrap_err().unwrap();
        if TransactionError::InstructionError(0, InstructionError::InvalidInstructionData) != result
        {
            assert_eq!(
                result,
                TransactionError::InstructionError(0, InstructionError::InvalidError)
            );
        }

        let instruction = Instruction::new_with_bytes(program_id, &[7], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        let result = result.unwrap_err().unwrap();
        if TransactionError::InstructionError(0, InstructionError::InvalidInstructionData) != result
        {
            assert_eq!(
                result,
                TransactionError::InstructionError(0, InstructionError::AccountBorrowFailed)
            );
        }

        let instruction = Instruction::new_with_bytes(program_id, &[8], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        assert_eq!(
            result.unwrap_err().unwrap(),
            TransactionError::InstructionError(0, InstructionError::InvalidInstructionData)
        );

        let instruction = Instruction::new_with_bytes(program_id, &[9], account_metas.clone());
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        assert_eq!(
            result.unwrap_err().unwrap(),
            TransactionError::InstructionError(0, InstructionError::MaxSeedLengthExceeded)
        );
    }
}

#[test]
fn test_program_bpf_invoke_sanity() {
    solana_logger::setup();

    const TEST_SUCCESS: u8 = 1;
    const TEST_PRIVILEGE_ESCALATION_SIGNER: u8 = 2;
    const TEST_PRIVILEGE_ESCALATION_WRITABLE: u8 = 3;
    const TEST_PPROGRAM_NOT_EXECUTABLE: u8 = 4;
    const TEST_EMPTY_ACCOUNTS_SLICE: u8 = 5;
    const TEST_CAP_SEEDS: u8 = 6;
    const TEST_CAP_SIGNERS: u8 = 7;
    const TEST_ALLOC_ACCESS_VIOLATION: u8 = 8;
    const TEST_INSTRUCTION_DATA_TOO_LARGE: u8 = 9;
    const TEST_INSTRUCTION_META_TOO_LARGE: u8 = 10;
    const TEST_RETURN_ERROR: u8 = 11;
    const TEST_PRIVILEGE_DEESCALATION_ESCALATION_SIGNER: u8 = 12;
    const TEST_PRIVILEGE_DEESCALATION_ESCALATION_WRITABLE: u8 = 13;
    const TEST_WRITABLE_DEESCALATION_WRITABLE: u8 = 14;
    const TEST_NESTED_INVOKE_TOO_DEEP: u8 = 15;
    const TEST_EXECUTABLE_LAMPORTS: u8 = 16;

    #[allow(dead_code)]
    #[derive(Debug)]
    enum Languages {
        C,
        Rust,
    }
    let mut programs = Vec::new();
    #[cfg(feature = "bpf_c")]
    {
        programs.push((Languages::C, "invoke", "invoked", "noop"));
    }
    #[cfg(feature = "bpf_rust")]
    {
        programs.push((
            Languages::Rust,
            "solana_bpf_rust_invoke",
            "solana_bpf_rust_invoked",
            "solana_bpf_rust_noop",
        ));
    }
    for program in programs.iter() {
        println!("Test program: {:?}", program);

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(50);
        let mut bank = Bank::new(&genesis_config);
        let (name, id, entrypoint) = solana_bpf_loader_program!();
        bank.add_builtin(&name, id, entrypoint);
        let bank = Arc::new(bank);
        let bank_client = BankClient::new_shared(&bank);

        let invoke_program_id =
            load_bpf_program(&bank_client, &bpf_loader::id(), &mint_keypair, program.1);
        let invoked_program_id =
            load_bpf_program(&bank_client, &bpf_loader::id(), &mint_keypair, program.2);
        let noop_program_id =
            load_bpf_program(&bank_client, &bpf_loader::id(), &mint_keypair, program.3);

        let argument_keypair = Keypair::new();
        let account = AccountSharedData::new(42, 100, &invoke_program_id);
        bank.store_account(&argument_keypair.pubkey(), &account);

        let invoked_argument_keypair = Keypair::new();
        let account = AccountSharedData::new(10, 10, &invoked_program_id);
        bank.store_account(&invoked_argument_keypair.pubkey(), &account);

        let from_keypair = Keypair::new();
        let account = AccountSharedData::new(84, 0, &solana_sdk::system_program::id());
        bank.store_account(&from_keypair.pubkey(), &account);

        let (derived_key1, bump_seed1) =
            Pubkey::find_program_address(&[b"You pass butter"], &invoke_program_id);
        let (derived_key2, bump_seed2) =
            Pubkey::find_program_address(&[b"Lil'", b"Bits"], &invoked_program_id);
        let (derived_key3, bump_seed3) =
            Pubkey::find_program_address(&[derived_key2.as_ref()], &invoked_program_id);

        let mint_pubkey = mint_keypair.pubkey();
        let account_metas = vec![
            AccountMeta::new(mint_pubkey, true),
            AccountMeta::new(argument_keypair.pubkey(), true),
            AccountMeta::new_readonly(invoked_program_id, false),
            AccountMeta::new(invoked_argument_keypair.pubkey(), true),
            AccountMeta::new_readonly(invoked_program_id, false),
            AccountMeta::new(argument_keypair.pubkey(), true),
            AccountMeta::new(derived_key1, false),
            AccountMeta::new(derived_key2, false),
            AccountMeta::new_readonly(derived_key3, false),
            AccountMeta::new_readonly(solana_sdk::system_program::id(), false),
            AccountMeta::new(from_keypair.pubkey(), true),
            AccountMeta::new_readonly(invoke_program_id, false),
        ];

        // success cases

        let instruction = Instruction::new_with_bytes(
            invoke_program_id,
            &[TEST_SUCCESS, bump_seed1, bump_seed2, bump_seed3],
            account_metas.clone(),
        );
        let noop_instruction = Instruction::new_with_bytes(noop_program_id, &[], vec![]);
        let message = Message::new(&[instruction, noop_instruction], Some(&mint_pubkey));
        let tx = Transaction::new(
            &[
                &mint_keypair,
                &argument_keypair,
                &invoked_argument_keypair,
                &from_keypair,
            ],
            message.clone(),
            bank.last_blockhash(),
        );
        let (result, inner_instructions) = process_transaction_and_record_inner(&bank, tx);
        assert_eq!(result, Ok(()));

        let invoked_programs: Vec<Pubkey> = inner_instructions[0]
            .iter()
            .map(|ix| message.account_keys[ix.program_id_index as usize].clone())
            .collect();
        let expected_invoked_programs = match program.0 {
            Languages::C => vec![
                solana_sdk::system_program::id(),
                solana_sdk::system_program::id(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
            ],
            Languages::Rust => vec![
                solana_sdk::system_program::id(),
                solana_sdk::system_program::id(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                solana_sdk::system_program::id(),
            ],
        };
        assert_eq!(invoked_programs.len(), expected_invoked_programs.len());
        assert_eq!(invoked_programs, expected_invoked_programs);
        let no_invoked_programs: Vec<Pubkey> = inner_instructions[1]
            .iter()
            .map(|ix| message.account_keys[ix.program_id_index as usize].clone())
            .collect();
        assert_eq!(no_invoked_programs.len(), 0);

        // failure cases

        let do_invoke_failure_test_local =
            |test: u8, expected_error: TransactionError, expected_invoked_programs: &[Pubkey]| {
                println!("Running failure test #{:?}", test);
                let instruction_data = &[test, bump_seed1, bump_seed2, bump_seed3];
                let signers = vec![
                    &mint_keypair,
                    &argument_keypair,
                    &invoked_argument_keypair,
                    &from_keypair,
                ];
                let instruction = Instruction::new_with_bytes(
                    invoke_program_id,
                    instruction_data,
                    account_metas.clone(),
                );
                let message = Message::new(&[instruction], Some(&mint_pubkey));
                let tx = Transaction::new(&signers, message.clone(), bank.last_blockhash());
                let (result, inner_instructions) = process_transaction_and_record_inner(&bank, tx);
                let invoked_programs: Vec<Pubkey> = inner_instructions[0]
                    .iter()
                    .map(|ix| message.account_keys[ix.program_id_index as usize].clone())
                    .collect();
                assert_eq!(result, Err(expected_error));
                assert_eq!(invoked_programs, expected_invoked_programs);
            };

        do_invoke_failure_test_local(
            TEST_PRIVILEGE_ESCALATION_SIGNER,
            TransactionError::InstructionError(0, InstructionError::PrivilegeEscalation),
            &[invoked_program_id.clone()],
        );

        do_invoke_failure_test_local(
            TEST_PRIVILEGE_ESCALATION_WRITABLE,
            TransactionError::InstructionError(0, InstructionError::PrivilegeEscalation),
            &[invoked_program_id.clone()],
        );

        do_invoke_failure_test_local(
            TEST_PPROGRAM_NOT_EXECUTABLE,
            TransactionError::InstructionError(0, InstructionError::AccountNotExecutable),
            &[],
        );

        do_invoke_failure_test_local(
            TEST_EMPTY_ACCOUNTS_SLICE,
            TransactionError::InstructionError(0, InstructionError::MissingAccount),
            &[],
        );

        do_invoke_failure_test_local(
            TEST_CAP_SEEDS,
            TransactionError::InstructionError(0, InstructionError::MaxSeedLengthExceeded),
            &[],
        );

        do_invoke_failure_test_local(
            TEST_CAP_SIGNERS,
            TransactionError::InstructionError(0, InstructionError::ProgramFailedToComplete),
            &[],
        );

        do_invoke_failure_test_local(
            TEST_INSTRUCTION_DATA_TOO_LARGE,
            TransactionError::InstructionError(0, InstructionError::ProgramFailedToComplete),
            &[],
        );

        do_invoke_failure_test_local(
            TEST_INSTRUCTION_META_TOO_LARGE,
            TransactionError::InstructionError(0, InstructionError::ProgramFailedToComplete),
            &[],
        );

        do_invoke_failure_test_local(
            TEST_RETURN_ERROR,
            TransactionError::InstructionError(0, InstructionError::Custom(42)),
            &[invoked_program_id.clone()],
        );

        do_invoke_failure_test_local(
            TEST_PRIVILEGE_DEESCALATION_ESCALATION_SIGNER,
            TransactionError::InstructionError(0, InstructionError::PrivilegeEscalation),
            &[invoked_program_id.clone()],
        );

        do_invoke_failure_test_local(
            TEST_PRIVILEGE_DEESCALATION_ESCALATION_WRITABLE,
            TransactionError::InstructionError(0, InstructionError::PrivilegeEscalation),
            &[invoked_program_id.clone()],
        );

        do_invoke_failure_test_local(
            TEST_WRITABLE_DEESCALATION_WRITABLE,
            TransactionError::InstructionError(0, InstructionError::ReadonlyDataModified),
            &[invoked_program_id.clone()],
        );

        do_invoke_failure_test_local(
            TEST_NESTED_INVOKE_TOO_DEEP,
            TransactionError::InstructionError(0, InstructionError::CallDepth),
            &[
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
                invoked_program_id.clone(),
            ],
        );

        do_invoke_failure_test_local(
            TEST_EXECUTABLE_LAMPORTS,
            TransactionError::InstructionError(0, InstructionError::ExecutableLamportChange),
            &[invoke_program_id.clone()],
        );

        // Check resulting state

        assert_eq!(43, bank.get_balance(&derived_key1));
        let account = bank.get_account(&derived_key1).unwrap();
        assert_eq!(&invoke_program_id, account.owner());
        assert_eq!(
            MAX_PERMITTED_DATA_INCREASE,
            bank.get_account(&derived_key1).unwrap().data().len()
        );
        for i in 0..20 {
            assert_eq!(i as u8, account.data()[i]);
        }

        // Attempt to realloc into unauthorized address space
        let account = AccountSharedData::new(84, 0, &solana_sdk::system_program::id());
        bank.store_account(&from_keypair.pubkey(), &account);
        bank.store_account(&derived_key1, &AccountSharedData::default());
        let instruction = Instruction::new_with_bytes(
            invoke_program_id,
            &[
                TEST_ALLOC_ACCESS_VIOLATION,
                bump_seed1,
                bump_seed2,
                bump_seed3,
            ],
            account_metas.clone(),
        );
        let message = Message::new(&[instruction], Some(&mint_pubkey));
        let tx = Transaction::new(
            &[
                &mint_keypair,
                &argument_keypair,
                &invoked_argument_keypair,
                &from_keypair,
            ],
            message.clone(),
            bank.last_blockhash(),
        );
        let (result, inner_instructions) = process_transaction_and_record_inner(&bank, tx);
        let invoked_programs: Vec<Pubkey> = inner_instructions[0]
            .iter()
            .map(|ix| message.account_keys[ix.program_id_index as usize].clone())
            .collect();
        assert_eq!(invoked_programs, vec![solana_sdk::system_program::id()]);
        assert_eq!(
            result.unwrap_err(),
            TransactionError::InstructionError(0, InstructionError::ProgramFailedToComplete)
        );
    }
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_program_id_spoofing() {
    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank = Arc::new(bank);
    let bank_client = BankClient::new_shared(&bank);

    let malicious_swap_pubkey = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_spoof1",
    );
    let malicious_system_pubkey = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_spoof1_system",
    );

    let from_pubkey = Pubkey::new_unique();
    let account = AccountSharedData::new(10, 0, &solana_sdk::system_program::id());
    bank.store_account(&from_pubkey, &account);

    let to_pubkey = Pubkey::new_unique();
    let account = AccountSharedData::new(0, 0, &solana_sdk::system_program::id());
    bank.store_account(&to_pubkey, &account);

    let account_metas = vec![
        AccountMeta::new_readonly(solana_sdk::system_program::id(), false),
        AccountMeta::new_readonly(malicious_system_pubkey, false),
        AccountMeta::new(from_pubkey, false),
        AccountMeta::new(to_pubkey, false),
    ];

    let instruction =
        Instruction::new_with_bytes(malicious_swap_pubkey, &[], account_metas.clone());
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::MissingRequiredSignature)
    );
    assert_eq!(10, bank.get_balance(&from_pubkey));
    assert_eq!(0, bank.get_balance(&to_pubkey));
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_caller_has_access_to_cpi_program() {
    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank = Arc::new(bank);
    let bank_client = BankClient::new_shared(&bank);

    let caller_pubkey = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_caller_access",
    );
    let caller2_pubkey = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_caller_access",
    );
    let account_metas = vec![
        AccountMeta::new_readonly(caller_pubkey, false),
        AccountMeta::new_readonly(caller2_pubkey, false),
    ];
    let instruction = Instruction::new_with_bytes(caller_pubkey, &[1], account_metas.clone());
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::MissingAccount)
    );
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_ro_modify() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank = Arc::new(bank);
    let bank_client = BankClient::new_shared(&bank);

    let program_pubkey = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_ro_modify",
    );

    let test_keypair = Keypair::new();
    let account = AccountSharedData::new(10, 0, &solana_sdk::system_program::id());
    bank.store_account(&test_keypair.pubkey(), &account);

    let account_metas = vec![
        AccountMeta::new_readonly(solana_sdk::system_program::id(), false),
        AccountMeta::new(test_keypair.pubkey(), true),
    ];

    let instruction = Instruction::new_with_bytes(program_pubkey, &[1], account_metas.clone());
    let message = Message::new(&[instruction], Some(&mint_keypair.pubkey()));
    let result = bank_client.send_and_confirm_message(&[&mint_keypair, &test_keypair], message);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::ProgramFailedToComplete)
    );

    let instruction = Instruction::new_with_bytes(program_pubkey, &[3], account_metas.clone());
    let message = Message::new(&[instruction], Some(&mint_keypair.pubkey()));
    let result = bank_client.send_and_confirm_message(&[&mint_keypair, &test_keypair], message);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::ProgramFailedToComplete)
    );

    let instruction = Instruction::new_with_bytes(program_pubkey, &[4], account_metas.clone());
    let message = Message::new(&[instruction], Some(&mint_keypair.pubkey()));
    let result = bank_client.send_and_confirm_message(&[&mint_keypair, &test_keypair], message);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::ProgramFailedToComplete)
    );
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_call_depth() {
    use solana_sdk::compute_budget::ComputeBudget;

    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank_client = BankClient::new(bank);
    let program_id = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_call_depth",
    );

    let instruction = Instruction::new_with_bincode(
        program_id,
        &(ComputeBudget::default().max_call_depth - 1),
        vec![],
    );
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
    assert!(result.is_ok());

    let instruction =
        Instruction::new_with_bincode(program_id, &ComputeBudget::default().max_call_depth, vec![]);
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
    assert!(result.is_err());
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_compute_budget() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank_client = BankClient::new(bank);
    let program_id = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_noop",
    );
    let message = Message::new(
        &[
            solana_sdk::compute_budget::ComputeBudgetInstruction::request_units(1),
            Instruction::new_with_bincode(program_id, &0, vec![]),
        ],
        Some(&mint_keypair.pubkey()),
    );
    let result = bank_client.send_and_confirm_message(&[&mint_keypair], message);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(1, InstructionError::ProgramFailedToComplete),
    );
}

#[test]
fn assert_instruction_count() {
    solana_logger::setup();

    let mut programs = Vec::new();
    #[cfg(feature = "bpf_c")]
    {
        programs.extend_from_slice(&[
            ("alloc", 1137),
            ("bpf_to_bpf", 13),
            ("multiple_static", 8),
            ("noop", 5),
            ("noop++", 5),
            ("relative_call", 10),
            ("sanity", 169),
            ("sanity++", 168),
            ("secp256k1_recover", 357),
            ("sha", 1040),
            ("struct_pass", 8),
            ("struct_ret", 22),
        ]);
    }
    #[cfg(feature = "bpf_rust")]
    {
        programs.extend_from_slice(&[
            ("solana_bpf_rust_128bit", 584),
            ("solana_bpf_rust_alloc", 7143),
            ("solana_bpf_rust_custom_heap", 522),
            ("solana_bpf_rust_dep_crate", 47),
            ("solana_bpf_rust_external_spend", 504),
            ("solana_bpf_rust_iter", 724),
            ("solana_bpf_rust_many_args", 233),
            ("solana_bpf_rust_mem", 3119),
            ("solana_bpf_rust_membuiltins", 4065),
            ("solana_bpf_rust_noop", 478),
            ("solana_bpf_rust_param_passing", 46),
            ("solana_bpf_rust_rand", 481),
            ("solana_bpf_rust_sanity", 907),
            ("solana_bpf_rust_secp256k1_recover", 301),
            ("solana_bpf_rust_sha", 32337),
        ]);
    }

    let mut passed = true;
    println!("\n  {:30} expected actual  diff", "BPF program");
    for program in programs.iter() {
        let program_id = solana_sdk::pubkey::new_rand();
        let key = solana_sdk::pubkey::new_rand();
        let mut account = RefCell::new(AccountSharedData::default());
        let parameter_accounts = vec![KeyedAccount::new(&key, false, &mut account)];
        let count = run_program(program.0, &program_id, parameter_accounts, &[]).unwrap();
        let diff: i64 = count as i64 - program.1 as i64;
        println!(
            "  {:30} {:8} {:6} {:+5} ({:+3.0}%)",
            program.0,
            program.1,
            count,
            diff,
            100.0_f64 * count as f64 / program.1 as f64 - 100.0_f64,
        );
        if count > program.1 {
            passed = false;
        }
    }
    assert!(passed);
}

#[cfg(any(feature = "bpf_rust"))]
#[test]
fn test_program_bpf_instruction_introspection() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50_000);
    let mut bank = Bank::new(&genesis_config);

    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank = Arc::new(bank);
    let bank_client = BankClient::new_shared(&bank);

    let program_id = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_instruction_introspection",
    );

    // Passing transaction
    let account_metas = vec![AccountMeta::new_readonly(
        solana_sdk::sysvar::instructions::id(),
        false,
    )];
    let instruction0 = Instruction::new_with_bytes(program_id, &[0u8, 0u8], account_metas.clone());
    let instruction1 = Instruction::new_with_bytes(program_id, &[0u8, 1u8], account_metas.clone());
    let instruction2 = Instruction::new_with_bytes(program_id, &[0u8, 2u8], account_metas);
    let message = Message::new(
        &[instruction0, instruction1, instruction2],
        Some(&mint_keypair.pubkey()),
    );
    let result = bank_client.send_and_confirm_message(&[&mint_keypair], message);
    assert!(result.is_ok());

    // writable special instructions11111 key, should not be allowed
    let account_metas = vec![AccountMeta::new(
        solana_sdk::sysvar::instructions::id(),
        false,
    )];
    let instruction = Instruction::new_with_bytes(program_id, &[0], account_metas);
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
    assert_eq!(
        result.unwrap_err().unwrap(),
        // sysvar write locks are demoted to read only. So this will no longer
        // cause InvalidAccountIndex error.
        TransactionError::InstructionError(0, InstructionError::ProgramFailedToComplete),
    );

    // No accounts, should error
    let instruction = Instruction::new_with_bytes(program_id, &[0], vec![]);
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(
            0,
            solana_sdk::instruction::InstructionError::NotEnoughAccountKeys
        )
    );
    assert!(bank
        .get_account(&solana_sdk::sysvar::instructions::id())
        .is_none());
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_test_use_latest_executor() {
    use solana_sdk::{loader_instruction, system_instruction};

    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank_client = BankClient::new(bank);
    let panic_id = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_panic",
    );

    let program_keypair = Keypair::new();

    // Write the panic program into the program account
    let elf = read_bpf_program("solana_bpf_rust_panic");
    let message = Message::new(
        &[system_instruction::create_account(
            &mint_keypair.pubkey(),
            &program_keypair.pubkey(),
            1,
            elf.len() as u64 * 2,
            &bpf_loader::id(),
        )],
        Some(&mint_keypair.pubkey()),
    );
    assert!(bank_client
        .send_and_confirm_message(&[&mint_keypair, &program_keypair], message)
        .is_ok());
    write_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        &program_keypair,
        &elf,
    );

    // Finalize the panic program, but fail the tx
    let message = Message::new(
        &[
            loader_instruction::finalize(&program_keypair.pubkey(), &bpf_loader::id()),
            Instruction::new_with_bytes(panic_id, &[0], vec![]),
        ],
        Some(&mint_keypair.pubkey()),
    );
    assert!(bank_client
        .send_and_confirm_message(&[&mint_keypair, &program_keypair], message)
        .is_err());

    // Write the noop program into the same program account
    let elf = read_bpf_program("solana_bpf_rust_noop");
    write_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        &program_keypair,
        &elf,
    );

    // Finalize the noop program
    let message = Message::new(
        &[loader_instruction::finalize(
            &program_keypair.pubkey(),
            &bpf_loader::id(),
        )],
        Some(&mint_keypair.pubkey()),
    );
    assert!(bank_client
        .send_and_confirm_message(&[&mint_keypair, &program_keypair], message)
        .is_ok());

    // Call the noop program, should get noop not panic
    let message = Message::new(
        &[Instruction::new_with_bytes(
            program_keypair.pubkey(),
            &[0],
            vec![],
        )],
        Some(&mint_keypair.pubkey()),
    );
    assert!(bank_client
        .send_and_confirm_message(&[&mint_keypair], message)
        .is_ok());
}

#[ignore] // Invoking BPF loaders from CPI not allowed
#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_test_use_latest_executor2() {
    use solana_sdk::{loader_instruction, system_instruction};

    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank_client = BankClient::new(bank);
    let invoke_and_error = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_invoke_and_error",
    );
    let invoke_and_ok = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_invoke_and_ok",
    );

    let program_keypair = Keypair::new();

    // Write the panic program into the program account
    let elf = read_bpf_program("solana_bpf_rust_panic");
    let message = Message::new(
        &[system_instruction::create_account(
            &mint_keypair.pubkey(),
            &program_keypair.pubkey(),
            1,
            elf.len() as u64 * 2,
            &bpf_loader::id(),
        )],
        Some(&mint_keypair.pubkey()),
    );
    assert!(bank_client
        .send_and_confirm_message(&[&mint_keypair, &program_keypair], message)
        .is_ok());
    write_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        &program_keypair,
        &elf,
    );

    // - invoke finalize and return error, swallow error
    let mut instruction =
        loader_instruction::finalize(&program_keypair.pubkey(), &bpf_loader::id());
    instruction.accounts.insert(
        0,
        AccountMeta {
            is_signer: false,
            is_writable: false,
            pubkey: instruction.program_id,
        },
    );
    instruction.program_id = invoke_and_ok;
    instruction.accounts.insert(
        0,
        AccountMeta {
            is_signer: false,
            is_writable: false,
            pubkey: invoke_and_error,
        },
    );
    let message = Message::new(&[instruction], Some(&mint_keypair.pubkey()));
    assert!(bank_client
        .send_and_confirm_message(&[&mint_keypair, &program_keypair], message)
        .is_ok());

    // invoke program, verify not found
    let message = Message::new(
        &[Instruction::new_with_bytes(
            program_keypair.pubkey(),
            &[0],
            vec![],
        )],
        Some(&mint_keypair.pubkey()),
    );
    assert_eq!(
        bank_client
            .send_and_confirm_message(&[&mint_keypair], message)
            .unwrap_err()
            .unwrap(),
        TransactionError::InvalidProgramForExecution
    );

    // Write the noop program into the same program account
    let elf = read_bpf_program("solana_bpf_rust_noop");
    write_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        &program_keypair,
        &elf,
    );

    // Finalize the noop program
    let message = Message::new(
        &[loader_instruction::finalize(
            &program_keypair.pubkey(),
            &bpf_loader::id(),
        )],
        Some(&mint_keypair.pubkey()),
    );
    assert!(bank_client
        .send_and_confirm_message(&[&mint_keypair, &program_keypair], message)
        .is_ok());

    // Call the program, should get noop, not panic
    let message = Message::new(
        &[Instruction::new_with_bytes(
            program_keypair.pubkey(),
            &[0],
            vec![],
        )],
        Some(&mint_keypair.pubkey()),
    );
    assert!(bank_client
        .send_and_confirm_message(&[&mint_keypair], message)
        .is_ok());
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_upgrade() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_upgradeable_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank_client = BankClient::new(bank);

    // Deploy upgrade program
    let buffer_keypair = Keypair::new();
    let program_keypair = Keypair::new();
    let program_id = program_keypair.pubkey();
    let authority_keypair = Keypair::new();
    load_upgradeable_bpf_program(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &program_keypair,
        &authority_keypair,
        "solana_bpf_rust_upgradeable",
    );

    let mut instruction = Instruction::new_with_bytes(
        program_id,
        &[0],
        vec![
            AccountMeta::new(program_id.clone(), false),
            AccountMeta::new(clock::id(), false),
            AccountMeta::new(fees::id(), false),
        ],
    );

    // Call upgrade program
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction.clone());
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::Custom(42))
    );

    // Upgrade program
    let buffer_keypair = Keypair::new();
    upgrade_bpf_program(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &program_id,
        &authority_keypair,
        "solana_bpf_rust_upgraded",
    );

    // Call upgraded program
    instruction.data[0] += 1;
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction.clone());
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::Custom(43))
    );

    // Set a new authority
    let new_authority_keypair = Keypair::new();
    set_upgrade_authority(
        &bank_client,
        &mint_keypair,
        &program_id,
        &authority_keypair,
        Some(&new_authority_keypair.pubkey()),
    );

    // Upgrade back to the original program
    let buffer_keypair = Keypair::new();
    upgrade_bpf_program(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &program_id,
        &new_authority_keypair,
        "solana_bpf_rust_upgradeable",
    );

    // Call original program
    instruction.data[0] += 1;
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::Custom(42))
    );
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_upgrade_and_invoke_in_same_tx() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_upgradeable_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank = Arc::new(bank);
    let bank_client = BankClient::new_shared(&bank);

    // Deploy upgrade program
    let buffer_keypair = Keypair::new();
    let program_keypair = Keypair::new();
    let program_id = program_keypair.pubkey();
    let authority_keypair = Keypair::new();
    load_upgradeable_bpf_program(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &program_keypair,
        &authority_keypair,
        "solana_bpf_rust_noop",
    );

    let invoke_instruction = Instruction::new_with_bytes(
        program_id,
        &[0],
        vec![
            AccountMeta::new(program_id.clone(), false),
            AccountMeta::new(clock::id(), false),
            AccountMeta::new(fees::id(), false),
        ],
    );

    // Call upgradeable program
    let result =
        bank_client.send_and_confirm_instruction(&mint_keypair, invoke_instruction.clone());
    assert!(result.is_ok());

    // Prepare for upgrade
    let buffer_keypair = Keypair::new();
    load_upgradeable_buffer(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &authority_keypair,
        "solana_bpf_rust_panic",
    );

    // Invoke, then upgrade the program, and then invoke again in same tx
    let message = Message::new(
        &[
            invoke_instruction.clone(),
            bpf_loader_upgradeable::upgrade(
                &program_id,
                &buffer_keypair.pubkey(),
                &authority_keypair.pubkey(),
                &mint_keypair.pubkey(),
            ),
            invoke_instruction,
        ],
        Some(&mint_keypair.pubkey()),
    );
    let tx = Transaction::new(
        &[&mint_keypair, &authority_keypair],
        message.clone(),
        bank.last_blockhash(),
    );
    let (result, _) = process_transaction_and_record_inner(&bank, tx);
    assert_eq!(
        result.unwrap_err(),
        TransactionError::InstructionError(2, InstructionError::ProgramFailedToComplete)
    );
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_invoke_upgradeable_via_cpi() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let (name, id, entrypoint) = solana_bpf_loader_upgradeable_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank_client = BankClient::new(bank);
    let invoke_and_return = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_invoke_and_return",
    );

    // Deploy upgradeable program
    let buffer_keypair = Keypair::new();
    let program_keypair = Keypair::new();
    let program_id = program_keypair.pubkey();
    let authority_keypair = Keypair::new();
    load_upgradeable_bpf_program(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &program_keypair,
        &authority_keypair,
        "solana_bpf_rust_upgradeable",
    );

    let mut instruction = Instruction::new_with_bytes(
        invoke_and_return,
        &[0],
        vec![
            AccountMeta::new(program_id, false),
            AccountMeta::new(program_id, false),
            AccountMeta::new(clock::id(), false),
            AccountMeta::new(fees::id(), false),
        ],
    );

    // Call invoker program to invoke the upgradeable program
    instruction.data[0] += 1;
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction.clone());
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::Custom(42))
    );

    // Upgrade program
    let buffer_keypair = Keypair::new();
    upgrade_bpf_program(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &program_id,
        &authority_keypair,
        "solana_bpf_rust_upgraded",
    );

    // Call the upgraded program
    instruction.data[0] += 1;
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction.clone());
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::Custom(43))
    );

    // Set a new authority
    let new_authority_keypair = Keypair::new();
    set_upgrade_authority(
        &bank_client,
        &mint_keypair,
        &program_id,
        &authority_keypair,
        Some(&new_authority_keypair.pubkey()),
    );

    // Upgrade back to the original program
    let buffer_keypair = Keypair::new();
    upgrade_bpf_program(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &program_id,
        &new_authority_keypair,
        "solana_bpf_rust_upgradeable",
    );

    // Call original program
    instruction.data[0] += 1;
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction.clone());
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::Custom(42))
    );
}

#[test]
#[cfg(any(feature = "bpf_c", feature = "bpf_rust"))]
fn test_program_bpf_disguised_as_bpf_loader() {
    solana_logger::setup();

    let mut programs = Vec::new();
    #[cfg(feature = "bpf_c")]
    {
        programs.extend_from_slice(&[("noop")]);
    }
    #[cfg(feature = "bpf_rust")]
    {
        programs.extend_from_slice(&[("solana_bpf_rust_noop")]);
    }

    for program in programs.iter() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(50);
        let mut bank = Bank::new(&genesis_config);
        let (name, id, entrypoint) = solana_bpf_loader_deprecated_program!();
        bank.add_builtin(&name, id, entrypoint);
        let bank_client = BankClient::new(bank);

        let program_id = load_bpf_program(
            &bank_client,
            &bpf_loader_deprecated::id(),
            &mint_keypair,
            program,
        );
        let account_metas = vec![AccountMeta::new_readonly(program_id, false)];
        let instruction =
            Instruction::new_with_bytes(bpf_loader_deprecated::id(), &[1], account_metas);
        let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction);
        assert_eq!(
            result.unwrap_err().unwrap(),
            TransactionError::InstructionError(0, InstructionError::IncorrectProgramId)
        );
    }
}

#[test]
#[cfg(feature = "bpf_c")]
fn test_program_bpf_c_dup() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);

    let account_address = Pubkey::new_unique();
    let account =
        AccountSharedData::new_data(42, &[1_u8, 2, 3], &solana_sdk::system_program::id()).unwrap();
    bank.store_account(&account_address, &account);

    let bank_client = BankClient::new(bank);

    let program_id = load_bpf_program(&bank_client, &bpf_loader::id(), &mint_keypair, "ser");
    let account_metas = vec![
        AccountMeta::new_readonly(account_address, false),
        AccountMeta::new_readonly(account_address, false),
    ];
    let instruction = Instruction::new_with_bytes(program_id, &[4, 5, 6, 7], account_metas);
    bank_client
        .send_and_confirm_instruction(&mint_keypair, instruction)
        .unwrap();
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_upgrade_via_cpi() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let (name, id, entrypoint) = solana_bpf_loader_upgradeable_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank_client = BankClient::new(bank);
    let invoke_and_return = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_invoke_and_return",
    );

    // Deploy upgradeable program
    let buffer_keypair = Keypair::new();
    let program_keypair = Keypair::new();
    let program_id = program_keypair.pubkey();
    let authority_keypair = Keypair::new();
    load_upgradeable_bpf_program(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &program_keypair,
        &authority_keypair,
        "solana_bpf_rust_upgradeable",
    );
    let program_account = bank_client.get_account(&program_id).unwrap().unwrap();
    let programdata_address = match program_account.state() {
        Ok(bpf_loader_upgradeable::UpgradeableLoaderState::Program {
            programdata_address,
        }) => programdata_address,
        _ => unreachable!(),
    };
    let original_programdata = bank_client
        .get_account_data(&programdata_address)
        .unwrap()
        .unwrap();

    let mut instruction = Instruction::new_with_bytes(
        invoke_and_return,
        &[0],
        vec![
            AccountMeta::new(program_id, false),
            AccountMeta::new(program_id, false),
            AccountMeta::new(clock::id(), false),
            AccountMeta::new(fees::id(), false),
        ],
    );

    // Call the upgradable program
    instruction.data[0] += 1;
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction.clone());
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::Custom(42))
    );

    // Load the buffer account
    let path = create_bpf_path("solana_bpf_rust_upgraded");
    let mut file = File::open(&path).unwrap_or_else(|err| {
        panic!("Failed to open {}: {}", path.display(), err);
    });
    let mut elf = Vec::new();
    file.read_to_end(&mut elf).unwrap();
    let buffer_keypair = Keypair::new();
    load_buffer_account(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &authority_keypair,
        &elf,
    );

    // Upgrade program via CPI
    let mut upgrade_instruction = bpf_loader_upgradeable::upgrade(
        &program_id,
        &buffer_keypair.pubkey(),
        &authority_keypair.pubkey(),
        &mint_keypair.pubkey(),
    );
    upgrade_instruction.program_id = invoke_and_return;
    upgrade_instruction
        .accounts
        .insert(0, AccountMeta::new(bpf_loader_upgradeable::id(), false));
    let message = Message::new(&[upgrade_instruction], Some(&mint_keypair.pubkey()));
    bank_client
        .send_and_confirm_message(&[&mint_keypair, &authority_keypair], message)
        .unwrap();

    // Call the upgraded program
    instruction.data[0] += 1;
    let result = bank_client.send_and_confirm_instruction(&mint_keypair, instruction.clone());
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::Custom(43))
    );

    // Validate that the programdata was actually overwritten
    let programdata = bank_client
        .get_account_data(&programdata_address)
        .unwrap()
        .unwrap();
    assert_ne!(programdata, original_programdata);
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_upgrade_self_via_cpi() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let (name, id, entrypoint) = solana_bpf_loader_upgradeable_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank = Arc::new(bank);
    let bank_client = BankClient::new_shared(&bank);
    let noop_program_id = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_noop",
    );

    // Deploy upgradeable program
    let buffer_keypair = Keypair::new();
    let program_keypair = Keypair::new();
    let program_id = program_keypair.pubkey();
    let authority_keypair = Keypair::new();
    load_upgradeable_bpf_program(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &program_keypair,
        &authority_keypair,
        "solana_bpf_rust_invoke_and_return",
    );

    let mut invoke_instruction = Instruction::new_with_bytes(
        program_id,
        &[0],
        vec![
            AccountMeta::new(noop_program_id, false),
            AccountMeta::new(noop_program_id, false),
            AccountMeta::new(clock::id(), false),
            AccountMeta::new(fees::id(), false),
        ],
    );

    // Call the upgraded program
    invoke_instruction.data[0] += 1;
    let result =
        bank_client.send_and_confirm_instruction(&mint_keypair, invoke_instruction.clone());
    assert!(result.is_ok());

    // Prepare for upgrade
    let buffer_keypair = Keypair::new();
    load_upgradeable_buffer(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &authority_keypair,
        "solana_bpf_rust_panic",
    );

    // Invoke, then upgrade the program, and then invoke again in same tx
    let message = Message::new(
        &[
            invoke_instruction.clone(),
            bpf_loader_upgradeable::upgrade(
                &program_id,
                &buffer_keypair.pubkey(),
                &authority_keypair.pubkey(),
                &mint_keypair.pubkey(),
            ),
            invoke_instruction,
        ],
        Some(&mint_keypair.pubkey()),
    );
    let tx = Transaction::new(
        &[&mint_keypair, &authority_keypair],
        message.clone(),
        bank.last_blockhash(),
    );
    let (result, _) = process_transaction_and_record_inner(&bank, tx);
    assert_eq!(
        result.unwrap_err(),
        TransactionError::InstructionError(2, InstructionError::ProgramFailedToComplete)
    );
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_set_upgrade_authority_via_cpi() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let (name, id, entrypoint) = solana_bpf_loader_upgradeable_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank_client = BankClient::new(bank);

    // Deploy CPI invoker program
    let invoke_and_return = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_invoke_and_return",
    );

    // Deploy upgradeable program
    let buffer_keypair = Keypair::new();
    let program_keypair = Keypair::new();
    let program_id = program_keypair.pubkey();
    let authority_keypair = Keypair::new();
    load_upgradeable_bpf_program(
        &bank_client,
        &mint_keypair,
        &buffer_keypair,
        &program_keypair,
        &authority_keypair,
        "solana_bpf_rust_upgradeable",
    );

    // Set program upgrade authority instruction to invoke via CPI
    let new_upgrade_authority_key = Keypair::new().pubkey();
    let mut set_upgrade_authority_instruction = bpf_loader_upgradeable::set_upgrade_authority(
        &program_id,
        &authority_keypair.pubkey(),
        Some(&new_upgrade_authority_key),
    );

    // Invoke set_upgrade_authority via CPI invoker program
    set_upgrade_authority_instruction.program_id = invoke_and_return;
    set_upgrade_authority_instruction
        .accounts
        .insert(0, AccountMeta::new(bpf_loader_upgradeable::id(), false));

    let message = Message::new(
        &[set_upgrade_authority_instruction],
        Some(&mint_keypair.pubkey()),
    );
    bank_client
        .send_and_confirm_message(&[&mint_keypair, &authority_keypair], message)
        .unwrap();

    // Assert upgrade authority was changed
    let program_account_data = bank_client.get_account_data(&program_id).unwrap().unwrap();
    let program_account = parse_bpf_upgradeable_loader(&program_account_data).unwrap();

    let upgrade_authority_key = match program_account {
        BpfUpgradeableLoaderAccountType::Program(ui_program) => {
            let program_data_account_key = Pubkey::from_str(&ui_program.program_data).unwrap();
            let program_data_account_data = bank_client
                .get_account_data(&program_data_account_key)
                .unwrap()
                .unwrap();
            let program_data_account =
                parse_bpf_upgradeable_loader(&program_data_account_data).unwrap();

            match program_data_account {
                BpfUpgradeableLoaderAccountType::ProgramData(ui_program_data) => ui_program_data
                    .authority
                    .map(|a| Pubkey::from_str(&a).unwrap()),
                _ => None,
            }
        }
        _ => None,
    };

    assert_eq!(Some(new_upgrade_authority_key), upgrade_authority_key);
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_upgradeable_locks() {
    fn setup_program_upgradeable_locks(
        payer_keypair: &Keypair,
        buffer_keypair: &Keypair,
        program_keypair: &Keypair,
    ) -> (Arc<Bank>, Transaction, Transaction) {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2_000_000_000);
        let mut bank = Bank::new(&genesis_config);
        let (name, id, entrypoint) = solana_bpf_loader_upgradeable_program!();
        bank.add_builtin(&name, id, entrypoint);
        let bank = Arc::new(bank);
        let bank_client = BankClient::new_shared(&bank);

        load_upgradeable_bpf_program(
            &bank_client,
            &mint_keypair,
            buffer_keypair,
            program_keypair,
            payer_keypair,
            "solana_bpf_rust_panic",
        );

        // Load the buffer account
        let path = create_bpf_path("solana_bpf_rust_noop");
        let mut file = File::open(&path).unwrap_or_else(|err| {
            panic!("Failed to open {}: {}", path.display(), err);
        });
        let mut elf = Vec::new();
        file.read_to_end(&mut elf).unwrap();
        load_buffer_account(
            &bank_client,
            &mint_keypair,
            buffer_keypair,
            &payer_keypair,
            &elf,
        );

        bank_client
            .send_and_confirm_instruction(
                &mint_keypair,
                system_instruction::transfer(
                    &mint_keypair.pubkey(),
                    &payer_keypair.pubkey(),
                    1_000_000_000,
                ),
            )
            .unwrap();

        let invoke_tx = Transaction::new(
            &[payer_keypair],
            Message::new(
                &[Instruction::new_with_bytes(
                    program_keypair.pubkey(),
                    &[0; 0],
                    vec![],
                )],
                Some(&payer_keypair.pubkey()),
            ),
            bank.last_blockhash(),
        );
        let upgrade_tx = Transaction::new(
            &[payer_keypair],
            Message::new(
                &[bpf_loader_upgradeable::upgrade(
                    &program_keypair.pubkey(),
                    &buffer_keypair.pubkey(),
                    &payer_keypair.pubkey(),
                    &payer_keypair.pubkey(),
                )],
                Some(&payer_keypair.pubkey()),
            ),
            bank.last_blockhash(),
        );

        (bank, invoke_tx, upgrade_tx)
    }

    let payer_keypair = keypair_from_seed(&[56u8; 32]).unwrap();
    let buffer_keypair = keypair_from_seed(&[11; 32]).unwrap();
    let program_keypair = keypair_from_seed(&[77u8; 32]).unwrap();

    let results1 = {
        let (bank, invoke_tx, upgrade_tx) =
            setup_program_upgradeable_locks(&payer_keypair, &buffer_keypair, &program_keypair);
        execute_transactions(&bank, &[upgrade_tx, invoke_tx])
    };

    let results2 = {
        let (bank, invoke_tx, upgrade_tx) =
            setup_program_upgradeable_locks(&payer_keypair, &buffer_keypair, &program_keypair);
        execute_transactions(&bank, &[invoke_tx, upgrade_tx])
    };

    if false {
        println!("upgrade and invoke");
        for result in &results1 {
            print_confirmed_tx("result", result.clone());
        }
        println!("invoke and upgrade");
        for result in &results2 {
            print_confirmed_tx("result", result.clone());
        }
    }

    if let Some(ref meta) = results1[0].transaction.meta {
        assert_eq!(meta.status, Ok(()));
    } else {
        panic!("no meta");
    }
    if let Some(ref meta) = results1[1].transaction.meta {
        assert_eq!(meta.status, Err(TransactionError::AccountInUse));
    } else {
        panic!("no meta");
    }
    if let Some(ref meta) = results2[0].transaction.meta {
        assert_eq!(
            meta.status,
            Err(TransactionError::InstructionError(
                0,
                InstructionError::ProgramFailedToComplete
            ))
        );
    } else {
        panic!("no meta");
    }
    if let Some(ref meta) = results2[1].transaction.meta {
        assert_eq!(meta.status, Err(TransactionError::AccountInUse));
    } else {
        panic!("no meta");
    }
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_finalize() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank = Arc::new(bank);
    let bank_client = BankClient::new_shared(&bank);

    let program_pubkey = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_finalize",
    );

    let noop_keypair = Keypair::new();

    // Write the noop program into the same program account
    let elf = read_bpf_program("solana_bpf_rust_noop");
    let message = Message::new(
        &[system_instruction::create_account(
            &mint_keypair.pubkey(),
            &noop_keypair.pubkey(),
            1,
            elf.len() as u64 * 2,
            &bpf_loader::id(),
        )],
        Some(&mint_keypair.pubkey()),
    );
    assert!(bank_client
        .send_and_confirm_message(&[&mint_keypair, &noop_keypair], message)
        .is_ok());
    write_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        &noop_keypair,
        &elf,
    );

    let account_metas = vec![
        AccountMeta::new(noop_keypair.pubkey(), true),
        AccountMeta::new_readonly(bpf_loader::id(), false),
        AccountMeta::new(rent::id(), false),
    ];
    let instruction = Instruction::new_with_bytes(program_pubkey, &[], account_metas.clone());
    let message = Message::new(&[instruction], Some(&mint_keypair.pubkey()));
    let result = bank_client.send_and_confirm_message(&[&mint_keypair, &noop_keypair], message);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::ProgramFailedToComplete)
    );
}

#[cfg(feature = "bpf_rust")]
#[test]
fn test_program_bpf_ro_account_modify() {
    solana_logger::setup();

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(50);
    let mut bank = Bank::new(&genesis_config);
    let (name, id, entrypoint) = solana_bpf_loader_program!();
    bank.add_builtin(&name, id, entrypoint);
    let bank = Arc::new(bank);
    let bank_client = BankClient::new_shared(&bank);

    let program_id = load_bpf_program(
        &bank_client,
        &bpf_loader::id(),
        &mint_keypair,
        "solana_bpf_rust_ro_account_modify",
    );

    let argument_keypair = Keypair::new();
    let account = AccountSharedData::new(42, 100, &program_id);
    bank.store_account(&argument_keypair.pubkey(), &account);

    let from_keypair = Keypair::new();
    let account = AccountSharedData::new(84, 0, &solana_sdk::system_program::id());
    bank.store_account(&from_keypair.pubkey(), &account);

    let mint_pubkey = mint_keypair.pubkey();
    let account_metas = vec![
        AccountMeta::new_readonly(argument_keypair.pubkey(), false),
        AccountMeta::new_readonly(program_id, false),
    ];

    let instruction = Instruction::new_with_bytes(program_id, &[0], account_metas.clone());
    let message = Message::new(&[instruction], Some(&mint_pubkey));
    let result = bank_client.send_and_confirm_message(&[&mint_keypair], message);
    println!("result: {:?}", result);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::ReadonlyDataModified)
    );

    let instruction = Instruction::new_with_bytes(program_id, &[1], account_metas.clone());
    let message = Message::new(&[instruction], Some(&mint_pubkey));
    let result = bank_client.send_and_confirm_message(&[&mint_keypair], message);
    println!("result: {:?}", result);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::ReadonlyDataModified)
    );

    let instruction = Instruction::new_with_bytes(program_id, &[2], account_metas.clone());
    let message = Message::new(&[instruction], Some(&mint_pubkey));
    let result = bank_client.send_and_confirm_message(&[&mint_keypair], message);
    println!("result: {:?}", result);
    assert_eq!(
        result.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, InstructionError::ReadonlyDataModified)
    );
}
