use crate::account_state::{pubkey_to_address, LibraAccountState, ModuleBytes};
use crate::data_store::DataStore;
use crate::error_mappers::*;
use bytecode_verifier::verifier::{VerifiedModule, VerifiedScript};
use log::*;
use serde_derive::{Deserialize, Serialize};
use solana_sdk::{
    account::KeyedAccount,
    instruction::InstructionError,
    instruction_processor_utils::{limited_deserialize, next_keyed_account},
    loader_instruction::LoaderInstruction,
    move_loader::id,
    pubkey::Pubkey,
    sysvar::rent,
};
use types::{
    account_address::AccountAddress,
    account_config,
    identifier::Identifier,
    transaction::{Program, TransactionArgument, TransactionOutput},
};
use vm::{
    access::ModuleAccess,
    file_format::{CompiledModule, CompiledScript},
    gas_schedule::{MAXIMUM_NUMBER_OF_GAS_UNITS, MAX_PRICE_PER_GAS_UNIT},
    transaction_metadata::TransactionMetadata,
    vm_string::VMString,
};
use vm_cache_map::Arena;
use vm_runtime::{
    code_cache::{
        module_adapter::ModuleFetcherImpl,
        module_cache::{BlockModuleCache, ModuleCache, VMModuleCache},
    },
    txn_executor::TransactionExecutor,
};
use vm_runtime_types::value::Value;

pub fn process_instruction(
    _program_id: &Pubkey,
    keyed_accounts: &mut [KeyedAccount],
    data: &[u8],
) -> Result<(), InstructionError> {
    solana_logger::setup();

    match limited_deserialize(data)? {
        LoaderInstruction::Write { offset, bytes } => {
            MoveProcessor::do_write(keyed_accounts, offset, &bytes)
        }
        LoaderInstruction::Finalize => MoveProcessor::do_finalize(keyed_accounts),
        LoaderInstruction::InvokeMain { data } => {
            MoveProcessor::do_invoke_main(keyed_accounts, &data)
        }
    }
}

/// Command to invoke
#[derive(Debug, Serialize, Deserialize)]
pub enum InvokeCommand {
    /// Create a new genesis account
    CreateGenesis(u64),
    /// Run a Move program
    RunProgram {
        /// Sender of the "transaction", the "sender" who is running this program
        sender_address: AccountAddress,
        /// Name of the program's function to call
        function_name: String,
        /// Arguments to pass to the program being called
        args: Vec<TransactionArgument>,
    },
}

pub struct MoveProcessor {}

impl MoveProcessor {
    #[allow(clippy::needless_pass_by_value)]
    fn missing_account() -> InstructionError {
        debug!("Error: Missing account");
        InstructionError::InvalidAccountData
    }

    fn arguments_to_values(args: Vec<TransactionArgument>) -> Vec<Value> {
        let mut locals = vec![];
        for arg in args {
            locals.push(match arg {
                TransactionArgument::U64(i) => Value::u64(i),
                TransactionArgument::Address(a) => Value::address(a),
                TransactionArgument::ByteArray(b) => Value::byte_array(b),
                TransactionArgument::String(s) => Value::string(VMString::new(s)),
            });
        }
        locals
    }

    fn serialize_and_enforce_length(
        state: &LibraAccountState,
        data: &mut Vec<u8>,
    ) -> Result<(), InstructionError> {
        let original_len = data.len() as u64;
        let mut writer = std::io::Cursor::new(data);
        bincode::config()
            .limit(original_len)
            .serialize_into(&mut writer, &state)
            .map_err(map_data_error)?;
        if writer.position() < original_len {
            writer.set_position(original_len);
        }
        Ok(())
    }

    fn verify_program(
        script: &VerifiedScript,
        modules: &[VerifiedModule],
    ) -> Result<(LibraAccountState), InstructionError> {
        let mut script_bytes = vec![];
        script
            .as_inner()
            .serialize(&mut script_bytes)
            .map_err(map_failure_error)?;
        let mut modules_bytes = vec![];
        for module in modules.iter() {
            let mut buf = vec![];
            module
                .as_inner()
                .serialize(&mut buf)
                .map_err(map_failure_error)?;
            modules_bytes.push(ModuleBytes { bytes: buf });
        }
        Ok(LibraAccountState::VerifiedProgram {
            script_bytes,
            modules_bytes,
        })
    }

    fn deserialize_compiled_program(
        data: &[u8],
    ) -> Result<(CompiledScript, Vec<CompiledModule>), InstructionError> {
        match limited_deserialize(data)? {
            LibraAccountState::CompiledProgram(string) => {
                let program: Program = serde_json::from_str(&string).map_err(map_json_error)?;

                let script =
                    CompiledScript::deserialize(&program.code()).map_err(map_err_vm_status)?;
                let modules = program
                    .modules()
                    .iter()
                    .map(|bytes| CompiledModule::deserialize(&bytes))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_err_vm_status)?;

                Ok((script, modules))
            }
            _ => {
                debug!("Error: Program account does not contain a program");
                Err(InstructionError::InvalidArgument)
            }
        }
    }

    fn deserialize_verified_program(
        data: &[u8],
    ) -> Result<(VerifiedScript, Vec<VerifiedModule>), InstructionError> {
        match limited_deserialize(data)? {
            LibraAccountState::VerifiedProgram {
                script_bytes,
                modules_bytes,
            } => {
                let script =
                    VerifiedScript::deserialize(&script_bytes).map_err(map_err_vm_status)?;
                let modules = modules_bytes
                    .iter()
                    .map(|module_bytes| VerifiedModule::deserialize(&module_bytes.bytes))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_err_vm_status)?;

                Ok((script, modules))
            }
            _ => {
                debug!("Error: Program account does not contain a program");
                Err(InstructionError::InvalidArgument)
            }
        }
    }

    fn execute(
        sender_address: AccountAddress,
        function_name: &str,
        args: Vec<TransactionArgument>,
        script: VerifiedScript,
        modules: Vec<VerifiedModule>,
        data_store: &DataStore,
    ) -> Result<TransactionOutput, InstructionError> {
        let allocator = Arena::new();
        let code_cache = VMModuleCache::new(&allocator);
        let module_cache = BlockModuleCache::new(&code_cache, ModuleFetcherImpl::new(data_store));
        let mut modules_to_publish = vec![];

        let main_module = script.into_module();
        let module_id = main_module.self_id();
        module_cache.cache_module(main_module);
        for verified_module in modules {
            let mut raw_bytes = vec![];
            verified_module
                .as_inner()
                .serialize(&mut raw_bytes)
                .map_err(map_failure_error)?;
            modules_to_publish.push((verified_module.self_id(), raw_bytes));
            module_cache.cache_module(verified_module);
        }

        let mut txn_metadata = TransactionMetadata::default();
        txn_metadata.sender = sender_address;

        // Caps execution to the Libra prescribed 10 milliseconds
        txn_metadata.max_gas_amount = *MAXIMUM_NUMBER_OF_GAS_UNITS;
        txn_metadata.gas_unit_price = *MAX_PRICE_PER_GAS_UNIT;

        let mut vm = TransactionExecutor::new(&module_cache, data_store, txn_metadata);
        vm.execute_function(
            &module_id,
            &Identifier::new(function_name).unwrap(),
            Self::arguments_to_values(args),
        )
        .map_err(map_err_vm_status)?;

        Ok(vm
            .make_write_set(modules_to_publish, Ok(()))
            .map_err(map_err_vm_status)?)
    }

    fn keyed_accounts_to_data_store(
        keyed_accounts: &[KeyedAccount],
    ) -> Result<DataStore, InstructionError> {
        let mut data_store = DataStore::default();

        let mut keyed_accounts_iter = keyed_accounts.iter();
        let genesis = next_keyed_account(&mut keyed_accounts_iter)?;

        match limited_deserialize(&genesis.account.data)? {
            LibraAccountState::Genesis(write_set) => data_store.apply_write_set(&write_set),
            _ => {
                debug!("Must include a genesis account");
                return Err(InstructionError::InvalidArgument);
            }
        }

        for keyed_account in keyed_accounts_iter {
            if let LibraAccountState::User(owner, write_set) =
                limited_deserialize(&keyed_account.account.data)?
            {
                if owner != *genesis.unsigned_key() {
                    debug!("All user accounts must be owned by the genesis");
                    return Err(InstructionError::InvalidArgument);
                }
                data_store.apply_write_set(&write_set)
            }
        }
        Ok(data_store)
    }

    fn data_store_to_keyed_accounts(
        data_store: DataStore,
        keyed_accounts: &mut [KeyedAccount],
    ) -> Result<(), InstructionError> {
        let mut write_sets = data_store
            .into_write_sets()
            .map_err(|_| InstructionError::GenericError)?;

        let mut keyed_accounts_iter = keyed_accounts.iter_mut();

        // Genesis account holds both mint and stdlib
        let genesis = next_keyed_account(&mut keyed_accounts_iter)?;
        let genesis_key = *genesis.unsigned_key();
        let mut write_set = write_sets
            .remove(&account_config::association_address())
            .ok_or_else(Self::missing_account)?
            .into_mut();
        for (access_path, write_op) in write_sets
            .remove(&account_config::core_code_address())
            .ok_or_else(Self::missing_account)?
            .into_iter()
        {
            write_set.push((access_path, write_op));
        }
        let write_set = write_set.freeze().unwrap();
        Self::serialize_and_enforce_length(
            &LibraAccountState::Genesis(write_set),
            &mut genesis.account.data,
        )?;

        // Now do the rest of the accounts
        for keyed_account in keyed_accounts_iter {
            let write_set = write_sets
                .remove(&pubkey_to_address(keyed_account.unsigned_key()))
                .ok_or_else(Self::missing_account)?;
            Self::serialize_and_enforce_length(
                &LibraAccountState::User(genesis_key, write_set),
                &mut keyed_account.account.data,
            )?;
        }
        if !write_sets.is_empty() {
            debug!("Error: Missing keyed accounts");
            return Err(InstructionError::GenericError);
        }
        Ok(())
    }

    pub fn do_write(
        keyed_accounts: &mut [KeyedAccount],
        offset: u32,
        bytes: &[u8],
    ) -> Result<(), InstructionError> {
        let mut keyed_accounts_iter = keyed_accounts.iter_mut();
        let program = next_keyed_account(&mut keyed_accounts_iter)?;

        if program.signer_key().is_none() {
            debug!("Error: key[0] did not sign the transaction");
            return Err(InstructionError::MissingRequiredSignature);
        }
        let offset = offset as usize;
        let len = bytes.len();
        trace!("Write: offset={} length={}", offset, len);
        if program.account.data.len() < offset + len {
            debug!(
                "Error: Write overflow: {} < {}",
                program.account.data.len(),
                offset + len
            );
            return Err(InstructionError::AccountDataTooSmall);
        }
        program.account.data[offset..offset + len].copy_from_slice(&bytes);
        Ok(())
    }

    pub fn do_finalize(keyed_accounts: &mut [KeyedAccount]) -> Result<(), InstructionError> {
        let mut keyed_accounts_iter = keyed_accounts.iter_mut();
        let program = next_keyed_account(&mut keyed_accounts_iter)?;
        let rent = next_keyed_account(&mut keyed_accounts_iter)?;

        if program.signer_key().is_none() {
            debug!("Error: key[0] did not sign the transaction");
            return Err(InstructionError::MissingRequiredSignature);
        }

        rent::verify_rent_exemption(&program, &rent)?;

        let (compiled_script, compiled_modules) =
            Self::deserialize_compiled_program(&program.account.data)?;

        let verified_script = VerifiedScript::new(compiled_script).unwrap();
        let verified_modules = compiled_modules
            .into_iter()
            .map(VerifiedModule::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_vm_verification_error)?;

        Self::serialize_and_enforce_length(
            &Self::verify_program(&verified_script, &verified_modules)?,
            &mut program.account.data,
        )?;

        program.account.executable = true;

        info!(
            "Finalize: {:?}",
            program.signer_key().unwrap_or(&Pubkey::default())
        );
        Ok(())
    }

    pub fn do_invoke_main(
        keyed_accounts: &mut [KeyedAccount],
        data: &[u8],
    ) -> Result<(), InstructionError> {
        match limited_deserialize(&data)? {
            InvokeCommand::CreateGenesis(amount) => {
                let mut keyed_accounts_iter = keyed_accounts.iter_mut();
                let program = next_keyed_account(&mut keyed_accounts_iter)?;

                if program.account.owner != id() {
                    debug!("Error: Move program account not owned by Move loader");
                    return Err(InstructionError::InvalidArgument);
                }

                match limited_deserialize(&program.account.data)? {
                    LibraAccountState::Unallocated => Self::serialize_and_enforce_length(
                        &LibraAccountState::create_genesis(amount)?,
                        &mut program.account.data,
                    ),
                    _ => {
                        debug!("Error: Must provide an unallocated account");
                        Err(InstructionError::InvalidArgument)
                    }
                }
            }
            InvokeCommand::RunProgram {
                sender_address,
                function_name,
                args,
            } => {
                let mut keyed_accounts_iter = keyed_accounts.iter_mut();
                let program = next_keyed_account(&mut keyed_accounts_iter)?;

                if program.account.owner != id() {
                    debug!("Error: Move program account not owned by Move loader");
                    return Err(InstructionError::InvalidArgument);
                }
                if !program.account.executable {
                    debug!("Error: Move program account not executable");
                    return Err(InstructionError::AccountNotExecutable);
                }

                let data_accounts = keyed_accounts_iter.into_slice();

                let mut data_store = Self::keyed_accounts_to_data_store(&data_accounts)?;
                let (verified_script, verified_modules) =
                    Self::deserialize_verified_program(&program.account.data)?;

                let output = Self::execute(
                    sender_address,
                    &function_name,
                    args,
                    verified_script,
                    verified_modules,
                    &data_store,
                )?;
                for event in output.events() {
                    trace!("Event: {:?}", event);
                }

                data_store.apply_write_set(&output.write_set());
                Self::data_store_to_keyed_accounts(data_store, data_accounts)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::account::Account;
    use solana_sdk::rent::Rent;
    use solana_sdk::sysvar::rent;

    const BIG_ENOUGH: usize = 10_000;

    #[test]
    fn test_account_size() {
        let mut data =
            vec![0_u8; bincode::serialized_size(&LibraAccountState::Unallocated).unwrap() as usize];
        let len = data.len();
        assert_eq!(
            MoveProcessor::serialize_and_enforce_length(&LibraAccountState::Unallocated, &mut data),
            Ok(())
        );
        assert_eq!(len, data.len());

        data.resize(6000, 0);
        let len = data.len();
        assert_eq!(
            MoveProcessor::serialize_and_enforce_length(&LibraAccountState::Unallocated, &mut data),
            Ok(())
        );
        assert_eq!(len, data.len());

        data.resize(1, 0);
        assert_eq!(
            MoveProcessor::serialize_and_enforce_length(&LibraAccountState::Unallocated, &mut data),
            Err(InstructionError::AccountDataTooSmall)
        );
    }

    #[test]
    fn test_finalize() {
        solana_logger::setup();

        let code = "main() { return; }";
        let sender_address = AccountAddress::default();
        let mut program = LibraAccount::create_program(&sender_address, code, vec![]);
        let rent_id = rent::id();
        let mut rent_account = rent::create_account(1, &Rent::default());
        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&rent_id, false, &mut rent_account),
        ];
        MoveProcessor::do_finalize(&mut keyed_accounts).unwrap();
        let (_, _) = MoveProcessor::deserialize_verified_program(&program.account.data).unwrap();
    }

    #[test]
    fn test_create_genesis_account() {
        solana_logger::setup();

        let amount = 10_000_000;
        let mut unallocated = LibraAccount::create_unallocated();

        let mut keyed_accounts = vec![KeyedAccount::new(
            &unallocated.key,
            false,
            &mut unallocated.account,
        )];
        keyed_accounts[0].account.data.resize(BIG_ENOUGH, 0);
        MoveProcessor::do_invoke_main(
            &mut keyed_accounts,
            &bincode::serialize(&InvokeCommand::CreateGenesis(amount)).unwrap(),
        )
        .unwrap();

        assert_eq!(
            bincode::deserialize::<LibraAccountState>(
                &LibraAccount::create_genesis(amount).account.data
            )
            .unwrap(),
            bincode::deserialize::<LibraAccountState>(&keyed_accounts[0].account.data).unwrap()
        );
    }

    #[test]
    fn test_invoke_main() {
        solana_logger::setup();

        let code = "main() { return; }";
        let sender_address = AccountAddress::default();
        let mut program = LibraAccount::create_program(&sender_address, code, vec![]);
        let mut genesis = LibraAccount::create_genesis(1_000_000_000);

        let rent_id = rent::id();
        let mut rent_account = rent::create_account(1, &Rent::default());
        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&rent_id, false, &mut rent_account),
        ];

        MoveProcessor::do_finalize(&mut keyed_accounts).unwrap();

        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&genesis.key, false, &mut genesis.account),
        ];

        MoveProcessor::do_invoke_main(
            &mut keyed_accounts,
            &bincode::serialize(&InvokeCommand::RunProgram {
                sender_address,
                function_name: "main".to_string(),
                args: vec![],
            })
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn test_invoke_endless_loop() {
        solana_logger::setup();

        let code = "
            main() {
                loop {}
                return;
            }
        ";
        let sender_address = AccountAddress::default();
        let mut program = LibraAccount::create_program(&sender_address, code, vec![]);
        let mut genesis = LibraAccount::create_genesis(1_000_000_000);

        let rent_id = rent::id();
        let mut rent_account = rent::create_account(1, &Rent::default());
        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&rent_id, false, &mut rent_account),
        ];

        MoveProcessor::do_finalize(&mut keyed_accounts).unwrap();

        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&genesis.key, false, &mut genesis.account),
        ];

        assert_eq!(
            MoveProcessor::do_invoke_main(
                &mut keyed_accounts,
                &bincode::serialize(&InvokeCommand::RunProgram {
                    sender_address,
                    function_name: "main".to_string(),
                    args: vec![],
                })
                .unwrap(),
            ),
            Err(InstructionError::CustomError(4002))
        );
    }

    #[test]
    fn test_invoke_mint_to_address() {
        solana_logger::setup();

        let mut data_store = DataStore::default();

        let amount = 42;
        let mut accounts = mint_coins(amount).unwrap();
        let mut accounts_iter = accounts.iter_mut();

        let _program = next_libra_account(&mut accounts_iter).unwrap();
        let genesis = next_libra_account(&mut accounts_iter).unwrap();
        let payee = next_libra_account(&mut accounts_iter).unwrap();
        match bincode::deserialize(&payee.account.data).unwrap() {
            LibraAccountState::User(owner, write_set) => {
                if owner != genesis.key {
                    panic!();
                }
                data_store.apply_write_set(&write_set)
            }
            _ => panic!("Invalid account state"),
        }

        let payee_resource = data_store.read_account_resource(&payee.address).unwrap();
        println!("{}:{}", line!(), file!());

        assert_eq!(amount, payee_resource.balance());
        assert_eq!(0, payee_resource.sequence_number());
    }

    #[test]
    fn test_invoke_pay_from_sender() {
        solana_logger::setup();
        let amount_to_mint = 42;
        let mut accounts = mint_coins(amount_to_mint).unwrap();
        let mut accounts_iter = accounts.iter_mut();

        let _program = next_libra_account(&mut accounts_iter).unwrap();
        let genesis = next_libra_account(&mut accounts_iter).unwrap();
        let sender = next_libra_account(&mut accounts_iter).unwrap();

        let code = "
            import 0x0.LibraAccount;
            import 0x0.LibraCoin;
            main(payee: address, amount: u64) {
                LibraAccount.pay_from_sender(move(payee), move(amount));
                return;
            }
        ";
        let mut program = LibraAccount::create_program(&genesis.address, code, vec![]);
        let mut payee = LibraAccount::create_unallocated();

        let rent_id = rent::id();
        let mut rent_account = rent::create_account(1, &Rent::default());
        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&rent_id, false, &mut rent_account),
        ];
        keyed_accounts[0].account.data.resize(BIG_ENOUGH, 0);

        MoveProcessor::do_finalize(&mut keyed_accounts).unwrap();

        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&genesis.key, false, &mut genesis.account),
            KeyedAccount::new(&sender.key, false, &mut sender.account),
            KeyedAccount::new(&payee.key, false, &mut payee.account),
        ];
        keyed_accounts[2].account.data.resize(BIG_ENOUGH, 0);
        keyed_accounts[3].account.data.resize(BIG_ENOUGH, 0);

        let amount = 2;
        MoveProcessor::do_invoke_main(
            &mut keyed_accounts,
            &bincode::serialize(&InvokeCommand::RunProgram {
                sender_address: sender.address.clone(),
                function_name: "main".to_string(),
                args: vec![
                    TransactionArgument::Address(payee.address.clone()),
                    TransactionArgument::U64(amount),
                ],
            })
            .unwrap(),
        )
        .unwrap();

        let data_store = MoveProcessor::keyed_accounts_to_data_store(&keyed_accounts[1..]).unwrap();
        let sender_resource = data_store.read_account_resource(&sender.address).unwrap();
        let payee_resource = data_store.read_account_resource(&payee.address).unwrap();

        assert_eq!(amount_to_mint - amount, sender_resource.balance());
        assert_eq!(0, sender_resource.sequence_number());
        assert_eq!(amount, payee_resource.balance());
        assert_eq!(0, payee_resource.sequence_number());
    }

    #[test]
    fn test_invoke_local_module() {
        solana_logger::setup();

        let code = "
            modules:

            module M {
                public universal_truth(): u64 {
                    return 42;
                }
            }

            script:

            import Transaction.M;
            main() {
                let x: u64;
                x = M.universal_truth();
                return;
            }
        ";
        let mut genesis = LibraAccount::create_genesis(1_000_000_000);
        let mut payee = LibraAccount::create_unallocated();
        let mut program = LibraAccount::create_program(&payee.address, code, vec![]);

        let rent_id = rent::id();
        let mut rent_account = rent::create_account(1, &Rent::default());
        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&rent_id, false, &mut rent_account),
        ];
        keyed_accounts[0].account.data.resize(BIG_ENOUGH, 0);

        MoveProcessor::do_finalize(&mut keyed_accounts).unwrap();

        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&genesis.key, false, &mut genesis.account),
            KeyedAccount::new(&payee.key, false, &mut payee.account),
        ];
        keyed_accounts[2].account.data.resize(BIG_ENOUGH, 0);

        MoveProcessor::do_invoke_main(
            &mut keyed_accounts,
            &bincode::serialize(&InvokeCommand::RunProgram {
                sender_address: payee.address,
                function_name: "main".to_string(),
                args: vec![],
            })
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn test_invoke_published_module() {
        solana_logger::setup();

        // First publish the module

        let code = "
            module M {
                public universal_truth(): u64 {
                    return 42;
                }
            }
        ";
        let mut module = LibraAccount::create_unallocated();
        let mut program = LibraAccount::create_program(&module.address, code, vec![]);
        let mut genesis = LibraAccount::create_genesis(1_000_000_000);

        let rent_id = rent::id();
        let mut rent_account = rent::create_account(1, &Rent::default());
        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&rent_id, false, &mut rent_account),
        ];
        keyed_accounts[0].account.data.resize(BIG_ENOUGH, 0);

        MoveProcessor::do_finalize(&mut keyed_accounts).unwrap();

        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&genesis.key, false, &mut genesis.account),
            KeyedAccount::new(&module.key, false, &mut module.account),
        ];
        keyed_accounts[2].account.data.resize(BIG_ENOUGH, 0);

        MoveProcessor::do_invoke_main(
            &mut keyed_accounts,
            &bincode::serialize(&InvokeCommand::RunProgram {
                sender_address: module.address,
                function_name: "main".to_string(),
                args: vec![],
            })
            .unwrap(),
        )
        .unwrap();

        // Next invoke the published module

        let code = format!(
            "
            import 0x{}.M;
            main() {{
                let x: u64;
                x = M.universal_truth();
                return;
            }}
            ",
            module.address
        );
        let mut program =
            LibraAccount::create_program(&module.address, &code, vec![&module.account.data]);

        let rent_id = rent::id();
        let mut rent_account = rent::create_account(1, &Rent::default());
        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&rent_id, false, &mut rent_account),
        ];

        MoveProcessor::do_finalize(&mut keyed_accounts).unwrap();

        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&genesis.key, false, &mut genesis.account),
            KeyedAccount::new(&module.key, false, &mut module.account),
        ];

        MoveProcessor::do_invoke_main(
            &mut keyed_accounts,
            &bincode::serialize(&InvokeCommand::RunProgram {
                sender_address: program.address,
                function_name: "main".to_string(),
                args: vec![],
            })
            .unwrap(),
        )
        .unwrap();
    }

    // Helpers

    fn mint_coins(amount: u64) -> Result<Vec<LibraAccount>, InstructionError> {
        let code = "
            import 0x0.LibraAccount;
            import 0x0.LibraCoin;
            main(payee: address, amount: u64) {
                LibraAccount.mint_to_address(move(payee), move(amount));
                return;
            }
        ";
        let mut genesis = LibraAccount::create_genesis(1_000_000_000);
        let mut program = LibraAccount::create_program(&genesis.address, code, vec![]);
        let mut payee = LibraAccount::create_unallocated();

        let rent_id = rent::id();
        let mut rent_account = rent::create_account(1, &Rent::default());
        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&rent_id, false, &mut rent_account),
        ];
        keyed_accounts[0].account.data.resize(BIG_ENOUGH, 0);

        MoveProcessor::do_finalize(&mut keyed_accounts).unwrap();

        let mut keyed_accounts = vec![
            KeyedAccount::new(&program.key, true, &mut program.account),
            KeyedAccount::new(&genesis.key, false, &mut genesis.account),
            KeyedAccount::new(&payee.key, false, &mut payee.account),
        ];
        keyed_accounts[2].account.data.resize(BIG_ENOUGH, 0);

        MoveProcessor::do_invoke_main(
            &mut keyed_accounts,
            &bincode::serialize(&InvokeCommand::RunProgram {
                sender_address: genesis.address.clone(),
                function_name: "main".to_string(),
                args: vec![
                    TransactionArgument::Address(pubkey_to_address(&payee.key)),
                    TransactionArgument::U64(amount),
                ],
            })
            .unwrap(),
        )
        .unwrap();

        Ok(vec![
            LibraAccount::new(program.key, program.account),
            LibraAccount::new(genesis.key, genesis.account),
            LibraAccount::new(payee.key, payee.account),
        ])
    }

    #[derive(Eq, PartialEq, Debug, Default)]
    struct LibraAccount {
        pub key: Pubkey,
        pub address: AccountAddress,
        pub account: Account,
    }

    pub fn next_libra_account<I: Iterator>(iter: &mut I) -> Result<I::Item, InstructionError> {
        iter.next().ok_or(InstructionError::NotEnoughAccountKeys)
    }

    impl LibraAccount {
        pub fn new(key: Pubkey, account: Account) -> Self {
            let address = pubkey_to_address(&key);
            Self {
                key,
                address,
                account,
            }
        }

        pub fn create_unallocated() -> Self {
            let key = Pubkey::new_rand();
            let account = Account {
                lamports: 1,
                data: bincode::serialize(&LibraAccountState::create_unallocated()).unwrap(),
                owner: id(),
                ..Account::default()
            };
            Self::new(key, account)
        }

        pub fn create_genesis(amount: u64) -> Self {
            let account = Account {
                lamports: 1,
                owner: id(),
                ..Account::default()
            };
            let mut genesis = Self::new(
                Pubkey::new(&account_config::association_address().to_vec()),
                account,
            );
            let pre_data = LibraAccountState::create_genesis(amount).unwrap();
            let _hi = "hello";
            genesis.account.data = bincode::serialize(&pre_data).unwrap();
            genesis
        }

        pub fn create_program(
            sender_address: &AccountAddress,
            code: &str,
            deps: Vec<&Vec<u8>>,
        ) -> Self {
            let mut program = Self::create_unallocated();
            program.account.data = bincode::serialize(&LibraAccountState::create_program(
                sender_address,
                code,
                deps,
            ))
            .unwrap();
            program.account.executable = true;
            program
        }
    }
}
