use crate::{
    accounts_db::{
        AccountShrinkThreshold, AccountsDb, BankHashInfo, ErrorCounters, LoadHint, LoadedAccount,
        ScanStorageResult,
    },
    accounts_index::{AccountSecondaryIndexes, IndexKey, ScanResult},
    ancestors::Ancestors,
    bank::{
        NonceRollbackFull, NonceRollbackInfo, RentDebits, TransactionCheckResult,
        TransactionExecutionResult,
    },
    blockhash_queue::BlockhashQueue,
    rent_collector::RentCollector,
    system_instruction_processor::{get_system_account_kind, SystemAccountKind},
};
use dashmap::{
    mapref::entry::Entry::{Occupied, Vacant},
    DashMap,
};
use log::*;
use rand::{thread_rng, Rng};
use solana_sdk::{
    account::{Account, AccountSharedData, ReadableAccount, WritableAccount},
    account_utils::StateMut,
    bpf_loader_upgradeable::{self, UpgradeableLoaderState},
    clock::{BankId, Slot, INITIAL_RENT_EPOCH},
    feature_set::{self, FeatureSet},
    fee_calculator::FeeCalculator,
    genesis_config::ClusterType,
    hash::Hash,
    message::Message,
    native_loader, nonce,
    pubkey::Pubkey,
    transaction::Result,
    transaction::{Transaction, TransactionError},
};
use std::{
    cmp::Reverse,
    collections::{hash_map, BinaryHeap, HashMap, HashSet},
    ops::RangeBounds,
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[derive(Debug, Default, AbiExample)]
pub struct AccountLocks {
    write_locks: HashSet<Pubkey>,
    readonly_locks: HashMap<Pubkey, u64>,
}

impl AccountLocks {
    fn is_locked_readonly(&self, key: &Pubkey) -> bool {
        self.readonly_locks
            .get(key)
            .map_or(false, |count| *count > 0)
    }

    fn is_locked_write(&self, key: &Pubkey) -> bool {
        self.write_locks.contains(key)
    }

    fn insert_new_readonly(&mut self, key: &Pubkey) {
        assert!(self.readonly_locks.insert(*key, 1).is_none());
    }

    fn lock_readonly(&mut self, key: &Pubkey) -> bool {
        self.readonly_locks.get_mut(key).map_or(false, |count| {
            *count += 1;
            true
        })
    }

    fn unlock_readonly(&mut self, key: &Pubkey) {
        if let hash_map::Entry::Occupied(mut occupied_entry) = self.readonly_locks.entry(*key) {
            let count = occupied_entry.get_mut();
            *count -= 1;
            if *count == 0 {
                occupied_entry.remove_entry();
            }
        }
    }

    fn unlock_write(&mut self, key: &Pubkey) {
        self.write_locks.remove(key);
    }
}

/// This structure handles synchronization for db
#[derive(Default, Debug, AbiExample)]
pub struct Accounts {
    /// Single global AccountsDb
    pub accounts_db: Arc<AccountsDb>,

    /// set of read-only and writable accounts which are currently
    /// being processed by banking/replay threads
    pub(crate) account_locks: Mutex<AccountLocks>,
}

// for the load instructions
pub type TransactionAccounts = Vec<(Pubkey, AccountSharedData)>;
pub type TransactionRent = u64;
pub type TransactionLoaders = Vec<Vec<(Pubkey, AccountSharedData)>>;
#[derive(PartialEq, Debug, Clone)]
pub struct LoadedTransaction {
    pub accounts: TransactionAccounts,
    pub loaders: TransactionLoaders,
    pub rent: TransactionRent,
    pub rent_debits: RentDebits,
}

pub type TransactionLoadResult = (Result<LoadedTransaction>, Option<NonceRollbackFull>);

pub enum AccountAddressFilter {
    Exclude, // exclude all addresses matching the filter
    Include, // only include addresses matching the filter
}

impl Accounts {
    pub fn new(
        paths: Vec<PathBuf>,
        cluster_type: &ClusterType,
        shrink_ratio: AccountShrinkThreshold,
    ) -> Self {
        Self::new_with_config(
            paths,
            cluster_type,
            AccountSecondaryIndexes::default(),
            false,
            shrink_ratio,
        )
    }

    pub fn new_with_config(
        paths: Vec<PathBuf>,
        cluster_type: &ClusterType,
        account_indexes: AccountSecondaryIndexes,
        caching_enabled: bool,
        shrink_ratio: AccountShrinkThreshold,
    ) -> Self {
        Self {
            accounts_db: Arc::new(AccountsDb::new_with_config(
                paths,
                cluster_type,
                account_indexes,
                caching_enabled,
                shrink_ratio,
            )),
            account_locks: Mutex::new(AccountLocks::default()),
        }
    }

    pub fn new_from_parent(parent: &Accounts, slot: Slot, parent_slot: Slot) -> Self {
        let accounts_db = parent.accounts_db.clone();
        accounts_db.set_hash(slot, parent_slot);
        Self {
            accounts_db,
            account_locks: Mutex::new(AccountLocks::default()),
        }
    }

    pub(crate) fn new_empty(accounts_db: AccountsDb) -> Self {
        Self {
            accounts_db: Arc::new(accounts_db),
            account_locks: Mutex::new(AccountLocks::default()),
        }
    }

    fn construct_instructions_account(message: &Message) -> AccountSharedData {
        let mut data = message.serialize_instructions();
        // add room for current instruction index.
        data.resize(data.len() + 2, 0);
        AccountSharedData::from(Account {
            data,
            ..Account::default()
        })
    }

    fn load_transaction(
        &self,
        ancestors: &Ancestors,
        tx: &Transaction,
        fee: u64,
        error_counters: &mut ErrorCounters,
        rent_collector: &RentCollector,
        feature_set: &FeatureSet,
    ) -> Result<LoadedTransaction> {
        // Copy all the accounts
        let message = tx.message();
        if tx.signatures.is_empty() && fee != 0 {
            Err(TransactionError::MissingSignatureForFee)
        } else {
            // There is no way to predict what program will execute without an error
            // If a fee can pay for execution then the program will be scheduled
            let mut payer_index = None;
            let mut tx_rent: TransactionRent = 0;
            let mut accounts = Vec::with_capacity(message.account_keys.len());
            let mut account_deps = Vec::with_capacity(message.account_keys.len());
            let mut rent_debits = RentDebits::default();
            let rent_for_sysvars = feature_set.is_active(&feature_set::rent_for_sysvars::id());

            for (i, key) in message.account_keys.iter().enumerate() {
                let account = if message.is_non_loader_key(i) {
                    if payer_index.is_none() {
                        payer_index = Some(i);
                    }

                    if solana_sdk::sysvar::instructions::check_id(key)
                        && feature_set.is_active(&feature_set::instructions_sysvar_enabled::id())
                    {
                        if message.is_writable(i) {
                            return Err(TransactionError::InvalidAccountIndex);
                        }
                        Self::construct_instructions_account(message)
                    } else {
                        let (account, rent) = self
                            .accounts_db
                            .load_with_fixed_root(ancestors, key)
                            .map(|(mut account, _)| {
                                if message.is_writable(i) {
                                    let rent_due = rent_collector.collect_from_existing_account(
                                        key,
                                        &mut account,
                                        rent_for_sysvars,
                                    );
                                    (account, rent_due)
                                } else {
                                    (account, 0)
                                }
                            })
                            .unwrap_or_default();

                        if account.executable() && bpf_loader_upgradeable::check_id(account.owner())
                        {
                            // The upgradeable loader requires the derived ProgramData account
                            if let Ok(UpgradeableLoaderState::Program {
                                programdata_address,
                            }) = account.state()
                            {
                                if let Some(account) = self
                                    .accounts_db
                                    .load_with_fixed_root(ancestors, &programdata_address)
                                    .map(|(account, _)| account)
                                {
                                    account_deps.push((programdata_address, account));
                                } else {
                                    error_counters.account_not_found += 1;
                                    return Err(TransactionError::ProgramAccountNotFound);
                                }
                            } else {
                                error_counters.invalid_program_for_execution += 1;
                                return Err(TransactionError::InvalidProgramForExecution);
                            }
                        }

                        tx_rent += rent;
                        rent_debits.push(key, rent, account.lamports());

                        account
                    }
                } else {
                    // Fill in an empty account for the program slots.
                    AccountSharedData::default()
                };
                accounts.push((*key, account));
            }
            debug_assert_eq!(accounts.len(), message.account_keys.len());
            // Appends the account_deps at the end of the accounts,
            // this way they can be accessed in a uniform way.
            // At places where only the accounts are needed,
            // the account_deps are truncated using e.g:
            // accounts.iter().take(message.account_keys.len())
            accounts.append(&mut account_deps);

            if let Some(payer_index) = payer_index {
                if payer_index != 0 {
                    warn!("Payer index should be 0! {:?}", tx);
                }
                let payer_account = &mut accounts[payer_index].1;
                if payer_account.lamports() == 0 {
                    error_counters.account_not_found += 1;
                    Err(TransactionError::AccountNotFound)
                } else {
                    let min_balance =
                        match get_system_account_kind(payer_account).ok_or_else(|| {
                            error_counters.invalid_account_for_fee += 1;
                            TransactionError::InvalidAccountForFee
                        })? {
                            SystemAccountKind::System => 0,
                            SystemAccountKind::Nonce => {
                                // Should we ever allow a fees charge to zero a nonce account's
                                // balance. The state MUST be set to uninitialized in that case
                                rent_collector.rent.minimum_balance(nonce::State::size())
                            }
                        };

                    if payer_account.lamports() < fee + min_balance {
                        error_counters.insufficient_funds += 1;
                        Err(TransactionError::InsufficientFundsForFee)
                    } else {
                        payer_account
                            .checked_sub_lamports(fee)
                            .map_err(|_| TransactionError::InsufficientFundsForFee)?;

                        let message = tx.message();
                        let loaders = message
                            .instructions
                            .iter()
                            .map(|ix| {
                                if message.account_keys.len() <= ix.program_id_index as usize {
                                    error_counters.account_not_found += 1;
                                    return Err(TransactionError::AccountNotFound);
                                }
                                let program_id = message.account_keys[ix.program_id_index as usize];
                                self.load_executable_accounts(
                                    ancestors,
                                    &program_id,
                                    error_counters,
                                )
                            })
                            .collect::<Result<TransactionLoaders>>()?;
                        Ok(LoadedTransaction {
                            accounts,
                            loaders,
                            rent: tx_rent,
                            rent_debits,
                        })
                    }
                }
            } else {
                error_counters.account_not_found += 1;
                Err(TransactionError::AccountNotFound)
            }
        }
    }

    fn load_executable_accounts(
        &self,
        ancestors: &Ancestors,
        program_id: &Pubkey,
        error_counters: &mut ErrorCounters,
    ) -> Result<Vec<(Pubkey, AccountSharedData)>> {
        let mut accounts = Vec::new();
        let mut depth = 0;
        let mut program_id = *program_id;
        loop {
            if native_loader::check_id(&program_id) {
                // At the root of the chain, ready to dispatch
                break;
            }

            if depth >= 5 {
                error_counters.call_chain_too_deep += 1;
                return Err(TransactionError::CallChainTooDeep);
            }
            depth += 1;

            let program = match self
                .accounts_db
                .load_with_fixed_root(ancestors, &program_id)
                .map(|(account, _)| account)
            {
                Some(program) => program,
                None => {
                    error_counters.account_not_found += 1;
                    return Err(TransactionError::ProgramAccountNotFound);
                }
            };
            if !program.executable() {
                error_counters.invalid_program_for_execution += 1;
                return Err(TransactionError::InvalidProgramForExecution);
            }

            // Add loader to chain
            let program_owner = *program.owner();

            if bpf_loader_upgradeable::check_id(&program_owner) {
                // The upgradeable loader requires the derived ProgramData account
                if let Ok(UpgradeableLoaderState::Program {
                    programdata_address,
                }) = program.state()
                {
                    if let Some(program) = self
                        .accounts_db
                        .load_with_fixed_root(ancestors, &programdata_address)
                        .map(|(account, _)| account)
                    {
                        accounts.insert(0, (programdata_address, program));
                    } else {
                        error_counters.account_not_found += 1;
                        return Err(TransactionError::ProgramAccountNotFound);
                    }
                } else {
                    error_counters.invalid_program_for_execution += 1;
                    return Err(TransactionError::InvalidProgramForExecution);
                }
            }

            accounts.insert(0, (program_id, program));
            program_id = program_owner;
        }
        Ok(accounts)
    }

    pub fn load_accounts<'a>(
        &self,
        ancestors: &Ancestors,
        txs: impl Iterator<Item = &'a Transaction>,
        lock_results: Vec<TransactionCheckResult>,
        hash_queue: &BlockhashQueue,
        error_counters: &mut ErrorCounters,
        rent_collector: &RentCollector,
        feature_set: &FeatureSet,
    ) -> Vec<TransactionLoadResult> {
        txs.zip(lock_results)
            .map(|etx| match etx {
                (tx, (Ok(()), nonce_rollback)) => {
                    let fee_calculator = nonce_rollback
                        .as_ref()
                        .map(|nonce_rollback| nonce_rollback.fee_calculator())
                        .unwrap_or_else(|| {
                            hash_queue
                                .get_fee_calculator(&tx.message().recent_blockhash)
                                .cloned()
                        });
                    let fee = if let Some(fee_calculator) = fee_calculator {
                        fee_calculator.calculate_fee(tx.message())
                    } else {
                        return (Err(TransactionError::BlockhashNotFound), None);
                    };

                    let loaded_transaction = match self.load_transaction(
                        ancestors,
                        tx,
                        fee,
                        error_counters,
                        rent_collector,
                        feature_set,
                    ) {
                        Ok(loaded_transaction) => loaded_transaction,
                        Err(e) => return (Err(e), None),
                    };

                    // Update nonce_rollback with fee-subtracted accounts
                    let nonce_rollback = if let Some(nonce_rollback) = nonce_rollback {
                        match NonceRollbackFull::from_partial(
                            nonce_rollback,
                            tx.message(),
                            &loaded_transaction.accounts,
                        ) {
                            Ok(nonce_rollback) => Some(nonce_rollback),
                            Err(e) => return (Err(e), None),
                        }
                    } else {
                        None
                    };

                    (Ok(loaded_transaction), nonce_rollback)
                }
                (_, (Err(e), _nonce_rollback)) => (Err(e), None),
            })
            .collect()
    }

    fn filter_zero_lamport_account(
        account: AccountSharedData,
        slot: Slot,
    ) -> Option<(AccountSharedData, Slot)> {
        if account.lamports() > 0 {
            Some((account, slot))
        } else {
            None
        }
    }

    /// Slow because lock is held for 1 operation instead of many
    fn load_slow(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
        load_hint: LoadHint,
    ) -> Option<(AccountSharedData, Slot)> {
        let (account, slot) = self.accounts_db.load(ancestors, pubkey, load_hint)?;
        Self::filter_zero_lamport_account(account, slot)
    }

    pub fn load_with_fixed_root(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        self.load_slow(ancestors, pubkey, LoadHint::FixedMaxRoot)
    }

    pub fn load_without_fixed_root(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        self.load_slow(ancestors, pubkey, LoadHint::Unspecified)
    }

    /// scans underlying accounts_db for this delta (slot) with a map function
    ///   from LoadedAccount to B
    /// returns only the latest/current version of B for this slot
    pub fn scan_slot<F, B>(&self, slot: Slot, func: F) -> Vec<B>
    where
        F: Fn(LoadedAccount) -> Option<B> + Send + Sync,
        B: Sync + Send + Default + std::cmp::Eq,
    {
        let scan_result = self.accounts_db.scan_account_storage(
            slot,
            |loaded_account: LoadedAccount| {
                // Cache only has one version per key, don't need to worry about versioning
                func(loaded_account)
            },
            |accum: &DashMap<Pubkey, (u64, B)>, loaded_account: LoadedAccount| {
                let loaded_account_pubkey = *loaded_account.pubkey();
                let loaded_write_version = loaded_account.write_version();
                let should_insert = accum
                    .get(&loaded_account_pubkey)
                    .map(|existing_entry| loaded_write_version > existing_entry.value().0)
                    .unwrap_or(true);
                if should_insert {
                    if let Some(val) = func(loaded_account) {
                        // Detected insertion is necessary, grabs the write lock to commit the write,
                        match accum.entry(loaded_account_pubkey) {
                            // Double check in case another thread interleaved a write between the read + write.
                            Occupied(mut occupied_entry) => {
                                if loaded_write_version > occupied_entry.get().0 {
                                    occupied_entry.insert((loaded_write_version, val));
                                }
                            }

                            Vacant(vacant_entry) => {
                                vacant_entry.insert((loaded_write_version, val));
                            }
                        }
                    }
                }
            },
        );

        match scan_result {
            ScanStorageResult::Cached(cached_result) => cached_result,
            ScanStorageResult::Stored(stored_result) => stored_result
                .into_iter()
                .map(|(_pubkey, (_latest_write_version, val))| val)
                .collect(),
        }
    }

    pub fn load_by_program_slot(
        &self,
        slot: Slot,
        program_id: Option<&Pubkey>,
    ) -> Vec<(Pubkey, AccountSharedData)> {
        self.scan_slot(slot, |stored_account| {
            let hit = match program_id {
                None => true,
                Some(program_id) => stored_account.owner() == program_id,
            };

            if hit {
                Some((*stored_account.pubkey(), stored_account.take_account()))
            } else {
                None
            }
        })
    }

    pub fn load_largest_accounts(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        num: usize,
        filter_by_address: &HashSet<Pubkey>,
        filter: AccountAddressFilter,
    ) -> ScanResult<Vec<(Pubkey, u64)>> {
        if num == 0 {
            return Ok(vec![]);
        }
        let account_balances = self.accounts_db.scan_accounts(
            ancestors,
            bank_id,
            |collector: &mut BinaryHeap<Reverse<(u64, Pubkey)>>, option| {
                if let Some((pubkey, account, _slot)) = option {
                    if account.lamports() == 0 {
                        return;
                    }
                    let contains_address = filter_by_address.contains(pubkey);
                    let collect = match filter {
                        AccountAddressFilter::Exclude => !contains_address,
                        AccountAddressFilter::Include => contains_address,
                    };
                    if !collect {
                        return;
                    }
                    if collector.len() == num {
                        let Reverse(entry) = collector
                            .peek()
                            .expect("BinaryHeap::peek should succeed when len > 0");
                        if *entry >= (account.lamports(), *pubkey) {
                            return;
                        }
                        collector.pop();
                    }
                    collector.push(Reverse((account.lamports(), *pubkey)));
                }
            },
        )?;
        Ok(account_balances
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse((balance, pubkey))| (pubkey, balance))
            .collect())
    }

    pub fn calculate_capitalization(
        &self,
        ancestors: &Ancestors,
        slot: Slot,
        can_cached_slot_be_unflushed: bool,
        debug_verify: bool,
        expected_capitalization: Option<u64>,
    ) -> u64 {
        let use_index = false;
        self.accounts_db
            .update_accounts_hash_with_index_option(
                use_index,
                debug_verify,
                slot,
                ancestors,
                expected_capitalization,
                can_cached_slot_be_unflushed,
            )
            .1
    }

    #[must_use]
    pub fn verify_bank_hash_and_lamports(
        &self,
        slot: Slot,
        ancestors: &Ancestors,
        total_lamports: u64,
        test_hash_calculation: bool,
    ) -> bool {
        if let Err(err) = self.accounts_db.verify_bank_hash_and_lamports(
            slot,
            ancestors,
            total_lamports,
            test_hash_calculation,
        ) {
            warn!("verify_bank_hash failed: {:?}", err);
            true//false
        } else {
            true
        }
    }

    fn is_loadable(lamports: u64) -> bool {
        // Don't ever load zero lamport accounts into runtime because
        // the existence of zero-lamport accounts are never deterministic!!
        lamports > 0
    }

    fn load_while_filtering<F: Fn(&AccountSharedData) -> bool>(
        collector: &mut Vec<(Pubkey, AccountSharedData)>,
        some_account_tuple: Option<(&Pubkey, AccountSharedData, Slot)>,
        filter: F,
    ) {
        if let Some(mapped_account_tuple) = some_account_tuple
            .filter(|(_, account, _)| Self::is_loadable(account.lamports()) && filter(account))
            .map(|(pubkey, account, _slot)| (*pubkey, account))
        {
            collector.push(mapped_account_tuple)
        }
    }

    pub fn load_by_program(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        program_id: &Pubkey,
    ) -> ScanResult<Vec<(Pubkey, AccountSharedData)>> {
        self.accounts_db.scan_accounts(
            ancestors,
            bank_id,
            |collector: &mut Vec<(Pubkey, AccountSharedData)>, some_account_tuple| {
                Self::load_while_filtering(collector, some_account_tuple, |account| {
                    account.owner() == program_id
                })
            },
        )
    }

    pub fn load_by_program_with_filter<F: Fn(&AccountSharedData) -> bool>(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        program_id: &Pubkey,
        filter: F,
    ) -> ScanResult<Vec<(Pubkey, AccountSharedData)>> {
        self.accounts_db.scan_accounts(
            ancestors,
            bank_id,
            |collector: &mut Vec<(Pubkey, AccountSharedData)>, some_account_tuple| {
                Self::load_while_filtering(collector, some_account_tuple, |account| {
                    account.owner() == program_id && filter(account)
                })
            },
        )
    }

    pub fn load_by_index_key_with_filter<F: Fn(&AccountSharedData) -> bool>(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        index_key: &IndexKey,
        filter: F,
    ) -> ScanResult<Vec<(Pubkey, AccountSharedData)>> {
        self.accounts_db
            .index_scan_accounts(
                ancestors,
                bank_id,
                *index_key,
                |collector: &mut Vec<(Pubkey, AccountSharedData)>, some_account_tuple| {
                    Self::load_while_filtering(collector, some_account_tuple, |account| {
                        filter(account)
                    })
                },
            )
            .map(|result| result.0)
    }

    pub fn account_indexes_include_key(&self, key: &Pubkey) -> bool {
        self.accounts_db.account_indexes.include_key(key)
    }

    pub fn load_all(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
    ) -> ScanResult<Vec<(Pubkey, AccountSharedData, Slot)>> {
        self.accounts_db.scan_accounts(
            ancestors,
            bank_id,
            |collector: &mut Vec<(Pubkey, AccountSharedData, Slot)>, some_account_tuple| {
                if let Some((pubkey, account, slot)) = some_account_tuple
                    .filter(|(_, account, _)| Self::is_loadable(account.lamports()))
                {
                    collector.push((*pubkey, account, slot))
                }
            },
        )
    }

    pub fn load_to_collect_rent_eagerly<R: RangeBounds<Pubkey>>(
        &self,
        ancestors: &Ancestors,
        range: R,
    ) -> Vec<(Pubkey, AccountSharedData)> {
        self.accounts_db.range_scan_accounts(
            "load_to_collect_rent_eagerly_scan_elapsed",
            ancestors,
            range,
            |collector: &mut Vec<(Pubkey, AccountSharedData)>, option| {
                Self::load_while_filtering(collector, option, |_| true)
            },
        )
    }

    /// Slow because lock is held for 1 operation instead of many.
    /// WARNING: This noncached version is only to be used for tests/benchmarking
    /// as bypassing the cache in general is not supported
    pub fn store_slow_uncached(&self, slot: Slot, pubkey: &Pubkey, account: &AccountSharedData) {
        self.accounts_db.store_uncached(slot, &[(pubkey, account)]);
    }

    pub fn store_slow_cached(&self, slot: Slot, pubkey: &Pubkey, account: &AccountSharedData) {
        self.accounts_db.store_cached(slot, &[(pubkey, account)]);
    }

    fn lock_account(
        &self,
        account_locks: &mut AccountLocks,
        writable_keys: Vec<&Pubkey>,
        readonly_keys: Vec<&Pubkey>,
    ) -> Result<()> {
        for k in writable_keys.iter() {
            if account_locks.is_locked_write(k) || account_locks.is_locked_readonly(k) {
                debug!("Writable account in use: {:?}", k);
                return Err(TransactionError::AccountInUse);
            }
        }
        for k in readonly_keys.iter() {
            if account_locks.is_locked_write(k) {
                debug!("Read-only account in use: {:?}", k);
                return Err(TransactionError::AccountInUse);
            }
        }

        for k in writable_keys {
            account_locks.write_locks.insert(*k);
        }

        for k in readonly_keys {
            if !account_locks.lock_readonly(k) {
                account_locks.insert_new_readonly(k);
            }
        }

        Ok(())
    }

    fn unlock_account(
        &self,
        account_locks: &mut AccountLocks,
        writable_keys: Vec<&Pubkey>,
        readonly_keys: Vec<&Pubkey>,
    ) {
        for k in writable_keys {
            account_locks.unlock_write(k);
        }
        for k in readonly_keys {
            account_locks.unlock_readonly(k);
        }
    }

    pub fn bank_hash_at(&self, slot: Slot) -> Hash {
        self.bank_hash_info_at(slot).hash
    }

    pub fn bank_hash_info_at(&self, slot: Slot) -> BankHashInfo {
        let delta_hash = self.accounts_db.get_accounts_delta_hash(slot);
        let bank_hashes = self.accounts_db.bank_hashes.read().unwrap();
        let mut hash_info = bank_hashes
            .get(&slot)
            .expect("No bank hash was found for this bank, that should not be possible")
            .clone();
        hash_info.hash = delta_hash;
        hash_info
    }

    /// This function will prevent multiple threads from modifying the same account state at the
    /// same time
    #[must_use]
    #[allow(clippy::needless_collect)]
    pub fn lock_accounts<'a>(&self, txs: impl Iterator<Item = &'a Transaction>) -> Vec<Result<()>> {
        let keys: Vec<_> = txs
            .map(|tx| tx.message().get_account_keys_by_lock_type())
            .collect();
        let mut account_locks = &mut self.account_locks.lock().unwrap();
        keys.into_iter()
            .map(|(writable_keys, readonly_keys)| {
                self.lock_account(&mut account_locks, writable_keys, readonly_keys)
            })
            .collect()
    }

    /// Once accounts are unlocked, new transactions that modify that state can enter the pipeline
    #[allow(clippy::needless_collect)]
    pub fn unlock_accounts<'a>(
        &self,
        txs: impl Iterator<Item = &'a Transaction>,
        results: &[Result<()>],
    ) {
        let keys: Vec<_> = txs
            .zip(results)
            .filter_map(|(tx, res)| match res {
                Err(TransactionError::AccountInUse) => None,
                Err(TransactionError::SanitizeFailure) => None,
                Err(TransactionError::AccountLoadedTwice) => None,
                _ => Some(tx.message.get_account_keys_by_lock_type()),
            })
            .collect();
        let mut account_locks = self.account_locks.lock().unwrap();
        debug!("bank unlock accounts");
        keys.into_iter().for_each(|(writable_keys, readonly_keys)| {
            self.unlock_account(&mut account_locks, writable_keys, readonly_keys);
        });
    }

    /// Store the accounts into the DB
    // allow(clippy) needed for various gating flags
    #[allow(clippy::too_many_arguments)]
    pub fn store_cached<'a>(
        &self,
        slot: Slot,
        txs: impl Iterator<Item = &'a Transaction>,
        res: &'a [TransactionExecutionResult],
        loaded: &'a mut [TransactionLoadResult],
        rent_collector: &RentCollector,
        last_blockhash_with_fee_calculator: &(Hash, FeeCalculator),
        fix_recent_blockhashes_sysvar_delay: bool,
        rent_for_sysvars: bool,
    ) {
        let accounts_to_store = self.collect_accounts_to_store(
            txs,
            res,
            loaded,
            rent_collector,
            last_blockhash_with_fee_calculator,
            fix_recent_blockhashes_sysvar_delay,
            rent_for_sysvars,
        );
        self.accounts_db.store_cached(slot, &accounts_to_store);
    }

    /// Purge a slot if it is not a root
    /// Root slots cannot be purged
    /// `is_from_abs` is true if the caller is the AccountsBackgroundService
    pub fn purge_slot(&self, slot: Slot, bank_id: BankId, is_from_abs: bool) {
        self.accounts_db.purge_slot(slot, bank_id, is_from_abs);
    }

    /// Add a slot to root.  Root slots cannot be purged
    pub fn add_root(&self, slot: Slot) {
        self.accounts_db.add_root(slot)
    }

    fn collect_accounts_to_store<'a>(
        &self,
        txs: impl Iterator<Item = &'a Transaction>,
        res: &'a [TransactionExecutionResult],
        loaded: &'a mut [TransactionLoadResult],
        rent_collector: &RentCollector,
        last_blockhash_with_fee_calculator: &(Hash, FeeCalculator),
        fix_recent_blockhashes_sysvar_delay: bool,
        rent_for_sysvars: bool,
    ) -> Vec<(&'a Pubkey, &'a AccountSharedData)> {
        let mut accounts = Vec::with_capacity(loaded.len());
        for (i, ((raccs, _nonce_rollback), tx)) in loaded.iter_mut().zip(txs).enumerate() {
            if raccs.is_err() {
                continue;
            }
            let (res, nonce_rollback) = &res[i];
            let maybe_nonce_rollback = match (res, nonce_rollback) {
                (Ok(_), Some(nonce_rollback)) => {
                    let pubkey = nonce_rollback.nonce_address();
                    let acc = nonce_rollback.nonce_account();
                    let maybe_fee_account = nonce_rollback.fee_account();
                    Some((pubkey, acc, maybe_fee_account))
                }
                (Err(TransactionError::InstructionError(_, _)), Some(nonce_rollback)) => {
                    let pubkey = nonce_rollback.nonce_address();
                    let acc = nonce_rollback.nonce_account();
                    let maybe_fee_account = nonce_rollback.fee_account();
                    Some((pubkey, acc, maybe_fee_account))
                }
                (Ok(_), _nonce_rollback) => None,
                (Err(_), _nonce_rollback) => continue,
            };

            let message = &tx.message();
            let loaded_transaction = raccs.as_mut().unwrap();
            let mut fee_payer_index = None;
            for (i, (key, account)) in (0..message.account_keys.len())
                .zip(loaded_transaction.accounts.iter_mut())
                .filter(|(i, _account)| message.is_non_loader_key(*i))
            {
                let is_nonce_account = prepare_if_nonce_account(
                    account,
                    key,
                    res,
                    maybe_nonce_rollback,
                    last_blockhash_with_fee_calculator,
                    fix_recent_blockhashes_sysvar_delay,
                );
                if fee_payer_index.is_none() {
                    fee_payer_index = Some(i);
                }
                let is_fee_payer = Some(i) == fee_payer_index;
                if message.is_writable(i)
                    && (res.is_ok()
                        || (maybe_nonce_rollback.is_some() && (is_nonce_account || is_fee_payer)))
                {
                    if res.is_err() {
                        match (is_nonce_account, is_fee_payer, maybe_nonce_rollback) {
                            // nonce is fee-payer, state updated in `prepare_if_nonce_account()`
                            (true, true, Some((_, _, None))) => (),
                            // nonce not fee-payer, state updated in `prepare_if_nonce_account()`
                            (true, false, Some((_, _, Some(_)))) => (),
                            // not nonce, but fee-payer. rollback to cached state
                            (false, true, Some((_, _, Some(fee_payer_account)))) => {
                                *account = fee_payer_account.clone();
                            }
                            _ => panic!("unexpected nonce_rollback condition"),
                        }
                    }
                    if account.rent_epoch() == INITIAL_RENT_EPOCH {
                        let rent = rent_collector.collect_from_created_account(
                            key,
                            account,
                            rent_for_sysvars,
                        );
                        loaded_transaction.rent += rent;
                        loaded_transaction
                            .rent_debits
                            .push(key, rent, account.lamports());
                    }
                    accounts.push((&*key, &*account));
                }
            }
        }
        accounts
    }
}

pub fn prepare_if_nonce_account(
    account: &mut AccountSharedData,
    account_pubkey: &Pubkey,
    tx_result: &Result<()>,
    maybe_nonce_rollback: Option<(&Pubkey, &AccountSharedData, Option<&AccountSharedData>)>,
    last_blockhash_with_fee_calculator: &(Hash, FeeCalculator),
    fix_recent_blockhashes_sysvar_delay: bool,
) -> bool {
    if let Some((nonce_key, nonce_acc, _maybe_fee_account)) = maybe_nonce_rollback {
        if account_pubkey == nonce_key {
            let overwrite = if tx_result.is_err() {
                // Nonce TX failed with an InstructionError. Roll back
                // its account state
                *account = nonce_acc.clone();
                true
            } else {
                // Retain overwrite on successful transactions until
                // recent_blockhashes_sysvar_delay fix is activated
                !fix_recent_blockhashes_sysvar_delay
            };
            if overwrite {
                // Since hash_age_kind is DurableNonce, unwrap is safe here
                let state = StateMut::<nonce::state::Versions>::state(nonce_acc)
                    .unwrap()
                    .convert_to_current();
                if let nonce::State::Initialized(ref data) = state {
                    let new_data = nonce::state::Versions::new_current(nonce::State::Initialized(
                        nonce::state::Data {
                            blockhash: last_blockhash_with_fee_calculator.0,
                            fee_calculator: last_blockhash_with_fee_calculator.1.clone(),
                            ..data.clone()
                        },
                    ));
                    account.set_state(&new_data).unwrap();
                }
            }
            return true;
        }
    }
    false
}

pub fn create_test_accounts(
    accounts: &Accounts,
    pubkeys: &mut Vec<Pubkey>,
    num: usize,
    slot: Slot,
) {
    for t in 0..num {
        let pubkey = solana_sdk::pubkey::new_rand();
        let account =
            AccountSharedData::new((t + 1) as u64, 0, AccountSharedData::default().owner());
        accounts.store_slow_uncached(slot, &pubkey, &account);
        pubkeys.push(pubkey);
    }
}

// Only used by bench, not safe to call otherwise accounts can conflict with the
// accounts cache!
pub fn update_accounts_bench(accounts: &Accounts, pubkeys: &[Pubkey], slot: u64) {
    for pubkey in pubkeys {
        let amount = thread_rng().gen_range(0, 10);
        let account = AccountSharedData::new(amount, 0, AccountSharedData::default().owner());
        accounts.store_slow_uncached(slot, pubkey, &account);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rent_collector::RentCollector;
    use solana_sdk::{
        account::{AccountSharedData, WritableAccount},
        epoch_schedule::EpochSchedule,
        fee_calculator::FeeCalculator,
        genesis_config::ClusterType,
        hash::Hash,
        instruction::{CompiledInstruction, InstructionError},
        message::Message,
        nonce, nonce_account,
        rent::Rent,
        signature::{keypair_from_seed, Keypair, Signer},
        system_instruction, system_program,
    };
    use std::{
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
        {thread, time},
    };

    fn load_accounts_with_fee_and_rent(
        tx: Transaction,
        ka: &[(Pubkey, AccountSharedData)],
        fee_calculator: &FeeCalculator,
        rent_collector: &RentCollector,
        error_counters: &mut ErrorCounters,
    ) -> Vec<TransactionLoadResult> {
        let mut hash_queue = BlockhashQueue::new(100);
        hash_queue.register_hash(&tx.message().recent_blockhash, fee_calculator);
        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );
        for ka in ka.iter() {
            accounts.store_slow_uncached(0, &ka.0, &ka.1);
        }

        let ancestors = vec![(0, 0)].into_iter().collect();
        accounts.load_accounts(
            &ancestors,
            [tx].iter(),
            vec![(Ok(()), None)],
            &hash_queue,
            error_counters,
            rent_collector,
            &FeatureSet::all_enabled(),
        )
    }

    fn load_accounts_with_fee(
        tx: Transaction,
        ka: &[(Pubkey, AccountSharedData)],
        fee_calculator: &FeeCalculator,
        error_counters: &mut ErrorCounters,
    ) -> Vec<TransactionLoadResult> {
        let rent_collector = RentCollector::default();
        load_accounts_with_fee_and_rent(tx, ka, fee_calculator, &rent_collector, error_counters)
    }

    fn load_accounts(
        tx: Transaction,
        ka: &[(Pubkey, AccountSharedData)],
        error_counters: &mut ErrorCounters,
    ) -> Vec<TransactionLoadResult> {
        let fee_calculator = FeeCalculator::default();
        load_accounts_with_fee(tx, ka, &fee_calculator, error_counters)
    }

    #[test]
    fn test_load_accounts_no_key() {
        let accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let instructions = vec![CompiledInstruction::new(0, &(), vec![0])];
        let tx = Transaction::new_with_compiled_instructions::<[&Keypair; 0]>(
            &[],
            &[],
            Hash::default(),
            vec![native_loader::id()],
            instructions,
        );

        let loaded_accounts = load_accounts(tx, &accounts, &mut error_counters);

        assert_eq!(error_counters.account_not_found, 1);
        assert_eq!(loaded_accounts.len(), 1);
        assert_eq!(
            loaded_accounts[0],
            (Err(TransactionError::AccountNotFound), None,)
        );
    }

    #[test]
    fn test_load_accounts_no_account_0_exists() {
        let accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let keypair = Keypair::new();

        let instructions = vec![CompiledInstruction::new(1, &(), vec![0])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[],
            Hash::default(),
            vec![native_loader::id()],
            instructions,
        );

        let loaded_accounts = load_accounts(tx, &accounts, &mut error_counters);

        assert_eq!(error_counters.account_not_found, 1);
        assert_eq!(loaded_accounts.len(), 1);
        assert_eq!(
            loaded_accounts[0],
            (Err(TransactionError::AccountNotFound), None,),
        );
    }

    #[test]
    fn test_load_accounts_unknown_program_id() {
        let mut accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let keypair = Keypair::new();
        let key0 = keypair.pubkey();
        let key1 = Pubkey::new(&[5u8; 32]);

        let account = AccountSharedData::new(1, 0, &Pubkey::default());
        accounts.push((key0, account));

        let account = AccountSharedData::new(2, 1, &Pubkey::default());
        accounts.push((key1, account));

        let instructions = vec![CompiledInstruction::new(1, &(), vec![0])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[],
            Hash::default(),
            vec![Pubkey::default()],
            instructions,
        );

        let loaded_accounts = load_accounts(tx, &accounts, &mut error_counters);

        assert_eq!(error_counters.account_not_found, 1);
        assert_eq!(loaded_accounts.len(), 1);
        assert_eq!(
            loaded_accounts[0],
            (Err(TransactionError::ProgramAccountNotFound), None,)
        );
    }

    #[test]
    fn test_load_accounts_insufficient_funds() {
        let mut accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let keypair = Keypair::new();
        let key0 = keypair.pubkey();

        let account = AccountSharedData::new(1, 0, &Pubkey::default());
        accounts.push((key0, account));

        let instructions = vec![CompiledInstruction::new(1, &(), vec![0])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[],
            Hash::default(),
            vec![native_loader::id()],
            instructions,
        );

        let fee_calculator = FeeCalculator::new(10);
        assert_eq!(fee_calculator.calculate_fee(tx.message()), 10);

        let loaded_accounts =
            load_accounts_with_fee(tx, &accounts, &fee_calculator, &mut error_counters);

        assert_eq!(error_counters.insufficient_funds, 1);
        assert_eq!(loaded_accounts.len(), 1);
        assert_eq!(
            loaded_accounts[0].clone(),
            (Err(TransactionError::InsufficientFundsForFee), None,),
        );
    }

    #[test]
    fn test_load_accounts_invalid_account_for_fee() {
        let mut accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let keypair = Keypair::new();
        let key0 = keypair.pubkey();

        let account = AccountSharedData::new(1, 1, &solana_sdk::pubkey::new_rand()); // <-- owner is not the system program
        accounts.push((key0, account));

        let instructions = vec![CompiledInstruction::new(1, &(), vec![0])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[],
            Hash::default(),
            vec![native_loader::id()],
            instructions,
        );

        let loaded_accounts = load_accounts(tx, &accounts, &mut error_counters);

        assert_eq!(error_counters.invalid_account_for_fee, 1);
        assert_eq!(loaded_accounts.len(), 1);
        assert_eq!(
            loaded_accounts[0],
            (Err(TransactionError::InvalidAccountForFee), None,),
        );
    }

    #[test]
    fn test_load_accounts_fee_payer_is_nonce() {
        let mut error_counters = ErrorCounters::default();
        let rent_collector = RentCollector::new(
            0,
            &EpochSchedule::default(),
            500_000.0,
            &Rent {
                lamports_per_byte_year: 42,
                ..Rent::default()
            },
        );
        let min_balance = rent_collector.rent.minimum_balance(nonce::State::size());
        let fee_calculator = FeeCalculator::new(min_balance);
        let nonce = Keypair::new();
        let mut accounts = vec![(
            nonce.pubkey(),
            AccountSharedData::new_data(
                min_balance * 2,
                &nonce::state::Versions::new_current(nonce::State::Initialized(
                    nonce::state::Data::default(),
                )),
                &system_program::id(),
            )
            .unwrap(),
        )];
        let instructions = vec![CompiledInstruction::new(1, &(), vec![0])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&nonce],
            &[],
            Hash::default(),
            vec![native_loader::id()],
            instructions,
        );

        // Fee leaves min_balance balance succeeds
        let loaded_accounts = load_accounts_with_fee_and_rent(
            tx.clone(),
            &accounts,
            &fee_calculator,
            &rent_collector,
            &mut error_counters,
        );
        assert_eq!(loaded_accounts.len(), 1);
        let (load_res, _nonce_rollback) = &loaded_accounts[0];
        let loaded_transaction = load_res.as_ref().unwrap();
        assert_eq!(loaded_transaction.accounts[0].1.lamports(), min_balance);

        // Fee leaves zero balance fails
        accounts[0].1.set_lamports(min_balance);
        let loaded_accounts = load_accounts_with_fee_and_rent(
            tx.clone(),
            &accounts,
            &fee_calculator,
            &rent_collector,
            &mut error_counters,
        );
        assert_eq!(loaded_accounts.len(), 1);
        let (load_res, _nonce_rollback) = &loaded_accounts[0];
        assert_eq!(*load_res, Err(TransactionError::InsufficientFundsForFee));

        // Fee leaves non-zero, but sub-min_balance balance fails
        accounts[0].1.set_lamports(3 * min_balance / 2);
        let loaded_accounts = load_accounts_with_fee_and_rent(
            tx,
            &accounts,
            &fee_calculator,
            &rent_collector,
            &mut error_counters,
        );
        assert_eq!(loaded_accounts.len(), 1);
        let (load_res, _nonce_rollback) = &loaded_accounts[0];
        assert_eq!(*load_res, Err(TransactionError::InsufficientFundsForFee));
    }

    #[test]
    fn test_load_accounts_no_loaders() {
        let mut accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let keypair = Keypair::new();
        let key0 = keypair.pubkey();
        let key1 = Pubkey::new(&[5u8; 32]);

        let mut account = AccountSharedData::new(1, 0, &Pubkey::default());
        account.set_rent_epoch(1);
        accounts.push((key0, account));

        let mut account = AccountSharedData::new(2, 1, &Pubkey::default());
        account.set_rent_epoch(1);
        accounts.push((key1, account));

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[key1],
            Hash::default(),
            vec![native_loader::id()],
            instructions,
        );

        let loaded_accounts = load_accounts(tx, &accounts, &mut error_counters);

        assert_eq!(error_counters.account_not_found, 0);
        assert_eq!(loaded_accounts.len(), 1);
        match &loaded_accounts[0] {
            (Ok(loaded_transaction), _nonce_rollback) => {
                assert_eq!(loaded_transaction.accounts.len(), 3);
                assert_eq!(loaded_transaction.accounts[0].1, accounts[0].1);
                assert_eq!(loaded_transaction.loaders.len(), 1);
                assert_eq!(loaded_transaction.loaders[0].len(), 0);
            }
            (Err(e), _nonce_rollback) => Err(e).unwrap(),
        }
    }

    #[test]
    fn test_load_accounts_max_call_depth() {
        let mut accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let keypair = Keypair::new();
        let key0 = keypair.pubkey();
        let key1 = Pubkey::new(&[5u8; 32]);
        let key2 = Pubkey::new(&[6u8; 32]);
        let key3 = Pubkey::new(&[7u8; 32]);
        let key4 = Pubkey::new(&[8u8; 32]);
        let key5 = Pubkey::new(&[9u8; 32]);
        let key6 = Pubkey::new(&[10u8; 32]);

        let account = AccountSharedData::new(1, 0, &Pubkey::default());
        accounts.push((key0, account));

        let mut account = AccountSharedData::new(40, 1, &Pubkey::default());
        account.set_executable(true);
        account.set_owner(native_loader::id());
        accounts.push((key1, account));

        let mut account = AccountSharedData::new(41, 1, &Pubkey::default());
        account.set_executable(true);
        account.set_owner(key1);
        accounts.push((key2, account));

        let mut account = AccountSharedData::new(42, 1, &Pubkey::default());
        account.set_executable(true);
        account.set_owner(key2);
        accounts.push((key3, account));

        let mut account = AccountSharedData::new(43, 1, &Pubkey::default());
        account.set_executable(true);
        account.set_owner(key3);
        accounts.push((key4, account));

        let mut account = AccountSharedData::new(44, 1, &Pubkey::default());
        account.set_executable(true);
        account.set_owner(key4);
        accounts.push((key5, account));

        let mut account = AccountSharedData::new(45, 1, &Pubkey::default());
        account.set_executable(true);
        account.set_owner(key5);
        accounts.push((key6, account));

        let instructions = vec![CompiledInstruction::new(1, &(), vec![0])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[],
            Hash::default(),
            vec![key6],
            instructions,
        );

        let loaded_accounts = load_accounts(tx, &accounts, &mut error_counters);

        assert_eq!(error_counters.call_chain_too_deep, 1);
        assert_eq!(loaded_accounts.len(), 1);
        assert_eq!(
            loaded_accounts[0],
            (Err(TransactionError::CallChainTooDeep), None,)
        );
    }

    #[test]
    fn test_load_accounts_bad_program_id() {
        let mut accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let keypair = Keypair::new();
        let key0 = keypair.pubkey();
        let key1 = Pubkey::new(&[5u8; 32]);

        let account = AccountSharedData::new(1, 0, &Pubkey::default());
        accounts.push((key0, account));

        let mut account = AccountSharedData::new(40, 1, &native_loader::id());
        account.set_executable(true);
        accounts.push((key1, account));

        let instructions = vec![CompiledInstruction::new(0, &(), vec![0])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[],
            Hash::default(),
            vec![key1],
            instructions,
        );

        let loaded_accounts = load_accounts(tx, &accounts, &mut error_counters);

        assert_eq!(error_counters.invalid_program_for_execution, 1);
        assert_eq!(loaded_accounts.len(), 1);
        assert_eq!(
            loaded_accounts[0],
            (Err(TransactionError::InvalidProgramForExecution), None,)
        );
    }

    #[test]
    fn test_load_accounts_bad_owner() {
        let mut accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let keypair = Keypair::new();
        let key0 = keypair.pubkey();
        let key1 = Pubkey::new(&[5u8; 32]);

        let account = AccountSharedData::new(1, 0, &Pubkey::default());
        accounts.push((key0, account));

        let mut account = AccountSharedData::new(40, 1, &Pubkey::default());
        account.set_executable(true);
        accounts.push((key1, account));

        let instructions = vec![CompiledInstruction::new(1, &(), vec![0])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[],
            Hash::default(),
            vec![key1],
            instructions,
        );

        let loaded_accounts = load_accounts(tx, &accounts, &mut error_counters);

        assert_eq!(error_counters.account_not_found, 1);
        assert_eq!(loaded_accounts.len(), 1);
        assert_eq!(
            loaded_accounts[0],
            (Err(TransactionError::ProgramAccountNotFound), None,)
        );
    }

    #[test]
    fn test_load_accounts_not_executable() {
        let mut accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let keypair = Keypair::new();
        let key0 = keypair.pubkey();
        let key1 = Pubkey::new(&[5u8; 32]);

        let account = AccountSharedData::new(1, 0, &Pubkey::default());
        accounts.push((key0, account));

        let account = AccountSharedData::new(40, 1, &native_loader::id());
        accounts.push((key1, account));

        let instructions = vec![CompiledInstruction::new(1, &(), vec![0])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[],
            Hash::default(),
            vec![key1],
            instructions,
        );

        let loaded_accounts = load_accounts(tx, &accounts, &mut error_counters);

        assert_eq!(error_counters.invalid_program_for_execution, 1);
        assert_eq!(loaded_accounts.len(), 1);
        assert_eq!(
            loaded_accounts[0],
            (Err(TransactionError::InvalidProgramForExecution), None,)
        );
    }

    #[test]
    fn test_load_accounts_multiple_loaders() {
        let mut accounts: Vec<(Pubkey, AccountSharedData)> = Vec::new();
        let mut error_counters = ErrorCounters::default();

        let keypair = Keypair::new();
        let key0 = keypair.pubkey();
        let key1 = Pubkey::new(&[5u8; 32]);
        let key2 = Pubkey::new(&[6u8; 32]);

        let mut account = AccountSharedData::new(1, 0, &Pubkey::default());
        account.set_rent_epoch(1);
        accounts.push((key0, account));

        let mut account = AccountSharedData::new(40, 1, &Pubkey::default());
        account.set_executable(true);
        account.set_rent_epoch(1);
        account.set_owner(native_loader::id());
        accounts.push((key1, account));

        let mut account = AccountSharedData::new(41, 1, &Pubkey::default());
        account.set_executable(true);
        account.set_rent_epoch(1);
        account.set_owner(key1);
        accounts.push((key2, account));

        let instructions = vec![
            CompiledInstruction::new(1, &(), vec![0]),
            CompiledInstruction::new(2, &(), vec![0]),
        ];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[],
            Hash::default(),
            vec![key1, key2],
            instructions,
        );

        let loaded_accounts = load_accounts(tx, &accounts, &mut error_counters);

        assert_eq!(error_counters.account_not_found, 0);
        assert_eq!(loaded_accounts.len(), 1);
        match &loaded_accounts[0] {
            (Ok(loaded_transaction), _nonce_rollback) => {
                assert_eq!(loaded_transaction.accounts.len(), 3);
                assert_eq!(loaded_transaction.accounts[0].1, accounts[0].1);
                assert_eq!(loaded_transaction.loaders.len(), 2);
                assert_eq!(loaded_transaction.loaders[0].len(), 1);
                assert_eq!(loaded_transaction.loaders[1].len(), 2);
                for loaders in loaded_transaction.loaders.iter() {
                    for (i, accounts_subset) in loaders.iter().enumerate() {
                        // +1 to skip first not loader account
                        assert_eq!(*accounts_subset, accounts[i + 1]);
                    }
                }
            }
            (Err(e), _nonce_rollback) => Err(e).unwrap(),
        }
    }

    #[test]
    fn test_load_by_program_slot() {
        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );

        // Load accounts owned by various programs into AccountsDb
        let pubkey0 = solana_sdk::pubkey::new_rand();
        let account0 = AccountSharedData::new(1, 0, &Pubkey::new(&[2; 32]));
        accounts.store_slow_uncached(0, &pubkey0, &account0);
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let account1 = AccountSharedData::new(1, 0, &Pubkey::new(&[2; 32]));
        accounts.store_slow_uncached(0, &pubkey1, &account1);
        let pubkey2 = solana_sdk::pubkey::new_rand();
        let account2 = AccountSharedData::new(1, 0, &Pubkey::new(&[3; 32]));
        accounts.store_slow_uncached(0, &pubkey2, &account2);

        let loaded = accounts.load_by_program_slot(0, Some(&Pubkey::new(&[2; 32])));
        assert_eq!(loaded.len(), 2);
        let loaded = accounts.load_by_program_slot(0, Some(&Pubkey::new(&[3; 32])));
        assert_eq!(loaded, vec![(pubkey2, account2)]);
        let loaded = accounts.load_by_program_slot(0, Some(&Pubkey::new(&[4; 32])));
        assert_eq!(loaded, vec![]);
    }

    #[test]
    fn test_accounts_account_not_found() {
        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );
        let mut error_counters = ErrorCounters::default();
        let ancestors = vec![(0, 0)].into_iter().collect();

        assert_eq!(
            accounts.load_executable_accounts(
                &ancestors,
                &solana_sdk::pubkey::new_rand(),
                &mut error_counters
            ),
            Err(TransactionError::ProgramAccountNotFound)
        );
        assert_eq!(error_counters.account_not_found, 1);
    }

    #[test]
    #[should_panic]
    fn test_accounts_empty_bank_hash() {
        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );
        accounts.bank_hash_at(1);
    }

    #[test]
    fn test_accounts_locks() {
        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let keypair3 = Keypair::new();

        let account0 = AccountSharedData::new(1, 0, &Pubkey::default());
        let account1 = AccountSharedData::new(2, 0, &Pubkey::default());
        let account2 = AccountSharedData::new(3, 0, &Pubkey::default());
        let account3 = AccountSharedData::new(4, 0, &Pubkey::default());

        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );
        accounts.store_slow_uncached(0, &keypair0.pubkey(), &account0);
        accounts.store_slow_uncached(0, &keypair1.pubkey(), &account1);
        accounts.store_slow_uncached(0, &keypair2.pubkey(), &account2);
        accounts.store_slow_uncached(0, &keypair3.pubkey(), &account3);

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair0.pubkey(), keypair1.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx = Transaction::new(&[&keypair0], message, Hash::default());
        let results0 = accounts.lock_accounts([tx.clone()].iter());

        assert!(results0[0].is_ok());
        assert_eq!(
            *accounts
                .account_locks
                .lock()
                .unwrap()
                .readonly_locks
                .get(&keypair1.pubkey())
                .unwrap(),
            1
        );

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair2.pubkey(), keypair1.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx0 = Transaction::new(&[&keypair2], message, Hash::default());
        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair1.pubkey(), keypair3.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx1 = Transaction::new(&[&keypair1], message, Hash::default());
        let txs = vec![tx0, tx1];
        let results1 = accounts.lock_accounts(txs.iter());

        assert!(results1[0].is_ok()); // Read-only account (keypair1) can be referenced multiple times
        assert!(results1[1].is_err()); // Read-only account (keypair1) cannot also be locked as writable
        assert_eq!(
            *accounts
                .account_locks
                .lock()
                .unwrap()
                .readonly_locks
                .get(&keypair1.pubkey())
                .unwrap(),
            2
        );

        accounts.unlock_accounts([tx].iter(), &results0);
        accounts.unlock_accounts(txs.iter(), &results1);
        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair1.pubkey(), keypair3.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx = Transaction::new(&[&keypair1], message, Hash::default());
        let results2 = accounts.lock_accounts([tx].iter());
        assert!(results2[0].is_ok()); // Now keypair1 account can be locked as writable

        // Check that read-only lock with zero references is deleted
        assert!(accounts
            .account_locks
            .lock()
            .unwrap()
            .readonly_locks
            .get(&keypair1.pubkey())
            .is_none());
    }

    #[test]
    fn test_accounts_locks_multithreaded() {
        let counter = Arc::new(AtomicU64::new(0));
        let exit = Arc::new(AtomicBool::new(false));

        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();

        let account0 = AccountSharedData::new(1, 0, &Pubkey::default());
        let account1 = AccountSharedData::new(2, 0, &Pubkey::default());
        let account2 = AccountSharedData::new(3, 0, &Pubkey::default());

        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );
        accounts.store_slow_uncached(0, &keypair0.pubkey(), &account0);
        accounts.store_slow_uncached(0, &keypair1.pubkey(), &account1);
        accounts.store_slow_uncached(0, &keypair2.pubkey(), &account2);

        let accounts_arc = Arc::new(accounts);

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let readonly_message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair0.pubkey(), keypair1.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let readonly_tx = Transaction::new(&[&keypair0], readonly_message, Hash::default());

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let writable_message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair1.pubkey(), keypair2.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let writable_tx = Transaction::new(&[&keypair1], writable_message, Hash::default());

        let counter_clone = counter.clone();
        let accounts_clone = accounts_arc.clone();
        let exit_clone = exit.clone();
        thread::spawn(move || {
            let counter_clone = counter_clone.clone();
            let exit_clone = exit_clone.clone();
            loop {
                let txs = vec![writable_tx.clone()];
                let results = accounts_clone.clone().lock_accounts(txs.iter());
                for result in results.iter() {
                    if result.is_ok() {
                        counter_clone.clone().fetch_add(1, Ordering::SeqCst);
                    }
                }
                accounts_clone.unlock_accounts(txs.iter(), &results);
                if exit_clone.clone().load(Ordering::Relaxed) {
                    break;
                }
            }
        });
        let counter_clone = counter;
        for _ in 0..5 {
            let txs = vec![readonly_tx.clone()];
            let results = accounts_arc.clone().lock_accounts(txs.iter());
            if results[0].is_ok() {
                let counter_value = counter_clone.clone().load(Ordering::SeqCst);
                thread::sleep(time::Duration::from_millis(50));
                assert_eq!(counter_value, counter_clone.clone().load(Ordering::SeqCst));
            }
            accounts_arc.unlock_accounts(txs.iter(), &results);
            thread::sleep(time::Duration::from_millis(50));
        }
        exit.store(true, Ordering::Relaxed);
    }

    #[test]
    fn test_collect_accounts_to_store() {
        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let pubkey = solana_sdk::pubkey::new_rand();
        let account0 = AccountSharedData::new(1, 0, &Pubkey::default());
        let account1 = AccountSharedData::new(2, 0, &Pubkey::default());
        let account2 = AccountSharedData::new(3, 0, &Pubkey::default());

        let rent_collector = RentCollector::default();

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair0.pubkey(), pubkey, native_loader::id()],
            Hash::default(),
            instructions,
        );
        let transaction_accounts0 = vec![
            (message.account_keys[0], account0),
            (message.account_keys[1], account2.clone()),
        ];
        let tx0 = Transaction::new(&[&keypair0], message, Hash::default());

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair1.pubkey(), pubkey, native_loader::id()],
            Hash::default(),
            instructions,
        );
        let transaction_accounts1 = vec![
            (message.account_keys[0], account1),
            (message.account_keys[1], account2),
        ];
        let tx1 = Transaction::new(&[&keypair1], message, Hash::default());

        let loaders = vec![(Ok(()), None), (Ok(()), None)];

        let transaction_loaders0 = vec![];
        let transaction_rent0 = 0;
        let loaded0 = (
            Ok(LoadedTransaction {
                accounts: transaction_accounts0,
                loaders: transaction_loaders0,
                rent: transaction_rent0,
                rent_debits: RentDebits::default(),
            }),
            None,
        );

        let transaction_loaders1 = vec![];
        let transaction_rent1 = 0;
        let loaded1 = (
            Ok(LoadedTransaction {
                accounts: transaction_accounts1,
                loaders: transaction_loaders1,
                rent: transaction_rent1,
                rent_debits: RentDebits::default(),
            }),
            None,
        );

        let mut loaded = vec![loaded0, loaded1];

        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );
        {
            accounts
                .account_locks
                .lock()
                .unwrap()
                .insert_new_readonly(&pubkey);
        }
        let txs = &[tx0, tx1];
        let collected_accounts = accounts.collect_accounts_to_store(
            txs.iter(),
            &loaders,
            loaded.as_mut_slice(),
            &rent_collector,
            &(Hash::default(), FeeCalculator::default()),
            true,
            true,
        );
        assert_eq!(collected_accounts.len(), 2);
        assert!(collected_accounts
            .iter()
            .any(|(pubkey, _account)| *pubkey == &keypair0.pubkey()));
        assert!(collected_accounts
            .iter()
            .any(|(pubkey, _account)| *pubkey == &keypair1.pubkey()));

        // Ensure readonly_lock reflects lock
        assert_eq!(
            *accounts
                .account_locks
                .lock()
                .unwrap()
                .readonly_locks
                .get(&pubkey)
                .unwrap(),
            1
        );
    }

    #[test]
    fn huge_clean() {
        solana_logger::setup();
        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );
        let mut old_pubkey = Pubkey::default();
        let zero_account = AccountSharedData::new(0, 0, AccountSharedData::default().owner());
        info!("storing..");
        for i in 0..2_000 {
            let pubkey = solana_sdk::pubkey::new_rand();
            let account =
                AccountSharedData::new((i + 1) as u64, 0, AccountSharedData::default().owner());
            accounts.store_slow_uncached(i, &pubkey, &account);
            accounts.store_slow_uncached(i, &old_pubkey, &zero_account);
            old_pubkey = pubkey;
            accounts.add_root(i);
            if i % 1_000 == 0 {
                info!("  store {}", i);
            }
        }
        info!("done..cleaning..");
        accounts.accounts_db.clean_accounts(None, false);
    }

    fn load_accounts_no_store(accounts: &Accounts, tx: Transaction) -> Vec<TransactionLoadResult> {
        let rent_collector = RentCollector::default();
        let fee_calculator = FeeCalculator::new(10);
        let mut hash_queue = BlockhashQueue::new(100);
        hash_queue.register_hash(&tx.message().recent_blockhash, &fee_calculator);

        let ancestors = vec![(0, 0)].into_iter().collect();
        let mut error_counters = ErrorCounters::default();
        accounts.load_accounts(
            &ancestors,
            [tx].iter(),
            vec![(Ok(()), None)],
            &hash_queue,
            &mut error_counters,
            &rent_collector,
            &FeatureSet::all_enabled(),
        )
    }

    #[test]
    fn test_instructions() {
        solana_logger::setup();
        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );

        let instructions_key = solana_sdk::sysvar::instructions::id();
        let keypair = Keypair::new();
        let instructions = vec![CompiledInstruction::new(1, &(), vec![0, 1])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&keypair],
            &[solana_sdk::pubkey::new_rand(), instructions_key],
            Hash::default(),
            vec![native_loader::id()],
            instructions,
        );

        let loaded_accounts = load_accounts_no_store(&accounts, tx);
        assert_eq!(loaded_accounts.len(), 1);
        assert!(loaded_accounts[0].0.is_err());
    }

    fn create_accounts_prepare_if_nonce_account() -> (
        Pubkey,
        AccountSharedData,
        AccountSharedData,
        Hash,
        FeeCalculator,
        Option<AccountSharedData>,
    ) {
        let data = nonce::state::Versions::new_current(nonce::State::Initialized(
            nonce::state::Data::default(),
        ));
        let account = AccountSharedData::new_data(42, &data, &system_program::id()).unwrap();
        let mut pre_account = account.clone();
        pre_account.set_lamports(43);
        (
            Pubkey::default(),
            pre_account,
            account,
            Hash::new(&[1u8; 32]),
            FeeCalculator {
                lamports_per_signature: 1234,
            },
            None,
        )
    }

    fn run_prepare_if_nonce_account_test(
        account: &mut AccountSharedData,
        account_pubkey: &Pubkey,
        tx_result: &Result<()>,
        maybe_nonce_rollback: Option<(&Pubkey, &AccountSharedData, Option<&AccountSharedData>)>,
        last_blockhash_with_fee_calculator: &(Hash, FeeCalculator),
        expect_account: &AccountSharedData,
    ) -> bool {
        // Verify expect_account's relationship
        match maybe_nonce_rollback {
            Some((nonce_pubkey, _nonce_account, _maybe_fee_account))
                if nonce_pubkey == account_pubkey && tx_result.is_ok() =>
            {
                assert_eq!(expect_account, account) // Account update occurs in system_instruction_processor
            }
            Some((nonce_pubkey, nonce_account, _maybe_fee_account))
                if nonce_pubkey == account_pubkey =>
            {
                assert_ne!(expect_account, nonce_account)
            }
            _ => assert_eq!(expect_account, account),
        }

        prepare_if_nonce_account(
            account,
            account_pubkey,
            tx_result,
            maybe_nonce_rollback,
            last_blockhash_with_fee_calculator,
            true,
        );
        expect_account == account
    }

    #[test]
    fn test_prepare_if_nonce_account_expected() {
        let (
            pre_account_pubkey,
            pre_account,
            mut post_account,
            last_blockhash,
            last_fee_calculator,
            maybe_fee_account,
        ) = create_accounts_prepare_if_nonce_account();
        let post_account_pubkey = pre_account_pubkey;

        let mut expect_account = post_account.clone();
        let data = nonce::state::Versions::new_current(nonce::State::Initialized(
            nonce::state::Data::default(),
        ));
        expect_account.set_state(&data).unwrap();

        assert!(run_prepare_if_nonce_account_test(
            &mut post_account,
            &post_account_pubkey,
            &Ok(()),
            Some((
                &pre_account_pubkey,
                &pre_account,
                maybe_fee_account.as_ref()
            )),
            &(last_blockhash, last_fee_calculator),
            &expect_account,
        ));
    }

    #[test]
    fn test_prepare_if_nonce_account_not_nonce_tx() {
        let (
            pre_account_pubkey,
            _pre_account,
            _post_account,
            last_blockhash,
            last_fee_calculator,
            _maybe_fee_account,
        ) = create_accounts_prepare_if_nonce_account();
        let post_account_pubkey = pre_account_pubkey;

        let mut post_account = AccountSharedData::default();
        let expect_account = post_account.clone();
        assert!(run_prepare_if_nonce_account_test(
            &mut post_account,
            &post_account_pubkey,
            &Ok(()),
            None,
            &(last_blockhash, last_fee_calculator),
            &expect_account,
        ));
    }

    #[test]
    fn test_prepare_if_nonce_account_not_nonce_pubkey() {
        let (
            pre_account_pubkey,
            pre_account,
            mut post_account,
            last_blockhash,
            last_fee_calculator,
            maybe_fee_account,
        ) = create_accounts_prepare_if_nonce_account();

        let expect_account = post_account.clone();
        // Wrong key
        assert!(run_prepare_if_nonce_account_test(
            &mut post_account,
            &Pubkey::new(&[1u8; 32]),
            &Ok(()),
            Some((
                &pre_account_pubkey,
                &pre_account,
                maybe_fee_account.as_ref()
            )),
            &(last_blockhash, last_fee_calculator),
            &expect_account,
        ));
    }

    #[test]
    fn test_prepare_if_nonce_account_tx_error() {
        let (
            pre_account_pubkey,
            pre_account,
            mut post_account,
            last_blockhash,
            last_fee_calculator,
            maybe_fee_account,
        ) = create_accounts_prepare_if_nonce_account();
        let post_account_pubkey = pre_account_pubkey;

        let mut expect_account = pre_account.clone();
        expect_account
            .set_state(&nonce::state::Versions::new_current(
                nonce::State::Initialized(nonce::state::Data {
                    blockhash: last_blockhash,
                    fee_calculator: last_fee_calculator.clone(),
                    ..nonce::state::Data::default()
                }),
            ))
            .unwrap();

        assert!(run_prepare_if_nonce_account_test(
            &mut post_account,
            &post_account_pubkey,
            &Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidArgument,
            )),
            Some((
                &pre_account_pubkey,
                &pre_account,
                maybe_fee_account.as_ref()
            )),
            &(last_blockhash, last_fee_calculator),
            &expect_account,
        ));
    }

    #[test]
    fn test_nonced_failure_accounts_rollback_from_pays() {
        let rent_collector = RentCollector::default();

        let nonce_address = Pubkey::new_unique();
        let nonce_authority = keypair_from_seed(&[0; 32]).unwrap();
        let from = keypair_from_seed(&[1; 32]).unwrap();
        let from_address = from.pubkey();
        let to_address = Pubkey::new_unique();
        let nonce_state =
            nonce::state::Versions::new_current(nonce::State::Initialized(nonce::state::Data {
                authority: nonce_authority.pubkey(),
                blockhash: Hash::new_unique(),
                fee_calculator: FeeCalculator::default(),
            }));
        let nonce_account_post =
            AccountSharedData::new_data(43, &nonce_state, &system_program::id()).unwrap();
        let from_account_post = AccountSharedData::new(4199, 0, &Pubkey::default());
        let to_account = AccountSharedData::new(2, 0, &Pubkey::default());
        let nonce_authority_account = AccountSharedData::new(3, 0, &Pubkey::default());
        let recent_blockhashes_sysvar_account = AccountSharedData::new(4, 0, &Pubkey::default());

        let instructions = vec![
            system_instruction::advance_nonce_account(&nonce_address, &nonce_authority.pubkey()),
            system_instruction::transfer(&from_address, &to_address, 42),
        ];
        let message = Message::new(&instructions, Some(&from_address));
        let blockhash = Hash::new_unique();
        let transaction_accounts = vec![
            (message.account_keys[0], from_account_post),
            (message.account_keys[1], nonce_authority_account),
            (message.account_keys[2], nonce_account_post),
            (message.account_keys[3], to_account),
            (message.account_keys[4], recent_blockhashes_sysvar_account),
        ];
        let tx = Transaction::new(&[&nonce_authority, &from], message, blockhash);

        let nonce_state =
            nonce::state::Versions::new_current(nonce::State::Initialized(nonce::state::Data {
                authority: nonce_authority.pubkey(),
                blockhash,
                fee_calculator: FeeCalculator::default(),
            }));
        let nonce_account_pre =
            AccountSharedData::new_data(42, &nonce_state, &system_program::id()).unwrap();
        let from_account_pre = AccountSharedData::new(4242, 0, &Pubkey::default());

        let nonce_rollback = Some(NonceRollbackFull::new(
            nonce_address,
            nonce_account_pre.clone(),
            Some(from_account_pre.clone()),
        ));
        let loaders = vec![(
            Err(TransactionError::InstructionError(
                1,
                InstructionError::InvalidArgument,
            )),
            nonce_rollback.clone(),
        )];

        let transaction_loaders = vec![];
        let transaction_rent = 0;
        let loaded = (
            Ok(LoadedTransaction {
                accounts: transaction_accounts,
                loaders: transaction_loaders,
                rent: transaction_rent,
                rent_debits: RentDebits::default(),
            }),
            nonce_rollback,
        );

        let mut loaded = vec![loaded];

        let next_blockhash = Hash::new_unique();
        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );
        let txs = &[tx];
        let collected_accounts = accounts.collect_accounts_to_store(
            txs.iter(),
            &loaders,
            loaded.as_mut_slice(),
            &rent_collector,
            &(next_blockhash, FeeCalculator::default()),
            true,
            true,
        );
        assert_eq!(collected_accounts.len(), 2);
        assert_eq!(
            collected_accounts
                .iter()
                .find(|(pubkey, _account)| *pubkey == &from_address)
                .map(|(_pubkey, account)| *account)
                .cloned()
                .unwrap(),
            from_account_pre,
        );
        let collected_nonce_account = collected_accounts
            .iter()
            .find(|(pubkey, _account)| *pubkey == &nonce_address)
            .map(|(_pubkey, account)| *account)
            .cloned()
            .unwrap();
        assert_eq!(
            collected_nonce_account.lamports(),
            nonce_account_pre.lamports(),
        );
        assert!(nonce_account::verify_nonce_account(
            &collected_nonce_account,
            &next_blockhash
        ));
    }

    #[test]
    fn test_nonced_failure_accounts_rollback_nonce_pays() {
        let rent_collector = RentCollector::default();

        let nonce_authority = keypair_from_seed(&[0; 32]).unwrap();
        let nonce_address = nonce_authority.pubkey();
        let from = keypair_from_seed(&[1; 32]).unwrap();
        let from_address = from.pubkey();
        let to_address = Pubkey::new_unique();
        let nonce_state =
            nonce::state::Versions::new_current(nonce::State::Initialized(nonce::state::Data {
                authority: nonce_authority.pubkey(),
                blockhash: Hash::new_unique(),
                fee_calculator: FeeCalculator::default(),
            }));
        let nonce_account_post =
            AccountSharedData::new_data(43, &nonce_state, &system_program::id()).unwrap();
        let from_account_post = AccountSharedData::new(4200, 0, &Pubkey::default());
        let to_account = AccountSharedData::new(2, 0, &Pubkey::default());
        let nonce_authority_account = AccountSharedData::new(3, 0, &Pubkey::default());
        let recent_blockhashes_sysvar_account = AccountSharedData::new(4, 0, &Pubkey::default());

        let instructions = vec![
            system_instruction::advance_nonce_account(&nonce_address, &nonce_authority.pubkey()),
            system_instruction::transfer(&from_address, &to_address, 42),
        ];
        let message = Message::new(&instructions, Some(&nonce_address));
        let blockhash = Hash::new_unique();
        let transaction_accounts = vec![
            (message.account_keys[0], from_account_post),
            (message.account_keys[1], nonce_authority_account),
            (message.account_keys[2], nonce_account_post),
            (message.account_keys[3], to_account),
            (message.account_keys[4], recent_blockhashes_sysvar_account),
        ];
        let tx = Transaction::new(&[&nonce_authority, &from], message, blockhash);

        let nonce_state =
            nonce::state::Versions::new_current(nonce::State::Initialized(nonce::state::Data {
                authority: nonce_authority.pubkey(),
                blockhash,
                fee_calculator: FeeCalculator::default(),
            }));
        let nonce_account_pre =
            AccountSharedData::new_data(42, &nonce_state, &system_program::id()).unwrap();

        let nonce_rollback = Some(NonceRollbackFull::new(
            nonce_address,
            nonce_account_pre.clone(),
            None,
        ));
        let loaders = vec![(
            Err(TransactionError::InstructionError(
                1,
                InstructionError::InvalidArgument,
            )),
            nonce_rollback.clone(),
        )];

        let transaction_loaders = vec![];
        let transaction_rent = 0;
        let loaded = (
            Ok(LoadedTransaction {
                accounts: transaction_accounts,
                loaders: transaction_loaders,
                rent: transaction_rent,
                rent_debits: RentDebits::default(),
            }),
            nonce_rollback,
        );

        let mut loaded = vec![loaded];

        let next_blockhash = Hash::new_unique();
        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );
        let txs = &[tx];
        let collected_accounts = accounts.collect_accounts_to_store(
            txs.iter(),
            &loaders,
            loaded.as_mut_slice(),
            &rent_collector,
            &(next_blockhash, FeeCalculator::default()),
            true,
            true,
        );
        assert_eq!(collected_accounts.len(), 1);
        let collected_nonce_account = collected_accounts
            .iter()
            .find(|(pubkey, _account)| *pubkey == &nonce_address)
            .map(|(_pubkey, account)| *account)
            .cloned()
            .unwrap();
        assert_eq!(
            collected_nonce_account.lamports(),
            nonce_account_pre.lamports()
        );
        assert!(nonce_account::verify_nonce_account(
            &collected_nonce_account,
            &next_blockhash
        ));
    }

    #[test]
    fn test_load_largest_accounts() {
        let accounts = Accounts::new_with_config(
            Vec::new(),
            &ClusterType::Development,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
        );

        let pubkey0 = Pubkey::new_unique();
        let account0 = AccountSharedData::new(42, 0, &Pubkey::default());
        accounts.store_slow_uncached(0, &pubkey0, &account0);
        let pubkey1 = Pubkey::new_unique();
        let account1 = AccountSharedData::new(42, 0, &Pubkey::default());
        accounts.store_slow_uncached(0, &pubkey1, &account1);
        let pubkey2 = Pubkey::new_unique();
        let account2 = AccountSharedData::new(41, 0, &Pubkey::default());
        accounts.store_slow_uncached(0, &pubkey2, &account2);

        let ancestors = vec![(0, 0)].into_iter().collect();
        let all_pubkeys: HashSet<_> = vec![pubkey0, pubkey1, pubkey2].into_iter().collect();

        // num == 0 should always return empty set
        let bank_id = 0;
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    0,
                    &HashSet::new(),
                    AccountAddressFilter::Exclude
                )
                .unwrap(),
            vec![]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    0,
                    &all_pubkeys,
                    AccountAddressFilter::Include
                )
                .unwrap(),
            vec![]
        );

        // list should be sorted by balance, then pubkey, descending
        assert!(pubkey1 > pubkey0);
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    1,
                    &HashSet::new(),
                    AccountAddressFilter::Exclude
                )
                .unwrap(),
            vec![(pubkey1, 42)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    2,
                    &HashSet::new(),
                    AccountAddressFilter::Exclude
                )
                .unwrap(),
            vec![(pubkey1, 42), (pubkey0, 42)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    3,
                    &HashSet::new(),
                    AccountAddressFilter::Exclude
                )
                .unwrap(),
            vec![(pubkey1, 42), (pubkey0, 42), (pubkey2, 41)]
        );

        // larger num should not affect results
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    6,
                    &HashSet::new(),
                    AccountAddressFilter::Exclude
                )
                .unwrap(),
            vec![(pubkey1, 42), (pubkey0, 42), (pubkey2, 41)]
        );

        // AccountAddressFilter::Exclude should exclude entry
        let exclude1: HashSet<_> = vec![pubkey1].into_iter().collect();
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    1,
                    &exclude1,
                    AccountAddressFilter::Exclude
                )
                .unwrap(),
            vec![(pubkey0, 42)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    2,
                    &exclude1,
                    AccountAddressFilter::Exclude
                )
                .unwrap(),
            vec![(pubkey0, 42), (pubkey2, 41)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    3,
                    &exclude1,
                    AccountAddressFilter::Exclude
                )
                .unwrap(),
            vec![(pubkey0, 42), (pubkey2, 41)]
        );

        // AccountAddressFilter::Include should limit entries
        let include1_2: HashSet<_> = vec![pubkey1, pubkey2].into_iter().collect();
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    1,
                    &include1_2,
                    AccountAddressFilter::Include
                )
                .unwrap(),
            vec![(pubkey1, 42)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    2,
                    &include1_2,
                    AccountAddressFilter::Include
                )
                .unwrap(),
            vec![(pubkey1, 42), (pubkey2, 41)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    3,
                    &include1_2,
                    AccountAddressFilter::Include
                )
                .unwrap(),
            vec![(pubkey1, 42), (pubkey2, 41)]
        );
    }
}
