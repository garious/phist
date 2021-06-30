use {
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_measure::measure::Measure,
    solana_metrics::datapoint_info,
    solana_runtime::bank::Bank,
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock, RwLockWriteGuard,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

#[derive(Debug, Clone, Copy)]
pub struct AccountHistoryConfig {
    pub num_slots: usize,
    pub updates_only: bool,
}

pub type AccountHistory = BTreeMap<Slot, HashMap<Pubkey, Account>>;
pub type AccountKeys = HashSet<Pubkey>;

pub struct RpcAccountHistoryService {
    thread_hdl: JoinHandle<()>,
}

impl RpcAccountHistoryService {
    pub fn new(
        config: AccountHistoryConfig,
        account_keys: Arc<RwLock<AccountKeys>>,
        account_history: Arc<RwLock<AccountHistory>>,
        account_history_receiver: Receiver<Arc<Bank>>,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        let exit = exit.clone();
        let thread_hdl = Builder::new()
            .name("solana-account-history".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(RecvTimeoutError::Disconnected) = Self::receive_bank(
                    &config,
                    &account_keys,
                    &account_history,
                    &account_history_receiver,
                ) {
                    break;
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    fn receive_bank(
        config: &AccountHistoryConfig,
        account_keys: &Arc<RwLock<AccountKeys>>,
        account_history: &Arc<RwLock<AccountHistory>>,
        account_history_receiver: &Receiver<Arc<Bank>>,
    ) -> Result<(), RecvTimeoutError> {
        let frozen_bank = account_history_receiver.recv_timeout(Duration::from_secs(1))?;
        debug!(
            "rpc account-history service received bank: {:?}",
            frozen_bank.slot()
        );

        let r_account_keys = account_keys.read().unwrap();
        let mut measure_collect = Measure::start("collect-account-history");
        let slot_accounts =
            Self::collect_accounts(&frozen_bank, &r_account_keys, config.updates_only);
        measure_collect.stop();
        drop(r_account_keys);

        let mut measure_write = Measure::start("write-account-history");
        let mut w_account_history = account_history.write().unwrap();
        w_account_history.insert(frozen_bank.slot(), slot_accounts);
        measure_write.stop();

        let mut measure_prune = Measure::start("prune-account-history");
        Self::remove_old_slots(w_account_history, &config.num_slots);
        measure_prune.stop();

        datapoint_info!(
            "rpc_account_history",
            ("collect", measure_collect.as_us(), i64),
            ("write", measure_write.as_us(), i64),
            ("prune", measure_prune.as_us(), i64),
        );

        Ok(())
    }

    fn collect_accounts(
        frozen_bank: &Bank,
        accounts: &HashSet<Pubkey>,
        updates_only: bool,
    ) -> HashMap<Pubkey, Account> {
        let mut slot_accounts = HashMap::new();
        for address in accounts.iter() {
            let shared_account = if updates_only {
                frozen_bank
                    .get_account_modified_slot(address)
                    .filter(|(_shared_account, slot)| *slot == frozen_bank.slot())
                    .map(|(shared_account, _slot)| shared_account)
            } else {
                frozen_bank.get_account(address)
            };
            if let Some(shared_account) = shared_account {
                slot_accounts.insert(*address, shared_account.into());
            }
        }
        slot_accounts
    }

    fn remove_old_slots(
        mut w_account_history: RwLockWriteGuard<AccountHistory>,
        num_slots: &usize,
    ) {
        while w_account_history.len() > *num_slots {
            let oldest_slot = w_account_history.keys().cloned().next().unwrap_or_default();
            w_account_history.remove(&oldest_slot);
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_runtime::bank::Bank, solana_sdk::genesis_config::create_genesis_config};

    #[test]
    fn test_collect_accounts() {
        let (genesis_config, mint_keypair) = create_genesis_config(1_000);
        let mut bank = Arc::new(Bank::new(&genesis_config));

        let address = Pubkey::new_unique();
        let account_keys = vec![address].into_iter().collect();

        bank.transfer(42, &mint_keypair, &address).unwrap();
        let all_accounts = RpcAccountHistoryService::collect_accounts(&bank, &account_keys, false);
        let updated_accounts =
            RpcAccountHistoryService::collect_accounts(&bank, &account_keys, true);
        assert_eq!(all_accounts, updated_accounts);
        assert_eq!(all_accounts.len(), 1);

        bank = Arc::new(Bank::new_from_parent(
            &bank,
            &Pubkey::default(),
            bank.slot() + 1,
        ));
        let all_accounts = RpcAccountHistoryService::collect_accounts(&bank, &account_keys, false);
        let updated_accounts =
            RpcAccountHistoryService::collect_accounts(&bank, &account_keys, true);
        assert_ne!(all_accounts, updated_accounts);
        assert_eq!(all_accounts.len(), 1);
        assert_eq!(updated_accounts.len(), 0);
    }

    #[test]
    fn test_remove_old_slots() {
        let num_slots = 3;
        let account_history = RwLock::new(BTreeMap::new());
        assert_eq!(account_history.read().unwrap().len(), 0);
        RpcAccountHistoryService::remove_old_slots(account_history.write().unwrap(), &num_slots);
        assert_eq!(account_history.read().unwrap().len(), 0);

        let accounts: HashMap<Pubkey, Account> = vec![
            (Pubkey::new_unique(), Account::default()),
            (Pubkey::new_unique(), Account::default()),
        ]
        .into_iter()
        .collect();
        account_history.write().unwrap().insert(0, accounts.clone());
        assert_eq!(account_history.read().unwrap().len(), 1);
        RpcAccountHistoryService::remove_old_slots(account_history.write().unwrap(), &num_slots);
        assert_eq!(account_history.read().unwrap().len(), 1);

        for slot in 1..num_slots {
            account_history
                .write()
                .unwrap()
                .insert(slot as Slot, accounts.clone());
        }
        assert_eq!(account_history.read().unwrap().len(), num_slots);
        RpcAccountHistoryService::remove_old_slots(account_history.write().unwrap(), &num_slots);
        assert_eq!(account_history.read().unwrap().len(), num_slots);

        for slot in num_slots..num_slots + 2 {
            account_history
                .write()
                .unwrap()
                .insert(slot as Slot, accounts.clone());
        }
        assert_eq!(account_history.read().unwrap().len(), num_slots + 2);
        RpcAccountHistoryService::remove_old_slots(account_history.write().unwrap(), &num_slots);
        assert_eq!(account_history.read().unwrap().len(), num_slots);
        assert_eq!(*account_history.read().unwrap().iter().next().unwrap().0, 2);
    }
}
