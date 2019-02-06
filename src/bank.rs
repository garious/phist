//! The `bank` module tracks client accounts and the progress of on-chain
//! programs. It offers a high-level API that signs transactions
//! on behalf of the caller, and a low-level API for when they have
//! already been signed and verified.

use crate::bank_delta::BankDelta;
use crate::bank_fork::BankFork;
use crate::entry::Entry;
use crate::entry::EntrySlice;
use crate::forks::{self, Forks};
use crate::genesis_block::GenesisBlock;
use crate::leader_scheduler::LeaderScheduler;
use crate::leader_scheduler::DEFAULT_TICKS_PER_SLOT;
use crate::poh_recorder::PohRecorder;
use crate::rpc_pubsub::RpcSubscriptions;
use bincode::deserialize;
use itertools::Itertools;
use solana_native_loader;
use solana_sdk::account::Account;
use solana_sdk::bpf_loader;
use solana_sdk::budget_program;
use solana_sdk::hash::Hash;
use solana_sdk::native_program::ProgramError;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signature::Signature;
use solana_sdk::storage_program;
use solana_sdk::system_program;
use solana_sdk::system_transaction::SystemTransaction;
use solana_sdk::token_program;
use solana_sdk::transaction::Transaction;
use solana_sdk::vote_program;
use std;
use std::result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

/// Reasons a transaction might be rejected.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BankError {
    /// This Pubkey is being processed in another transaction
    AccountInUse,

    /// Attempt to debit from `Pubkey`, but no found no record of a prior credit.
    AccountNotFound,

    /// The from `Pubkey` does not have sufficient balance to pay the fee to schedule the transaction
    InsufficientFundsForFee,

    /// The bank has seen `Signature` before. This can occur under normal operation
    /// when a UDP packet is duplicated, as a user error from a client not updating
    /// its `last_id`, or as a double-spend attack.
    DuplicateSignature,

    /// The bank has not seen the given `last_id` or the transaction is too old and
    /// the `last_id` has been discarded.
    LastIdNotFound,

    /// Proof of History verification failed.
    LedgerVerificationFailed,

    /// The program returned an error
    ProgramError(u8, ProgramError),

    /// Recoding into PoH failed
    RecordFailure,

    /// Loader call chain too deep
    CallChainTooDeep,

    /// Transaction has a fee but has no signature present
    MissingSignatureForFee,

    // Poh recorder hit the maximum tick height before leader rotation
    MaxHeightReached,
    /// Fork is not in the Deltas DAG
    UnknownFork,

    /// The specified trunk is not in the Deltas DAG
    InvalidTrunk,

    /// Specified base delta is still live
    DeltaNotFrozen,

    /// Requested live delta is frozen
    DeltaIsFrozen,
}

pub type Result<T> = result::Result<T, BankError>;

pub const VERIFY_BLOCK_SIZE: usize = 16;

pub trait BankSubscriptions {
    fn check_account(&self, pubkey: &Pubkey, account: &Account);
    fn check_signature(&self, signature: &Signature, status: &Result<()>);
}

/// Manager for the state of all accounts and programs after processing its entries.
pub struct Bank {
    forks: RwLock<Forks>,

    // The latest confirmation time for the network
    confirmation_time: AtomicUsize,

    /// Tracks and updates the leader schedule based on the votes and account stakes
    /// processed by the bank
    pub leader_scheduler: Arc<RwLock<LeaderScheduler>>,
    subscriptions: RwLock<Option<Arc<RpcSubscriptions>>>,
}

impl Default for Bank {
    fn default() -> Self {
        Bank {
            forks: RwLock::new(Forks::default()),
            confirmation_time: AtomicUsize::new(std::usize::MAX),
            leader_scheduler: Arc::new(RwLock::new(LeaderScheduler::default())),
            subscriptions: RwLock::new(None),
        }
    }
}

impl Bank {
    pub fn new(genesis_block: &GenesisBlock) -> Self {
        let bank = Self::default();
        let last_id = genesis_block.last_id();
        bank.init_root(&last_id);
        bank.process_genesis_block(genesis_block);
        bank.add_builtin_programs();
        bank
    }
    pub fn init_fork(&self, current: u64, last_id: &Hash, base: u64) -> Result<()> {
        if self.forks.read().unwrap().is_active_fork(current) {
            return Ok(());
        }
        self.forks
            .write()
            .unwrap()
            .init_fork(current, last_id, base)
    }
    pub fn active_fork(&self) -> BankFork {
        self.forks.read().unwrap().active_fork()
    }
    pub fn root(&self) -> BankFork {
        self.forks.read().unwrap().root()
    }
    pub fn fork(&self, slot: u64) -> Option<BankFork> {
        self.forks.read().unwrap().fork(slot)
    }

    pub fn set_subscriptions(&self, subscriptions: Arc<RpcSubscriptions>) {
        let mut sub = self.subscriptions.write().unwrap();
        *sub = Some(subscriptions)
    }

    /// Init the root fork.  Only tests should be using this.
    pub fn init_root(&self, last_id: &Hash) {
        self.forks
            .write()
            .unwrap()
            .init_root(BankDelta::new(0, &last_id));
    }

    fn process_genesis_block(&self, genesis_block: &GenesisBlock) {
        assert!(genesis_block.mint_id != Pubkey::default());
        assert!(genesis_block.tokens >= genesis_block.bootstrap_leader_tokens);

        let mut mint_account = Account::default();
        let mut bootstrap_leader_account = Account::default();
        mint_account.tokens += genesis_block.tokens;

        if genesis_block.bootstrap_leader_id != Pubkey::default() {
            mint_account.tokens -= genesis_block.bootstrap_leader_tokens;
            bootstrap_leader_account.tokens += genesis_block.bootstrap_leader_tokens;
            self.root().head().store_slow(
                true,
                &genesis_block.bootstrap_leader_id,
                &bootstrap_leader_account,
            );
        };

        self.root()
            .head()
            .store_slow(true, &genesis_block.mint_id, &mint_account);

        self.root()
            .head()
            .set_genesis_last_id(&genesis_block.last_id());
    }

    fn add_system_program(&self) {
        let system_program_account = Account {
            tokens: 1,
            owner: system_program::id(),
            userdata: b"solana_system_program".to_vec(),
            executable: true,
            loader: solana_native_loader::id(),
        };
        self.root()
            .head()
            .store_slow(true, &system_program::id(), &system_program_account);
    }

    fn add_builtin_programs(&self) {
        self.add_system_program();

        // Vote program
        let vote_program_account = Account {
            tokens: 1,
            owner: vote_program::id(),
            userdata: b"solana_vote_program".to_vec(),
            executable: true,
            loader: solana_native_loader::id(),
        };
        self.root()
            .head()
            .store_slow(true, &vote_program::id(), &vote_program_account);

        // Storage program
        let storage_program_account = Account {
            tokens: 1,
            owner: storage_program::id(),
            userdata: b"solana_storage_program".to_vec(),
            executable: true,
            loader: solana_native_loader::id(),
        };
        self.root()
            .head()
            .store_slow(true, &storage_program::id(), &storage_program_account);

        let storage_system_account = Account {
            tokens: 1,
            owner: storage_program::system_id(),
            userdata: vec![0; 16 * 1024],
            executable: false,
            loader: Pubkey::default(),
        };
        self.root()
            .head()
            .store_slow(true, &storage_program::system_id(), &storage_system_account);

        // Bpf Loader
        let bpf_loader_account = Account {
            tokens: 1,
            owner: bpf_loader::id(),
            userdata: b"solana_bpf_loader".to_vec(),
            executable: true,
            loader: solana_native_loader::id(),
        };

        self.root()
            .head()
            .store_slow(true, &bpf_loader::id(), &bpf_loader_account);

        // Budget program
        let budget_program_account = Account {
            tokens: 1,
            owner: budget_program::id(),
            userdata: b"solana_budget_program".to_vec(),
            executable: true,
            loader: solana_native_loader::id(),
        };
        self.root()
            .head()
            .store_slow(true, &budget_program::id(), &budget_program_account);

        // Erc20 token program
        let erc20_account = Account {
            tokens: 1,
            owner: token_program::id(),
            userdata: b"solana_erc20".to_vec(),
            executable: true,
            loader: solana_native_loader::id(),
        };

        self.root()
            .head()
            .store_slow(true, &token_program::id(), &erc20_account);
    }

    pub fn get_storage_entry_height(&self) -> u64 {
        //TODO: root or live?
        match self
            .active_fork()
            .get_account_slow(&storage_program::system_id())
        {
            Some(storage_system_account) => {
                let state = deserialize(&storage_system_account.userdata);
                if let Ok(state) = state {
                    let state: storage_program::StorageProgramState = state;
                    return state.entry_height;
                }
            }
            None => {
                info!("error in reading entry_height");
            }
        }
        0
    }

    pub fn get_storage_last_id(&self) -> Hash {
        if let Some(storage_system_account) = self
            .active_fork()
            .get_account_slow(&storage_program::system_id())
        {
            let state = deserialize(&storage_system_account.userdata);
            if let Ok(state) = state {
                let state: storage_program::StorageProgramState = state;
                return state.id;
            }
        }
        Hash::default()
    }

    /// Starting from the genesis block, append the provided entries to the ledger verifying them
    /// along the way.
    pub fn process_ledger<I>(&mut self, entries: I) -> Result<(u64, Hash)>
    where
        I: IntoIterator<Item = Entry>,
    {
        let mut entry_height = 0;
        // assumes this function is starting from genesis
        let mut last_id = self.root().last_id();

        // Ledger verification needs to be parallelized, but we can't pull the whole
        // thing into memory. We therefore chunk it.
        for block in &entries.into_iter().chunks(VERIFY_BLOCK_SIZE) {
            let block: Vec<_> = block.collect();

            if !block.verify(&last_id) {
                warn!("Ledger proof of history failed at entry: {}", entry_height);
                return Err(BankError::LedgerVerificationFailed);
            }

            let slot = block[0].tick_height / DEFAULT_TICKS_PER_SLOT;
            if slot > 0 && block[0].tick_height % DEFAULT_TICKS_PER_SLOT == 0 {
                //TODO: EntryTree should provide base slot
                let base = slot - 1;
                {
                    info!("freezing from ledger at {}", base);
                    let base_state = self.fork(base).expect("base fork");
                    base_state.head().freeze();
                }
                self.init_fork(slot, &block[0].id, base)
                    .expect("init new fork");
                self.merge_into_root(slot);
            }

            let bank_state = self.fork(slot).unwrap();
            bank_state.process_entries(&block)?;
            last_id = block.last().unwrap().id;
            entry_height += block.len() as u64;
        }
        Ok((entry_height, last_id))
    }

    #[must_use]
    pub fn process_and_record_transactions(
        &self,
        txs: &[Transaction],
        poh: Option<&PohRecorder>,
    ) -> Result<Vec<Result<()>>> {
        let sub = self.subscriptions.read().unwrap();
        self.active_fork()
            .process_and_record_transactions(&sub, txs, poh)
    }

    /// Process a Transaction. This is used for unit tests and simply calls the vector Bank::process_transactions method.
    pub fn process_transaction(&self, tx: &Transaction) -> Result<()> {
        let txs = vec![tx.clone()];
        match self.process_transactions(&txs)[0] {
            Err(ref e) => {
                info!("process_transaction error: {:?}", e);
                Err((*e).clone())
            }
            Ok(_) => Ok(()),
        }
    }

    #[must_use]
    pub fn process_transactions(&self, txs: &[Transaction]) -> Vec<Result<()>> {
        self.process_and_record_transactions(txs, None)
            .expect("record skipped")
    }

    /// Create, sign, and process a Transaction from `keypair` to `to` of
    /// `n` tokens where `last_id` is the last Entry ID observed by the client.
    pub fn transfer(
        &self,
        n: u64,
        keypair: &Keypair,
        to: Pubkey,
        last_id: Hash,
    ) -> Result<Signature> {
        let tx = SystemTransaction::new_account(keypair, to, n, last_id, 0);
        let signature = tx.signatures[0];
        self.process_transaction(&tx).map(|_| signature)
    }

    pub fn confirmation_time(&self) -> usize {
        self.confirmation_time.load(Ordering::Relaxed)
    }

    pub fn set_confirmation_time(&self, confirmation: usize) {
        self.confirmation_time
            .store(confirmation, Ordering::Relaxed);
    }

    pub fn get_current_leader(&self) -> Option<(Pubkey, u64)> {
        let live_height = self.active_fork().tick_height();
        self.leader_scheduler
            .read()
            .unwrap()
            .get_scheduled_leader(live_height + 1)
    }

    /// An active chain is computed from the leaf_slot
    /// The base that is a direct descendant of the root and is in the active chain to the leaf
    /// is merged into root, and any forks not attached to the new root are purged.
    pub fn merge_into_root(&self, leaf_slot: u64) {
        //there is only one base, and its the current live fork
        self.forks
            .write()
            .unwrap()
            .merge_into_root(forks::ROLLBACK_DEPTH, leaf_slot)
            .expect("merge into root");
        let height = self.root().tick_height();
        self.leader_scheduler
            .write()
            .unwrap()
            .update_height(height, &self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bank_fork::BankFork;
    use crate::entry::{next_entries, next_entry, Entry};
    use crate::gen_keys::GenKeys;
    use crate::poh_recorder::PohRecorder;
    use bincode::serialize;
    use hashbrown::HashSet;
    use solana_sdk::hash::hash;
    use solana_sdk::native_program::ProgramError;
    use solana_sdk::signature::Keypair;
    use solana_sdk::signature::KeypairUtil;
    use solana_sdk::storage_program::{StorageTransaction, ENTRIES_PER_SEGMENT};
    use solana_sdk::system_instruction::SystemInstruction;
    use solana_sdk::system_transaction::SystemTransaction;
    use solana_sdk::transaction::Instruction;
    use std;
    use std::sync::mpsc::channel;

    #[test]
    fn test_bank_new() {
        let (genesis_block, _) = GenesisBlock::new(10_000);
        let bank = Bank::new(&genesis_block);
        assert_eq!(
            bank.active_fork().get_balance_slow(&genesis_block.mint_id),
            10_000
        );
    }

    #[test]
    fn test_bank_new_with_leader() {
        let dummy_leader_id = Keypair::new().pubkey();
        let dummy_leader_tokens = 1;
        let (genesis_block, _) =
            GenesisBlock::new_with_leader(10_000, dummy_leader_id, dummy_leader_tokens);
        let bank = Bank::new(&genesis_block);
        assert_eq!(
            bank.active_fork().get_balance_slow(&genesis_block.mint_id),
            9999
        );
        assert_eq!(bank.active_fork().get_balance_slow(&dummy_leader_id), 1);
    }

    #[test]
    fn test_two_payments_to_one_party() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(10_000);
        let pubkey = Keypair::new().pubkey();
        let bank = Bank::new(&genesis_block);
        assert_eq!(bank.active_fork().last_id(), genesis_block.last_id());

        bank.transfer(1_000, &mint_keypair, pubkey, genesis_block.last_id())
            .unwrap();
        assert_eq!(bank.active_fork().get_balance_slow(&pubkey), 1_000);

        bank.transfer(500, &mint_keypair, pubkey, genesis_block.last_id())
            .unwrap();
        assert_eq!(bank.active_fork().get_balance_slow(&pubkey), 1_500);
        assert_eq!(bank.active_fork().transaction_count(), 2);
    }

    #[test]
    fn test_one_source_two_tx_one_batch() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(1);
        let key1 = Keypair::new().pubkey();
        let key2 = Keypair::new().pubkey();
        let bank = Bank::new(&genesis_block);
        assert_eq!(bank.active_fork().last_id(), genesis_block.last_id());

        let t1 = SystemTransaction::new_move(&mint_keypair, key1, 1, genesis_block.last_id(), 0);
        let t2 = SystemTransaction::new_move(&mint_keypair, key2, 1, genesis_block.last_id(), 0);
        let res = bank.process_transactions(&vec![t1.clone(), t2.clone()]);
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], Ok(()));
        assert_eq!(res[1], Err(BankError::AccountInUse));
        assert_eq!(
            bank.active_fork().get_balance_slow(&mint_keypair.pubkey()),
            0
        );
        assert_eq!(bank.active_fork().get_balance_slow(&key1), 1);
        assert_eq!(bank.active_fork().get_balance_slow(&key2), 0);
        assert_eq!(
            bank.active_fork().get_signature_status(&t1.signatures[0]),
            Some(Ok(()))
        );
        // TODO: Transactions that fail to pay a fee could be dropped silently
        assert_eq!(
            bank.active_fork().get_signature_status(&t2.signatures[0]),
            Some(Err(BankError::AccountInUse))
        );
    }

    #[test]
    fn test_one_tx_two_out_atomic_fail() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(1);
        let key1 = Keypair::new().pubkey();
        let key2 = Keypair::new().pubkey();
        let bank = Bank::new(&genesis_block);
        let spend = SystemInstruction::Move { tokens: 1 };
        let instructions = vec![
            Instruction {
                program_ids_index: 0,
                userdata: serialize(&spend).unwrap(),
                accounts: vec![0, 1],
            },
            Instruction {
                program_ids_index: 0,
                userdata: serialize(&spend).unwrap(),
                accounts: vec![0, 2],
            },
        ];

        let t1 = Transaction::new_with_instructions(
            &[&mint_keypair],
            &[key1, key2],
            genesis_block.last_id(),
            0,
            vec![system_program::id()],
            instructions,
        );
        let res = bank.process_transactions(&vec![t1.clone()]);
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            Err(BankError::ProgramError(
                1,
                ProgramError::ResultWithNegativeTokens
            ))
        );
        assert_eq!(
            bank.active_fork().get_balance_slow(&mint_keypair.pubkey()),
            1
        );
        assert_eq!(bank.active_fork().get_balance_slow(&key1), 0);
        assert_eq!(bank.active_fork().get_balance_slow(&key2), 0);
        assert_eq!(
            bank.active_fork().get_signature_status(&t1.signatures[0]),
            Some(Err(BankError::ProgramError(
                1,
                ProgramError::ResultWithNegativeTokens
            )))
        );
    }

    #[test]
    fn test_one_tx_two_out_atomic_pass() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(2);
        let key1 = Keypair::new().pubkey();
        let key2 = Keypair::new().pubkey();
        let bank = Bank::new(&genesis_block);
        let t1 = SystemTransaction::new_move_many(
            &mint_keypair,
            &[(key1, 1), (key2, 1)],
            genesis_block.last_id(),
            0,
        );
        let res = bank.process_transactions(&vec![t1.clone()]);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], Ok(()));
        assert_eq!(
            bank.active_fork().get_balance_slow(&mint_keypair.pubkey()),
            0
        );
        assert_eq!(bank.active_fork().get_balance_slow(&key1), 1);
        assert_eq!(bank.active_fork().get_balance_slow(&key2), 1);
        assert_eq!(
            bank.active_fork().get_signature_status(&t1.signatures[0]),
            Some(Ok(()))
        );
    }

    // TODO: This test demonstrates that fees are not paid when a program fails.
    // See github issue 1157 (https://github.com/solana-labs/solana/issues/1157)
    #[test]
    fn test_detect_failed_duplicate_transactions_issue_1157() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(1);
        let bank = Bank::new(&genesis_block);
        let dest = Keypair::new();

        // source with 0 program context
        let tx = SystemTransaction::new_account(
            &mint_keypair,
            dest.pubkey(),
            2,
            genesis_block.last_id(),
            1,
        );
        let signature = tx.signatures[0];
        assert!(!bank.active_fork().head().has_signature(&signature));
        let res = bank.process_transaction(&tx);

        // Result failed, but signature is registered
        assert!(res.is_err());
        assert!(bank.active_fork().head().has_signature(&signature));
        assert_matches!(
            bank.active_fork().get_signature_status(&signature),
            Some(Err(BankError::ProgramError(
                0,
                ProgramError::ResultWithNegativeTokens
            )))
        );

        // The tokens didn't move, but the from address paid the transaction fee.
        assert_eq!(bank.active_fork().get_balance_slow(&dest.pubkey()), 0);

        // BUG: This should be the original balance minus the transaction fee.
        //assert_eq!(bank.active_fork().get_balance_slow(&mint_keypair.pubkey()), 0);
    }

    #[test]
    fn test_account_not_found() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(1);
        let bank = Bank::new(&genesis_block);
        let keypair = Keypair::new();
        assert_eq!(
            bank.transfer(1, &keypair, mint_keypair.pubkey(), genesis_block.last_id()),
            Err(BankError::AccountNotFound)
        );
        assert_eq!(bank.active_fork().transaction_count(), 0);
    }

    #[test]
    fn test_insufficient_funds() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(11_000);
        let bank = Bank::new(&genesis_block);
        let pubkey = Keypair::new().pubkey();
        bank.transfer(1_000, &mint_keypair, pubkey, genesis_block.last_id())
            .unwrap();
        assert_eq!(bank.active_fork().transaction_count(), 1);
        assert_eq!(bank.active_fork().get_balance_slow(&pubkey), 1_000);
        assert_matches!(
            bank.transfer(10_001, &mint_keypair, pubkey, genesis_block.last_id()),
            Err(BankError::ProgramError(
                0,
                ProgramError::ResultWithNegativeTokens
            ))
        );
        assert_eq!(bank.active_fork().transaction_count(), 1);

        let mint_pubkey = mint_keypair.pubkey();
        assert_eq!(bank.active_fork().get_balance_slow(&mint_pubkey), 10_000);
        assert_eq!(bank.active_fork().get_balance_slow(&pubkey), 1_000);
    }

    #[test]
    fn test_transfer_to_newb() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(10_000);
        let bank = Bank::new(&genesis_block);
        let pubkey = Keypair::new().pubkey();
        bank.transfer(500, &mint_keypair, pubkey, genesis_block.last_id())
            .unwrap();
        assert_eq!(bank.active_fork().get_balance_slow(&pubkey), 500);
    }

    #[test]
    fn test_debits_before_credits() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(2);
        let bank = Bank::new(&genesis_block);
        let keypair = Keypair::new();
        let tx0 = SystemTransaction::new_account(
            &mint_keypair,
            keypair.pubkey(),
            2,
            genesis_block.last_id(),
            0,
        );
        let tx1 = SystemTransaction::new_account(
            &keypair,
            mint_keypair.pubkey(),
            1,
            genesis_block.last_id(),
            0,
        );
        let txs = vec![tx0, tx1];
        let results = bank.process_transactions(&txs);
        assert!(results[1].is_err());

        // Assert bad transactions aren't counted.
        assert_eq!(bank.active_fork().transaction_count(), 1);
    }

    #[test]
    fn test_process_empty_entry_is_registered() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(1);
        let bank = Bank::new(&genesis_block);
        let keypair = Keypair::new();
        let entry = next_entry(&genesis_block.last_id(), 1, vec![]);
        let tx = SystemTransaction::new_account(&mint_keypair, keypair.pubkey(), 1, entry.id, 0);

        // First, ensure the TX is rejected because of the unregistered last ID
        assert_eq!(
            bank.process_transaction(&tx),
            Err(BankError::LastIdNotFound)
        );

        // Now ensure the TX is accepted despite pointing to the ID of an empty entry.
        bank.active_fork().process_entries(&[entry]).unwrap();
        assert_eq!(bank.process_transaction(&tx), Ok(()));
    }

    #[test]
    fn test_process_genesis() {
        let dummy_leader_id = Keypair::new().pubkey();
        let dummy_leader_tokens = 1;
        let (genesis_block, _) =
            GenesisBlock::new_with_leader(5, dummy_leader_id, dummy_leader_tokens);
        let bank = Bank::default();
        bank.init_root(&genesis_block.last_id());
        bank.process_genesis_block(&genesis_block);
        assert_eq!(
            bank.active_fork().get_balance_slow(&genesis_block.mint_id),
            4
        );
        assert_eq!(bank.active_fork().get_balance_slow(&dummy_leader_id), 1);
        // TODO: Restore next assert_eq() once leader scheduler configuration is stored in the
        // genesis block
        /*
        assert_eq!(
            bank.leader_scheduler.read().unwrap().bootstrap_leader,
            dummy_leader_id
        );
        */
    }

    fn create_sample_block_with_next_entries_using_keypairs(
        genesis_block: &GenesisBlock,
        mint_keypair: &Keypair,
        keypairs: &[Keypair],
    ) -> impl Iterator<Item = Entry> {
        let mut entries: Vec<Entry> = vec![];

        // Start off the ledger with a tick linked to the genesis block
        let tick = Entry::new(&genesis_block.last_id(), 0, 1, vec![]);
        let mut hash = tick.id;
        let mut last_id = tick.id;
        entries.push(tick);

        let num_hashes = 1;
        for k in keypairs {
            let tx = SystemTransaction::new_account(mint_keypair, k.pubkey(), 1, last_id, 0);
            let txs = vec![tx];
            let mut e = next_entries(&hash, 0, txs);
            entries.append(&mut e);
            hash = entries.last().unwrap().id;
            let tick = Entry::new(&hash, 0, num_hashes, vec![]);
            hash = tick.id;
            last_id = hash;
            entries.push(tick);
        }
        entries.into_iter()
    }

    // create a ledger with a tick every `tick_interval` entries and a couple other transactions
    fn create_sample_block_with_ticks(
        genesis_block: &GenesisBlock,
        mint_keypair: &Keypair,
        num_entries: usize,
        tick_interval: usize,
    ) -> impl Iterator<Item = Entry> {
        assert!(num_entries > 0);
        let mut entries = Vec::with_capacity(num_entries);

        // Start off the ledger with a tick linked to the genesis block
        let tick = Entry::new(&genesis_block.last_id(), 0, 1, vec![]);
        let mut hash = tick.id;
        let mut last_id = tick.id;
        entries.push(tick);

        let num_hashes = 1;
        for i in 1..num_entries {
            let keypair = Keypair::new();
            let tx = SystemTransaction::new_account(mint_keypair, keypair.pubkey(), 1, last_id, 0);
            let entry = Entry::new(&hash, 0, num_hashes, vec![tx]);
            hash = entry.id;
            entries.push(entry);

            // Add a second Transaction that will produce a
            // ProgramError<0, ResultWithNegativeTokens> error when processed
            let keypair2 = Keypair::new();
            let tx = SystemTransaction::new_account(&keypair, keypair2.pubkey(), 42, last_id, 0);
            let entry = Entry::new(&hash, 0, num_hashes, vec![tx]);
            hash = entry.id;
            entries.push(entry);

            if (i + 1) % tick_interval == 0 {
                let tick = Entry::new(&hash, 0, num_hashes, vec![]);
                hash = tick.id;
                last_id = hash;
                entries.push(tick);
            }
        }
        entries.into_iter()
    }

    fn create_sample_ledger(
        tokens: u64,
        num_entries: usize,
    ) -> (GenesisBlock, Keypair, impl Iterator<Item = Entry>) {
        let mint_keypair = Keypair::new();
        let genesis_block = GenesisBlock {
            bootstrap_leader_id: Keypair::new().pubkey(),
            bootstrap_leader_tokens: 1,
            mint_id: mint_keypair.pubkey(),
            tokens,
        };
        let block =
            create_sample_block_with_ticks(&genesis_block, &mint_keypair, num_entries, num_entries);
        (genesis_block, mint_keypair, block)
    }

    #[test]
    fn test_process_ledger_simple() {
        let (genesis_block, mint_keypair, ledger) = create_sample_ledger(100, 2);
        let mut bank = Bank::default();
        bank.init_root(&genesis_block.last_id());
        bank.process_genesis_block(&genesis_block);
        assert_eq!(bank.active_fork().tick_height(), 0);
        bank.add_system_program();
        let (ledger_height, last_id) = bank.process_ledger(ledger).unwrap();
        assert_eq!(
            bank.active_fork().get_balance_slow(&mint_keypair.pubkey()),
            98
        );
        assert_eq!(ledger_height, 4);
        assert_eq!(bank.active_fork().tick_height(), 2);
        assert_eq!(bank.active_fork().last_id(), last_id);
    }

    #[test]
    fn test_hash_internal_state() {
        let mint_keypair = Keypair::new();
        let genesis_block = GenesisBlock {
            bootstrap_leader_id: Keypair::new().pubkey(),
            bootstrap_leader_tokens: 1,
            mint_id: mint_keypair.pubkey(),
            tokens: 2_000,
        };
        let seed = [0u8; 32];
        let mut rnd = GenKeys::new(seed);
        let keypairs = rnd.gen_n_keypairs(5);
        let ledger0 = create_sample_block_with_next_entries_using_keypairs(
            &genesis_block,
            &mint_keypair,
            &keypairs,
        );
        let ledger1 = create_sample_block_with_next_entries_using_keypairs(
            &genesis_block,
            &mint_keypair,
            &keypairs,
        );

        let mut bank0 = Bank::default();
        bank0.init_root(&genesis_block.last_id());
        bank0.add_system_program();
        bank0.process_genesis_block(&genesis_block);
        bank0.process_ledger(ledger0).unwrap();
        let mut bank1 = Bank::default();
        bank1.init_root(&genesis_block.last_id());
        bank1.add_system_program();
        bank1.process_genesis_block(&genesis_block);
        bank1.process_ledger(ledger1).unwrap();

        let initial_state = bank0.active_fork().hash_internal_state();

        assert_eq!(bank1.active_fork().hash_internal_state(), initial_state);

        let pubkey = keypairs[0].pubkey();
        bank0
            .transfer(1_000, &mint_keypair, pubkey, bank0.active_fork().last_id())
            .unwrap();
        assert_ne!(bank0.active_fork().hash_internal_state(), initial_state);
        bank1
            .transfer(1_000, &mint_keypair, pubkey, bank1.active_fork().last_id())
            .unwrap();
        assert_eq!(
            bank0.active_fork().hash_internal_state(),
            bank1.active_fork().hash_internal_state()
        );
    }
    #[test]
    fn test_confirmation_time() {
        let def_bank = Bank::default();
        assert_eq!(def_bank.confirmation_time(), std::usize::MAX);
        def_bank.set_confirmation_time(90);
        assert_eq!(def_bank.confirmation_time(), 90);
    }
    #[test]
    fn test_par_process_entries_tick() {
        let (genesis_block, _mint_keypair) = GenesisBlock::new(1000);
        let bank = Bank::new(&genesis_block);

        // ensure bank can process a tick
        let tick = next_entry(&genesis_block.last_id(), 1, vec![]);
        assert_eq!(bank.active_fork().process_entries(&[tick.clone()]), Ok(()));
        assert_eq!(bank.active_fork().last_id(), tick.id);
    }
    #[test]
    fn test_par_process_entries_2_entries_collision() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(1000);
        let bank = Bank::new(&genesis_block);
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();

        let last_id = bank.active_fork().last_id();

        // ensure bank can process 2 entries that have a common account and no tick is registered
        let tx = SystemTransaction::new_account(
            &mint_keypair,
            keypair1.pubkey(),
            2,
            bank.active_fork().last_id(),
            0,
        );
        let entry_1 = next_entry(&last_id, 1, vec![tx]);
        let tx = SystemTransaction::new_account(
            &mint_keypair,
            keypair2.pubkey(),
            2,
            bank.active_fork().last_id(),
            0,
        );
        let entry_2 = next_entry(&entry_1.id, 1, vec![tx]);
        assert_eq!(
            bank.active_fork().process_entries(&[entry_1, entry_2]),
            Ok(())
        );
        assert_eq!(bank.active_fork().get_balance_slow(&keypair1.pubkey()), 2);
        assert_eq!(bank.active_fork().get_balance_slow(&keypair2.pubkey()), 2);
        assert_eq!(bank.active_fork().last_id(), last_id);
    }
    #[test]
    fn test_par_process_entries_2_txes_collision() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(1000);
        let bank = Bank::new(&genesis_block);
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let keypair3 = Keypair::new();

        // fund: put 4 in each of 1 and 2
        assert_matches!(
            bank.transfer(
                4,
                &mint_keypair,
                keypair1.pubkey(),
                bank.active_fork().last_id()
            ),
            Ok(_)
        );
        assert_matches!(
            bank.transfer(
                4,
                &mint_keypair,
                keypair2.pubkey(),
                bank.active_fork().last_id()
            ),
            Ok(_)
        );

        // construct an Entry whose 2nd transaction would cause a lock conflict with previous entry
        let entry_1_to_mint = next_entry(
            &bank.active_fork().last_id(),
            1,
            vec![SystemTransaction::new_account(
                &keypair1,
                mint_keypair.pubkey(),
                1,
                bank.active_fork().last_id(),
                0,
            )],
        );

        let entry_2_to_3_mint_to_1 = next_entry(
            &entry_1_to_mint.id,
            1,
            vec![
                SystemTransaction::new_account(
                    &keypair2,
                    keypair3.pubkey(),
                    2,
                    bank.active_fork().last_id(),
                    0,
                ), // should be fine
                SystemTransaction::new_account(
                    &keypair1,
                    mint_keypair.pubkey(),
                    2,
                    bank.active_fork().last_id(),
                    0,
                ), // will collide
            ],
        );

        assert_eq!(
            bank.active_fork()
                .process_entries(&[entry_1_to_mint, entry_2_to_3_mint_to_1]),
            Ok(())
        );

        assert_eq!(bank.active_fork().get_balance_slow(&keypair1.pubkey()), 1);
        assert_eq!(bank.active_fork().get_balance_slow(&keypair2.pubkey()), 2);
        assert_eq!(bank.active_fork().get_balance_slow(&keypair3.pubkey()), 2);
    }
    #[test]
    fn test_par_process_entries_2_entries_par() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(1000);
        let bank = Bank::new(&genesis_block);
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let keypair3 = Keypair::new();
        let keypair4 = Keypair::new();

        //load accounts
        let tx = SystemTransaction::new_account(
            &mint_keypair,
            keypair1.pubkey(),
            1,
            bank.active_fork().last_id(),
            0,
        );
        assert_eq!(bank.process_transaction(&tx), Ok(()));
        let tx = SystemTransaction::new_account(
            &mint_keypair,
            keypair2.pubkey(),
            1,
            bank.active_fork().last_id(),
            0,
        );
        assert_eq!(bank.process_transaction(&tx), Ok(()));

        // ensure bank can process 2 entries that do not have a common account and no tick is registered
        let last_id = bank.active_fork().last_id();
        let tx = SystemTransaction::new_account(
            &keypair1,
            keypair3.pubkey(),
            1,
            bank.active_fork().last_id(),
            0,
        );
        let entry_1 = next_entry(&last_id, 1, vec![tx]);
        let tx = SystemTransaction::new_account(
            &keypair2,
            keypair4.pubkey(),
            1,
            bank.active_fork().last_id(),
            0,
        );
        let entry_2 = next_entry(&entry_1.id, 1, vec![tx]);
        assert_eq!(
            bank.active_fork().process_entries(&[entry_1, entry_2]),
            Ok(())
        );
        assert_eq!(bank.active_fork().get_balance_slow(&keypair3.pubkey()), 1);
        assert_eq!(bank.active_fork().get_balance_slow(&keypair4.pubkey()), 1);
        assert_eq!(bank.active_fork().last_id(), last_id);
    }
    #[test]
    fn test_par_process_entries_2_entries_tick() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(1000);
        let bank = Bank::new(&genesis_block);
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let keypair3 = Keypair::new();
        let keypair4 = Keypair::new();

        //load accounts
        let tx = SystemTransaction::new_account(
            &mint_keypair,
            keypair1.pubkey(),
            1,
            bank.active_fork().last_id(),
            0,
        );
        assert_eq!(bank.process_transaction(&tx), Ok(()));
        let tx = SystemTransaction::new_account(
            &mint_keypair,
            keypair2.pubkey(),
            1,
            bank.active_fork().last_id(),
            0,
        );
        assert_eq!(bank.process_transaction(&tx), Ok(()));

        let last_id = bank.active_fork().last_id();

        // ensure bank can process 2 entries that do not have a common account and tick is registered
        let tx = SystemTransaction::new_account(
            &keypair2,
            keypair3.pubkey(),
            1,
            bank.active_fork().last_id(),
            0,
        );
        let entry_1 = next_entry(&last_id, 1, vec![tx]);
        let tick = next_entry(&entry_1.id, 1, vec![]);
        let tx = SystemTransaction::new_account(&keypair1, keypair4.pubkey(), 1, tick.id, 0);
        let entry_2 = next_entry(&tick.id, 1, vec![tx]);
        assert_eq!(
            bank.active_fork()
                .process_entries(&[entry_1.clone(), tick.clone(), entry_2.clone()]),
            Ok(())
        );
        assert_eq!(bank.active_fork().get_balance_slow(&keypair3.pubkey()), 1);
        assert_eq!(bank.active_fork().get_balance_slow(&keypair4.pubkey()), 1);
        assert_eq!(bank.active_fork().last_id(), tick.id);
        // ensure that an error is returned for an empty account (keypair2)
        let tx = SystemTransaction::new_account(&keypair2, keypair3.pubkey(), 1, tick.id, 0);
        let entry_3 = next_entry(&entry_2.id, 1, vec![tx]);
        assert_eq!(
            bank.active_fork().process_entries(&[entry_3]),
            Err(BankError::AccountNotFound)
        );
    }

    #[test]
    fn test_program_ids() {
        let system = Pubkey::new(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ]);
        let native = Pubkey::new(&[
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ]);
        let bpf = Pubkey::new(&[
            128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
        ]);
        let budget = Pubkey::new(&[
            129, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
        ]);
        let storage = Pubkey::new(&[
            130, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
        ]);
        let token = Pubkey::new(&[
            131, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
        ]);
        let vote = Pubkey::new(&[
            132, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
        ]);
        let storage_system = Pubkey::new(&[
            133, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
        ]);

        assert_eq!(system_program::id(), system);
        assert_eq!(solana_native_loader::id(), native);
        assert_eq!(bpf_loader::id(), bpf);
        assert_eq!(budget_program::id(), budget);
        assert_eq!(storage_program::id(), storage);
        assert_eq!(token_program::id(), token);
        assert_eq!(vote_program::id(), vote);
        assert_eq!(storage_program::system_id(), storage_system);
    }

    #[test]
    fn test_program_id_uniqueness() {
        let mut unique = HashSet::new();
        let ids = vec![
            system_program::id(),
            solana_native_loader::id(),
            bpf_loader::id(),
            budget_program::id(),
            storage_program::id(),
            token_program::id(),
            vote_program::id(),
            storage_program::system_id(),
        ];
        assert!(ids.into_iter().all(move |id| unique.insert(id)));
    }

    #[test]
    fn test_bank_record_transactions() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(10_000);
        let bank = Arc::new(Bank::new(&genesis_block));
        let (entry_sender, entry_receiver) = channel();
        let poh_recorder = PohRecorder::new(
            bank.clone(),
            entry_sender,
            bank.active_fork().last_id(),
            None,
        );
        let pubkey = Keypair::new().pubkey();

        let transactions = vec![
            SystemTransaction::new_move(&mint_keypair, pubkey, 1, genesis_block.last_id(), 0),
            SystemTransaction::new_move(&mint_keypair, pubkey, 1, genesis_block.last_id(), 0),
        ];

        let mut results = vec![Ok(()), Ok(())];
        BankFork::record_transactions(&transactions, &results, &poh_recorder).unwrap();
        let entries = entry_receiver.recv().unwrap();
        assert_eq!(entries[0].transactions.len(), transactions.len());

        // ProgramErrors should still be recorded
        results[0] = Err(BankError::ProgramError(
            1,
            ProgramError::ResultWithNegativeTokens,
        ));
        BankFork::record_transactions(&transactions, &results, &poh_recorder).unwrap();
        let entries = entry_receiver.recv().unwrap();
        assert_eq!(entries[0].transactions.len(), transactions.len());

        // Other BankErrors should not be recorded
        results[0] = Err(BankError::AccountNotFound);
        BankFork::record_transactions(&transactions, &results, &poh_recorder).unwrap();
        let entries = entry_receiver.recv().unwrap();
        assert_eq!(entries[0].transactions.len(), transactions.len() - 1);
    }

    #[test]
    fn test_bank_storage() {
        solana_logger::setup();
        let (genesis_block, alice) = GenesisBlock::new(1000);
        let bank = Bank::new(&genesis_block);

        let bob = Keypair::new();
        let jack = Keypair::new();
        let jill = Keypair::new();

        let x = 42;
        let last_id = hash(&[x]);
        let x2 = x * 2;
        let storage_last_id = hash(&[x2]);

        bank.active_fork().register_tick(&last_id);

        bank.transfer(10, &alice, jill.pubkey(), last_id).unwrap();

        bank.transfer(10, &alice, bob.pubkey(), last_id).unwrap();
        bank.transfer(10, &alice, jack.pubkey(), last_id).unwrap();

        let tx = StorageTransaction::new_advertise_last_id(
            &bob,
            storage_last_id,
            last_id,
            ENTRIES_PER_SEGMENT,
        );

        bank.process_transaction(&tx).unwrap();

        let entry_height = 0;

        let tx = StorageTransaction::new_mining_proof(
            &jack,
            Hash::default(),
            last_id,
            entry_height,
            Signature::default(),
        );

        bank.process_transaction(&tx).unwrap();

        assert_eq!(bank.get_storage_entry_height(), ENTRIES_PER_SEGMENT);
        assert_eq!(bank.get_storage_last_id(), storage_last_id);
    }

    #[test]
    fn test_bank_process_and_record_transactions() {
        let (genesis_block, mint_keypair) = GenesisBlock::new(10_000);
        let bank = Arc::new(Bank::new(&genesis_block));
        let pubkey = Keypair::new().pubkey();

        let transactions = vec![SystemTransaction::new_move(
            &mint_keypair,
            pubkey,
            1,
            genesis_block.last_id(),
            0,
        )];

        let (entry_sender, entry_receiver) = channel();
        let mut poh_recorder = PohRecorder::new(
            bank.clone(),
            entry_sender,
            bank.active_fork().last_id(),
            Some(bank.active_fork().tick_height() + 1),
        );

        bank.process_and_record_transactions(&transactions, Some(&poh_recorder))
            .unwrap();
        poh_recorder.tick().unwrap();

        let mut need_tick = true;
        // read entries until I find mine, might be ticks...
        while need_tick {
            let entries = entry_receiver.recv().unwrap();
            for entry in entries {
                if !entry.is_tick() {
                    assert_eq!(entry.transactions.len(), transactions.len());
                    assert_eq!(bank.active_fork().get_balance_slow(&pubkey), 1);
                } else {
                    need_tick = false;
                }
            }
        }

        let transactions = vec![SystemTransaction::new_move(
            &mint_keypair,
            pubkey,
            2,
            genesis_block.last_id(),
            0,
        )];

        assert_eq!(
            bank.process_and_record_transactions(&transactions, Some(&poh_recorder)),
            Err(BankError::MaxHeightReached)
        );

        assert_eq!(bank.active_fork().get_balance_slow(&pubkey), 1);
    }
}
