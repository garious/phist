//! The `rpc` module implements the Solana RPC interface.

use {
    crate::{
        max_slots::MaxSlots,
        optimistically_confirmed_bank_tracker::OptimisticallyConfirmedBank,
        parsed_token_accounts::*,
        rpc_health::*,
        send_transaction_service::{SendTransactionService, TransactionInfo},
    },
    bincode::{config::Options, serialize},
    jsonrpc_core::{futures::future, types::error, BoxFuture, Error, Metadata, Result},
    jsonrpc_derive::rpc,
    serde::{Deserialize, Serialize},
    solana_account_decoder::{
        parse_token::{spl_token_id_v2_0, token_amount_to_ui_amount, UiTokenAmount},
        UiAccount, UiAccountEncoding, UiDataSliceConfig,
    },
    solana_client::{
        rpc_cache::LargestAccountsCache,
        rpc_config::*,
        rpc_custom_error::RpcCustomError,
        rpc_deprecated_config::*,
        rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
        rpc_request::{
            TokenAccountsFilter, DELINQUENT_VALIDATOR_SLOT_DISTANCE,
            MAX_GET_CONFIRMED_BLOCKS_RANGE, MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT,
            MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS_SLOT_RANGE, MAX_GET_PROGRAM_ACCOUNT_FILTERS,
            MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS, MAX_GET_SLOT_LEADERS, MAX_MULTIPLE_ACCOUNTS,
            NUM_LARGEST_ACCOUNTS,
        },
        rpc_response::Response as RpcResponse,
        rpc_response::*,
    },
    solana_faucet::faucet::request_airdrop_transaction,
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
    solana_ledger::{
        blockstore::Blockstore, blockstore_db::BlockstoreError, get_tmp_ledger_path,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_metrics::inc_new_counter_info,
    solana_perf::packet::PACKET_DATA_SIZE,
    solana_runtime::{
        accounts::AccountAddressFilter,
        accounts_index::{AccountIndex, AccountSecondaryIndexes, IndexKey},
        bank::{Bank, TransactionSimulationResult},
        bank_forks::BankForks,
        commitment::{BlockCommitmentArray, BlockCommitmentCache, CommitmentSlots},
        inline_spl_token_v2_0::{SPL_TOKEN_ACCOUNT_MINT_OFFSET, SPL_TOKEN_ACCOUNT_OWNER_OFFSET},
        non_circulating_supply::calculate_non_circulating_supply,
        snapshot_config::SnapshotConfig,
        snapshot_utils,
    },
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        account_utils::StateMut,
        clock::{Slot, UnixTimestamp, MAX_RECENT_BLOCKHASHES},
        commitment_config::{CommitmentConfig, CommitmentLevel},
        epoch_info::EpochInfo,
        epoch_schedule::EpochSchedule,
        exit::Exit,
        hash::Hash,
        pubkey::Pubkey,
        sanitize::Sanitize,
        signature::{Keypair, Signature, Signer},
        stake::state::StakeState,
        stake_history::StakeHistory,
        system_instruction,
        sysvar::stake_history,
        transaction::{self, Transaction, TransactionError},
    },
    solana_streamer::socket::SocketAddrSpace,
    solana_transaction_status::{
        EncodedConfirmedTransaction, Reward, RewardType, TransactionConfirmationStatus,
        TransactionStatus, UiConfirmedBlock, UiTransactionEncoding,
    },
    solana_vote_program::vote_state::{VoteState, MAX_LOCKOUT_HISTORY},
    spl_token_v2_0::{
        solana_program::program_pack::Pack,
        state::{Account as TokenAccount, Mint},
    },
    std::{
        cmp::{max, min},
        collections::{HashMap, HashSet},
        net::SocketAddr,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            mpsc::{channel, Receiver, Sender},
            Arc, Mutex, RwLock,
        },
        time::Duration,
    },
};

type RpcCustomResult<T> = std::result::Result<T, RpcCustomError>;

pub const MAX_REQUEST_PAYLOAD_SIZE: usize = 50 * (1 << 10); // 50kB
pub const PERFORMANCE_SAMPLES_LIMIT: usize = 720;

// Limit the length of the `epoch_credits` array for each validator in a `get_vote_accounts`
// response
const MAX_RPC_EPOCH_CREDITS_HISTORY: usize = 5;

fn new_response<T>(bank: &Bank, value: T) -> RpcResponse<T> {
    let context = RpcResponseContext { slot: bank.slot() };
    Response { context, value }
}

/// Wrapper for rpc return types of methods that provide responses both with and without context.
/// Main purpose of this is to fix methods that lack context information in their return type,
/// without breaking backwards compatibility.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OptionalContext<T> {
    Context(RpcResponse<T>),
    NoContext(T),
}

fn is_finalized(
    block_commitment_cache: &BlockCommitmentCache,
    bank: &Bank,
    blockstore: &Blockstore,
    slot: Slot,
) -> bool {
    slot <= block_commitment_cache.highest_confirmed_root()
        && (blockstore.is_root(slot) || bank.status_cache_ancestors().contains(&slot))
}

#[derive(Debug, Default, Clone)]
pub struct JsonRpcConfig {
    pub enable_rpc_transaction_history: bool,
    pub enable_cpi_and_log_storage: bool,
    pub faucet_addr: Option<SocketAddr>,
    pub health_check_slot_distance: u64,
    pub enable_bigtable_ledger_storage: bool,
    pub enable_bigtable_ledger_upload: bool,
    pub max_multiple_accounts: Option<usize>,
    pub account_indexes: AccountSecondaryIndexes,
    pub rpc_threads: usize,
    pub rpc_bigtable_timeout: Option<Duration>,
    pub minimal_api: bool,
    pub obsolete_v1_7_api: bool,
    pub rpc_scan_and_fix_roots: bool,
}

#[derive(Clone)]
pub struct JsonRpcRequestProcessor {
    bank_forks: Arc<RwLock<BankForks>>,
    block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    blockstore: Arc<Blockstore>,
    config: JsonRpcConfig,
    snapshot_config: Option<SnapshotConfig>,
    validator_exit: Arc<RwLock<Exit>>,
    health: Arc<RpcHealth>,
    cluster_info: Arc<ClusterInfo>,
    genesis_hash: Hash,
    transaction_sender: Arc<Mutex<Sender<TransactionInfo>>>,
    bigtable_ledger_storage: Option<solana_storage_bigtable::LedgerStorage>,
    optimistically_confirmed_bank: Arc<RwLock<OptimisticallyConfirmedBank>>,
    largest_accounts_cache: Arc<RwLock<LargestAccountsCache>>,
    max_slots: Arc<MaxSlots>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    max_complete_transaction_status_slot: Arc<AtomicU64>,
}
impl Metadata for JsonRpcRequestProcessor {}

impl JsonRpcRequestProcessor {
    #[allow(deprecated)]
    fn bank(&self, commitment: Option<CommitmentConfig>) -> Arc<Bank> {
        debug!("RPC commitment_config: {:?}", commitment);
        let r_bank_forks = self.bank_forks.read().unwrap();

        let commitment = commitment.unwrap_or_default();

        if commitment.is_confirmed() {
            let bank = self
                .optimistically_confirmed_bank
                .read()
                .unwrap()
                .bank
                .clone();
            debug!("RPC using optimistically confirmed slot: {:?}", bank.slot());
            return bank;
        }

        let slot = self
            .block_commitment_cache
            .read()
            .unwrap()
            .slot_with_commitment(commitment.commitment);

        match commitment.commitment {
            // Recent variant is deprecated
            CommitmentLevel::Recent | CommitmentLevel::Processed => {
                debug!("RPC using the heaviest slot: {:?}", slot);
            }
            // Root variant is deprecated
            CommitmentLevel::Root => {
                debug!("RPC using node root: {:?}", slot);
            }
            // Single variant is deprecated
            CommitmentLevel::Single => {
                debug!("RPC using confirmed slot: {:?}", slot);
            }
            // Max variant is deprecated
            CommitmentLevel::Max | CommitmentLevel::Finalized => {
                debug!("RPC using block: {:?}", slot);
            }
            CommitmentLevel::SingleGossip | CommitmentLevel::Confirmed => unreachable!(), // SingleGossip variant is deprecated
        };

        r_bank_forks.get(slot).cloned().unwrap_or_else(|| {
            // We log a warning instead of returning an error, because all known error cases
            // are due to known bugs that should be fixed instead.
            //
            // The slot may not be found as a result of a known bug in snapshot creation, where
            // the bank at the given slot was not included in the snapshot.
            // Also, it may occur after an old bank has been purged from BankForks and a new
            // BlockCommitmentCache has not yet arrived. To make this case impossible,
            // BlockCommitmentCache should hold an `Arc<Bank>` everywhere it currently holds
            // a slot.
            //
            // For more information, see https://github.com/solana-labs/solana/issues/11078
            warn!(
                "Bank with {:?} not found at slot: {:?}",
                commitment.commitment, slot
            );
            r_bank_forks.root_bank()
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: JsonRpcConfig,
        snapshot_config: Option<SnapshotConfig>,
        bank_forks: Arc<RwLock<BankForks>>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        blockstore: Arc<Blockstore>,
        validator_exit: Arc<RwLock<Exit>>,
        health: Arc<RpcHealth>,
        cluster_info: Arc<ClusterInfo>,
        genesis_hash: Hash,
        bigtable_ledger_storage: Option<solana_storage_bigtable::LedgerStorage>,
        optimistically_confirmed_bank: Arc<RwLock<OptimisticallyConfirmedBank>>,
        largest_accounts_cache: Arc<RwLock<LargestAccountsCache>>,
        max_slots: Arc<MaxSlots>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        max_complete_transaction_status_slot: Arc<AtomicU64>,
    ) -> (Self, Receiver<TransactionInfo>) {
        let (sender, receiver) = channel();
        (
            Self {
                config,
                snapshot_config,
                bank_forks,
                block_commitment_cache,
                blockstore,
                validator_exit,
                health,
                cluster_info,
                genesis_hash,
                transaction_sender: Arc::new(Mutex::new(sender)),
                bigtable_ledger_storage,
                optimistically_confirmed_bank,
                largest_accounts_cache,
                max_slots,
                leader_schedule_cache,
                max_complete_transaction_status_slot,
            },
            receiver,
        )
    }

    // Useful for unit testing
    pub fn new_from_bank(bank: &Arc<Bank>, socket_addr_space: SocketAddrSpace) -> Self {
        let genesis_hash = bank.hash();
        let bank_forks = Arc::new(RwLock::new(BankForks::new_from_banks(
            &[bank.clone()],
            bank.slot(),
        )));
        let blockstore = Arc::new(Blockstore::open(&get_tmp_ledger_path!()).unwrap());
        let exit = Arc::new(AtomicBool::new(false));
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::default(),
            Arc::new(Keypair::new()),
            socket_addr_space,
        ));
        let tpu_address = cluster_info.my_contact_info().tpu;
        let (sender, receiver) = channel();
        SendTransactionService::new(tpu_address, &bank_forks, None, receiver, 1000, 1);

        Self {
            config: JsonRpcConfig::default(),
            snapshot_config: None,
            bank_forks,
            block_commitment_cache: Arc::new(RwLock::new(BlockCommitmentCache::new(
                HashMap::new(),
                0,
                CommitmentSlots::new_from_slot(bank.slot()),
            ))),
            blockstore,
            validator_exit: create_validator_exit(&exit),
            health: Arc::new(RpcHealth::new(cluster_info.clone(), None, 0, exit.clone())),
            cluster_info,
            genesis_hash,
            transaction_sender: Arc::new(Mutex::new(sender)),
            bigtable_ledger_storage: None,
            optimistically_confirmed_bank: Arc::new(RwLock::new(OptimisticallyConfirmedBank {
                bank: bank.clone(),
            })),
            largest_accounts_cache: Arc::new(RwLock::new(LargestAccountsCache::new(30))),
            max_slots: Arc::new(MaxSlots::default()),
            leader_schedule_cache: Arc::new(LeaderScheduleCache::new_from_bank(bank)),
            max_complete_transaction_status_slot: Arc::new(AtomicU64::default()),
        }
    }

    pub fn get_account_info(
        &self,
        pubkey: &Pubkey,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<RpcResponse<Option<UiAccount>>> {
        let config = config.unwrap_or_default();
        let bank = self.bank(config.commitment);
        let encoding = config.encoding.unwrap_or(UiAccountEncoding::Binary);
        check_slice_and_encoding(&encoding, config.data_slice.is_some())?;

        let response = get_encoded_account(&bank, pubkey, encoding, config.data_slice)?;
        Ok(new_response(&bank, response))
    }

    pub fn get_multiple_accounts(
        &self,
        pubkeys: Vec<Pubkey>,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<RpcResponse<Vec<Option<UiAccount>>>> {
        let mut accounts: Vec<Option<UiAccount>> = vec![];

        let config = config.unwrap_or_default();
        let bank = self.bank(config.commitment);
        let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64);
        check_slice_and_encoding(&encoding, config.data_slice.is_some())?;

        for pubkey in pubkeys {
            let response_account =
                get_encoded_account(&bank, &pubkey, encoding, config.data_slice)?;
            accounts.push(response_account)
        }
        Ok(new_response(&bank, accounts))
    }

    pub fn get_minimum_balance_for_rent_exemption(
        &self,
        data_len: usize,
        commitment: Option<CommitmentConfig>,
    ) -> u64 {
        self.bank(commitment)
            .get_minimum_balance_for_rent_exemption(data_len)
    }

    pub fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        config: Option<RpcAccountInfoConfig>,
        filters: Vec<RpcFilterType>,
        with_context: bool,
    ) -> Result<OptionalContext<Vec<RpcKeyedAccount>>> {
        let config = config.unwrap_or_default();
        let bank = self.bank(config.commitment);
        let encoding = config.encoding.unwrap_or(UiAccountEncoding::Binary);
        let data_slice_config = config.data_slice;
        check_slice_and_encoding(&encoding, data_slice_config.is_some())?;
        let keyed_accounts = {
            if let Some(owner) = get_spl_token_owner_filter(program_id, &filters) {
                self.get_filtered_spl_token_accounts_by_owner(&bank, &owner, filters)?
            } else if let Some(mint) = get_spl_token_mint_filter(program_id, &filters) {
                self.get_filtered_spl_token_accounts_by_mint(&bank, &mint, filters)?
            } else {
                self.get_filtered_program_accounts(&bank, program_id, filters)?
            }
        };
        let result =
            if program_id == &spl_token_id_v2_0() && encoding == UiAccountEncoding::JsonParsed {
                get_parsed_token_accounts(bank.clone(), keyed_accounts.into_iter()).collect()
            } else {
                let mut encoded_accounts = vec![];
                for (pubkey, account) in keyed_accounts {
                    encoded_accounts.push(RpcKeyedAccount {
                        pubkey: pubkey.to_string(),
                        account: encode_account(&account, &pubkey, encoding, data_slice_config)?,
                    });
                }
                encoded_accounts
            };
        Ok(result).map(|result| match with_context {
            true => OptionalContext::Context(new_response(&bank, result)),
            false => OptionalContext::NoContext(result),
        })
    }

    pub async fn get_inflation_reward(
        &self,
        addresses: Vec<Pubkey>,
        config: Option<RpcEpochConfig>,
    ) -> Result<Vec<Option<RpcInflationReward>>> {
        let config = config.unwrap_or_default();
        let epoch_schedule = self.get_epoch_schedule();
        let first_available_block = self.get_first_available_block().await;
        let epoch = config.epoch.unwrap_or_else(|| {
            epoch_schedule
                .get_epoch(self.get_slot(config.commitment))
                .saturating_sub(1)
        });

        // Rewards for this epoch are found in the first confirmed block of the next epoch
        let first_slot_in_epoch = epoch_schedule.get_first_slot_in_epoch(epoch.saturating_add(1));
        if first_slot_in_epoch < first_available_block {
            if self.bigtable_ledger_storage.is_some() {
                return Err(RpcCustomError::LongTermStorageSlotSkipped {
                    slot: first_slot_in_epoch,
                }
                .into());
            } else {
                return Err(RpcCustomError::BlockCleanedUp {
                    slot: first_slot_in_epoch,
                    first_available_block,
                }
                .into());
            }
        }

        let first_confirmed_block_in_epoch = *self
            .get_blocks_with_limit(first_slot_in_epoch, 1, config.commitment)
            .await?
            .get(0)
            .ok_or(RpcCustomError::BlockNotAvailable {
                slot: first_slot_in_epoch,
            })?;

        let first_confirmed_block = if let Ok(Some(first_confirmed_block)) = self
            .get_block(
                first_confirmed_block_in_epoch,
                Some(RpcBlockConfig::rewards_with_commitment(config.commitment).into()),
            )
            .await
        {
            first_confirmed_block
        } else {
            return Err(RpcCustomError::BlockNotAvailable {
                slot: first_confirmed_block_in_epoch,
            }
            .into());
        };

        let addresses: Vec<String> = addresses
            .into_iter()
            .map(|pubkey| pubkey.to_string())
            .collect();

        let reward_hash: HashMap<String, Reward> = first_confirmed_block
            .rewards
            .unwrap_or_default()
            .into_iter()
            .filter_map(|reward| match reward.reward_type? {
                RewardType::Staking | RewardType::Voting => addresses
                    .contains(&reward.pubkey)
                    .then(|| (reward.clone().pubkey, reward)),
                _ => None,
            })
            .collect();

        let rewards = addresses
            .iter()
            .map(|address| {
                if let Some(reward) = reward_hash.get(address) {
                    return Some(RpcInflationReward {
                        epoch,
                        effective_slot: first_confirmed_block_in_epoch,
                        amount: reward.lamports.abs() as u64,
                        post_balance: reward.post_balance,
                        commission: reward.commission,
                    });
                }
                None
            })
            .collect();

        Ok(rewards)
    }

    pub fn get_inflation_governor(
        &self,
        commitment: Option<CommitmentConfig>,
    ) -> RpcInflationGovernor {
        self.bank(commitment).inflation().into()
    }

    pub fn get_inflation_rate(&self) -> RpcInflationRate {
        let bank = self.bank(None);
        let epoch = bank.epoch();
        let inflation = bank.inflation();
        let slot_in_year = bank.slot_in_year_for_inflation();

        RpcInflationRate {
            total: inflation.total(slot_in_year),
            validator: inflation.validator(slot_in_year),
            foundation: inflation.foundation(slot_in_year),
            epoch,
        }
    }

    pub fn get_epoch_schedule(&self) -> EpochSchedule {
        // Since epoch schedule data comes from the genesis config, any commitment level should be
        // fine
        let bank = self.bank(Some(CommitmentConfig::finalized()));
        *bank.epoch_schedule()
    }

    pub fn get_balance(
        &self,
        pubkey: &Pubkey,
        commitment: Option<CommitmentConfig>,
    ) -> RpcResponse<u64> {
        let bank = self.bank(commitment);
        new_response(&bank, bank.get_balance(pubkey))
    }

    fn get_recent_blockhash(
        &self,
        commitment: Option<CommitmentConfig>,
    ) -> RpcResponse<RpcBlockhashFeeCalculator> {
        let bank = self.bank(commitment);
        let (blockhash, fee_calculator) = bank.confirmed_last_blockhash();
        new_response(
            &bank,
            RpcBlockhashFeeCalculator {
                blockhash: blockhash.to_string(),
                fee_calculator,
            },
        )
    }

    fn get_fees(&self, commitment: Option<CommitmentConfig>) -> RpcResponse<RpcFees> {
        let bank = self.bank(commitment);
        let (blockhash, fee_calculator) = bank.confirmed_last_blockhash();
        let last_valid_slot = bank
            .get_blockhash_last_valid_slot(&blockhash)
            .expect("bank blockhash queue should contain blockhash");
        let last_valid_block_height = bank
            .get_blockhash_last_valid_block_height(&blockhash)
            .expect("bank blockhash queue should contain blockhash");
        new_response(
            &bank,
            RpcFees {
                blockhash: blockhash.to_string(),
                fee_calculator,
                last_valid_slot,
                last_valid_block_height,
            },
        )
    }

    fn get_fee_calculator_for_blockhash(
        &self,
        blockhash: &Hash,
        commitment: Option<CommitmentConfig>,
    ) -> RpcResponse<Option<RpcFeeCalculator>> {
        let bank = self.bank(commitment);
        let fee_calculator = bank.get_fee_calculator(blockhash);
        new_response(
            &bank,
            fee_calculator.map(|fee_calculator| RpcFeeCalculator { fee_calculator }),
        )
    }

    fn get_fee_rate_governor(&self) -> RpcResponse<RpcFeeRateGovernor> {
        let bank = self.bank(None);
        let fee_rate_governor = bank.get_fee_rate_governor();
        new_response(
            &bank,
            RpcFeeRateGovernor {
                fee_rate_governor: fee_rate_governor.clone(),
            },
        )
    }

    pub fn confirm_transaction(
        &self,
        signature: &Signature,
        commitment: Option<CommitmentConfig>,
    ) -> RpcResponse<bool> {
        let bank = self.bank(commitment);
        let status = bank.get_signature_status(signature);
        match status {
            Some(status) => new_response(&bank, status.is_ok()),
            None => new_response(&bank, false),
        }
    }

    fn get_block_commitment(&self, block: Slot) -> RpcBlockCommitment<BlockCommitmentArray> {
        let r_block_commitment = self.block_commitment_cache.read().unwrap();
        RpcBlockCommitment {
            commitment: r_block_commitment
                .get_block_commitment(block)
                .map(|block_commitment| block_commitment.commitment),
            total_stake: r_block_commitment.total_stake(),
        }
    }

    fn get_slot(&self, commitment: Option<CommitmentConfig>) -> Slot {
        self.bank(commitment).slot()
    }

    fn get_block_height(&self, commitment: Option<CommitmentConfig>) -> u64 {
        self.bank(commitment).block_height()
    }

    fn get_max_retransmit_slot(&self) -> Slot {
        self.max_slots.retransmit.load(Ordering::Relaxed)
    }

    fn get_max_shred_insert_slot(&self) -> Slot {
        self.max_slots.shred_insert.load(Ordering::Relaxed)
    }

    fn get_slot_leader(&self, commitment: Option<CommitmentConfig>) -> String {
        self.bank(commitment).collector_id().to_string()
    }

    fn get_slot_leaders(
        &self,
        commitment: Option<CommitmentConfig>,
        start_slot: Slot,
        limit: usize,
    ) -> Result<Vec<Pubkey>> {
        let bank = self.bank(commitment);

        let (mut epoch, mut slot_index) =
            bank.epoch_schedule().get_epoch_and_slot_index(start_slot);

        let mut slot_leaders = Vec::with_capacity(limit);
        while slot_leaders.len() < limit {
            if let Some(leader_schedule) =
                self.leader_schedule_cache.get_epoch_leader_schedule(epoch)
            {
                slot_leaders.extend(
                    leader_schedule
                        .get_slot_leaders()
                        .iter()
                        .skip(slot_index as usize)
                        .take(limit.saturating_sub(slot_leaders.len())),
                );
            } else {
                return Err(Error::invalid_params(format!(
                    "Invalid slot range: leader schedule for epoch {} is unavailable",
                    epoch
                )));
            }

            epoch += 1;
            slot_index = 0;
        }

        Ok(slot_leaders)
    }

    fn minimum_ledger_slot(&self) -> Result<Slot> {
        match self.blockstore.slot_meta_iterator(0) {
            Ok(mut metas) => match metas.next() {
                Some((slot, _meta)) => Ok(slot),
                None => Err(Error::invalid_request()),
            },
            Err(err) => {
                warn!("slot_meta_iterator failed: {:?}", err);
                Err(Error::invalid_request())
            }
        }
    }

    fn get_transaction_count(&self, commitment: Option<CommitmentConfig>) -> u64 {
        self.bank(commitment).transaction_count() as u64
    }

    fn get_total_supply(&self, commitment: Option<CommitmentConfig>) -> u64 {
        self.bank(commitment).capitalization()
    }

    fn get_cached_largest_accounts(
        &self,
        filter: &Option<RpcLargestAccountsFilter>,
    ) -> Option<(u64, Vec<RpcAccountBalance>)> {
        let largest_accounts_cache = self.largest_accounts_cache.read().unwrap();
        largest_accounts_cache.get_largest_accounts(filter)
    }

    fn set_cached_largest_accounts(
        &self,
        filter: &Option<RpcLargestAccountsFilter>,
        slot: u64,
        accounts: &[RpcAccountBalance],
    ) {
        let mut largest_accounts_cache = self.largest_accounts_cache.write().unwrap();
        largest_accounts_cache.set_largest_accounts(filter, slot, accounts)
    }

    fn get_largest_accounts(
        &self,
        config: Option<RpcLargestAccountsConfig>,
    ) -> RpcCustomResult<RpcResponse<Vec<RpcAccountBalance>>> {
        let config = config.unwrap_or_default();
        let bank = self.bank(config.commitment);

        if let Some((slot, accounts)) = self.get_cached_largest_accounts(&config.filter) {
            Ok(Response {
                context: RpcResponseContext { slot },
                value: accounts,
            })
        } else {
            let (addresses, address_filter) = if let Some(filter) = config.clone().filter {
                let non_circulating_supply =
                    calculate_non_circulating_supply(&bank).map_err(|e| {
                        RpcCustomError::ScanError {
                            message: e.to_string(),
                        }
                    })?;
                let addresses = non_circulating_supply.accounts.into_iter().collect();
                let address_filter = match filter {
                    RpcLargestAccountsFilter::Circulating => AccountAddressFilter::Exclude,
                    RpcLargestAccountsFilter::NonCirculating => AccountAddressFilter::Include,
                };
                (addresses, address_filter)
            } else {
                (HashSet::new(), AccountAddressFilter::Exclude)
            };
            let accounts = bank
                .get_largest_accounts(NUM_LARGEST_ACCOUNTS, &addresses, address_filter)
                .map_err(|e| RpcCustomError::ScanError {
                    message: e.to_string(),
                })?
                .into_iter()
                .map(|(address, lamports)| RpcAccountBalance {
                    address: address.to_string(),
                    lamports,
                })
                .collect::<Vec<RpcAccountBalance>>();

            self.set_cached_largest_accounts(&config.filter, bank.slot(), &accounts);
            Ok(new_response(&bank, accounts))
        }
    }

    fn get_supply(
        &self,
        commitment: Option<CommitmentConfig>,
    ) -> RpcCustomResult<RpcResponse<RpcSupply>> {
        let bank = self.bank(commitment);
        let non_circulating_supply =
            calculate_non_circulating_supply(&bank).map_err(|e| RpcCustomError::ScanError {
                message: e.to_string(),
            })?;
        let total_supply = bank.capitalization();
        Ok(new_response(
            &bank,
            RpcSupply {
                total: total_supply,
                circulating: total_supply - non_circulating_supply.lamports,
                non_circulating: non_circulating_supply.lamports,
                non_circulating_accounts: non_circulating_supply
                    .accounts
                    .iter()
                    .map(|pubkey| pubkey.to_string())
                    .collect(),
            },
        ))
    }

    fn get_vote_accounts(
        &self,
        config: Option<RpcGetVoteAccountsConfig>,
    ) -> Result<RpcVoteAccountStatus> {
        let config = config.unwrap_or_default();

        let filter_by_vote_pubkey = if let Some(ref vote_pubkey) = config.vote_pubkey {
            Some(verify_pubkey(vote_pubkey)?)
        } else {
            None
        };

        let bank = self.bank(config.commitment);
        let vote_accounts = bank.vote_accounts();
        let epoch_vote_accounts = bank
            .epoch_vote_accounts(bank.get_epoch_and_slot_index(bank.slot()).0)
            .ok_or_else(Error::invalid_request)?;
        let default_vote_state = VoteState::default();
        let delinquent_validator_slot_distance = config
            .delinquent_slot_distance
            .unwrap_or(DELINQUENT_VALIDATOR_SLOT_DISTANCE);
        let (current_vote_accounts, delinquent_vote_accounts): (
            Vec<RpcVoteAccountInfo>,
            Vec<RpcVoteAccountInfo>,
        ) = vote_accounts
            .iter()
            .filter_map(|(vote_pubkey, (activated_stake, account))| {
                if let Some(filter_by_vote_pubkey) = filter_by_vote_pubkey {
                    if *vote_pubkey != filter_by_vote_pubkey {
                        return None;
                    }
                }

                let vote_state = account.vote_state();
                let vote_state = vote_state.as_ref().unwrap_or(&default_vote_state);
                let last_vote = if let Some(vote) = vote_state.votes.iter().last() {
                    vote.slot
                } else {
                    0
                };

                let epoch_credits = vote_state.epoch_credits();
                let epoch_credits = if epoch_credits.len() > MAX_RPC_EPOCH_CREDITS_HISTORY {
                    epoch_credits
                        .iter()
                        .skip(epoch_credits.len() - MAX_RPC_EPOCH_CREDITS_HISTORY)
                        .cloned()
                        .collect()
                } else {
                    epoch_credits.clone()
                };

                Some(RpcVoteAccountInfo {
                    vote_pubkey: vote_pubkey.to_string(),
                    node_pubkey: vote_state.node_pubkey.to_string(),
                    activated_stake: *activated_stake,
                    commission: vote_state.commission,
                    root_slot: vote_state.root_slot.unwrap_or(0),
                    epoch_credits,
                    epoch_vote_account: epoch_vote_accounts.contains_key(vote_pubkey),
                    last_vote,
                })
            })
            .partition(|vote_account_info| {
                if bank.slot() >= delinquent_validator_slot_distance as u64 {
                    vote_account_info.last_vote
                        > bank.slot() - delinquent_validator_slot_distance as u64
                } else {
                    vote_account_info.last_vote > 0
                }
            });

        let keep_unstaked_delinquents = config.keep_unstaked_delinquents.unwrap_or_default();
        let delinquent_vote_accounts = if !keep_unstaked_delinquents {
            delinquent_vote_accounts
                .into_iter()
                .filter(|vote_account_info| vote_account_info.activated_stake > 0)
                .collect::<Vec<_>>()
        } else {
            delinquent_vote_accounts
        };

        Ok(RpcVoteAccountStatus {
            current: current_vote_accounts,
            delinquent: delinquent_vote_accounts,
        })
    }

    fn check_blockstore_root<T>(
        &self,
        result: &std::result::Result<T, BlockstoreError>,
        slot: Slot,
    ) -> Result<()>
    where
        T: std::fmt::Debug,
    {
        if result.is_err() {
            let err = result.as_ref().unwrap_err();
            debug!(
                "check_blockstore_root, slot: {:?}, max root: {:?}, err: {:?}",
                slot,
                self.blockstore.max_root(),
                err
            );
            if slot >= self.blockstore.max_root() {
                return Err(RpcCustomError::BlockNotAvailable { slot }.into());
            }
            if self.blockstore.is_skipped(slot) {
                return Err(RpcCustomError::SlotSkipped { slot }.into());
            }
        }
        Ok(())
    }

    fn check_slot_cleaned_up<T>(
        &self,
        result: &std::result::Result<T, BlockstoreError>,
        slot: Slot,
    ) -> Result<()>
    where
        T: std::fmt::Debug,
    {
        if result.is_err() {
            if let BlockstoreError::SlotCleanedUp = result.as_ref().unwrap_err() {
                return Err(RpcCustomError::BlockCleanedUp {
                    slot,
                    first_available_block: self
                        .blockstore
                        .get_first_available_block()
                        .unwrap_or_default(),
                }
                .into());
            }
        }
        Ok(())
    }

    fn check_bigtable_result<T>(
        &self,
        result: &std::result::Result<T, solana_storage_bigtable::Error>,
    ) -> Result<()>
    where
        T: std::fmt::Debug,
    {
        if result.is_err() {
            let err = result.as_ref().unwrap_err();
            if let solana_storage_bigtable::Error::BlockNotFound(slot) = err {
                return Err(RpcCustomError::LongTermStorageSlotSkipped { slot: *slot }.into());
            }
        }
        Ok(())
    }

    pub async fn get_block(
        &self,
        slot: Slot,
        config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
    ) -> Result<Option<UiConfirmedBlock>> {
        if self.config.enable_rpc_transaction_history {
            let config = config
                .map(|config| config.convert_to_current())
                .unwrap_or_default();
            let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
            let transaction_details = config.transaction_details.unwrap_or_default();
            let show_rewards = config.rewards.unwrap_or(true);
            let commitment = config.commitment.unwrap_or_default();
            check_is_at_least_confirmed(commitment)?;

            // Block is old enough to be finalized
            if slot
                <= self
                    .block_commitment_cache
                    .read()
                    .unwrap()
                    .highest_confirmed_root()
            {
                let result = self.blockstore.get_rooted_block(slot, true);
                self.check_blockstore_root(&result, slot)?;
                if result.is_err() {
                    if let Some(bigtable_ledger_storage) = &self.bigtable_ledger_storage {
                        let bigtable_result =
                            bigtable_ledger_storage.get_confirmed_block(slot).await;
                        self.check_bigtable_result(&bigtable_result)?;
                        return Ok(bigtable_result.ok().map(|confirmed_block| {
                            confirmed_block.configure(encoding, transaction_details, show_rewards)
                        }));
                    }
                }
                self.check_slot_cleaned_up(&result, slot)?;
                return Ok(result.ok().map(|confirmed_block| {
                    confirmed_block.configure(encoding, transaction_details, show_rewards)
                }));
            } else if commitment.is_confirmed() {
                // Check if block is confirmed
                let confirmed_bank = self.bank(Some(CommitmentConfig::confirmed()));
                if confirmed_bank.status_cache_ancestors().contains(&slot)
                    && slot
                        <= self
                            .max_complete_transaction_status_slot
                            .load(Ordering::SeqCst)
                {
                    let result = self.blockstore.get_complete_block(slot, true);
                    return Ok(result.ok().map(|mut confirmed_block| {
                        if confirmed_block.block_time.is_none()
                            || confirmed_block.block_height.is_none()
                        {
                            let r_bank_forks = self.bank_forks.read().unwrap();
                            let bank = r_bank_forks.get(slot).cloned();
                            if let Some(bank) = bank {
                                if confirmed_block.block_time.is_none() {
                                    confirmed_block.block_time = Some(bank.clock().unix_timestamp);
                                }
                                if confirmed_block.block_height.is_none() {
                                    confirmed_block.block_height = Some(bank.block_height());
                                }
                            }
                        }
                        confirmed_block.configure(encoding, transaction_details, show_rewards)
                    }));
                }
            }
        } else {
            return Err(RpcCustomError::TransactionHistoryNotAvailable.into());
        }
        Err(RpcCustomError::BlockNotAvailable { slot }.into())
    }

    pub async fn get_blocks(
        &self,
        start_slot: Slot,
        end_slot: Option<Slot>,
        commitment: Option<CommitmentConfig>,
    ) -> Result<Vec<Slot>> {
        let commitment = commitment.unwrap_or_default();
        check_is_at_least_confirmed(commitment)?;

        let highest_confirmed_root = self
            .block_commitment_cache
            .read()
            .unwrap()
            .highest_confirmed_root();

        let end_slot = min(
            end_slot.unwrap_or_else(|| start_slot.saturating_add(MAX_GET_CONFIRMED_BLOCKS_RANGE)),
            if commitment.is_finalized() {
                highest_confirmed_root
            } else {
                self.bank(Some(CommitmentConfig::confirmed())).slot()
            },
        );
        if end_slot < start_slot {
            return Ok(vec![]);
        }
        if end_slot - start_slot > MAX_GET_CONFIRMED_BLOCKS_RANGE {
            return Err(Error::invalid_params(format!(
                "Slot range too large; max {}",
                MAX_GET_CONFIRMED_BLOCKS_RANGE
            )));
        }

        let lowest_blockstore_slot = self.blockstore.lowest_slot();
        if start_slot < lowest_blockstore_slot {
            // If the starting slot is lower than what's available in blockstore assume the entire
            // [start_slot..end_slot] can be fetched from BigTable. This range should not ever run
            // into unfinalized confirmed blocks due to MAX_GET_CONFIRMED_BLOCKS_RANGE
            if let Some(bigtable_ledger_storage) = &self.bigtable_ledger_storage {
                return bigtable_ledger_storage
                    .get_confirmed_blocks(start_slot, (end_slot - start_slot) as usize + 1) // increment limit by 1 to ensure returned range is inclusive of both start_slot and end_slot
                    .await
                    .map(|mut bigtable_blocks| {
                        bigtable_blocks.retain(|&slot| slot <= end_slot);
                        bigtable_blocks
                    })
                    .map_err(|_| {
                        Error::invalid_params(
                            "BigTable query failed (maybe timeout due to too large range?)"
                                .to_string(),
                        )
                    });
            }
        }

        // Finalized blocks
        let mut blocks: Vec<_> = self
            .blockstore
            .rooted_slot_iterator(max(start_slot, lowest_blockstore_slot))
            .map_err(|_| Error::internal_error())?
            .filter(|&slot| slot <= end_slot && slot <= highest_confirmed_root)
            .collect();
        let last_element = blocks.last().cloned().unwrap_or_default();

        // Maybe add confirmed blocks
        if commitment.is_confirmed() && last_element < end_slot {
            let confirmed_bank = self.bank(Some(CommitmentConfig::confirmed()));
            let mut confirmed_blocks = confirmed_bank
                .status_cache_ancestors()
                .into_iter()
                .filter(|&slot| slot <= end_slot && slot > last_element)
                .collect();
            blocks.append(&mut confirmed_blocks);
        }

        Ok(blocks)
    }

    pub async fn get_blocks_with_limit(
        &self,
        start_slot: Slot,
        limit: usize,
        commitment: Option<CommitmentConfig>,
    ) -> Result<Vec<Slot>> {
        let commitment = commitment.unwrap_or_default();
        check_is_at_least_confirmed(commitment)?;

        if limit > MAX_GET_CONFIRMED_BLOCKS_RANGE as usize {
            return Err(Error::invalid_params(format!(
                "Limit too large; max {}",
                MAX_GET_CONFIRMED_BLOCKS_RANGE
            )));
        }

        let lowest_blockstore_slot = self.blockstore.lowest_slot();

        if start_slot < lowest_blockstore_slot {
            // If the starting slot is lower than what's available in blockstore assume the entire
            // range can be fetched from BigTable. This range should not ever run into unfinalized
            // confirmed blocks due to MAX_GET_CONFIRMED_BLOCKS_RANGE
            if let Some(bigtable_ledger_storage) = &self.bigtable_ledger_storage {
                return Ok(bigtable_ledger_storage
                    .get_confirmed_blocks(start_slot, limit)
                    .await
                    .unwrap_or_default());
            }
        }

        let highest_confirmed_root = self
            .block_commitment_cache
            .read()
            .unwrap()
            .highest_confirmed_root();

        // Finalized blocks
        let mut blocks: Vec<_> = self
            .blockstore
            .rooted_slot_iterator(max(start_slot, lowest_blockstore_slot))
            .map_err(|_| Error::internal_error())?
            .take(limit)
            .filter(|&slot| slot <= highest_confirmed_root)
            .collect();

        // Maybe add confirmed blocks
        if commitment.is_confirmed() && blocks.len() < limit {
            let last_element = blocks
                .last()
                .cloned()
                .unwrap_or_else(|| start_slot.saturating_sub(1));
            let confirmed_bank = self.bank(Some(CommitmentConfig::confirmed()));
            let mut confirmed_blocks = confirmed_bank
                .status_cache_ancestors()
                .into_iter()
                .filter(|&slot| slot > last_element)
                .collect();
            blocks.append(&mut confirmed_blocks);
            blocks.truncate(limit);
        }

        Ok(blocks)
    }

    pub async fn get_block_time(&self, slot: Slot) -> Result<Option<UnixTimestamp>> {
        if slot
            <= self
                .block_commitment_cache
                .read()
                .unwrap()
                .highest_confirmed_root()
        {
            let result = self.blockstore.get_block_time(slot);
            self.check_blockstore_root(&result, slot)?;
            if result.is_err() || matches!(result, Ok(None)) {
                if let Some(bigtable_ledger_storage) = &self.bigtable_ledger_storage {
                    let bigtable_result = bigtable_ledger_storage.get_confirmed_block(slot).await;
                    self.check_bigtable_result(&bigtable_result)?;
                    return Ok(bigtable_result
                        .ok()
                        .and_then(|confirmed_block| confirmed_block.block_time));
                }
            }
            self.check_slot_cleaned_up(&result, slot)?;
            Ok(result.ok().unwrap_or(None))
        } else {
            let r_bank_forks = self.bank_forks.read().unwrap();
            if let Some(bank) = r_bank_forks.get(slot) {
                Ok(Some(bank.clock().unix_timestamp))
            } else {
                Err(RpcCustomError::BlockNotAvailable { slot }.into())
            }
        }
    }

    pub fn get_signature_confirmation_status(
        &self,
        signature: Signature,
        commitment: Option<CommitmentConfig>,
    ) -> Option<RpcSignatureConfirmation> {
        let bank = self.bank(commitment);
        let transaction_status = self.get_transaction_status(signature, &bank)?;
        let confirmations = transaction_status
            .confirmations
            .unwrap_or(MAX_LOCKOUT_HISTORY + 1);
        Some(RpcSignatureConfirmation {
            confirmations,
            status: transaction_status.status,
        })
    }

    pub fn get_signature_status(
        &self,
        signature: Signature,
        commitment: Option<CommitmentConfig>,
    ) -> Option<transaction::Result<()>> {
        let bank = self.bank(commitment);
        let (_, status) = bank.get_signature_status_slot(&signature)?;
        Some(status)
    }

    pub async fn get_signature_statuses(
        &self,
        signatures: Vec<Signature>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> Result<RpcResponse<Vec<Option<TransactionStatus>>>> {
        let mut statuses: Vec<Option<TransactionStatus>> = vec![];

        let search_transaction_history = config
            .map(|x| x.search_transaction_history)
            .unwrap_or(false);
        let bank = self.bank(Some(CommitmentConfig::processed()));

        if search_transaction_history && !self.config.enable_rpc_transaction_history {
            return Err(RpcCustomError::TransactionHistoryNotAvailable.into());
        }

        for signature in signatures {
            let status = if let Some(status) = self.get_transaction_status(signature, &bank) {
                Some(status)
            } else if self.config.enable_rpc_transaction_history && search_transaction_history {
                if let Some(status) = self
                    .blockstore
                    .get_rooted_transaction_status(signature)
                    .map_err(|_| Error::internal_error())?
                    .filter(|(slot, _status_meta)| {
                        slot <= &self
                            .block_commitment_cache
                            .read()
                            .unwrap()
                            .highest_confirmed_root()
                    })
                    .map(|(slot, status_meta)| {
                        let err = status_meta.status.clone().err();
                        TransactionStatus {
                            slot,
                            status: status_meta.status,
                            confirmations: None,
                            err,
                            confirmation_status: Some(TransactionConfirmationStatus::Finalized),
                        }
                    })
                {
                    Some(status)
                } else if let Some(bigtable_ledger_storage) = &self.bigtable_ledger_storage {
                    bigtable_ledger_storage
                        .get_signature_status(&signature)
                        .await
                        .map(Some)
                        .unwrap_or(None)
                } else {
                    None
                }
            } else {
                None
            };
            statuses.push(status);
        }
        Ok(new_response(&bank, statuses))
    }

    fn get_transaction_status(
        &self,
        signature: Signature,
        bank: &Arc<Bank>,
    ) -> Option<TransactionStatus> {
        let (slot, status) = bank.get_signature_status_slot(&signature)?;
        let r_block_commitment_cache = self.block_commitment_cache.read().unwrap();

        let optimistically_confirmed_bank = self.bank(Some(CommitmentConfig::confirmed()));
        let optimistically_confirmed =
            optimistically_confirmed_bank.get_signature_status_slot(&signature);

        let confirmations = if r_block_commitment_cache.root() >= slot
            && is_finalized(&r_block_commitment_cache, bank, &self.blockstore, slot)
        {
            None
        } else {
            r_block_commitment_cache
                .get_confirmation_count(slot)
                .or(Some(0))
        };
        let err = status.clone().err();
        Some(TransactionStatus {
            slot,
            status,
            confirmations,
            err,
            confirmation_status: if confirmations.is_none() {
                Some(TransactionConfirmationStatus::Finalized)
            } else if optimistically_confirmed.is_some() {
                Some(TransactionConfirmationStatus::Confirmed)
            } else {
                Some(TransactionConfirmationStatus::Processed)
            },
        })
    }

    pub async fn get_transaction(
        &self,
        signature: Signature,
        config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
    ) -> Result<Option<EncodedConfirmedTransaction>> {
        let config = config
            .map(|config| config.convert_to_current())
            .unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
        let commitment = config.commitment.unwrap_or_default();
        check_is_at_least_confirmed(commitment)?;

        if self.config.enable_rpc_transaction_history {
            let confirmed_bank = self.bank(Some(CommitmentConfig::confirmed()));
            let transaction = if commitment.is_confirmed() {
                let highest_confirmed_slot = confirmed_bank.slot();
                self.blockstore
                    .get_complete_transaction(signature, highest_confirmed_slot)
            } else {
                self.blockstore.get_rooted_transaction(signature)
            };
            match transaction.unwrap_or(None) {
                Some(mut confirmed_transaction) => {
                    if commitment.is_confirmed()
                        && confirmed_bank // should be redundant
                            .status_cache_ancestors()
                            .contains(&confirmed_transaction.slot)
                    {
                        if confirmed_transaction.block_time.is_none() {
                            let r_bank_forks = self.bank_forks.read().unwrap();
                            confirmed_transaction.block_time = r_bank_forks
                                .get(confirmed_transaction.slot)
                                .map(|bank| bank.clock().unix_timestamp);
                        }
                        return Ok(Some(confirmed_transaction.encode(encoding)));
                    }
                    if confirmed_transaction.slot
                        <= self
                            .block_commitment_cache
                            .read()
                            .unwrap()
                            .highest_confirmed_root()
                    {
                        return Ok(Some(confirmed_transaction.encode(encoding)));
                    }
                }
                None => {
                    if let Some(bigtable_ledger_storage) = &self.bigtable_ledger_storage {
                        return Ok(bigtable_ledger_storage
                            .get_confirmed_transaction(&signature)
                            .await
                            .unwrap_or(None)
                            .map(|confirmed| confirmed.encode(encoding)));
                    }
                }
            }
        } else {
            return Err(RpcCustomError::TransactionHistoryNotAvailable.into());
        }
        Ok(None)
    }

    pub fn get_confirmed_signatures_for_address(
        &self,
        pubkey: Pubkey,
        start_slot: Slot,
        end_slot: Slot,
    ) -> Vec<Signature> {
        if self.config.enable_rpc_transaction_history {
            // TODO: Add bigtable_ledger_storage support as a part of
            // https://github.com/solana-labs/solana/pull/10928
            let end_slot = min(
                end_slot,
                self.block_commitment_cache
                    .read()
                    .unwrap()
                    .highest_confirmed_root(),
            );
            self.blockstore
                .get_confirmed_signatures_for_address(pubkey, start_slot, end_slot)
                .unwrap_or_default()
        } else {
            vec![]
        }
    }

    pub async fn get_signatures_for_address(
        &self,
        address: Pubkey,
        mut before: Option<Signature>,
        until: Option<Signature>,
        mut limit: usize,
        commitment: Option<CommitmentConfig>,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        let commitment = commitment.unwrap_or_default();
        check_is_at_least_confirmed(commitment)?;

        if self.config.enable_rpc_transaction_history {
            let highest_confirmed_root = self
                .block_commitment_cache
                .read()
                .unwrap()
                .highest_confirmed_root();
            let highest_slot = if commitment.is_confirmed() {
                let confirmed_bank = self.bank(Some(CommitmentConfig::confirmed()));
                confirmed_bank.slot()
            } else {
                highest_confirmed_root
            };

            let mut results = self
                .blockstore
                .get_confirmed_signatures_for_address2(address, highest_slot, before, until, limit)
                .map_err(|err| Error::invalid_params(format!("{}", err)))?;

            if results.len() < limit {
                if let Some(bigtable_ledger_storage) = &self.bigtable_ledger_storage {
                    if !results.is_empty() {
                        limit -= results.len();
                        before = results.last().map(|x| x.signature);
                    }

                    let bigtable_results = bigtable_ledger_storage
                        .get_confirmed_signatures_for_address(
                            &address,
                            before.as_ref(),
                            until.as_ref(),
                            limit,
                        )
                        .await;
                    match bigtable_results {
                        Ok(bigtable_results) => {
                            results.extend(bigtable_results.into_iter().map(|x| x.0));
                        }
                        Err(err) => {
                            warn!("{:?}", err);
                        }
                    }
                }
            }

            Ok(results
                .into_iter()
                .map(|x| {
                    let mut item: RpcConfirmedTransactionStatusWithSignature = x.into();
                    if item.slot <= highest_confirmed_root {
                        item.confirmation_status = Some(TransactionConfirmationStatus::Finalized);
                    } else {
                        item.confirmation_status = Some(TransactionConfirmationStatus::Confirmed);
                        if item.block_time.is_none() {
                            let r_bank_forks = self.bank_forks.read().unwrap();
                            item.block_time = r_bank_forks
                                .get(item.slot)
                                .map(|bank| bank.clock().unix_timestamp);
                        }
                    }
                    item
                })
                .collect())
        } else {
            Err(RpcCustomError::TransactionHistoryNotAvailable.into())
        }
    }

    pub async fn get_first_available_block(&self) -> Slot {
        let slot = self
            .blockstore
            .get_first_available_block()
            .unwrap_or_default();

        if let Some(bigtable_ledger_storage) = &self.bigtable_ledger_storage {
            let bigtable_slot = bigtable_ledger_storage
                .get_first_available_block()
                .await
                .unwrap_or(None)
                .unwrap_or(slot);

            if bigtable_slot < slot {
                return bigtable_slot;
            }
        }
        slot
    }

    pub fn get_stake_activation(
        &self,
        pubkey: &Pubkey,
        config: Option<RpcEpochConfig>,
    ) -> Result<RpcStakeActivation> {
        let config = config.unwrap_or_default();
        let bank = self.bank(config.commitment);
        let epoch = config.epoch.unwrap_or_else(|| bank.epoch());
        if bank.epoch().saturating_sub(epoch) > solana_sdk::stake_history::MAX_ENTRIES as u64 {
            return Err(Error::invalid_params(format!(
                "Invalid param: epoch {:?} is too far in the past",
                epoch
            )));
        }
        if epoch > bank.epoch() {
            return Err(Error::invalid_params(format!(
                "Invalid param: epoch {:?} has not yet started",
                epoch
            )));
        }

        let stake_account = bank
            .get_account(pubkey)
            .ok_or_else(|| Error::invalid_params("Invalid param: account not found".to_string()))?;
        let stake_state: StakeState = stake_account
            .state()
            .map_err(|_| Error::invalid_params("Invalid param: not a stake account".to_string()))?;
        let delegation = stake_state.delegation();
        if delegation.is_none() {
            match stake_state.meta() {
                None => {
                    return Err(Error::invalid_params(
                        "Invalid param: stake account not initialized".to_string(),
                    ));
                }
                Some(meta) => {
                    let rent_exempt_reserve = meta.rent_exempt_reserve;
                    return Ok(RpcStakeActivation {
                        state: StakeActivationState::Inactive,
                        active: 0,
                        inactive: stake_account.lamports().saturating_sub(rent_exempt_reserve),
                    });
                }
            }
        }
        let delegation = delegation.unwrap();

        let stake_history_account = bank
            .get_account(&stake_history::id())
            .ok_or_else(Error::internal_error)?;
        let stake_history =
            solana_sdk::account::from_account::<StakeHistory, _>(&stake_history_account)
                .ok_or_else(Error::internal_error)?;

        let (active, activating, deactivating) = delegation.stake_activating_and_deactivating(
            epoch,
            Some(&stake_history),
            bank.stake_program_v2_enabled(),
        );
        let stake_activation_state = if deactivating > 0 {
            StakeActivationState::Deactivating
        } else if activating > 0 {
            StakeActivationState::Activating
        } else if active > 0 {
            StakeActivationState::Active
        } else {
            StakeActivationState::Inactive
        };
        let inactive_stake = match stake_activation_state {
            StakeActivationState::Activating => activating,
            StakeActivationState::Active => 0,
            StakeActivationState::Deactivating => delegation.stake.saturating_sub(active),
            StakeActivationState::Inactive => delegation.stake,
        };
        Ok(RpcStakeActivation {
            state: stake_activation_state,
            active,
            inactive: inactive_stake,
        })
    }

    pub fn get_token_account_balance(
        &self,
        pubkey: &Pubkey,
        commitment: Option<CommitmentConfig>,
    ) -> Result<RpcResponse<UiTokenAmount>> {
        let bank = self.bank(commitment);
        let account = bank.get_account(pubkey).ok_or_else(|| {
            Error::invalid_params("Invalid param: could not find account".to_string())
        })?;

        if account.owner() != &spl_token_id_v2_0() {
            return Err(Error::invalid_params(
                "Invalid param: not a v2.0 Token account".to_string(),
            ));
        }
        let token_account = TokenAccount::unpack(account.data()).map_err(|_| {
            Error::invalid_params("Invalid param: not a v2.0 Token account".to_string())
        })?;
        let mint = &Pubkey::from_str(&token_account.mint.to_string())
            .expect("Token account mint should be convertible to Pubkey");
        let (_, decimals) = get_mint_owner_and_decimals(&bank, mint)?;
        let balance = token_amount_to_ui_amount(token_account.amount, decimals);
        Ok(new_response(&bank, balance))
    }

    pub fn get_token_supply(
        &self,
        mint: &Pubkey,
        commitment: Option<CommitmentConfig>,
    ) -> Result<RpcResponse<UiTokenAmount>> {
        let bank = self.bank(commitment);
        let mint_account = bank.get_account(mint).ok_or_else(|| {
            Error::invalid_params("Invalid param: could not find account".to_string())
        })?;
        if mint_account.owner() != &spl_token_id_v2_0() {
            return Err(Error::invalid_params(
                "Invalid param: not a v2.0 Token mint".to_string(),
            ));
        }
        let mint = Mint::unpack(mint_account.data()).map_err(|_| {
            Error::invalid_params("Invalid param: mint could not be unpacked".to_string())
        })?;

        let supply = token_amount_to_ui_amount(mint.supply, mint.decimals);
        Ok(new_response(&bank, supply))
    }

    pub fn get_token_largest_accounts(
        &self,
        mint: &Pubkey,
        commitment: Option<CommitmentConfig>,
    ) -> Result<RpcResponse<Vec<RpcTokenAccountBalance>>> {
        let bank = self.bank(commitment);
        let (mint_owner, decimals) = get_mint_owner_and_decimals(&bank, mint)?;
        if mint_owner != spl_token_id_v2_0() {
            return Err(Error::invalid_params(
                "Invalid param: not a v2.0 Token mint".to_string(),
            ));
        }
        let mut token_balances: Vec<RpcTokenAccountBalance> = self
            .get_filtered_spl_token_accounts_by_mint(&bank, mint, vec![])?
            .into_iter()
            .map(|(address, account)| {
                let amount = TokenAccount::unpack(account.data())
                    .map(|account| account.amount)
                    .unwrap_or(0);
                let amount = token_amount_to_ui_amount(amount, decimals);
                RpcTokenAccountBalance {
                    address: address.to_string(),
                    amount,
                }
            })
            .collect();
        token_balances.sort_by(|a, b| {
            a.amount
                .amount
                .parse::<u64>()
                .unwrap()
                .cmp(&b.amount.amount.parse::<u64>().unwrap())
                .reverse()
        });
        token_balances.truncate(NUM_LARGEST_ACCOUNTS);
        Ok(new_response(&bank, token_balances))
    }

    pub fn get_token_accounts_by_owner(
        &self,
        owner: &Pubkey,
        token_account_filter: TokenAccountsFilter,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<RpcResponse<Vec<RpcKeyedAccount>>> {
        let config = config.unwrap_or_default();
        let bank = self.bank(config.commitment);
        let encoding = config.encoding.unwrap_or(UiAccountEncoding::Binary);
        let data_slice_config = config.data_slice;
        check_slice_and_encoding(&encoding, data_slice_config.is_some())?;
        let (_, mint) = get_token_program_id_and_mint(&bank, token_account_filter)?;

        let mut filters = vec![];
        if let Some(mint) = mint {
            // Optional filter on Mint address
            filters.push(RpcFilterType::Memcmp(Memcmp {
                offset: 0,
                bytes: MemcmpEncodedBytes::Binary(mint.to_string()),
                encoding: None,
            }));
        }

        let keyed_accounts =
            self.get_filtered_spl_token_accounts_by_owner(&bank, owner, filters)?;
        let accounts = if encoding == UiAccountEncoding::JsonParsed {
            get_parsed_token_accounts(bank.clone(), keyed_accounts.into_iter()).collect()
        } else {
            keyed_accounts
                .into_iter()
                .map(|(pubkey, account)| RpcKeyedAccount {
                    pubkey: pubkey.to_string(),
                    account: UiAccount::encode(
                        &pubkey,
                        &account,
                        encoding,
                        None,
                        data_slice_config,
                    ),
                })
                .collect()
        };
        Ok(new_response(&bank, accounts))
    }

    pub fn get_token_accounts_by_delegate(
        &self,
        delegate: &Pubkey,
        token_account_filter: TokenAccountsFilter,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<RpcResponse<Vec<RpcKeyedAccount>>> {
        let config = config.unwrap_or_default();
        let bank = self.bank(config.commitment);
        let encoding = config.encoding.unwrap_or(UiAccountEncoding::Binary);
        let data_slice_config = config.data_slice;
        check_slice_and_encoding(&encoding, data_slice_config.is_some())?;
        let (token_program_id, mint) = get_token_program_id_and_mint(&bank, token_account_filter)?;

        let mut filters = vec![
            // Filter on Delegate is_some()
            RpcFilterType::Memcmp(Memcmp {
                offset: 72,
                bytes: MemcmpEncodedBytes::Binary(
                    bs58::encode(bincode::serialize(&1u32).unwrap()).into_string(),
                ),
                encoding: None,
            }),
            // Filter on Delegate address
            RpcFilterType::Memcmp(Memcmp {
                offset: 76,
                bytes: MemcmpEncodedBytes::Binary(delegate.to_string()),
                encoding: None,
            }),
        ];
        // Optional filter on Mint address, uses mint account index for scan
        let keyed_accounts = if let Some(mint) = mint {
            self.get_filtered_spl_token_accounts_by_mint(&bank, &mint, filters)?
        } else {
            // Filter on Token Account state
            filters.push(RpcFilterType::DataSize(
                TokenAccount::get_packed_len() as u64
            ));
            self.get_filtered_program_accounts(&bank, &token_program_id, filters)?
        };
        let accounts = if encoding == UiAccountEncoding::JsonParsed {
            get_parsed_token_accounts(bank.clone(), keyed_accounts.into_iter()).collect()
        } else {
            keyed_accounts
                .into_iter()
                .map(|(pubkey, account)| RpcKeyedAccount {
                    pubkey: pubkey.to_string(),
                    account: UiAccount::encode(
                        &pubkey,
                        &account,
                        encoding,
                        None,
                        data_slice_config,
                    ),
                })
                .collect()
        };
        Ok(new_response(&bank, accounts))
    }

    /// Use a set of filters to get an iterator of keyed program accounts from a bank
    fn get_filtered_program_accounts(
        &self,
        bank: &Arc<Bank>,
        program_id: &Pubkey,
        filters: Vec<RpcFilterType>,
    ) -> RpcCustomResult<Vec<(Pubkey, AccountSharedData)>> {
        let filter_closure = |account: &AccountSharedData| {
            filters.iter().all(|filter_type| match filter_type {
                RpcFilterType::DataSize(size) => account.data().len() as u64 == *size,
                RpcFilterType::Memcmp(compare) => compare.bytes_match(account.data()),
            })
        };
        if self
            .config
            .account_indexes
            .contains(&AccountIndex::ProgramId)
        {
            if !self.config.account_indexes.include_key(program_id) {
                return Err(RpcCustomError::KeyExcludedFromSecondaryIndex {
                    index_key: program_id.to_string(),
                });
            }
            Ok(bank
                .get_filtered_indexed_accounts(&IndexKey::ProgramId(*program_id), |account| {
                    // The program-id account index checks for Account owner on inclusion. However, due
                    // to the current AccountsDb implementation, an account may remain in storage as a
                    // zero-lamport AccountSharedData::Default() after being wiped and reinitialized in later
                    // updates. We include the redundant filters here to avoid returning these
                    // accounts.
                    account.owner() == program_id && filter_closure(account)
                })
                .map_err(|e| RpcCustomError::ScanError {
                    message: e.to_string(),
                })?)
        } else {
            Ok(bank
                .get_filtered_program_accounts(program_id, filter_closure)
                .map_err(|e| RpcCustomError::ScanError {
                    message: e.to_string(),
                })?)
        }
    }

    /// Get an iterator of spl-token accounts by owner address
    fn get_filtered_spl_token_accounts_by_owner(
        &self,
        bank: &Arc<Bank>,
        owner_key: &Pubkey,
        mut filters: Vec<RpcFilterType>,
    ) -> RpcCustomResult<Vec<(Pubkey, AccountSharedData)>> {
        // The by-owner accounts index checks for Token Account state and Owner address on
        // inclusion. However, due to the current AccountsDb implementation, an account may remain
        // in storage as a zero-lamport AccountSharedData::Default() after being wiped and reinitialized in
        // later updates. We include the redundant filters here to avoid returning these accounts.
        //
        // Filter on Token Account state
        filters.push(RpcFilterType::DataSize(
            TokenAccount::get_packed_len() as u64
        ));
        // Filter on Owner address
        filters.push(RpcFilterType::Memcmp(Memcmp {
            offset: SPL_TOKEN_ACCOUNT_OWNER_OFFSET,
            bytes: MemcmpEncodedBytes::Binary(owner_key.to_string()),
            encoding: None,
        }));

        if self
            .config
            .account_indexes
            .contains(&AccountIndex::SplTokenOwner)
        {
            if !self.config.account_indexes.include_key(owner_key) {
                return Err(RpcCustomError::KeyExcludedFromSecondaryIndex {
                    index_key: owner_key.to_string(),
                });
            }
            Ok(bank
                .get_filtered_indexed_accounts(&IndexKey::SplTokenOwner(*owner_key), |account| {
                    account.owner() == &spl_token_id_v2_0()
                        && filters.iter().all(|filter_type| match filter_type {
                            RpcFilterType::DataSize(size) => account.data().len() as u64 == *size,
                            RpcFilterType::Memcmp(compare) => compare.bytes_match(account.data()),
                        })
                })
                .map_err(|e| RpcCustomError::ScanError {
                    message: e.to_string(),
                })?)
        } else {
            self.get_filtered_program_accounts(bank, &spl_token_id_v2_0(), filters)
        }
    }

    /// Get an iterator of spl-token accounts by mint address
    fn get_filtered_spl_token_accounts_by_mint(
        &self,
        bank: &Arc<Bank>,
        mint_key: &Pubkey,
        mut filters: Vec<RpcFilterType>,
    ) -> RpcCustomResult<Vec<(Pubkey, AccountSharedData)>> {
        // The by-mint accounts index checks for Token Account state and Mint address on inclusion.
        // However, due to the current AccountsDb implementation, an account may remain in storage
        // as be zero-lamport AccountSharedData::Default() after being wiped and reinitialized in later
        // updates. We include the redundant filters here to avoid returning these accounts.
        //
        // Filter on Token Account state
        filters.push(RpcFilterType::DataSize(
            TokenAccount::get_packed_len() as u64
        ));
        // Filter on Mint address
        filters.push(RpcFilterType::Memcmp(Memcmp {
            offset: SPL_TOKEN_ACCOUNT_MINT_OFFSET,
            bytes: MemcmpEncodedBytes::Binary(mint_key.to_string()),
            encoding: None,
        }));
        if self
            .config
            .account_indexes
            .contains(&AccountIndex::SplTokenMint)
        {
            if !self.config.account_indexes.include_key(mint_key) {
                return Err(RpcCustomError::KeyExcludedFromSecondaryIndex {
                    index_key: mint_key.to_string(),
                });
            }
            Ok(bank
                .get_filtered_indexed_accounts(&IndexKey::SplTokenMint(*mint_key), |account| {
                    account.owner() == &spl_token_id_v2_0()
                        && filters.iter().all(|filter_type| match filter_type {
                            RpcFilterType::DataSize(size) => account.data().len() as u64 == *size,
                            RpcFilterType::Memcmp(compare) => compare.bytes_match(account.data()),
                        })
                })
                .map_err(|e| RpcCustomError::ScanError {
                    message: e.to_string(),
                })?)
        } else {
            self.get_filtered_program_accounts(bank, &spl_token_id_v2_0(), filters)
        }
    }
}

fn verify_transaction(
    transaction: &Transaction,
    libsecp256k1_0_5_upgrade_enabled: bool,
) -> Result<()> {
    if transaction.verify().is_err() {
        return Err(RpcCustomError::TransactionSignatureVerificationFailure.into());
    }

    if let Err(e) = transaction.verify_precompiles(libsecp256k1_0_5_upgrade_enabled) {
        return Err(RpcCustomError::TransactionPrecompileVerificationFailure(e).into());
    }

    if !transaction.verify_signatures_len() {
        return Err(RpcCustomError::TransactionSignatureVerificationFailure.into());
    }

    Ok(())
}

fn verify_filter(input: &RpcFilterType) -> Result<()> {
    input
        .verify()
        .map_err(|e| Error::invalid_params(format!("Invalid param: {:?}", e)))
}

fn verify_pubkey(input: &str) -> Result<Pubkey> {
    input
        .parse()
        .map_err(|e| Error::invalid_params(format!("Invalid param: {:?}", e)))
}

fn verify_hash(input: &str) -> Result<Hash> {
    input
        .parse()
        .map_err(|e| Error::invalid_params(format!("Invalid param: {:?}", e)))
}

fn verify_signature(input: &str) -> Result<Signature> {
    input
        .parse()
        .map_err(|e| Error::invalid_params(format!("Invalid param: {:?}", e)))
}

fn verify_token_account_filter(
    token_account_filter: RpcTokenAccountsFilter,
) -> Result<TokenAccountsFilter> {
    match token_account_filter {
        RpcTokenAccountsFilter::Mint(mint_str) => {
            let mint = verify_pubkey(&mint_str)?;
            Ok(TokenAccountsFilter::Mint(mint))
        }
        RpcTokenAccountsFilter::ProgramId(program_id_str) => {
            let program_id = verify_pubkey(&program_id_str)?;
            Ok(TokenAccountsFilter::ProgramId(program_id))
        }
    }
}

fn verify_and_parse_signatures_for_address_params(
    address: String,
    before: Option<String>,
    until: Option<String>,
    limit: Option<usize>,
) -> Result<(Pubkey, Option<Signature>, Option<Signature>, usize)> {
    let address = verify_pubkey(&address)?;
    let before = before
        .map(|ref before| verify_signature(before))
        .transpose()?;
    let until = until.map(|ref until| verify_signature(until)).transpose()?;
    let limit = limit.unwrap_or(MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT);

    if limit == 0 || limit > MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT {
        return Err(Error::invalid_params(format!(
            "Invalid limit; max {}",
            MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT
        )));
    }
    Ok((address, before, until, limit))
}

fn check_is_at_least_confirmed(commitment: CommitmentConfig) -> Result<()> {
    if !commitment.is_at_least_confirmed() {
        return Err(Error::invalid_params(
            "Method does not support commitment below `confirmed`",
        ));
    }
    Ok(())
}

fn check_slice_and_encoding(encoding: &UiAccountEncoding, data_slice_is_some: bool) -> Result<()> {
    match encoding {
        UiAccountEncoding::JsonParsed => {
            if data_slice_is_some {
                let message =
                    "Sliced account data can only be encoded using binary (base 58) or base64 encoding."
                        .to_string();
                Err(error::Error {
                    code: error::ErrorCode::InvalidRequest,
                    message,
                    data: None,
                })
            } else {
                Ok(())
            }
        }
        UiAccountEncoding::Binary
        | UiAccountEncoding::Base58
        | UiAccountEncoding::Base64
        | UiAccountEncoding::Base64Zstd => Ok(()),
    }
}

fn get_encoded_account(
    bank: &Arc<Bank>,
    pubkey: &Pubkey,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
) -> Result<Option<UiAccount>> {
    match bank.get_account(pubkey) {
        Some(account) => {
            let response = if account.owner() == &spl_token_id_v2_0()
                && encoding == UiAccountEncoding::JsonParsed
            {
                get_parsed_token_account(bank.clone(), pubkey, account)
            } else {
                encode_account(&account, pubkey, encoding, data_slice)?
            };
            Ok(Some(response))
        }
        None => Ok(None),
    }
}

fn encode_account<T: ReadableAccount>(
    account: &T,
    pubkey: &Pubkey,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
) -> Result<UiAccount> {
    if (encoding == UiAccountEncoding::Binary || encoding == UiAccountEncoding::Base58)
        && account.data().len() > 128
    {
        let message = "Encoded binary (base 58) data should be less than 128 bytes, please use Base64 encoding.".to_string();
        Err(error::Error {
            code: error::ErrorCode::InvalidRequest,
            message,
            data: None,
        })
    } else {
        Ok(UiAccount::encode(
            pubkey, account, encoding, None, data_slice,
        ))
    }
}

fn get_spl_token_owner_filter(program_id: &Pubkey, filters: &[RpcFilterType]) -> Option<Pubkey> {
    if program_id != &spl_token_id_v2_0() {
        return None;
    }
    let mut data_size_filter: Option<u64> = None;
    let mut owner_key: Option<Pubkey> = None;
    for filter in filters {
        match filter {
            RpcFilterType::DataSize(size) => data_size_filter = Some(*size),
            RpcFilterType::Memcmp(Memcmp {
                offset: SPL_TOKEN_ACCOUNT_OWNER_OFFSET,
                bytes: MemcmpEncodedBytes::Binary(bytes),
                ..
            }) => {
                if let Ok(key) = Pubkey::from_str(bytes) {
                    owner_key = Some(key)
                }
            }
            _ => {}
        }
    }
    if data_size_filter == Some(TokenAccount::get_packed_len() as u64) {
        owner_key
    } else {
        None
    }
}

fn get_spl_token_mint_filter(program_id: &Pubkey, filters: &[RpcFilterType]) -> Option<Pubkey> {
    if program_id != &spl_token_id_v2_0() {
        return None;
    }
    let mut data_size_filter: Option<u64> = None;
    let mut mint: Option<Pubkey> = None;
    for filter in filters {
        match filter {
            RpcFilterType::DataSize(size) => data_size_filter = Some(*size),
            RpcFilterType::Memcmp(Memcmp {
                offset: SPL_TOKEN_ACCOUNT_MINT_OFFSET,
                bytes: MemcmpEncodedBytes::Binary(bytes),
                ..
            }) => {
                if let Ok(key) = Pubkey::from_str(bytes) {
                    mint = Some(key)
                }
            }
            _ => {}
        }
    }
    if data_size_filter == Some(TokenAccount::get_packed_len() as u64) {
        mint
    } else {
        None
    }
}

/// Analyze a passed Pubkey that may be a Token program id or Mint address to determine the program
/// id and optional Mint
fn get_token_program_id_and_mint(
    bank: &Arc<Bank>,
    token_account_filter: TokenAccountsFilter,
) -> Result<(Pubkey, Option<Pubkey>)> {
    match token_account_filter {
        TokenAccountsFilter::Mint(mint) => {
            let (mint_owner, _) = get_mint_owner_and_decimals(bank, &mint)?;
            if mint_owner != spl_token_id_v2_0() {
                return Err(Error::invalid_params(
                    "Invalid param: not a v2.0 Token mint".to_string(),
                ));
            }
            Ok((mint_owner, Some(mint)))
        }
        TokenAccountsFilter::ProgramId(program_id) => {
            if program_id == spl_token_id_v2_0() {
                Ok((program_id, None))
            } else {
                Err(Error::invalid_params(
                    "Invalid param: unrecognized Token program id".to_string(),
                ))
            }
        }
    }
}

fn _send_transaction(
    meta: JsonRpcRequestProcessor,
    transaction: Transaction,
    wire_transaction: Vec<u8>,
    last_valid_slot: Slot,
    durable_nonce_info: Option<(Pubkey, Hash)>,
) -> Result<String> {
    if transaction.signatures.is_empty() {
        return Err(RpcCustomError::TransactionSignatureVerificationFailure.into());
    }
    let signature = transaction.signatures[0];
    let transaction_info = TransactionInfo::new(
        signature,
        wire_transaction,
        last_valid_slot,
        durable_nonce_info,
    );
    meta.transaction_sender
        .lock()
        .unwrap()
        .send(transaction_info)
        .unwrap_or_else(|err| warn!("Failed to enqueue transaction: {}", err));

    Ok(signature.to_string())
}

// Minimal RPC interface that trusted validators are expected to provide
pub mod rpc_minimal {
    use super::*;
    #[rpc]
    pub trait Minimal {
        type Metadata;

        #[rpc(meta, name = "getBalance")]
        fn get_balance(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<u64>>;

        #[rpc(meta, name = "getEpochInfo")]
        fn get_epoch_info(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<EpochInfo>;

        #[rpc(meta, name = "getHealth")]
        fn get_health(&self, meta: Self::Metadata) -> Result<String>;

        #[rpc(meta, name = "getIdentity")]
        fn get_identity(&self, meta: Self::Metadata) -> Result<RpcIdentity>;

        #[rpc(meta, name = "getSlot")]
        fn get_slot(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<Slot>;

        #[rpc(meta, name = "getBlockHeight")]
        fn get_block_height(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<u64>;

        #[rpc(meta, name = "getSnapshotSlot")]
        fn get_snapshot_slot(&self, meta: Self::Metadata) -> Result<Slot>;

        #[rpc(meta, name = "getTransactionCount")]
        fn get_transaction_count(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<u64>;

        #[rpc(meta, name = "getVersion")]
        fn get_version(&self, meta: Self::Metadata) -> Result<RpcVersionInfo>;

        // TODO: Refactor `solana-validator wait-for-restart-window` to not require this method, so
        //       it can be removed from rpc_minimal
        #[rpc(meta, name = "getVoteAccounts")]
        fn get_vote_accounts(
            &self,
            meta: Self::Metadata,
            config: Option<RpcGetVoteAccountsConfig>,
        ) -> Result<RpcVoteAccountStatus>;

        // TODO: Refactor `solana-validator wait-for-restart-window` to not require this method, so
        //       it can be removed from rpc_minimal
        #[rpc(meta, name = "getLeaderSchedule")]
        fn get_leader_schedule(
            &self,
            meta: Self::Metadata,
            options: Option<RpcLeaderScheduleConfigWrapper>,
            config: Option<RpcLeaderScheduleConfig>,
        ) -> Result<Option<RpcLeaderSchedule>>;
    }

    pub struct MinimalImpl;
    impl Minimal for MinimalImpl {
        type Metadata = JsonRpcRequestProcessor;

        fn get_balance(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<u64>> {
            debug!("get_balance rpc request received: {:?}", pubkey_str);
            let pubkey = verify_pubkey(&pubkey_str)?;
            Ok(meta.get_balance(&pubkey, commitment))
        }

        fn get_epoch_info(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<EpochInfo> {
            debug!("get_epoch_info rpc request received");
            let bank = meta.bank(commitment);
            Ok(bank.get_epoch_info())
        }

        fn get_health(&self, meta: Self::Metadata) -> Result<String> {
            match meta.health.check() {
                RpcHealthStatus::Ok => Ok("ok".to_string()),
                RpcHealthStatus::Unknown => Err(RpcCustomError::NodeUnhealthy {
                    num_slots_behind: None,
                }
                .into()),
                RpcHealthStatus::Behind { num_slots } => Err(RpcCustomError::NodeUnhealthy {
                    num_slots_behind: Some(num_slots),
                }
                .into()),
            }
        }

        fn get_identity(&self, meta: Self::Metadata) -> Result<RpcIdentity> {
            debug!("get_identity rpc request received");
            Ok(RpcIdentity {
                identity: meta.cluster_info.id().to_string(),
            })
        }

        fn get_slot(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<Slot> {
            debug!("get_slot rpc request received");
            Ok(meta.get_slot(commitment))
        }

        fn get_block_height(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<u64> {
            debug!("get_block_height rpc request received");
            Ok(meta.get_block_height(commitment))
        }

        fn get_snapshot_slot(&self, meta: Self::Metadata) -> Result<Slot> {
            debug!("get_snapshot_slot rpc request received");

            meta.snapshot_config
                .and_then(|snapshot_config| {
                    snapshot_utils::get_highest_full_snapshot_archive_slot(
                        &snapshot_config.snapshot_package_output_path,
                    )
                })
                .ok_or_else(|| RpcCustomError::NoSnapshot.into())
        }

        fn get_transaction_count(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<u64> {
            debug!("get_transaction_count rpc request received");
            Ok(meta.get_transaction_count(commitment))
        }

        fn get_version(&self, _: Self::Metadata) -> Result<RpcVersionInfo> {
            debug!("get_version rpc request received");
            let version = solana_version::Version::default();
            Ok(RpcVersionInfo {
                solana_core: version.to_string(),
                feature_set: Some(version.feature_set),
            })
        }

        // TODO: Refactor `solana-validator wait-for-restart-window` to not require this method, so
        //       it can be removed from rpc_minimal
        fn get_vote_accounts(
            &self,
            meta: Self::Metadata,
            config: Option<RpcGetVoteAccountsConfig>,
        ) -> Result<RpcVoteAccountStatus> {
            debug!("get_vote_accounts rpc request received");
            meta.get_vote_accounts(config)
        }

        // TODO: Refactor `solana-validator wait-for-restart-window` to not require this method, so
        //       it can be removed from rpc_minimal
        fn get_leader_schedule(
            &self,
            meta: Self::Metadata,
            options: Option<RpcLeaderScheduleConfigWrapper>,
            config: Option<RpcLeaderScheduleConfig>,
        ) -> Result<Option<RpcLeaderSchedule>> {
            let (slot, maybe_config) = options.map(|options| options.unzip()).unwrap_or_default();
            let config = maybe_config.or(config).unwrap_or_default();

            if let Some(ref identity) = config.identity {
                let _ = verify_pubkey(identity)?;
            }

            let bank = meta.bank(config.commitment);
            let slot = slot.unwrap_or_else(|| bank.slot());
            let epoch = bank.epoch_schedule().get_epoch(slot);

            debug!("get_leader_schedule rpc request received: {:?}", slot);

            Ok(meta
                .leader_schedule_cache
                .get_epoch_leader_schedule(epoch)
                .map(|leader_schedule| {
                    let mut schedule_by_identity =
                        solana_ledger::leader_schedule_utils::leader_schedule_by_identity(
                            leader_schedule.get_slot_leaders().iter().enumerate(),
                        );
                    if let Some(identity) = config.identity {
                        schedule_by_identity.retain(|k, _| *k == identity);
                    }
                    schedule_by_identity
                }))
        }
    }
}

// RPC interface that only depends on immediate Bank data
// Expected to be provided by both API nodes and (future) accounts replica nodes
pub mod rpc_bank {
    use super::*;
    #[rpc]
    pub trait BankData {
        type Metadata;

        #[rpc(meta, name = "getMinimumBalanceForRentExemption")]
        fn get_minimum_balance_for_rent_exemption(
            &self,
            meta: Self::Metadata,
            data_len: usize,
            commitment: Option<CommitmentConfig>,
        ) -> Result<u64>;

        #[rpc(meta, name = "getInflationGovernor")]
        fn get_inflation_governor(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcInflationGovernor>;

        #[rpc(meta, name = "getInflationRate")]
        fn get_inflation_rate(&self, meta: Self::Metadata) -> Result<RpcInflationRate>;

        #[rpc(meta, name = "getEpochSchedule")]
        fn get_epoch_schedule(&self, meta: Self::Metadata) -> Result<EpochSchedule>;

        #[rpc(meta, name = "getRecentBlockhash")]
        fn get_recent_blockhash(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcBlockhashFeeCalculator>>;

        #[rpc(meta, name = "getFees")]
        fn get_fees(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcFees>>;

        #[rpc(meta, name = "getFeeCalculatorForBlockhash")]
        fn get_fee_calculator_for_blockhash(
            &self,
            meta: Self::Metadata,
            blockhash: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<Option<RpcFeeCalculator>>>;

        #[rpc(meta, name = "getFeeRateGovernor")]
        fn get_fee_rate_governor(
            &self,
            meta: Self::Metadata,
        ) -> Result<RpcResponse<RpcFeeRateGovernor>>;

        #[rpc(meta, name = "getSlotLeader")]
        fn get_slot_leader(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<String>;

        #[rpc(meta, name = "getSlotLeaders")]
        fn get_slot_leaders(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            limit: u64,
        ) -> Result<Vec<String>>;

        #[rpc(meta, name = "getBlockProduction")]
        fn get_block_production(
            &self,
            meta: Self::Metadata,
            config: Option<RpcBlockProductionConfig>,
        ) -> Result<RpcResponse<RpcBlockProduction>>;
    }

    pub struct BankDataImpl;
    impl BankData for BankDataImpl {
        type Metadata = JsonRpcRequestProcessor;

        fn get_minimum_balance_for_rent_exemption(
            &self,
            meta: Self::Metadata,
            data_len: usize,
            commitment: Option<CommitmentConfig>,
        ) -> Result<u64> {
            debug!(
                "get_minimum_balance_for_rent_exemption rpc request received: {:?}",
                data_len
            );
            if data_len as u64 > system_instruction::MAX_PERMITTED_DATA_LENGTH {
                return Err(Error::invalid_request());
            }
            Ok(meta.get_minimum_balance_for_rent_exemption(data_len, commitment))
        }

        fn get_inflation_governor(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcInflationGovernor> {
            debug!("get_inflation_governor rpc request received");
            Ok(meta.get_inflation_governor(commitment))
        }

        fn get_inflation_rate(&self, meta: Self::Metadata) -> Result<RpcInflationRate> {
            debug!("get_inflation_rate rpc request received");
            Ok(meta.get_inflation_rate())
        }

        fn get_epoch_schedule(&self, meta: Self::Metadata) -> Result<EpochSchedule> {
            debug!("get_epoch_schedule rpc request received");
            Ok(meta.get_epoch_schedule())
        }

        fn get_recent_blockhash(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcBlockhashFeeCalculator>> {
            debug!("get_recent_blockhash rpc request received");
            Ok(meta.get_recent_blockhash(commitment))
        }

        fn get_fees(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcFees>> {
            debug!("get_fees rpc request received");
            Ok(meta.get_fees(commitment))
        }

        fn get_fee_calculator_for_blockhash(
            &self,
            meta: Self::Metadata,
            blockhash: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<Option<RpcFeeCalculator>>> {
            debug!("get_fee_calculator_for_blockhash rpc request received");
            let blockhash = Hash::from_str(&blockhash)
                .map_err(|e| Error::invalid_params(format!("{:?}", e)))?;
            Ok(meta.get_fee_calculator_for_blockhash(&blockhash, commitment))
        }

        fn get_fee_rate_governor(
            &self,
            meta: Self::Metadata,
        ) -> Result<RpcResponse<RpcFeeRateGovernor>> {
            debug!("get_fee_rate_governor rpc request received");
            Ok(meta.get_fee_rate_governor())
        }

        fn get_slot_leader(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<String> {
            debug!("get_slot_leader rpc request received");
            Ok(meta.get_slot_leader(commitment))
        }

        fn get_slot_leaders(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            limit: u64,
        ) -> Result<Vec<String>> {
            debug!(
                "get_slot_leaders rpc request received (start: {} limit: {})",
                start_slot, limit
            );

            let limit = limit as usize;
            if limit > MAX_GET_SLOT_LEADERS {
                return Err(Error::invalid_params(format!(
                    "Invalid limit; max {}",
                    MAX_GET_SLOT_LEADERS
                )));
            }

            Ok(meta
                .get_slot_leaders(None, start_slot, limit)?
                .into_iter()
                .map(|identity| identity.to_string())
                .collect())
        }

        fn get_block_production(
            &self,
            meta: Self::Metadata,
            config: Option<RpcBlockProductionConfig>,
        ) -> Result<RpcResponse<RpcBlockProduction>> {
            debug!("get_block_production rpc request received");

            let config = config.unwrap_or_default();
            let filter_by_identity = if let Some(ref identity) = config.identity {
                Some(verify_pubkey(identity)?)
            } else {
                None
            };

            let bank = meta.bank(config.commitment);
            let (first_slot, last_slot) = match config.range {
                None => (
                    bank.epoch_schedule().get_first_slot_in_epoch(bank.epoch()),
                    bank.slot(),
                ),
                Some(range) => {
                    let first_slot = range.first_slot;
                    let last_slot = range.last_slot.unwrap_or_else(|| bank.slot());
                    if last_slot < first_slot {
                        return Err(Error::invalid_params(format!(
                            "lastSlot, {}, cannot be less than firstSlot, {}",
                            last_slot, first_slot
                        )));
                    }
                    (first_slot, last_slot)
                }
            };

            let slot_history = bank.get_slot_history();
            if first_slot < slot_history.oldest() {
                return Err(Error::invalid_params(format!(
                    "firstSlot, {}, is too small; min {}",
                    first_slot,
                    slot_history.oldest()
                )));
            }
            if last_slot > slot_history.newest() {
                return Err(Error::invalid_params(format!(
                    "lastSlot, {}, is too large; max {}",
                    last_slot,
                    slot_history.newest()
                )));
            }

            let slot_leaders = meta.get_slot_leaders(
                config.commitment,
                first_slot,
                last_slot.saturating_sub(first_slot) as usize + 1, // +1 because last_slot is inclusive
            )?;

            let mut block_production: HashMap<_, (usize, usize)> = HashMap::new();

            let mut slot = first_slot;
            for identity in slot_leaders {
                if let Some(ref filter_by_identity) = filter_by_identity {
                    if identity != *filter_by_identity {
                        slot += 1;
                        continue;
                    }
                }

                let mut entry = block_production.entry(identity).or_default();
                if slot_history.check(slot) == solana_sdk::slot_history::Check::Found {
                    entry.1 += 1; // Increment blocks_produced
                }
                entry.0 += 1; // Increment leader_slots
                slot += 1;
            }

            Ok(new_response(
                &bank,
                RpcBlockProduction {
                    by_identity: block_production
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect(),
                    range: RpcBlockProductionRange {
                        first_slot,
                        last_slot,
                    },
                },
            ))
        }
    }
}

// RPC interface that depends on AccountsDB
// Expected to be provided by API nodes, but collected for easy separation and offloading to
// accounts replica nodes in the future.
pub mod rpc_accounts {
    use super::*;
    #[rpc]
    pub trait AccountsData {
        type Metadata;

        #[rpc(meta, name = "getAccountInfo")]
        fn get_account_info(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            config: Option<RpcAccountInfoConfig>,
        ) -> Result<RpcResponse<Option<UiAccount>>>;

        #[rpc(meta, name = "getMultipleAccounts")]
        fn get_multiple_accounts(
            &self,
            meta: Self::Metadata,
            pubkey_strs: Vec<String>,
            config: Option<RpcAccountInfoConfig>,
        ) -> Result<RpcResponse<Vec<Option<UiAccount>>>>;

        #[rpc(meta, name = "getProgramAccounts")]
        fn get_program_accounts(
            &self,
            meta: Self::Metadata,
            program_id_str: String,
            config: Option<RpcProgramAccountsConfig>,
        ) -> Result<OptionalContext<Vec<RpcKeyedAccount>>>;

        #[rpc(meta, name = "getBlockCommitment")]
        fn get_block_commitment(
            &self,
            meta: Self::Metadata,
            block: Slot,
        ) -> Result<RpcBlockCommitment<BlockCommitmentArray>>;

        #[rpc(meta, name = "getLargestAccounts")]
        fn get_largest_accounts(
            &self,
            meta: Self::Metadata,
            config: Option<RpcLargestAccountsConfig>,
        ) -> Result<RpcResponse<Vec<RpcAccountBalance>>>;

        #[rpc(meta, name = "getSupply")]
        fn get_supply(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcSupply>>;

        #[rpc(meta, name = "getStakeActivation")]
        fn get_stake_activation(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            config: Option<RpcEpochConfig>,
        ) -> Result<RpcStakeActivation>;

        // SPL Token-specific RPC endpoints
        // See https://github.com/solana-labs/solana-program-library/releases/tag/token-v2.0.0 for
        // program details

        #[rpc(meta, name = "getTokenAccountBalance")]
        fn get_token_account_balance(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<UiTokenAmount>>;

        #[rpc(meta, name = "getTokenSupply")]
        fn get_token_supply(
            &self,
            meta: Self::Metadata,
            mint_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<UiTokenAmount>>;

        #[rpc(meta, name = "getTokenLargestAccounts")]
        fn get_token_largest_accounts(
            &self,
            meta: Self::Metadata,
            mint_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<Vec<RpcTokenAccountBalance>>>;

        #[rpc(meta, name = "getTokenAccountsByOwner")]
        fn get_token_accounts_by_owner(
            &self,
            meta: Self::Metadata,
            owner_str: String,
            token_account_filter: RpcTokenAccountsFilter,
            config: Option<RpcAccountInfoConfig>,
        ) -> Result<RpcResponse<Vec<RpcKeyedAccount>>>;

        #[rpc(meta, name = "getTokenAccountsByDelegate")]
        fn get_token_accounts_by_delegate(
            &self,
            meta: Self::Metadata,
            delegate_str: String,
            token_account_filter: RpcTokenAccountsFilter,
            config: Option<RpcAccountInfoConfig>,
        ) -> Result<RpcResponse<Vec<RpcKeyedAccount>>>;
    }

    pub struct AccountsDataImpl;
    impl AccountsData for AccountsDataImpl {
        type Metadata = JsonRpcRequestProcessor;

        fn get_account_info(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            config: Option<RpcAccountInfoConfig>,
        ) -> Result<RpcResponse<Option<UiAccount>>> {
            debug!("get_account_info rpc request received: {:?}", pubkey_str);
            let pubkey = verify_pubkey(&pubkey_str)?;
            meta.get_account_info(&pubkey, config)
        }

        fn get_multiple_accounts(
            &self,
            meta: Self::Metadata,
            pubkey_strs: Vec<String>,
            config: Option<RpcAccountInfoConfig>,
        ) -> Result<RpcResponse<Vec<Option<UiAccount>>>> {
            debug!(
                "get_multiple_accounts rpc request received: {:?}",
                pubkey_strs.len()
            );

            let max_multiple_accounts = meta
                .config
                .max_multiple_accounts
                .unwrap_or(MAX_MULTIPLE_ACCOUNTS);
            if pubkey_strs.len() > max_multiple_accounts {
                return Err(Error::invalid_params(format!(
                    "Too many inputs provided; max {}",
                    max_multiple_accounts
                )));
            }
            let mut pubkeys: Vec<Pubkey> = vec![];
            for pubkey_str in pubkey_strs {
                pubkeys.push(verify_pubkey(&pubkey_str)?);
            }
            meta.get_multiple_accounts(pubkeys, config)
        }

        fn get_program_accounts(
            &self,
            meta: Self::Metadata,
            program_id_str: String,
            config: Option<RpcProgramAccountsConfig>,
        ) -> Result<OptionalContext<Vec<RpcKeyedAccount>>> {
            debug!(
                "get_program_accounts rpc request received: {:?}",
                program_id_str
            );
            let program_id = verify_pubkey(&program_id_str)?;
            let (config, filters, with_context) = if let Some(config) = config {
                (
                    Some(config.account_config),
                    config.filters.unwrap_or_default(),
                    config.with_context.unwrap_or_default(),
                )
            } else {
                (None, vec![], false)
            };
            if filters.len() > MAX_GET_PROGRAM_ACCOUNT_FILTERS {
                return Err(Error::invalid_params(format!(
                    "Too many filters provided; max {}",
                    MAX_GET_PROGRAM_ACCOUNT_FILTERS
                )));
            }
            for filter in &filters {
                verify_filter(filter)?;
            }
            meta.get_program_accounts(&program_id, config, filters, with_context)
        }

        fn get_block_commitment(
            &self,
            meta: Self::Metadata,
            block: Slot,
        ) -> Result<RpcBlockCommitment<BlockCommitmentArray>> {
            debug!("get_block_commitment rpc request received");
            Ok(meta.get_block_commitment(block))
        }

        fn get_largest_accounts(
            &self,
            meta: Self::Metadata,
            config: Option<RpcLargestAccountsConfig>,
        ) -> Result<RpcResponse<Vec<RpcAccountBalance>>> {
            debug!("get_largest_accounts rpc request received");
            Ok(meta.get_largest_accounts(config)?)
        }

        fn get_supply(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcSupply>> {
            debug!("get_supply rpc request received");
            Ok(meta.get_supply(commitment)?)
        }

        fn get_stake_activation(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            config: Option<RpcEpochConfig>,
        ) -> Result<RpcStakeActivation> {
            debug!(
                "get_stake_activation rpc request received: {:?}",
                pubkey_str
            );
            let pubkey = verify_pubkey(&pubkey_str)?;
            meta.get_stake_activation(&pubkey, config)
        }

        fn get_token_account_balance(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<UiTokenAmount>> {
            debug!(
                "get_token_account_balance rpc request received: {:?}",
                pubkey_str
            );
            let pubkey = verify_pubkey(&pubkey_str)?;
            meta.get_token_account_balance(&pubkey, commitment)
        }

        fn get_token_supply(
            &self,
            meta: Self::Metadata,
            mint_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<UiTokenAmount>> {
            debug!("get_token_supply rpc request received: {:?}", mint_str);
            let mint = verify_pubkey(&mint_str)?;
            meta.get_token_supply(&mint, commitment)
        }

        fn get_token_largest_accounts(
            &self,
            meta: Self::Metadata,
            mint_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<Vec<RpcTokenAccountBalance>>> {
            debug!(
                "get_token_largest_accounts rpc request received: {:?}",
                mint_str
            );
            let mint = verify_pubkey(&mint_str)?;
            meta.get_token_largest_accounts(&mint, commitment)
        }

        fn get_token_accounts_by_owner(
            &self,
            meta: Self::Metadata,
            owner_str: String,
            token_account_filter: RpcTokenAccountsFilter,
            config: Option<RpcAccountInfoConfig>,
        ) -> Result<RpcResponse<Vec<RpcKeyedAccount>>> {
            debug!(
                "get_token_accounts_by_owner rpc request received: {:?}",
                owner_str
            );
            let owner = verify_pubkey(&owner_str)?;
            let token_account_filter = verify_token_account_filter(token_account_filter)?;
            meta.get_token_accounts_by_owner(&owner, token_account_filter, config)
        }

        fn get_token_accounts_by_delegate(
            &self,
            meta: Self::Metadata,
            delegate_str: String,
            token_account_filter: RpcTokenAccountsFilter,
            config: Option<RpcAccountInfoConfig>,
        ) -> Result<RpcResponse<Vec<RpcKeyedAccount>>> {
            debug!(
                "get_token_accounts_by_delegate rpc request received: {:?}",
                delegate_str
            );
            let delegate = verify_pubkey(&delegate_str)?;
            let token_account_filter = verify_token_account_filter(token_account_filter)?;
            meta.get_token_accounts_by_delegate(&delegate, token_account_filter, config)
        }
    }
}

// Full RPC interface that an API node is expected to provide
// (rpc_minimal should also be provided by an API node)
pub mod rpc_full {
    use super::*;
    #[rpc]
    pub trait Full {
        type Metadata;

        #[rpc(meta, name = "getInflationReward")]
        fn get_inflation_reward(
            &self,
            meta: Self::Metadata,
            address_strs: Vec<String>,
            config: Option<RpcEpochConfig>,
        ) -> BoxFuture<Result<Vec<Option<RpcInflationReward>>>>;

        #[rpc(meta, name = "getClusterNodes")]
        fn get_cluster_nodes(&self, meta: Self::Metadata) -> Result<Vec<RpcContactInfo>>;

        #[rpc(meta, name = "getRecentPerformanceSamples")]
        fn get_recent_performance_samples(
            &self,
            meta: Self::Metadata,
            limit: Option<usize>,
        ) -> Result<Vec<RpcPerfSample>>;

        #[rpc(meta, name = "getGenesisHash")]
        fn get_genesis_hash(&self, meta: Self::Metadata) -> Result<String>;

        #[rpc(meta, name = "getSignatureStatuses")]
        fn get_signature_statuses(
            &self,
            meta: Self::Metadata,
            signature_strs: Vec<String>,
            config: Option<RpcSignatureStatusConfig>,
        ) -> BoxFuture<Result<RpcResponse<Vec<Option<TransactionStatus>>>>>;

        #[rpc(meta, name = "getMaxRetransmitSlot")]
        fn get_max_retransmit_slot(&self, meta: Self::Metadata) -> Result<Slot>;

        #[rpc(meta, name = "getMaxShredInsertSlot")]
        fn get_max_shred_insert_slot(&self, meta: Self::Metadata) -> Result<Slot>;

        #[rpc(meta, name = "requestAirdrop")]
        fn request_airdrop(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            lamports: u64,
            config: Option<RpcRequestAirdropConfig>,
        ) -> Result<String>;

        #[rpc(meta, name = "sendTransaction")]
        fn send_transaction(
            &self,
            meta: Self::Metadata,
            data: String,
            config: Option<RpcSendTransactionConfig>,
        ) -> Result<String>;

        #[rpc(meta, name = "simulateTransaction")]
        fn simulate_transaction(
            &self,
            meta: Self::Metadata,
            data: String,
            config: Option<RpcSimulateTransactionConfig>,
        ) -> Result<RpcResponse<RpcSimulateTransactionResult>>;

        #[rpc(meta, name = "minimumLedgerSlot")]
        fn minimum_ledger_slot(&self, meta: Self::Metadata) -> Result<Slot>;

        #[rpc(meta, name = "getBlock")]
        fn get_block(
            &self,
            meta: Self::Metadata,
            slot: Slot,
            config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
        ) -> BoxFuture<Result<Option<UiConfirmedBlock>>>;

        #[rpc(meta, name = "getBlockTime")]
        fn get_block_time(
            &self,
            meta: Self::Metadata,
            slot: Slot,
        ) -> BoxFuture<Result<Option<UnixTimestamp>>>;

        #[rpc(meta, name = "getBlocks")]
        fn get_blocks(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            config: Option<RpcBlocksConfigWrapper>,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>>;

        #[rpc(meta, name = "getBlocksWithLimit")]
        fn get_blocks_with_limit(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            limit: usize,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>>;

        #[rpc(meta, name = "getTransaction")]
        fn get_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
        ) -> BoxFuture<Result<Option<EncodedConfirmedTransaction>>>;

        #[rpc(meta, name = "getSignaturesForAddress")]
        fn get_signatures_for_address(
            &self,
            meta: Self::Metadata,
            address: String,
            config: Option<RpcSignaturesForAddressConfig>,
        ) -> BoxFuture<Result<Vec<RpcConfirmedTransactionStatusWithSignature>>>;

        #[rpc(meta, name = "getFirstAvailableBlock")]
        fn get_first_available_block(&self, meta: Self::Metadata) -> BoxFuture<Result<Slot>>;
    }

    pub struct FullImpl;
    impl Full for FullImpl {
        type Metadata = JsonRpcRequestProcessor;

        fn get_recent_performance_samples(
            &self,
            meta: Self::Metadata,
            limit: Option<usize>,
        ) -> Result<Vec<RpcPerfSample>> {
            debug!("get_recent_performance_samples request received");

            let limit = limit.unwrap_or(PERFORMANCE_SAMPLES_LIMIT);

            if limit > PERFORMANCE_SAMPLES_LIMIT {
                return Err(Error::invalid_params(format!(
                    "Invalid limit; max {}",
                    PERFORMANCE_SAMPLES_LIMIT
                )));
            }

            Ok(meta
                .blockstore
                .get_recent_perf_samples(limit)
                .map_err(|err| {
                    warn!("get_recent_performance_samples failed: {:?}", err);
                    Error::invalid_request()
                })?
                .iter()
                .map(|(slot, sample)| RpcPerfSample {
                    slot: *slot,
                    num_transactions: sample.num_transactions,
                    num_slots: sample.num_slots,
                    sample_period_secs: sample.sample_period_secs,
                })
                .collect())
        }

        fn get_cluster_nodes(&self, meta: Self::Metadata) -> Result<Vec<RpcContactInfo>> {
            debug!("get_cluster_nodes rpc request received");
            let cluster_info = &meta.cluster_info;
            let socket_addr_space = cluster_info.socket_addr_space();
            let valid_address_or_none = |addr: &SocketAddr| -> Option<SocketAddr> {
                if ContactInfo::is_valid_address(addr, socket_addr_space) {
                    Some(*addr)
                } else {
                    None
                }
            };
            let my_shred_version = cluster_info.my_shred_version();
            Ok(cluster_info
                .all_peers()
                .iter()
                .filter_map(|(contact_info, _)| {
                    if my_shred_version == contact_info.shred_version
                        && ContactInfo::is_valid_address(&contact_info.gossip, socket_addr_space)
                    {
                        let (version, feature_set) = if let Some(version) =
                            cluster_info.get_node_version(&contact_info.id)
                        {
                            (Some(version.to_string()), Some(version.feature_set))
                        } else {
                            (None, None)
                        };
                        Some(RpcContactInfo {
                            pubkey: contact_info.id.to_string(),
                            gossip: Some(contact_info.gossip),
                            tpu: valid_address_or_none(&contact_info.tpu),
                            rpc: valid_address_or_none(&contact_info.rpc),
                            version,
                            feature_set,
                            shred_version: Some(my_shred_version),
                        })
                    } else {
                        None // Exclude spy nodes
                    }
                })
                .collect())
        }

        fn get_genesis_hash(&self, meta: Self::Metadata) -> Result<String> {
            debug!("get_genesis_hash rpc request received");
            Ok(meta.genesis_hash.to_string())
        }

        fn get_signature_statuses(
            &self,
            meta: Self::Metadata,
            signature_strs: Vec<String>,
            config: Option<RpcSignatureStatusConfig>,
        ) -> BoxFuture<Result<RpcResponse<Vec<Option<TransactionStatus>>>>> {
            debug!(
                "get_signature_statuses rpc request received: {:?}",
                signature_strs.len()
            );
            if signature_strs.len() > MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS {
                return Box::pin(future::err(Error::invalid_params(format!(
                    "Too many inputs provided; max {}",
                    MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS
                ))));
            }
            let mut signatures: Vec<Signature> = vec![];
            for signature_str in signature_strs {
                match verify_signature(&signature_str) {
                    Ok(signature) => {
                        signatures.push(signature);
                    }
                    Err(err) => return Box::pin(future::err(err)),
                }
            }
            Box::pin(async move { meta.get_signature_statuses(signatures, config).await })
        }

        fn get_max_retransmit_slot(&self, meta: Self::Metadata) -> Result<Slot> {
            debug!("get_max_retransmit_slot rpc request received");
            Ok(meta.get_max_retransmit_slot())
        }

        fn get_max_shred_insert_slot(&self, meta: Self::Metadata) -> Result<Slot> {
            debug!("get_max_shred_insert_slot rpc request received");
            Ok(meta.get_max_shred_insert_slot())
        }

        fn request_airdrop(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            lamports: u64,
            config: Option<RpcRequestAirdropConfig>,
        ) -> Result<String> {
            debug!("request_airdrop rpc request received");
            trace!(
                "request_airdrop id={} lamports={} config: {:?}",
                pubkey_str,
                lamports,
                &config
            );

            let faucet_addr = meta.config.faucet_addr.ok_or_else(Error::invalid_request)?;
            let pubkey = verify_pubkey(&pubkey_str)?;

            let config = config.unwrap_or_default();
            let bank = meta.bank(config.commitment);

            let blockhash = if let Some(blockhash) = config.recent_blockhash {
                verify_hash(&blockhash)?
            } else {
                bank.confirmed_last_blockhash().0
            };
            let last_valid_slot = bank.get_blockhash_last_valid_slot(&blockhash).unwrap_or(0);

            let transaction =
                request_airdrop_transaction(&faucet_addr, &pubkey, lamports, blockhash).map_err(
                    |err| {
                        info!("request_airdrop_transaction failed: {:?}", err);
                        Error::internal_error()
                    },
                )?;

            let wire_transaction = serialize(&transaction).map_err(|err| {
                info!("request_airdrop: serialize error: {:?}", err);
                Error::internal_error()
            })?;

            _send_transaction(meta, transaction, wire_transaction, last_valid_slot, None)
        }

        fn send_transaction(
            &self,
            meta: Self::Metadata,
            data: String,
            config: Option<RpcSendTransactionConfig>,
        ) -> Result<String> {
            debug!("send_transaction rpc request received");
            let config = config.unwrap_or_default();
            let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base58);
            let (wire_transaction, transaction) = deserialize_transaction(data, encoding)?;

            let preflight_commitment = config
                .preflight_commitment
                .map(|commitment| CommitmentConfig { commitment });
            let preflight_bank = &*meta.bank(preflight_commitment);

            let mut last_valid_slot = preflight_bank
                .get_blockhash_last_valid_slot(&transaction.message.recent_blockhash)
                .unwrap_or(0);

            let durable_nonce_info = solana_sdk::transaction::uses_durable_nonce(&transaction)
                .and_then(|nonce_ix| {
                    solana_sdk::transaction::get_nonce_pubkey_from_instruction(
                        nonce_ix,
                        &transaction,
                    )
                })
                .map(|&pubkey| (pubkey, transaction.message.recent_blockhash));
            if durable_nonce_info.is_some() {
                // While it uses a defined constant, this last_valid_slot value is chosen arbitrarily.
                // It provides a fallback timeout for durable-nonce transaction retries in case of
                // malicious packing of the retry queue. Durable-nonce transactions are otherwise
                // retried until the nonce is advanced.
                last_valid_slot = preflight_bank.slot() + MAX_RECENT_BLOCKHASHES as u64;
            }

            if !config.skip_preflight {
                if let Err(e) = verify_transaction(
                    &transaction,
                    preflight_bank.libsecp256k1_0_5_upgrade_enabled(),
                ) {
                    return Err(e);
                }

                match meta.health.check() {
                    RpcHealthStatus::Ok => (),
                    RpcHealthStatus::Unknown => {
                        inc_new_counter_info!("rpc-send-tx_health-unknown", 1);
                        return Err(RpcCustomError::NodeUnhealthy {
                            num_slots_behind: None,
                        }
                        .into());
                    }
                    RpcHealthStatus::Behind { num_slots } => {
                        inc_new_counter_info!("rpc-send-tx_health-behind", 1);
                        return Err(RpcCustomError::NodeUnhealthy {
                            num_slots_behind: Some(num_slots),
                        }
                        .into());
                    }
                }

                if let TransactionSimulationResult {
                    result: Err(err),
                    logs,
                    post_simulation_accounts: _,
                    units_consumed,
                } = preflight_bank.simulate_transaction(&transaction)
                {
                    match err {
                        TransactionError::BlockhashNotFound => {
                            inc_new_counter_info!("rpc-send-tx_err-blockhash-not-found", 1);
                        }
                        _ => {
                            inc_new_counter_info!("rpc-send-tx_err-other", 1);
                        }
                    }
                    return Err(RpcCustomError::SendTransactionPreflightFailure {
                        message: format!("Transaction simulation failed: {}", err),
                        result: RpcSimulateTransactionResult {
                            err: Some(err),
                            logs: Some(logs),
                            accounts: None,
                            units_consumed: Some(units_consumed),
                        },
                    }
                    .into());
                }
            }

            _send_transaction(
                meta,
                transaction,
                wire_transaction,
                last_valid_slot,
                durable_nonce_info,
            )
        }

        fn simulate_transaction(
            &self,
            meta: Self::Metadata,
            data: String,
            config: Option<RpcSimulateTransactionConfig>,
        ) -> Result<RpcResponse<RpcSimulateTransactionResult>> {
            debug!("simulate_transaction rpc request received");
            let config = config.unwrap_or_default();
            let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base58);
            let (_, mut transaction) = deserialize_transaction(data, encoding)?;

            let bank = &*meta.bank(config.commitment);
            if config.sig_verify {
                if config.replace_recent_blockhash {
                    return Err(Error::invalid_params(
                        "sigVerify may not be used with replaceRecentBlockhash",
                    ));
                }

                if let Err(e) =
                    verify_transaction(&transaction, bank.libsecp256k1_0_5_upgrade_enabled())
                {
                    return Err(e);
                }
            }
            if config.replace_recent_blockhash {
                transaction.message.recent_blockhash = bank.last_blockhash();
            }
            let TransactionSimulationResult {
                result,
                logs,
                post_simulation_accounts,
                units_consumed,
            } = bank.simulate_transaction(&transaction);

            let accounts = if let Some(config_accounts) = config.accounts {
                let accounts_encoding = config_accounts
                    .encoding
                    .unwrap_or(UiAccountEncoding::Base64);

                if accounts_encoding == UiAccountEncoding::Binary
                    || accounts_encoding == UiAccountEncoding::Base58
                {
                    return Err(Error::invalid_params("base58 encoding not supported"));
                }

                if config_accounts.addresses.len() > post_simulation_accounts.len() {
                    return Err(Error::invalid_params(format!(
                        "Too many accounts provided; max {}",
                        post_simulation_accounts.len()
                    )));
                }

                let mut accounts = vec![];
                for address_str in config_accounts.addresses {
                    let address = verify_pubkey(&address_str)?;
                    accounts.push(if result.is_err() {
                        None
                    } else {
                        (0..transaction.message.account_keys.len())
                            .position(|i| {
                                post_simulation_accounts
                                    .get(i)
                                    .map(|(key, _account)| *key == address)
                                    .unwrap_or(false)
                            })
                            .map(|i| {
                                UiAccount::encode(
                                    &address,
                                    &post_simulation_accounts[i].1,
                                    accounts_encoding,
                                    None,
                                    None,
                                )
                            })
                    });
                }
                Some(accounts)
            } else {
                None
            };

            Ok(new_response(
                bank,
                RpcSimulateTransactionResult {
                    err: result.err(),
                    logs: Some(logs),
                    accounts,
                    units_consumed: Some(units_consumed),
                },
            ))
        }

        fn minimum_ledger_slot(&self, meta: Self::Metadata) -> Result<Slot> {
            debug!("minimum_ledger_slot rpc request received");
            meta.minimum_ledger_slot()
        }

        fn get_block(
            &self,
            meta: Self::Metadata,
            slot: Slot,
            config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
        ) -> BoxFuture<Result<Option<UiConfirmedBlock>>> {
            debug!("get_block rpc request received: {:?}", slot);
            Box::pin(async move { meta.get_block(slot, config).await })
        }

        fn get_blocks(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            config: Option<RpcBlocksConfigWrapper>,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>> {
            let (end_slot, maybe_commitment) =
                config.map(|config| config.unzip()).unwrap_or_default();
            debug!(
                "get_blocks rpc request received: {}-{:?}",
                start_slot, end_slot
            );
            Box::pin(async move {
                meta.get_blocks(start_slot, end_slot, commitment.or(maybe_commitment))
                    .await
            })
        }

        fn get_blocks_with_limit(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            limit: usize,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>> {
            debug!(
                "get_blocks_with_limit rpc request received: {}-{}",
                start_slot, limit,
            );
            Box::pin(async move {
                meta.get_blocks_with_limit(start_slot, limit, commitment)
                    .await
            })
        }

        fn get_block_time(
            &self,
            meta: Self::Metadata,
            slot: Slot,
        ) -> BoxFuture<Result<Option<UnixTimestamp>>> {
            Box::pin(async move { meta.get_block_time(slot).await })
        }

        fn get_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
        ) -> BoxFuture<Result<Option<EncodedConfirmedTransaction>>> {
            debug!("get_transaction rpc request received: {:?}", signature_str);
            let signature = verify_signature(&signature_str);
            if let Err(err) = signature {
                return Box::pin(future::err(err));
            }
            Box::pin(async move { meta.get_transaction(signature.unwrap(), config).await })
        }

        fn get_signatures_for_address(
            &self,
            meta: Self::Metadata,
            address: String,
            config: Option<RpcSignaturesForAddressConfig>,
        ) -> BoxFuture<Result<Vec<RpcConfirmedTransactionStatusWithSignature>>> {
            let config = config.unwrap_or_default();
            let commitment = config.commitment;
            let verification = verify_and_parse_signatures_for_address_params(
                address,
                config.before,
                config.until,
                config.limit,
            );

            match verification {
                Err(err) => Box::pin(future::err(err)),
                Ok((address, before, until, limit)) => Box::pin(async move {
                    meta.get_signatures_for_address(address, before, until, limit, commitment)
                        .await
                }),
            }
        }

        fn get_first_available_block(&self, meta: Self::Metadata) -> BoxFuture<Result<Slot>> {
            debug!("get_first_available_block rpc request received");
            Box::pin(async move { Ok(meta.get_first_available_block().await) })
        }

        fn get_inflation_reward(
            &self,
            meta: Self::Metadata,
            address_strs: Vec<String>,
            config: Option<RpcEpochConfig>,
        ) -> BoxFuture<Result<Vec<Option<RpcInflationReward>>>> {
            debug!(
                "get_inflation_reward rpc request received: {:?}",
                address_strs.len()
            );

            let mut addresses: Vec<Pubkey> = vec![];
            for address_str in address_strs {
                match verify_pubkey(&address_str) {
                    Ok(pubkey) => {
                        addresses.push(pubkey);
                    }
                    Err(err) => return Box::pin(future::err(err)),
                }
            }

            Box::pin(async move { meta.get_inflation_reward(addresses, config).await })
        }
    }
}

// Deprecated RPC methods, collected for easy deactivation and removal in v1.8
pub mod rpc_deprecated_v1_7 {
    #![allow(deprecated)]
    use super::*;
    #[rpc]
    pub trait DeprecatedV1_7 {
        type Metadata;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedBlock")]
        fn get_confirmed_block(
            &self,
            meta: Self::Metadata,
            slot: Slot,
            config: Option<RpcEncodingConfigWrapper<RpcConfirmedBlockConfig>>,
        ) -> BoxFuture<Result<Option<UiConfirmedBlock>>>;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedBlocks")]
        fn get_confirmed_blocks(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            config: Option<RpcConfirmedBlocksConfigWrapper>,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>>;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedBlocksWithLimit")]
        fn get_confirmed_blocks_with_limit(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            limit: usize,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>>;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedTransaction")]
        fn get_confirmed_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            config: Option<RpcEncodingConfigWrapper<RpcConfirmedTransactionConfig>>,
        ) -> BoxFuture<Result<Option<EncodedConfirmedTransaction>>>;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedSignaturesForAddress2")]
        fn get_confirmed_signatures_for_address2(
            &self,
            meta: Self::Metadata,
            address: String,
            config: Option<RpcGetConfirmedSignaturesForAddress2Config>,
        ) -> BoxFuture<Result<Vec<RpcConfirmedTransactionStatusWithSignature>>>;
    }

    pub struct DeprecatedV1_7Impl;
    impl DeprecatedV1_7 for DeprecatedV1_7Impl {
        type Metadata = JsonRpcRequestProcessor;

        fn get_confirmed_block(
            &self,
            meta: Self::Metadata,
            slot: Slot,
            config: Option<RpcEncodingConfigWrapper<RpcConfirmedBlockConfig>>,
        ) -> BoxFuture<Result<Option<UiConfirmedBlock>>> {
            debug!("get_confirmed_block rpc request received: {:?}", slot);
            Box::pin(async move {
                meta.get_block(slot, config.map(|config| config.convert()))
                    .await
            })
        }

        fn get_confirmed_blocks(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            config: Option<RpcConfirmedBlocksConfigWrapper>,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>> {
            let (end_slot, maybe_commitment) =
                config.map(|config| config.unzip()).unwrap_or_default();
            debug!(
                "get_confirmed_blocks rpc request received: {}-{:?}",
                start_slot, end_slot
            );
            Box::pin(async move {
                meta.get_blocks(start_slot, end_slot, commitment.or(maybe_commitment))
                    .await
            })
        }

        fn get_confirmed_blocks_with_limit(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            limit: usize,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>> {
            debug!(
                "get_confirmed_blocks_with_limit rpc request received: {}-{}",
                start_slot, limit,
            );
            Box::pin(async move {
                meta.get_blocks_with_limit(start_slot, limit, commitment)
                    .await
            })
        }

        fn get_confirmed_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            config: Option<RpcEncodingConfigWrapper<RpcConfirmedTransactionConfig>>,
        ) -> BoxFuture<Result<Option<EncodedConfirmedTransaction>>> {
            debug!(
                "get_confirmed_transaction rpc request received: {:?}",
                signature_str
            );
            let signature = verify_signature(&signature_str);
            if let Err(err) = signature {
                return Box::pin(future::err(err));
            }
            Box::pin(async move {
                meta.get_transaction(signature.unwrap(), config.map(|config| config.convert()))
                    .await
            })
        }

        fn get_confirmed_signatures_for_address2(
            &self,
            meta: Self::Metadata,
            address: String,
            config: Option<RpcGetConfirmedSignaturesForAddress2Config>,
        ) -> BoxFuture<Result<Vec<RpcConfirmedTransactionStatusWithSignature>>> {
            let config = config.unwrap_or_default();
            let commitment = config.commitment;
            let verification = verify_and_parse_signatures_for_address_params(
                address,
                config.before,
                config.until,
                config.limit,
            );

            match verification {
                Err(err) => Box::pin(future::err(err)),
                Ok((address, before, until, limit)) => Box::pin(async move {
                    meta.get_signatures_for_address(address, before, until, limit, commitment)
                        .await
                }),
            }
        }
    }
}

// Obsolete RPC methods, collected for easy deactivation and removal
pub mod rpc_obsolete_v1_7 {
    use super::*;
    #[rpc]
    pub trait ObsoleteV1_7 {
        type Metadata;

        // DEPRECATED
        #[rpc(meta, name = "confirmTransaction")]
        fn confirm_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<bool>>;

        // DEPRECATED
        #[rpc(meta, name = "getSignatureStatus")]
        fn get_signature_status(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<Option<transaction::Result<()>>>;

        // DEPRECATED (used by Trust Wallet)
        #[rpc(meta, name = "getSignatureConfirmation")]
        fn get_signature_confirmation(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<Option<RpcSignatureConfirmation>>;

        // DEPRECATED
        #[rpc(meta, name = "getTotalSupply")]
        fn get_total_supply(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<u64>;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedSignaturesForAddress")]
        fn get_confirmed_signatures_for_address(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            start_slot: Slot,
            end_slot: Slot,
        ) -> Result<Vec<String>>;
    }

    pub struct ObsoleteV1_7Impl;
    impl ObsoleteV1_7 for ObsoleteV1_7Impl {
        type Metadata = JsonRpcRequestProcessor;

        fn confirm_transaction(
            &self,
            meta: Self::Metadata,
            id: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<bool>> {
            debug!("confirm_transaction rpc request received: {:?}", id);
            let signature = verify_signature(&id)?;
            Ok(meta.confirm_transaction(&signature, commitment))
        }

        fn get_signature_status(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<Option<transaction::Result<()>>> {
            debug!(
                "get_signature_status rpc request received: {:?}",
                signature_str
            );
            let signature = verify_signature(&signature_str)?;
            Ok(meta.get_signature_status(signature, commitment))
        }

        fn get_signature_confirmation(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<Option<RpcSignatureConfirmation>> {
            debug!(
                "get_signature_confirmation rpc request received: {:?}",
                signature_str
            );
            let signature = verify_signature(&signature_str)?;
            Ok(meta.get_signature_confirmation_status(signature, commitment))
        }

        fn get_total_supply(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<u64> {
            debug!("get_total_supply rpc request received");
            Ok(meta.get_total_supply(commitment))
        }

        fn get_confirmed_signatures_for_address(
            &self,
            meta: Self::Metadata,
            pubkey_str: String,
            start_slot: Slot,
            end_slot: Slot,
        ) -> Result<Vec<String>> {
            debug!(
                "get_confirmed_signatures_for_address rpc request received: {:?} {:?}-{:?}",
                pubkey_str, start_slot, end_slot
            );
            let pubkey = verify_pubkey(&pubkey_str)?;
            if end_slot < start_slot {
                return Err(Error::invalid_params(format!(
                    "start_slot {} must be less than or equal to end_slot {}",
                    start_slot, end_slot
                )));
            }
            if end_slot - start_slot > MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS_SLOT_RANGE {
                return Err(Error::invalid_params(format!(
                    "Slot range too large; max {}",
                    MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS_SLOT_RANGE
                )));
            }
            Ok(meta
                .get_confirmed_signatures_for_address(pubkey, start_slot, end_slot)
                .iter()
                .map(|signature| signature.to_string())
                .collect())
        }
    }
}

const WORST_CASE_BASE58_TX: usize = 1683; // Golden, bump if PACKET_DATA_SIZE changes
const WORST_CASE_BASE64_TX: usize = 1644; // Golden, bump if PACKET_DATA_SIZE changes
fn deserialize_transaction(
    encoded_transaction: String,
    encoding: UiTransactionEncoding,
) -> Result<(Vec<u8>, Transaction)> {
    let wire_transaction = match encoding {
        UiTransactionEncoding::Base58 => {
            inc_new_counter_info!("rpc-base58_encoded_tx", 1);
            if encoded_transaction.len() > WORST_CASE_BASE58_TX {
                return Err(Error::invalid_params(format!(
                    "encoded transaction too large: {} bytes (max: encoded/raw {}/{})",
                    encoded_transaction.len(),
                    WORST_CASE_BASE58_TX,
                    PACKET_DATA_SIZE,
                )));
            }
            bs58::decode(encoded_transaction)
                .into_vec()
                .map_err(|e| Error::invalid_params(format!("{:?}", e)))?
        }
        UiTransactionEncoding::Base64 => {
            inc_new_counter_info!("rpc-base64_encoded_tx", 1);
            if encoded_transaction.len() > WORST_CASE_BASE64_TX {
                return Err(Error::invalid_params(format!(
                    "encoded transaction too large: {} bytes (max: encoded/raw {}/{})",
                    encoded_transaction.len(),
                    WORST_CASE_BASE64_TX,
                    PACKET_DATA_SIZE,
                )));
            }
            base64::decode(encoded_transaction)
                .map_err(|e| Error::invalid_params(format!("{:?}", e)))?
        }
        _ => {
            return Err(Error::invalid_params(format!(
                "unsupported transaction encoding: {}. Supported encodings: base58, base64",
                encoding
            )))
        }
    };
    if wire_transaction.len() > PACKET_DATA_SIZE {
        let err = format!(
            "transaction too large: {} bytes (max: {} bytes)",
            wire_transaction.len(),
            PACKET_DATA_SIZE
        );
        info!("{}", err);
        return Err(Error::invalid_params(&err));
    }
    bincode::options()
        .with_limit(PACKET_DATA_SIZE as u64)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(&wire_transaction[..])
        .map_err(|err| {
            info!("transaction deserialize error: {:?}", err);
            Error::invalid_params(&err.to_string())
        })
        .and_then(|transaction: Transaction| {
            if let Err(err) = transaction.sanitize() {
                Err(Error::invalid_params(format!(
                    "invalid transaction: {}",
                    err
                )))
            } else {
                Ok(transaction)
            }
        })
        .map(|transaction| (wire_transaction, transaction))
}

pub(crate) fn create_validator_exit(exit: &Arc<AtomicBool>) -> Arc<RwLock<Exit>> {
    let mut validator_exit = Exit::default();
    let exit_ = exit.clone();
    validator_exit.register_exit(Box::new(move || exit_.store(true, Ordering::Relaxed)));
    Arc::new(RwLock::new(validator_exit))
}

// Used for tests
pub fn create_test_transactions_and_populate_blockstore(
    keypairs: Vec<&Keypair>,
    previous_slot: Slot,
    bank: Arc<Bank>,
    blockstore: Arc<Blockstore>,
    max_complete_transaction_status_slot: Arc<AtomicU64>,
) -> Vec<Signature> {
    let mint_keypair = keypairs[0];
    let keypair1 = keypairs[1];
    let keypair2 = keypairs[2];
    let keypair3 = keypairs[3];
    let slot = bank.slot();
    let blockhash = bank.confirmed_last_blockhash().0;

    // Generate transactions for processing
    // Successful transaction
    let success_tx =
        solana_sdk::system_transaction::transfer(mint_keypair, &keypair1.pubkey(), 2, blockhash);
    let success_signature = success_tx.signatures[0];
    let entry_1 = solana_entry::entry::next_entry(&blockhash, 1, vec![success_tx]);
    // Failed transaction, InstructionError
    let ix_error_tx =
        solana_sdk::system_transaction::transfer(keypair2, &keypair3.pubkey(), 10, blockhash);
    let ix_error_signature = ix_error_tx.signatures[0];
    let entry_2 = solana_entry::entry::next_entry(&entry_1.hash, 1, vec![ix_error_tx]);
    // Failed transaction
    let fail_tx = solana_sdk::system_transaction::transfer(
        mint_keypair,
        &keypair2.pubkey(),
        2,
        Hash::default(),
    );
    let entry_3 = solana_entry::entry::next_entry(&entry_2.hash, 1, vec![fail_tx]);
    let mut entries = vec![entry_1, entry_2, entry_3];

    let shreds = solana_ledger::blockstore::entries_to_test_shreds(
        entries.clone(),
        slot,
        previous_slot,
        true,
        0,
    );
    blockstore.insert_shreds(shreds, None, false).unwrap();
    blockstore.set_roots(std::iter::once(&slot)).unwrap();

    let (transaction_status_sender, transaction_status_receiver) = crossbeam_channel::unbounded();
    let (replay_vote_sender, _replay_vote_receiver) = crossbeam_channel::unbounded();
    let transaction_status_service =
        crate::transaction_status_service::TransactionStatusService::new(
            transaction_status_receiver,
            max_complete_transaction_status_slot,
            blockstore,
            &Arc::new(AtomicBool::new(false)),
        );

    // Check that process_entries successfully writes can_commit transactions statuses, and
    // that they are matched properly by get_rooted_block
    let _result = solana_ledger::blockstore_processor::process_entries(
        &bank,
        &mut entries,
        true,
        Some(
            &solana_ledger::blockstore_processor::TransactionStatusSender {
                sender: transaction_status_sender,
                enable_cpi_and_log_storage: false,
            },
        ),
        Some(&replay_vote_sender),
    );

    transaction_status_service.join().unwrap();

    vec![success_signature, ix_error_signature]
}

#[cfg(test)]
pub mod tests {
    use {
        super::{rpc_accounts::*, rpc_bank::*, rpc_full::*, rpc_minimal::*, *},
        crate::{
            optimistically_confirmed_bank_tracker::{
                BankNotification, OptimisticallyConfirmedBankTracker,
            },
            rpc_subscriptions::RpcSubscriptions,
        },
        bincode::deserialize,
        jsonrpc_core::{futures, ErrorCode, MetaIoHandler, Output, Response, Value},
        jsonrpc_core_client::transports::local,
        solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes},
        solana_gossip::{contact_info::ContactInfo, socketaddr},
        solana_ledger::{
            blockstore_meta::PerfSample,
            blockstore_processor::fill_blockstore_slot_with_ticks,
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
        },
        solana_runtime::{
            accounts_background_service::AbsRequestSender, commitment::BlockCommitment,
            non_circulating_supply::non_circulating_accounts,
        },
        solana_sdk::{
            account::Account,
            clock::MAX_RECENT_BLOCKHASHES,
            fee_calculator::DEFAULT_BURN_PERCENT,
            hash::{hash, Hash},
            instruction::InstructionError,
            message::Message,
            nonce, rpc_port,
            signature::{Keypair, Signer},
            system_program, system_transaction,
            timing::slot_duration_from_slots_per_year,
            transaction::{self, TransactionError},
        },
        solana_transaction_status::{
            EncodedConfirmedBlock, EncodedTransaction, EncodedTransactionWithStatusMeta,
            TransactionDetails, UiMessage,
        },
        solana_vote_program::{
            vote_instruction,
            vote_state::{BlockTimestamp, Vote, VoteInit, VoteStateVersions, MAX_LOCKOUT_HISTORY},
        },
        spl_token_v2_0::{
            solana_program::{program_option::COption, pubkey::Pubkey as SplTokenPubkey},
            state::AccountState as TokenAccountState,
            state::Mint,
        },
        std::collections::HashMap,
    };

    const TEST_MINT_LAMPORTS: u64 = 1_000_000;
    const TEST_SLOTS_PER_EPOCH: u64 = DELINQUENT_VALIDATOR_SLOT_DISTANCE + 1;

    struct RpcHandler {
        io: MetaIoHandler<JsonRpcRequestProcessor>,
        meta: JsonRpcRequestProcessor,
        bank: Arc<Bank>,
        bank_forks: Arc<RwLock<BankForks>>,
        blockhash: Hash,
        alice: Keypair,
        leader_pubkey: Pubkey,
        leader_vote_keypair: Arc<Keypair>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        confirmed_block_signatures: Vec<Signature>,
    }

    fn start_rpc_handler_with_tx(pubkey: &Pubkey) -> RpcHandler {
        start_rpc_handler_with_tx_and_blockstore(pubkey, vec![])
    }

    fn start_rpc_handler_with_tx_and_blockstore(
        pubkey: &Pubkey,
        blockstore_roots: Vec<Slot>,
    ) -> RpcHandler {
        let (bank_forks, alice, leader_vote_keypair) = new_bank_forks();
        let bank = bank_forks.read().unwrap().working_bank();

        let vote_pubkey = leader_vote_keypair.pubkey();
        let mut vote_account = bank.get_account(&vote_pubkey).unwrap_or_default();
        let mut vote_state = VoteState::from(&vote_account).unwrap_or_default();
        vote_state.last_timestamp = BlockTimestamp {
            slot: bank.slot(),
            timestamp: bank.clock().unix_timestamp,
        };
        let versioned = VoteStateVersions::new_current(vote_state);
        VoteState::to(&versioned, &mut vote_account).unwrap();
        bank.store_account(&vote_pubkey, &vote_account);

        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&ledger_path).unwrap();
        let blockstore = Arc::new(blockstore);

        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let keypair3 = Keypair::new();
        bank.transfer(4, &alice, &keypair2.pubkey()).unwrap();
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::new(blockstore.max_root()));
        let confirmed_block_signatures = create_test_transactions_and_populate_blockstore(
            vec![&alice, &keypair1, &keypair2, &keypair3],
            0,
            bank.clone(),
            blockstore.clone(),
            max_complete_transaction_status_slot.clone(),
        );

        let mut commitment_slot0 = BlockCommitment::default();
        commitment_slot0.increase_confirmation_stake(2, 9);
        let mut commitment_slot1 = BlockCommitment::default();
        commitment_slot1.increase_confirmation_stake(1, 9);
        let mut block_commitment: HashMap<u64, BlockCommitment> = HashMap::new();
        block_commitment.entry(0).or_insert(commitment_slot0);
        block_commitment.entry(1).or_insert(commitment_slot1);
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::new(
            block_commitment,
            10,
            CommitmentSlots::new_from_slot(bank.slot()),
        )));

        let mut roots = blockstore_roots;
        if !roots.is_empty() {
            roots.retain(|&x| x > 0);
            let mut parent_bank = bank;
            for (i, root) in roots.iter().enumerate() {
                let new_bank =
                    Bank::new_from_parent(&parent_bank, parent_bank.collector_id(), *root);
                parent_bank = bank_forks.write().unwrap().insert(new_bank);
                let parent = if i > 0 { roots[i - 1] } else { 0 };
                fill_blockstore_slot_with_ticks(&blockstore, 5, *root, parent, Hash::default());
            }
            blockstore.set_roots(roots.iter()).unwrap();
            let new_bank = Bank::new_from_parent(
                &parent_bank,
                parent_bank.collector_id(),
                roots.iter().max().unwrap() + 1,
            );
            bank_forks.write().unwrap().insert(new_bank);

            for root in roots.iter() {
                bank_forks
                    .write()
                    .unwrap()
                    .set_root(*root, &AbsRequestSender::default(), Some(0));
                let mut stakes = HashMap::new();
                stakes.insert(
                    leader_vote_keypair.pubkey(),
                    (1, AccountSharedData::default()),
                );
                let block_time = bank_forks
                    .read()
                    .unwrap()
                    .get(*root)
                    .unwrap()
                    .clock()
                    .unix_timestamp;
                blockstore.cache_block_time(*root, block_time).unwrap();
            }
        }

        let bank = bank_forks.read().unwrap().working_bank();

        let leader_pubkey = *bank.collector_id();
        let exit = Arc::new(AtomicBool::new(false));
        let validator_exit = create_validator_exit(&exit);

        let blockhash = bank.confirmed_last_blockhash().0;
        let tx = system_transaction::transfer(&alice, pubkey, 20, blockhash);
        bank.process_transaction(&tx).expect("process transaction");
        let tx =
            system_transaction::transfer(&alice, &non_circulating_accounts()[0], 20, blockhash);
        bank.process_transaction(&tx).expect("process transaction");

        let tx = system_transaction::transfer(&alice, pubkey, std::u64::MAX, blockhash);
        let _ = bank.process_transaction(&tx);

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo {
                id: alice.pubkey(),
                ..ContactInfo::default()
            },
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        let tpu_address = cluster_info.my_contact_info().tpu;

        cluster_info.insert_info(ContactInfo::new_with_pubkey_socketaddr(
            &leader_pubkey,
            &socketaddr!("127.0.0.1:1234"),
        ));

        let sample1 = PerfSample {
            num_slots: 1,
            num_transactions: 4,
            sample_period_secs: 60,
        };

        blockstore
            .write_perf_sample(0, &sample1)
            .expect("write to blockstore");

        let max_slots = Arc::new(MaxSlots::default());
        max_slots.retransmit.store(42, Ordering::Relaxed);
        max_slots.shred_insert.store(43, Ordering::Relaxed);

        let (meta, receiver) = JsonRpcRequestProcessor::new(
            JsonRpcConfig {
                enable_rpc_transaction_history: true,
                ..JsonRpcConfig::default()
            },
            None,
            bank_forks.clone(),
            block_commitment_cache.clone(),
            blockstore,
            validator_exit,
            RpcHealth::stub(),
            cluster_info.clone(),
            Hash::default(),
            None,
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
            Arc::new(RwLock::new(LargestAccountsCache::new(30))),
            max_slots,
            Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            max_complete_transaction_status_slot,
        );
        SendTransactionService::new(tpu_address, &bank_forks, None, receiver, 1000, 1);

        cluster_info.insert_info(ContactInfo::new_with_pubkey_socketaddr(
            &leader_pubkey,
            &socketaddr!("127.0.0.1:1234"),
        ));

        let mut io = MetaIoHandler::default();
        io.extend_with(rpc_minimal::MinimalImpl.to_delegate());
        io.extend_with(rpc_bank::BankDataImpl.to_delegate());
        io.extend_with(rpc_accounts::AccountsDataImpl.to_delegate());
        io.extend_with(rpc_full::FullImpl.to_delegate());
        RpcHandler {
            io,
            meta,
            bank,
            bank_forks,
            blockhash,
            alice,
            leader_pubkey,
            leader_vote_keypair,
            block_commitment_cache,
            confirmed_block_signatures,
        }
    }

    #[test]
    fn test_rpc_request_processor_new() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let genesis = create_genesis_config(100);
        let bank = Arc::new(Bank::new(&genesis.genesis_config));
        bank.transfer(20, &genesis.mint_keypair, &bob_pubkey)
            .unwrap();
        let request_processor =
            JsonRpcRequestProcessor::new_from_bank(&bank, SocketAddrSpace::Unspecified);
        assert_eq!(request_processor.get_transaction_count(None), 1);
    }

    #[test]
    fn test_rpc_get_balance() {
        let genesis = create_genesis_config(20);
        let mint_pubkey = genesis.mint_keypair.pubkey();
        let bank = Arc::new(Bank::new(&genesis.genesis_config));
        let meta = JsonRpcRequestProcessor::new_from_bank(&bank, SocketAddrSpace::Unspecified);

        let mut io = MetaIoHandler::default();
        io.extend_with(rpc_minimal::MinimalImpl.to_delegate());

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["{}"]}}"#,
            mint_pubkey
        );
        let res = io.handle_request_sync(&req, meta);
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "context":{"slot":0},
                "value":20,
                },
            "id": 1,
        });
        let result = serde_json::from_str::<Value>(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_balance_via_client() {
        let genesis = create_genesis_config(20);
        let mint_pubkey = genesis.mint_keypair.pubkey();
        let bank = Arc::new(Bank::new(&genesis.genesis_config));
        let meta = JsonRpcRequestProcessor::new_from_bank(&bank, SocketAddrSpace::Unspecified);

        let mut io = MetaIoHandler::default();
        io.extend_with(rpc_minimal::MinimalImpl.to_delegate());

        async fn use_client(client: rpc_minimal::gen_client::Client, mint_pubkey: Pubkey) -> u64 {
            client
                .get_balance(mint_pubkey.to_string(), None)
                .await
                .unwrap()
                .value
        }

        let fut = async {
            let (client, server) =
                local::connect_with_metadata::<rpc_minimal::gen_client::Client, _, _>(&io, meta);
            let client = use_client(client, mint_pubkey);

            futures::join!(client, server)
        };
        let (response, _) = futures::executor::block_on(fut);
        assert_eq!(response, 20);
    }

    #[test]
    fn test_rpc_get_cluster_nodes() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io,
            meta,
            leader_pubkey,
            ..
        } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getClusterNodes"}"#;

        let res = io.handle_request_sync(req, meta);
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        let expected = format!(
            r#"{{"jsonrpc":"2.0","result":[{{"pubkey": "{}", "gossip": "127.0.0.1:1235", "shredVersion": 0, "tpu": "127.0.0.1:1234", "rpc": "127.0.0.1:{}", "version": null, "featureSet": null}}],"id":1}}"#,
            leader_pubkey,
            rpc_port::DEFAULT_RPC_PORT
        );

        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_recent_performance_samples() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getRecentPerformanceSamples"}"#;

        let res = io.handle_request_sync(req, meta);
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        let expected = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                {
                    "slot": 0,
                    "numSlots": 1,
                    "numTransactions": 4,
                    "samplePeriodSecs": 60
                }
            ],
        });

        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_recent_performance_samples_invalid_limit() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let req =
            r#"{"jsonrpc":"2.0","id":1,"method":"getRecentPerformanceSamples","params":[10000]}"#;

        let res = io.handle_request_sync(req, meta);
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        let expected = json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32602,
                "message": "Invalid limit; max 720"
            },
            "id": 1
        });

        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_slot_leader() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io,
            meta,
            leader_pubkey,
            ..
        } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getSlotLeader"}"#;
        let res = io.handle_request_sync(req, meta);
        let expected = format!(r#"{{"jsonrpc":"2.0","result":"{}","id":1}}"#, leader_pubkey);
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_tx_count() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let genesis = create_genesis_config(10);
        let bank = Arc::new(Bank::new(&genesis.genesis_config));
        // Add 4 transactions
        bank.transfer(1, &genesis.mint_keypair, &bob_pubkey)
            .unwrap();
        bank.transfer(2, &genesis.mint_keypair, &bob_pubkey)
            .unwrap();
        bank.transfer(3, &genesis.mint_keypair, &bob_pubkey)
            .unwrap();
        bank.transfer(4, &genesis.mint_keypair, &bob_pubkey)
            .unwrap();

        let meta = JsonRpcRequestProcessor::new_from_bank(&bank, SocketAddrSpace::Unspecified);

        let mut io = MetaIoHandler::default();
        io.extend_with(rpc_minimal::MinimalImpl.to_delegate());

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getTransactionCount"}"#;
        let res = io.handle_request_sync(req, meta);
        let expected = r#"{"jsonrpc":"2.0","result":4,"id":1}"#;
        let expected: Response =
            serde_json::from_str(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_minimum_ledger_slot() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"minimumLedgerSlot"}"#;
        let res = io.handle_request_sync(req, meta);
        let expected = r#"{"jsonrpc":"2.0","result":0,"id":1}"#;
        let expected: Response =
            serde_json::from_str(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_get_supply() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, .. } = start_rpc_handler_with_tx(&bob_pubkey);
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getSupply"}"#;
        let res = io.handle_request_sync(req, meta);
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let supply: RpcSupply = serde_json::from_value(json["result"]["value"].clone())
            .expect("actual response deserialization");
        assert_eq!(supply.non_circulating, 20);
        assert!(supply.circulating >= TEST_MINT_LAMPORTS);
        assert!(supply.total >= TEST_MINT_LAMPORTS + 20);
        let expected_accounts: Vec<String> = non_circulating_accounts()
            .iter()
            .map(|pubkey| pubkey.to_string())
            .collect();
        assert_eq!(
            supply.non_circulating_accounts.len(),
            expected_accounts.len()
        );
        for address in supply.non_circulating_accounts {
            assert!(expected_accounts.contains(&address));
        }
    }

    #[test]
    fn test_get_largest_accounts() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io, meta, alice, ..
        } = start_rpc_handler_with_tx(&bob_pubkey);
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getLargestAccounts"}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let largest_accounts: Vec<RpcAccountBalance> =
            serde_json::from_value(json["result"]["value"].clone())
                .expect("actual response deserialization");
        assert_eq!(largest_accounts.len(), 20);

        // Get Alice balance
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["{}"]}}"#,
            alice.pubkey()
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let alice_balance: u64 = serde_json::from_value(json["result"]["value"].clone())
            .expect("actual response deserialization");
        assert!(largest_accounts.contains(&RpcAccountBalance {
            address: alice.pubkey().to_string(),
            lamports: alice_balance,
        }));

        // Get Bob balance
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["{}"]}}"#,
            bob_pubkey
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let bob_balance: u64 = serde_json::from_value(json["result"]["value"].clone())
            .expect("actual response deserialization");
        assert!(largest_accounts.contains(&RpcAccountBalance {
            address: bob_pubkey.to_string(),
            lamports: bob_balance,
        }));

        // Test Circulating/NonCirculating Filter
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getLargestAccounts","params":[{"filter":"circulating"}]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let largest_accounts: Vec<RpcAccountBalance> =
            serde_json::from_value(json["result"]["value"].clone())
                .expect("actual response deserialization");
        assert_eq!(largest_accounts.len(), 20);
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getLargestAccounts","params":[{"filter":"nonCirculating"}]}"#;
        let res = io.handle_request_sync(req, meta);
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let largest_accounts: Vec<RpcAccountBalance> =
            serde_json::from_value(json["result"]["value"].clone())
                .expect("actual response deserialization");
        assert_eq!(largest_accounts.len(), 1);
    }

    #[test]
    fn test_rpc_get_minimum_balance_for_rent_exemption() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let data_len = 50;
        let RpcHandler { io, meta, bank, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getMinimumBalanceForRentExemption","params":[{}]}}"#,
            data_len
        );
        let rep = io.handle_request_sync(&req, meta);
        let res: Response = serde_json::from_str(&rep.expect("actual response"))
            .expect("actual response deserialization");
        let minimum_balance: u64 = if let Response::Single(res) = res {
            if let Output::Success(res) = res {
                if let Value::Number(num) = res.result {
                    num.as_u64().unwrap()
                } else {
                    panic!("Expected number");
                }
            } else {
                panic!("Expected success");
            }
        } else {
            panic!("Expected single response");
        };
        assert_eq!(
            minimum_balance,
            bank.get_minimum_balance_for_rent_exemption(data_len)
        );
    }

    #[test]
    fn test_rpc_get_inflation() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, bank, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getInflationGovernor"}"#;
        let rep = io.handle_request_sync(req, meta.clone());
        let res: Response = serde_json::from_str(&rep.expect("actual response"))
            .expect("actual response deserialization");
        let inflation_governor: RpcInflationGovernor = if let Response::Single(res) = res {
            if let Output::Success(res) = res {
                serde_json::from_value(res.result).unwrap()
            } else {
                panic!("Expected success");
            }
        } else {
            panic!("Expected single response");
        };
        let expected_inflation_governor: RpcInflationGovernor = bank.inflation().into();
        assert_eq!(inflation_governor, expected_inflation_governor);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getInflationRate"}"#; // Queries current epoch
        let rep = io.handle_request_sync(req, meta);
        let res: Response = serde_json::from_str(&rep.expect("actual response"))
            .expect("actual response deserialization");
        let inflation_rate: RpcInflationRate = if let Response::Single(res) = res {
            if let Output::Success(res) = res {
                serde_json::from_value(res.result).unwrap()
            } else {
                panic!("Expected success");
            }
        } else {
            panic!("Expected single response");
        };
        let inflation = bank.inflation();
        let epoch = bank.epoch();
        let slot_in_year = bank.slot_in_year_for_inflation();
        let expected_inflation_rate = RpcInflationRate {
            total: inflation.total(slot_in_year),
            validator: inflation.validator(slot_in_year),
            foundation: inflation.foundation(slot_in_year),
            epoch,
        };
        assert_eq!(inflation_rate, expected_inflation_rate);
    }

    #[test]
    fn test_rpc_get_epoch_schedule() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, bank, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getEpochSchedule"}"#;
        let rep = io.handle_request_sync(req, meta);
        let res: Response = serde_json::from_str(&rep.expect("actual response"))
            .expect("actual response deserialization");

        let epoch_schedule: EpochSchedule = if let Response::Single(res) = res {
            if let Output::Success(res) = res {
                serde_json::from_value(res.result).unwrap()
            } else {
                panic!("Expected success");
            }
        } else {
            panic!("Expected single response");
        };
        assert_eq!(epoch_schedule, *bank.epoch_schedule());
    }

    #[test]
    fn test_rpc_get_leader_schedule() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, bank, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        for req in [
            r#"{"jsonrpc":"2.0","id":1,"method":"getLeaderSchedule", "params": [0]}"#,
            r#"{"jsonrpc":"2.0","id":1,"method":"getLeaderSchedule"}"#,
            &format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"getLeaderSchedule", "params": [null, {{ "identity": "{}" }}]}}"#,
                bank.collector_id().to_string()
            ),
            &format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"getLeaderSchedule", "params": [{{ "identity": "{}" }}]}}"#,
                bank.collector_id().to_string()
            ),
        ]
        .iter()
        {
            let rep = io.handle_request_sync(req, meta.clone());
            let res: Response = serde_json::from_str(&rep.expect("actual response"))
                .expect("actual response deserialization");

            let schedule: Option<RpcLeaderSchedule> = if let Response::Single(res) = res {
                if let Output::Success(res) = res {
                    serde_json::from_value(res.result).unwrap()
                } else {
                    panic!("Expected success for {}", req);
                }
            } else {
                panic!("Expected single response");
            };
            let schedule = schedule.expect("leader schedule");

            let bob_schedule = schedule
                .get(&bank.collector_id().to_string())
                .expect("leader not in the leader schedule");

            assert_eq!(
                bob_schedule.len(),
                solana_ledger::leader_schedule_utils::leader_schedule(bank.epoch(), &bank)
                    .unwrap()
                    .get_slot_leaders()
                    .len()
            );
        }

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getLeaderSchedule", "params": [42424242]}"#;
        let rep = io.handle_request_sync(req, meta.clone());
        let res: Response = serde_json::from_str(&rep.expect("actual response"))
            .expect("actual response deserialization");

        let schedule: Option<RpcLeaderSchedule> = if let Response::Single(res) = res {
            if let Output::Success(res) = res {
                serde_json::from_value(res.result).unwrap()
            } else {
                panic!("Expected success");
            }
        } else {
            panic!("Expected single response");
        };
        assert_eq!(schedule, None);

        // `bob` is not in the leader schedule, look for an empty response
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getLeaderSchedule", "params": [{{ "identity": "{}"}}]}}"#,
            bob_pubkey
        );

        let rep = io.handle_request_sync(&req, meta);
        let res: Response = serde_json::from_str(&rep.expect("actual response"))
            .expect("actual response deserialization");

        let schedule: Option<RpcLeaderSchedule> = if let Response::Single(res) = res {
            if let Output::Success(res) = res {
                serde_json::from_value(res.result).unwrap()
            } else {
                panic!("Expected success");
            }
        } else {
            panic!("Expected single response");
        };
        assert_eq!(schedule, Some(HashMap::default()));
    }

    #[test]
    fn test_rpc_get_slot_leaders() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, bank, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        // Test that slot leaders will be returned across epochs
        let query_start = 0;
        let query_limit = 2 * bank.epoch_schedule().slots_per_epoch;

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSlotLeaders", "params": [{}, {}]}}"#,
            query_start, query_limit
        );
        let rep = io.handle_request_sync(&req, meta.clone());
        let res: Response = serde_json::from_str(&rep.expect("actual response"))
            .expect("actual response deserialization");

        let slot_leaders: Vec<String> = if let Response::Single(res) = res {
            if let Output::Success(res) = res {
                serde_json::from_value(res.result).unwrap()
            } else {
                panic!("Expected success for {} but received: {:?}", req, res);
            }
        } else {
            panic!("Expected single response");
        };

        assert_eq!(slot_leaders.len(), query_limit as usize);

        // Test that invalid limit returns an error
        let query_start = 0;
        let query_limit = 5001;

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSlotLeaders", "params": [{}, {}]}}"#,
            query_start, query_limit
        );
        let rep = io.handle_request_sync(&req, meta.clone());
        let res: Value = serde_json::from_str(&rep.expect("actual response"))
            .expect("actual response deserialization");
        assert!(res.get("error").is_some());

        // Test that invalid epoch returns an error
        let query_start = 2 * bank.epoch_schedule().slots_per_epoch;
        let query_limit = 10;

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSlotLeaders", "params": [{}, {}]}}"#,
            query_start, query_limit
        );
        let rep = io.handle_request_sync(&req, meta);
        let res: Value = serde_json::from_str(&rep.expect("actual response"))
            .expect("actual response deserialization");
        assert!(res.get("error").is_some());
    }

    #[test]
    fn test_rpc_get_account_info() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, bank, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{}"]}}"#,
            bob_pubkey
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "context":{"slot":0},
                "value":{
                    "owner": "11111111111111111111111111111111",
                    "lamports": 20,
                    "data": "",
                    "executable": false,
                    "rentEpoch": 0
                },
            },
            "id": 1,
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        let address = solana_sdk::pubkey::new_rand();
        let data = vec![1, 2, 3, 4, 5];
        let mut account = AccountSharedData::new(42, 5, &Pubkey::default());
        account.set_data(data.clone());
        bank.store_account(&address, &account);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{}", {{"encoding":"base64"}}]}}"#,
            address
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(
            result["result"]["value"]["data"],
            json!([base64::encode(&data), "base64"]),
        );

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{}", {{"encoding":"base64", "dataSlice": {{"length": 2, "offset": 1}}}}]}}"#,
            address
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(
            result["result"]["value"]["data"],
            json!([base64::encode(&data[1..3]), "base64"]),
        );

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{}", {{"encoding":"binary", "dataSlice": {{"length": 2, "offset": 1}}}}]}}"#,
            address
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(
            result["result"]["value"]["data"],
            bs58::encode(&data[1..3]).into_string(),
        );

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{}", {{"encoding":"jsonParsed", "dataSlice": {{"length": 2, "offset": 1}}}}]}}"#,
            address
        );
        let res = io.handle_request_sync(&req, meta);
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        result["error"].as_object().unwrap();
    }

    #[test]
    fn test_rpc_get_multiple_accounts() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, bank, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let address = Pubkey::new(&[9; 32]);
        let data = vec![1, 2, 3, 4, 5];
        let mut account = AccountSharedData::new(42, 5, &Pubkey::default());
        account.set_data(data.clone());
        bank.store_account(&address, &account);

        let non_existent_address = Pubkey::new(&[8; 32]);

        // Test 3 accounts, one non-existent, and one with data
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[["{}", "{}", "{}"]]}}"#,
            bob_pubkey, non_existent_address, address,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "context":{"slot":0},
                "value":[{
                    "owner": "11111111111111111111111111111111",
                    "lamports": 20,
                    "data": ["", "base64"],
                    "executable": false,
                    "rentEpoch": 0
                },
                null,
                {
                    "owner": "11111111111111111111111111111111",
                    "lamports": 42,
                    "data": [base64::encode(&data), "base64"],
                    "executable": false,
                    "rentEpoch": 0
                }],
            },
            "id": 1,
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Test config settings still work with multiple accounts
        let req = format!(
            r#"{{
                "jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[
                ["{}", "{}", "{}"],
                {{"encoding":"base58"}}
                ]
            }}"#,
            bob_pubkey, non_existent_address, address,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(result["result"]["value"].as_array().unwrap().len(), 3);
        assert_eq!(
            result["result"]["value"][2]["data"],
            json!([bs58::encode(&data).into_string(), "base58"]),
        );

        let req = format!(
            r#"{{
                "jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[
                ["{}", "{}", "{}"],
                {{"encoding":"base64", "dataSlice": {{"length": 2, "offset": 1}}}}
                ]
            }}"#,
            bob_pubkey, non_existent_address, address,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(result["result"]["value"].as_array().unwrap().len(), 3);
        assert_eq!(
            result["result"]["value"][2]["data"],
            json!([base64::encode(&data[1..3]), "base64"]),
        );

        let req = format!(
            r#"{{
                "jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[
                ["{}", "{}", "{}"],
                {{"encoding":"binary", "dataSlice": {{"length": 2, "offset": 1}}}}
                ]
            }}"#,
            bob_pubkey, non_existent_address, address,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(result["result"]["value"].as_array().unwrap().len(), 3);
        assert_eq!(
            result["result"]["value"][2]["data"],
            bs58::encode(&data[1..3]).into_string(),
        );

        let req = format!(
            r#"{{
                "jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[
                ["{}", "{}", "{}"],
                {{"encoding":"jsonParsed", "dataSlice": {{"length": 2, "offset": 1}}}}
                ]
            }}"#,
            bob_pubkey, non_existent_address, address,
        );
        let res = io.handle_request_sync(&req, meta);
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        result["error"].as_object().unwrap();
    }

    #[test]
    fn test_rpc_get_program_accounts() {
        let bob = Keypair::new();
        let RpcHandler {
            io,
            meta,
            bank,
            blockhash,
            alice,
            ..
        } = start_rpc_handler_with_tx(&bob.pubkey());

        let new_program_id = solana_sdk::pubkey::new_rand();
        let tx = system_transaction::assign(&bob, blockhash, &new_program_id);
        bank.process_transaction(&tx).unwrap();
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getProgramAccounts","params":["{}"]}}"#,
            new_program_id
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = format!(
            r#"{{
                "jsonrpc":"2.0",
                "result":[
                    {{
                        "pubkey": "{}",
                        "account": {{
                            "owner": "{}",
                            "lamports": 20,
                            "data": "",
                            "executable": false,
                            "rentEpoch": 0
                        }}
                    }}
                ],
                "id":1}}
            "#,
            bob.pubkey(),
            new_program_id
        );
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Test returns context
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getProgramAccounts",
                "params":["{}",{{
                    "withContext": true
                }}]
            }}"#,
            system_program::id(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response")).unwrap();
        let contains_slot = result["result"]["context"]
            .as_object()
            .expect("must contain context")
            .contains_key("slot");
        assert!(contains_slot);

        // Set up nonce accounts to test filters
        let nonce_keypair0 = Keypair::new();
        let instruction = system_instruction::create_nonce_account(
            &alice.pubkey(),
            &nonce_keypair0.pubkey(),
            &bob.pubkey(),
            100_000,
        );
        let message = Message::new(&instruction, Some(&alice.pubkey()));
        let tx = Transaction::new(&[&alice, &nonce_keypair0], message, blockhash);
        bank.process_transaction(&tx).unwrap();

        let nonce_keypair1 = Keypair::new();
        let authority = solana_sdk::pubkey::new_rand();
        let instruction = system_instruction::create_nonce_account(
            &alice.pubkey(),
            &nonce_keypair1.pubkey(),
            &authority,
            100_000,
        );
        let message = Message::new(&instruction, Some(&alice.pubkey()));
        let tx = Transaction::new(&[&alice, &nonce_keypair1], message, blockhash);
        bank.process_transaction(&tx).unwrap();

        // Test memcmp filter; filter on Initialized state
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getProgramAccounts",
                "params":["{}",{{"filters": [
                    {{
                        "memcmp": {{"offset": 4,"bytes": "{}"}}
                    }}
                ]}}]
            }}"#,
            system_program::id(),
            bs58::encode(vec![1]).into_string(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let accounts: Vec<RpcKeyedAccount> = serde_json::from_value(json["result"].clone())
            .expect("actual response deserialization");
        assert_eq!(accounts.len(), 2);

        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getProgramAccounts",
                "params":["{}",{{"filters": [
                    {{
                        "memcmp": {{"offset": 0,"bytes": "{}"}}
                    }}
                ]}}]
            }}"#,
            system_program::id(),
            bs58::encode(vec![1]).into_string(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let accounts: Vec<RpcKeyedAccount> = serde_json::from_value(json["result"].clone())
            .expect("actual response deserialization");
        assert_eq!(accounts.len(), 0);

        // Test dataSize filter
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getProgramAccounts",
                "params":["{}",{{"filters": [
                    {{
                        "dataSize": {}
                    }}
                ]}}]
            }}"#,
            system_program::id(),
            nonce::State::size(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let accounts: Vec<RpcKeyedAccount> = serde_json::from_value(json["result"].clone())
            .expect("actual response deserialization");
        assert_eq!(accounts.len(), 2);

        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getProgramAccounts",
                "params":["{}",{{"filters": [
                    {{
                        "dataSize": 1
                    }}
                ]}}]
            }}"#,
            system_program::id(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let accounts: Vec<RpcKeyedAccount> = serde_json::from_value(json["result"].clone())
            .expect("actual response deserialization");
        assert_eq!(accounts.len(), 0);

        // Test multiple filters
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getProgramAccounts",
                "params":["{}",{{"filters": [
                    {{
                        "memcmp": {{"offset": 4,"bytes": "{}"}}
                    }},
                    {{
                        "memcmp": {{"offset": 8,"bytes": "{}"}}
                    }}
                ]}}]
            }}"#,
            system_program::id(),
            bs58::encode(vec![1]).into_string(),
            authority,
        ); // Filter on Initialized and Nonce authority
        let res = io.handle_request_sync(&req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let accounts: Vec<RpcKeyedAccount> = serde_json::from_value(json["result"].clone())
            .expect("actual response deserialization");
        assert_eq!(accounts.len(), 1);

        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getProgramAccounts",
                "params":["{}",{{"filters": [
                    {{
                        "memcmp": {{"offset": 4,"bytes": "{}"}}
                    }},
                    {{
                        "dataSize": 1
                    }}
                ]}}]
            }}"#,
            system_program::id(),
            bs58::encode(vec![1]).into_string(),
        ); // Filter on Initialized and non-matching data size
        let res = io.handle_request_sync(&req, meta);
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let accounts: Vec<RpcKeyedAccount> = serde_json::from_value(json["result"].clone())
            .expect("actual response deserialization");
        assert_eq!(accounts.len(), 0);
    }

    #[test]
    fn test_rpc_simulate_transaction() {
        let RpcHandler {
            io,
            meta,
            blockhash,
            alice,
            bank,
            ..
        } = start_rpc_handler_with_tx(&solana_sdk::pubkey::new_rand());

        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let mut tx = system_transaction::transfer(&alice, &bob_pubkey, 1234, blockhash);
        let tx_serialized_encoded = bs58::encode(serialize(&tx).unwrap()).into_string();
        tx.signatures[0] = Signature::default();
        let tx_badsig_serialized_encoded = bs58::encode(serialize(&tx).unwrap()).into_string();
        tx.message.recent_blockhash = Hash::default();
        let tx_invalid_recent_blockhash = bs58::encode(serialize(&tx).unwrap()).into_string();

        bank.freeze(); // Ensure the root bank is frozen, `start_rpc_handler_with_tx()` doesn't do this

        // Good signature with sigVerify=true
        let req = format!(
            r#"{{"jsonrpc":"2.0",
                 "id":1,
                 "method":"simulateTransaction",
                 "params":[
                   "{}",
                   {{
                     "sigVerify": true,
                     "accounts": {{
                       "encoding": "jsonParsed",
                       "addresses": ["{}", "{}"]
                     }}
                   }}
                 ]
            }}"#,
            tx_serialized_encoded,
            solana_sdk::pubkey::new_rand(),
            bob_pubkey,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "context":{"slot":0},
                "value":{
                    "accounts": [
                        null,
                        {
                            "data": ["", "base64"],
                            "executable": false,
                            "owner": "11111111111111111111111111111111",
                            "lamports": 1234,
                            "rentEpoch": 0
                        }
                    ],
                    "err":null,
                    "logs":[
                        "Program 11111111111111111111111111111111 invoke [1]",
                        "Program 11111111111111111111111111111111 success"
                    ],
                    "unitsConsumed":0
                }
            },
            "id": 1,
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Too many input accounts...
        let req = format!(
            r#"{{"jsonrpc":"2.0",
                 "id":1,
                 "method":"simulateTransaction",
                 "params":[
                   "{}",
                   {{
                     "sigVerify": true,
                     "accounts": {{
                       "addresses": [
                          "11111111111111111111111111111111",
                          "11111111111111111111111111111111",
                          "11111111111111111111111111111111",
                          "11111111111111111111111111111111"
                        ]
                     }}
                   }}
                 ]
            }}"#,
            tx_serialized_encoded,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = json!({
            "jsonrpc":"2.0",
            "error": {
                "code": error::ErrorCode::InvalidParams.code(),
                "message": "Too many accounts provided; max 3"
            },
            "id":1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Bad signature with sigVerify=true
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"simulateTransaction","params":["{}", {{"sigVerify": true}}]}}"#,
            tx_badsig_serialized_encoded,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = json!({
            "jsonrpc":"2.0",
            "error": {
                "code": -32003,
                "message": "Transaction signature verification failure"
            },
            "id":1
        });

        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Bad signature with sigVerify=false
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"simulateTransaction","params":["{}", {{"sigVerify": false}}]}}"#,
            tx_serialized_encoded,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "context":{"slot":0},
                "value":{
                    "accounts":null,
                    "err":null,
                    "logs":[
                        "Program 11111111111111111111111111111111 invoke [1]",
                        "Program 11111111111111111111111111111111 success"
                    ],
                    "unitsConsumed":0
                }
            },
            "id": 1,
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Bad signature with default sigVerify setting (false)
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"simulateTransaction","params":["{}"]}}"#,
            tx_serialized_encoded,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "context":{"slot":0},
                "value":{
                    "accounts":null,
                    "err":null,
                    "logs":[
                        "Program 11111111111111111111111111111111 invoke [1]",
                        "Program 11111111111111111111111111111111 success"
                    ],
                    "unitsConsumed":0
                }
            },
            "id": 1,
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Enabled both sigVerify=true and replaceRecentBlockhash=true
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"simulateTransaction","params":["{}", {}]}}"#,
            tx_serialized_encoded,
            json!({
                "sigVerify": true,
                "replaceRecentBlockhash": true,
            })
            .to_string()
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = json!({
            "jsonrpc":"2.0",
            "error": {
                "code": ErrorCode::InvalidParams,
                "message": "sigVerify may not be used with replaceRecentBlockhash"
            },
            "id":1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Bad recent blockhash with replaceRecentBlockhash=false
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"simulateTransaction","params":["{}", {{"replaceRecentBlockhash": false}}]}}"#,
            tx_invalid_recent_blockhash,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = json!({
            "jsonrpc":"2.0",
            "result": {
                "context":{"slot":0},
                "value":{
                    "err":"BlockhashNotFound",
                    "accounts":null,
                    "logs":[],
                    "unitsConsumed":0
                }
            },
            "id":1
        });

        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Bad recent blockhash with replaceRecentBlockhash=true
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"simulateTransaction","params":["{}", {{"replaceRecentBlockhash": true}}]}}"#,
            tx_invalid_recent_blockhash,
        );
        let res = io.handle_request_sync(&req, meta);
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "context":{"slot":0},
                "value":{
                    "accounts":null,
                    "err":null,
                    "logs":[
                        "Program 11111111111111111111111111111111 invoke [1]",
                        "Program 11111111111111111111111111111111 success"
                    ],
                    "unitsConsumed":0
                }
            },
            "id": 1,
        });

        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    #[should_panic]
    fn test_rpc_simulate_transaction_panic_on_unfrozen_bank() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io,
            meta,
            blockhash,
            alice,
            bank,
            ..
        } = start_rpc_handler_with_tx(&bob_pubkey);

        let tx = system_transaction::transfer(&alice, &bob_pubkey, 1234, blockhash);
        let tx_serialized_encoded = bs58::encode(serialize(&tx).unwrap()).into_string();

        assert!(!bank.is_frozen());

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"simulateTransaction","params":["{}", {{"sigVerify": true}}]}}"#,
            tx_serialized_encoded,
        );

        // should panic because `bank` is not frozen
        let _ = io.handle_request_sync(&req, meta);
    }

    #[test]
    fn test_rpc_get_signature_statuses() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io,
            mut meta,
            blockhash,
            alice,
            confirmed_block_signatures,
            ..
        } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[["{}"]]}}"#,
            confirmed_block_signatures[0]
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected_res: transaction::Result<()> = Ok(());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let result: Option<TransactionStatus> =
            serde_json::from_value(json["result"]["value"][0].clone())
                .expect("actual response deserialization");
        let result = result.as_ref().unwrap();
        assert_eq!(expected_res, result.status);
        assert_eq!(None, result.confirmations);

        // Test getSignatureStatus request on unprocessed tx
        let tx = system_transaction::transfer(&alice, &bob_pubkey, 10, blockhash);
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[["{}"]]}}"#,
            tx.signatures[0]
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let result: Option<TransactionStatus> =
            serde_json::from_value(json["result"]["value"][0].clone())
                .expect("actual response deserialization");
        assert!(result.is_none());

        // Test getSignatureStatus request on a TransactionError
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[["{}"]]}}"#,
            confirmed_block_signatures[1]
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected_res: transaction::Result<()> = Err(TransactionError::InstructionError(
            0,
            InstructionError::Custom(1),
        ));
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let result: Option<TransactionStatus> =
            serde_json::from_value(json["result"]["value"][0].clone())
                .expect("actual response deserialization");
        assert_eq!(expected_res, result.as_ref().unwrap().status);

        // disable rpc-tx-history, but attempt historical query
        meta.config.enable_rpc_transaction_history = false;
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[["{}"], {{"searchTransactionHistory": true}}]}}"#,
            confirmed_block_signatures[1]
        );
        let res = io.handle_request_sync(&req, meta);
        assert_eq!(
            res,
            Some(
                r#"{"jsonrpc":"2.0","error":{"code":-32011,"message":"Transaction history is not available from this node"},"id":1}"#.to_string(),
            )
        );
    }

    #[test]
    fn test_rpc_get_recent_blockhash() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io,
            meta,
            blockhash,
            ..
        } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getRecentBlockhash"}"#;
        let res = io.handle_request_sync(req, meta);
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
            "context":{"slot":0},
            "value":{
                "blockhash": blockhash.to_string(),
                "feeCalculator": {
                    "lamportsPerSignature": 0,
                }
            }},
            "id": 1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_fees() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io,
            meta,
            blockhash,
            ..
        } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getFees"}"#;
        let res = io.handle_request_sync(req, meta);
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
            "context":{"slot":0},
            "value":{
                "blockhash": blockhash.to_string(),
                "feeCalculator": {
                    "lamportsPerSignature": 0,
                },
                "lastValidSlot": MAX_RECENT_BLOCKHASHES,
                "lastValidBlockHeight": MAX_RECENT_BLOCKHASHES,
            }},
            "id": 1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_fee_calculator_for_blockhash() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, bank, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let (blockhash, fee_calculator) = bank.last_blockhash_with_fee_calculator();
        let fee_calculator = RpcFeeCalculator { fee_calculator };

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getFeeCalculatorForBlockhash","params":["{:?}"]}}"#,
            blockhash
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "context":{"slot":0},
                "value":fee_calculator,
            },
            "id": 1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        // Expired (non-existent) blockhash
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getFeeCalculatorForBlockhash","params":["{:?}"]}}"#,
            Hash::default()
        );
        let res = io.handle_request_sync(&req, meta);
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "context":{"slot":0},
                "value":Value::Null,
            },
            "id": 1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_get_fee_rate_governor() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getFeeRateGovernor"}"#;
        let res = io.handle_request_sync(req, meta);
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
            "context":{"slot":0},
            "value":{
                "feeRateGovernor": {
                    "burnPercent": DEFAULT_BURN_PERCENT,
                    "maxLamportsPerSignature": 0,
                    "minLamportsPerSignature": 0,
                    "targetLamportsPerSignature": 0,
                    "targetSignaturesPerSlot": 0
                }
            }},
            "id": 1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_fail_request_airdrop() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        // Expect internal error because no faucet is available
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"requestAirdrop","params":["{}", 50]}}"#,
            bob_pubkey
        );
        let res = io.handle_request_sync(&req, meta);
        let expected =
            r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"},"id":1}"#;
        let expected: Response =
            serde_json::from_str(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_send_bad_tx() {
        let genesis = create_genesis_config(100);
        let bank = Arc::new(Bank::new(&genesis.genesis_config));
        let meta = JsonRpcRequestProcessor::new_from_bank(&bank, SocketAddrSpace::Unspecified);

        let mut io = MetaIoHandler::default();
        io.extend_with(rpc_full::FullImpl.to_delegate());

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["37u9WtQpcm6ULa3Vmu7ySnANv"]}"#;
        let res = io.handle_request_sync(req, meta);
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let error = &json["error"];
        assert_eq!(error["code"], ErrorCode::InvalidParams.code());
    }

    #[test]
    fn test_rpc_send_transaction_preflight() {
        let exit = Arc::new(AtomicBool::new(false));
        let validator_exit = create_validator_exit(&exit);
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(Blockstore::open(&ledger_path).unwrap());
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
        let (bank_forks, mint_keypair, ..) = new_bank_forks();
        let health = RpcHealth::stub();

        // Freeze bank 0 to prevent a panic in `run_transaction_simulation()`
        bank_forks.write().unwrap().get(0).unwrap().freeze();

        let mut io = MetaIoHandler::default();
        io.extend_with(rpc_full::FullImpl.to_delegate());
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_with_socketaddr(&socketaddr!("127.0.0.1:1234")),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        let tpu_address = cluster_info.my_contact_info().tpu;
        let (meta, receiver) = JsonRpcRequestProcessor::new(
            JsonRpcConfig::default(),
            None,
            bank_forks.clone(),
            block_commitment_cache,
            blockstore,
            validator_exit,
            health.clone(),
            cluster_info,
            Hash::default(),
            None,
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
            Arc::new(RwLock::new(LargestAccountsCache::new(30))),
            Arc::new(MaxSlots::default()),
            Arc::new(LeaderScheduleCache::default()),
            Arc::new(AtomicU64::default()),
        );
        SendTransactionService::new(tpu_address, &bank_forks, None, receiver, 1000, 1);

        let mut bad_transaction = system_transaction::transfer(
            &mint_keypair,
            &solana_sdk::pubkey::new_rand(),
            42,
            Hash::default(),
        );

        // sendTransaction will fail because the blockhash is invalid
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["{}"]}}"#,
            bs58::encode(serialize(&bad_transaction).unwrap()).into_string()
        );
        let res = io.handle_request_sync(&req, meta.clone());
        assert_eq!(
            res,
            Some(
                r#"{"jsonrpc":"2.0","error":{"code":-32002,"message":"Transaction simulation failed: Blockhash not found","data":{"accounts":null,"err":"BlockhashNotFound","logs":[],"unitsConsumed":0}},"id":1}"#.to_string(),
            )
        );

        // sendTransaction will fail due to insanity
        bad_transaction.message.instructions[0].program_id_index = 0u8;
        let recent_blockhash = bank_forks.read().unwrap().root_bank().last_blockhash();
        bad_transaction.sign(&[&mint_keypair], recent_blockhash);
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["{}"]}}"#,
            bs58::encode(serialize(&bad_transaction).unwrap()).into_string()
        );
        let res = io.handle_request_sync(&req, meta.clone());
        assert_eq!(
            res,
            Some(
                r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"invalid transaction: index out of bounds"},"id":1}"#.to_string(),
            )
        );
        let mut bad_transaction = system_transaction::transfer(
            &mint_keypair,
            &solana_sdk::pubkey::new_rand(),
            42,
            recent_blockhash,
        );

        // sendTransaction will fail due to poor node health
        health.stub_set_health_status(Some(RpcHealthStatus::Behind { num_slots: 42 }));
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["{}"]}}"#,
            bs58::encode(serialize(&bad_transaction).unwrap()).into_string()
        );
        let res = io.handle_request_sync(&req, meta.clone());
        assert_eq!(
            res,
            Some(
                r#"{"jsonrpc":"2.0","error":{"code":-32005,"message":"Node is behind by 42 slots","data":{"numSlotsBehind":42}},"id":1}"#.to_string(),
            )
        );
        health.stub_set_health_status(None);

        // sendTransaction will fail due to invalid signature
        bad_transaction.signatures[0] = Signature::default();

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["{}"]}}"#,
            bs58::encode(serialize(&bad_transaction).unwrap()).into_string()
        );
        let res = io.handle_request_sync(&req, meta.clone());
        assert_eq!(
            res,
            Some(
                r#"{"jsonrpc":"2.0","error":{"code":-32003,"message":"Transaction signature verification failure"},"id":1}"#.to_string(),
            )
        );

        // sendTransaction will now succeed because skipPreflight=true even though it's a bad
        // transaction
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["{}", {{"skipPreflight": true}}]}}"#,
            bs58::encode(serialize(&bad_transaction).unwrap()).into_string()
        );
        let res = io.handle_request_sync(&req, meta.clone());
        assert_eq!(
            res,
            Some(
                r#"{"jsonrpc":"2.0","result":"1111111111111111111111111111111111111111111111111111111111111111","id":1}"#.to_string(),
            )
        );

        // sendTransaction will fail due to sanitization failure
        bad_transaction.signatures.clear();
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["{}"]}}"#,
            bs58::encode(serialize(&bad_transaction).unwrap()).into_string()
        );
        let res = io.handle_request_sync(&req, meta);
        assert_eq!(
            res,
            Some(
                r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"invalid transaction: index out of bounds"},"id":1}"#.to_string(),
            )
        );
    }

    #[test]
    fn test_rpc_verify_filter() {
        let filter = RpcFilterType::Memcmp(Memcmp {
            offset: 0,
            bytes: MemcmpEncodedBytes::Binary(
                "13LeFbG6m2EP1fqCj9k66fcXsoTHMMtgr7c78AivUrYD".to_string(),
            ),
            encoding: None,
        });
        assert_eq!(verify_filter(&filter), Ok(()));
        // Invalid base-58
        let filter = RpcFilterType::Memcmp(Memcmp {
            offset: 0,
            bytes: MemcmpEncodedBytes::Binary("III".to_string()),
            encoding: None,
        });
        assert!(verify_filter(&filter).is_err());
    }

    #[test]
    fn test_rpc_verify_pubkey() {
        let pubkey = solana_sdk::pubkey::new_rand();
        assert_eq!(verify_pubkey(&pubkey.to_string()).unwrap(), pubkey);
        let bad_pubkey = "a1b2c3d4";
        assert_eq!(
            verify_pubkey(&bad_pubkey.to_string()),
            Err(Error::invalid_params("Invalid param: WrongSize"))
        );
    }

    #[test]
    fn test_rpc_verify_signature() {
        let tx = system_transaction::transfer(
            &Keypair::new(),
            &solana_sdk::pubkey::new_rand(),
            20,
            hash(&[0]),
        );
        assert_eq!(
            verify_signature(&tx.signatures[0].to_string()).unwrap(),
            tx.signatures[0]
        );
        let bad_signature = "a1b2c3d4";
        assert_eq!(
            verify_signature(&bad_signature.to_string()),
            Err(Error::invalid_params("Invalid param: WrongSize"))
        );
    }

    fn new_bank_forks() -> (Arc<RwLock<BankForks>>, Keypair, Arc<Keypair>) {
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            voting_keypair,
        } = create_genesis_config(TEST_MINT_LAMPORTS);

        genesis_config.rent.lamports_per_byte_year = 50;
        genesis_config.rent.exemption_threshold = 2.0;
        genesis_config.epoch_schedule =
            EpochSchedule::custom(TEST_SLOTS_PER_EPOCH, TEST_SLOTS_PER_EPOCH, false);

        let bank = Bank::new(&genesis_config);
        (
            Arc::new(RwLock::new(BankForks::new(bank))),
            mint_keypair,
            Arc::new(voting_keypair),
        )
    }

    #[test]
    fn test_rpc_get_identity() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io, meta, alice, ..
        } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getIdentity"}"#;
        let res = io.handle_request_sync(req, meta);
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "identity": alice.pubkey().to_string()
            },
            "id": 1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    fn test_basic_slot(method: &str, expected: Slot) {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = format!("{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"{}\"}}", method);
        let res = io.handle_request_sync(&req, meta);

        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let slot: Slot = serde_json::from_value(json["result"].clone()).unwrap();
        assert_eq!(slot, expected);
    }

    #[test]
    fn test_rpc_get_max_slots() {
        test_basic_slot("getMaxRetransmitSlot", 42);
        test_basic_slot("getMaxShredInsertSlot", 43);
    }

    #[test]
    fn test_rpc_get_version() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler { io, meta, .. } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getVersion"}"#;
        let res = io.handle_request_sync(req, meta);
        let version = solana_version::Version::default();
        let expected = json!({
            "jsonrpc": "2.0",
            "result": {
                "solana-core": version.to_string(),
                "feature-set": version.feature_set,
            },
            "id": 1
        });
        let expected: Response =
            serde_json::from_value(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    #[test]
    fn test_rpc_processor_get_block_commitment() {
        let exit = Arc::new(AtomicBool::new(false));
        let validator_exit = create_validator_exit(&exit);
        let bank_forks = new_bank_forks().0;
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(Blockstore::open(&ledger_path).unwrap());

        let commitment_slot0 = BlockCommitment::new([8; MAX_LOCKOUT_HISTORY + 1]);
        let commitment_slot1 = BlockCommitment::new([9; MAX_LOCKOUT_HISTORY + 1]);
        let mut block_commitment: HashMap<u64, BlockCommitment> = HashMap::new();
        block_commitment
            .entry(0)
            .or_insert_with(|| commitment_slot0.clone());
        block_commitment
            .entry(1)
            .or_insert_with(|| commitment_slot1.clone());
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::new(
            block_commitment,
            42,
            CommitmentSlots::new_from_slot(bank_forks.read().unwrap().highest_slot()),
        )));

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::default(),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        let tpu_address = cluster_info.my_contact_info().tpu;
        let (request_processor, receiver) = JsonRpcRequestProcessor::new(
            JsonRpcConfig::default(),
            None,
            bank_forks.clone(),
            block_commitment_cache,
            blockstore,
            validator_exit,
            RpcHealth::stub(),
            cluster_info,
            Hash::default(),
            None,
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
            Arc::new(RwLock::new(LargestAccountsCache::new(30))),
            Arc::new(MaxSlots::default()),
            Arc::new(LeaderScheduleCache::default()),
            Arc::new(AtomicU64::default()),
        );
        SendTransactionService::new(tpu_address, &bank_forks, None, receiver, 1000, 1);
        assert_eq!(
            request_processor.get_block_commitment(0),
            RpcBlockCommitment {
                commitment: Some(commitment_slot0.commitment),
                total_stake: 42,
            }
        );
        assert_eq!(
            request_processor.get_block_commitment(1),
            RpcBlockCommitment {
                commitment: Some(commitment_slot1.commitment),
                total_stake: 42,
            }
        );
        assert_eq!(
            request_processor.get_block_commitment(2),
            RpcBlockCommitment {
                commitment: None,
                total_stake: 42,
            }
        );
    }

    #[test]
    fn test_rpc_get_block_commitment() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io,
            meta,
            block_commitment_cache,
            ..
        } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlockCommitment","params":[0]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let RpcBlockCommitment {
            commitment,
            total_stake,
        } = if let Response::Single(res) = result {
            if let Output::Success(res) = res {
                serde_json::from_value(res.result).unwrap()
            } else {
                panic!("Expected success");
            }
        } else {
            panic!("Expected single response");
        };
        assert_eq!(
            commitment,
            block_commitment_cache
                .read()
                .unwrap()
                .get_block_commitment(0)
                .map(|block_commitment| block_commitment.commitment)
        );
        assert_eq!(total_stake, 10);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlockCommitment","params":[2]}"#;
        let res = io.handle_request_sync(req, meta);
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let commitment_response: RpcBlockCommitment<BlockCommitmentArray> =
            if let Response::Single(res) = result {
                if let Output::Success(res) = res {
                    serde_json::from_value(res.result).unwrap()
                } else {
                    panic!("Expected success");
                }
            } else {
                panic!("Expected single response");
            };
        assert_eq!(commitment_response.commitment, None);
        assert_eq!(commitment_response.total_stake, 10);
    }

    #[test]
    fn test_get_block() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io,
            mut meta,
            confirmed_block_signatures,
            blockhash,
            ..
        } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[0]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_block: Option<EncodedConfirmedBlock> =
            serde_json::from_value(result["result"].clone()).unwrap();
        let confirmed_block = confirmed_block.unwrap();
        assert_eq!(confirmed_block.transactions.len(), 3);
        assert_eq!(confirmed_block.rewards, vec![]);

        for EncodedTransactionWithStatusMeta { transaction, meta } in
            confirmed_block.transactions.into_iter()
        {
            if let EncodedTransaction::Json(transaction) = transaction {
                if transaction.signatures[0] == confirmed_block_signatures[0].to_string() {
                    let meta = meta.unwrap();
                    let transaction_recent_blockhash = match transaction.message {
                        UiMessage::Parsed(message) => message.recent_blockhash,
                        UiMessage::Raw(message) => message.recent_blockhash,
                    };
                    assert_eq!(transaction_recent_blockhash, blockhash.to_string());
                    assert_eq!(meta.status, Ok(()));
                    assert_eq!(meta.err, None);
                } else if transaction.signatures[0] == confirmed_block_signatures[1].to_string() {
                    let meta = meta.unwrap();
                    assert_eq!(
                        meta.err,
                        Some(TransactionError::InstructionError(
                            0,
                            InstructionError::Custom(1)
                        ))
                    );
                    assert_eq!(
                        meta.status,
                        Err(TransactionError::InstructionError(
                            0,
                            InstructionError::Custom(1)
                        ))
                    );
                } else {
                    assert_eq!(meta, None);
                }
            }
        }

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[0,"binary"]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_block: Option<EncodedConfirmedBlock> =
            serde_json::from_value(result["result"].clone()).unwrap();
        let confirmed_block = confirmed_block.unwrap();
        assert_eq!(confirmed_block.transactions.len(), 3);
        assert_eq!(confirmed_block.rewards, vec![]);

        for EncodedTransactionWithStatusMeta { transaction, meta } in
            confirmed_block.transactions.into_iter()
        {
            if let EncodedTransaction::LegacyBinary(transaction) = transaction {
                let decoded_transaction: Transaction =
                    deserialize(&bs58::decode(&transaction).into_vec().unwrap()).unwrap();
                if decoded_transaction.signatures[0] == confirmed_block_signatures[0] {
                    let meta = meta.unwrap();
                    assert_eq!(decoded_transaction.message.recent_blockhash, blockhash);
                    assert_eq!(meta.status, Ok(()));
                    assert_eq!(meta.err, None);
                } else if decoded_transaction.signatures[0] == confirmed_block_signatures[1] {
                    let meta = meta.unwrap();
                    assert_eq!(
                        meta.err,
                        Some(TransactionError::InstructionError(
                            0,
                            InstructionError::Custom(1)
                        ))
                    );
                    assert_eq!(
                        meta.status,
                        Err(TransactionError::InstructionError(
                            0,
                            InstructionError::Custom(1)
                        ))
                    );
                } else {
                    assert_eq!(meta, None);
                }
            }
        }

        // disable rpc-tx-history
        meta.config.enable_rpc_transaction_history = false;
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[0]}"#;
        let res = io.handle_request_sync(req, meta);
        assert_eq!(
            res,
            Some(
                r#"{"jsonrpc":"2.0","error":{"code":-32011,"message":"Transaction history is not available from this node"},"id":1}"#.to_string(),
            )
        );
    }

    #[test]
    fn test_get_block_config() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io,
            meta,
            confirmed_block_signatures,
            ..
        } = start_rpc_handler_with_tx(&bob_pubkey);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[0,{}]}}"#,
            json!(RpcBlockConfig {
                encoding: None,
                transaction_details: Some(TransactionDetails::Signatures),
                rewards: Some(false),
                commitment: None,
            })
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_block: Option<UiConfirmedBlock> =
            serde_json::from_value(result["result"].clone()).unwrap();
        let confirmed_block = confirmed_block.unwrap();
        assert!(confirmed_block.transactions.is_none());
        assert!(confirmed_block.rewards.is_none());
        for (i, signature) in confirmed_block.signatures.unwrap()[..2].iter().enumerate() {
            assert_eq!(*signature, confirmed_block_signatures[i].to_string());
        }

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[0,{}]}}"#,
            json!(RpcBlockConfig {
                encoding: None,
                transaction_details: Some(TransactionDetails::None),
                rewards: Some(true),
                commitment: None,
            })
        );
        let res = io.handle_request_sync(&req, meta);
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_block: Option<UiConfirmedBlock> =
            serde_json::from_value(result["result"].clone()).unwrap();
        let confirmed_block = confirmed_block.unwrap();
        assert!(confirmed_block.transactions.is_none());
        assert!(confirmed_block.signatures.is_none());
        assert_eq!(confirmed_block.rewards.unwrap(), vec![]);
    }

    #[test]
    fn test_get_block_production() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let roots = vec![0, 1, 3, 4, 8];
        let RpcHandler {
            io,
            meta,
            block_commitment_cache,
            leader_pubkey,
            ..
        } = start_rpc_handler_with_tx_and_blockstore(&bob_pubkey, roots);
        block_commitment_cache
            .write()
            .unwrap()
            .set_highest_confirmed_root(8);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlockProduction","params":[]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let block_production: RpcBlockProduction =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert_eq!(
            block_production.by_identity.get(&leader_pubkey.to_string()),
            Some(&(9, 5))
        );
        assert_eq!(
            block_production.range,
            RpcBlockProductionRange {
                first_slot: 0,
                last_slot: 8
            }
        );

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBlockProduction","params":[{{"identity": "{}"}}]}}"#,
            leader_pubkey
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let block_production: RpcBlockProduction =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert_eq!(
            block_production.by_identity.get(&leader_pubkey.to_string()),
            Some(&(9, 5))
        );
        assert_eq!(
            block_production.range,
            RpcBlockProductionRange {
                first_slot: 0,
                last_slot: 8
            }
        );

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBlockProduction","params":[{{"range": {{"firstSlot": 0, "lastSlot": 4}}, "identity": "{}"}}]}}"#,
            bob_pubkey
        );
        let res = io.handle_request_sync(&req, meta);
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let block_production: RpcBlockProduction =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert_eq!(
            block_production.by_identity.get(&leader_pubkey.to_string()),
            None
        );
        assert_eq!(
            block_production.range,
            RpcBlockProductionRange {
                first_slot: 0,
                last_slot: 4
            }
        );
    }

    #[test]
    fn test_get_blocks() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let roots = vec![0, 1, 3, 4, 8];
        let RpcHandler {
            io,
            meta,
            block_commitment_cache,
            ..
        } = start_rpc_handler_with_tx_and_blockstore(&bob_pubkey, roots.clone());
        block_commitment_cache
            .write()
            .unwrap()
            .set_highest_confirmed_root(8);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[0]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(confirmed_blocks, roots[1..].to_vec());

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[2]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(confirmed_blocks, vec![3, 4, 8]);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[0,4]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(confirmed_blocks, vec![1, 3, 4]);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[0,7]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(confirmed_blocks, vec![1, 3, 4]);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[9,11]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(confirmed_blocks, Vec::<Slot>::new());

        block_commitment_cache
            .write()
            .unwrap()
            .set_highest_confirmed_root(std::u64::MAX);
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[0,{}]}}"#,
            MAX_GET_CONFIRMED_BLOCKS_RANGE
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(confirmed_blocks, vec![1, 3, 4, 8]);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[0,{}]}}"#,
            MAX_GET_CONFIRMED_BLOCKS_RANGE + 1
        );
        let res = io.handle_request_sync(&req, meta);
        assert_eq!(
            res,
            Some(
                r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"Slot range too large; max 500000"},"id":1}"#.to_string(),
            )
        );
    }

    #[test]
    fn test_get_blocks_with_limit() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let roots = vec![0, 1, 3, 4, 8];
        let RpcHandler {
            io,
            meta,
            block_commitment_cache,
            ..
        } = start_rpc_handler_with_tx_and_blockstore(&bob_pubkey, roots);
        block_commitment_cache
            .write()
            .unwrap()
            .set_highest_confirmed_root(8);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocksWithLimit","params":[0,500001]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        assert_eq!(
            res,
            Some(
                r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"Limit too large; max 500000"},"id":1}"#.to_string(),
            )
        );

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocksWithLimit","params":[0,0]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert!(confirmed_blocks.is_empty());

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocksWithLimit","params":[2,2]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(confirmed_blocks, vec![3, 4]);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocksWithLimit","params":[2,3]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(confirmed_blocks, vec![3, 4, 8]);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocksWithLimit","params":[2,500000]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(confirmed_blocks, vec![3, 4, 8]);

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"getBlocksWithLimit","params":[9,500000]}"#;
        let res = io.handle_request_sync(req, meta);
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let confirmed_blocks: Vec<Slot> = serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(confirmed_blocks, Vec::<Slot>::new());
    }

    #[test]
    fn test_get_block_time() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let RpcHandler {
            io,
            meta,
            bank,
            block_commitment_cache,
            bank_forks,
            ..
        } = start_rpc_handler_with_tx_and_blockstore(&bob_pubkey, vec![1, 2, 3, 4, 5, 6, 7]);
        let base_timestamp = bank_forks
            .read()
            .unwrap()
            .get(0)
            .unwrap()
            .unix_timestamp_from_genesis();
        block_commitment_cache
            .write()
            .unwrap()
            .set_highest_confirmed_root(7);

        let slot_duration = slot_duration_from_slots_per_year(bank.slots_per_year());

        let slot = 2;
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBlockTime","params":[{}]}}"#,
            slot
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = format!(r#"{{"jsonrpc":"2.0","result":{},"id":1}}"#, base_timestamp);
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        let slot = 7;
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBlockTime","params":[{}]}}"#,
            slot
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let expected = format!(
            r#"{{"jsonrpc":"2.0","result":{},"id":1}}"#,
            base_timestamp + (7 * slot_duration).as_secs() as i64
        );
        let expected: Response =
            serde_json::from_str(&expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);

        let slot = 12345;
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getBlockTime","params":[{}]}}"#,
            slot
        );
        let res = io.handle_request_sync(&req, meta);
        let expected = r#"{"jsonrpc":"2.0","error":{"code":-32004,"message":"Block not available for slot 12345"},"id":1}"#;
        let expected: Response =
            serde_json::from_str(expected).expect("expected response deserialization");
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(expected, result);
    }

    fn advance_block_commitment_cache(
        block_commitment_cache: &Arc<RwLock<BlockCommitmentCache>>,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) {
        let mut new_block_commitment = BlockCommitmentCache::new(
            HashMap::new(),
            0,
            CommitmentSlots::new_from_slot(bank_forks.read().unwrap().highest_slot()),
        );
        let mut w_block_commitment_cache = block_commitment_cache.write().unwrap();
        std::mem::swap(&mut *w_block_commitment_cache, &mut new_block_commitment);
    }

    #[test]
    fn test_get_vote_accounts() {
        let RpcHandler {
            io,
            meta,
            mut bank,
            bank_forks,
            alice,
            leader_vote_keypair,
            block_commitment_cache,
            ..
        } = start_rpc_handler_with_tx(&solana_sdk::pubkey::new_rand());

        assert_eq!(bank.vote_accounts().len(), 1);

        // Create a vote account with no stake.
        let alice_vote_keypair = Keypair::new();
        let instructions = vote_instruction::create_account(
            &alice.pubkey(),
            &alice_vote_keypair.pubkey(),
            &VoteInit {
                node_pubkey: alice.pubkey(),
                authorized_voter: alice_vote_keypair.pubkey(),
                authorized_withdrawer: alice_vote_keypair.pubkey(),
                commission: 0,
            },
            bank.get_minimum_balance_for_rent_exemption(VoteState::size_of()),
        );

        let message = Message::new(&instructions, Some(&alice.pubkey()));
        let transaction = Transaction::new(
            &[&alice, &alice_vote_keypair],
            message,
            bank.last_blockhash(),
        );
        bank.process_transaction(&transaction)
            .expect("process transaction");
        assert_eq!(bank.vote_accounts().len(), 2);

        // Check getVoteAccounts: the bootstrap validator vote account will be delinquent as it has
        // stake but has never voted, and the vote account with no stake should not be present.
        {
            let req = r#"{"jsonrpc":"2.0","id":1,"method":"getVoteAccounts"}"#;
            let res = io.handle_request_sync(req, meta.clone());
            let result: Value = serde_json::from_str(&res.expect("actual response"))
                .expect("actual response deserialization");

            let vote_account_status: RpcVoteAccountStatus =
                serde_json::from_value(result["result"].clone()).unwrap();

            assert!(vote_account_status.current.is_empty());
            assert_eq!(vote_account_status.delinquent.len(), 1);
            for vote_account_info in vote_account_status.delinquent {
                assert_ne!(vote_account_info.activated_stake, 0);
            }
        }

        let mut advance_bank = || {
            bank.freeze();

            // Votes
            let instructions = [
                vote_instruction::vote(
                    &leader_vote_keypair.pubkey(),
                    &leader_vote_keypair.pubkey(),
                    Vote {
                        slots: vec![bank.slot()],
                        hash: bank.hash(),
                        timestamp: None,
                    },
                ),
                vote_instruction::vote(
                    &alice_vote_keypair.pubkey(),
                    &alice_vote_keypair.pubkey(),
                    Vote {
                        slots: vec![bank.slot()],
                        hash: bank.hash(),
                        timestamp: None,
                    },
                ),
            ];

            bank = bank_forks.write().unwrap().insert(Bank::new_from_parent(
                &bank,
                &Pubkey::default(),
                bank.slot() + 1,
            ));
            advance_block_commitment_cache(&block_commitment_cache, &bank_forks);

            let transaction = Transaction::new_signed_with_payer(
                &instructions,
                Some(&alice.pubkey()),
                &[&alice, &leader_vote_keypair, &alice_vote_keypair],
                bank.last_blockhash(),
            );

            bank.process_transaction(&transaction)
                .expect("process transaction");
        };

        // Advance bank to the next epoch
        for _ in 0..TEST_SLOTS_PER_EPOCH {
            advance_bank();
        }

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getVoteAccounts","params":{}}}"#,
            json!([CommitmentConfig::processed()])
        );

        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        let vote_account_status: RpcVoteAccountStatus =
            serde_json::from_value(result["result"].clone()).unwrap();

        // The vote account with no stake should not be present.
        assert!(vote_account_status.delinquent.is_empty());

        // Both accounts should be active and have voting history.
        assert_eq!(vote_account_status.current.len(), 2);
        let leader_info = vote_account_status
            .current
            .iter()
            .find(|x| x.vote_pubkey == leader_vote_keypair.pubkey().to_string())
            .unwrap();
        assert_ne!(leader_info.activated_stake, 0);
        // Subtract one because the last vote always carries over to the next epoch
        let expected_credits = TEST_SLOTS_PER_EPOCH - MAX_LOCKOUT_HISTORY as u64 - 1;
        assert_eq!(
            leader_info.epoch_credits,
            vec![
                (0, expected_credits, 0),
                (1, expected_credits + 1, expected_credits) // one vote in current epoch
            ]
        );

        // Filter request based on the leader:
        {
            let req = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"getVoteAccounts","params":{}}}"#,
                json!([RpcGetVoteAccountsConfig {
                    vote_pubkey: Some(leader_vote_keypair.pubkey().to_string()),
                    commitment: Some(CommitmentConfig::processed()),
                    ..RpcGetVoteAccountsConfig::default()
                }])
            );

            let res = io.handle_request_sync(&req, meta.clone());
            let result: Value = serde_json::from_str(&res.expect("actual response"))
                .expect("actual response deserialization");

            let vote_account_status: RpcVoteAccountStatus =
                serde_json::from_value(result["result"].clone()).unwrap();

            assert_eq!(vote_account_status.current.len(), 1);
            assert_eq!(vote_account_status.delinquent.len(), 0);
            for vote_account_info in vote_account_status.current {
                assert_eq!(
                    vote_account_info.vote_pubkey,
                    leader_vote_keypair.pubkey().to_string()
                );
            }
        }

        // Overflow the epoch credits history and ensure only `MAX_RPC_EPOCH_CREDITS_HISTORY`
        // results are returned
        for _ in 0..(TEST_SLOTS_PER_EPOCH * (MAX_RPC_EPOCH_CREDITS_HISTORY) as u64) {
            advance_bank();
        }

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getVoteAccounts","params":{}}}"#,
            json!([CommitmentConfig::processed()])
        );

        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        let vote_account_status: RpcVoteAccountStatus =
            serde_json::from_value(result["result"].clone()).unwrap();

        assert!(vote_account_status.delinquent.is_empty());
        assert!(!vote_account_status
            .current
            .iter()
            .any(|x| x.epoch_credits.len() != MAX_RPC_EPOCH_CREDITS_HISTORY));

        // Advance bank with no voting
        bank.freeze();
        bank_forks.write().unwrap().insert(Bank::new_from_parent(
            &bank,
            &Pubkey::default(),
            bank.slot() + TEST_SLOTS_PER_EPOCH,
        ));
        advance_block_commitment_cache(&block_commitment_cache, &bank_forks);

        // The leader vote account should now be delinquent, and the other vote account disappears
        // because it's inactive with no stake
        {
            let req = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"getVoteAccounts","params":{}}}"#,
                json!([CommitmentConfig::processed()])
            );

            let res = io.handle_request_sync(&req, meta);
            let result: Value = serde_json::from_str(&res.expect("actual response"))
                .expect("actual response deserialization");

            let vote_account_status: RpcVoteAccountStatus =
                serde_json::from_value(result["result"].clone()).unwrap();

            assert!(vote_account_status.current.is_empty());
            assert_eq!(vote_account_status.delinquent.len(), 1);
            for vote_account_info in vote_account_status.delinquent {
                assert_eq!(
                    vote_account_info.vote_pubkey,
                    leader_vote_keypair.pubkey().to_string()
                );
            }
        }
    }

    #[test]
    fn test_is_finalized() {
        let bank = Arc::new(Bank::default());
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(Blockstore::open(&ledger_path).unwrap());
        blockstore.set_roots(vec![0, 1].iter()).unwrap();
        // Build BlockCommitmentCache with rooted slots
        let mut cache0 = BlockCommitment::default();
        cache0.increase_rooted_stake(50);
        let mut cache1 = BlockCommitment::default();
        cache1.increase_rooted_stake(40);
        let mut cache2 = BlockCommitment::default();
        cache2.increase_rooted_stake(20);

        let mut block_commitment = HashMap::new();
        block_commitment.entry(1).or_insert(cache0);
        block_commitment.entry(2).or_insert(cache1);
        block_commitment.entry(3).or_insert(cache2);
        let highest_confirmed_root = 1;
        let block_commitment_cache = BlockCommitmentCache::new(
            block_commitment,
            50,
            CommitmentSlots {
                slot: bank.slot(),
                highest_confirmed_root,
                ..CommitmentSlots::default()
            },
        );

        assert!(is_finalized(&block_commitment_cache, &bank, &blockstore, 0));
        assert!(is_finalized(&block_commitment_cache, &bank, &blockstore, 1));
        assert!(!is_finalized(
            &block_commitment_cache,
            &bank,
            &blockstore,
            2
        ));
        assert!(!is_finalized(
            &block_commitment_cache,
            &bank,
            &blockstore,
            3
        ));
    }

    #[test]
    fn test_token_rpcs() {
        let RpcHandler { io, meta, bank, .. } =
            start_rpc_handler_with_tx(&solana_sdk::pubkey::new_rand());

        let mut account_data = vec![0; TokenAccount::get_packed_len()];
        let mint = SplTokenPubkey::new(&[2; 32]);
        let owner = SplTokenPubkey::new(&[3; 32]);
        let delegate = SplTokenPubkey::new(&[4; 32]);
        let token_account = TokenAccount {
            mint,
            owner,
            delegate: COption::Some(delegate),
            amount: 420,
            state: TokenAccountState::Initialized,
            is_native: COption::None,
            delegated_amount: 30,
            close_authority: COption::Some(owner),
        };
        TokenAccount::pack(token_account, &mut account_data).unwrap();
        let token_account = AccountSharedData::from(Account {
            lamports: 111,
            data: account_data.to_vec(),
            owner: spl_token_id_v2_0(),
            ..Account::default()
        });
        let token_account_pubkey = solana_sdk::pubkey::new_rand();
        bank.store_account(&token_account_pubkey, &token_account);

        // Add the mint
        let mut mint_data = vec![0; Mint::get_packed_len()];
        let mint_state = Mint {
            mint_authority: COption::Some(owner),
            supply: 500,
            decimals: 2,
            is_initialized: true,
            freeze_authority: COption::Some(owner),
        };
        Mint::pack(mint_state, &mut mint_data).unwrap();
        let mint_account = AccountSharedData::from(Account {
            lamports: 111,
            data: mint_data.to_vec(),
            owner: spl_token_id_v2_0(),
            ..Account::default()
        });
        bank.store_account(&Pubkey::from_str(&mint.to_string()).unwrap(), &mint_account);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getTokenAccountBalance","params":["{}"]}}"#,
            token_account_pubkey,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let balance: UiTokenAmount =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        let error = f64::EPSILON;
        assert!((balance.ui_amount.unwrap() - 4.2).abs() < error);
        assert_eq!(balance.amount, 420.to_string());
        assert_eq!(balance.decimals, 2);
        assert_eq!(balance.ui_amount_string, "4.2".to_string());

        // Test non-existent token account
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getTokenAccountBalance","params":["{}"]}}"#,
            solana_sdk::pubkey::new_rand(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert!(result.get("error").is_some());

        // Test get token supply, pulls supply from mint
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getTokenSupply","params":["{}"]}}"#,
            mint,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let supply: UiTokenAmount =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        let error = f64::EPSILON;
        assert!((supply.ui_amount.unwrap() - 5.0).abs() < error);
        assert_eq!(supply.amount, 500.to_string());
        assert_eq!(supply.decimals, 2);
        assert_eq!(supply.ui_amount_string, "5".to_string());

        // Test non-existent mint address
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getTokenSupply","params":["{}"]}}"#,
            solana_sdk::pubkey::new_rand(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert!(result.get("error").is_some());

        // Add another token account with the same owner, delegate, and mint
        let other_token_account_pubkey = solana_sdk::pubkey::new_rand();
        bank.store_account(&other_token_account_pubkey, &token_account);

        // Add another token account with the same owner and delegate but different mint
        let mut account_data = vec![0; TokenAccount::get_packed_len()];
        let new_mint = SplTokenPubkey::new(&[5; 32]);
        let token_account = TokenAccount {
            mint: new_mint,
            owner,
            delegate: COption::Some(delegate),
            amount: 42,
            state: TokenAccountState::Initialized,
            is_native: COption::None,
            delegated_amount: 30,
            close_authority: COption::Some(owner),
        };
        TokenAccount::pack(token_account, &mut account_data).unwrap();
        let token_account = AccountSharedData::from(Account {
            lamports: 111,
            data: account_data.to_vec(),
            owner: spl_token_id_v2_0(),
            ..Account::default()
        });
        let token_with_different_mint_pubkey = solana_sdk::pubkey::new_rand();
        bank.store_account(&token_with_different_mint_pubkey, &token_account);

        // Test getTokenAccountsByOwner with Token program id returns all accounts, regardless of Mint address
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getTokenAccountsByOwner",
                "params":["{}", {{"programId": "{}"}}]
            }}"#,
            owner,
            spl_token_id_v2_0(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let accounts: Vec<RpcKeyedAccount> =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert_eq!(accounts.len(), 3);

        // Test getTokenAccountsByOwner with jsonParsed encoding doesn't return accounts with invalid mints
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getTokenAccountsByOwner",
                "params":["{}", {{"programId": "{}"}}, {{"encoding": "jsonParsed"}}]
            }}"#,
            owner,
            spl_token_id_v2_0(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let accounts: Vec<RpcKeyedAccount> =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert_eq!(accounts.len(), 2);

        // Test getProgramAccounts with jsonParsed encoding returns mints, but doesn't return accounts with invalid mints
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getProgramAccounts",
                "params":["{}", {{"encoding": "jsonParsed"}}]
            }}"#,
            spl_token_id_v2_0(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let accounts: Vec<RpcKeyedAccount> =
            serde_json::from_value(result["result"].clone()).unwrap();
        assert_eq!(accounts.len(), 4);

        // Test returns only mint accounts
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,"method":"getTokenAccountsByOwner",
                "params":["{}", {{"mint": "{}"}}]
            }}"#,
            owner, mint,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let accounts: Vec<RpcKeyedAccount> =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert_eq!(accounts.len(), 2);

        // Test non-existent Mint/program id
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getTokenAccountsByOwner",
                "params":["{}", {{"programId": "{}"}}]
            }}"#,
            owner,
            solana_sdk::pubkey::new_rand(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert!(result.get("error").is_some());
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getTokenAccountsByOwner",
                "params":["{}", {{"mint": "{}"}}]
            }}"#,
            owner,
            solana_sdk::pubkey::new_rand(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert!(result.get("error").is_some());

        // Test non-existent Owner
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getTokenAccountsByOwner",
                "params":["{}", {{"programId": "{}"}}]
            }}"#,
            solana_sdk::pubkey::new_rand(),
            spl_token_id_v2_0(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let accounts: Vec<RpcKeyedAccount> =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert!(accounts.is_empty());

        // Test getTokenAccountsByDelegate with Token program id returns all accounts, regardless of Mint address
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getTokenAccountsByDelegate",
                "params":["{}", {{"programId": "{}"}}]
            }}"#,
            delegate,
            spl_token_id_v2_0(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let accounts: Vec<RpcKeyedAccount> =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert_eq!(accounts.len(), 3);

        // Test returns only mint accounts
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,"method":
                "getTokenAccountsByDelegate",
                "params":["{}", {{"mint": "{}"}}]
            }}"#,
            delegate, mint,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let accounts: Vec<RpcKeyedAccount> =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert_eq!(accounts.len(), 2);

        // Test non-existent Mint/program id
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getTokenAccountsByDelegate",
                "params":["{}", {{"programId": "{}"}}]
            }}"#,
            delegate,
            solana_sdk::pubkey::new_rand(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert!(result.get("error").is_some());
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getTokenAccountsByDelegate",
                "params":["{}", {{"mint": "{}"}}]
            }}"#,
            delegate,
            solana_sdk::pubkey::new_rand(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert!(result.get("error").is_some());

        // Test non-existent Delegate
        let req = format!(
            r#"{{
                "jsonrpc":"2.0",
                "id":1,
                "method":"getTokenAccountsByDelegate",
                "params":["{}", {{"programId": "{}"}}]
            }}"#,
            solana_sdk::pubkey::new_rand(),
            spl_token_id_v2_0(),
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let accounts: Vec<RpcKeyedAccount> =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert!(accounts.is_empty());

        // Add new_mint, and another token account on new_mint with different balance
        let mut mint_data = vec![0; Mint::get_packed_len()];
        let mint_state = Mint {
            mint_authority: COption::Some(owner),
            supply: 500,
            decimals: 2,
            is_initialized: true,
            freeze_authority: COption::Some(owner),
        };
        Mint::pack(mint_state, &mut mint_data).unwrap();
        let mint_account = AccountSharedData::from(Account {
            lamports: 111,
            data: mint_data.to_vec(),
            owner: spl_token_id_v2_0(),
            ..Account::default()
        });
        bank.store_account(
            &Pubkey::from_str(&new_mint.to_string()).unwrap(),
            &mint_account,
        );
        let mut account_data = vec![0; TokenAccount::get_packed_len()];
        let token_account = TokenAccount {
            mint: new_mint,
            owner,
            delegate: COption::Some(delegate),
            amount: 10,
            state: TokenAccountState::Initialized,
            is_native: COption::None,
            delegated_amount: 30,
            close_authority: COption::Some(owner),
        };
        TokenAccount::pack(token_account, &mut account_data).unwrap();
        let token_account = AccountSharedData::from(Account {
            lamports: 111,
            data: account_data.to_vec(),
            owner: spl_token_id_v2_0(),
            ..Account::default()
        });
        let token_with_smaller_balance = solana_sdk::pubkey::new_rand();
        bank.store_account(&token_with_smaller_balance, &token_account);

        // Test largest token accounts
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getTokenLargestAccounts","params":["{}"]}}"#,
            new_mint,
        );
        let res = io.handle_request_sync(&req, meta);
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let largest_accounts: Vec<RpcTokenAccountBalance> =
            serde_json::from_value(result["result"]["value"].clone()).unwrap();
        assert_eq!(
            largest_accounts,
            vec![
                RpcTokenAccountBalance {
                    address: token_with_different_mint_pubkey.to_string(),
                    amount: UiTokenAmount {
                        ui_amount: Some(0.42),
                        decimals: 2,
                        amount: "42".to_string(),
                        ui_amount_string: "0.42".to_string(),
                    }
                },
                RpcTokenAccountBalance {
                    address: token_with_smaller_balance.to_string(),
                    amount: UiTokenAmount {
                        ui_amount: Some(0.1),
                        decimals: 2,
                        amount: "10".to_string(),
                        ui_amount_string: "0.1".to_string(),
                    }
                }
            ]
        );
    }

    #[test]
    fn test_token_parsing() {
        let RpcHandler { io, meta, bank, .. } =
            start_rpc_handler_with_tx(&solana_sdk::pubkey::new_rand());

        let mut account_data = vec![0; TokenAccount::get_packed_len()];
        let mint = SplTokenPubkey::new(&[2; 32]);
        let owner = SplTokenPubkey::new(&[3; 32]);
        let delegate = SplTokenPubkey::new(&[4; 32]);
        let token_account = TokenAccount {
            mint,
            owner,
            delegate: COption::Some(delegate),
            amount: 420,
            state: TokenAccountState::Initialized,
            is_native: COption::Some(10),
            delegated_amount: 30,
            close_authority: COption::Some(owner),
        };
        TokenAccount::pack(token_account, &mut account_data).unwrap();
        let token_account = AccountSharedData::from(Account {
            lamports: 111,
            data: account_data.to_vec(),
            owner: spl_token_id_v2_0(),
            ..Account::default()
        });
        let token_account_pubkey = solana_sdk::pubkey::new_rand();
        bank.store_account(&token_account_pubkey, &token_account);

        // Add the mint
        let mut mint_data = vec![0; Mint::get_packed_len()];
        let mint_state = Mint {
            mint_authority: COption::Some(owner),
            supply: 500,
            decimals: 2,
            is_initialized: true,
            freeze_authority: COption::Some(owner),
        };
        Mint::pack(mint_state, &mut mint_data).unwrap();
        let mint_account = AccountSharedData::from(Account {
            lamports: 111,
            data: mint_data.to_vec(),
            owner: spl_token_id_v2_0(),
            ..Account::default()
        });
        bank.store_account(&Pubkey::from_str(&mint.to_string()).unwrap(), &mint_account);

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{}", {{"encoding": "jsonParsed"}}]}}"#,
            token_account_pubkey,
        );
        let res = io.handle_request_sync(&req, meta.clone());
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(
            result["result"]["value"]["data"],
            json!({
                "program": "spl-token",
                "space": TokenAccount::get_packed_len(),
                "parsed": {
                    "type": "account",
                    "info": {
                        "mint": mint.to_string(),
                        "owner": owner.to_string(),
                        "tokenAmount": {
                            "uiAmount": 4.2,
                            "decimals": 2,
                            "amount": "420",
                            "uiAmountString": "4.2",
                        },
                        "delegate": delegate.to_string(),
                        "state": "initialized",
                        "isNative": true,
                        "rentExemptReserve": {
                            "uiAmount": 0.1,
                            "decimals": 2,
                            "amount": "10",
                            "uiAmountString": "0.1",
                        },
                        "delegatedAmount": {
                            "uiAmount": 0.3,
                            "decimals": 2,
                            "amount": "30",
                            "uiAmountString": "0.3",
                        },
                        "closeAuthority": owner.to_string(),
                    }
                }
            })
        );

        // Test Mint
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{}", {{"encoding": "jsonParsed"}}]}}"#,
            mint,
        );
        let res = io.handle_request_sync(&req, meta);
        let result: Value = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        assert_eq!(
            result["result"]["value"]["data"],
            json!({
                "program": "spl-token",
                "space": Mint::get_packed_len(),
                "parsed": {
                    "type": "mint",
                    "info": {
                        "mintAuthority": owner.to_string(),
                        "decimals": 2,
                        "supply": "500".to_string(),
                        "isInitialized": true,
                        "freezeAuthority": owner.to_string(),
                    }
                }
            })
        );
    }

    #[test]
    fn test_get_spl_token_owner_filter() {
        let owner = Pubkey::new_unique();
        assert_eq!(
            get_spl_token_owner_filter(
                &Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap(),
                &[
                    RpcFilterType::Memcmp(Memcmp {
                        offset: 32,
                        bytes: MemcmpEncodedBytes::Binary(owner.to_string()),
                        encoding: None
                    }),
                    RpcFilterType::DataSize(165)
                ],
            )
            .unwrap(),
            owner
        );

        // Filtering on mint instead of owner
        assert!(get_spl_token_owner_filter(
            &Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap(),
            &[
                RpcFilterType::Memcmp(Memcmp {
                    offset: 0,
                    bytes: MemcmpEncodedBytes::Binary(owner.to_string()),
                    encoding: None
                }),
                RpcFilterType::DataSize(165)
            ],
        )
        .is_none());

        // Wrong program id
        assert!(get_spl_token_owner_filter(
            &Pubkey::new_unique(),
            &[
                RpcFilterType::Memcmp(Memcmp {
                    offset: 32,
                    bytes: MemcmpEncodedBytes::Binary(owner.to_string()),
                    encoding: None
                }),
                RpcFilterType::DataSize(165)
            ],
        )
        .is_none());
    }

    #[test]
    fn test_rpc_single_gossip() {
        let exit = Arc::new(AtomicBool::new(false));
        let validator_exit = create_validator_exit(&exit);
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(Blockstore::open(&ledger_path).unwrap());
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::default(),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(100);
        let bank = Bank::new(&genesis_config);

        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank0 = bank_forks.read().unwrap().get(0).unwrap().clone();
        let bank1 = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        bank_forks.write().unwrap().insert(bank1);
        let bank1 = bank_forks.read().unwrap().get(1).unwrap().clone();
        let bank2 = Bank::new_from_parent(&bank1, &Pubkey::default(), 2);
        bank_forks.write().unwrap().insert(bank2);
        let bank2 = bank_forks.read().unwrap().get(2).unwrap().clone();
        let bank3 = Bank::new_from_parent(&bank2, &Pubkey::default(), 3);
        bank_forks.write().unwrap().insert(bank3);

        let optimistically_confirmed_bank =
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);
        let mut pending_optimistically_confirmed_banks = HashSet::new();

        let subscriptions = Arc::new(RpcSubscriptions::new(
            &exit,
            bank_forks.clone(),
            block_commitment_cache.clone(),
            optimistically_confirmed_bank.clone(),
        ));

        let (meta, _receiver) = JsonRpcRequestProcessor::new(
            JsonRpcConfig::default(),
            None,
            bank_forks.clone(),
            block_commitment_cache,
            blockstore,
            validator_exit,
            RpcHealth::stub(),
            cluster_info,
            Hash::default(),
            None,
            optimistically_confirmed_bank.clone(),
            Arc::new(RwLock::new(LargestAccountsCache::new(30))),
            Arc::new(MaxSlots::default()),
            Arc::new(LeaderScheduleCache::default()),
            Arc::new(AtomicU64::default()),
        );

        let mut io = MetaIoHandler::default();
        io.extend_with(rpc_minimal::MinimalImpl.to_delegate());
        io.extend_with(rpc_full::FullImpl.to_delegate());

        let req =
            r#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"confirmed"}]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let slot: Slot = serde_json::from_value(json["result"].clone()).unwrap();
        assert_eq!(slot, 0);

        OptimisticallyConfirmedBankTracker::process_notification(
            BankNotification::OptimisticallyConfirmed(2),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
        );
        let req =
            r#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment": "confirmed"}]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let slot: Slot = serde_json::from_value(json["result"].clone()).unwrap();
        assert_eq!(slot, 2);

        // Test rollback does not appear to happen, even if slots are notified out of order
        OptimisticallyConfirmedBankTracker::process_notification(
            BankNotification::OptimisticallyConfirmed(1),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
        );
        let req =
            r#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment": "confirmed"}]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let slot: Slot = serde_json::from_value(json["result"].clone()).unwrap();
        assert_eq!(slot, 2);

        // Test bank will only be cached when frozen
        OptimisticallyConfirmedBankTracker::process_notification(
            BankNotification::OptimisticallyConfirmed(3),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
        );
        let req =
            r#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment": "confirmed"}]}"#;
        let res = io.handle_request_sync(req, meta.clone());
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let slot: Slot = serde_json::from_value(json["result"].clone()).unwrap();
        assert_eq!(slot, 2);

        // Test freezing an optimistically confirmed bank will update cache
        let bank3 = bank_forks.read().unwrap().get(3).unwrap().clone();
        OptimisticallyConfirmedBankTracker::process_notification(
            BankNotification::Frozen(bank3),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
        );
        let req =
            r#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment": "confirmed"}]}"#;
        let res = io.handle_request_sync(req, meta);
        let json: Value = serde_json::from_str(&res.unwrap()).unwrap();
        let slot: Slot = serde_json::from_value(json["result"].clone()).unwrap();
        assert_eq!(slot, 3);
    }

    #[test]
    fn test_worst_case_encoded_tx_goldens() {
        let ff_tx = vec![0xffu8; PACKET_DATA_SIZE];
        let tx58 = bs58::encode(&ff_tx).into_string();
        assert_eq!(tx58.len(), WORST_CASE_BASE58_TX);
        let tx64 = base64::encode(&ff_tx);
        assert_eq!(tx64.len(), WORST_CASE_BASE64_TX);
    }

    #[test]
    fn test_deserialize_transaction_too_large_payloads_fail() {
        // +2 because +1 still fits in base64 encoded worst-case
        let too_big = PACKET_DATA_SIZE + 2;
        let tx_ser = vec![0xffu8; too_big];
        let tx58 = bs58::encode(&tx_ser).into_string();
        let tx58_len = tx58.len();
        let expect58 = Error::invalid_params(format!(
            "encoded transaction too large: {} bytes (max: encoded/raw {}/{})",
            tx58_len, WORST_CASE_BASE58_TX, PACKET_DATA_SIZE,
        ));
        assert_eq!(
            deserialize_transaction(tx58, UiTransactionEncoding::Base58).unwrap_err(),
            expect58
        );
        let tx64 = base64::encode(&tx_ser);
        let tx64_len = tx64.len();
        let expect64 = Error::invalid_params(format!(
            "encoded transaction too large: {} bytes (max: encoded/raw {}/{})",
            tx64_len, WORST_CASE_BASE64_TX, PACKET_DATA_SIZE,
        ));
        assert_eq!(
            deserialize_transaction(tx64, UiTransactionEncoding::Base64).unwrap_err(),
            expect64
        );
        let too_big = PACKET_DATA_SIZE + 1;
        let tx_ser = vec![0x00u8; too_big];
        let tx58 = bs58::encode(&tx_ser).into_string();
        let expect = Error::invalid_params(format!(
            "transaction too large: {} bytes (max: {} bytes)",
            too_big, PACKET_DATA_SIZE
        ));
        assert_eq!(
            deserialize_transaction(tx58, UiTransactionEncoding::Base58).unwrap_err(),
            expect
        );
        let tx64 = base64::encode(&tx_ser);
        assert_eq!(
            deserialize_transaction(tx64, UiTransactionEncoding::Base64).unwrap_err(),
            expect
        );
    }

    #[test]
    fn test_deserialize_transaction_unsanitary() {
        let unsanitary_tx58 = "ju9xZWuDBX4pRxX2oZkTjxU5jB4SSTgEGhX8bQ8PURNzyzqKMPPpNvWihx8zUe\
             FfrbVNoAaEsNKZvGzAnTDy5bhNT9kt6KFCTBixpvrLCzg4M5UdFUQYrn1gdgjX\
             pLHxcaShD81xBNaFDgnA2nkkdHnKtZt4hVSfKAmw3VRZbjrZ7L2fKZBx21CwsG\
             hD6onjM2M3qZW5C8J6d1pj41MxKmZgPBSha3MyKkNLkAGFASK"
            .to_string();

        let expect58 =
            Error::invalid_params("invalid transaction: index out of bounds".to_string());
        assert_eq!(
            deserialize_transaction(unsanitary_tx58, UiTransactionEncoding::Base58).unwrap_err(),
            expect58
        );
    }
}
