use crate::consensus::VOTE_THRESHOLD_SIZE;
use solana_measure::measure::Measure;
use solana_metrics::inc_new_counter_info;
use solana_runtime::bank::Bank;
use solana_sdk::clock::Slot;
use solana_vote_program::{vote_state::VoteState, vote_state::MAX_LOCKOUT_HISTORY};
use std::{
    collections::{BTreeMap, HashMap},
    sync::atomic::{AtomicBool, Ordering},
    sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender},
    sync::{Arc, RwLock},
    thread::{self, Builder, JoinHandle},
    time::Duration,
};

pub type BlockCommitmentArray = [u64; MAX_LOCKOUT_HISTORY + 1];

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockCommitment {
    pub commitment: BlockCommitmentArray,
}

impl BlockCommitment {
    pub fn increase_confirmation_stake(&mut self, confirmation_count: usize, stake: u64) {
        assert!(confirmation_count > 0 && confirmation_count <= MAX_LOCKOUT_HISTORY);
        self.commitment[confirmation_count - 1] += stake;
    }

    pub fn get_confirmation_stake(&mut self, confirmation_count: usize) -> u64 {
        assert!(confirmation_count > 0 && confirmation_count <= MAX_LOCKOUT_HISTORY);
        self.commitment[confirmation_count - 1]
    }

    pub fn increase_rooted_stake(&mut self, stake: u64) {
        self.commitment[MAX_LOCKOUT_HISTORY] += stake;
    }

    pub fn get_rooted_stake(&self) -> u64 {
        self.commitment[MAX_LOCKOUT_HISTORY]
    }

    #[cfg(test)]
    pub(crate) fn new(commitment: BlockCommitmentArray) -> Self {
        Self { commitment }
    }
}

#[derive(Default)]
pub struct BlockCommitmentCache {
    block_commitment: HashMap<Slot, BlockCommitment>,
    largest_confirmed_root: Slot,
    total_stake: u64,
    bank: Arc<Bank>,
    root: Slot,
}

impl std::fmt::Debug for BlockCommitmentCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCommitmentCache")
            .field("block_commitment", &self.block_commitment)
            .field("total_stake", &self.total_stake)
            .field(
                "bank",
                &format_args!("Bank({{current_slot: {:?}}})", self.bank.slot()),
            )
            .field("root", &self.root)
            .finish()
    }
}

impl BlockCommitmentCache {
    pub fn new(
        block_commitment: HashMap<Slot, BlockCommitment>,
        largest_confirmed_root: Slot,
        total_stake: u64,
        bank: Arc<Bank>,
        root: Slot,
    ) -> Self {
        Self {
            block_commitment,
            largest_confirmed_root,
            total_stake,
            bank,
            root,
        }
    }

    pub fn get_block_commitment(&self, slot: Slot) -> Option<&BlockCommitment> {
        self.block_commitment.get(&slot)
    }

    pub fn largest_confirmed_root(&self) -> Slot {
        self.largest_confirmed_root
    }

    pub fn total_stake(&self) -> u64 {
        self.total_stake
    }

    pub fn bank(&self) -> Arc<Bank> {
        self.bank.clone()
    }

    pub fn slot(&self) -> Slot {
        self.bank.slot()
    }

    pub fn root(&self) -> Slot {
        self.root
    }

    pub fn get_confirmation_count(&self, slot: Slot) -> Option<usize> {
        self.get_lockout_count(slot, VOTE_THRESHOLD_SIZE)
    }

    // Returns the lowest level at which at least `minimum_stake_percentage` of the total epoch
    // stake is locked out
    fn get_lockout_count(&self, slot: Slot, minimum_stake_percentage: f64) -> Option<usize> {
        self.get_block_commitment(slot).map(|block_commitment| {
            let iterator = block_commitment.commitment.iter().enumerate().rev();
            let mut sum = 0;
            for (i, stake) in iterator {
                sum += stake;
                if (sum as f64 / self.total_stake as f64) > minimum_stake_percentage {
                    return i + 1;
                }
            }
            0
        })
    }

    pub fn is_confirmed_rooted(&self, slot: Slot) -> bool {
        if let Some(block_commitment) = self.get_block_commitment(slot) {
            (block_commitment.get_rooted_stake() as f64 / self.total_stake as f64)
                > VOTE_THRESHOLD_SIZE
        } else {
            slot <= self.largest_confirmed_root()
        }
    }

    #[cfg(test)]
    pub fn new_for_tests() -> Self {
        let mut block_commitment: HashMap<Slot, BlockCommitment> = HashMap::new();
        block_commitment.insert(0, BlockCommitment::default());
        Self {
            block_commitment,
            total_stake: 42,
            ..Self::default()
        }
    }
}

pub struct CommitmentAggregationData {
    bank: Arc<Bank>,
    root: Slot,
    total_staked: u64,
}

impl CommitmentAggregationData {
    pub fn new(bank: Arc<Bank>, root: Slot, total_staked: u64) -> Self {
        Self {
            bank,
            root,
            total_staked,
        }
    }
}

pub struct AggregateCommitmentService {
    t_commitment: JoinHandle<()>,
}

impl AggregateCommitmentService {
    pub fn new(
        exit: &Arc<AtomicBool>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    ) -> (Sender<CommitmentAggregationData>, Self) {
        let (sender, receiver): (
            Sender<CommitmentAggregationData>,
            Receiver<CommitmentAggregationData>,
        ) = channel();
        let exit_ = exit.clone();
        (
            sender,
            Self {
                t_commitment: Builder::new()
                    .name("solana-aggregate-stake-lockouts".to_string())
                    .spawn(move || loop {
                        if exit_.load(Ordering::Relaxed) {
                            break;
                        }

                        if let Err(RecvTimeoutError::Disconnected) =
                            Self::run(&receiver, &block_commitment_cache, &exit_)
                        {
                            break;
                        }
                    })
                    .unwrap(),
            },
        )
    }

    fn run(
        receiver: &Receiver<CommitmentAggregationData>,
        block_commitment_cache: &RwLock<BlockCommitmentCache>,
        exit: &Arc<AtomicBool>,
    ) -> Result<(), RecvTimeoutError> {
        loop {
            if exit.load(Ordering::Relaxed) {
                return Ok(());
            }

            let mut aggregation_data = receiver.recv_timeout(Duration::from_secs(1))?;

            while let Ok(new_data) = receiver.try_recv() {
                aggregation_data = new_data;
            }

            let ancestors = aggregation_data.bank.status_cache_ancestors();
            if ancestors.is_empty() {
                continue;
            }

            let mut aggregate_commitment_time = Measure::start("aggregate-commitment-ms");
            let (block_commitment, rooted_stake) =
                Self::aggregate_commitment(&ancestors, &aggregation_data.bank);

            let largest_confirmed_root = rooted_stake
                .into_iter()
                .rev()
                .find(|(_, stake)| {
                    (*stake as f64 / aggregation_data.total_staked as f64) > VOTE_THRESHOLD_SIZE
                })
                .unwrap_or_default()
                .0;

            let mut new_block_commitment = BlockCommitmentCache::new(
                block_commitment,
                largest_confirmed_root,
                aggregation_data.total_staked,
                aggregation_data.bank,
                aggregation_data.root,
            );

            let mut w_block_commitment_cache = block_commitment_cache.write().unwrap();

            std::mem::swap(&mut *w_block_commitment_cache, &mut new_block_commitment);
            aggregate_commitment_time.stop();
            inc_new_counter_info!(
                "aggregate-commitment-ms",
                aggregate_commitment_time.as_ms() as usize
            );
        }
    }

    pub fn aggregate_commitment(
        ancestors: &[Slot],
        bank: &Bank,
    ) -> (HashMap<Slot, BlockCommitment>, BTreeMap<Slot, u64>) {
        assert!(!ancestors.is_empty());

        // Check ancestors is sorted
        for a in ancestors.windows(2) {
            assert!(a[0] < a[1]);
        }

        let mut commitment = HashMap::new();
        let mut rooted_stake = BTreeMap::new();
        for (_, (lamports, account)) in bank.vote_accounts().into_iter() {
            if lamports == 0 {
                continue;
            }
            let vote_state = VoteState::from(&account);
            if vote_state.is_none() {
                continue;
            }

            let vote_state = vote_state.unwrap();
            Self::aggregate_commitment_for_vote_account(
                &mut commitment,
                &mut rooted_stake,
                &vote_state,
                ancestors,
                lamports,
            );
        }

        (commitment, rooted_stake)
    }

    fn aggregate_commitment_for_vote_account(
        commitment: &mut HashMap<Slot, BlockCommitment>,
        rooted_stake: &mut BTreeMap<Slot, u64>,
        vote_state: &VoteState,
        ancestors: &[Slot],
        lamports: u64,
    ) {
        assert!(!ancestors.is_empty());
        let mut ancestors_index = 0;
        if let Some(root) = vote_state.root_slot {
            for (i, a) in ancestors.iter().enumerate() {
                if *a <= root {
                    commitment
                        .entry(*a)
                        .or_insert_with(BlockCommitment::default)
                        .increase_rooted_stake(lamports);
                } else {
                    ancestors_index = i;
                    break;
                }
            }
            let mut insert_amount = lamports;
            for (slot, stake) in rooted_stake.iter_mut() {
                if slot < &root {
                    *stake += lamports;
                } else if slot > &root {
                    insert_amount += *stake;
                    break;
                }
            }
            rooted_stake
                .entry(root)
                .and_modify(|stake| *stake += lamports)
                .or_insert(insert_amount);
        }

        for vote in &vote_state.votes {
            while ancestors[ancestors_index] <= vote.slot {
                commitment
                    .entry(ancestors[ancestors_index])
                    .or_insert_with(BlockCommitment::default)
                    .increase_confirmation_stake(vote.confirmation_count as usize, lamports);
                ancestors_index += 1;

                if ancestors_index == ancestors.len() {
                    return;
                }
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_commitment.join()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo};
    use solana_sdk::pubkey::Pubkey;
    use solana_stake_program::stake_state;
    use solana_vote_program::vote_state::{self, VoteStateVersions};

    #[test]
    fn test_block_commitment() {
        let mut cache = BlockCommitment::default();
        assert_eq!(cache.get_confirmation_stake(1), 0);
        cache.increase_confirmation_stake(1, 10);
        assert_eq!(cache.get_confirmation_stake(1), 10);
        cache.increase_confirmation_stake(1, 20);
        assert_eq!(cache.get_confirmation_stake(1), 30);
    }

    #[test]
    fn test_get_confirmations() {
        let bank = Arc::new(Bank::default());
        // Build BlockCommitmentCache with votes at depths 0 and 1 for 2 slots
        let mut cache0 = BlockCommitment::default();
        cache0.increase_confirmation_stake(1, 5);
        cache0.increase_confirmation_stake(2, 40);

        let mut cache1 = BlockCommitment::default();
        cache1.increase_confirmation_stake(1, 40);
        cache1.increase_confirmation_stake(2, 5);

        let mut cache2 = BlockCommitment::default();
        cache2.increase_confirmation_stake(1, 20);
        cache2.increase_confirmation_stake(2, 5);

        let mut block_commitment = HashMap::new();
        block_commitment.entry(0).or_insert(cache0.clone());
        block_commitment.entry(1).or_insert(cache1.clone());
        block_commitment.entry(2).or_insert(cache2.clone());
        let block_commitment_cache = BlockCommitmentCache::new(block_commitment, 0, 50, bank, 0);

        assert_eq!(block_commitment_cache.get_confirmation_count(0), Some(2));
        assert_eq!(block_commitment_cache.get_confirmation_count(1), Some(1));
        assert_eq!(block_commitment_cache.get_confirmation_count(2), Some(0),);
        assert_eq!(block_commitment_cache.get_confirmation_count(3), None,);
    }

    #[test]
    fn test_is_confirmed_rooted() {
        let bank = Arc::new(Bank::default());
        // Build BlockCommitmentCache with rooted slots
        let mut cache0 = BlockCommitment::default();
        cache0.increase_rooted_stake(50);
        let mut cache1 = BlockCommitment::default();
        cache1.increase_rooted_stake(40);
        let mut cache2 = BlockCommitment::default();
        cache2.increase_rooted_stake(20);

        let mut block_commitment = HashMap::new();
        block_commitment.entry(2).or_insert(cache0.clone());
        block_commitment.entry(3).or_insert(cache1.clone());
        block_commitment.entry(4).or_insert(cache2.clone());
        let largest_confirmed_root = 1;
        let block_commitment_cache =
            BlockCommitmentCache::new(block_commitment, largest_confirmed_root, 50, bank, 0);

        assert!(block_commitment_cache.is_confirmed_rooted(0));
        assert!(block_commitment_cache.is_confirmed_rooted(1));
        assert!(block_commitment_cache.is_confirmed_rooted(2));
        assert!(block_commitment_cache.is_confirmed_rooted(3));
        assert!(!block_commitment_cache.is_confirmed_rooted(4));
    }

    #[test]
    fn test_aggregate_commitment_for_vote_account_1() {
        let ancestors = vec![3, 4, 5, 7, 9, 11];
        let mut commitment = HashMap::new();
        let mut rooted_stake = BTreeMap::new();
        let lamports = 5;
        let mut vote_state = VoteState::default();

        let root = ancestors.last().unwrap();
        vote_state.root_slot = Some(*root);
        AggregateCommitmentService::aggregate_commitment_for_vote_account(
            &mut commitment,
            &mut rooted_stake,
            &vote_state,
            &ancestors,
            lamports,
        );

        for a in ancestors {
            let mut expected = BlockCommitment::default();
            expected.increase_rooted_stake(lamports);
            assert_eq!(*commitment.get(&a).unwrap(), expected);
        }
    }

    #[test]
    fn test_aggregate_commitment_for_vote_account_2() {
        let ancestors = vec![3, 4, 5, 7, 9, 11];
        let mut commitment = HashMap::new();
        let mut rooted_stake = BTreeMap::new();
        let lamports = 5;
        let mut vote_state = VoteState::default();

        let root = ancestors[2];
        vote_state.root_slot = Some(root);
        vote_state.process_slot_vote_unchecked(*ancestors.last().unwrap());
        AggregateCommitmentService::aggregate_commitment_for_vote_account(
            &mut commitment,
            &mut rooted_stake,
            &vote_state,
            &ancestors,
            lamports,
        );

        for a in ancestors {
            if a <= root {
                let mut expected = BlockCommitment::default();
                expected.increase_rooted_stake(lamports);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(1, lamports);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            }
        }
        assert_eq!(rooted_stake.get(&root).unwrap(), &lamports);
    }

    #[test]
    fn test_aggregate_commitment_for_vote_account_3() {
        let ancestors = vec![3, 4, 5, 7, 9, 10, 11];
        let mut commitment = HashMap::new();
        let mut rooted_stake = BTreeMap::new();
        let lamports = 5;
        let mut vote_state = VoteState::default();

        let root = ancestors[2];
        vote_state.root_slot = Some(root);
        assert!(ancestors[4] + 2 >= ancestors[6]);
        vote_state.process_slot_vote_unchecked(ancestors[4]);
        vote_state.process_slot_vote_unchecked(ancestors[6]);
        AggregateCommitmentService::aggregate_commitment_for_vote_account(
            &mut commitment,
            &mut rooted_stake,
            &vote_state,
            &ancestors,
            lamports,
        );

        for (i, a) in ancestors.iter().enumerate() {
            if *a <= root {
                let mut expected = BlockCommitment::default();
                expected.increase_rooted_stake(lamports);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else if i <= 4 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(2, lamports);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else if i <= 6 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(1, lamports);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            }
        }
        assert_eq!(rooted_stake.get(&root).unwrap(), &lamports);
    }

    #[test]
    fn test_aggregate_commitment_validity() {
        let ancestors = vec![3, 4, 5, 7, 9, 10, 11];
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config(10_000);

        let rooted_stake_amount = 33;

        let sk1 = Pubkey::new_rand();
        let pk1 = Pubkey::new_rand();
        let mut vote_account1 = vote_state::create_account(&pk1, &Pubkey::new_rand(), 0, 100);
        let stake_account1 =
            stake_state::create_account(&sk1, &pk1, &vote_account1, &genesis_config.rent, 100);
        let sk2 = Pubkey::new_rand();
        let pk2 = Pubkey::new_rand();
        let mut vote_account2 = vote_state::create_account(&pk2, &Pubkey::new_rand(), 0, 50);
        let stake_account2 =
            stake_state::create_account(&sk2, &pk2, &vote_account2, &genesis_config.rent, 50);
        let sk3 = Pubkey::new_rand();
        let pk3 = Pubkey::new_rand();
        let mut vote_account3 = vote_state::create_account(&pk3, &Pubkey::new_rand(), 0, 1);
        let stake_account3 = stake_state::create_account(
            &sk3,
            &pk3,
            &vote_account3,
            &genesis_config.rent,
            rooted_stake_amount,
        );
        let sk4 = Pubkey::new_rand();
        let pk4 = Pubkey::new_rand();
        let mut vote_account4 = vote_state::create_account(&pk4, &Pubkey::new_rand(), 0, 1);
        let stake_account4 = stake_state::create_account(
            &sk4,
            &pk4,
            &vote_account4,
            &genesis_config.rent,
            rooted_stake_amount,
        );

        genesis_config.accounts.extend(vec![
            (pk1, vote_account1.clone()),
            (sk1, stake_account1),
            (pk2, vote_account2.clone()),
            (sk2, stake_account2),
            (pk3, vote_account3.clone()),
            (sk3, stake_account3),
            (pk4, vote_account4.clone()),
            (sk4, stake_account4),
        ]);

        // Create bank
        let bank = Arc::new(Bank::new(&genesis_config));

        let mut vote_state1 = VoteState::from(&vote_account1).unwrap();
        vote_state1.process_slot_vote_unchecked(3);
        vote_state1.process_slot_vote_unchecked(5);
        let versioned = VoteStateVersions::Current(Box::new(vote_state1));
        VoteState::to(&versioned, &mut vote_account1).unwrap();
        bank.store_account(&pk1, &vote_account1);

        let mut vote_state2 = VoteState::from(&vote_account2).unwrap();
        vote_state2.process_slot_vote_unchecked(9);
        vote_state2.process_slot_vote_unchecked(10);
        let versioned = VoteStateVersions::Current(Box::new(vote_state2));
        VoteState::to(&versioned, &mut vote_account2).unwrap();
        bank.store_account(&pk2, &vote_account2);

        let mut vote_state3 = VoteState::from(&vote_account3).unwrap();
        vote_state3.root_slot = Some(1);
        let versioned = VoteStateVersions::Current(Box::new(vote_state3));
        VoteState::to(&versioned, &mut vote_account3).unwrap();
        bank.store_account(&pk3, &vote_account3);

        let mut vote_state4 = VoteState::from(&vote_account4).unwrap();
        vote_state4.root_slot = Some(2);
        let versioned = VoteStateVersions::Current(Box::new(vote_state4));
        VoteState::to(&versioned, &mut vote_account4).unwrap();
        bank.store_account(&pk4, &vote_account4);

        let (commitment, rooted_stake) =
            AggregateCommitmentService::aggregate_commitment(&ancestors, &bank);

        for a in ancestors {
            if a <= 3 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(2, 150);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else if a <= 5 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(1, 100);
                expected.increase_confirmation_stake(2, 50);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else if a <= 9 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(2, 50);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else if a <= 10 {
                let mut expected = BlockCommitment::default();
                expected.increase_confirmation_stake(1, 50);
                assert_eq!(*commitment.get(&a).unwrap(), expected);
            } else {
                assert!(commitment.get(&a).is_none());
            }
        }
        assert_eq!(*rooted_stake.get(&1).unwrap(), 2 * rooted_stake_amount);
        assert_eq!(*rooted_stake.get(&2).unwrap(), rooted_stake_amount);
    }
}
