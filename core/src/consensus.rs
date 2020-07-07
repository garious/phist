use crate::{
    progress_map::{LockoutIntervals, ProgressMap},
    pubkey_references::PubkeyReferences,
};
use chrono::prelude::*;
use solana_ledger::{blockstore::Blockstore, blockstore_db};
use solana_measure::measure::Measure;
use solana_runtime::{bank::Bank, bank_forks::BankForks, commitment::VOTE_THRESHOLD_SIZE};
use solana_sdk::{
    account::Account,
    clock::{Slot, UnixTimestamp},
    hash::Hash,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    slot_history::SlotHistory,
};
use solana_vote_program::{
    vote_instruction,
    vote_state::{BlockTimestamp, Lockout, Vote, VoteState, MAX_LOCKOUT_HISTORY},
};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::BufReader,
    ops::Bound::{Included, Unbounded},
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;

#[derive(PartialEq, Clone, Debug)]
pub enum SwitchForkDecision {
    SwitchProof(Hash),
    NoSwitch,
    FailedSwitchThreshold,
}

impl SwitchForkDecision {
    pub fn to_vote_instruction(
        &self,
        vote: Vote,
        vote_account_pubkey: &Pubkey,
        authorized_voter_pubkey: &Pubkey,
    ) -> Option<Instruction> {
        match self {
            SwitchForkDecision::FailedSwitchThreshold => None,
            SwitchForkDecision::NoSwitch => Some(vote_instruction::vote(
                vote_account_pubkey,
                authorized_voter_pubkey,
                vote,
            )),
            SwitchForkDecision::SwitchProof(switch_proof_hash) => {
                Some(vote_instruction::vote_switch(
                    vote_account_pubkey,
                    authorized_voter_pubkey,
                    vote,
                    *switch_proof_hash,
                ))
            }
        }
    }
}

pub const VOTE_THRESHOLD_DEPTH: usize = 8;
pub const SWITCH_FORK_THRESHOLD: f64 = 0.38;

pub type Result<T> = std::result::Result<T, TowerError>;

pub type Stake = u64;
pub type VotedStakes = HashMap<Slot, Stake>;
pub type PubkeyVotes = Vec<(Pubkey, Slot)>;

pub(crate) struct ComputedBankState {
    pub voted_stakes: VotedStakes,
    pub total_stake: Stake,
    pub bank_weight: u128,
    // Tree of intervals of lockouts of the form [slot, slot + slot.lockout],
    // keyed by end of the range
    pub lockout_intervals: LockoutIntervals,
    pub pubkey_votes: Arc<PubkeyVotes>,
}

#[frozen_abi(digest = "2ZUeCLMVQxmHYbeqMH7M97ifVSKoVErGvRHzyxcQRjgU")]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, AbiExample)]
pub struct Tower {
    node_pubkey: Pubkey,
    threshold_depth: usize,
    threshold_size: f64,
    lockouts: VoteState,
    last_vote: Vote,
    last_timestamp: BlockTimestamp,
    #[serde(skip)]
    path: PathBuf,
    #[serde(skip)]
    tmp_path: PathBuf, // used before atomic fs::rename()
    #[serde(skip)]
    stray_restored_slots: HashSet<Slot>,
}

impl Default for Tower {
    fn default() -> Self {
        Self {
            node_pubkey: Pubkey::default(),
            threshold_depth: VOTE_THRESHOLD_DEPTH,
            threshold_size: VOTE_THRESHOLD_SIZE,
            lockouts: VoteState::default(),
            last_vote: Vote::default(),
            last_timestamp: BlockTimestamp::default(),
            path: PathBuf::default(),
            tmp_path: PathBuf::default(),
            stray_restored_slots: HashSet::default(),
        }
    }
}

impl Tower {
    pub fn new(
        node_pubkey: &Pubkey,
        vote_account_pubkey: &Pubkey,
        root: Slot,
        heaviest_bank: &Bank,
        path: &Path,
    ) -> Self {
        let path = Self::get_filename(&path, node_pubkey);
        let tmp_path = Self::get_tmp_filename(&path);
        let mut tower = Self {
            node_pubkey: *node_pubkey,
            path,
            tmp_path,
            ..Tower::default()
        };
        tower.initialize_lockouts_from_bank_forks(vote_account_pubkey, root, heaviest_bank);

        tower
    }

    #[cfg(test)]
    pub fn new_with_key(node_pubkey: &Pubkey) -> Self {
        Self {
            node_pubkey: *node_pubkey,
            ..Tower::default()
        }
    }

    #[cfg(test)]
    pub fn new_for_tests(threshold_depth: usize, threshold_size: f64) -> Self {
        Self {
            threshold_depth,
            threshold_size,
            ..Tower::default()
        }
    }

    pub fn new_from_bankforks(
        bank_forks: &BankForks,
        ledger_path: &Path,
        my_pubkey: &Pubkey,
        vote_account: &Pubkey,
    ) -> Self {
        let root_bank = bank_forks.root_bank();
        let (_progress, heaviest_subtree_fork_choice, unlock_heaviest_subtree_fork_choice_slot) =
            crate::replay_stage::ReplayStage::initialize_progress_and_fork_choice(
                root_bank,
                bank_forks.frozen_banks().values().cloned().collect(),
                &my_pubkey,
                &vote_account,
            );
        let root = root_bank.slot();

        let heaviest_bank = if root > unlock_heaviest_subtree_fork_choice_slot {
            bank_forks
                .get(heaviest_subtree_fork_choice.best_overall_slot())
                .expect("The best overall slot must be one of `frozen_banks` which all exist in bank_forks")
                .clone()
        } else {
            Tower::find_heaviest_bank(&bank_forks, &my_pubkey).unwrap_or_else(|| root_bank.clone())
        };

        Self::new(
            &my_pubkey,
            &vote_account,
            root,
            &heaviest_bank,
            &ledger_path,
        )
    }

    pub(crate) fn collect_vote_lockouts<F>(
        node_pubkey: &Pubkey,
        bank_slot: Slot,
        vote_accounts: F,
        ancestors: &HashMap<Slot, HashSet<Slot>>,
        all_pubkeys: &mut PubkeyReferences,
    ) -> ComputedBankState
    where
        F: Iterator<Item = (Pubkey, (u64, Account))>,
    {
        let mut voted_stakes = HashMap::new();
        let mut total_stake = 0;
        let mut bank_weight = 0;
        // Tree of intervals of lockouts of the form [slot, slot + slot.lockout],
        // keyed by end of the range
        let mut lockout_intervals = LockoutIntervals::new();
        let mut pubkey_votes = vec![];
        for (key, (voted_stake, account)) in vote_accounts {
            if voted_stake == 0 {
                continue;
            }
            trace!("{} {} with stake {}", node_pubkey, key, voted_stake);
            let vote_state = VoteState::from(&account);
            if vote_state.is_none() {
                datapoint_warn!(
                    "tower_warn",
                    (
                        "warn",
                        format!("Unable to get vote_state from account {}", key),
                        String
                    ),
                );
                continue;
            }
            let mut vote_state = vote_state.unwrap();

            for vote in &vote_state.votes {
                let key = all_pubkeys.get_or_insert(&key);
                lockout_intervals
                    .entry(vote.expiration_slot())
                    .or_insert_with(Vec::new)
                    .push((vote.slot, key));
            }

            if key == *node_pubkey || vote_state.node_pubkey == *node_pubkey {
                debug!("vote state {:?}", vote_state);
                debug!(
                    "observed slot {}",
                    vote_state.nth_recent_vote(0).map(|v| v.slot).unwrap_or(0) as i64
                );
                debug!("observed root {}", vote_state.root_slot.unwrap_or(0) as i64);
                datapoint_info!(
                    "tower-observed",
                    (
                        "slot",
                        vote_state.nth_recent_vote(0).map(|v| v.slot).unwrap_or(0),
                        i64
                    ),
                    ("root", vote_state.root_slot.unwrap_or(0), i64)
                );
            }
            let start_root = vote_state.root_slot;

            // Add the last vote to update the `heaviest_subtree_fork_choice`
            if let Some(last_voted_slot) = vote_state.last_voted_slot() {
                pubkey_votes.push((key, last_voted_slot));
            }

            vote_state.process_slot_vote_unchecked(bank_slot);

            for vote in &vote_state.votes {
                bank_weight += vote.lockout() as u128 * voted_stake as u128;
                Self::populate_ancestor_voted_stakes(&mut voted_stakes, &vote, ancestors);
            }

            if start_root != vote_state.root_slot {
                if let Some(root) = start_root {
                    let vote = Lockout {
                        confirmation_count: MAX_LOCKOUT_HISTORY as u32,
                        slot: root,
                    };
                    trace!("ROOT: {}", vote.slot);
                    bank_weight += vote.lockout() as u128 * voted_stake as u128;
                    Self::populate_ancestor_voted_stakes(&mut voted_stakes, &vote, ancestors);
                }
            }
            if let Some(root) = vote_state.root_slot {
                let vote = Lockout {
                    confirmation_count: MAX_LOCKOUT_HISTORY as u32,
                    slot: root,
                };
                bank_weight += vote.lockout() as u128 * voted_stake as u128;
                Self::populate_ancestor_voted_stakes(&mut voted_stakes, &vote, ancestors);
            }

            // The last vote in the vote stack is a simulated vote on bank_slot, which
            // we added to the vote stack earlier in this function by calling process_vote().
            // We don't want to update the ancestors stakes of this vote b/c it does not
            // represent an actual vote by the validator.

            // Note: It should not be possible for any vote state in this bank to have
            // a vote for a slot >= bank_slot, so we are guaranteed that the last vote in
            // this vote stack is the simulated vote, so this fetch should be sufficient
            // to find the last unsimulated vote.
            assert_eq!(
                vote_state.nth_recent_vote(0).map(|l| l.slot),
                Some(bank_slot)
            );
            if let Some(vote) = vote_state.nth_recent_vote(1) {
                // Update all the parents of this last vote with the stake of this vote account
                Self::update_ancestor_voted_stakes(
                    &mut voted_stakes,
                    vote.slot,
                    voted_stake,
                    ancestors,
                );
            }
            total_stake += voted_stake;
        }

        ComputedBankState {
            voted_stakes,
            total_stake,
            bank_weight,
            lockout_intervals,
            pubkey_votes: Arc::new(pubkey_votes),
        }
    }

    pub fn is_slot_confirmed(
        &self,
        slot: Slot,
        voted_stakes: &VotedStakes,
        total_stake: Stake,
    ) -> bool {
        voted_stakes
            .get(&slot)
            .map(|stake| (*stake as f64 / total_stake as f64) > self.threshold_size)
            .unwrap_or(false)
    }

    fn new_vote(
        local_vote_state: &VoteState,
        slot: Slot,
        hash: Hash,
        last_voted_slot_in_bank: Option<Slot>,
    ) -> (Vote, usize) {
        let mut local_vote_state = local_vote_state.clone();
        let vote = Vote::new(vec![slot], hash);
        local_vote_state.process_vote_unchecked(&vote);
        let slots = if let Some(last_voted_slot_in_bank) = last_voted_slot_in_bank {
            local_vote_state
                .votes
                .iter()
                .map(|v| v.slot)
                .skip_while(|s| *s <= last_voted_slot_in_bank)
                .collect()
        } else {
            local_vote_state.votes.iter().map(|v| v.slot).collect()
        };
        trace!(
            "new vote with {:?} {:?} {:?}",
            last_voted_slot_in_bank,
            slots,
            local_vote_state.votes
        );
        (Vote::new(slots, hash), local_vote_state.votes.len() - 1)
    }

    fn last_voted_slot_in_bank(bank: &Bank, vote_account_pubkey: &Pubkey) -> Option<Slot> {
        let vote_account = bank.vote_accounts().get(vote_account_pubkey)?.1.clone();
        let bank_vote_state = VoteState::deserialize(&vote_account.data).ok()?;
        bank_vote_state.last_voted_slot()
    }

    pub fn new_vote_from_bank(&self, bank: &Bank, vote_account_pubkey: &Pubkey) -> (Vote, usize) {
        let voted_slot = Self::last_voted_slot_in_bank(bank, vote_account_pubkey);
        Self::new_vote(&self.lockouts, bank.slot(), bank.hash(), voted_slot)
    }

    pub fn record_bank_vote(&mut self, vote: Vote) -> Option<Slot> {
        let slot = vote.last_voted_slot().unwrap_or(0);
        trace!("{} record_vote for {}", self.node_pubkey, slot);
        let root_slot = self.lockouts.root_slot;
        self.lockouts.process_vote_unchecked(&vote);
        self.last_vote = vote;

        datapoint_info!(
            "tower-vote",
            ("latest", slot, i64),
            ("root", self.lockouts.root_slot.unwrap_or(0), i64)
        );
        if root_slot != self.lockouts.root_slot {
            Some(self.lockouts.root_slot.unwrap())
        } else {
            None
        }
    }

    #[cfg(test)]
    pub fn record_vote(&mut self, slot: Slot, hash: Hash) -> Option<Slot> {
        let vote = Vote::new(vec![slot], hash);
        self.record_bank_vote(vote)
    }

    pub fn last_vote(&self) -> &Vote {
        &self.last_vote
    }

    pub fn last_voted_slot(&self) -> Option<Slot> {
        self.last_vote().last_voted_slot()
    }

    pub fn last_vote_and_timestamp(&mut self) -> Vote {
        let mut last_vote = self.last_vote.clone();
        last_vote.timestamp = self.maybe_timestamp(last_vote.last_voted_slot().unwrap_or(0));
        last_vote
    }

    fn maybe_timestamp(&mut self, current_slot: Slot) -> Option<UnixTimestamp> {
        if current_slot > self.last_timestamp.slot
            || self.last_timestamp.slot == 0 && current_slot == self.last_timestamp.slot
        {
            let timestamp = Utc::now().timestamp();
            if timestamp >= self.last_timestamp.timestamp {
                self.last_timestamp = BlockTimestamp {
                    slot: current_slot,
                    timestamp,
                };
                return Some(timestamp);
            }
        }
        None
    }

    pub fn root(&self) -> Option<Slot> {
        self.lockouts.root_slot
    }

    // a slot is recent if it's newer than the last vote we have
    pub fn is_recent(&self, slot: Slot) -> bool {
        if let Some(last_voted_slot) = self.lockouts.last_voted_slot() {
            if slot <= last_voted_slot {
                return false;
            }
        }
        true
    }

    pub fn has_voted(&self, slot: Slot) -> bool {
        for vote in &self.lockouts.votes {
            if slot == vote.slot {
                return true;
            }
        }
        false
    }

    pub fn is_locked_out(&self, slot: Slot, ancestors: &HashMap<Slot, HashSet<Slot>>) -> bool {
        assert!(ancestors.contains_key(&slot));

        if !self.is_recent(slot) {
            return true;
        }

        let mut lockouts = self.lockouts.clone();
        lockouts.process_slot_vote_unchecked(slot);
        for vote in &lockouts.votes {
            if vote.slot == slot {
                continue;
            }
            if !ancestors[&slot].contains(&vote.slot) {
                return true;
            }
        }
        if let Some(root_slot) = lockouts.root_slot {
            // This case should never happen because bank forks purges all
            // non-descendants of the root every time root is set
            if slot != root_slot {
                assert!(ancestors[&slot].contains(&root_slot));
            }
        }

        false
    }

    pub(crate) fn check_switch_threshold(
        &self,
        switch_slot: u64,
        ancestors: &HashMap<Slot, HashSet<u64>>,
        descendants: &HashMap<Slot, HashSet<u64>>,
        progress: &ProgressMap,
        total_stake: u64,
        epoch_vote_accounts: &HashMap<Pubkey, (u64, Account)>,
    ) -> SwitchForkDecision {
        self.last_voted_slot()
            .map(|last_voted_slot| {
                let last_vote_ancestors = if self.is_stray_last_vote() {
                    // Use stray restored slots because we can't derive them from given ancestors (=bank_forks)
                    &self.stray_restored_slots
                } else {
                    ancestors.get(&last_voted_slot).unwrap()
                };

                let switch_slot_ancestors = ancestors.get(&switch_slot).unwrap();

                if switch_slot == last_voted_slot || switch_slot_ancestors.contains(&last_voted_slot) {
                    // If the `switch_slot is a descendant of the last vote,
                    // no switching proof is necessary
                    return SwitchForkDecision::NoSwitch;
                }

                // Should never consider switching to an ancestor
                // of your last vote
                assert!(!last_vote_ancestors.contains(&switch_slot));

                // By this point, we know the `switch_slot` is on a different fork
                // (is neither an ancestor nor descendant of `last_vote`), so a
                // switching proof is necessary
                let switch_proof = Hash::default();
                let mut locked_out_stake = 0;
                let mut locked_out_vote_accounts = HashSet::new();
                for (candidate_slot, descendants) in descendants.iter() {
                    // 1) Don't consider any banks that haven't been frozen yet
                    //    because the needed stats are unavailable
                    // 2) Only consider lockouts at the latest `frozen` bank
                    //    on each fork, as that bank will contain all the
                    //    lockout intervals for ancestors on that fork as well.
                    // 3) Don't consider lockouts on the `last_vote` itself
                    // 4) Don't consider lockouts on any descendants of
                    //    `last_vote`
                    // 5) Don't consider any banks before the root because
                    //    all lockouts must be ancestors of `last_vote`
                    if !progress.get_fork_stats(*candidate_slot).map(|stats| stats.computed).unwrap_or(false)
                        // If any of the descendants have the `computed` flag set, then there must be a more
                        // recent frozen bank on this fork to use, so we can ignore this one. Otherwise,
                        // even if this bank has descendants, if they have not yet been frozen / stats computed,
                        // then use this bank as a representative for the fork.
                        || descendants.iter().any(|d| progress.get_fork_stats(*d).map(|stats| stats.computed).unwrap_or(false))
                        || *candidate_slot == last_voted_slot
                        || ancestors
                            .get(&candidate_slot)
                            .expect(
                                "empty descendants implies this is a child, not parent of root, so must
                                exist in the ancestors map",
                            )
                            .contains(&last_voted_slot)
                        || *candidate_slot <= root
                    {
                        continue;
                    }

                    // By the time we reach here, any ancestors of the `last_vote`,
                    // should have been filtered out, as they all have a descendant,
                    // namely the `last_vote` itself.
                    assert!(!last_vote_ancestors.contains(candidate_slot));

                    // Evaluate which vote accounts in the bank are locked out
                    // in the interval candidate_slot..last_vote, which means
                    // finding any lockout intervals in the `lockout_intervals` tree
                    // for this bank that contain `last_vote`.
                    let lockout_intervals = &progress
                        .get(&candidate_slot)
                        .unwrap()
                        .fork_stats
                        .lockout_intervals;
                    // Find any locked out intervals in this bank with endpoint >= last_vote,
                    // implies they are locked out at last_vote
                    for (_lockout_interval_end, intervals_keyed_by_end) in lockout_intervals.range((Included(last_voted_slot), Unbounded)) {
                        for (lockout_interval_start, vote_account_pubkey) in intervals_keyed_by_end {
                            if locked_out_vote_accounts.contains(vote_account_pubkey) {
                                continue;
                            }

                            // Only count lockouts on slots that are:
                            // 1) Not ancestors of `last_vote`
                            // 2) Not from before the current root as we can't determine if
                            // anything before the root was an ancestor of `last_vote` or not
                            if !last_vote_ancestors.contains(lockout_interval_start)
                                // Given a `lockout_interval_start` < root that appears in a
                                // bank for a `candidate_slot`, it must be that `lockout_interval_start`
                                // is an ancestor of the current root, because `candidate_slot` is a
                                // descendant of the current root
                                && *lockout_interval_start > root
                            {
                                let stake = epoch_vote_accounts
                                    .get(vote_account_pubkey)
                                    .map(|(stake, _)| *stake)
                                    .unwrap_or(0);
                                locked_out_stake += stake;
                                locked_out_vote_accounts.insert(vote_account_pubkey);
                            }
                        }
                    }
                }

                if (locked_out_stake as f64 / total_stake as f64) > SWITCH_FORK_THRESHOLD {
                    SwitchForkDecision::SwitchProof(switch_proof)
                } else {
                    SwitchForkDecision::FailedSwitchThreshold
                }
            })
            .unwrap_or(SwitchForkDecision::NoSwitch)
    }

    pub fn check_vote_stake_threshold(
        &self,
        slot: Slot,
        voted_stakes: &VotedStakes,
        total_stake: Stake,
    ) -> bool {
        let mut lockouts = self.lockouts.clone();
        lockouts.process_slot_vote_unchecked(slot);
        let vote = lockouts.nth_recent_vote(self.threshold_depth);
        if let Some(vote) = vote {
            if let Some(fork_stake) = voted_stakes.get(&vote.slot) {
                let lockout = *fork_stake as f64 / total_stake as f64;
                trace!(
                    "fork_stake slot: {}, vote slot: {}, lockout: {} fork_stake: {} total_stake: {}",
                    slot, vote.slot, lockout, fork_stake, total_stake
                );
                if vote.confirmation_count as usize > self.threshold_depth {
                    for old_vote in &self.lockouts.votes {
                        if old_vote.slot == vote.slot
                            && old_vote.confirmation_count == vote.confirmation_count
                        {
                            return true;
                        }
                    }
                }
                lockout > self.threshold_size
            } else {
                false
            }
        } else {
            true
        }
    }

    /// Update lockouts for all the ancestors
    pub(crate) fn populate_ancestor_voted_stakes(
        voted_stakes: &mut VotedStakes,
        vote: &Lockout,
        ancestors: &HashMap<Slot, HashSet<Slot>>,
    ) {
        // If there's no ancestors, that means this slot must be from before the current root,
        // in which case the lockouts won't be calculated in bank_weight anyways, so ignore
        // this slot
        let vote_slot_ancestors = ancestors.get(&vote.slot);
        if vote_slot_ancestors.is_none() {
            return;
        }
        let mut slot_with_ancestors = vec![vote.slot];
        slot_with_ancestors.extend(vote_slot_ancestors.unwrap());
        for slot in slot_with_ancestors {
            voted_stakes.entry(slot).or_default();
        }
    }

    pub(crate) fn find_heaviest_bank(
        bank_forks: &BankForks,
        node_pubkey: &Pubkey,
    ) -> Option<Arc<Bank>> {
        let ancestors = bank_forks.ancestors();
        let mut bank_weights: Vec<_> = bank_forks
            .frozen_banks()
            .values()
            .map(|b| {
                (
                    Self::bank_weight(node_pubkey, b, &ancestors),
                    b.parents().len(),
                    b.clone(),
                )
            })
            .collect();
        bank_weights.sort_by_key(|b| (b.0, b.1));
        bank_weights.pop().map(|b| b.2)
    }

    /// Update stake for all the ancestors.
    /// Note, stake is the same for all the ancestor.
    fn update_ancestor_voted_stakes(
        voted_stakes: &mut VotedStakes,
        voted_slot: Slot,
        voted_stake: u64,
        ancestors: &HashMap<Slot, HashSet<Slot>>,
    ) {
        // If there's no ancestors, that means this slot must be from
        // before the current root, so ignore this slot
        let vote_slot_ancestors = ancestors.get(&voted_slot);
        if vote_slot_ancestors.is_none() {
            return;
        }
        let mut slot_with_ancestors = vec![voted_slot];
        slot_with_ancestors.extend(vote_slot_ancestors.unwrap());
        for slot in slot_with_ancestors {
            let current = voted_stakes.entry(slot).or_default();
            *current += voted_stake;
        }
    }

    fn bank_weight(
        node_pubkey: &Pubkey,
        bank: &Bank,
        ancestors: &HashMap<Slot, HashSet<Slot>>,
    ) -> u128 {
        let ComputedBankState { bank_weight, .. } = Self::collect_vote_lockouts(
            node_pubkey,
            bank.slot(),
            bank.vote_accounts().into_iter(),
            ancestors,
            &mut PubkeyReferences::default(),
        );
        bank_weight
    }

    fn voted_slots(&self) -> Vec<Slot> {
        self.lockouts
            .votes
            .iter()
            .map(|lockout| lockout.slot)
            .collect()
    }

    pub fn is_stray_last_vote(&self) -> bool {
        if let Some(last_voted_slot) = self.last_voted_slot() {
            self.stray_restored_slots.contains(&last_voted_slot)
        } else {
            false
        }
    }

    // The tower root can be older/newer if the validator booted from a newer/older snapshot, so
    // tower lockouts may need adjustment
    pub fn adjust_lockouts_after_replay(
        mut self,
        replayed_root_slot: Slot,
        slot_history: &SlotHistory,
    ) -> Result<Self> {
        info!("adjusting lockouts after replay up to {}: {:?}", replayed_root_slot, self.voted_slots());

        assert_eq!(slot_history.check(replayed_root_slot), Check::Found);
        // reconcile_blockstore_roots_with_tower() should already have aligned these.
        assert!(
            self.root().is_none() || self.root().unwrap() <= replayed_root_slot,
            format!(
                "tower root: {:?} >= replayed root slot: {}",
                self.root().unwrap(),
                replayed_root_slot
            )
        );
        assert!(
            self.last_vote == Vote::default() && self.lockouts.votes.is_empty()
                || self.last_vote != Vote::default() && !self.lockouts.votes.is_empty(),
            format!(
                "last vote: {:?} lockouts.votes: {:?}",
                self.last_vote, self.lockouts.votes
            )
        );

        use solana_sdk::slot_history::Check;

        // return immediately if votes are empty...
        if self.lockouts.votes.is_empty() {
            assert_eq!(self.root(), None);
            return Ok(self);
        }

        let last_voted_slot = self.last_voted_slot().unwrap();
        if slot_history.check(last_voted_slot) == Check::TooOld {
            // We could try hard to anchor with other older votes, but opt to simplify the
            // following logic
            return Err(TowerError::TooOld(last_voted_slot, slot_history.oldest()));
        }

        // only divergent slots will be retained
        let mut retain_flags_for_each_vote_in_reverse: Vec<_> = Vec::with_capacity(self.lockouts.votes.len());
        let mut still_in_future = true;
        let mut past_outside_history = false;
        let mut found = false;

        // iterate over votes in the newest => oldest order
        // bail out early if bad condition is found
        for vote in self.lockouts.votes.iter().rev() {
            let check = slot_history.check(vote.slot);

            if !found && check == Check::Found {
                found = true;
            } else if found && check == Check::NotFound {
                // this can't happen unless we're fed with bogus snapshot
                return Err(TowerError::InconsistentWithSlotHistory(
                    "diverged ancestor?".to_owned(),
                ));
            }

            if still_in_future && check != Check::Future {
                still_in_future = false;
            } else if !still_in_future && check == Check::Future {
                // really odd cases: bad ordered votes?
                return Err(TowerError::InconsistentWithSlotHistory(
                    "time warmped?".to_owned(),
                ));
            }
            if !past_outside_history && check == Check::TooOld {
                past_outside_history = true;
            } else if past_outside_history && check != Check::TooOld {
                // really odd cases: bad ordered votes?
                return Err(TowerError::InconsistentWithSlotHistory(
                    "not too old once after got too old?".to_owned(),
                ));
            }

            retain_flags_for_each_vote_in_reverse.push(!found);
        }
        let mut retain_flags_for_each_vote = retain_flags_for_each_vote_in_reverse.into_iter().rev();

        self.lockouts
            .votes
            .retain(move |_| retain_flags_for_each_vote.next().unwrap());

        if self.lockouts.votes.is_empty() {
            info!("All restored votes were behind replayed_root_slot; resetting root_slot and last_vote in tower!");

            self.lockouts.root_slot = None;
            self.last_vote = Vote::default();
        } else {
            info!("Some restored votes were on different fork: {:?}!", self.voted_slots());

            self.lockouts.root_slot = Some(replayed_root_slot);
            assert_eq!(
                self.last_vote.last_voted_slot().unwrap(),
                *self.voted_slots().last().unwrap()
            );
            // should call self.votes.pop_expired_votes()?
            self.stray_restored_slots = self.voted_slots().into_iter().collect();
        }

        Ok(self)
    }

    fn initialize_lockouts_from_bank_forks(
        &mut self,
        vote_account_pubkey: &Pubkey,
        root: Slot,
        heaviest_bank: &Bank,
    ) {
        if let Some((_stake, vote_account)) = heaviest_bank.vote_accounts().get(vote_account_pubkey)
        {
            let mut vote_state = VoteState::deserialize(&vote_account.data)
                .expect("vote_account isn't a VoteState?");
            vote_state.root_slot = Some(root);
            vote_state.votes.retain(|v| v.slot > root);
            trace!(
                "{} lockouts initialized to {:?}",
                self.node_pubkey,
                vote_state
            );
            assert_eq!(
                vote_state.node_pubkey, self.node_pubkey,
                "vote account's node_pubkey doesn't match",
            );
            self.lockouts = vote_state;
        } else {
            info!(
                "vote account({}) not found in heaviest bank (slot={})",
                vote_account_pubkey,
                heaviest_bank.slot()
            );
        }
    }

    pub fn get_filename(path: &Path, node_pubkey: &Pubkey) -> PathBuf {
        path.join(format!("tower-{}", node_pubkey))
            .with_extension("bin")
    }

    pub fn get_tmp_filename(path: &Path) -> PathBuf {
        path.with_extension("bin.new")
    }

    pub fn save(&self, node_keypair: &Arc<Keypair>) -> Result<()> {
        let mut measure = Measure::start("tower_save-ms");

        if self.node_pubkey != node_keypair.pubkey() {
            return Err(TowerError::WrongTower(format!(
                "node_pubkey is {:?} but found tower for {:?}",
                node_keypair.pubkey(),
                self.node_pubkey
            )));
        }

        let filename = &self.path;
        let new_filename = &self.tmp_path;
        {
            // overwrite anything if exists
            let mut file = File::create(&new_filename)?;
            let saved_tower = SavedTower::new(self, node_keypair)?;
            bincode::serialize_into(&mut file, &saved_tower)?;
            // file.sync_all() hurts performance; pipeline sync-ing and submitting votes to the cluster!
        }
        fs::rename(&new_filename, &filename)?;
        // self.path.parent().sync_all() hurts performance; pipeline sync-ing and submitting votes to the cluster!

        measure.stop();
        inc_new_counter_info!("tower_save-ms", measure.as_ms() as usize);

        Ok(())
    }

    pub fn restore(path: &Path, node_pubkey: &Pubkey) -> Result<Self> {
        let filename = Self::get_filename(path, node_pubkey);

        // Ensure to create parent dir here, because restore() precedes save() always
        fs::create_dir_all(&filename.parent().unwrap())?;

        let file = File::open(&filename)?;
        let mut stream = BufReader::new(file);

        let saved_tower: SavedTower = bincode::deserialize_from(&mut stream)?;
        if !saved_tower.verify(node_pubkey) {
            return Err(TowerError::InvalidSignature);
        }
        let mut tower = saved_tower.deserialize()?;
        tower.path = filename;
        tower.tmp_path = Self::get_tmp_filename(&tower.path);

        // check that the tower actually belongs to this node
        if &tower.node_pubkey != node_pubkey {
            return Err(TowerError::WrongTower(format!(
                "node_pubkey is {:?} but found tower for {:?}",
                node_pubkey, tower.node_pubkey
            )));
        }
        Ok(tower)
    }
}

#[derive(Error, Debug)]
pub enum TowerError {
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Serialization Error: {0}")]
    SerializeError(#[from] bincode::Error),

    #[error("The signature on the saved tower is invalid")]
    InvalidSignature,

    #[error("The tower does not match this validator: {0}")]
    WrongTower(String),

    #[error("The tower is too old: last voted slot in tower ({0}) < oldest slot in available history ({1})")]
    TooOld(Slot, Slot),

    #[error("The tower is inconsistent with slot history: {0}")]
    InconsistentWithSlotHistory(String),
}

#[derive(Default, Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct SavedTower {
    signature: Signature,
    data: Vec<u8>,
}

impl SavedTower {
    pub fn new<T: Signer>(tower: &Tower, keypair: &Arc<T>) -> Result<Self> {
        let data = bincode::serialize(tower)?;
        let signature = keypair.sign_message(&data);
        Ok(Self { data, signature })
    }

    pub fn verify(&self, pubkey: &Pubkey) -> bool {
        self.signature.verify(pubkey.as_ref(), &self.data)
    }

    pub fn deserialize(&self) -> Result<Tower> {
        bincode::deserialize(&self.data).map_err(|e| e.into())
    }
}

// Given an untimely crash, tower may have roots that are not reflected in blockstore because
// `ReplayState::handle_votable_bank()` saves tower before setting blockstore roots
pub fn reconcile_blockstore_roots_with_tower(
    tower: &Tower,
    blockstore: &Blockstore,
) -> blockstore_db::Result<()> {
    if let Some(tower_root) = tower.root() {
        let last_blockstore_root = blockstore.last_root();
        if last_blockstore_root < tower_root {
            let new_roots: Vec<_> = blockstore
                .slot_meta_iterator(last_blockstore_root + 1)?
                .map(|(slot, _)| slot)
                .take_while(|slot| *slot <= tower_root)
                .collect();
            blockstore.set_roots(&new_roots)?
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::{
        bank_weight_fork_choice::BankWeightForkChoice,
        cluster_info_vote_listener::VoteTracker,
        cluster_slots::ClusterSlots,
        fork_choice::SelectVoteAndResetForkResult,
        heaviest_subtree_fork_choice::HeaviestSubtreeForkChoice,
        progress_map::ForkProgress,
        replay_stage::{HeaviestForkFailures, ReplayStage},
    };
    use solana_ledger::{blockstore::make_slot_entries, get_tmp_ledger_path};
    use solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::{
            create_genesis_config_with_vote_accounts, GenesisConfigInfo, ValidatorVoteKeypairs,
        },
    };
    use solana_sdk::{
        clock::Slot,
        hash::Hash,
        pubkey::Pubkey,
        signature::Signer,
        slot_history::SlotHistory,
    };
    use solana_vote_program::{
        vote_state::{Vote, VoteStateVersions, MAX_LOCKOUT_HISTORY},
        vote_transaction,
    };
    use std::{
        collections::HashMap,
        fs::{remove_file, OpenOptions},
        io::{Read, Seek, SeekFrom, Write},
        rc::Rc,
        sync::RwLock,
    };
    use tempfile::TempDir;
    use trees::{tr, Tree, TreeWalk};

    pub(crate) struct VoteSimulator {
        pub validator_keypairs: HashMap<Pubkey, ValidatorVoteKeypairs>,
        pub node_pubkeys: Vec<Pubkey>,
        pub vote_pubkeys: Vec<Pubkey>,
        pub bank_forks: RwLock<BankForks>,
        pub progress: ProgressMap,
        pub heaviest_subtree_fork_choice: HeaviestSubtreeForkChoice,
    }

    impl VoteSimulator {
        pub(crate) fn new(num_keypairs: usize) -> Self {
            let (
                validator_keypairs,
                node_pubkeys,
                vote_pubkeys,
                bank_forks,
                progress,
                heaviest_subtree_fork_choice,
            ) = Self::init_state(num_keypairs);
            Self {
                validator_keypairs,
                node_pubkeys,
                vote_pubkeys,
                bank_forks: RwLock::new(bank_forks),
                progress,
                heaviest_subtree_fork_choice,
            }
        }
        pub(crate) fn fill_bank_forks(
            &mut self,
            forks: Tree<u64>,
            cluster_votes: &HashMap<Pubkey, Vec<u64>>,
        ) {
            let root = forks.root().data;
            assert!(self.bank_forks.read().unwrap().get(root).is_some());

            let mut walk = TreeWalk::from(forks);

            while let Some(visit) = walk.get() {
                let slot = visit.node().data;
                self.progress
                    .entry(slot)
                    .or_insert_with(|| ForkProgress::new(Hash::default(), None, None, 0, 0));
                if self.bank_forks.read().unwrap().get(slot).is_some() {
                    walk.forward();
                    continue;
                }
                let parent = walk.get_parent().unwrap().data;
                let parent_bank = self.bank_forks.read().unwrap().get(parent).unwrap().clone();
                let new_bank = Bank::new_from_parent(&parent_bank, &Pubkey::default(), slot);
                for (pubkey, vote) in cluster_votes.iter() {
                    if vote.contains(&parent) {
                        let keypairs = self.validator_keypairs.get(pubkey).unwrap();
                        let last_blockhash = parent_bank.last_blockhash();
                        let vote_tx = vote_transaction::new_vote_transaction(
                            // Must vote > root to be processed
                            vec![parent],
                            parent_bank.hash(),
                            last_blockhash,
                            &keypairs.node_keypair,
                            &keypairs.vote_keypair,
                            &keypairs.vote_keypair,
                            None,
                        );
                        info!("voting {} {}", parent_bank.slot(), parent_bank.hash());
                        new_bank.process_transaction(&vote_tx).unwrap();
                    }
                }
                new_bank.freeze();
                self.heaviest_subtree_fork_choice
                    .add_new_leaf_slot(new_bank.slot(), Some(new_bank.parent_slot()));
                self.bank_forks.write().unwrap().insert(new_bank);
                walk.forward();
            }
        }

        pub(crate) fn simulate_vote(
            &mut self,
            vote_slot: Slot,
            my_pubkey: &Pubkey,
            tower: &mut Tower,
        ) -> Vec<HeaviestForkFailures> {
            // Try to simulate the vote
            let my_keypairs = self.validator_keypairs.get(&my_pubkey).unwrap();
            let my_vote_pubkey = my_keypairs.vote_keypair.pubkey();
            let ancestors = self.bank_forks.read().unwrap().ancestors();
            let mut frozen_banks: Vec<_> = self
                .bank_forks
                .read()
                .unwrap()
                .frozen_banks()
                .values()
                .cloned()
                .collect();

            let _ = ReplayStage::compute_bank_stats(
                &my_pubkey,
                &ancestors,
                &mut frozen_banks,
                tower,
                &mut self.progress,
                &VoteTracker::default(),
                &ClusterSlots::default(),
                &self.bank_forks,
                &mut PubkeyReferences::default(),
                &mut self.heaviest_subtree_fork_choice,
                &mut BankWeightForkChoice::default(),
            );

            let vote_bank = self
                .bank_forks
                .read()
                .unwrap()
                .get(vote_slot)
                .expect("Bank must have been created before vote simulation")
                .clone();

            // Try to vote on the given slot
            let descendants = self.bank_forks.read().unwrap().descendants();
            let SelectVoteAndResetForkResult {
                heaviest_fork_failures,
                ..
            } = ReplayStage::select_vote_and_reset_forks(
                &vote_bank,
                &None,
                &ancestors,
                &descendants,
                &self.progress,
                &tower,
            );

            // Make sure this slot isn't locked out or failing threshold
            info!("Checking vote: {}", vote_bank.slot());
            if !heaviest_fork_failures.is_empty() {
                return heaviest_fork_failures;
            }
            let vote = tower.new_vote_from_bank(&vote_bank, &my_vote_pubkey).0;
            if let Some(new_root) = tower.record_bank_vote(vote) {
                self.set_root(new_root);
            }

            vec![]
        }

        pub fn set_root(&mut self, new_root: Slot) {
            ReplayStage::handle_new_root(
                new_root,
                &self.bank_forks,
                &mut self.progress,
                &None,
                &mut PubkeyReferences::default(),
                None,
                &mut self.heaviest_subtree_fork_choice,
            )
        }

        fn create_and_vote_new_branch(
            &mut self,
            start_slot: Slot,
            end_slot: Slot,
            cluster_votes: &HashMap<Pubkey, Vec<u64>>,
            votes_to_simulate: &HashSet<Slot>,
            my_pubkey: &Pubkey,
            tower: &mut Tower,
        ) -> HashMap<Slot, Vec<HeaviestForkFailures>> {
            (start_slot + 1..=end_slot)
                .filter_map(|slot| {
                    let mut fork_tip_parent = tr(slot - 1);
                    fork_tip_parent.push_front(tr(slot));
                    self.fill_bank_forks(fork_tip_parent, &cluster_votes);
                    if votes_to_simulate.contains(&slot) {
                        Some((slot, self.simulate_vote(slot, &my_pubkey, tower)))
                    } else {
                        None
                    }
                })
                .collect()
        }

        fn simulate_lockout_interval(
            &mut self,
            slot: Slot,
            lockout_interval: (u64, u64),
            vote_account_pubkey: &Pubkey,
        ) {
            self.progress
                .entry(slot)
                .or_insert_with(|| ForkProgress::new(Hash::default(), None, None, 0, 0))
                .fork_stats
                .lockout_intervals
                .entry(lockout_interval.1)
                .or_default()
                .push((lockout_interval.0, Rc::new(*vote_account_pubkey)));
        }

        fn can_progress_on_fork(
            &mut self,
            my_pubkey: &Pubkey,
            tower: &mut Tower,
            start_slot: u64,
            num_slots: u64,
            cluster_votes: &mut HashMap<Pubkey, Vec<u64>>,
        ) -> bool {
            // Check that within some reasonable time, validator can make a new
            // root on this fork
            let old_root = tower.root();

            for i in 1..num_slots {
                // The parent of the tip of the fork
                let mut fork_tip_parent = tr(start_slot + i - 1);
                // The tip of the fork
                fork_tip_parent.push_front(tr(start_slot + i));
                self.fill_bank_forks(fork_tip_parent, cluster_votes);
                if self
                    .simulate_vote(i + start_slot, &my_pubkey, tower)
                    .is_empty()
                {
                    cluster_votes
                        .entry(*my_pubkey)
                        .or_default()
                        .push(start_slot + i);
                }
                if old_root != tower.root() {
                    return true;
                }
            }

            false
        }

        fn init_state(
            num_keypairs: usize,
        ) -> (
            HashMap<Pubkey, ValidatorVoteKeypairs>,
            Vec<Pubkey>,
            Vec<Pubkey>,
            BankForks,
            ProgressMap,
            HeaviestSubtreeForkChoice,
        ) {
            let keypairs: HashMap<_, _> = std::iter::repeat_with(|| {
                let vote_keypairs = ValidatorVoteKeypairs::new_rand();
                (vote_keypairs.node_keypair.pubkey(), vote_keypairs)
            })
            .take(num_keypairs)
            .collect();
            let node_pubkeys: Vec<_> = keypairs
                .values()
                .map(|keys| keys.node_keypair.pubkey())
                .collect();
            let vote_pubkeys: Vec<_> = keypairs
                .values()
                .map(|keys| keys.vote_keypair.pubkey())
                .collect();

            let (bank_forks, progress, heaviest_subtree_fork_choice) =
                initialize_state(&keypairs, 10_000);
            (
                keypairs,
                node_pubkeys,
                vote_pubkeys,
                bank_forks,
                progress,
                heaviest_subtree_fork_choice,
            )
        }
    }

    // Setup BankForks with bank 0 and all the validator accounts
    pub(crate) fn initialize_state(
        validator_keypairs_map: &HashMap<Pubkey, ValidatorVoteKeypairs>,
        stake: u64,
    ) -> (BankForks, ProgressMap, HeaviestSubtreeForkChoice) {
        let validator_keypairs: Vec<_> = validator_keypairs_map.values().collect();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            voting_keypair: _,
        } = create_genesis_config_with_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            vec![stake; validator_keypairs.len()],
        );

        let bank0 = Bank::new(&genesis_config);

        for pubkey in validator_keypairs_map.keys() {
            bank0.transfer(10_000, &mint_keypair, pubkey).unwrap();
        }

        bank0.freeze();
        let mut progress = ProgressMap::default();
        progress.insert(
            0,
            ForkProgress::new(bank0.last_blockhash(), None, None, 0, 0),
        );
        let bank_forks = BankForks::new(bank0);
        let heaviest_subtree_fork_choice =
            HeaviestSubtreeForkChoice::new_from_bank_forks(&bank_forks);
        (bank_forks, progress, heaviest_subtree_fork_choice)
    }

    fn gen_stakes(stake_votes: &[(u64, &[u64])]) -> Vec<(Pubkey, (u64, Account))> {
        let mut stakes = vec![];
        for (lamports, votes) in stake_votes {
            let mut account = Account::default();
            account.data = vec![0; VoteState::size_of()];
            account.lamports = *lamports;
            let mut vote_state = VoteState::default();
            for slot in *votes {
                vote_state.process_slot_vote_unchecked(*slot);
            }
            VoteState::serialize(
                &VoteStateVersions::Current(Box::new(vote_state)),
                &mut account.data,
            )
            .expect("serialize state");
            stakes.push((Pubkey::new_rand(), (*lamports, account)));
        }
        stakes
    }

    #[test]
    fn test_to_vote_instruction() {
        let vote = Vote::default();
        let mut decision = SwitchForkDecision::FailedSwitchThreshold;
        assert!(decision
            .to_vote_instruction(vote.clone(), &Pubkey::default(), &Pubkey::default())
            .is_none());
        decision = SwitchForkDecision::NoSwitch;
        assert_eq!(
            decision.to_vote_instruction(vote.clone(), &Pubkey::default(), &Pubkey::default()),
            Some(vote_instruction::vote(
                &Pubkey::default(),
                &Pubkey::default(),
                vote.clone(),
            ))
        );
        decision = SwitchForkDecision::SwitchProof(Hash::default());
        assert_eq!(
            decision.to_vote_instruction(vote.clone(), &Pubkey::default(), &Pubkey::default()),
            Some(vote_instruction::vote_switch(
                &Pubkey::default(),
                &Pubkey::default(),
                vote,
                Hash::default()
            ))
        );
    }

    #[test]
    fn test_simple_votes() {
        // Init state
        let mut vote_simulator = VoteSimulator::new(1);
        let node_pubkey = vote_simulator.node_pubkeys[0];
        let mut tower = Tower::new_with_key(&node_pubkey);

        // Create the tree of banks
        let forks = tr(0) / (tr(1) / (tr(2) / (tr(3) / (tr(4) / tr(5)))));

        // Set the voting behavior
        let mut cluster_votes = HashMap::new();
        let votes = vec![0, 1, 2, 3, 4, 5];
        cluster_votes.insert(node_pubkey, votes.clone());
        vote_simulator.fill_bank_forks(forks, &cluster_votes);

        // Simulate the votes
        for vote in votes {
            assert!(vote_simulator
                .simulate_vote(vote, &node_pubkey, &mut tower,)
                .is_empty());
        }

        for i in 0..5 {
            assert_eq!(tower.lockouts.votes[i].slot as usize, i);
            assert_eq!(tower.lockouts.votes[i].confirmation_count as usize, 6 - i);
        }
    }

    #[test]
    fn test_switch_threshold_across_tower_reload() {
        solana_logger::setup();
        // Init state
        let mut vote_simulator = VoteSimulator::new(2);
        let my_pubkey = vote_simulator.node_pubkeys[0];
        let other_vote_account = vote_simulator.vote_pubkeys[1];
        let bank0 = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .get(0)
            .unwrap()
            .clone();
        let total_stake = bank0.total_epoch_stake();
        assert_eq!(
            total_stake,
            vote_simulator.validator_keypairs.len() as u64 * 10_000
        );

        // Create the tree of banks
        let forks = tr(0)
            / (tr(1)
                / (tr(2)
                    / tr(10)
                    / (tr(43)
                        / (tr(44)
                            // Minor fork 2
                            / (tr(45) / (tr(46) / (tr(47) / (tr(48) / (tr(49) / (tr(50)))))))
                            / (tr(110) / tr(111))))));

        // Fill the BankForks according to the above fork structure
        vote_simulator.fill_bank_forks(forks, &HashMap::new());
        let ancestors = vote_simulator.bank_forks.read().unwrap().ancestors();
        let descendants = vote_simulator.bank_forks.read().unwrap().descendants();
        let mut tower = Tower::new_with_key(&my_pubkey);

        tower.record_vote(43, Hash::default());
        tower.record_vote(44, Hash::default());
        tower.record_vote(45, Hash::default());
        tower.record_vote(46, Hash::default());
        tower.record_vote(47, Hash::default());
        tower.record_vote(48, Hash::default());
        tower.record_vote(49, Hash::default());

        // Trying to switch to a descendant of last vote should always work
        assert_eq!(
            tower.check_switch_threshold(
                50,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::NoSwitch
        );

        // Trying to switch to another fork at 110 should fail
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::FailedSwitchThreshold
        );

        vote_simulator.simulate_lockout_interval(111, (10, 49), &other_vote_account);

        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::SwitchProof(Hash::default())
        );

        assert_eq!(tower.voted_slots(), vec![43, 44, 45, 46, 47, 48, 49]);
        {
            let mut tower = tower.clone();
            tower.record_vote(110, Hash::default());
            tower.record_vote(111, Hash::default());
            assert_eq!(tower.voted_slots(), vec![43, 110, 111]);
            assert_eq!(tower.lockouts.root_slot, None);
        }

        let mut vote_simulator = VoteSimulator::new(2);
        let other_vote_account = vote_simulator.vote_pubkeys[1];
        let bank0 = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .get(0)
            .unwrap()
            .clone();
        let total_stake = bank0.total_epoch_stake();
        let forks = tr(0)
            / (tr(1)
                / (tr(2)
                    / tr(10)
                    / (tr(43) / (tr(44) / (tr(45) / tr(222)) / (tr(110) / tr(111))))));
        let replayed_root_slot = 44;

        // Fill the BankForks according to the above fork structure
        vote_simulator.fill_bank_forks(forks, &HashMap::new());

        // prepend tower restart!
        let mut slot_history = SlotHistory::default();
        vote_simulator.set_root(replayed_root_slot);
        let ancestors = vote_simulator.bank_forks.read().unwrap().ancestors();
        let descendants = vote_simulator.bank_forks.read().unwrap().descendants();
        for slot in &[0, 1, 2, 43, replayed_root_slot] {
            slot_history.add(*slot);
        }
        let mut tower = tower
            .adjust_lockouts_after_replay(replayed_root_slot, &slot_history)
            .unwrap();

        assert_eq!(tower.voted_slots(), vec![45, 46, 47, 48, 49]);

        // Trying to switch to another fork at 110 should fail
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::FailedSwitchThreshold
        );

        vote_simulator.simulate_lockout_interval(111, (45, 50), &other_vote_account);
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::FailedSwitchThreshold
        );

        vote_simulator.simulate_lockout_interval(111, (110, 200), &other_vote_account);
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::SwitchProof(Hash::default())
        );

        tower.record_vote(110, Hash::default());
        tower.record_vote(111, Hash::default());
        assert_eq!(tower.voted_slots(), vec![110, 111]);
        assert_eq!(tower.lockouts.root_slot, Some(replayed_root_slot));
    }

    #[test]
    fn test_switch_threshold() {
        // Init state
        let mut vote_simulator = VoteSimulator::new(2);
        let my_pubkey = vote_simulator.node_pubkeys[0];
        let other_vote_account = vote_simulator.vote_pubkeys[1];
        let bank0 = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .get(0)
            .unwrap()
            .clone();
        let total_stake = bank0.total_epoch_stake();
        assert_eq!(
            total_stake,
            vote_simulator.validator_keypairs.len() as u64 * 10_000
        );

        // Create the tree of banks
        let forks = tr(0)
            / (tr(1)
                / (tr(2)
                    // Minor fork 1
                    / (tr(10) / (tr(11) / (tr(12) / (tr(13) / (tr(14))))))
                    / (tr(43)
                        / (tr(44)
                            // Minor fork 2
                            / (tr(45) / (tr(46) / (tr(47) / (tr(48) / (tr(49) / (tr(50)))))))
                            / (tr(110))))));

        // Fill the BankForks according to the above fork structure
        vote_simulator.fill_bank_forks(forks, &HashMap::new());
        for (_, fork_progress) in vote_simulator.progress.iter_mut() {
            fork_progress.fork_stats.computed = true;
        }
        let ancestors = vote_simulator.bank_forks.read().unwrap().ancestors();
        let mut descendants = vote_simulator.bank_forks.read().unwrap().descendants();
        let mut tower = Tower::new_with_key(&my_pubkey);

        // Last vote is 47
        tower.record_vote(47, Hash::default());

        // Trying to switch to a descendant of last vote should always work
        assert_eq!(
            tower.check_switch_threshold(
                48,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::NoSwitch
        );

        // Trying to switch to another fork at 110 should fail
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::FailedSwitchThreshold
        );

        // Adding another validator lockout on a descendant of last vote should
        // not count toward the switch threshold
        vote_simulator.simulate_lockout_interval(50, (49, 100), &other_vote_account);
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::FailedSwitchThreshold
        );

        // Adding another validator lockout on an ancestor of last vote should
        // not count toward the switch threshold
        vote_simulator.simulate_lockout_interval(50, (45, 100), &other_vote_account);
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::FailedSwitchThreshold
        );

        // Adding another validator lockout on a different fork, but the lockout
        // doesn't cover the last vote, should not satisfy the switch threshold
        vote_simulator.simulate_lockout_interval(14, (12, 46), &other_vote_account);
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::FailedSwitchThreshold
        );

        // Adding another validator lockout on a different fork, and the lockout
        // covers the last vote would count towards the switch threshold,
        // unless the bank is not the most recent frozen bank on the fork (14 is a
        // frozen/computed bank > 13 on the same fork in this case)
        vote_simulator.simulate_lockout_interval(13, (12, 47), &other_vote_account);
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::FailedSwitchThreshold
        );

        // Adding another validator lockout on a different fork, and the lockout
        // covers the last vote, should satisfy the switch threshold
        vote_simulator.simulate_lockout_interval(14, (12, 47), &other_vote_account);
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::SwitchProof(Hash::default())
        );

        // Adding another unfrozen descendant of the tip of 14 should not remove
        // slot 14 from consideration because it is still the most recent frozen
        // bank on its fork
        descendants.get_mut(&14).unwrap().insert(10000);
        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::SwitchProof(Hash::default())
        );

        // If we set a root, then any lockout intervals below the root shouldn't
        // count toward the switch threshold. This means the other validator's
        // vote lockout no longer counts
        tower.lockouts.root_slot = Some(43);
        // Refresh ancestors and descendants for new root.
        let ancestors = vote_simulator.bank_forks.read().unwrap().ancestors();
        let descendants = vote_simulator.bank_forks.read().unwrap().descendants();

        assert_eq!(
            tower.check_switch_threshold(
                110,
                &ancestors,
                &descendants,
                &vote_simulator.progress,
                total_stake,
                bank0.epoch_vote_accounts(0).unwrap(),
            ),
            SwitchForkDecision::FailedSwitchThreshold
        );
    }

    #[test]
    fn test_switch_threshold_votes() {
        // Init state
        let mut vote_simulator = VoteSimulator::new(4);
        let my_pubkey = vote_simulator.node_pubkeys[0];
        let mut tower = Tower::new_with_key(&my_pubkey);
        let forks = tr(0)
            / (tr(1)
                / (tr(2)
                    // Minor fork 1
                    / (tr(10) / (tr(11) / (tr(12) / (tr(13) / (tr(14))))))
                    / (tr(43)
                        / (tr(44)
                            // Minor fork 2
                            / (tr(45) / (tr(46))))
                            / (tr(110)))));

        // Have two validators, each representing 20% of the stake vote on
        // minor fork 2 at slots 46 + 47
        let mut cluster_votes: HashMap<Pubkey, Vec<Slot>> = HashMap::new();
        cluster_votes.insert(vote_simulator.node_pubkeys[1], vec![46]);
        cluster_votes.insert(vote_simulator.node_pubkeys[2], vec![47]);
        vote_simulator.fill_bank_forks(forks, &cluster_votes);

        // Vote on the first minor fork at slot 14, should succeed
        assert!(vote_simulator
            .simulate_vote(14, &my_pubkey, &mut tower,)
            .is_empty());

        // The other two validators voted at slots 46, 47, which
        // will only both show up in slot 48, at which point
        // 2/5 > SWITCH_FORK_THRESHOLD of the stake has voted
        // on another fork, so switching should succeed
        let votes_to_simulate = (46..=48).collect();
        let results = vote_simulator.create_and_vote_new_branch(
            45,
            48,
            &cluster_votes,
            &votes_to_simulate,
            &my_pubkey,
            &mut tower,
        );
        for slot in 46..=48 {
            if slot == 48 {
                assert!(results.get(&slot).unwrap().is_empty());
            } else {
                assert_eq!(
                    *results.get(&slot).unwrap(),
                    vec![HeaviestForkFailures::FailedSwitchThreshold(slot)]
                );
            }
        }
    }

    #[test]
    fn test_double_partition() {
        // Init state
        let mut vote_simulator = VoteSimulator::new(2);
        let node_pubkey = vote_simulator.node_pubkeys[0];
        let vote_pubkey = vote_simulator.vote_pubkeys[0];
        let mut tower = Tower::new_with_key(&node_pubkey);

        let num_slots_to_try = 200;
        // Create the tree of banks
        let forks = tr(0)
            / (tr(1)
                / (tr(2)
                    / (tr(3)
                        / (tr(4)
                            / (tr(5)
                                / (tr(6)
                                    / (tr(7)
                                        / (tr(8)
                                            / (tr(9)
                                                // Minor fork 1
                                                / (tr(10) / (tr(11) / (tr(12) / (tr(13) / (tr(14))))))
                                                / (tr(43)
                                                    / (tr(44)
                                                        // Minor fork 2
                                                        / (tr(45) / (tr(46) / (tr(47) / (tr(48) / (tr(49) / (tr(50)))))))
                                                        / (tr(110) / (tr(110 + 2 * num_slots_to_try))))))))))))));

        // Set the successful voting behavior
        let mut cluster_votes = HashMap::new();
        let mut my_votes: Vec<Slot> = vec![];
        let next_unlocked_slot = 110;
        // Vote on the first minor fork
        my_votes.extend(0..=14);
        // Come back to the main fork
        my_votes.extend(43..=44);
        // Vote on the second minor fork
        my_votes.extend(45..=50);
        // Vote to come back to main fork
        my_votes.push(next_unlocked_slot);
        cluster_votes.insert(node_pubkey, my_votes.clone());
        // Make the other validator vote fork to pass the threshold checks
        let other_votes = my_votes.clone();
        cluster_votes.insert(vote_simulator.node_pubkeys[1], other_votes);
        vote_simulator.fill_bank_forks(forks, &cluster_votes);

        // Simulate the votes.
        for vote in &my_votes {
            // All these votes should be ok
            assert!(vote_simulator
                .simulate_vote(*vote, &node_pubkey, &mut tower,)
                .is_empty());
        }

        info!("local tower: {:#?}", tower.lockouts.votes);
        let vote_accounts = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .get(next_unlocked_slot)
            .unwrap()
            .vote_accounts();
        let observed = vote_accounts.get(&vote_pubkey).unwrap();
        let state = VoteState::from(&observed.1).unwrap();
        info!("observed tower: {:#?}", state.votes);

        let num_slots_to_try = 200;
        cluster_votes
            .get_mut(&vote_simulator.node_pubkeys[1])
            .unwrap()
            .extend(next_unlocked_slot + 1..next_unlocked_slot + num_slots_to_try);
        assert!(vote_simulator.can_progress_on_fork(
            &node_pubkey,
            &mut tower,
            next_unlocked_slot,
            num_slots_to_try,
            &mut cluster_votes,
        ));
    }

    #[test]
    fn test_collect_vote_lockouts_sums() {
        //two accounts voting for slot 0 with 1 token staked
        let mut accounts = gen_stakes(&[(1, &[0]), (1, &[0])]);
        accounts.sort_by_key(|(pk, _)| *pk);
        let account_latest_votes: PubkeyVotes =
            accounts.iter().map(|(pubkey, _)| (*pubkey, 0)).collect();

        let ancestors = vec![(1, vec![0].into_iter().collect()), (0, HashSet::new())]
            .into_iter()
            .collect();
        let ComputedBankState {
            voted_stakes,
            total_stake,
            bank_weight,
            pubkey_votes,
            ..
        } = Tower::collect_vote_lockouts(
            &Pubkey::default(),
            1,
            accounts.into_iter(),
            &ancestors,
            &mut PubkeyReferences::default(),
        );
        assert_eq!(voted_stakes[&0], 2);
        assert_eq!(total_stake, 2);
        let mut pubkey_votes = Arc::try_unwrap(pubkey_votes).unwrap();
        pubkey_votes.sort();
        assert_eq!(pubkey_votes, account_latest_votes);

        // Each account has 1 vote in it. After simulating a vote in collect_vote_lockouts,
        // the account will have 2 votes, with lockout 2 + 4 = 6. So expected weight for
        assert_eq!(bank_weight, 12)
    }

    #[test]
    fn test_collect_vote_lockouts_root() {
        let votes: Vec<u64> = (0..MAX_LOCKOUT_HISTORY as u64).collect();
        //two accounts voting for slots 0..MAX_LOCKOUT_HISTORY with 1 token staked
        let mut accounts = gen_stakes(&[(1, &votes), (1, &votes)]);
        accounts.sort_by_key(|(pk, _)| *pk);
        let account_latest_votes: PubkeyVotes = accounts
            .iter()
            .map(|(pubkey, _)| (*pubkey, (MAX_LOCKOUT_HISTORY - 1) as Slot))
            .collect();
        let mut tower = Tower::new_for_tests(0, 0.67);
        let mut ancestors = HashMap::new();
        for i in 0..(MAX_LOCKOUT_HISTORY + 1) {
            tower.record_vote(i as u64, Hash::default());
            ancestors.insert(i as u64, (0..i as u64).collect());
        }
        let root = Lockout {
            confirmation_count: MAX_LOCKOUT_HISTORY as u32,
            slot: 0,
        };
        let root_weight = root.lockout() as u128;
        let vote_account_expected_weight = tower
            .lockouts
            .votes
            .iter()
            .map(|v| v.lockout() as u128)
            .sum::<u128>()
            + root_weight;
        let expected_bank_weight = 2 * vote_account_expected_weight;
        assert_eq!(tower.lockouts.root_slot, Some(0));
        let ComputedBankState {
            voted_stakes,
            bank_weight,
            pubkey_votes,
            ..
        } = Tower::collect_vote_lockouts(
            &Pubkey::default(),
            MAX_LOCKOUT_HISTORY as u64,
            accounts.into_iter(),
            &ancestors,
            &mut PubkeyReferences::default(),
        );
        for i in 0..MAX_LOCKOUT_HISTORY {
            assert_eq!(voted_stakes[&(i as u64)], 2);
        }

        // should be the sum of all the weights for root
        assert_eq!(bank_weight, expected_bank_weight);
        let mut pubkey_votes = Arc::try_unwrap(pubkey_votes).unwrap();
        pubkey_votes.sort();
        assert_eq!(pubkey_votes, account_latest_votes);
    }

    #[test]
    fn test_check_vote_threshold_without_votes() {
        let tower = Tower::new_for_tests(1, 0.67);
        let stakes = vec![(0, 1 as Stake)].into_iter().collect();
        assert!(tower.check_vote_stake_threshold(0, &stakes, 2));
    }

    #[test]
    fn test_check_vote_threshold_no_skip_lockout_with_new_root() {
        solana_logger::setup();
        let mut tower = Tower::new_for_tests(4, 0.67);
        let mut stakes = HashMap::new();
        for i in 0..(MAX_LOCKOUT_HISTORY as u64 + 1) {
            stakes.insert(i, 1 as Stake);
            tower.record_vote(i, Hash::default());
        }
        assert!(!tower.check_vote_stake_threshold(MAX_LOCKOUT_HISTORY as u64 + 1, &stakes, 2,));
    }

    #[test]
    fn test_is_slot_confirmed_not_enough_stake_failure() {
        let tower = Tower::new_for_tests(1, 0.67);
        let stakes = vec![(0, 1 as Stake)].into_iter().collect();
        assert!(!tower.is_slot_confirmed(0, &stakes, 2));
    }

    #[test]
    fn test_is_slot_confirmed_unknown_slot() {
        let tower = Tower::new_for_tests(1, 0.67);
        let stakes = HashMap::new();
        assert!(!tower.is_slot_confirmed(0, &stakes, 2));
    }

    #[test]
    fn test_is_slot_confirmed_pass() {
        let tower = Tower::new_for_tests(1, 0.67);
        let stakes = vec![(0, 2 as Stake)].into_iter().collect();
        assert!(tower.is_slot_confirmed(0, &stakes, 2));
    }

    #[test]
    fn test_is_locked_out_empty() {
        let tower = Tower::new_for_tests(0, 0.67);
        let ancestors = vec![(0, HashSet::new())].into_iter().collect();
        assert!(!tower.is_locked_out(0, &ancestors));
    }

    #[test]
    fn test_is_locked_out_root_slot_child_pass() {
        let mut tower = Tower::new_for_tests(0, 0.67);
        let ancestors = vec![(1, vec![0].into_iter().collect())]
            .into_iter()
            .collect();
        tower.lockouts.root_slot = Some(0);
        assert!(!tower.is_locked_out(1, &ancestors));
    }

    #[test]
    fn test_is_locked_out_root_slot_sibling_fail() {
        let mut tower = Tower::new_for_tests(0, 0.67);
        let ancestors = vec![(2, vec![0].into_iter().collect())]
            .into_iter()
            .collect();
        tower.lockouts.root_slot = Some(0);
        tower.record_vote(1, Hash::default());
        assert!(tower.is_locked_out(2, &ancestors));
    }

    #[test]
    fn test_check_already_voted() {
        let mut tower = Tower::new_for_tests(0, 0.67);
        tower.record_vote(0, Hash::default());
        assert!(tower.has_voted(0));
        assert!(!tower.has_voted(1));
    }

    #[test]
    fn test_check_recent_slot() {
        let mut tower = Tower::new_for_tests(0, 0.67);
        assert!(tower.is_recent(0));
        assert!(tower.is_recent(32));
        for i in 0..64 {
            tower.record_vote(i, Hash::default());
        }
        assert!(!tower.is_recent(0));
        assert!(!tower.is_recent(32));
        assert!(!tower.is_recent(63));
        assert!(tower.is_recent(65));
    }

    #[test]
    fn test_is_locked_out_double_vote() {
        let mut tower = Tower::new_for_tests(0, 0.67);
        let ancestors = vec![(1, vec![0].into_iter().collect()), (0, HashSet::new())]
            .into_iter()
            .collect();
        tower.record_vote(0, Hash::default());
        tower.record_vote(1, Hash::default());
        assert!(tower.is_locked_out(0, &ancestors));
    }

    #[test]
    fn test_is_locked_out_child() {
        let mut tower = Tower::new_for_tests(0, 0.67);
        let ancestors = vec![(1, vec![0].into_iter().collect())]
            .into_iter()
            .collect();
        tower.record_vote(0, Hash::default());
        assert!(!tower.is_locked_out(1, &ancestors));
    }

    #[test]
    fn test_is_locked_out_sibling() {
        let mut tower = Tower::new_for_tests(0, 0.67);
        let ancestors = vec![
            (0, HashSet::new()),
            (1, vec![0].into_iter().collect()),
            (2, vec![0].into_iter().collect()),
        ]
        .into_iter()
        .collect();
        tower.record_vote(0, Hash::default());
        tower.record_vote(1, Hash::default());
        assert!(tower.is_locked_out(2, &ancestors));
    }

    #[test]
    fn test_is_locked_out_last_vote_expired() {
        let mut tower = Tower::new_for_tests(0, 0.67);
        let ancestors = vec![
            (0, HashSet::new()),
            (1, vec![0].into_iter().collect()),
            (4, vec![0].into_iter().collect()),
        ]
        .into_iter()
        .collect();
        tower.record_vote(0, Hash::default());
        tower.record_vote(1, Hash::default());
        assert!(!tower.is_locked_out(4, &ancestors));
        tower.record_vote(4, Hash::default());
        assert_eq!(tower.lockouts.votes[0].slot, 0);
        assert_eq!(tower.lockouts.votes[0].confirmation_count, 2);
        assert_eq!(tower.lockouts.votes[1].slot, 4);
        assert_eq!(tower.lockouts.votes[1].confirmation_count, 1);
    }

    #[test]
    fn test_check_vote_threshold_below_threshold() {
        let mut tower = Tower::new_for_tests(1, 0.67);
        let stakes = vec![(0, 1 as Stake)].into_iter().collect();
        tower.record_vote(0, Hash::default());
        assert!(!tower.check_vote_stake_threshold(1, &stakes, 2));
    }
    #[test]
    fn test_check_vote_threshold_above_threshold() {
        let mut tower = Tower::new_for_tests(1, 0.67);
        let stakes = vec![(0, 2 as Stake)].into_iter().collect();
        tower.record_vote(0, Hash::default());
        assert!(tower.check_vote_stake_threshold(1, &stakes, 2));
    }

    #[test]
    fn test_check_vote_threshold_above_threshold_after_pop() {
        let mut tower = Tower::new_for_tests(1, 0.67);
        let stakes = vec![(0, 2 as Stake)].into_iter().collect();
        tower.record_vote(0, Hash::default());
        tower.record_vote(1, Hash::default());
        tower.record_vote(2, Hash::default());
        assert!(tower.check_vote_stake_threshold(6, &stakes, 2));
    }

    #[test]
    fn test_check_vote_threshold_above_threshold_no_stake() {
        let mut tower = Tower::new_for_tests(1, 0.67);
        let stakes = HashMap::new();
        tower.record_vote(0, Hash::default());
        assert!(!tower.check_vote_stake_threshold(1, &stakes, 2));
    }

    #[test]
    fn test_check_vote_threshold_lockouts_not_updated() {
        solana_logger::setup();
        let mut tower = Tower::new_for_tests(1, 0.67);
        let stakes = vec![(0, 1 as Stake), (1, 2 as Stake)].into_iter().collect();
        tower.record_vote(0, Hash::default());
        tower.record_vote(1, Hash::default());
        tower.record_vote(2, Hash::default());
        assert!(tower.check_vote_stake_threshold(6, &stakes, 2,));
    }

    #[test]
    fn test_stake_is_updated_for_entire_branch() {
        let mut voted_stakes = HashMap::new();
        let mut account = Account::default();
        account.lamports = 1;
        let set: HashSet<u64> = vec![0u64, 1u64].into_iter().collect();
        let ancestors: HashMap<u64, HashSet<u64>> = [(2u64, set)].iter().cloned().collect();
        Tower::update_ancestor_voted_stakes(&mut voted_stakes, 2, account.lamports, &ancestors);
        assert_eq!(voted_stakes[&0], 1);
        assert_eq!(voted_stakes[&1], 1);
        assert_eq!(voted_stakes[&2], 1);
    }

    #[test]
    fn test_new_vote() {
        let local = VoteState::default();
        let vote = Tower::new_vote(&local, 0, Hash::default(), None);
        assert_eq!(local.votes.len(), 0);
        assert_eq!(vote.0.slots, vec![0]);
        assert_eq!(vote.1, 0);
    }

    #[test]
    fn test_new_vote_dup_vote() {
        let local = VoteState::default();
        let vote = Tower::new_vote(&local, 0, Hash::default(), Some(0));
        assert!(vote.0.slots.is_empty());
    }

    #[test]
    fn test_new_vote_next_vote() {
        let mut local = VoteState::default();
        let vote = Vote {
            slots: vec![0],
            hash: Hash::default(),
            timestamp: None,
        };
        local.process_vote_unchecked(&vote);
        assert_eq!(local.votes.len(), 1);
        let vote = Tower::new_vote(&local, 1, Hash::default(), Some(0));
        assert_eq!(vote.0.slots, vec![1]);
        assert_eq!(vote.1, 1);
    }

    #[test]
    fn test_new_vote_next_after_expired_vote() {
        let mut local = VoteState::default();
        let vote = Vote {
            slots: vec![0],
            hash: Hash::default(),
            timestamp: None,
        };
        local.process_vote_unchecked(&vote);
        assert_eq!(local.votes.len(), 1);
        let vote = Tower::new_vote(&local, 3, Hash::default(), Some(0));
        //first vote expired, so index should be 0
        assert_eq!(vote.0.slots, vec![3]);
        assert_eq!(vote.1, 0);
    }

    #[test]
    fn test_check_vote_threshold_forks() {
        // Create the ancestor relationships
        let ancestors = (0..=(VOTE_THRESHOLD_DEPTH + 1) as u64)
            .map(|slot| {
                let slot_parents: HashSet<_> = (0..slot).collect();
                (slot, slot_parents)
            })
            .collect();

        // Create votes such that
        // 1) 3/4 of the stake has voted on slot: VOTE_THRESHOLD_DEPTH - 2, lockout: 2
        // 2) 1/4 of the stake has voted on slot: VOTE_THRESHOLD_DEPTH, lockout: 2^9
        let total_stake = 4;
        let threshold_size = 0.67;
        let threshold_stake = (f64::ceil(total_stake as f64 * threshold_size)) as u64;
        let tower_votes: Vec<Slot> = (0..VOTE_THRESHOLD_DEPTH as u64).collect();
        let accounts = gen_stakes(&[
            (threshold_stake, &[(VOTE_THRESHOLD_DEPTH - 2) as u64]),
            (total_stake - threshold_stake, &tower_votes[..]),
        ]);

        // Initialize tower
        let mut tower = Tower::new_for_tests(VOTE_THRESHOLD_DEPTH, threshold_size);

        // CASE 1: Record the first VOTE_THRESHOLD tower votes for fork 2. We want to
        // evaluate a vote on slot VOTE_THRESHOLD_DEPTH. The nth most recent vote should be
        // for slot 0, which is common to all account vote states, so we should pass the
        // threshold check
        let vote_to_evaluate = VOTE_THRESHOLD_DEPTH as u64;
        for vote in &tower_votes {
            tower.record_vote(*vote, Hash::default());
        }
        let ComputedBankState {
            voted_stakes,
            total_stake,
            ..
        } = Tower::collect_vote_lockouts(
            &Pubkey::default(),
            vote_to_evaluate,
            accounts.clone().into_iter(),
            &ancestors,
            &mut PubkeyReferences::default(),
        );
        assert!(tower.check_vote_stake_threshold(vote_to_evaluate, &voted_stakes, total_stake,));

        // CASE 2: Now we want to evaluate a vote for slot VOTE_THRESHOLD_DEPTH + 1. This slot
        // will expire the vote in one of the vote accounts, so we should have insufficient
        // stake to pass the threshold
        let vote_to_evaluate = VOTE_THRESHOLD_DEPTH as u64 + 1;
        let ComputedBankState {
            voted_stakes,
            total_stake,
            ..
        } = Tower::collect_vote_lockouts(
            &Pubkey::default(),
            vote_to_evaluate,
            accounts.into_iter(),
            &ancestors,
            &mut PubkeyReferences::default(),
        );
        assert!(!tower.check_vote_stake_threshold(vote_to_evaluate, &voted_stakes, total_stake,));
    }

    fn vote_and_check_recent(num_votes: usize) {
        let mut tower = Tower::new_for_tests(1, 0.67);
        let slots = if num_votes > 0 {
            vec![num_votes as u64 - 1]
        } else {
            vec![]
        };
        let expected = Vote::new(slots, Hash::default());
        for i in 0..num_votes {
            tower.record_vote(i as u64, Hash::default());
        }
        assert_eq!(expected, tower.last_vote)
    }

    #[test]
    fn test_recent_votes_full() {
        vote_and_check_recent(MAX_LOCKOUT_HISTORY)
    }

    #[test]
    fn test_recent_votes_empty() {
        vote_and_check_recent(0)
    }

    #[test]
    fn test_recent_votes_exact() {
        vote_and_check_recent(5)
    }

    #[test]
    fn test_maybe_timestamp() {
        let mut tower = Tower::default();
        assert!(tower.maybe_timestamp(0).is_some());
        assert!(tower.maybe_timestamp(1).is_some());
        assert!(tower.maybe_timestamp(0).is_none()); // Refuse to timestamp an older slot
        assert!(tower.maybe_timestamp(1).is_none()); // Refuse to timestamp the same slot twice

        tower.last_timestamp.timestamp -= 1; // Move last_timestamp into the past
        assert!(tower.maybe_timestamp(2).is_some()); // slot 2 gets a timestamp

        tower.last_timestamp.timestamp += 1_000_000; // Move last_timestamp well into the future
        assert!(tower.maybe_timestamp(3).is_none()); // slot 3 gets no timestamp
    }

    fn run_test_load_tower_snapshot<F, G>(
        modify_original: F,
        modify_serialized: G,
    ) -> (Tower, Result<Tower>)
    where
        F: Fn(&mut Tower, &Pubkey),
        G: Fn(&PathBuf),
    {
        let dir = TempDir::new().unwrap();
        let identity_keypair = Arc::new(Keypair::new());

        // Use values that will not match the default derived from BankForks
        let mut tower = Tower::new_for_tests(10, 0.9);
        tower.path = Tower::get_filename(&dir.path().to_path_buf(), &identity_keypair.pubkey());
        tower.tmp_path = Tower::get_tmp_filename(&tower.path);

        modify_original(&mut tower, &identity_keypair.pubkey());

        tower.save(&identity_keypair).unwrap();
        modify_serialized(&tower.path);
        let loaded = Tower::restore(&dir.path(), &identity_keypair.pubkey());

        (tower, loaded)
    }

    #[test]
    fn test_load_tower_ok() {
        let (tower, loaded) =
            run_test_load_tower_snapshot(|tower, pubkey| tower.node_pubkey = *pubkey, |_| ());
        let loaded = loaded.unwrap();
        assert_eq!(loaded, tower);
        assert_eq!(tower.threshold_depth, 10);
        assert_eq!(tower.threshold_size, 0.9);
        assert_eq!(loaded.threshold_depth, 10);
        assert_eq!(loaded.threshold_size, 0.9);
    }

    #[test]
    fn test_load_tower_wrong_identity() {
        let identity_keypair = Arc::new(Keypair::new());
        let tower = Tower::new_with_key(&Pubkey::default());
        assert_matches!(
            tower.save(&identity_keypair),
            Err(TowerError::WrongTower(_))
        )
    }

    #[test]
    fn test_load_tower_invalid_signature() {
        let (_, loaded) = run_test_load_tower_snapshot(
            |tower, pubkey| tower.node_pubkey = *pubkey,
            |path| {
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path)
                    .unwrap();
                let mut buf = [0u8];
                assert_eq!(file.read(&mut buf).unwrap(), 1);
                buf[0] += 1;
                assert_eq!(file.seek(SeekFrom::Start(0)).unwrap(), 0);
                assert_eq!(file.write(&buf).unwrap(), 1);
            },
        );
        assert_matches!(loaded, Err(TowerError::InvalidSignature))
    }

    #[test]
    fn test_load_tower_deser_failure() {
        let (_, loaded) = run_test_load_tower_snapshot(
            |tower, pubkey| tower.node_pubkey = *pubkey,
            |path| {
                OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&path)
                    .unwrap_or_else(|_| panic!("Failed to truncate file: {:?}", path));
            },
        );
        assert_matches!(loaded, Err(TowerError::SerializeError(_)))
    }

    #[test]
    fn test_load_tower_missing() {
        let (_, loaded) = run_test_load_tower_snapshot(
            |tower, pubkey| tower.node_pubkey = *pubkey,
            |path| {
                remove_file(path).unwrap();
            },
        );
        assert_matches!(loaded, Err(TowerError::IOError(_)))
    }

    #[test]
    fn test_reconcile_blockstore_roots_with_tower() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            assert_eq!(blockstore.last_root(), 0);

            let (shreds, _) = make_slot_entries(1, 0, 42);
            blockstore.insert_shreds(shreds, None, false).unwrap();
            assert_eq!(blockstore.last_root(), 0);

            let mut tower = Tower::new_with_key(&Pubkey::default());
            tower.lockouts.root_slot = Some(1);
            reconcile_blockstore_roots_with_tower(&tower, &blockstore).unwrap();
            assert_eq!(blockstore.last_root(), 1);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_expire_old_votes_on_load() {
        let mut tower = Tower::new_for_tests(10, 0.9);
        tower.record_vote(0, Hash::default());
        tower.record_vote(1, Hash::default());
        tower.record_vote(2, Hash::default());
        tower.record_vote(3, Hash::default());

        let mut slot_history = SlotHistory::default();
        slot_history.add(0);
        slot_history.add(1);

        let replayed_root_slot = 1;
        tower = tower
            .adjust_lockouts_after_replay(replayed_root_slot, &slot_history)
            .unwrap();

        assert_eq!(tower.voted_slots(), vec![2, 3]);
        assert_eq!(tower.root(), Some(replayed_root_slot));
    }

    #[test]
    fn test_expire_old_votes_on_load2() {
        let mut tower = Tower::new_for_tests(10, 0.9);
        tower.record_vote(0, Hash::default());
        tower.record_vote(1, Hash::default());
        tower.record_vote(2, Hash::default());
        tower.record_vote(3, Hash::default());

        let mut slot_history = SlotHistory::default();
        slot_history.add(0);
        slot_history.add(1);
        slot_history.add(4);

        let replayed_root_slot = 4;
        tower = tower
            .adjust_lockouts_after_replay(replayed_root_slot, &slot_history)
            .unwrap();

        assert_eq!(tower.voted_slots(), vec![2, 3]);
        assert_eq!(tower.root(), Some(replayed_root_slot));
    }

    #[test]
    fn test_expire_old_votes_on_load3() {
        let mut tower = Tower::new_for_tests(10, 0.9);
        tower.record_vote(0, Hash::default());
        tower.record_vote(1, Hash::default());
        tower.record_vote(2, Hash::default());

        let mut slot_history = SlotHistory::default();
        slot_history.add(0);
        slot_history.add(1);
        slot_history.add(2);
        slot_history.add(3);
        slot_history.add(4);
        slot_history.add(5);

        let replayed_root_slot = 5;
        tower = tower
            .adjust_lockouts_after_replay(replayed_root_slot, &slot_history)
            .unwrap();

        assert_eq!(tower.voted_slots(), vec![] as Vec<Slot>);
        assert_eq!(tower.root(), None);
        assert_eq!(tower.stray_restored_slots, HashSet::default());
    }

    #[test]
    fn test_expire_old_votes_on_load4() {
        use solana_sdk::slot_history::MAX_ENTRIES;

        let mut tower = Tower::new_for_tests(10, 0.9);
        tower.record_vote(0, Hash::default());

        let mut slot_history = SlotHistory::default();
        slot_history.add(0);
        slot_history.add(MAX_ENTRIES);

        let result = tower.adjust_lockouts_after_replay(MAX_ENTRIES, &slot_history);
        assert_eq!(format!("{}", result.unwrap_err()), "The tower is too old: last voted slot in tower (0) < oldest slot in available history (1)");
    }

    #[test]
    fn test_expire_old_votes_on_load40() {
        use solana_sdk::slot_history::MAX_ENTRIES;

        let mut tower = Tower::new_for_tests(10, 0.9);
        tower.record_vote(0, Hash::default());
        tower.record_vote(1, Hash::default());
        tower.record_vote(2, Hash::default());

        let mut slot_history = SlotHistory::default();
        slot_history.add(0);
        slot_history.add(1);
        slot_history.add(2);
        slot_history.add(MAX_ENTRIES);

        tower = tower
            .adjust_lockouts_after_replay(MAX_ENTRIES, &slot_history)
            .unwrap();
        assert_eq!(tower.voted_slots(), vec![] as Vec<Slot>);
        assert_eq!(tower.root(), None);
    }

    #[test]
    fn test_expire_old_votes_on_load41() {
        let mut tower = Tower::new_for_tests(10, 0.9);
        tower.lockouts.votes.push_back(Lockout::new(1));
        tower.lockouts.votes.push_back(Lockout::new(0));
        let vote = Vote::new(vec![0], Hash::default());
        tower.last_vote = vote;

        let mut slot_history = SlotHistory::default();
        slot_history.add(0);

        let result = tower.adjust_lockouts_after_replay(0, &slot_history);
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "The tower is inconsistent with slot history: time warmped?"
        );
    }

    #[test]
    fn test_expire_old_votes_on_load42() {
        let mut tower = Tower::new_for_tests(10, 0.9);
        tower.lockouts.votes.push_back(Lockout::new(1));
        tower.lockouts.votes.push_back(Lockout::new(2));
        let vote = Vote::new(vec![2], Hash::default());
        tower.last_vote = vote;

        let mut slot_history = SlotHistory::default();
        slot_history.add(0);
        slot_history.add(2);

        let result = tower.adjust_lockouts_after_replay(2, &slot_history);
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "The tower is inconsistent with slot history: diverged ancestor?"
        );
    }

    #[test]
    fn test_expire_old_votes_on_load43() {
        use solana_sdk::slot_history::MAX_ENTRIES;

        let mut tower = Tower::new_for_tests(10, 0.9);
        tower
            .lockouts
            .votes
            .push_back(Lockout::new(MAX_ENTRIES - 1));
        tower.lockouts.votes.push_back(Lockout::new(0));
        tower.lockouts.votes.push_back(Lockout::new(1));
        let vote = Vote::new(vec![1], Hash::default());
        tower.last_vote = vote;

        let mut slot_history = SlotHistory::default();
        slot_history.add(MAX_ENTRIES);

        let result = tower.adjust_lockouts_after_replay(MAX_ENTRIES, &slot_history);
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "The tower is inconsistent with slot history: not too old once after got too old?"
        );
    }

    #[test]
    fn test_expire_old_votes_on_load5() {
        let mut tower = Tower::new_for_tests(10, 0.9);
        tower.record_vote(0, Hash::default());
        tower.record_vote(1, Hash::default());
        tower.record_vote(2, Hash::default());
        tower.record_vote(3, Hash::default());
        tower.record_vote(4, Hash::default());

        let mut slot_history = SlotHistory::default();
        slot_history.add(0);
        slot_history.add(1);
        slot_history.add(2);

        let replayed_root_slot = 2;
        tower = tower
            .adjust_lockouts_after_replay(replayed_root_slot, &slot_history)
            .unwrap();

        assert_eq!(tower.voted_slots(), vec![3, 4]);
        assert_eq!(tower.root(), Some(replayed_root_slot));
    }

    #[test]
    fn test_expire_old_votes_on_load6() {
        let mut tower = Tower::new_for_tests(10, 0.9);
        tower.record_vote(5, Hash::default());
        tower.record_vote(6, Hash::default());

        let mut slot_history = SlotHistory::default();
        slot_history.add(0);
        slot_history.add(1);
        slot_history.add(2);

        let replayed_root_slot = 2;
        tower = tower
            .adjust_lockouts_after_replay(replayed_root_slot, &slot_history)
            .unwrap();

        assert_eq!(tower.voted_slots(), vec![5, 6]);
        assert_eq!(tower.root(), Some(replayed_root_slot));
    }

    #[test]
    fn test_expire_old_votes_on_load7() {
        let mut tower = Tower::new_for_tests(10, 0.9);

        let mut slot_history = SlotHistory::default();
        slot_history.add(0);

        let replayed_root_slot = 0;
        tower = tower
            .adjust_lockouts_after_replay(replayed_root_slot, &slot_history)
            .unwrap();

        assert_eq!(tower.voted_slots(), vec![] as Vec<Slot>);
        assert_eq!(tower.root(), None);
    }
}
