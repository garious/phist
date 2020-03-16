use crate::contact_info::ContactInfo;
use bincode::{serialize, serialized_size};
use solana_sdk::timing::timestamp;
use solana_sdk::{
    clock::Slot,
    hash::Hash,
    pubkey::Pubkey,
    signature::{Keypair, Signable, Signature},
    transaction::Transaction,
};
use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeSet, HashSet},
    fmt,
};

pub type VoteIndex = u8;
pub const MAX_VOTES: VoteIndex = 32;

pub type EpochSlotIndex = u8;

/// CrdsValue that is replicated across the cluster
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CrdsValue {
    pub signature: Signature,
    pub data: CrdsData,
}

impl Signable for CrdsValue {
    fn pubkey(&self) -> Pubkey {
        self.pubkey()
    }

    fn signable_data(&self) -> Cow<[u8]> {
        Cow::Owned(serialize(&self.data).expect("failed to serialize CrdsData"))
    }

    fn get_signature(&self) -> Signature {
        self.signature
    }

    fn set_signature(&mut self, signature: Signature) {
        self.signature = signature
    }

    fn verify(&self) -> bool {
        let sig_check = self
            .get_signature()
            .verify(&self.pubkey().as_ref(), self.signable_data().borrow());
        let data_check = match &self.data {
            CrdsData::Vote(ix, _) => *ix < MAX_VOTES,
            _ => true,
        };
        sig_check && data_check
    }
}

/// CrdsData that defines the different types of items CrdsValues can hold
/// * Merge Strategy - Latest wallclock is picked
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum CrdsData {
    ContactInfo(ContactInfo),
    Vote(VoteIndex, Vote),
<<<<<<< HEAD
    EpochSlots(EpochSlotIndex, EpochSlots),
    SnapshotHash(SnapshotHash),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum CompressionType {
    Uncompressed,
    GZip,
    BZip2,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::Uncompressed
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct EpochIncompleteSlots {
    pub first: Slot,
    pub compression: CompressionType,
    pub compressed_list: Vec<u8>,
=======
    LowestSlot(u8, LowestSlot),
    SnapshotHashes(SnapshotHash),
    EpochSlots(EpochSlotsIndex, EpochSlots),
    AccountsHashes(SnapshotHash),
>>>>>>> dc347dd3d... Add Accounts hash consistency halting (#8772)
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SnapshotHash {
    pub from: Pubkey,
    pub hashes: Vec<(Slot, Hash)>,
    pub wallclock: u64,
}

impl SnapshotHash {
    pub fn new(from: Pubkey, hashes: Vec<(Slot, Hash)>) -> Self {
        Self {
            from,
            hashes,
            wallclock: timestamp(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EpochSlots {
    pub from: Pubkey,
    pub root: Slot,
    pub lowest: Slot,
    pub slots: BTreeSet<Slot>,
    pub stash: Vec<EpochIncompleteSlots>,
    pub wallclock: u64,
}

impl EpochSlots {
    pub fn new(
        from: Pubkey,
        root: Slot,
        lowest: Slot,
        slots: BTreeSet<Slot>,
        stash: Vec<EpochIncompleteSlots>,
        wallclock: u64,
    ) -> Self {
        Self {
            from,
            root,
            lowest,
            slots,
            stash,
            wallclock,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Vote {
    pub from: Pubkey,
    pub transaction: Transaction,
    pub wallclock: u64,
}

impl Vote {
    pub fn new(from: &Pubkey, transaction: Transaction, wallclock: u64) -> Self {
        Self {
            from: *from,
            transaction,
            wallclock,
        }
    }
}

/// Type of the replicated value
/// These are labels for values in a record that is associated with `Pubkey`
#[derive(PartialEq, Hash, Eq, Clone, Debug)]
pub enum CrdsValueLabel {
    ContactInfo(Pubkey),
    Vote(VoteIndex, Pubkey),
<<<<<<< HEAD
    EpochSlots(Pubkey),
    SnapshotHash(Pubkey),
=======
    LowestSlot(Pubkey),
    SnapshotHashes(Pubkey),
    EpochSlots(EpochSlotsIndex, Pubkey),
    AccountsHashes(Pubkey),
>>>>>>> dc347dd3d... Add Accounts hash consistency halting (#8772)
}

impl fmt::Display for CrdsValueLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CrdsValueLabel::ContactInfo(_) => write!(f, "ContactInfo({})", self.pubkey()),
            CrdsValueLabel::Vote(ix, _) => write!(f, "Vote({}, {})", ix, self.pubkey()),
<<<<<<< HEAD
            CrdsValueLabel::EpochSlots(_) => write!(f, "EpochSlots({})", self.pubkey()),
            CrdsValueLabel::SnapshotHash(_) => write!(f, "SnapshotHash({})", self.pubkey()),
=======
            CrdsValueLabel::LowestSlot(_) => write!(f, "LowestSlot({})", self.pubkey()),
            CrdsValueLabel::SnapshotHashes(_) => write!(f, "SnapshotHash({})", self.pubkey()),
            CrdsValueLabel::EpochSlots(ix, _) => write!(f, "EpochSlots({}, {})", ix, self.pubkey()),
            CrdsValueLabel::AccountsHashes(_) => write!(f, "AccountsHashes({})", self.pubkey()),
>>>>>>> dc347dd3d... Add Accounts hash consistency halting (#8772)
        }
    }
}

impl CrdsValueLabel {
    pub fn pubkey(&self) -> Pubkey {
        match self {
            CrdsValueLabel::ContactInfo(p) => *p,
            CrdsValueLabel::Vote(_, p) => *p,
<<<<<<< HEAD
            CrdsValueLabel::EpochSlots(p) => *p,
            CrdsValueLabel::SnapshotHash(p) => *p,
=======
            CrdsValueLabel::LowestSlot(p) => *p,
            CrdsValueLabel::SnapshotHashes(p) => *p,
            CrdsValueLabel::EpochSlots(_, p) => *p,
            CrdsValueLabel::AccountsHashes(p) => *p,
>>>>>>> dc347dd3d... Add Accounts hash consistency halting (#8772)
        }
    }
}

impl CrdsValue {
    pub fn new_unsigned(data: CrdsData) -> Self {
        Self {
            signature: Signature::default(),
            data,
        }
    }

    pub fn new_signed(data: CrdsData, keypair: &Keypair) -> Self {
        let mut value = Self::new_unsigned(data);
        value.sign(keypair);
        value
    }
    /// Totally unsecure unverfiable wallclock of the node that generated this message
    /// Latest wallclock is always picked.
    /// This is used to time out push messages.
    pub fn wallclock(&self) -> u64 {
        match &self.data {
            CrdsData::ContactInfo(contact_info) => contact_info.wallclock,
            CrdsData::Vote(_, vote) => vote.wallclock,
<<<<<<< HEAD
            CrdsData::EpochSlots(_, vote) => vote.wallclock,
            CrdsData::SnapshotHash(hash) => hash.wallclock,
=======
            CrdsData::LowestSlot(_, obj) => obj.wallclock,
            CrdsData::SnapshotHashes(hash) => hash.wallclock,
            CrdsData::EpochSlots(_, p) => p.wallclock,
            CrdsData::AccountsHashes(hash) => hash.wallclock,
>>>>>>> dc347dd3d... Add Accounts hash consistency halting (#8772)
        }
    }
    pub fn pubkey(&self) -> Pubkey {
        match &self.data {
            CrdsData::ContactInfo(contact_info) => contact_info.id,
            CrdsData::Vote(_, vote) => vote.from,
<<<<<<< HEAD
            CrdsData::EpochSlots(_, slots) => slots.from,
            CrdsData::SnapshotHash(hash) => hash.from,
=======
            CrdsData::LowestSlot(_, slots) => slots.from,
            CrdsData::SnapshotHashes(hash) => hash.from,
            CrdsData::EpochSlots(_, p) => p.from,
            CrdsData::AccountsHashes(hash) => hash.from,
>>>>>>> dc347dd3d... Add Accounts hash consistency halting (#8772)
        }
    }
    pub fn label(&self) -> CrdsValueLabel {
        match &self.data {
            CrdsData::ContactInfo(_) => CrdsValueLabel::ContactInfo(self.pubkey()),
            CrdsData::Vote(ix, _) => CrdsValueLabel::Vote(*ix, self.pubkey()),
<<<<<<< HEAD
            CrdsData::EpochSlots(_, _) => CrdsValueLabel::EpochSlots(self.pubkey()),
            CrdsData::SnapshotHash(_) => CrdsValueLabel::SnapshotHash(self.pubkey()),
=======
            CrdsData::LowestSlot(_, _) => CrdsValueLabel::LowestSlot(self.pubkey()),
            CrdsData::SnapshotHashes(_) => CrdsValueLabel::SnapshotHashes(self.pubkey()),
            CrdsData::EpochSlots(ix, _) => CrdsValueLabel::EpochSlots(*ix, self.pubkey()),
            CrdsData::AccountsHashes(_) => CrdsValueLabel::AccountsHashes(self.pubkey()),
>>>>>>> dc347dd3d... Add Accounts hash consistency halting (#8772)
        }
    }
    pub fn contact_info(&self) -> Option<&ContactInfo> {
        match &self.data {
            CrdsData::ContactInfo(contact_info) => Some(contact_info),
            _ => None,
        }
    }
    pub fn vote(&self) -> Option<&Vote> {
        match &self.data {
            CrdsData::Vote(_, vote) => Some(vote),
            _ => None,
        }
    }

    pub fn vote_index(&self) -> Option<VoteIndex> {
        match &self.data {
            CrdsData::Vote(ix, _) => Some(*ix),
            _ => None,
        }
    }

    pub fn epoch_slots(&self) -> Option<&EpochSlots> {
        match &self.data {
            CrdsData::EpochSlots(_, slots) => Some(slots),
            _ => None,
        }
    }

    pub fn snapshot_hash(&self) -> Option<&SnapshotHash> {
        match &self.data {
            CrdsData::SnapshotHashes(slots) => Some(slots),
            _ => None,
        }
    }

    pub fn accounts_hash(&self) -> Option<&SnapshotHash> {
        match &self.data {
            CrdsData::AccountsHashes(slots) => Some(slots),
            _ => None,
        }
    }

    /// Return all the possible labels for a record identified by Pubkey.
    pub fn record_labels(key: &Pubkey) -> Vec<CrdsValueLabel> {
        let mut labels = vec![
            CrdsValueLabel::ContactInfo(*key),
<<<<<<< HEAD
            CrdsValueLabel::EpochSlots(*key),
            CrdsValueLabel::SnapshotHash(*key),
=======
            CrdsValueLabel::LowestSlot(*key),
            CrdsValueLabel::SnapshotHashes(*key),
            CrdsValueLabel::AccountsHashes(*key),
>>>>>>> dc347dd3d... Add Accounts hash consistency halting (#8772)
        ];
        labels.extend((0..MAX_VOTES).map(|ix| CrdsValueLabel::Vote(ix, *key)));
        labels
    }

    /// Returns the size (in bytes) of a CrdsValue
    pub fn size(&self) -> u64 {
        serialized_size(&self).expect("unable to serialize contact info")
    }

    pub fn compute_vote_index(tower_index: usize, mut votes: Vec<&CrdsValue>) -> VoteIndex {
        let mut available: HashSet<VoteIndex> = (0..MAX_VOTES).collect();
        votes.iter().filter_map(|v| v.vote_index()).for_each(|ix| {
            available.remove(&ix);
        });

        // free index
        if !available.is_empty() {
            return *available.iter().next().unwrap();
        }

        assert!(votes.len() == MAX_VOTES as usize);
        votes.sort_by_key(|v| v.vote().expect("all values must be votes").wallclock);

        // If Tower is full, oldest removed first
        if tower_index + 1 == MAX_VOTES as usize {
            return votes[0].vote_index().expect("all values must be votes");
        }

        // If Tower is not full, the early votes have expired
        assert!(tower_index < MAX_VOTES as usize);

        votes[tower_index]
            .vote_index()
            .expect("all values must be votes")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::contact_info::ContactInfo;
    use bincode::deserialize;
    use solana_perf::test_tx::test_tx;
    use solana_sdk::signature::{Keypair, Signer};
    use solana_sdk::timing::timestamp;

    #[test]
    fn test_labels() {
<<<<<<< HEAD
        let mut hits = [false; 3 + MAX_VOTES as usize];
=======
        let mut hits = [false; 4 + MAX_VOTES as usize + MAX_EPOCH_SLOTS as usize];
>>>>>>> dc347dd3d... Add Accounts hash consistency halting (#8772)
        // this method should cover all the possible labels
        for v in &CrdsValue::record_labels(&Pubkey::default()) {
            match v {
                CrdsValueLabel::ContactInfo(_) => hits[0] = true,
<<<<<<< HEAD
                CrdsValueLabel::EpochSlots(_) => hits[1] = true,
                CrdsValueLabel::SnapshotHash(_) => hits[2] = true,
                CrdsValueLabel::Vote(ix, _) => hits[*ix as usize + 3] = true,
=======
                CrdsValueLabel::LowestSlot(_) => hits[1] = true,
                CrdsValueLabel::SnapshotHashes(_) => hits[2] = true,
                CrdsValueLabel::AccountsHashes(_) => hits[3] = true,
                CrdsValueLabel::Vote(ix, _) => hits[*ix as usize + 4] = true,
                CrdsValueLabel::EpochSlots(ix, _) => {
                    hits[*ix as usize + MAX_VOTES as usize + 4] = true
                }
>>>>>>> dc347dd3d... Add Accounts hash consistency halting (#8772)
            }
        }
        assert!(hits.iter().all(|x| *x));
    }
    #[test]
    fn test_keys_and_values() {
        let v = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::default()));
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().contact_info().unwrap().id;
        assert_eq!(v.label(), CrdsValueLabel::ContactInfo(key));

        let v = CrdsValue::new_unsigned(CrdsData::Vote(
            0,
            Vote::new(&Pubkey::default(), test_tx(), 0),
        ));
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().vote().unwrap().from;
        assert_eq!(v.label(), CrdsValueLabel::Vote(0, key));

        let v = CrdsValue::new_unsigned(CrdsData::EpochSlots(
            0,
            EpochSlots::new(Pubkey::default(), 0, 0, BTreeSet::new(), vec![], 0),
        ));
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().epoch_slots().unwrap().from;
        assert_eq!(v.label(), CrdsValueLabel::EpochSlots(key));
    }

    #[test]
    fn test_signature() {
        let keypair = Keypair::new();
        let wrong_keypair = Keypair::new();
        let mut v = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
            &keypair.pubkey(),
            timestamp(),
        )));
        verify_signatures(&mut v, &keypair, &wrong_keypair);
        v = CrdsValue::new_unsigned(CrdsData::Vote(
            0,
            Vote::new(&keypair.pubkey(), test_tx(), timestamp()),
        ));
        verify_signatures(&mut v, &keypair, &wrong_keypair);
        let btreeset: BTreeSet<Slot> = vec![1, 2, 3, 6, 8].into_iter().collect();
        v = CrdsValue::new_unsigned(CrdsData::EpochSlots(
            0,
            EpochSlots::new(keypair.pubkey(), 0, 0, btreeset, vec![], timestamp()),
        ));
        verify_signatures(&mut v, &keypair, &wrong_keypair);
    }

    #[test]
    fn test_max_vote_index() {
        let keypair = Keypair::new();
        let vote = CrdsValue::new_signed(
            CrdsData::Vote(
                MAX_VOTES,
                Vote::new(&keypair.pubkey(), test_tx(), timestamp()),
            ),
            &keypair,
        );
        assert!(!vote.verify());
    }

    #[test]
    fn test_compute_vote_index_empty() {
        for i in 0..MAX_VOTES {
            let votes = vec![];
            assert!(CrdsValue::compute_vote_index(i as usize, votes) < MAX_VOTES);
        }
    }

    #[test]
    fn test_compute_vote_index_one() {
        let keypair = Keypair::new();
        let vote = CrdsValue::new_unsigned(CrdsData::Vote(
            0,
            Vote::new(&keypair.pubkey(), test_tx(), 0),
        ));
        for i in 0..MAX_VOTES {
            let votes = vec![&vote];
            assert!(CrdsValue::compute_vote_index(i as usize, votes) > 0);
            let votes = vec![&vote];
            assert!(CrdsValue::compute_vote_index(i as usize, votes) < MAX_VOTES);
        }
    }

    #[test]
    fn test_compute_vote_index_full() {
        let keypair = Keypair::new();
        let votes: Vec<_> = (0..MAX_VOTES)
            .map(|x| {
                CrdsValue::new_unsigned(CrdsData::Vote(
                    x,
                    Vote::new(&keypair.pubkey(), test_tx(), x as u64),
                ))
            })
            .collect();
        let vote_refs = votes.iter().collect();
        //pick the oldest vote when full
        assert_eq!(CrdsValue::compute_vote_index(31, vote_refs), 0);
        //pick the index
        let vote_refs = votes.iter().collect();
        assert_eq!(CrdsValue::compute_vote_index(0, vote_refs), 0);
        let vote_refs = votes.iter().collect();
        assert_eq!(CrdsValue::compute_vote_index(30, vote_refs), 30);
    }

    fn serialize_deserialize_value(value: &mut CrdsValue, keypair: &Keypair) {
        let num_tries = 10;
        value.sign(keypair);
        let original_signature = value.get_signature();
        for _ in 0..num_tries {
            let serialized_value = serialize(value).unwrap();
            let deserialized_value: CrdsValue = deserialize(&serialized_value).unwrap();

            // Signatures shouldn't change
            let deserialized_signature = deserialized_value.get_signature();
            assert_eq!(original_signature, deserialized_signature);

            // After deserializing, check that the signature is still the same
            assert!(deserialized_value.verify());
        }
    }

    fn verify_signatures(
        value: &mut CrdsValue,
        correct_keypair: &Keypair,
        wrong_keypair: &Keypair,
    ) {
        assert!(!value.verify());
        value.sign(&correct_keypair);
        assert!(value.verify());
        value.sign(&wrong_keypair);
        assert!(!value.verify());
        serialize_deserialize_value(value, correct_keypair);
    }
}
