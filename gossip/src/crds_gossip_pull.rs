//! Crds Gossip Pull overlay.
//!
//! This module implements the anti-entropy protocol for the network.
//!
//! The basic strategy is as follows:
//!
//! 1. Construct a bloom filter of the local data set
//! 2. Randomly ask a node on the network for data that is not contained in the bloom filter.
//!
//! Bloom filters have a false positive rate.  Each requests uses a different bloom filter
//! with random hash functions.  So each subsequent request will have a different distribution
//! of false positives.

use {
    crate::{
        cluster_info::{Ping, CRDS_UNIQUE_PUBKEY_CAPACITY},
        contact_info::ContactInfo,
        crds::{Crds, VersionedCrdsValue},
        crds_gossip::{get_stake, get_weight},
        crds_gossip_error::CrdsGossipError,
        crds_value::CrdsValue,
        ping_pong::PingCache,
        sync::RwLock,
        weighted_shuffle::WeightedShuffle,
    },
    itertools::Itertools,
    lru::LruCache,
    rand::Rng,
    rayon::{prelude::*, ThreadPool},
    solana_runtime::bloom::{AtomicBloom, Bloom},
    solana_sdk::{
        hash::{hash, Hash},
        pubkey::Pubkey,
        signature::{Keypair, Signer},
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        collections::{HashMap, HashSet, VecDeque},
        convert::TryInto,
        iter::{repeat, repeat_with},
        net::SocketAddr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
        time::{Duration, Instant},
    },
};

pub const CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS: u64 = 15000;
// The maximum age of a value received over pull responses
pub const CRDS_GOSSIP_PULL_MSG_TIMEOUT_MS: u64 = 60000;
// Retention period of hashes of received outdated values.
const FAILED_INSERTS_RETENTION_MS: u64 = 20_000;
// Do not pull from peers which have not been updated for this long.
const PULL_ACTIVE_TIMEOUT_MS: u64 = 60_000;
pub const FALSE_RATE: f64 = 0.1f64;
pub const KEYS: f64 = 8f64;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, AbiExample)]
pub struct CrdsFilter {
    pub filter: Bloom<Hash>,
    mask: u64,
    mask_bits: u32,
}

impl Default for CrdsFilter {
    fn default() -> Self {
        CrdsFilter {
            filter: Bloom::default(),
            mask: !0u64,
            mask_bits: 0u32,
        }
    }
}

impl solana_sdk::sanitize::Sanitize for CrdsFilter {
    fn sanitize(&self) -> std::result::Result<(), solana_sdk::sanitize::SanitizeError> {
        self.filter.sanitize()?;
        Ok(())
    }
}

impl CrdsFilter {
    #[cfg(test)]
    pub(crate) fn new_rand(num_items: usize, max_bytes: usize) -> Self {
        let max_bits = (max_bytes * 8) as f64;
        let max_items = Self::max_items(max_bits, FALSE_RATE, KEYS);
        let mask_bits = Self::mask_bits(num_items as f64, max_items as f64);
        let filter = Bloom::random(max_items as usize, FALSE_RATE, max_bits as usize);
        let seed: u64 = rand::thread_rng().gen_range(0, 2u64.pow(mask_bits));
        let mask = Self::compute_mask(seed, mask_bits);
        CrdsFilter {
            filter,
            mask,
            mask_bits,
        }
    }

    fn compute_mask(seed: u64, mask_bits: u32) -> u64 {
        assert!(seed <= 2u64.pow(mask_bits));
        let seed: u64 = seed.checked_shl(64 - mask_bits).unwrap_or(0x0);
        seed | (!0u64).checked_shr(mask_bits).unwrap_or(!0x0) as u64
    }
    fn max_items(max_bits: f64, false_rate: f64, num_keys: f64) -> f64 {
        let m = max_bits;
        let p = false_rate;
        let k = num_keys;
        (m / (-k / (1f64 - (p.ln() / k).exp()).ln())).ceil()
    }
    fn mask_bits(num_items: f64, max_items: f64) -> u32 {
        // for small ratios this can result in a negative number, ensure it returns 0 instead
        ((num_items / max_items).log2().ceil()).max(0.0) as u32
    }
    pub fn hash_as_u64(item: &Hash) -> u64 {
        let buf = item.as_ref()[..8].try_into().unwrap();
        u64::from_le_bytes(buf)
    }
    fn test_mask(&self, item: &Hash) -> bool {
        // only consider the highest mask_bits bits from the hash and set the rest to 1.
        let ones = (!0u64).checked_shr(self.mask_bits).unwrap_or(!0u64);
        let bits = Self::hash_as_u64(item) | ones;
        bits == self.mask
    }
    #[cfg(test)]
    fn add(&mut self, item: &Hash) {
        if self.test_mask(item) {
            self.filter.add(item);
        }
    }
    #[cfg(test)]
    fn contains(&self, item: &Hash) -> bool {
        if !self.test_mask(item) {
            return true;
        }
        self.filter.contains(item)
    }
    fn filter_contains(&self, item: &Hash) -> bool {
        self.filter.contains(item)
    }
}

/// A vector of crds filters that together hold a complete set of Hashes.
struct CrdsFilterSet {
    filters: Vec<AtomicBloom<Hash>>,
    mask_bits: u32,
}

impl CrdsFilterSet {
    fn new(num_items: usize, max_bytes: usize) -> Self {
        let max_bits = (max_bytes * 8) as f64;
        let max_items = CrdsFilter::max_items(max_bits, FALSE_RATE, KEYS);
        let mask_bits = CrdsFilter::mask_bits(num_items as f64, max_items as f64);
        let filters =
            repeat_with(|| Bloom::random(max_items as usize, FALSE_RATE, max_bits as usize).into())
                .take(1 << mask_bits)
                .collect();
        Self { filters, mask_bits }
    }

    fn add(&self, hash_value: Hash) {
        let index = CrdsFilter::hash_as_u64(&hash_value)
            .checked_shr(64 - self.mask_bits)
            .unwrap_or(0);
        self.filters[index as usize].add(&hash_value);
    }
}

impl From<CrdsFilterSet> for Vec<CrdsFilter> {
    fn from(cfs: CrdsFilterSet) -> Self {
        let mask_bits = cfs.mask_bits;
        cfs.filters
            .into_iter()
            .enumerate()
            .map(|(seed, filter)| CrdsFilter {
                filter: filter.into(),
                mask: CrdsFilter::compute_mask(seed as u64, mask_bits),
                mask_bits,
            })
            .collect()
    }
}

#[derive(Default)]
pub struct ProcessPullStats {
    pub success: usize,
    pub failed_insert: usize,
    pub failed_timeout: usize,
    pub timeout_count: usize,
}

pub struct CrdsGossipPull {
    /// Timestamp of last request
    pull_request_time: RwLock<LruCache<Pubkey, /*timestamp:*/ u64>>,
    // Hash value and record time (ms) of the pull responses which failed to be
    // inserted in crds table; Preserved to stop the sender to send back the
    // same outdated payload again by adding them to the filter for the next
    // pull request.
    failed_inserts: RwLock<VecDeque<(Hash, /*timestamp:*/ u64)>>,
    pub crds_timeout: u64,
    msg_timeout: u64,
    pub num_pulls: AtomicUsize,
}

impl Default for CrdsGossipPull {
    fn default() -> Self {
        Self {
            pull_request_time: RwLock::new(LruCache::new(CRDS_UNIQUE_PUBKEY_CAPACITY)),
            failed_inserts: RwLock::default(),
            crds_timeout: CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS,
            msg_timeout: CRDS_GOSSIP_PULL_MSG_TIMEOUT_MS,
            num_pulls: AtomicUsize::default(),
        }
    }
}
impl CrdsGossipPull {
    /// Generate a random request
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_pull_request(
        &self,
        thread_pool: &ThreadPool,
        crds: &RwLock<Crds>,
        self_keypair: &Keypair,
        self_shred_version: u16,
        now: u64,
        gossip_validators: Option<&HashSet<Pubkey>>,
        stakes: &HashMap<Pubkey, u64>,
        bloom_size: usize,
        ping_cache: &Mutex<PingCache>,
        pings: &mut Vec<(SocketAddr, Ping)>,
        socket_addr_space: &SocketAddrSpace,
    ) -> Result<(ContactInfo, Vec<CrdsFilter>), CrdsGossipError> {
        let (weights, peers): (Vec<_>, Vec<_>) = {
            self.pull_options(
                crds,
                &self_keypair.pubkey(),
                self_shred_version,
                now,
                gossip_validators,
                stakes,
                socket_addr_space,
            )
            .into_iter()
            .map(|(weight, node, gossip_addr)| (weight, (node, gossip_addr)))
            .unzip()
        };
        if peers.is_empty() {
            return Err(CrdsGossipError::NoPeers);
        }
        let mut rng = rand::thread_rng();
        let mut peers = WeightedShuffle::new(&mut rng, &weights)
            .unwrap()
            .map(|i| peers[i]);
        let peer = {
            let mut rng = rand::thread_rng();
            let mut ping_cache = ping_cache.lock().unwrap();
            let mut pingf = move || Ping::new_rand(&mut rng, self_keypair).ok();
            let now = Instant::now();
            peers.find(|node| {
                let (_, gossip_addr) = *node;
                let (check, ping) = ping_cache.check(now, *node, &mut pingf);
                if let Some(ping) = ping {
                    pings.push((gossip_addr, ping));
                }
                check
            })
        };
        let peer = match peer {
            None => return Err(CrdsGossipError::NoPeers),
            Some((node, _gossip_addr)) => node,
        };
        let filters = self.build_crds_filters(thread_pool, crds, bloom_size);
        let peer = match crds.read().unwrap().get::<&ContactInfo>(peer) {
            None => return Err(CrdsGossipError::NoPeers),
            Some(node) => node.clone(),
        };
        Ok((peer, filters))
    }

    fn pull_options(
        &self,
        crds: &RwLock<Crds>,
        self_id: &Pubkey,
        self_shred_version: u16,
        now: u64,
        gossip_validators: Option<&HashSet<Pubkey>>,
        stakes: &HashMap<Pubkey, u64>,
        socket_addr_space: &SocketAddrSpace,
    ) -> Vec<(
        u64,        // weight
        Pubkey,     // node
        SocketAddr, // gossip address
    )> {
        let mut rng = rand::thread_rng();
        let active_cutoff = now.saturating_sub(PULL_ACTIVE_TIMEOUT_MS);
        let pull_request_time = self.pull_request_time.read().unwrap();
        // crds should be locked last after self.pull_request_time.
        let crds = crds.read().unwrap();
        crds.get_nodes()
            .filter_map(|value| {
                let info = value.value.contact_info().unwrap();
                // Stop pulling from nodes which have not been active recently.
                if value.local_timestamp < active_cutoff {
                    // In order to mitigate eclipse attack, for staked nodes
                    // continue retrying periodically.
                    let stake = stakes.get(&info.id).unwrap_or(&0);
                    if *stake == 0 || !rng.gen_ratio(1, 16) {
                        return None;
                    }
                }
                Some(info)
            })
            .filter(|v| {
                v.id != *self_id
                    && ContactInfo::is_valid_address(&v.gossip, socket_addr_space)
                    && (self_shred_version == 0 || self_shred_version == v.shred_version)
                    && gossip_validators
                        .map_or(true, |gossip_validators| gossip_validators.contains(&v.id))
            })
            .map(|item| {
                let max_weight = f32::from(u16::max_value()) - 1.0;
                let req_time: u64 = pull_request_time
                    .peek(&item.id)
                    .copied()
                    .unwrap_or_default();
                let since = (now.saturating_sub(req_time).min(3600 * 1000) / 1024) as u32;
                let stake = get_stake(&item.id, stakes);
                let weight = get_weight(max_weight, since, stake);
                // Weights are bounded by max_weight defined above.
                // So this type-cast should be safe.
                ((weight * 100.0) as u64, item.id, item.gossip)
            })
            .collect()
    }

    /// Time when a request to `from` was initiated.
    ///
    /// This is used for weighted random selection during `new_pull_request`
    /// It's important to use the local nodes request creation time as the weight
    /// instead of the response received time otherwise failed nodes will increase their weight.
    pub(crate) fn mark_pull_request_creation_time(&self, from: Pubkey, now: u64) {
        self.pull_request_time.write().unwrap().put(from, now);
    }

    /// Process a pull request
    pub(crate) fn process_pull_requests<I>(crds: &RwLock<Crds>, callers: I, now: u64)
    where
        I: IntoIterator<Item = CrdsValue>,
    {
        let mut crds = crds.write().unwrap();
        for caller in callers {
            let key = caller.pubkey();
            let _ = crds.insert(caller, now);
            crds.update_record_timestamp(&key, now);
        }
    }

    /// Create gossip responses to pull requests
    pub(crate) fn generate_pull_responses(
        crds: &RwLock<Crds>,
        requests: &[(CrdsValue, CrdsFilter)],
        output_size_limit: usize, // Limit number of crds values returned.
        now: u64,
    ) -> Vec<Vec<CrdsValue>> {
        Self::filter_crds_values(crds, requests, output_size_limit, now)
    }

    // Checks if responses should be inserted and
    // returns those responses converted to VersionedCrdsValue
    // Separated in three vecs as:
    //  .0 => responses that update the owner timestamp
    //  .1 => responses that do not update the owner timestamp
    //  .2 => hash value of outdated values which will fail to insert.
    pub(crate) fn filter_pull_responses(
        &self,
        crds: &RwLock<Crds>,
        timeouts: &HashMap<Pubkey, u64>,
        responses: Vec<CrdsValue>,
        now: u64,
        stats: &mut ProcessPullStats,
    ) -> (Vec<CrdsValue>, Vec<CrdsValue>, Vec<Hash>) {
        let mut active_values = vec![];
        let mut expired_values = vec![];
        let default_timeout = timeouts
            .get(&Pubkey::default())
            .copied()
            .unwrap_or(self.msg_timeout);
        let crds = crds.read().unwrap();
        let upsert = |response: CrdsValue| {
            let owner = response.label().pubkey();
            // Check if the crds value is older than the msg_timeout
            let timeout = timeouts.get(&owner).copied().unwrap_or(default_timeout);
            // Before discarding this value, check if a ContactInfo for the
            // owner exists in the table. If it doesn't, that implies that this
            // value can be discarded
            if !crds.upserts(&response) {
                Some(response)
            } else if now <= response.wallclock().saturating_add(timeout) {
                active_values.push(response);
                None
            } else if crds.get::<&ContactInfo>(owner).is_some() {
                // Silently insert this old value without bumping record
                // timestamps
                expired_values.push(response);
                None
            } else {
                stats.timeout_count += 1;
                stats.failed_timeout += 1;
                Some(response)
            }
        };
        let failed_inserts = responses
            .into_iter()
            .filter_map(upsert)
            .map(|resp| hash(&bincode::serialize(&resp).unwrap()))
            .collect();
        (active_values, expired_values, failed_inserts)
    }

    /// Process a vec of pull responses
    pub(crate) fn process_pull_responses(
        &self,
        crds: &RwLock<Crds>,
        from: &Pubkey,
        responses: Vec<CrdsValue>,
        responses_expired_timeout: Vec<CrdsValue>,
        failed_inserts: Vec<Hash>,
        now: u64,
        stats: &mut ProcessPullStats,
    ) {
        let mut owners = HashSet::new();
        let mut crds = crds.write().unwrap();
        for response in responses_expired_timeout {
            let _ = crds.insert(response, now);
        }
        let mut num_inserts = 0;
        for response in responses {
            let owner = response.pubkey();
            if let Ok(()) = crds.insert(response, now) {
                num_inserts += 1;
                owners.insert(owner);
            }
        }
        stats.success += num_inserts;
        self.num_pulls.fetch_add(num_inserts, Ordering::Relaxed);
        owners.insert(*from);
        for owner in owners {
            crds.update_record_timestamp(&owner, now);
        }
        drop(crds);
        stats.failed_insert += failed_inserts.len();
        self.purge_failed_inserts(now);
        let failed_inserts = failed_inserts.into_iter().zip(repeat(now));
        self.failed_inserts.write().unwrap().extend(failed_inserts);
    }

    pub(crate) fn purge_failed_inserts(&self, now: u64) {
        if FAILED_INSERTS_RETENTION_MS < now {
            let cutoff = now - FAILED_INSERTS_RETENTION_MS;
            let mut failed_inserts = self.failed_inserts.write().unwrap();
            let outdated = failed_inserts
                .iter()
                .take_while(|(_, ts)| *ts < cutoff)
                .count();
            failed_inserts.drain(..outdated);
        }
    }

    pub(crate) fn failed_inserts_size(&self) -> usize {
        self.failed_inserts.read().unwrap().len()
    }

    // build a set of filters of the current crds table
    // num_filters - used to increase the likelyhood of a value in crds being added to some filter
    pub fn build_crds_filters(
        &self,
        thread_pool: &ThreadPool,
        crds: &RwLock<Crds>,
        bloom_size: usize,
    ) -> Vec<CrdsFilter> {
        const PAR_MIN_LENGTH: usize = 512;
        #[cfg(debug_assertions)]
        const MIN_NUM_BLOOM_ITEMS: usize = 512;
        #[cfg(not(debug_assertions))]
        const MIN_NUM_BLOOM_ITEMS: usize = 65_536;
        let failed_inserts = self.failed_inserts.read().unwrap();
        // crds should be locked last after self.failed_inserts.
        let crds = crds.read().unwrap();
        let num_items = crds.len() + crds.num_purged() + failed_inserts.len();
        let num_items = MIN_NUM_BLOOM_ITEMS.max(num_items);
        let filters = CrdsFilterSet::new(num_items, bloom_size);
        thread_pool.install(|| {
            crds.par_values()
                .with_min_len(PAR_MIN_LENGTH)
                .map(|v| v.value_hash)
                .chain(crds.purged().with_min_len(PAR_MIN_LENGTH))
                .chain(
                    failed_inserts
                        .par_iter()
                        .with_min_len(PAR_MIN_LENGTH)
                        .map(|(v, _)| *v),
                )
                .for_each(|v| filters.add(v));
        });
        drop(crds);
        drop(failed_inserts);
        filters.into()
    }

    /// Filter values that fail the bloom filter up to `max_bytes`.
    fn filter_crds_values(
        crds: &RwLock<Crds>,
        filters: &[(CrdsValue, CrdsFilter)],
        mut output_size_limit: usize, // Limit number of crds values returned.
        now: u64,
    ) -> Vec<Vec<CrdsValue>> {
        let msg_timeout = CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS;
        let jitter = rand::thread_rng().gen_range(0, msg_timeout / 4);
        //skip filters from callers that are too old
        let caller_wallclock_window =
            now.saturating_sub(msg_timeout)..now.saturating_add(msg_timeout);
        let mut dropped_requests = 0;
        let mut total_skipped = 0;
        let crds = crds.read().unwrap();
        let ret: Vec<_> = filters
            .iter()
            .map(|(caller, filter)| {
                if output_size_limit == 0 {
                    return None;
                }
                let caller_wallclock = caller.wallclock();
                if !caller_wallclock_window.contains(&caller_wallclock) {
                    dropped_requests += 1;
                    return Some(vec![]);
                }
                let caller_pubkey = caller.pubkey();
                let caller_wallclock = caller_wallclock.checked_add(jitter).unwrap_or(0);
                let pred = |entry: &&VersionedCrdsValue| {
                    debug_assert!(filter.test_mask(&entry.value_hash));
                    // Skip values that are too new.
                    if entry.value.wallclock() > caller_wallclock {
                        total_skipped += 1;
                        false
                    } else {
                        !filter.filter_contains(&entry.value_hash)
                            && (entry.value.pubkey() != caller_pubkey
                                || entry.value.should_force_push(&caller_pubkey))
                    }
                };
                let out: Vec<_> = crds
                    .filter_bitmask(filter.mask, filter.mask_bits)
                    .filter(pred)
                    .map(|entry| entry.value.clone())
                    .take(output_size_limit)
                    .collect();
                output_size_limit -= out.len();
                Some(out)
            })
            .while_some()
            .collect();
        inc_new_counter_info!(
            "gossip_filter_crds_values-dropped_requests",
            dropped_requests + filters.len() - ret.len()
        );
        inc_new_counter_info!("gossip_filter_crds_values-dropped_values", total_skipped);
        ret
    }

    pub(crate) fn make_timeouts(
        &self,
        self_pubkey: Pubkey,
        stakes: &HashMap<Pubkey, u64>,
        epoch_duration: Duration,
    ) -> HashMap<Pubkey, u64> {
        let extended_timeout = self.crds_timeout.max(epoch_duration.as_millis() as u64);
        let default_timeout = if stakes.values().all(|stake| *stake == 0) {
            extended_timeout
        } else {
            self.crds_timeout
        };
        stakes
            .iter()
            .filter(|(_, stake)| **stake > 0)
            .map(|(pubkey, _)| (*pubkey, extended_timeout))
            .chain(vec![
                (Pubkey::default(), default_timeout),
                (self_pubkey, u64::MAX),
            ])
            .collect()
    }

    /// Purge values from the crds that are older then `active_timeout`
    pub(crate) fn purge_active(
        thread_pool: &ThreadPool,
        crds: &RwLock<Crds>,
        now: u64,
        timeouts: &HashMap<Pubkey, u64>,
    ) -> usize {
        let mut crds = crds.write().unwrap();
        let labels = crds.find_old_labels(thread_pool, now, timeouts);
        for label in &labels {
            crds.remove(label, now);
        }
        labels.len()
    }

    /// For legacy tests
    #[cfg(test)]
    fn process_pull_response(
        &self,
        crds: &RwLock<Crds>,
        from: &Pubkey,
        timeouts: &HashMap<Pubkey, u64>,
        response: Vec<CrdsValue>,
        now: u64,
    ) -> (usize, usize, usize) {
        let mut stats = ProcessPullStats::default();
        let (versioned, versioned_expired_timeout, failed_inserts) =
            self.filter_pull_responses(crds, timeouts, response, now, &mut stats);
        self.process_pull_responses(
            crds,
            from,
            versioned,
            versioned_expired_timeout,
            failed_inserts,
            now,
            &mut stats,
        );
        (
            stats.failed_timeout + stats.failed_insert,
            stats.timeout_count,
            stats.success,
        )
    }

    // Only for tests and simulations.
    pub(crate) fn mock_clone(&self) -> Self {
        let pull_request_time = {
            let pull_request_time = self.pull_request_time.read().unwrap();
            let mut clone = LruCache::new(pull_request_time.cap());
            for (k, v) in pull_request_time.iter().rev() {
                clone.put(*k, *v);
            }
            clone
        };
        let failed_inserts = self.failed_inserts.read().unwrap().clone();
        Self {
            pull_request_time: RwLock::new(pull_request_time),
            failed_inserts: RwLock::new(failed_inserts),
            num_pulls: AtomicUsize::new(self.num_pulls.load(Ordering::Relaxed)),
            ..*self
        }
    }

    #[cfg(test)]
    pub(crate) fn pull_request_time(&self) -> std::sync::RwLockReadGuard<LruCache<Pubkey, u64>> {
        self.pull_request_time.read().unwrap()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use {
        super::*,
        crate::{
            cluster_info::MAX_BLOOM_SIZE,
            contact_info::ContactInfo,
            crds_value::{CrdsData, Vote},
        },
        itertools::Itertools,
        rand::{seq::SliceRandom, thread_rng},
        rayon::ThreadPoolBuilder,
        solana_perf::test_tx::test_tx,
        solana_sdk::{
            hash::{hash, HASH_BYTES},
            packet::PACKET_DATA_SIZE,
            timing::timestamp,
        },
    };

    #[cfg(debug_assertions)]
    pub(crate) const MIN_NUM_BLOOM_FILTERS: usize = 1;
    #[cfg(not(debug_assertions))]
    pub(crate) const MIN_NUM_BLOOM_FILTERS: usize = 64;

    #[test]
    fn test_hash_as_u64() {
        let arr: Vec<u8> = (0..HASH_BYTES).map(|i| i as u8 + 1).collect();
        let hash = Hash::new(&arr);
        assert_eq!(CrdsFilter::hash_as_u64(&hash), 0x807060504030201);
    }

    #[test]
    fn test_hash_as_u64_random() {
        fn hash_as_u64_bitops(hash: &Hash) -> u64 {
            let mut out = 0;
            for (i, val) in hash.as_ref().iter().enumerate().take(8) {
                out |= (u64::from(*val)) << (i * 8) as u64;
            }
            out
        }
        let mut rng = thread_rng();
        for _ in 0..100 {
            let hash = solana_sdk::hash::new_rand(&mut rng);
            assert_eq!(CrdsFilter::hash_as_u64(&hash), hash_as_u64_bitops(&hash));
        }
    }

    #[test]
    fn test_crds_filter_default() {
        let filter = CrdsFilter::default();
        let mask = CrdsFilter::compute_mask(0, filter.mask_bits);
        assert_eq!(filter.mask, mask);
        let mut rng = thread_rng();
        for _ in 0..10 {
            let hash = solana_sdk::hash::new_rand(&mut rng);
            assert!(filter.test_mask(&hash));
        }
    }

    #[test]
    fn test_new_pull_with_stakes() {
        let mut crds = Crds::default();
        let mut stakes = HashMap::new();
        let node = CrdsGossipPull::default();
        let me = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
            &solana_sdk::pubkey::new_rand(),
            0,
        )));
        crds.insert(me.clone(), 0).unwrap();
        for i in 1..=30 {
            let entry = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
                &solana_sdk::pubkey::new_rand(),
                0,
            )));
            let id = entry.label().pubkey();
            crds.insert(entry.clone(), 0).unwrap();
            stakes.insert(id, i * 100);
        }
        let now = 1024;
        let crds = RwLock::new(crds);
        let mut options = node.pull_options(
            &crds,
            &me.label().pubkey(),
            0,
            now,
            None,
            &stakes,
            &SocketAddrSpace::Unspecified,
        );
        assert!(!options.is_empty());
        options
            .sort_by(|(weight_l, _, _), (weight_r, _, _)| weight_r.partial_cmp(weight_l).unwrap());
        // check that the highest stake holder is also the heaviest weighted.
        assert_eq!(stakes[&options[0].1], 3000_u64);
    }

    #[test]
    fn test_no_pulls_from_different_shred_versions() {
        let mut crds = Crds::default();
        let stakes = HashMap::new();
        let node = CrdsGossipPull::default();

        let gossip = socketaddr!("127.0.0.1:1234");

        let me = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo {
            id: solana_sdk::pubkey::new_rand(),
            shred_version: 123,
            gossip,
            ..ContactInfo::default()
        }));
        let spy = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo {
            id: solana_sdk::pubkey::new_rand(),
            shred_version: 0,
            gossip,
            ..ContactInfo::default()
        }));
        let node_123 = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo {
            id: solana_sdk::pubkey::new_rand(),
            shred_version: 123,
            gossip,
            ..ContactInfo::default()
        }));
        let node_456 = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo {
            id: solana_sdk::pubkey::new_rand(),
            shred_version: 456,
            gossip,
            ..ContactInfo::default()
        }));

        crds.insert(me.clone(), 0).unwrap();
        crds.insert(spy.clone(), 0).unwrap();
        crds.insert(node_123.clone(), 0).unwrap();
        crds.insert(node_456.clone(), 0).unwrap();
        let crds = RwLock::new(crds);

        // shred version 123 should ignore nodes with versions 0 and 456
        let options = node
            .pull_options(
                &crds,
                &me.label().pubkey(),
                123,
                0,
                None,
                &stakes,
                &SocketAddrSpace::Unspecified,
            )
            .iter()
            .map(|(_, pk, _)| *pk)
            .collect::<Vec<_>>();
        assert_eq!(options.len(), 1);
        assert!(!options.contains(&spy.pubkey()));
        assert!(options.contains(&node_123.pubkey()));

        // spy nodes will see all
        let options = node
            .pull_options(
                &crds,
                &spy.label().pubkey(),
                0,
                0,
                None,
                &stakes,
                &SocketAddrSpace::Unspecified,
            )
            .iter()
            .map(|(_, pk, _)| *pk)
            .collect::<Vec<_>>();
        assert_eq!(options.len(), 3);
        assert!(options.contains(&me.pubkey()));
        assert!(options.contains(&node_123.pubkey()));
        assert!(options.contains(&node_456.pubkey()));
    }

    #[test]
    fn test_pulls_only_from_allowed() {
        let mut crds = Crds::default();
        let stakes = HashMap::new();
        let node = CrdsGossipPull::default();
        let gossip = socketaddr!("127.0.0.1:1234");

        let me = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo {
            id: solana_sdk::pubkey::new_rand(),
            gossip,
            ..ContactInfo::default()
        }));
        let node_123 = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo {
            id: solana_sdk::pubkey::new_rand(),
            gossip,
            ..ContactInfo::default()
        }));

        crds.insert(me.clone(), 0).unwrap();
        crds.insert(node_123.clone(), 0).unwrap();
        let crds = RwLock::new(crds);

        // Empty gossip_validators -- will pull from nobody
        let mut gossip_validators = HashSet::new();
        let options = node.pull_options(
            &crds,
            &me.label().pubkey(),
            0,
            0,
            Some(&gossip_validators),
            &stakes,
            &SocketAddrSpace::Unspecified,
        );
        assert!(options.is_empty());

        // Unknown pubkey in gossip_validators -- will pull from nobody
        gossip_validators.insert(solana_sdk::pubkey::new_rand());
        let options = node.pull_options(
            &crds,
            &me.label().pubkey(),
            0,
            0,
            Some(&gossip_validators),
            &stakes,
            &SocketAddrSpace::Unspecified,
        );
        assert!(options.is_empty());

        // node_123 pubkey in gossip_validators -- will pull from it
        gossip_validators.insert(node_123.pubkey());
        let options = node.pull_options(
            &crds,
            &me.label().pubkey(),
            0,
            0,
            Some(&gossip_validators),
            &stakes,
            &SocketAddrSpace::Unspecified,
        );
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].1, node_123.pubkey());
    }

    #[test]
    fn test_crds_filter_set_add() {
        let mut rng = thread_rng();
        let crds_filter_set =
            CrdsFilterSet::new(/*num_items=*/ 9672788, /*max_bytes=*/ 8196);
        let hash_values: Vec<_> = repeat_with(|| solana_sdk::hash::new_rand(&mut rng))
            .take(1024)
            .collect();
        for hash_value in &hash_values {
            crds_filter_set.add(*hash_value);
        }
        let filters: Vec<CrdsFilter> = crds_filter_set.into();
        assert_eq!(filters.len(), 1024);
        for hash_value in hash_values {
            let mut num_hits = 0;
            let mut false_positives = 0;
            for filter in &filters {
                if filter.test_mask(&hash_value) {
                    num_hits += 1;
                    assert!(filter.contains(&hash_value));
                    assert!(filter.filter.contains(&hash_value));
                } else if filter.filter.contains(&hash_value) {
                    false_positives += 1;
                }
            }
            assert_eq!(num_hits, 1);
            assert!(false_positives < 5);
        }
    }

    #[test]
    fn test_crds_filter_set_new() {
        // Validates invariances required by CrdsFilterSet::get in the
        // vector of filters generated by CrdsFilterSet::new.
        let filters: Vec<CrdsFilter> =
            CrdsFilterSet::new(/*num_items=*/ 55345017, /*max_bytes=*/ 4098).into();
        assert_eq!(filters.len(), 16384);
        let mask_bits = filters[0].mask_bits;
        let right_shift = 64 - mask_bits;
        let ones = !0u64 >> mask_bits;
        for (i, filter) in filters.iter().enumerate() {
            // Check that all mask_bits are equal.
            assert_eq!(mask_bits, filter.mask_bits);
            assert_eq!(i as u64, filter.mask >> right_shift);
            assert_eq!(ones, ones & filter.mask);
        }
    }

    #[test]
    fn test_build_crds_filter() {
        let mut rng = thread_rng();
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let crds_gossip_pull = CrdsGossipPull::default();
        let mut crds = Crds::default();
        let keypairs: Vec<_> = repeat_with(Keypair::new).take(10_000).collect();
        let mut num_inserts = 0;
        for _ in 0..40_000 {
            let keypair = keypairs.choose(&mut rng).unwrap();
            let value = CrdsValue::new_rand(&mut rng, Some(keypair));
            if crds.insert(value, rng.gen()).is_ok() {
                num_inserts += 1;
            }
        }
        let crds = RwLock::new(crds);
        assert!(num_inserts > 30_000, "num inserts: {}", num_inserts);
        let filters = crds_gossip_pull.build_crds_filters(&thread_pool, &crds, MAX_BLOOM_SIZE);
        assert_eq!(filters.len(), MIN_NUM_BLOOM_FILTERS.max(32));
        let crds = crds.read().unwrap();
        let purged: Vec<_> = thread_pool.install(|| crds.purged().collect());
        let hash_values: Vec<_> = crds.values().map(|v| v.value_hash).chain(purged).collect();
        assert_eq!(hash_values.len(), 40_000);
        let mut false_positives = 0;
        for hash_value in hash_values {
            let mut num_hits = 0;
            for filter in &filters {
                if filter.test_mask(&hash_value) {
                    num_hits += 1;
                    assert!(filter.contains(&hash_value));
                    assert!(filter.filter.contains(&hash_value));
                } else if filter.filter.contains(&hash_value) {
                    false_positives += 1;
                }
            }
            assert_eq!(num_hits, 1);
        }
        assert!(false_positives < 150_000, "fp: {}", false_positives);
    }

    #[test]
    fn test_new_pull_request() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let crds = RwLock::<Crds>::default();
        let node_keypair = Keypair::new();
        let entry = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
            &node_keypair.pubkey(),
            0,
        )));
        let node = CrdsGossipPull::default();
        let mut pings = Vec::new();
        let ping_cache = Mutex::new(PingCache::new(
            Duration::from_secs(20 * 60), // ttl
            128,                          // capacity
        ));
        assert_eq!(
            node.new_pull_request(
                &thread_pool,
                &crds,
                &node_keypair,
                0,
                0,
                None,
                &HashMap::new(),
                PACKET_DATA_SIZE,
                &ping_cache,
                &mut pings,
                &SocketAddrSpace::Unspecified,
            ),
            Err(CrdsGossipError::NoPeers)
        );

        crds.write().unwrap().insert(entry, 0).unwrap();
        assert_eq!(
            node.new_pull_request(
                &thread_pool,
                &crds,
                &node_keypair,
                0,
                0,
                None,
                &HashMap::new(),
                PACKET_DATA_SIZE,
                &ping_cache,
                &mut pings,
                &SocketAddrSpace::Unspecified,
            ),
            Err(CrdsGossipError::NoPeers)
        );
        let now = 1625029781069;
        let new = ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), now);
        ping_cache
            .lock()
            .unwrap()
            .mock_pong(new.id, new.gossip, Instant::now());
        let new = CrdsValue::new_unsigned(CrdsData::ContactInfo(new));
        crds.write().unwrap().insert(new.clone(), now).unwrap();
        let req = node.new_pull_request(
            &thread_pool,
            &crds,
            &node_keypair,
            0,
            now,
            None,
            &HashMap::new(),
            PACKET_DATA_SIZE,
            &ping_cache,
            &mut pings,
            &SocketAddrSpace::Unspecified,
        );
        let (peer, _) = req.unwrap();
        assert_eq!(peer, *new.contact_info().unwrap());

        node.mark_pull_request_creation_time(new.contact_info().unwrap().id, now);
        let offline = ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), now);
        let offline = CrdsValue::new_unsigned(CrdsData::ContactInfo(offline));
        crds.write().unwrap().insert(offline, now).unwrap();
        let req = node.new_pull_request(
            &thread_pool,
            &crds,
            &node_keypair,
            0,
            now,
            None,
            &HashMap::new(),
            PACKET_DATA_SIZE,
            &ping_cache,
            &mut pings,
            &SocketAddrSpace::Unspecified,
        );
        // Even though the offline node should have higher weight, we shouldn't request from it
        // until we receive a ping.
        let (peer, _) = req.unwrap();
        assert_eq!(peer, *new.contact_info().unwrap());
    }

    #[test]
    fn test_new_mark_creation_time() {
        let now: u64 = 1_605_127_770_789;
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let mut ping_cache = PingCache::new(
            Duration::from_secs(20 * 60), // ttl
            128,                          // capacity
        );
        let mut crds = Crds::default();
        let node_keypair = Keypair::new();
        let entry = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
            &node_keypair.pubkey(),
            0,
        )));
        let node = CrdsGossipPull::default();
        crds.insert(entry, now).unwrap();
        let old = ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), 0);
        ping_cache.mock_pong(old.id, old.gossip, Instant::now());
        let old = CrdsValue::new_unsigned(CrdsData::ContactInfo(old));
        crds.insert(old.clone(), now).unwrap();
        let new = ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), 0);
        ping_cache.mock_pong(new.id, new.gossip, Instant::now());
        let new = CrdsValue::new_unsigned(CrdsData::ContactInfo(new));
        crds.insert(new.clone(), now).unwrap();
        let crds = RwLock::new(crds);

        // set request creation time to now.
        let now = now + 50_000;
        node.mark_pull_request_creation_time(new.label().pubkey(), now);

        // odds of getting the other request should be close to 1.
        let now = now + 1_000;
        let mut pings = Vec::new();
        let ping_cache = Mutex::new(ping_cache);
        let old = old.contact_info().unwrap();
        let count = repeat_with(|| {
            let (peer, _filters) = node
                .new_pull_request(
                    &thread_pool,
                    &crds,
                    &node_keypair,
                    0, // self_shred_version
                    now,
                    None,             // gossip_validators
                    &HashMap::new(),  // stakes
                    PACKET_DATA_SIZE, // bloom_size
                    &ping_cache,
                    &mut pings,
                    &SocketAddrSpace::Unspecified,
                )
                .unwrap();
            peer
        })
        .take(100)
        .filter(|peer| peer != old)
        .count();
        assert!(count < 2, "count of peer != old: {}", count);
    }

    #[test]
    fn test_pull_request_time() {
        const NUM_REPS: usize = 2 * CRDS_UNIQUE_PUBKEY_CAPACITY;
        let mut rng = rand::thread_rng();
        let pubkeys: Vec<_> = repeat_with(Pubkey::new_unique).take(NUM_REPS).collect();
        let node = CrdsGossipPull::default();
        let mut requests = HashMap::new();
        let now = timestamp();
        for k in 0..NUM_REPS {
            let pubkey = pubkeys[rng.gen_range(0, pubkeys.len())];
            let now = now + k as u64;
            node.mark_pull_request_creation_time(pubkey, now);
            *requests.entry(pubkey).or_default() = now;
        }
        let pull_request_time = node.pull_request_time.read().unwrap();
        assert!(pull_request_time.len() <= CRDS_UNIQUE_PUBKEY_CAPACITY);
        // Assert that timestamps match most recent request.
        for (pk, ts) in pull_request_time.iter() {
            assert_eq!(*ts, requests[pk]);
        }
        // Assert that most recent pull timestamps are maintained.
        let max_ts = requests
            .iter()
            .filter(|(pk, _)| !pull_request_time.contains(*pk))
            .map(|(_, ts)| *ts)
            .max()
            .unwrap();
        let min_ts = requests
            .iter()
            .filter(|(pk, _)| pull_request_time.contains(*pk))
            .map(|(_, ts)| *ts)
            .min()
            .unwrap();
        assert!(max_ts <= min_ts);
    }

    #[test]
    fn test_generate_pull_responses() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let node_keypair = Keypair::new();
        let mut node_crds = Crds::default();
        let mut ping_cache = PingCache::new(
            Duration::from_secs(20 * 60), // ttl
            128,                          // capacity
        );
        let entry = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
            &node_keypair.pubkey(),
            0,
        )));
        let caller = entry.clone();
        let node = CrdsGossipPull::default();
        node_crds.insert(entry, 0).unwrap();
        let new = ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), 0);
        ping_cache.mock_pong(new.id, new.gossip, Instant::now());
        let new = CrdsValue::new_unsigned(CrdsData::ContactInfo(new));
        node_crds.insert(new, 0).unwrap();
        let node_crds = RwLock::new(node_crds);
        let mut pings = Vec::new();
        let req = node.new_pull_request(
            &thread_pool,
            &node_crds,
            &node_keypair,
            0,
            0,
            None,
            &HashMap::new(),
            PACKET_DATA_SIZE,
            &Mutex::new(ping_cache),
            &mut pings,
            &SocketAddrSpace::Unspecified,
        );

        let dest_crds = RwLock::<Crds>::default();
        let (_, filters) = req.unwrap();
        let mut filters: Vec<_> = filters.into_iter().map(|f| (caller.clone(), f)).collect();
        let rsp = CrdsGossipPull::generate_pull_responses(
            &dest_crds,
            &filters,
            /*output_size_limit=*/ usize::MAX,
            0,
        );

        assert_eq!(rsp[0].len(), 0);

        let new = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
            &solana_sdk::pubkey::new_rand(),
            CRDS_GOSSIP_PULL_MSG_TIMEOUT_MS,
        )));
        dest_crds
            .write()
            .unwrap()
            .insert(new, CRDS_GOSSIP_PULL_MSG_TIMEOUT_MS)
            .unwrap();

        //should skip new value since caller is to old
        let rsp = CrdsGossipPull::generate_pull_responses(
            &dest_crds,
            &filters,
            /*output_size_limit=*/ usize::MAX,
            CRDS_GOSSIP_PULL_MSG_TIMEOUT_MS,
        );
        assert_eq!(rsp[0].len(), 0);
        assert_eq!(filters.len(), MIN_NUM_BLOOM_FILTERS);
        filters.extend({
            // Should return new value since caller is new.
            let now = CRDS_GOSSIP_PULL_MSG_TIMEOUT_MS + 1;
            let caller = ContactInfo::new_localhost(&Pubkey::new_unique(), now);
            let caller = CrdsValue::new_unsigned(CrdsData::ContactInfo(caller));
            filters
                .iter()
                .map(|(_, filter)| (caller.clone(), filter.clone()))
                .collect::<Vec<_>>()
        });
        let rsp = CrdsGossipPull::generate_pull_responses(
            &dest_crds,
            &filters,
            /*output_size_limit=*/ usize::MAX,
            CRDS_GOSSIP_PULL_MSG_TIMEOUT_MS,
        );
        assert_eq!(rsp.len(), 2 * MIN_NUM_BLOOM_FILTERS);
        // There should be only one non-empty response in the 2nd half.
        // Orders are also preserved.
        assert!(rsp.iter().take(MIN_NUM_BLOOM_FILTERS).all(|r| r.is_empty()));
        assert_eq!(rsp.iter().filter(|r| r.is_empty()).count(), rsp.len() - 1);
        assert_eq!(rsp.iter().find(|r| r.len() == 1).unwrap().len(), 1);
    }

    #[test]
    fn test_process_pull_request() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let node_keypair = Keypair::new();
        let mut node_crds = Crds::default();
        let entry = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
            &node_keypair.pubkey(),
            0,
        )));
        let caller = entry.clone();
        let node = CrdsGossipPull::default();
        node_crds.insert(entry, 0).unwrap();
        let mut ping_cache = PingCache::new(
            Duration::from_secs(20 * 60), // ttl
            128,                          // capacity
        );
        let new = ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), 0);
        ping_cache.mock_pong(new.id, new.gossip, Instant::now());
        let new = CrdsValue::new_unsigned(CrdsData::ContactInfo(new));
        node_crds.insert(new, 0).unwrap();
        let node_crds = RwLock::new(node_crds);
        let mut pings = Vec::new();
        let req = node.new_pull_request(
            &thread_pool,
            &node_crds,
            &node_keypair,
            0,
            0,
            None,
            &HashMap::new(),
            PACKET_DATA_SIZE,
            &Mutex::new(ping_cache),
            &mut pings,
            &SocketAddrSpace::Unspecified,
        );

        let dest_crds = RwLock::<Crds>::default();
        let (_, filters) = req.unwrap();
        let filters: Vec<_> = filters.into_iter().map(|f| (caller.clone(), f)).collect();
        let rsp = CrdsGossipPull::generate_pull_responses(
            &dest_crds,
            &filters,
            /*output_size_limit=*/ usize::MAX,
            0,
        );
        let callers = filters.into_iter().map(|(caller, _)| caller);
        CrdsGossipPull::process_pull_requests(&dest_crds, callers, 1);
        let dest_crds = dest_crds.read().unwrap();
        assert!(rsp.iter().all(|rsp| rsp.is_empty()));
        assert!(dest_crds.get::<&CrdsValue>(&caller.label()).is_some());
        assert_eq!(1, {
            let entry: &VersionedCrdsValue = dest_crds.get(&caller.label()).unwrap();
            entry.local_timestamp
        });
    }
    #[test]
    fn test_process_pull_request_response() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let node_keypair = Keypair::new();
        let mut node_crds = Crds::default();
        let entry = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
            &node_keypair.pubkey(),
            1,
        )));
        let caller = entry.clone();
        let node_pubkey = entry.label().pubkey();
        let node = CrdsGossipPull::default();
        node_crds.insert(entry, 0).unwrap();
        let mut ping_cache = PingCache::new(
            Duration::from_secs(20 * 60), // ttl
            128,                          // capacity
        );
        let new = ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), 1);
        ping_cache.mock_pong(new.id, new.gossip, Instant::now());
        let new = CrdsValue::new_unsigned(CrdsData::ContactInfo(new));
        node_crds.insert(new, 0).unwrap();

        let mut dest_crds = Crds::default();
        let new_id = solana_sdk::pubkey::new_rand();
        let new = ContactInfo::new_localhost(&new_id, 1);
        ping_cache.mock_pong(new.id, new.gossip, Instant::now());
        let new = CrdsValue::new_unsigned(CrdsData::ContactInfo(new));
        dest_crds.insert(new.clone(), 0).unwrap();
        let dest_crds = RwLock::new(dest_crds);

        // node contains a key from the dest node, but at an older local timestamp
        let same_key = ContactInfo::new_localhost(&new_id, 0);
        ping_cache.mock_pong(same_key.id, same_key.gossip, Instant::now());
        let same_key = CrdsValue::new_unsigned(CrdsData::ContactInfo(same_key));
        assert_eq!(same_key.label(), new.label());
        assert!(same_key.wallclock() < new.wallclock());
        node_crds.insert(same_key.clone(), 0).unwrap();
        assert_eq!(0, {
            let entry: &VersionedCrdsValue = node_crds.get(&same_key.label()).unwrap();
            entry.local_timestamp
        });
        let node_crds = RwLock::new(node_crds);
        let mut done = false;
        let mut pings = Vec::new();
        let ping_cache = Mutex::new(ping_cache);
        for _ in 0..30 {
            // there is a chance of a false positive with bloom filters
            let req = node.new_pull_request(
                &thread_pool,
                &node_crds,
                &node_keypair,
                0,
                0,
                None,
                &HashMap::new(),
                PACKET_DATA_SIZE,
                &ping_cache,
                &mut pings,
                &SocketAddrSpace::Unspecified,
            );
            let (_, filters) = req.unwrap();
            let filters: Vec<_> = filters.into_iter().map(|f| (caller.clone(), f)).collect();
            let rsp = CrdsGossipPull::generate_pull_responses(
                &dest_crds,
                &filters,
                /*output_size_limit=*/ usize::MAX,
                0,
            );
            CrdsGossipPull::process_pull_requests(
                &dest_crds,
                filters.into_iter().map(|(caller, _)| caller),
                0,
            );
            // if there is a false positive this is empty
            // prob should be around 0.1 per iteration
            if rsp.is_empty() {
                continue;
            }

            if rsp.is_empty() {
                continue;
            }
            assert_eq!(rsp.len(), MIN_NUM_BLOOM_FILTERS);
            let failed = node
                .process_pull_response(
                    &node_crds,
                    &node_pubkey,
                    &node.make_timeouts(node_pubkey, &HashMap::new(), Duration::default()),
                    rsp.into_iter().flatten().collect(),
                    1,
                )
                .0;
            assert_eq!(failed, 0);
            assert_eq!(1, {
                let node_crds = node_crds.read().unwrap();
                let entry: &VersionedCrdsValue = node_crds.get(&new.label()).unwrap();
                entry.local_timestamp
            });
            // verify that the whole record was updated for dest since this is a response from dest
            assert_eq!(1, {
                let node_crds = node_crds.read().unwrap();
                let entry: &VersionedCrdsValue = node_crds.get(&same_key.label()).unwrap();
                entry.local_timestamp
            });
            done = true;
            break;
        }
        assert!(done);
    }
    #[test]
    fn test_gossip_purge() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let mut node_crds = Crds::default();
        let entry = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
            &solana_sdk::pubkey::new_rand(),
            0,
        )));
        let node_label = entry.label();
        let node_pubkey = node_label.pubkey();
        let node = CrdsGossipPull::default();
        node_crds.insert(entry, 0).unwrap();
        let old = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::new_localhost(
            &solana_sdk::pubkey::new_rand(),
            0,
        )));
        node_crds.insert(old.clone(), 0).unwrap();
        let value_hash = {
            let entry: &VersionedCrdsValue = node_crds.get(&old.label()).unwrap();
            entry.value_hash
        };
        //verify self is valid
        assert_eq!(
            node_crds.get::<&CrdsValue>(&node_label).unwrap().label(),
            node_label
        );
        // purge
        let node_crds = RwLock::new(node_crds);
        let timeouts = node.make_timeouts(node_pubkey, &HashMap::new(), Duration::default());
        CrdsGossipPull::purge_active(&thread_pool, &node_crds, node.crds_timeout, &timeouts);

        //verify self is still valid after purge
        assert_eq!(node_label, {
            let node_crds = node_crds.read().unwrap();
            node_crds.get::<&CrdsValue>(&node_label).unwrap().label()
        });
        assert_eq!(
            node_crds.read().unwrap().get::<&CrdsValue>(&old.label()),
            None
        );
        assert_eq!(node_crds.read().unwrap().num_purged(), 1);
        for _ in 0..30 {
            // there is a chance of a false positive with bloom filters
            // assert that purged value is still in the set
            // chance of 30 consecutive false positives is 0.1^30
            let filters = node.build_crds_filters(&thread_pool, &node_crds, PACKET_DATA_SIZE);
            assert!(filters.iter().any(|filter| filter.contains(&value_hash)));
        }

        // purge the value
        let mut node_crds = node_crds.write().unwrap();
        node_crds.trim_purged(node.crds_timeout + 1);
        assert_eq!(node_crds.num_purged(), 0);
    }
    #[test]
    #[allow(clippy::float_cmp)]
    fn test_crds_filter_mask() {
        let filter = CrdsFilter::new_rand(1, 128);
        assert_eq!(filter.mask, !0x0);
        assert_eq!(CrdsFilter::max_items(80f64, 0.01, 8f64), 9f64);
        //1000/9 = 111, so 7 bits are needed to mask it
        assert_eq!(CrdsFilter::mask_bits(1000f64, 9f64), 7u32);
        let filter = CrdsFilter::new_rand(1000, 10);
        assert_eq!(filter.mask & 0x00_ffff_ffff, 0x00_ffff_ffff);
    }
    #[test]
    fn test_crds_filter_add_no_mask() {
        let mut filter = CrdsFilter::new_rand(1, 128);
        let h: Hash = hash(Hash::default().as_ref());
        assert!(!filter.contains(&h));
        filter.add(&h);
        assert!(filter.contains(&h));
        let h: Hash = hash(h.as_ref());
        assert!(!filter.contains(&h));
    }
    #[test]
    fn test_crds_filter_add_mask() {
        let mut filter = CrdsFilter::new_rand(1000, 10);
        let mut h: Hash = Hash::default();
        while !filter.test_mask(&h) {
            h = hash(h.as_ref());
        }
        assert!(filter.test_mask(&h));
        //if the mask succeeds, we want the guaranteed negative
        assert!(!filter.contains(&h));
        filter.add(&h);
        assert!(filter.contains(&h));
    }
    #[test]
    fn test_crds_filter_complete_set_add_mask() {
        let mut filters: Vec<CrdsFilter> = CrdsFilterSet::new(1000, 10).into();
        assert!(filters.iter().all(|f| f.mask_bits > 0));
        let mut h: Hash = Hash::default();
        // rev to make the hash::default() miss on the first few test_masks
        while !filters.iter().rev().any(|f| f.test_mask(&h)) {
            h = hash(h.as_ref());
        }
        let filter = filters.iter_mut().find(|f| f.test_mask(&h)).unwrap();
        assert!(filter.test_mask(&h));
        //if the mask succeeds, we want the guaranteed negative
        assert!(!filter.contains(&h));
        filter.add(&h);
        assert!(filter.contains(&h));
    }
    #[test]
    fn test_crds_filter_contains_mask() {
        let filter = CrdsFilter::new_rand(1000, 10);
        assert!(filter.mask_bits > 0);
        let mut h: Hash = Hash::default();
        while filter.test_mask(&h) {
            h = hash(h.as_ref());
        }
        assert!(!filter.test_mask(&h));
        //if the mask fails, the hash is contained in the set, and can be treated as a false
        //positive
        assert!(filter.contains(&h));
    }
    #[test]
    fn test_mask() {
        for i in 0..16 {
            run_test_mask(i);
        }
    }
    fn run_test_mask(mask_bits: u32) {
        assert_eq!(
            (0..2u64.pow(mask_bits))
                .map(|seed| CrdsFilter::compute_mask(seed, mask_bits))
                .dedup()
                .count(),
            2u64.pow(mask_bits) as usize
        )
    }

    #[test]
    fn test_process_pull_response() {
        let node_crds = RwLock::<Crds>::default();
        let node = CrdsGossipPull::default();

        let peer_pubkey = solana_sdk::pubkey::new_rand();
        let peer_entry = CrdsValue::new_unsigned(CrdsData::ContactInfo(
            ContactInfo::new_localhost(&peer_pubkey, 0),
        ));
        let mut timeouts = HashMap::new();
        timeouts.insert(Pubkey::default(), node.crds_timeout);
        timeouts.insert(peer_pubkey, node.msg_timeout + 1);
        // inserting a fresh value should be fine.
        assert_eq!(
            node.process_pull_response(
                &node_crds,
                &peer_pubkey,
                &timeouts,
                vec![peer_entry.clone()],
                1,
            )
            .0,
            0
        );

        let node_crds = RwLock::<Crds>::default();
        let unstaked_peer_entry = CrdsValue::new_unsigned(CrdsData::ContactInfo(
            ContactInfo::new_localhost(&peer_pubkey, 0),
        ));
        // check that old contact infos fail if they are too old, regardless of "timeouts"
        assert_eq!(
            node.process_pull_response(
                &node_crds,
                &peer_pubkey,
                &timeouts,
                vec![peer_entry.clone(), unstaked_peer_entry],
                node.msg_timeout + 100,
            )
            .0,
            4
        );

        let node_crds = RwLock::<Crds>::default();
        // check that old contact infos can still land as long as they have a "timeouts" entry
        assert_eq!(
            node.process_pull_response(
                &node_crds,
                &peer_pubkey,
                &timeouts,
                vec![peer_entry],
                node.msg_timeout + 1,
            )
            .0,
            0
        );

        // construct something that's not a contact info
        let peer_vote =
            CrdsValue::new_unsigned(CrdsData::Vote(0, Vote::new(peer_pubkey, test_tx(), 0)));
        // check that older CrdsValues (non-ContactInfos) infos pass even if are too old,
        // but a recent contact info (inserted above) exists
        assert_eq!(
            node.process_pull_response(
                &node_crds,
                &peer_pubkey,
                &timeouts,
                vec![peer_vote.clone()],
                node.msg_timeout + 1,
            )
            .0,
            0
        );

        let node_crds = RwLock::<Crds>::default();
        // without a contact info, inserting an old value should fail
        assert_eq!(
            node.process_pull_response(
                &node_crds,
                &peer_pubkey,
                &timeouts,
                vec![peer_vote],
                node.msg_timeout + 2,
            )
            .0,
            2
        );
    }
}
