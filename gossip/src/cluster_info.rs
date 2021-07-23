//! The `cluster_info` module defines a data structure that is shared by all the nodes in the network over
//! a gossip control plane.  The goal is to share small bits of off-chain information and detect and
//! repair partitions.
//!
//! This CRDT only supports a very limited set of types.  A map of Pubkey -> Versioned Struct.
//! The last version is always picked during an update.
//!
//! The network is arranged in layers:
//!
//! * layer 0 - Leader.
//! * layer 1 - As many nodes as we can fit
//! * layer 2 - Everyone else, if layer 1 is `2^10`, layer 2 should be able to fit `2^20` number of nodes.
//!
//! Bank needs to provide an interface for us to query the stake weight
use {
    crate::{
        cluster_info_metrics::{
            submit_gossip_stats, Counter, GossipStats, ScopedTimer, TimedGuard,
        },
        contact_info::ContactInfo,
        crds::{Crds, Cursor},
        crds_gossip::CrdsGossip,
        crds_gossip_error::CrdsGossipError,
        crds_gossip_pull::{CrdsFilter, ProcessPullStats, CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS},
        crds_value::{
            self, CrdsData, CrdsValue, CrdsValueLabel, EpochSlotsIndex, LowestSlot, NodeInstance,
            SnapshotHash, Version, Vote, MAX_WALLCLOCK,
        },
        data_budget::DataBudget,
        epoch_slots::EpochSlots,
        gossip_error::GossipError,
        ping_pong::{self, PingCache, Pong},
        socketaddr, socketaddr_any,
        weighted_shuffle::WeightedShuffle,
    },
    bincode::{serialize, serialized_size},
    itertools::Itertools,
    rand::{seq::SliceRandom, thread_rng, CryptoRng, Rng},
    rayon::{prelude::*, ThreadPool, ThreadPoolBuilder},
    serde::ser::Serialize,
    solana_ledger::shred::Shred,
    solana_measure::measure::Measure,
    solana_metrics::{inc_new_counter_debug, inc_new_counter_error},
    solana_net_utils::{
        bind_common, bind_common_in_range, bind_in_range, find_available_port_in_range,
        multi_bind_in_range, PortRange,
    },
    solana_perf::packet::{
        limited_deserialize, to_packets_with_destination, Packet, Packets, PacketsRecycler,
        PACKET_DATA_SIZE,
    },
    solana_rayon_threadlimit::get_thread_count,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{
        clock::{Slot, DEFAULT_MS_PER_SLOT, DEFAULT_SLOTS_PER_EPOCH},
        feature_set::{self, FeatureSet},
        hash::Hash,
        pubkey::Pubkey,
        sanitize::{Sanitize, SanitizeError},
        signature::{Keypair, Signable, Signature, Signer},
        timing::timestamp,
        transaction::Transaction,
    },
    solana_streamer::{
        packet,
        sendmmsg::{multi_target_send, SendPktsError},
        socket::SocketAddrSpace,
        streamer::{PacketReceiver, PacketSender},
    },
    solana_vote_program::vote_state::MAX_LOCKOUT_HISTORY,
    std::{
        borrow::Cow,
        collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
        fmt::Debug,
        fs::{self, File},
        io::BufReader,
        iter::repeat,
        net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, UdpSocket},
        ops::{Deref, Div},
        path::{Path, PathBuf},
        result::Result,
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc::{Receiver, RecvTimeoutError, Sender},
            {Arc, Mutex, RwLock, RwLockReadGuard},
        },
        thread::{sleep, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

pub const VALIDATOR_PORT_RANGE: PortRange = (8000, 10_000);
pub const MINIMUM_VALIDATOR_PORT_RANGE_WIDTH: u16 = 10; // VALIDATOR_PORT_RANGE must be at least this wide

/// The Data plane fanout size, also used as the neighborhood size
pub const DATA_PLANE_FANOUT: usize = 200;
/// milliseconds we sleep for between gossip requests
pub const GOSSIP_SLEEP_MILLIS: u64 = 100;
/// The maximum size of a bloom filter
pub const MAX_BLOOM_SIZE: usize = MAX_CRDS_OBJECT_SIZE;
pub const MAX_CRDS_OBJECT_SIZE: usize = 928;
/// A hard limit on incoming gossip messages
/// Chosen to be able to handle 1Gbps of pure gossip traffic
/// 128MB/PACKET_DATA_SIZE
const MAX_GOSSIP_TRAFFIC: usize = 128_000_000 / PACKET_DATA_SIZE;
/// Max size of serialized crds-values in a Protocol::PushMessage packet. This
/// is equal to PACKET_DATA_SIZE minus serialized size of an empty push
/// message: Protocol::PushMessage(Pubkey::default(), Vec::default())
const PUSH_MESSAGE_MAX_PAYLOAD_SIZE: usize = PACKET_DATA_SIZE - 44;
const DUPLICATE_SHRED_MAX_PAYLOAD_SIZE: usize = PACKET_DATA_SIZE - 115;
/// Maximum number of hashes in SnapshotHashes/AccountsHashes a node publishes
/// such that the serialized size of the push/pull message stays below
/// PACKET_DATA_SIZE.
// TODO: Update this to 26 once payload sizes are upgraded across fleet.
pub const MAX_SNAPSHOT_HASHES: usize = 16;
/// Maximum number of origin nodes that a PruneData may contain, such that the
/// serialized size of the PruneMessage stays below PACKET_DATA_SIZE.
const MAX_PRUNE_DATA_NODES: usize = 32;
/// Number of bytes in the randomly generated token sent with ping messages.
const GOSSIP_PING_TOKEN_SIZE: usize = 32;
const GOSSIP_PING_CACHE_CAPACITY: usize = 65536;
const GOSSIP_PING_CACHE_TTL: Duration = Duration::from_secs(1280);
pub const DEFAULT_CONTACT_DEBUG_INTERVAL_MILLIS: u64 = 10_000;
pub const DEFAULT_CONTACT_SAVE_INTERVAL_MILLIS: u64 = 60_000;
/// Minimum serialized size of a Protocol::PullResponse packet.
const PULL_RESPONSE_MIN_SERIALIZED_SIZE: usize = 161;
// Limit number of unique pubkeys in the crds table.
pub(crate) const CRDS_UNIQUE_PUBKEY_CAPACITY: usize = 4096;
/// Minimum stake that a node should have so that its CRDS values are
/// propagated through gossip (few types are exempted).
const MIN_STAKE_FOR_GOSSIP: u64 = solana_sdk::native_token::LAMPORTS_PER_SOL;
/// Minimum number of staked nodes for enforcing stakes in gossip.
const MIN_NUM_STAKED_NODES: usize = 500;

#[derive(Debug, PartialEq, Eq)]
pub enum ClusterInfoError {
    NoPeers,
    NoLeader,
    BadContactInfo,
    BadGossipAddress,
}

pub struct ClusterInfo {
    /// The network
    pub gossip: CrdsGossip,
    /// set the keypair that will be used to sign crds values generated. It is unset only in tests.
    keypair: RwLock<Arc<Keypair>>,
    /// Network entrypoints
    entrypoints: RwLock<Vec<ContactInfo>>,
    outbound_budget: DataBudget,
    my_contact_info: RwLock<ContactInfo>,
    ping_cache: Mutex<PingCache>,
    stats: GossipStats,
    socket: UdpSocket,
    local_message_pending_push_queue: Mutex<Vec<CrdsValue>>,
    contact_debug_interval: u64, // milliseconds, 0 = disabled
    contact_save_interval: u64,  // milliseconds, 0 = disabled
    instance: RwLock<NodeInstance>,
    contact_info_path: PathBuf,
    socket_addr_space: SocketAddrSpace,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, AbiExample)]
pub(crate) struct PruneData {
    /// Pubkey of the node that sent this prune data
    pubkey: Pubkey,
    /// Pubkeys of nodes that should be pruned
    prunes: Vec<Pubkey>,
    /// Signature of this Prune Message
    signature: Signature,
    /// The Pubkey of the intended node/destination for this message
    destination: Pubkey,
    /// Wallclock of the node that generated this message
    wallclock: u64,
}

impl PruneData {
    /// New random PruneData for tests and benchmarks.
    #[cfg(test)]
    fn new_rand<R: Rng>(rng: &mut R, self_keypair: &Keypair, num_nodes: Option<usize>) -> Self {
        let wallclock = crds_value::new_rand_timestamp(rng);
        let num_nodes = num_nodes.unwrap_or_else(|| rng.gen_range(0, MAX_PRUNE_DATA_NODES + 1));
        let prunes = std::iter::repeat_with(Pubkey::new_unique)
            .take(num_nodes)
            .collect();
        let mut prune_data = PruneData {
            pubkey: self_keypair.pubkey(),
            prunes,
            signature: Signature::default(),
            destination: Pubkey::new_unique(),
            wallclock,
        };
        prune_data.sign(self_keypair);
        prune_data
    }
}

impl Sanitize for PruneData {
    fn sanitize(&self) -> Result<(), SanitizeError> {
        if self.wallclock >= MAX_WALLCLOCK {
            return Err(SanitizeError::ValueOutOfBounds);
        }
        Ok(())
    }
}

impl Signable for PruneData {
    fn pubkey(&self) -> Pubkey {
        self.pubkey
    }

    fn signable_data(&self) -> Cow<[u8]> {
        #[derive(Serialize)]
        struct SignData {
            pubkey: Pubkey,
            prunes: Vec<Pubkey>,
            destination: Pubkey,
            wallclock: u64,
        }
        let data = SignData {
            pubkey: self.pubkey,
            prunes: self.prunes.clone(),
            destination: self.destination,
            wallclock: self.wallclock,
        };
        Cow::Owned(serialize(&data).expect("serialize PruneData"))
    }

    fn get_signature(&self) -> Signature {
        self.signature
    }

    fn set_signature(&mut self, signature: Signature) {
        self.signature = signature
    }
}

struct PullData {
    from_addr: SocketAddr,
    caller: CrdsValue,
    filter: CrdsFilter,
}

pub fn make_accounts_hashes_message(
    keypair: &Keypair,
    accounts_hashes: Vec<(Slot, Hash)>,
) -> Option<CrdsValue> {
    let message = CrdsData::AccountsHashes(SnapshotHash::new(keypair.pubkey(), accounts_hashes));
    Some(CrdsValue::new_signed(message, keypair))
}

pub(crate) type Ping = ping_pong::Ping<[u8; GOSSIP_PING_TOKEN_SIZE]>;

// TODO These messages should go through the gpu pipeline for spam filtering
#[frozen_abi(digest = "E9L7gLXScuPpap3vUqTYe7KTa58noJq7aghj4QLiKoRi")]
#[derive(Serialize, Deserialize, Debug, AbiEnumVisitor, AbiExample)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Protocol {
    /// Gossip protocol messages
    PullRequest(CrdsFilter, CrdsValue),
    PullResponse(Pubkey, Vec<CrdsValue>),
    PushMessage(Pubkey, Vec<CrdsValue>),
    // TODO: Remove the redundant outer pubkey here,
    // and use the inner PruneData.pubkey instead.
    PruneMessage(Pubkey, PruneData),
    PingMessage(Ping),
    PongMessage(Pong),
}

impl Protocol {
    fn par_verify(self) -> Option<Self> {
        match self {
            Protocol::PullRequest(_, ref caller) => {
                if caller.verify() {
                    Some(self)
                } else {
                    inc_new_counter_info!("cluster_info-gossip_pull_request_verify_fail", 1);
                    None
                }
            }
            Protocol::PullResponse(from, data) => {
                let size = data.len();
                let data: Vec<_> = data.into_par_iter().filter(Signable::verify).collect();
                if size != data.len() {
                    inc_new_counter_info!(
                        "cluster_info-gossip_pull_response_verify_fail",
                        size - data.len()
                    );
                }
                if data.is_empty() {
                    None
                } else {
                    Some(Protocol::PullResponse(from, data))
                }
            }
            Protocol::PushMessage(from, data) => {
                let size = data.len();
                let data: Vec<_> = data.into_par_iter().filter(Signable::verify).collect();
                if size != data.len() {
                    inc_new_counter_info!(
                        "cluster_info-gossip_push_msg_verify_fail",
                        size - data.len()
                    );
                }
                if data.is_empty() {
                    None
                } else {
                    Some(Protocol::PushMessage(from, data))
                }
            }
            Protocol::PruneMessage(_, ref data) => {
                if data.verify() {
                    Some(self)
                } else {
                    inc_new_counter_debug!("cluster_info-gossip_prune_msg_verify_fail", 1);
                    None
                }
            }
            Protocol::PingMessage(ref ping) => {
                if ping.verify() {
                    Some(self)
                } else {
                    inc_new_counter_info!("cluster_info-gossip_ping_msg_verify_fail", 1);
                    None
                }
            }
            Protocol::PongMessage(ref pong) => {
                if pong.verify() {
                    Some(self)
                } else {
                    inc_new_counter_info!("cluster_info-gossip_pong_msg_verify_fail", 1);
                    None
                }
            }
        }
    }
}

impl Sanitize for Protocol {
    fn sanitize(&self) -> Result<(), SanitizeError> {
        match self {
            Protocol::PullRequest(filter, val) => {
                filter.sanitize()?;
                val.sanitize()
            }
            Protocol::PullResponse(_, val) => val.sanitize(),
            Protocol::PushMessage(_, val) => val.sanitize(),
            Protocol::PruneMessage(from, val) => {
                if *from != val.pubkey {
                    Err(SanitizeError::InvalidValue)
                } else {
                    val.sanitize()
                }
            }
            Protocol::PingMessage(ping) => ping.sanitize(),
            Protocol::PongMessage(pong) => pong.sanitize(),
        }
    }
}

// Retains only CRDS values associated with nodes with enough stake.
// (some crds types are exempted)
fn retain_staked(values: &mut Vec<CrdsValue>, stakes: &HashMap<Pubkey, u64>) {
    values.retain(|value| {
        match value.data {
            CrdsData::ContactInfo(_) => true,
            // May Impact new validators starting up without any stake yet.
            CrdsData::Vote(_, _) => true,
            // Unstaked nodes can still help repair.
            CrdsData::EpochSlots(_, _) => true,
            // Unstaked nodes can still serve snapshots.
            CrdsData::SnapshotHashes(_) => true,
            // Otherwise unstaked voting nodes will show up with no version in
            // the various dashboards.
            CrdsData::Version(_) => true,
            CrdsData::NodeInstance(_) => true,
            CrdsData::LowestSlot(_, _)
            | CrdsData::AccountsHashes(_)
            | CrdsData::LegacyVersion(_)
            | CrdsData::DuplicateShred(_, _) => {
                let stake = stakes.get(&value.pubkey()).copied();
                stake.unwrap_or_default() >= MIN_STAKE_FOR_GOSSIP
            }
        }
    })
}

impl ClusterInfo {
    pub fn new(
        contact_info: ContactInfo,
        keypair: Arc<Keypair>,
        socket_addr_space: SocketAddrSpace,
    ) -> Self {
        let id = contact_info.id;
        let me = Self {
            gossip: CrdsGossip::default(),
            keypair: RwLock::new(keypair),
            entrypoints: RwLock::default(),
            outbound_budget: DataBudget::default(),
            my_contact_info: RwLock::new(contact_info),
            ping_cache: Mutex::new(PingCache::new(
                GOSSIP_PING_CACHE_TTL,
                GOSSIP_PING_CACHE_CAPACITY,
            )),
            stats: GossipStats::default(),
            socket: UdpSocket::bind("0.0.0.0:0").unwrap(),
            local_message_pending_push_queue: Mutex::default(),
            contact_debug_interval: DEFAULT_CONTACT_DEBUG_INTERVAL_MILLIS,
            instance: RwLock::new(NodeInstance::new(&mut thread_rng(), id, timestamp())),
            contact_info_path: PathBuf::default(),
            contact_save_interval: 0, // disabled
            socket_addr_space,
        };
        me.insert_self();
        me.push_self(&HashMap::new(), None);
        me
    }

    // Should only be used by tests and simulations
    pub fn clone_with_id(&self, new_id: &Pubkey) -> Self {
        let mut my_contact_info = self.my_contact_info.read().unwrap().clone();
        my_contact_info.id = *new_id;
        ClusterInfo {
            gossip: self.gossip.mock_clone(),
            keypair: RwLock::new(self.keypair.read().unwrap().clone()),
            entrypoints: RwLock::new(self.entrypoints.read().unwrap().clone()),
            outbound_budget: self.outbound_budget.clone_non_atomic(),
            my_contact_info: RwLock::new(my_contact_info),
            ping_cache: Mutex::new(self.ping_cache.lock().unwrap().mock_clone()),
            stats: GossipStats::default(),
            socket: UdpSocket::bind("0.0.0.0:0").unwrap(),
            local_message_pending_push_queue: Mutex::new(
                self.local_message_pending_push_queue
                    .lock()
                    .unwrap()
                    .clone(),
            ),
            contact_debug_interval: self.contact_debug_interval,
            instance: RwLock::new(NodeInstance::new(&mut thread_rng(), *new_id, timestamp())),
            contact_info_path: PathBuf::default(),
            contact_save_interval: 0, // disabled
            ..*self
        }
    }

    pub fn set_contact_debug_interval(&mut self, new: u64) {
        self.contact_debug_interval = new;
    }

    pub fn socket_addr_space(&self) -> &SocketAddrSpace {
        &self.socket_addr_space
    }

    fn push_self(
        &self,
        stakes: &HashMap<Pubkey, u64>,
        gossip_validators: Option<&HashSet<Pubkey>>,
    ) {
        let now = timestamp();
        self.my_contact_info.write().unwrap().wallclock = now;
        let entries: Vec<_> = vec![
            CrdsData::ContactInfo(self.my_contact_info()),
            CrdsData::NodeInstance(self.instance.read().unwrap().with_wallclock(now)),
        ]
        .into_iter()
        .map(|v| CrdsValue::new_signed(v, &self.keypair()))
        .collect();
        self.local_message_pending_push_queue
            .lock()
            .unwrap()
            .extend(entries);
        let ContactInfo {
            id: self_pubkey,
            shred_version,
            ..
        } = *self.my_contact_info.read().unwrap();
        self.gossip.refresh_push_active_set(
            &self_pubkey,
            shred_version,
            stakes,
            gossip_validators,
            &self.socket_addr_space,
        );
    }

    // TODO kill insert_info, only used by tests
    pub fn insert_info(&self, contact_info: ContactInfo) {
        let value = CrdsValue::new_signed(CrdsData::ContactInfo(contact_info), &self.keypair());
        let _ = self.gossip.crds.write().unwrap().insert(value, timestamp());
    }

    pub fn set_entrypoint(&self, entrypoint: ContactInfo) {
        self.set_entrypoints(vec![entrypoint]);
    }

    pub fn set_entrypoints(&self, entrypoints: Vec<ContactInfo>) {
        *self.entrypoints.write().unwrap() = entrypoints;
    }

    pub fn save_contact_info(&self) {
        let nodes = {
            let entrypoint_gossip_addrs = self
                .entrypoints
                .read()
                .unwrap()
                .iter()
                .map(|contact_info| contact_info.gossip)
                .collect::<HashSet<_>>();
            let self_pubkey = self.id();
            let gossip_crds = self.gossip.crds.read().unwrap();
            gossip_crds
                .get_nodes()
                .filter_map(|v| {
                    // Don't save:
                    // 1. Our ContactInfo. No point
                    // 2. Entrypoint ContactInfo. This will avoid adopting the incorrect shred
                    //    version on restart if the entrypoint shred version changes.  Also
                    //    there's not much point in saving entrypoint ContactInfo since by
                    //    definition that information is already available
                    let contact_info = v.value.contact_info().unwrap();
                    if contact_info.id != self_pubkey
                        && !entrypoint_gossip_addrs.contains(&contact_info.gossip)
                    {
                        return Some(v.value.clone());
                    }
                    None
                })
                .collect::<Vec<_>>()
        };

        if nodes.is_empty() {
            return;
        }

        let filename = self.contact_info_path.join("contact-info.bin");
        let tmp_filename = &filename.with_extension("tmp");

        match File::create(&tmp_filename) {
            Ok(mut file) => {
                if let Err(err) = bincode::serialize_into(&mut file, &nodes) {
                    warn!(
                        "Failed to serialize contact info info {}: {}",
                        tmp_filename.display(),
                        err
                    );
                    return;
                }
            }
            Err(err) => {
                warn!("Failed to create {}: {}", tmp_filename.display(), err);
                return;
            }
        }

        match fs::rename(&tmp_filename, &filename) {
            Ok(()) => {
                info!(
                    "Saved contact info for {} nodes into {}",
                    nodes.len(),
                    filename.display()
                );
            }
            Err(err) => {
                warn!(
                    "Failed to rename {} to {}: {}",
                    tmp_filename.display(),
                    filename.display(),
                    err
                );
            }
        }
    }

    pub fn restore_contact_info(&mut self, contact_info_path: &Path, contact_save_interval: u64) {
        self.contact_info_path = contact_info_path.into();
        self.contact_save_interval = contact_save_interval;

        let filename = contact_info_path.join("contact-info.bin");
        if !filename.exists() {
            return;
        }

        let nodes: Vec<CrdsValue> = match File::open(&filename) {
            Ok(file) => {
                bincode::deserialize_from(&mut BufReader::new(file)).unwrap_or_else(|err| {
                    warn!("Failed to deserialize {}: {}", filename.display(), err);
                    vec![]
                })
            }
            Err(err) => {
                warn!("Failed to open {}: {}", filename.display(), err);
                vec![]
            }
        };

        info!(
            "Loaded contact info for {} nodes from {}",
            nodes.len(),
            filename.display()
        );
        let now = timestamp();
        let mut gossip_crds = self.gossip.crds.write().unwrap();
        for node in nodes {
            if let Err(err) = gossip_crds.insert(node, now) {
                warn!("crds insert failed {:?}", err);
            }
        }
    }

    pub fn id(&self) -> Pubkey {
        self.my_contact_info.read().unwrap().id
    }

    pub fn keypair(&self) -> RwLockReadGuard<Arc<Keypair>> {
        self.keypair.read().unwrap()
    }

    pub fn set_keypair(&self, new_keypair: Arc<Keypair>) {
        let id = new_keypair.pubkey();
        {
            let mut instance = self.instance.write().unwrap();
            *instance = NodeInstance::new(&mut thread_rng(), id, timestamp());
        }
        *self.keypair.write().unwrap() = new_keypair;
        self.my_contact_info.write().unwrap().id = id;

        self.insert_self();
        self.push_self(&HashMap::new(), None);
    }

    pub fn lookup_contact_info<F, Y>(&self, id: &Pubkey, map: F) -> Option<Y>
    where
        F: FnOnce(&ContactInfo) -> Y,
    {
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds.get(*id).map(map)
    }

    pub fn lookup_contact_info_by_gossip_addr(
        &self,
        gossip_addr: &SocketAddr,
    ) -> Option<ContactInfo> {
        let gossip_crds = self.gossip.crds.read().unwrap();
        let mut nodes = gossip_crds.get_nodes_contact_info();
        nodes.find(|node| node.gossip == *gossip_addr).cloned()
    }

    pub fn my_contact_info(&self) -> ContactInfo {
        self.my_contact_info.read().unwrap().clone()
    }

    pub fn my_shred_version(&self) -> u16 {
        self.my_contact_info.read().unwrap().shred_version
    }

    fn lookup_epoch_slots(&self, ix: EpochSlotsIndex) -> EpochSlots {
        let self_pubkey = self.id();
        let label = CrdsValueLabel::EpochSlots(ix, self_pubkey);
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get::<&CrdsValue>(&label)
            .and_then(|v| v.epoch_slots())
            .cloned()
            .unwrap_or_else(|| EpochSlots::new(self_pubkey, timestamp()))
    }

    pub fn rpc_info_trace(&self) -> String {
        let now = timestamp();
        let my_pubkey = self.id();
        let my_shred_version = self.my_shred_version();
        let nodes: Vec<_> = self
            .all_peers()
            .into_iter()
            .filter_map(|(node, last_updated)| {
                if !ContactInfo::is_valid_address(&node.rpc, &self.socket_addr_space) {
                    return None;
                }

                let node_version = self.get_node_version(&node.id);
                if my_shred_version != 0
                    && (node.shred_version != 0 && node.shred_version != my_shred_version)
                {
                    return None;
                }

                let addr_to_string = |default_ip: &IpAddr, addr: &SocketAddr| -> String {
                    if ContactInfo::is_valid_address(addr, &self.socket_addr_space) {
                        if &addr.ip() == default_ip {
                            addr.port().to_string()
                        } else {
                            addr.to_string()
                        }
                    } else {
                        "none".to_string()
                    }
                };

                let rpc_addr = node.rpc.ip();
                Some(format!(
                    "{:15} {:2}| {:5} | {:44} |{:^9}| {:5}| {:5}| {}\n",
                    rpc_addr.to_string(),
                    if node.id == my_pubkey { "me" } else { "" }.to_string(),
                    now.saturating_sub(last_updated),
                    node.id.to_string(),
                    if let Some(node_version) = node_version {
                        node_version.to_string()
                    } else {
                        "-".to_string()
                    },
                    addr_to_string(&rpc_addr, &node.rpc),
                    addr_to_string(&rpc_addr, &node.rpc_pubsub),
                    node.shred_version,
                ))
            })
            .collect();

        format!(
            "RPC Address       |Age(ms)| Node identifier                              \
             | Version | RPC  |PubSub|ShredVer\n\
             ------------------+-------+----------------------------------------------+---------+\
             ------+------+--------\n\
             {}\
             RPC Enabled Nodes: {}",
            nodes.join(""),
            nodes.len(),
        )
    }

    pub fn contact_info_trace(&self) -> String {
        let now = timestamp();
        let mut shred_spy_nodes = 0usize;
        let mut total_spy_nodes = 0usize;
        let mut different_shred_nodes = 0usize;
        let my_pubkey = self.id();
        let my_shred_version = self.my_shred_version();
        let nodes: Vec<_> = self
            .all_peers()
            .into_iter()
            .filter_map(|(node, last_updated)| {
                let is_spy_node = Self::is_spy_node(&node, &self.socket_addr_space);
                if is_spy_node {
                    total_spy_nodes = total_spy_nodes.saturating_add(1);
                }

                let node_version = self.get_node_version(&node.id);
                if my_shred_version != 0 && (node.shred_version != 0 && node.shred_version != my_shred_version) {
                    different_shred_nodes = different_shred_nodes.saturating_add(1);
                    None
                } else {
                    if is_spy_node {
                        shred_spy_nodes = shred_spy_nodes.saturating_add(1);
                    }
                    let addr_to_string = |default_ip: &IpAddr, addr: &SocketAddr| -> String {
                        if ContactInfo::is_valid_address(addr, &self.socket_addr_space) {
                            if &addr.ip() == default_ip {
                                addr.port().to_string()
                            } else {
                                addr.to_string()
                            }
                        } else {
                            "none".to_string()
                        }
                    };
                    let ip_addr = node.gossip.ip();
                    Some(format!(
                        "{:15} {:2}| {:5} | {:44} |{:^9}| {:5}| {:5}| {:5}| {:5}| {:5}| {:5}| {:5}| {}\n",
                        if ContactInfo::is_valid_address(&node.gossip, &self.socket_addr_space) {
                            ip_addr.to_string()
                        } else {
                            "none".to_string()
                        },
                        if node.id == my_pubkey { "me" } else { "" }.to_string(),
                        now.saturating_sub(last_updated),
                        node.id.to_string(),
                        if let Some(node_version) = node_version {
                            node_version.to_string()
                        } else {
                            "-".to_string()
                        },
                        addr_to_string(&ip_addr, &node.gossip),
                        addr_to_string(&ip_addr, &node.tpu),
                        addr_to_string(&ip_addr, &node.tpu_forwards),
                        addr_to_string(&ip_addr, &node.tvu),
                        addr_to_string(&ip_addr, &node.tvu_forwards),
                        addr_to_string(&ip_addr, &node.repair),
                        addr_to_string(&ip_addr, &node.serve_repair),
                        node.shred_version,
                    ))
                }
            })
            .collect();

        format!(
            "IP Address        |Age(ms)| Node identifier                              \
             | Version |Gossip| TPU  |TPUfwd| TVU  |TVUfwd|Repair|ServeR|ShredVer\n\
             ------------------+-------+----------------------------------------------+---------+\
             ------+------+------+------+------+------+------+--------\n\
             {}\
             Nodes: {}{}{}",
            nodes.join(""),
            nodes.len().saturating_sub(shred_spy_nodes),
            if total_spy_nodes > 0 {
                format!("\nSpies: {}", total_spy_nodes)
            } else {
                "".to_string()
            },
            if different_shred_nodes > 0 {
                format!(
                    "\nNodes with different shred version: {}",
                    different_shred_nodes
                )
            } else {
                "".to_string()
            }
        )
    }

    // TODO: This has a race condition if called from more than one thread.
    pub fn push_lowest_slot(&self, min: Slot) {
        let self_pubkey = self.id();
        let last = {
            let gossip_crds = self.gossip.crds.read().unwrap();
            gossip_crds
                .get::<&LowestSlot>(self_pubkey)
                .map(|x| x.lowest)
                .unwrap_or_default()
        };
        if min > last {
            let now = timestamp();
            let entry = CrdsValue::new_signed(
                CrdsData::LowestSlot(0, LowestSlot::new(self_pubkey, min, now)),
                &self.keypair(),
            );
            self.local_message_pending_push_queue
                .lock()
                .unwrap()
                .push(entry);
        }
    }

    // TODO: If two threads call into this function then epoch_slot_index has a
    // race condition and the threads will overwrite each other in crds table.
    pub fn push_epoch_slots(&self, mut update: &[Slot]) {
        let self_pubkey = self.id();
        let current_slots: Vec<_> = {
            let gossip_crds =
                self.time_gossip_read_lock("lookup_epoch_slots", &self.stats.epoch_slots_lookup);
            (0..crds_value::MAX_EPOCH_SLOTS)
                .filter_map(|ix| {
                    let label = CrdsValueLabel::EpochSlots(ix, self_pubkey);
                    let epoch_slots = gossip_crds.get::<&CrdsValue>(&label)?.epoch_slots()?;
                    let first_slot = epoch_slots.first_slot()?;
                    Some((epoch_slots.wallclock, first_slot, ix))
                })
                .collect()
        };
        let min_slot: Slot = current_slots
            .iter()
            .map(|(_wallclock, slot, _index)| *slot)
            .min()
            .unwrap_or_default();
        let max_slot: Slot = update.iter().max().cloned().unwrap_or(0);
        let total_slots = max_slot as isize - min_slot as isize;
        // WARN if CRDS is not storing at least a full epoch worth of slots
        if DEFAULT_SLOTS_PER_EPOCH as isize > total_slots
            && crds_value::MAX_EPOCH_SLOTS as usize <= current_slots.len()
        {
            inc_new_counter_warn!("cluster_info-epoch_slots-filled", 1);
            warn!(
                "EPOCH_SLOTS are filling up FAST {}/{}",
                total_slots,
                current_slots.len()
            );
        }
        let mut reset = false;
        let mut epoch_slot_index = match current_slots.iter().max() {
            Some((_wallclock, _slot, index)) => *index,
            None => 0,
        };
        let mut entries = Vec::default();
        let keypair = self.keypair();
        while !update.is_empty() {
            let ix = (epoch_slot_index % crds_value::MAX_EPOCH_SLOTS) as u8;
            let now = timestamp();
            let mut slots = if !reset {
                self.lookup_epoch_slots(ix)
            } else {
                EpochSlots::new(self_pubkey, now)
            };
            let n = slots.fill(update, now);
            update = &update[n..];
            if n > 0 {
                let epoch_slots = CrdsData::EpochSlots(ix, slots);
                let entry = CrdsValue::new_signed(epoch_slots, &keypair);
                entries.push(entry);
            }
            epoch_slot_index += 1;
            reset = true;
        }
        let mut gossip_crds = self.gossip.crds.write().unwrap();
        let now = timestamp();
        for entry in entries {
            if let Err(err) = gossip_crds.insert(entry, now) {
                error!("push_epoch_slots failed: {:?}", err);
            }
        }
    }

    fn time_gossip_read_lock<'a>(
        &'a self,
        label: &'static str,
        counter: &'a Counter,
    ) -> TimedGuard<'a, RwLockReadGuard<Crds>> {
        TimedGuard::new(self.gossip.crds.read().unwrap(), label, counter)
    }

    pub fn push_message(&self, message: CrdsValue) {
        self.local_message_pending_push_queue
            .lock()
            .unwrap()
            .push(message);
    }

    pub fn push_accounts_hashes(&self, accounts_hashes: Vec<(Slot, Hash)>) {
        if accounts_hashes.len() > MAX_SNAPSHOT_HASHES {
            warn!(
                "accounts hashes too large, ignored: {}",
                accounts_hashes.len(),
            );
            return;
        }

        let message = CrdsData::AccountsHashes(SnapshotHash::new(self.id(), accounts_hashes));
        self.push_message(CrdsValue::new_signed(message, &self.keypair()));
    }

    pub fn push_snapshot_hashes(&self, snapshot_hashes: Vec<(Slot, Hash)>) {
        if snapshot_hashes.len() > MAX_SNAPSHOT_HASHES {
            warn!(
                "snapshot hashes too large, ignored: {}",
                snapshot_hashes.len(),
            );
            return;
        }

        let message = CrdsData::SnapshotHashes(SnapshotHash::new(self.id(), snapshot_hashes));
        self.push_message(CrdsValue::new_signed(message, &self.keypair()));
    }

    pub fn push_vote_at_index(&self, vote: Transaction, vote_index: u8) {
        assert!((vote_index as usize) < MAX_LOCKOUT_HISTORY);
        let self_pubkey = self.id();
        let now = timestamp();
        let vote = Vote::new(self_pubkey, vote, now);
        let vote = CrdsData::Vote(vote_index, vote);
        let vote = CrdsValue::new_signed(vote, &self.keypair());
        let mut gossip_crds = self.gossip.crds.write().unwrap();
        if let Err(err) = gossip_crds.insert(vote, now) {
            error!("push_vote failed: {:?}", err);
        }
    }

    pub fn push_vote(&self, tower: &[Slot], vote: Transaction) {
        debug_assert!(tower.iter().tuple_windows().all(|(a, b)| a < b));
        // Find a crds vote which is evicted from the tower, and recycle its
        // vote-index. This can be either an old vote which is popped off the
        // deque, or recent vote which has expired before getting enough
        // confirmations.
        // If all votes are still in the tower, add a new vote-index. If more
        // than one vote is evicted, the oldest one by wallclock is returned in
        // order to allow more recent votes more time to propagate through
        // gossip.
        // TODO: When there are more than one vote evicted from the tower, only
        // one crds vote is overwritten here. Decide what to do with the rest.
        let mut num_crds_votes = 0;
        let self_pubkey = self.id();
        // Returns true if the tower does not contain the vote.slot.
        let should_evict_vote = |vote: &Vote| -> bool {
            match vote.slot() {
                Some(slot) => !tower.contains(&slot),
                None => {
                    error!("crds vote with no slots!");
                    true
                }
            }
        };
        let vote_index = {
            let gossip_crds =
                self.time_gossip_read_lock("gossip_read_push_vote", &self.stats.push_vote_read);
            (0..MAX_LOCKOUT_HISTORY as u8)
                .filter_map(|ix| {
                    let vote = CrdsValueLabel::Vote(ix, self_pubkey);
                    let vote: &CrdsData = gossip_crds.get(&vote)?;
                    num_crds_votes += 1;
                    match &vote {
                        CrdsData::Vote(_, vote) if should_evict_vote(vote) => {
                            Some((vote.wallclock, ix))
                        }
                        CrdsData::Vote(_, _) => None,
                        _ => panic!("this should not happen!"),
                    }
                })
                .min() // Boot the oldest evicted vote by wallclock.
                .map(|(_ /*wallclock*/, ix)| ix)
        };
        let vote_index = vote_index.unwrap_or(num_crds_votes);
        self.push_vote_at_index(vote, vote_index);
    }

    pub fn refresh_vote(&self, vote: Transaction, vote_slot: Slot) {
        let vote_index = {
            let self_pubkey = self.id();
            let gossip_crds =
                self.time_gossip_read_lock("gossip_read_push_vote", &self.stats.push_vote_read);
            (0..MAX_LOCKOUT_HISTORY as u8).find(|ix| {
                let vote = CrdsValueLabel::Vote(*ix, self_pubkey);
                if let Some(vote) = gossip_crds.get::<&CrdsData>(&vote) {
                    match &vote {
                        CrdsData::Vote(_, prev_vote) => match prev_vote.slot() {
                            Some(prev_vote_slot) => prev_vote_slot == vote_slot,
                            None => {
                                error!("crds vote with no slots!");
                                false
                            }
                        },
                        _ => panic!("this should not happen!"),
                    }
                } else {
                    false
                }
            })
        };

        // If you don't see a vote with the same slot yet, this means you probably
        // restarted, and need to wait for your oldest vote to propagate back to you.
        //
        // We don't write to an arbitrary index, because it may replace one of this validator's
        // existing votes on the network.
        if let Some(vote_index) = vote_index {
            self.push_vote_at_index(vote, vote_index);
        }
    }

    pub fn send_transaction(
        &self,
        transaction: &Transaction,
        tpu: Option<SocketAddr>,
    ) -> Result<(), GossipError> {
        let tpu = tpu.unwrap_or_else(|| self.my_contact_info().tpu);
        let buf = serialize(transaction)?;
        self.socket.send_to(&buf, &tpu)?;
        Ok(())
    }

    /// Returns votes inserted since the given cursor.
    pub fn get_votes(&self, cursor: &mut Cursor) -> (Vec<CrdsValueLabel>, Vec<Transaction>) {
        let (labels, txs): (_, Vec<_>) = self
            .time_gossip_read_lock("get_votes", &self.stats.get_votes)
            .get_votes(cursor)
            .map(|vote| {
                let transaction = match &vote.value.data {
                    CrdsData::Vote(_, vote) => vote.transaction().clone(),
                    _ => panic!("this should not happen!"),
                };
                (vote.value.label(), transaction)
            })
            .unzip();
        inc_new_counter_info!("cluster_info-get_votes-count", txs.len());
        (labels, txs)
    }

    pub fn push_duplicate_shred(
        &self,
        shred: &Shred,
        other_payload: &[u8],
    ) -> Result<(), GossipError> {
        self.gossip.push_duplicate_shred(
            &self.keypair(),
            shred,
            other_payload,
            None::<fn(Slot) -> Option<Pubkey>>, // Leader schedule
            DUPLICATE_SHRED_MAX_PAYLOAD_SIZE,
        )?;
        Ok(())
    }

    pub fn get_accounts_hash_for_node<F, Y>(&self, pubkey: &Pubkey, map: F) -> Option<Y>
    where
        F: FnOnce(&Vec<(Slot, Hash)>) -> Y,
    {
        self.time_gossip_read_lock("get_accounts_hash", &self.stats.get_accounts_hash)
            .get::<&CrdsValue>(&CrdsValueLabel::AccountsHashes(*pubkey))
            .map(|x| &x.accounts_hash().unwrap().hashes)
            .map(map)
    }

    pub fn get_snapshot_hash_for_node<F, Y>(&self, pubkey: &Pubkey, map: F) -> Option<Y>
    where
        F: FnOnce(&Vec<(Slot, Hash)>) -> Y,
    {
        let gossip_crds = self.gossip.crds.read().unwrap();
        let hashes = &gossip_crds.get::<&SnapshotHash>(*pubkey)?.hashes;
        Some(map(hashes))
    }

    /// Returns epoch-slots inserted since the given cursor.
    /// Excludes entries from nodes with unkown or different shred version.
    pub fn get_epoch_slots(&self, cursor: &mut Cursor) -> Vec<EpochSlots> {
        let self_shred_version = Some(self.my_shred_version());
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_epoch_slots(cursor)
            .filter(|entry| {
                let origin = entry.value.pubkey();
                gossip_crds.get_shred_version(&origin) == self_shred_version
            })
            .map(|entry| match &entry.value.data {
                CrdsData::EpochSlots(_, slots) => slots.clone(),
                _ => panic!("this should not happen!"),
            })
            .collect()
    }

    pub fn get_node_version(&self, pubkey: &Pubkey) -> Option<solana_version::Version> {
        let gossip_crds = self.gossip.crds.read().unwrap();
        if let Some(version) = gossip_crds.get::<&Version>(*pubkey) {
            return Some(version.version.clone());
        }
        let version: &crds_value::LegacyVersion = gossip_crds.get(*pubkey)?;
        Some(version.version.clone().into())
    }

    /// all validators that have a valid rpc port regardless of `shred_version`.
    pub fn all_rpc_peers(&self) -> Vec<ContactInfo> {
        let self_pubkey = self.id();
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_nodes_contact_info()
            .filter(|x| {
                x.id != self_pubkey
                    && ContactInfo::is_valid_address(&x.rpc, &self.socket_addr_space)
            })
            .cloned()
            .collect()
    }

    // All nodes in gossip (including spy nodes) and the last time we heard about them
    pub fn all_peers(&self) -> Vec<(ContactInfo, u64)> {
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_nodes()
            .map(|x| (x.value.contact_info().unwrap().clone(), x.local_timestamp))
            .collect()
    }

    pub fn gossip_peers(&self) -> Vec<ContactInfo> {
        let me = self.id();
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_nodes_contact_info()
            // shred_version not considered for gossip peers (ie, spy nodes do not set shred_version)
            .filter(|x| {
                x.id != me && ContactInfo::is_valid_address(&x.gossip, &self.socket_addr_space)
            })
            .cloned()
            .collect()
    }

    /// all validators that have a valid tvu port regardless of `shred_version`.
    pub fn all_tvu_peers(&self) -> Vec<ContactInfo> {
        let self_pubkey = self.id();
        self.time_gossip_read_lock("all_tvu_peers", &self.stats.all_tvu_peers)
            .get_nodes_contact_info()
            .filter(|x| {
                ContactInfo::is_valid_address(&x.tvu, &self.socket_addr_space)
                    && x.id != self_pubkey
            })
            .cloned()
            .collect()
    }

    /// all validators that have a valid tvu port and are on the same `shred_version`.
    pub fn tvu_peers(&self) -> Vec<ContactInfo> {
        let self_pubkey = self.id();
        let self_shred_version = self.my_shred_version();
        self.time_gossip_read_lock("tvu_peers", &self.stats.tvu_peers)
            .get_nodes_contact_info()
            .filter(|node| {
                node.id != self_pubkey
                    && node.shred_version == self_shred_version
                    && ContactInfo::is_valid_tvu_address(&node.tvu)
            })
            .cloned()
            .collect()
    }

    /// all tvu peers with valid gossip addrs that likely have the slot being requested
    pub fn repair_peers(&self, slot: Slot) -> Vec<ContactInfo> {
        let _st = ScopedTimer::from(&self.stats.repair_peers);
        let self_pubkey = self.id();
        let self_shred_version = self.my_shred_version();
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_nodes_contact_info()
            .filter(|node| {
                node.id != self_pubkey
                    && node.shred_version == self_shred_version
                    && ContactInfo::is_valid_tvu_address(&node.tvu)
                    && ContactInfo::is_valid_address(&node.serve_repair, &self.socket_addr_space)
                    && match gossip_crds.get::<&LowestSlot>(node.id) {
                        None => true, // fallback to legacy behavior
                        Some(lowest_slot) => lowest_slot.lowest <= slot,
                    }
            })
            .cloned()
            .collect()
    }

    fn is_spy_node(contact_info: &ContactInfo, socket_addr_space: &SocketAddrSpace) -> bool {
        !ContactInfo::is_valid_address(&contact_info.tpu, socket_addr_space)
            || !ContactInfo::is_valid_address(&contact_info.gossip, socket_addr_space)
            || !ContactInfo::is_valid_address(&contact_info.tvu, socket_addr_space)
    }

    /// compute broadcast table
    pub fn tpu_peers(&self) -> Vec<ContactInfo> {
        let self_pubkey = self.id();
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_nodes_contact_info()
            .filter(|x| {
                x.id != self_pubkey
                    && ContactInfo::is_valid_address(&x.tpu, &self.socket_addr_space)
            })
            .cloned()
            .collect()
    }

    /// retransmit messages to a list of nodes
    /// # Remarks
    /// We need to avoid having obj locked while doing a io, such as the `send_to`
    pub fn retransmit_to(
        peers: &[&ContactInfo],
        packet: &Packet,
        s: &UdpSocket,
        forwarded: bool,
        socket_addr_space: &SocketAddrSpace,
    ) {
        trace!("retransmit orders {}", peers.len());
        let dests: Vec<_> = if forwarded {
            peers
                .iter()
                .map(|peer| &peer.tvu_forwards)
                .filter(|addr| ContactInfo::is_valid_address(addr, socket_addr_space))
                .collect()
        } else {
            peers
                .iter()
                .map(|peer| &peer.tvu)
                .filter(|addr| socket_addr_space.check(addr))
                .collect()
        };
        let data = &packet.data[..packet.meta.size];

        if let Err(SendPktsError::IoError(ioerr, num_failed)) = multi_target_send(s, data, &dests) {
            inc_new_counter_info!("cluster_info-retransmit-packets", dests.len(), 1);
            inc_new_counter_error!("cluster_info-retransmit-error", num_failed, 1);
            error!(
                "retransmit_to multi_target_send error: {:?}, {}/{} packets failed",
                ioerr,
                num_failed,
                dests.len(),
            );
        }
    }

    fn insert_self(&self) {
        let value = CrdsValue::new_signed(
            CrdsData::ContactInfo(self.my_contact_info()),
            &self.keypair(),
        );
        let _ = self.gossip.crds.write().unwrap().insert(value, timestamp());
    }

    // If the network entrypoint hasn't been discovered yet, add it to the crds table
    fn append_entrypoint_to_pulls(
        &self,
        thread_pool: &ThreadPool,
        pulls: &mut Vec<(ContactInfo, Vec<CrdsFilter>)>,
    ) {
        const THROTTLE_DELAY: u64 = CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS / 2;
        let entrypoint = {
            let mut entrypoints = self.entrypoints.write().unwrap();
            let entrypoint = match entrypoints.choose_mut(&mut rand::thread_rng()) {
                Some(entrypoint) => entrypoint,
                None => return,
            };
            if !pulls.is_empty() {
                let now = timestamp();
                if now <= entrypoint.wallclock.saturating_add(THROTTLE_DELAY) {
                    return;
                }
                entrypoint.wallclock = now;
                if self
                    .time_gossip_read_lock("entrypoint", &self.stats.entrypoint)
                    .get_nodes_contact_info()
                    .any(|node| node.gossip == entrypoint.gossip)
                {
                    return; // Found the entrypoint, no need to pull from it
                }
            }
            entrypoint.clone()
        };
        let filters = match pulls.first() {
            Some((_, filters)) => filters.clone(),
            None => {
                let _st = ScopedTimer::from(&self.stats.entrypoint2);
                self.gossip
                    .pull
                    .build_crds_filters(thread_pool, &self.gossip.crds, MAX_BLOOM_SIZE)
            }
        };
        self.stats.pull_from_entrypoint_count.add_relaxed(1);
        pulls.push((entrypoint, filters));
    }

    /// Splits an input feed of serializable data into chunks where the sum of
    /// serialized size of values within each chunk is no larger than
    /// max_chunk_size.
    /// Note: some messages cannot be contained within that size so in the worst case this returns
    /// N nested Vecs with 1 item each.
    fn split_gossip_messages<I, T>(
        max_chunk_size: usize,
        data_feed: I,
    ) -> impl Iterator<Item = Vec<T>>
    where
        T: Serialize + Debug,
        I: IntoIterator<Item = T>,
    {
        let mut data_feed = data_feed.into_iter().fuse();
        let mut buffer = vec![];
        let mut buffer_size = 0; // Serialized size of buffered values.
        std::iter::from_fn(move || loop {
            match data_feed.next() {
                None => {
                    return if buffer.is_empty() {
                        None
                    } else {
                        Some(std::mem::take(&mut buffer))
                    };
                }
                Some(data) => {
                    let data_size = match serialized_size(&data) {
                        Ok(size) => size as usize,
                        Err(err) => {
                            error!("serialized_size failed: {}", err);
                            continue;
                        }
                    };
                    if buffer_size + data_size <= max_chunk_size {
                        buffer_size += data_size;
                        buffer.push(data);
                    } else if data_size <= max_chunk_size {
                        buffer_size = data_size;
                        return Some(std::mem::replace(&mut buffer, vec![data]));
                    } else {
                        error!(
                            "dropping data larger than the maximum chunk size {:?}",
                            data
                        );
                    }
                }
            }
        })
    }

    #[allow(clippy::type_complexity)]
    fn new_pull_requests(
        &self,
        thread_pool: &ThreadPool,
        gossip_validators: Option<&HashSet<Pubkey>>,
        stakes: &HashMap<Pubkey, u64>,
    ) -> (
        Vec<(SocketAddr, Ping)>,     // Ping packets.
        Vec<(SocketAddr, Protocol)>, // Pull requests
    ) {
        let now = timestamp();
        let mut pings = Vec::new();
        let mut pulls: Vec<_> = {
            let _st = ScopedTimer::from(&self.stats.new_pull_requests);
            match self.gossip.new_pull_request(
                thread_pool,
                self.keypair().deref(),
                self.my_shred_version(),
                now,
                gossip_validators,
                stakes,
                MAX_BLOOM_SIZE,
                &self.ping_cache,
                &mut pings,
                &self.socket_addr_space,
            ) {
                Err(_) => Vec::default(),
                Ok((peer, filters)) => vec![(peer, filters)],
            }
        };
        self.append_entrypoint_to_pulls(thread_pool, &mut pulls);
        let num_requests = pulls.iter().map(|(_, filters)| filters.len() as u64).sum();
        self.stats.new_pull_requests_count.add_relaxed(num_requests);
        {
            let _st = ScopedTimer::from(&self.stats.mark_pull_request);
            for (peer, _) in &pulls {
                self.gossip.mark_pull_request_creation_time(peer.id, now);
            }
        }
        let self_info = CrdsData::ContactInfo(self.my_contact_info());
        let self_info = CrdsValue::new_signed(self_info, &self.keypair());
        let pulls = pulls
            .into_iter()
            .flat_map(|(peer, filters)| repeat(peer.gossip).zip(filters))
            .map(|(gossip_addr, filter)| {
                let request = Protocol::PullRequest(filter, self_info.clone());
                (gossip_addr, request)
            });
        self.stats
            .new_pull_requests_pings_count
            .add_relaxed(pings.len() as u64);
        (pings, pulls.collect())
    }

    fn drain_push_queue(&self) -> Vec<CrdsValue> {
        let mut push_queue = self.local_message_pending_push_queue.lock().unwrap();
        std::mem::take(&mut *push_queue)
    }
    // Used in tests
    pub fn flush_push_queue(&self) {
        let pending_push_messages = self.drain_push_queue();
        let mut gossip_crds = self.gossip.crds.write().unwrap();
        let now = timestamp();
        for entry in pending_push_messages {
            let _ = gossip_crds.insert(entry, now);
        }
    }
    fn new_push_requests(
        &self,
        stakes: &HashMap<Pubkey, u64>,
        require_stake_for_gossip: bool,
    ) -> Vec<(SocketAddr, Protocol)> {
        let self_id = self.id();
        let mut push_messages = {
            let _st = ScopedTimer::from(&self.stats.new_push_requests);
            self.gossip
                .new_push_messages(self.drain_push_queue(), timestamp())
        };
        if require_stake_for_gossip {
            push_messages.retain(|_, data| {
                retain_staked(data, stakes);
                !data.is_empty()
            })
        }
        let push_messages: Vec<_> = {
            let gossip_crds =
                self.time_gossip_read_lock("push_req_lookup", &self.stats.new_push_requests2);
            push_messages
                .into_iter()
                .filter_map(|(pubkey, messages)| {
                    let peer: &ContactInfo = gossip_crds.get(pubkey)?;
                    Some((peer.gossip, messages))
                })
                .collect()
        };
        let messages: Vec<_> = push_messages
            .into_iter()
            .flat_map(|(peer, msgs)| {
                Self::split_gossip_messages(PUSH_MESSAGE_MAX_PAYLOAD_SIZE, msgs)
                    .map(move |payload| (peer, Protocol::PushMessage(self_id, payload)))
            })
            .collect();
        self.stats
            .new_push_requests_num
            .add_relaxed(messages.len() as u64);
        messages
    }

    // Generate new push and pull requests
    fn generate_new_gossip_requests(
        &self,
        thread_pool: &ThreadPool,
        gossip_validators: Option<&HashSet<Pubkey>>,
        stakes: &HashMap<Pubkey, u64>,
        generate_pull_requests: bool,
        require_stake_for_gossip: bool,
    ) -> Vec<(SocketAddr, Protocol)> {
        self.trim_crds_table(CRDS_UNIQUE_PUBKEY_CAPACITY, stakes);
        // This will flush local pending push messages before generating
        // pull-request bloom filters, preventing pull responses to return the
        // same values back to the node itself. Note that packets will arrive
        // and are processed out of order.
        let mut out: Vec<_> = self.new_push_requests(stakes, require_stake_for_gossip);
        self.stats
            .packets_sent_push_messages_count
            .add_relaxed(out.len() as u64);
        if generate_pull_requests {
            let (pings, pull_requests) =
                self.new_pull_requests(thread_pool, gossip_validators, stakes);
            self.stats
                .packets_sent_pull_requests_count
                .add_relaxed(pull_requests.len() as u64);
            let pings = pings
                .into_iter()
                .map(|(addr, ping)| (addr, Protocol::PingMessage(ping)));
            out.extend(pull_requests);
            out.extend(pings);
        }
        out
    }

    /// At random pick a node and try to get updated changes from them
    fn run_gossip(
        &self,
        thread_pool: &ThreadPool,
        gossip_validators: Option<&HashSet<Pubkey>>,
        recycler: &PacketsRecycler,
        stakes: &HashMap<Pubkey, u64>,
        sender: &PacketSender,
        generate_pull_requests: bool,
        require_stake_for_gossip: bool,
    ) -> Result<(), GossipError> {
        let reqs = self.generate_new_gossip_requests(
            thread_pool,
            gossip_validators,
            stakes,
            generate_pull_requests,
            require_stake_for_gossip,
        );
        if !reqs.is_empty() {
            let packets = to_packets_with_destination(recycler.clone(), &reqs);
            self.stats
                .packets_sent_gossip_requests_count
                .add_relaxed(packets.packets.len() as u64);
            sender.send(packets)?;
        }
        Ok(())
    }

    fn process_entrypoints(&self) -> bool {
        let mut entrypoints = self.entrypoints.write().unwrap();
        if entrypoints.is_empty() {
            // No entrypoint specified.  Nothing more to process
            return true;
        }
        for entrypoint in entrypoints.iter_mut() {
            if entrypoint.id == Pubkey::default() {
                // If a pull from the entrypoint was successful it should exist in the CRDS table
                if let Some(entrypoint_from_gossip) =
                    self.lookup_contact_info_by_gossip_addr(&entrypoint.gossip)
                {
                    // Update the entrypoint's id so future entrypoint pulls correctly reference it
                    *entrypoint = entrypoint_from_gossip;
                }
            }
        }
        // Adopt an entrypoint's `shred_version` if ours is unset
        if self.my_shred_version() == 0 {
            if let Some(entrypoint) = entrypoints
                .iter()
                .find(|entrypoint| entrypoint.shred_version != 0)
            {
                info!(
                    "Setting shred version to {:?} from entrypoint {:?}",
                    entrypoint.shred_version, entrypoint.id
                );
                self.my_contact_info.write().unwrap().shred_version = entrypoint.shred_version;
            }
        }
        self.my_shred_version() != 0
            && entrypoints
                .iter()
                .all(|entrypoint| entrypoint.id != Pubkey::default())
    }

    fn handle_purge(
        &self,
        thread_pool: &ThreadPool,
        bank_forks: Option<&RwLock<BankForks>>,
        stakes: &HashMap<Pubkey, u64>,
    ) {
        let self_pubkey = self.id();
        let epoch_duration = get_epoch_duration(bank_forks);
        let timeouts = self
            .gossip
            .make_timeouts(self_pubkey, stakes, epoch_duration);
        let num_purged = {
            let _st = ScopedTimer::from(&self.stats.purge);
            self.gossip
                .purge(&self_pubkey, thread_pool, timestamp(), &timeouts)
        };
        inc_new_counter_info!("cluster_info-purge-count", num_purged);
    }

    // Trims the CRDS table by dropping all values associated with the pubkeys
    // with the lowest stake, so that the number of unique pubkeys are bounded.
    fn trim_crds_table(&self, cap: usize, stakes: &HashMap<Pubkey, u64>) {
        if !self.gossip.crds.read().unwrap().should_trim(cap) {
            return;
        }
        let keep: Vec<_> = self
            .entrypoints
            .read()
            .unwrap()
            .iter()
            .map(|k| k.id)
            .chain(std::iter::once(self.id()))
            .collect();
        self.stats.trim_crds_table.add_relaxed(1);
        let mut gossip_crds = self.gossip.crds.write().unwrap();
        match gossip_crds.trim(cap, &keep, stakes, timestamp()) {
            Err(err) => {
                self.stats.trim_crds_table_failed.add_relaxed(1);
                // TODO: Stakes are comming from the root-bank. Debug why/when
                // they are empty/zero.
                debug!("crds table trim failed: {:?}", err);
            }
            Ok(num_purged) => {
                self.stats
                    .trim_crds_table_purged_values_count
                    .add_relaxed(num_purged as u64);
            }
        }
    }

    /// randomly pick a node and ask them for updates asynchronously
    pub fn gossip(
        self: Arc<Self>,
        bank_forks: Option<Arc<RwLock<BankForks>>>,
        sender: PacketSender,
        gossip_validators: Option<HashSet<Pubkey>>,
        exit: &Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let exit = exit.clone();
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(std::cmp::min(get_thread_count(), 8))
            .thread_name(|i| format!("ClusterInfo::gossip-{}", i))
            .build()
            .unwrap();
        Builder::new()
            .name("solana-gossip".to_string())
            .spawn(move || {
                let mut last_push = timestamp();
                let mut last_contact_info_trace = timestamp();
                let mut last_contact_info_save = timestamp();
                let mut entrypoints_processed = false;
                let recycler = PacketsRecycler::default();
                let crds_data = vec![
                    CrdsData::Version(Version::new(self.id())),
                    CrdsData::NodeInstance(
                        self.instance.read().unwrap().with_wallclock(timestamp()),
                    ),
                ];
                for value in crds_data {
                    let value = CrdsValue::new_signed(value, &self.keypair());
                    self.push_message(value);
                }
                let mut generate_pull_requests = true;
                loop {
                    let start = timestamp();
                    if self.contact_debug_interval != 0
                        && start - last_contact_info_trace > self.contact_debug_interval
                    {
                        // Log contact info
                        info!(
                            "\n{}\n\n{}",
                            self.contact_info_trace(),
                            self.rpc_info_trace()
                        );
                        last_contact_info_trace = start;
                    }

                    if self.contact_save_interval != 0
                        && start - last_contact_info_save > self.contact_save_interval
                    {
                        self.save_contact_info();
                        last_contact_info_save = start;
                    }

                    let (stakes, feature_set) = match bank_forks {
                        Some(ref bank_forks) => {
                            let root_bank = bank_forks.read().unwrap().root_bank();
                            (
                                root_bank.staked_nodes(),
                                Some(root_bank.feature_set.clone()),
                            )
                        }
                        None => (HashMap::new(), None),
                    };
                    let require_stake_for_gossip =
                        self.require_stake_for_gossip(feature_set.as_deref(), &stakes);
                    let _ = self.run_gossip(
                        &thread_pool,
                        gossip_validators.as_ref(),
                        &recycler,
                        &stakes,
                        &sender,
                        generate_pull_requests,
                        require_stake_for_gossip,
                    );
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    self.handle_purge(&thread_pool, bank_forks.as_deref(), &stakes);
                    entrypoints_processed = entrypoints_processed || self.process_entrypoints();
                    //TODO: possibly tune this parameter
                    //we saw a deadlock passing an self.read().unwrap().timeout into sleep
                    if start - last_push > CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS / 2 {
                        self.push_self(&stakes, gossip_validators.as_ref());
                        last_push = timestamp();
                    }
                    let elapsed = timestamp() - start;
                    if GOSSIP_SLEEP_MILLIS > elapsed {
                        let time_left = GOSSIP_SLEEP_MILLIS - elapsed;
                        sleep(Duration::from_millis(time_left));
                    }
                    generate_pull_requests = !generate_pull_requests;
                }
            })
            .unwrap()
    }

    fn handle_batch_prune_messages(&self, messages: Vec<(Pubkey, PruneData)>) {
        let _st = ScopedTimer::from(&self.stats.handle_batch_prune_messages_time);
        if messages.is_empty() {
            return;
        }
        self.stats
            .prune_message_count
            .add_relaxed(messages.len() as u64);
        self.stats.prune_message_len.add_relaxed(
            messages
                .iter()
                .map(|(_, data)| data.prunes.len() as u64)
                .sum(),
        );
        let mut prune_message_timeout = 0;
        let mut bad_prune_destination = 0;
        let self_pubkey = self.id();
        {
            let _st = ScopedTimer::from(&self.stats.process_prune);
            let now = timestamp();
            for (from, data) in messages {
                match self.gossip.process_prune_msg(
                    &self_pubkey,
                    &from,
                    &data.destination,
                    &data.prunes,
                    data.wallclock,
                    now,
                ) {
                    Err(CrdsGossipError::PruneMessageTimeout) => {
                        prune_message_timeout += 1;
                    }
                    Err(CrdsGossipError::BadPruneDestination) => {
                        bad_prune_destination += 1;
                    }
                    _ => (),
                }
            }
        }
        if prune_message_timeout != 0 {
            inc_new_counter_debug!("cluster_info-prune_message_timeout", prune_message_timeout);
        }
        if bad_prune_destination != 0 {
            inc_new_counter_debug!("cluster_info-bad_prune_destination", bad_prune_destination);
        }
    }

    fn handle_batch_pull_requests(
        &self,
        // from address, crds filter, caller contact info
        requests: Vec<(SocketAddr, CrdsFilter, CrdsValue)>,
        thread_pool: &ThreadPool,
        recycler: &PacketsRecycler,
        stakes: &HashMap<Pubkey, u64>,
        response_sender: &PacketSender,
        require_stake_for_gossip: bool,
    ) {
        let _st = ScopedTimer::from(&self.stats.handle_batch_pull_requests_time);
        if requests.is_empty() {
            return;
        }
        let self_pubkey = self.id();
        let requests: Vec<_> = thread_pool.install(|| {
            requests
                .into_par_iter()
                .with_min_len(1024)
                .filter(|(_, _, caller)| match caller.contact_info() {
                    None => false,
                    Some(caller) if caller.id == self_pubkey => {
                        warn!("PullRequest ignored, I'm talking to myself");
                        inc_new_counter_debug!("cluster_info-window-request-loopback", 1);
                        false
                    }
                    Some(_) => true,
                })
                .map(|(from_addr, filter, caller)| PullData {
                    from_addr,
                    caller,
                    filter,
                })
                .collect()
        });
        if !requests.is_empty() {
            self.stats
                .pull_requests_count
                .add_relaxed(requests.len() as u64);
            let response =
                self.handle_pull_requests(recycler, requests, stakes, require_stake_for_gossip);
            if !response.is_empty() {
                self.stats
                    .packets_sent_pull_responses_count
                    .add_relaxed(response.packets.len() as u64);
                let _ = response_sender.send(response);
            }
        }
    }

    fn update_data_budget(&self, num_staked: usize) -> usize {
        const INTERVAL_MS: u64 = 100;
        // allow 50kBps per staked validator, epoch slots + votes ~= 1.5kB/slot ~= 4kB/s
        const BYTES_PER_INTERVAL: usize = 5000;
        const MAX_BUDGET_MULTIPLE: usize = 5; // allow budget build-up to 5x the interval default
        let num_staked = num_staked.max(2);
        self.outbound_budget.update(INTERVAL_MS, |bytes| {
            std::cmp::min(
                bytes + num_staked * BYTES_PER_INTERVAL,
                MAX_BUDGET_MULTIPLE * num_staked * BYTES_PER_INTERVAL,
            )
        })
    }

    // Returns a predicate checking if the pull request is from a valid
    // address, and if the address have responded to a ping request. Also
    // appends ping packets for the addresses which need to be (re)verified.
    fn check_pull_request<'a, R>(
        &'a self,
        now: Instant,
        mut rng: &'a mut R,
        packets: &'a mut Packets,
    ) -> impl FnMut(&PullData) -> bool + 'a
    where
        R: Rng + CryptoRng,
    {
        let mut cache = HashMap::<(Pubkey, SocketAddr), bool>::new();
        let mut pingf = move || Ping::new_rand(&mut rng, &self.keypair()).ok();
        let mut ping_cache = self.ping_cache.lock().unwrap();
        let mut hard_check = move |node| {
            let (check, ping) = ping_cache.check(now, node, &mut pingf);
            if let Some(ping) = ping {
                let ping = Protocol::PingMessage(ping);
                match Packet::from_data(Some(&node.1), ping) {
                    Ok(packet) => packets.packets.push(packet),
                    Err(err) => error!("failed to write ping packet: {:?}", err),
                };
            }
            if !check {
                self.stats
                    .pull_request_ping_pong_check_failed_count
                    .add_relaxed(1)
            }
            check
        };
        // Because pull-responses are sent back to packet.meta.addr() of
        // incoming pull-requests, pings are also sent to request.from_addr (as
        // opposed to caller.gossip address).
        move |request| {
            ContactInfo::is_valid_address(&request.from_addr, &self.socket_addr_space) && {
                let node = (request.caller.pubkey(), request.from_addr);
                *cache.entry(node).or_insert_with(|| hard_check(node))
            }
        }
    }

    // Pull requests take an incoming bloom filter of contained entries from a node
    // and tries to send back to them the values it detects are missing.
    fn handle_pull_requests(
        &self,
        recycler: &PacketsRecycler,
        requests: Vec<PullData>,
        stakes: &HashMap<Pubkey, u64>,
        require_stake_for_gossip: bool,
    ) -> Packets {
        const DEFAULT_EPOCH_DURATION_MS: u64 = DEFAULT_SLOTS_PER_EPOCH * DEFAULT_MS_PER_SLOT;
        let mut time = Measure::start("handle_pull_requests");
        let callers = crds_value::filter_current(requests.iter().map(|r| &r.caller));
        {
            let _st = ScopedTimer::from(&self.stats.process_pull_requests);
            self.gossip
                .process_pull_requests(callers.cloned(), timestamp());
        }
        let output_size_limit =
            self.update_data_budget(stakes.len()) / PULL_RESPONSE_MIN_SERIALIZED_SIZE;
        let mut packets =
            Packets::new_unpinned_with_recycler(recycler.clone(), 64, "handle_pull_requests");
        let (caller_and_filters, addrs): (Vec<_>, Vec<_>) = {
            let mut rng = rand::thread_rng();
            let check_pull_request =
                self.check_pull_request(Instant::now(), &mut rng, &mut packets);
            requests
                .into_iter()
                .filter(check_pull_request)
                .map(|r| ((r.caller, r.filter), r.from_addr))
                .unzip()
        };
        let now = timestamp();
        let self_id = self.id();
        let mut pull_responses = {
            let _st = ScopedTimer::from(&self.stats.generate_pull_responses);
            self.gossip
                .generate_pull_responses(&caller_and_filters, output_size_limit, now)
        };
        if require_stake_for_gossip {
            for resp in &mut pull_responses {
                retain_staked(resp, stakes);
            }
        }
        let (responses, scores): (Vec<_>, Vec<_>) = addrs
            .iter()
            .zip(pull_responses)
            .flat_map(|(addr, responses)| repeat(addr).zip(responses))
            .map(|(addr, response)| {
                let age = now.saturating_sub(response.wallclock());
                let score = DEFAULT_EPOCH_DURATION_MS
                    .saturating_sub(age)
                    .div(CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS)
                    .max(1);
                let score = if stakes.contains_key(&response.pubkey()) {
                    2 * score
                } else {
                    score
                };
                let score = match response.data {
                    CrdsData::ContactInfo(_) => 2 * score,
                    _ => score,
                };
                ((addr, response), score)
            })
            .unzip();
        if responses.is_empty() {
            return packets;
        }
        let mut rng = rand::thread_rng();
        let shuffle = WeightedShuffle::new(&mut rng, &scores).unwrap();
        let mut total_bytes = 0;
        let mut sent = 0;
        for (addr, response) in shuffle.map(|i| &responses[i]) {
            let response = vec![response.clone()];
            let response = Protocol::PullResponse(self_id, response);
            match Packet::from_data(Some(addr), response) {
                Err(err) => error!("failed to write pull-response packet: {:?}", err),
                Ok(packet) => {
                    if self.outbound_budget.take(packet.meta.size) {
                        total_bytes += packet.meta.size;
                        packets.packets.push(packet);
                        sent += 1;
                    } else {
                        inc_new_counter_info!("gossip_pull_request-no_budget", 1);
                        break;
                    }
                }
            }
        }
        time.stop();
        let dropped_responses = responses.len() - sent;
        inc_new_counter_info!("gossip_pull_request-sent_requests", sent);
        inc_new_counter_info!("gossip_pull_request-dropped_requests", dropped_responses);
        debug!(
            "handle_pull_requests: {} sent: {} total: {} total_bytes: {}",
            time,
            sent,
            responses.len(),
            total_bytes
        );
        packets
    }

    fn handle_batch_pull_responses(
        &self,
        responses: Vec<(Pubkey, Vec<CrdsValue>)>,
        thread_pool: &ThreadPool,
        stakes: &HashMap<Pubkey, u64>,
        epoch_duration: Duration,
    ) {
        let _st = ScopedTimer::from(&self.stats.handle_batch_pull_responses_time);
        if responses.is_empty() {
            return;
        }
        fn extend<K, V>(hash_map: &mut HashMap<K, Vec<V>>, (key, mut value): (K, Vec<V>))
        where
            K: Eq + std::hash::Hash,
        {
            match hash_map.entry(key) {
                Entry::Occupied(mut entry) => {
                    let entry_value = entry.get_mut();
                    if entry_value.len() < value.len() {
                        std::mem::swap(entry_value, &mut value);
                    }
                    entry_value.extend(value);
                }
                Entry::Vacant(entry) => {
                    entry.insert(value);
                }
            }
        }
        fn merge<K, V>(
            mut hash_map: HashMap<K, Vec<V>>,
            other: HashMap<K, Vec<V>>,
        ) -> HashMap<K, Vec<V>>
        where
            K: Eq + std::hash::Hash,
        {
            if hash_map.len() < other.len() {
                return merge(other, hash_map);
            }
            for kv in other {
                extend(&mut hash_map, kv);
            }
            hash_map
        }
        let responses = thread_pool.install(|| {
            responses
                .into_par_iter()
                .with_min_len(1024)
                .fold(HashMap::new, |mut hash_map, kv| {
                    extend(&mut hash_map, kv);
                    hash_map
                })
                .reduce(HashMap::new, merge)
        });
        if !responses.is_empty() {
            let self_pubkey = self.id();
            let timeouts = self
                .gossip
                .make_timeouts(self_pubkey, stakes, epoch_duration);
            for (from, data) in responses {
                self.handle_pull_response(&from, data, &timeouts);
            }
        }
    }

    // Returns (failed, timeout, success)
    fn handle_pull_response(
        &self,
        from: &Pubkey,
        crds_values: Vec<CrdsValue>,
        timeouts: &HashMap<Pubkey, u64>,
    ) -> (usize, usize, usize) {
        let len = crds_values.len();
        trace!("PullResponse me: {} from: {} len={}", self.id(), from, len);
        let mut pull_stats = ProcessPullStats::default();
        let (filtered_pulls, filtered_pulls_expired_timeout, failed_inserts) = {
            let _st = ScopedTimer::from(&self.stats.filter_pull_response);
            self.gossip
                .filter_pull_responses(timeouts, crds_values, timestamp(), &mut pull_stats)
        };
        if !filtered_pulls.is_empty()
            || !filtered_pulls_expired_timeout.is_empty()
            || !failed_inserts.is_empty()
        {
            let _st = ScopedTimer::from(&self.stats.process_pull_response);
            self.gossip.process_pull_responses(
                from,
                filtered_pulls,
                filtered_pulls_expired_timeout,
                failed_inserts,
                timestamp(),
                &mut pull_stats,
            );
        }
        self.stats.process_pull_response_count.add_relaxed(1);
        self.stats.process_pull_response_len.add_relaxed(len as u64);
        self.stats
            .process_pull_response_timeout
            .add_relaxed(pull_stats.timeout_count as u64);
        self.stats
            .process_pull_response_fail_insert
            .add_relaxed(pull_stats.failed_insert as u64);
        self.stats
            .process_pull_response_fail_timeout
            .add_relaxed(pull_stats.failed_timeout as u64);
        self.stats
            .process_pull_response_success
            .add_relaxed(pull_stats.success as u64);

        (
            pull_stats.failed_insert + pull_stats.failed_timeout,
            pull_stats.timeout_count,
            pull_stats.success,
        )
    }

    fn handle_batch_ping_messages<I>(
        &self,
        pings: I,
        recycler: &PacketsRecycler,
        response_sender: &PacketSender,
    ) where
        I: IntoIterator<Item = (SocketAddr, Ping)>,
    {
        let _st = ScopedTimer::from(&self.stats.handle_batch_ping_messages_time);
        if let Some(response) = self.handle_ping_messages(pings, recycler) {
            let _ = response_sender.send(response);
        }
    }

    fn handle_ping_messages<I>(&self, pings: I, recycler: &PacketsRecycler) -> Option<Packets>
    where
        I: IntoIterator<Item = (SocketAddr, Ping)>,
    {
        let keypair = self.keypair();
        let packets: Vec<_> = pings
            .into_iter()
            .filter_map(|(addr, ping)| {
                let pong = Pong::new(&ping, &keypair).ok()?;
                let pong = Protocol::PongMessage(pong);
                match Packet::from_data(Some(&addr), pong) {
                    Ok(packet) => Some(packet),
                    Err(err) => {
                        error!("failed to write pong packet: {:?}", err);
                        None
                    }
                }
            })
            .collect();
        if packets.is_empty() {
            None
        } else {
            let packets =
                Packets::new_unpinned_with_recycler_data(recycler, "handle_ping_messages", packets);
            Some(packets)
        }
    }

    fn handle_batch_pong_messages<I>(&self, pongs: I, now: Instant)
    where
        I: IntoIterator<Item = (SocketAddr, Pong)>,
    {
        let _st = ScopedTimer::from(&self.stats.handle_batch_pong_messages_time);
        let mut pongs = pongs.into_iter().peekable();
        if pongs.peek().is_some() {
            let mut ping_cache = self.ping_cache.lock().unwrap();
            for (addr, pong) in pongs {
                ping_cache.add(&pong, addr, now);
            }
        }
    }

    #[allow(clippy::needless_collect)]
    fn handle_batch_push_messages(
        &self,
        messages: Vec<(Pubkey, Vec<CrdsValue>)>,
        thread_pool: &ThreadPool,
        recycler: &PacketsRecycler,
        stakes: &HashMap<Pubkey, u64>,
        response_sender: &PacketSender,
        require_stake_for_gossip: bool,
    ) {
        let _st = ScopedTimer::from(&self.stats.handle_batch_push_messages_time);
        if messages.is_empty() {
            return;
        }
        self.stats
            .push_message_count
            .add_relaxed(messages.len() as u64);
        let num_crds_values: u64 = messages.iter().map(|(_, data)| data.len() as u64).sum();
        self.stats
            .push_message_value_count
            .add_relaxed(num_crds_values);
        // Origins' pubkeys of upserted crds values.
        let origins: HashSet<_> = {
            let _st = ScopedTimer::from(&self.stats.process_push_message);
            let now = timestamp();
            messages
                .into_iter()
                .flat_map(|(from, crds_values)| {
                    self.gossip.process_push_message(&from, crds_values, now)
                })
                .collect()
        };
        // Generate prune messages.
        let self_pubkey = self.id();
        let prunes = {
            let _st = ScopedTimer::from(&self.stats.prune_received_cache);
            self.gossip
                .prune_received_cache(&self_pubkey, origins, stakes)
        };
        let prunes: Vec<(Pubkey /*from*/, Vec<Pubkey> /*origins*/)> = prunes
            .into_iter()
            .flat_map(|(from, prunes)| {
                repeat(from).zip(
                    prunes
                        .into_iter()
                        .chunks(MAX_PRUNE_DATA_NODES)
                        .into_iter()
                        .map(Iterator::collect)
                        .collect::<Vec<_>>(),
                )
            })
            .collect();

        let prune_messages: Vec<_> = {
            let gossip_crds = self.gossip.crds.read().unwrap();
            let wallclock = timestamp();
            let self_pubkey = self.id();
            thread_pool.install(|| {
                prunes
                    .into_par_iter()
                    .with_min_len(256)
                    .filter_map(|(from, prunes)| {
                        let peer: &ContactInfo = gossip_crds.get(from)?;
                        let mut prune_data = PruneData {
                            pubkey: self_pubkey,
                            prunes,
                            signature: Signature::default(),
                            destination: from,
                            wallclock,
                        };
                        prune_data.sign(&self.keypair());
                        let prune_message = Protocol::PruneMessage(self_pubkey, prune_data);
                        Some((peer.gossip, prune_message))
                    })
                    .collect()
            })
        };
        if prune_messages.is_empty() {
            return;
        }
        let mut packets = to_packets_with_destination(recycler.clone(), &prune_messages);
        let num_prune_packets = packets.packets.len();
        self.stats
            .push_response_count
            .add_relaxed(packets.packets.len() as u64);
        let new_push_requests = self.new_push_requests(stakes, require_stake_for_gossip);
        inc_new_counter_debug!("cluster_info-push_message-pushes", new_push_requests.len());
        for (address, request) in new_push_requests {
            if ContactInfo::is_valid_address(&address, &self.socket_addr_space) {
                match Packet::from_data(Some(&address), &request) {
                    Ok(packet) => packets.packets.push(packet),
                    Err(err) => error!("failed to write push-request packet: {:?}", err),
                }
            } else {
                trace!("Dropping Gossip push response, as destination is unknown");
            }
        }
        self.stats
            .packets_sent_prune_messages_count
            .add_relaxed(num_prune_packets as u64);
        self.stats
            .packets_sent_push_messages_count
            .add_relaxed((packets.packets.len() - num_prune_packets) as u64);
        let _ = response_sender.send(packets);
    }

    fn require_stake_for_gossip(
        &self,
        feature_set: Option<&FeatureSet>,
        stakes: &HashMap<Pubkey, u64>,
    ) -> bool {
        match feature_set {
            None => {
                self.stats
                    .require_stake_for_gossip_unknown_feature_set
                    .add_relaxed(1);
                false
            }
            Some(feature_set) => {
                if !feature_set.is_active(&feature_set::require_stake_for_gossip::id()) {
                    false
                } else if stakes.len() < MIN_NUM_STAKED_NODES {
                    self.stats
                        .require_stake_for_gossip_unknown_stakes
                        .add_relaxed(1);
                    false
                } else {
                    true
                }
            }
        }
    }

    fn process_packets(
        &self,
        packets: VecDeque<(/*from:*/ SocketAddr, Protocol)>,
        thread_pool: &ThreadPool,
        recycler: &PacketsRecycler,
        response_sender: &PacketSender,
        stakes: &HashMap<Pubkey, u64>,
        feature_set: Option<&FeatureSet>,
        epoch_duration: Duration,
        should_check_duplicate_instance: bool,
    ) -> Result<(), GossipError> {
        let _st = ScopedTimer::from(&self.stats.process_gossip_packets_time);
        // Filter out values if the shred-versions are different.
        let self_shred_version = self.my_shred_version();
        let packets = if self_shred_version == 0 {
            packets
        } else {
            let gossip_crds = self.gossip.crds.read().unwrap();
            thread_pool.install(|| {
                packets
                    .into_par_iter()
                    .with_min_len(1024)
                    .filter_map(|(from, msg)| {
                        let msg = filter_on_shred_version(
                            msg,
                            self_shred_version,
                            &gossip_crds,
                            &self.stats,
                        )?;
                        Some((from, msg))
                    })
                    .collect()
            })
        };

        // Check if there is a duplicate instance of
        // this node with more recent timestamp.
        let instance = self.instance.read().unwrap();
        let check_duplicate_instance = |values: &[CrdsValue]| {
            if should_check_duplicate_instance {
                for value in values {
                    if instance.check_duplicate(value) {
                        return Err(GossipError::DuplicateNodeInstance);
                    }
                }
            }
            Ok(())
        };
        // Split packets based on their types.
        let mut pull_requests = vec![];
        let mut pull_responses = vec![];
        let mut push_messages = vec![];
        let mut prune_messages = vec![];
        let mut ping_messages = vec![];
        let mut pong_messages = vec![];
        for (from_addr, packet) in packets {
            match packet {
                Protocol::PullRequest(filter, caller) => {
                    pull_requests.push((from_addr, filter, caller))
                }
                Protocol::PullResponse(from, data) => {
                    check_duplicate_instance(&data)?;
                    pull_responses.push((from, data));
                }
                Protocol::PushMessage(from, data) => {
                    check_duplicate_instance(&data)?;
                    push_messages.push((from, data));
                }
                Protocol::PruneMessage(from, data) => prune_messages.push((from, data)),
                Protocol::PingMessage(ping) => ping_messages.push((from_addr, ping)),
                Protocol::PongMessage(pong) => pong_messages.push((from_addr, pong)),
            }
        }
        self.stats
            .packets_received_pull_requests_count
            .add_relaxed(pull_requests.len() as u64);
        self.stats
            .packets_received_pull_responses_count
            .add_relaxed(pull_responses.len() as u64);
        self.stats
            .packets_received_push_messages_count
            .add_relaxed(push_messages.len() as u64);
        self.stats
            .packets_received_prune_messages_count
            .add_relaxed(prune_messages.len() as u64);
        let require_stake_for_gossip = self.require_stake_for_gossip(feature_set, stakes);
        if require_stake_for_gossip {
            for (_, data) in &mut pull_responses {
                retain_staked(data, stakes);
            }
            for (_, data) in &mut push_messages {
                retain_staked(data, stakes);
            }
            pull_responses.retain(|(_, data)| !data.is_empty());
            push_messages.retain(|(_, data)| !data.is_empty());
        }
        self.handle_batch_ping_messages(ping_messages, recycler, response_sender);
        self.handle_batch_prune_messages(prune_messages);
        self.handle_batch_push_messages(
            push_messages,
            thread_pool,
            recycler,
            stakes,
            response_sender,
            require_stake_for_gossip,
        );
        self.handle_batch_pull_responses(pull_responses, thread_pool, stakes, epoch_duration);
        self.trim_crds_table(CRDS_UNIQUE_PUBKEY_CAPACITY, stakes);
        self.handle_batch_pong_messages(pong_messages, Instant::now());
        self.handle_batch_pull_requests(
            pull_requests,
            thread_pool,
            recycler,
            stakes,
            response_sender,
            require_stake_for_gossip,
        );
        Ok(())
    }

    // Consumes packets received from the socket, deserializing, sanitizing and
    // verifying them and then sending them down the channel for the actual
    // handling of requests/messages.
    fn run_socket_consume(
        &self,
        receiver: &PacketReceiver,
        sender: &Sender<Vec<(/*from:*/ SocketAddr, Protocol)>>,
        thread_pool: &ThreadPool,
    ) -> Result<(), GossipError> {
        const RECV_TIMEOUT: Duration = Duration::from_secs(1);
        let packets: Vec<_> = receiver.recv_timeout(RECV_TIMEOUT)?.packets.into();
        let mut packets = VecDeque::from(packets);
        for payload in receiver.try_iter() {
            packets.extend(payload.packets.iter().cloned());
            let excess_count = packets.len().saturating_sub(MAX_GOSSIP_TRAFFIC);
            if excess_count > 0 {
                packets.drain(0..excess_count);
                self.stats
                    .gossip_packets_dropped_count
                    .add_relaxed(excess_count as u64);
            }
        }
        self.stats
            .packets_received_count
            .add_relaxed(packets.len() as u64);
        let verify_packet = |packet: Packet| {
            let data = &packet.data[..packet.meta.size];
            let protocol: Protocol = limited_deserialize(data).ok()?;
            protocol.sanitize().ok()?;
            let protocol = protocol.par_verify()?;
            Some((packet.meta.addr(), protocol))
        };
        let packets: Vec<_> = {
            let _st = ScopedTimer::from(&self.stats.verify_gossip_packets_time);
            thread_pool.install(|| packets.into_par_iter().filter_map(verify_packet).collect())
        };
        self.stats
            .packets_received_verified_count
            .add_relaxed(packets.len() as u64);
        Ok(sender.send(packets)?)
    }

    /// Process messages from the network
    fn run_listen(
        &self,
        recycler: &PacketsRecycler,
        bank_forks: Option<&RwLock<BankForks>>,
        receiver: &Receiver<Vec<(/*from:*/ SocketAddr, Protocol)>>,
        response_sender: &PacketSender,
        thread_pool: &ThreadPool,
        last_print: &mut Instant,
        should_check_duplicate_instance: bool,
    ) -> Result<(), GossipError> {
        const RECV_TIMEOUT: Duration = Duration::from_secs(1);
        const SUBMIT_GOSSIP_STATS_INTERVAL: Duration = Duration::from_secs(2);
        let mut packets = VecDeque::from(receiver.recv_timeout(RECV_TIMEOUT)?);
        for payload in receiver.try_iter() {
            packets.extend(payload);
            let excess_count = packets.len().saturating_sub(MAX_GOSSIP_TRAFFIC);
            if excess_count > 0 {
                packets.drain(0..excess_count);
                self.stats
                    .gossip_packets_dropped_count
                    .add_relaxed(excess_count as u64);
            }
        }
        // Using root_bank instead of working_bank here so that an enbaled
        // feature does not roll back (if the feature happens to get enabled in
        // a minority fork).
        let (feature_set, stakes) = match bank_forks {
            None => (None, HashMap::default()),
            Some(bank_forks) => {
                let bank = bank_forks.read().unwrap().root_bank();
                let feature_set = bank.feature_set.clone();
                (Some(feature_set), bank.staked_nodes())
            }
        };
        self.process_packets(
            packets,
            thread_pool,
            recycler,
            response_sender,
            &stakes,
            feature_set.as_deref(),
            get_epoch_duration(bank_forks),
            should_check_duplicate_instance,
        )?;
        if last_print.elapsed() > SUBMIT_GOSSIP_STATS_INTERVAL {
            submit_gossip_stats(&self.stats, &self.gossip, &stakes);
            *last_print = Instant::now();
        }
        Ok(())
    }

    pub(crate) fn start_socket_consume_thread(
        self: Arc<Self>,
        receiver: PacketReceiver,
        sender: Sender<Vec<(/*from:*/ SocketAddr, Protocol)>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(get_thread_count().min(8))
            .thread_name(|i| format!("gossip-consume-{}", i))
            .build()
            .unwrap();
        let run_consume = move || {
            while !exit.load(Ordering::Relaxed) {
                match self.run_socket_consume(&receiver, &sender, &thread_pool) {
                    Err(GossipError::RecvTimeoutError(RecvTimeoutError::Disconnected)) => break,
                    Err(GossipError::RecvTimeoutError(RecvTimeoutError::Timeout)) => (),
                    Err(err) => error!("gossip consume: {}", err),
                    Ok(()) => (),
                }
            }
        };
        let thread_name = String::from("gossip-consume");
        Builder::new().name(thread_name).spawn(run_consume).unwrap()
    }

    pub(crate) fn listen(
        self: Arc<Self>,
        bank_forks: Option<Arc<RwLock<BankForks>>>,
        requests_receiver: Receiver<Vec<(/*from:*/ SocketAddr, Protocol)>>,
        response_sender: PacketSender,
        should_check_duplicate_instance: bool,
        exit: &Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let exit = exit.clone();
        let recycler = PacketsRecycler::default();
        Builder::new()
            .name("solana-listen".to_string())
            .spawn(move || {
                let thread_pool = ThreadPoolBuilder::new()
                    .num_threads(std::cmp::min(get_thread_count(), 8))
                    .thread_name(|i| format!("sol-gossip-work-{}", i))
                    .build()
                    .unwrap();
                let mut last_print = Instant::now();
                while !exit.load(Ordering::Relaxed) {
                    if let Err(err) = self.run_listen(
                        &recycler,
                        bank_forks.as_deref(),
                        &requests_receiver,
                        &response_sender,
                        &thread_pool,
                        &mut last_print,
                        should_check_duplicate_instance,
                    ) {
                        match err {
                            GossipError::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
                            GossipError::RecvTimeoutError(RecvTimeoutError::Timeout) => {
                                let table_size = self.gossip.crds.read().unwrap().len();
                                debug!(
                                    "{}: run_listen timeout, table size: {}",
                                    self.id(),
                                    table_size,
                                );
                            }
                            GossipError::DuplicateNodeInstance => {
                                error!(
                                    "duplicate running instances of the same validator node: {}",
                                    self.id()
                                );
                                exit.store(true, Ordering::Relaxed);
                                // TODO: Pass through Exit here so
                                // that this will exit cleanly.
                                std::process::exit(1);
                            }
                            _ => error!("gossip run_listen failed: {}", err),
                        }
                    }
                }
            })
            .unwrap()
    }

    pub fn gossip_contact_info(id: Pubkey, gossip: SocketAddr, shred_version: u16) -> ContactInfo {
        ContactInfo {
            id,
            gossip,
            wallclock: timestamp(),
            shred_version,
            ..ContactInfo::default()
        }
    }

    /// An alternative to Spy Node that has a valid gossip address and fully participate in Gossip.
    pub fn gossip_node(
        id: Pubkey,
        gossip_addr: &SocketAddr,
        shred_version: u16,
    ) -> (ContactInfo, UdpSocket, Option<TcpListener>) {
        let bind_ip_addr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
        let (port, (gossip_socket, ip_echo)) =
            Node::get_gossip_port(gossip_addr, VALIDATOR_PORT_RANGE, bind_ip_addr);
        let contact_info =
            Self::gossip_contact_info(id, SocketAddr::new(gossip_addr.ip(), port), shred_version);

        (contact_info, gossip_socket, Some(ip_echo))
    }

    /// A Node with dummy ports to spy on gossip via pull requests
    pub fn spy_node(
        id: Pubkey,
        shred_version: u16,
    ) -> (ContactInfo, UdpSocket, Option<TcpListener>) {
        let bind_ip_addr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
        let (_, gossip_socket) = bind_in_range(bind_ip_addr, VALIDATOR_PORT_RANGE).unwrap();
        let contact_info = Self::gossip_contact_info(id, socketaddr_any!(), shred_version);

        (contact_info, gossip_socket, None)
    }
}

// Returns root bank's epoch duration. Falls back on
//     DEFAULT_SLOTS_PER_EPOCH * DEFAULT_MS_PER_SLOT
// if there are no working banks.
fn get_epoch_duration(bank_forks: Option<&RwLock<BankForks>>) -> Duration {
    let num_slots = match bank_forks {
        None => {
            inc_new_counter_info!("cluster_info-purge-no_working_bank", 1);
            DEFAULT_SLOTS_PER_EPOCH
        }
        Some(bank_forks) => {
            let bank = bank_forks.read().unwrap().root_bank();
            bank.get_slots_in_epoch(bank.epoch())
        }
    };
    Duration::from_millis(num_slots * DEFAULT_MS_PER_SLOT)
}

/// Turbine logic
/// 1 - For the current node find out if it is in layer 1
/// 1.1 - If yes, then broadcast to all layer 1 nodes
///      1 - using the layer 1 index, broadcast to all layer 2 nodes assuming you know neighborhood size
/// 1.2 - If no, then figure out what layer the node is in and who the neighbors are and only broadcast to them
///      1 - also check if there are nodes in the next layer and repeat the layer 1 to layer 2 logic

/// Returns Neighbor Nodes and Children Nodes `(neighbors, children)` for a given node based on its stake
pub fn compute_retransmit_peers(
    fanout: usize,
    node: usize,
    index: &[usize],
) -> (Vec<usize> /*neighbors*/, Vec<usize> /*children*/) {
    // 1st layer: fanout    nodes starting at 0
    // 2nd layer: fanout**2 nodes starting at fanout
    // 3rd layer: fanout**3 nodes starting at fanout + fanout**2
    // ...
    // Each layer is divided into neighborhoods of fanout nodes each.
    let offset = node % fanout; // Node's index within its neighborhood.
    let anchor = node - offset; // First node in the neighborhood.
    let neighbors = (anchor..)
        .take(fanout)
        .map(|i| index.get(i).copied())
        .while_some()
        .collect();
    let children = ((anchor + 1) * fanout + offset..)
        .step_by(fanout)
        .take(fanout)
        .map(|i| index.get(i).copied())
        .while_some()
        .collect();
    (neighbors, children)
}

#[derive(Debug)]
pub struct Sockets {
    pub gossip: UdpSocket,
    pub ip_echo: Option<TcpListener>,
    pub tvu: Vec<UdpSocket>,
    pub tvu_forwards: Vec<UdpSocket>,
    pub tpu: Vec<UdpSocket>,
    pub tpu_forwards: Vec<UdpSocket>,
    pub broadcast: Vec<UdpSocket>,
    pub repair: UdpSocket,
    pub retransmit_sockets: Vec<UdpSocket>,
    pub serve_repair: UdpSocket,
}

#[derive(Debug)]
pub struct Node {
    pub info: ContactInfo,
    pub sockets: Sockets,
}

impl Node {
    pub fn new_localhost() -> Self {
        let pubkey = solana_sdk::pubkey::new_rand();
        Self::new_localhost_with_pubkey(&pubkey)
    }
    pub fn new_localhost_with_pubkey(pubkey: &Pubkey) -> Self {
        let bind_ip_addr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
        let tpu = UdpSocket::bind("127.0.0.1:0").unwrap();
        let (gossip_port, (gossip, ip_echo)) =
            bind_common_in_range(bind_ip_addr, (1024, 65535)).unwrap();
        let gossip_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), gossip_port);
        let tvu = UdpSocket::bind("127.0.0.1:0").unwrap();
        let tvu_forwards = UdpSocket::bind("127.0.0.1:0").unwrap();
        let tpu_forwards = UdpSocket::bind("127.0.0.1:0").unwrap();
        let repair = UdpSocket::bind("127.0.0.1:0").unwrap();
        let rpc_port = find_available_port_in_range(bind_ip_addr, (1024, 65535)).unwrap();
        let rpc_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), rpc_port);
        let rpc_pubsub_port = find_available_port_in_range(bind_ip_addr, (1024, 65535)).unwrap();
        let rpc_pubsub_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), rpc_pubsub_port);

        let broadcast = vec![UdpSocket::bind("0.0.0.0:0").unwrap()];
        let retransmit_socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let serve_repair = UdpSocket::bind("127.0.0.1:0").unwrap();
        let unused = UdpSocket::bind("0.0.0.0:0").unwrap();
        let info = ContactInfo {
            id: *pubkey,
            gossip: gossip_addr,
            tvu: tvu.local_addr().unwrap(),
            tvu_forwards: tvu_forwards.local_addr().unwrap(),
            repair: repair.local_addr().unwrap(),
            tpu: tpu.local_addr().unwrap(),
            tpu_forwards: tpu_forwards.local_addr().unwrap(),
            unused: unused.local_addr().unwrap(),
            rpc: rpc_addr,
            rpc_pubsub: rpc_pubsub_addr,
            serve_repair: serve_repair.local_addr().unwrap(),
            wallclock: timestamp(),
            shred_version: 0,
        };
        Node {
            info,
            sockets: Sockets {
                gossip,
                ip_echo: Some(ip_echo),
                tvu: vec![tvu],
                tvu_forwards: vec![tvu_forwards],
                tpu: vec![tpu],
                tpu_forwards: vec![tpu_forwards],
                broadcast,
                repair,
                retransmit_sockets: vec![retransmit_socket],
                serve_repair,
            },
        }
    }

    fn get_gossip_port(
        gossip_addr: &SocketAddr,
        port_range: PortRange,
        bind_ip_addr: IpAddr,
    ) -> (u16, (UdpSocket, TcpListener)) {
        if gossip_addr.port() != 0 {
            (
                gossip_addr.port(),
                bind_common(bind_ip_addr, gossip_addr.port(), false).unwrap_or_else(|e| {
                    panic!("gossip_addr bind_to port {}: {}", gossip_addr.port(), e)
                }),
            )
        } else {
            bind_common_in_range(bind_ip_addr, port_range).expect("Failed to bind")
        }
    }
    fn bind(bind_ip_addr: IpAddr, port_range: PortRange) -> (u16, UdpSocket) {
        bind_in_range(bind_ip_addr, port_range).expect("Failed to bind")
    }

    pub fn new_single_bind(
        pubkey: &Pubkey,
        gossip_addr: &SocketAddr,
        port_range: PortRange,
        bind_ip_addr: IpAddr,
    ) -> Self {
        let (gossip_port, (gossip, ip_echo)) =
            Self::get_gossip_port(gossip_addr, port_range, bind_ip_addr);
        let (tvu_port, tvu) = Self::bind(bind_ip_addr, port_range);
        let (tvu_forwards_port, tvu_forwards) = Self::bind(bind_ip_addr, port_range);
        let (tpu_port, tpu) = Self::bind(bind_ip_addr, port_range);
        let (tpu_forwards_port, tpu_forwards) = Self::bind(bind_ip_addr, port_range);
        let (_, retransmit_socket) = Self::bind(bind_ip_addr, port_range);
        let (repair_port, repair) = Self::bind(bind_ip_addr, port_range);
        let (serve_repair_port, serve_repair) = Self::bind(bind_ip_addr, port_range);
        let (_, broadcast) = Self::bind(bind_ip_addr, port_range);

        let rpc_port = find_available_port_in_range(bind_ip_addr, port_range).unwrap();
        let rpc_pubsub_port = find_available_port_in_range(bind_ip_addr, port_range).unwrap();

        let info = ContactInfo {
            id: *pubkey,
            gossip: SocketAddr::new(gossip_addr.ip(), gossip_port),
            tvu: SocketAddr::new(gossip_addr.ip(), tvu_port),
            tvu_forwards: SocketAddr::new(gossip_addr.ip(), tvu_forwards_port),
            repair: SocketAddr::new(gossip_addr.ip(), repair_port),
            tpu: SocketAddr::new(gossip_addr.ip(), tpu_port),
            tpu_forwards: SocketAddr::new(gossip_addr.ip(), tpu_forwards_port),
            unused: socketaddr_any!(),
            rpc: SocketAddr::new(gossip_addr.ip(), rpc_port),
            rpc_pubsub: SocketAddr::new(gossip_addr.ip(), rpc_pubsub_port),
            serve_repair: SocketAddr::new(gossip_addr.ip(), serve_repair_port),
            wallclock: timestamp(),
            shred_version: 0,
        };
        trace!("new ContactInfo: {:?}", info);

        Node {
            info,
            sockets: Sockets {
                gossip,
                ip_echo: Some(ip_echo),
                tvu: vec![tvu],
                tvu_forwards: vec![tvu_forwards],
                tpu: vec![tpu],
                tpu_forwards: vec![tpu_forwards],
                broadcast: vec![broadcast],
                repair,
                retransmit_sockets: vec![retransmit_socket],
                serve_repair,
            },
        }
    }

    pub fn new_with_external_ip(
        pubkey: &Pubkey,
        gossip_addr: &SocketAddr,
        port_range: PortRange,
        bind_ip_addr: IpAddr,
    ) -> Node {
        let (gossip_port, (gossip, ip_echo)) =
            Self::get_gossip_port(gossip_addr, port_range, bind_ip_addr);

        let (tvu_port, tvu_sockets) =
            multi_bind_in_range(bind_ip_addr, port_range, 8).expect("tvu multi_bind");

        let (tvu_forwards_port, tvu_forwards_sockets) =
            multi_bind_in_range(bind_ip_addr, port_range, 8).expect("tvu_forwards multi_bind");

        let (tpu_port, tpu_sockets) =
            multi_bind_in_range(bind_ip_addr, port_range, 32).expect("tpu multi_bind");

        let (tpu_forwards_port, tpu_forwards_sockets) =
            multi_bind_in_range(bind_ip_addr, port_range, 8).expect("tpu_forwards multi_bind");

        let (_, retransmit_sockets) =
            multi_bind_in_range(bind_ip_addr, port_range, 8).expect("retransmit multi_bind");

        let (repair_port, repair) = Self::bind(bind_ip_addr, port_range);
        let (serve_repair_port, serve_repair) = Self::bind(bind_ip_addr, port_range);

        let (_, broadcast) =
            multi_bind_in_range(bind_ip_addr, port_range, 4).expect("broadcast multi_bind");

        let info = ContactInfo {
            id: *pubkey,
            gossip: SocketAddr::new(gossip_addr.ip(), gossip_port),
            tvu: SocketAddr::new(gossip_addr.ip(), tvu_port),
            tvu_forwards: SocketAddr::new(gossip_addr.ip(), tvu_forwards_port),
            repair: SocketAddr::new(gossip_addr.ip(), repair_port),
            tpu: SocketAddr::new(gossip_addr.ip(), tpu_port),
            tpu_forwards: SocketAddr::new(gossip_addr.ip(), tpu_forwards_port),
            unused: socketaddr_any!(),
            rpc: socketaddr_any!(),
            rpc_pubsub: socketaddr_any!(),
            serve_repair: SocketAddr::new(gossip_addr.ip(), serve_repair_port),
            wallclock: 0,
            shred_version: 0,
        };
        trace!("new ContactInfo: {:?}", info);

        Node {
            info,
            sockets: Sockets {
                gossip,
                tvu: tvu_sockets,
                tvu_forwards: tvu_forwards_sockets,
                tpu: tpu_sockets,
                tpu_forwards: tpu_forwards_sockets,
                broadcast,
                repair,
                retransmit_sockets,
                serve_repair,
                ip_echo: Some(ip_echo),
            },
        }
    }
}

pub fn push_messages_to_peer(
    messages: Vec<CrdsValue>,
    self_id: Pubkey,
    peer_gossip: SocketAddr,
    socket_addr_space: &SocketAddrSpace,
) -> Result<(), GossipError> {
    let reqs: Vec<_> = ClusterInfo::split_gossip_messages(PUSH_MESSAGE_MAX_PAYLOAD_SIZE, messages)
        .map(move |payload| (peer_gossip, Protocol::PushMessage(self_id, payload)))
        .collect();
    let packets = to_packets_with_destination(PacketsRecycler::default(), &reqs);
    let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
    packet::send_to(&packets, &sock, socket_addr_space)?;
    Ok(())
}

// Filters out values from nodes with different shred-version.
fn filter_on_shred_version(
    mut msg: Protocol,
    self_shred_version: u16,
    crds: &Crds,
    stats: &GossipStats,
) -> Option<Protocol> {
    let filter_values = |from: &Pubkey, values: &mut Vec<CrdsValue>, skipped_counter: &Counter| {
        let num_values = values.len();
        if crds.get_shred_version(from) == Some(self_shred_version) {
            // Retain values with the same shred-vesion, or those which are
            // contact-info so that shred-versions can be updated.
            values.retain(|value| match &value.data {
                CrdsData::ContactInfo(_) => true,
                _ => crds.get_shred_version(&value.pubkey()) == Some(self_shred_version),
            })
        } else {
            // Only allow node to update its own contact info in case their
            // shred-version changes.
            values.retain(|value| match &value.data {
                CrdsData::ContactInfo(node) => node.id == *from,
                _ => false,
            })
        }
        let num_skipped = num_values - values.len();
        if num_skipped != 0 {
            skipped_counter.add_relaxed(num_skipped as u64);
        }
    };
    match &mut msg {
        Protocol::PullRequest(_, caller) => match &caller.data {
            // Allow spy nodes with shred-verion == 0 to pull from other nodes.
            CrdsData::ContactInfo(node)
                if node.shred_version == 0 || node.shred_version == self_shred_version =>
            {
                Some(msg)
            }
            _ => {
                stats.skip_pull_shred_version.add_relaxed(1);
                None
            }
        },
        Protocol::PullResponse(from, values) => {
            filter_values(from, values, &stats.skip_pull_response_shred_version);
            if values.is_empty() {
                None
            } else {
                Some(msg)
            }
        }
        Protocol::PushMessage(from, values) => {
            filter_values(from, values, &stats.skip_push_message_shred_version);
            if values.is_empty() {
                None
            } else {
                Some(msg)
            }
        }
        Protocol::PruneMessage(_, _) | Protocol::PingMessage(_) | Protocol::PongMessage(_) => {
            Some(msg)
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            crds_gossip_pull::tests::MIN_NUM_BLOOM_FILTERS,
            crds_value::{CrdsValue, CrdsValueLabel, Vote as CrdsVote},
            duplicate_shred::{self, tests::new_rand_shred, MAX_DUPLICATE_SHREDS},
        },
        itertools::izip,
        rand::{seq::SliceRandom, SeedableRng},
        rand_chacha::ChaChaRng,
        solana_ledger::shred::Shredder,
        solana_sdk::signature::{Keypair, Signer},
        solana_vote_program::{vote_instruction, vote_state::Vote},
        std::{
            iter::repeat_with,
            net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV4},
            sync::Arc,
        },
    };

    #[test]
    fn test_gossip_node() {
        //check that a gossip nodes always show up as spies
        let (node, _, _) = ClusterInfo::spy_node(solana_sdk::pubkey::new_rand(), 0);
        assert!(ClusterInfo::is_spy_node(
            &node,
            &SocketAddrSpace::Unspecified
        ));
        let (node, _, _) = ClusterInfo::gossip_node(
            solana_sdk::pubkey::new_rand(),
            &"1.1.1.1:1111".parse().unwrap(),
            0,
        );
        assert!(ClusterInfo::is_spy_node(
            &node,
            &SocketAddrSpace::Unspecified
        ));
    }

    #[test]
    fn test_handle_pull() {
        solana_logger::setup();
        let node = Node::new_localhost();
        let cluster_info = Arc::new(ClusterInfo::new(
            node.info,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));

        let entrypoint_pubkey = solana_sdk::pubkey::new_rand();
        let data = test_crds_values(entrypoint_pubkey);
        let timeouts = HashMap::new();
        assert_eq!(
            (0, 0, 1),
            ClusterInfo::handle_pull_response(
                &cluster_info,
                &entrypoint_pubkey,
                data.clone(),
                &timeouts
            )
        );

        let entrypoint_pubkey2 = solana_sdk::pubkey::new_rand();
        assert_eq!(
            (1, 0, 0),
            ClusterInfo::handle_pull_response(&cluster_info, &entrypoint_pubkey2, data, &timeouts)
        );
    }

    fn new_rand_socket_addr<R: Rng>(rng: &mut R) -> SocketAddr {
        let addr = if rng.gen_bool(0.5) {
            IpAddr::V4(Ipv4Addr::new(rng.gen(), rng.gen(), rng.gen(), rng.gen()))
        } else {
            IpAddr::V6(Ipv6Addr::new(
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
            ))
        };
        SocketAddr::new(addr, /*port=*/ rng.gen())
    }

    fn new_rand_remote_node<R>(rng: &mut R) -> (Keypair, SocketAddr)
    where
        R: Rng,
    {
        let keypair = Keypair::new();
        let socket = new_rand_socket_addr(rng);
        (keypair, socket)
    }

    #[test]
    fn test_handle_pong_messages() {
        let now = Instant::now();
        let mut rng = rand::thread_rng();
        let this_node = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&this_node.pubkey(), timestamp()),
            this_node.clone(),
            SocketAddrSpace::Unspecified,
        );
        let remote_nodes: Vec<(Keypair, SocketAddr)> =
            repeat_with(|| new_rand_remote_node(&mut rng))
                .take(128)
                .collect();
        let pings: Vec<_> = {
            let mut ping_cache = cluster_info.ping_cache.lock().unwrap();
            let mut pingf = || Ping::new_rand(&mut rng, &this_node).ok();
            remote_nodes
                .iter()
                .map(|(keypair, socket)| {
                    let node = (keypair.pubkey(), *socket);
                    let (check, ping) = ping_cache.check(now, node, &mut pingf);
                    // Assert that initially remote nodes will not pass the
                    // ping/pong check.
                    assert!(!check);
                    ping.unwrap()
                })
                .collect()
        };
        let pongs: Vec<(SocketAddr, Pong)> = pings
            .iter()
            .zip(&remote_nodes)
            .map(|(ping, (keypair, socket))| (*socket, Pong::new(ping, keypair).unwrap()))
            .collect();
        let now = now + Duration::from_millis(1);
        cluster_info.handle_batch_pong_messages(pongs, now);
        // Assert that remote nodes now pass the ping/pong check.
        {
            let mut ping_cache = cluster_info.ping_cache.lock().unwrap();
            for (keypair, socket) in &remote_nodes {
                let node = (keypair.pubkey(), *socket);
                let (check, _) = ping_cache.check(now, node, || -> Option<Ping> { None });
                assert!(check);
            }
        }
        // Assert that a new random remote node still will not pass the check.
        {
            let mut ping_cache = cluster_info.ping_cache.lock().unwrap();
            let (keypair, socket) = new_rand_remote_node(&mut rng);
            let node = (keypair.pubkey(), socket);
            let (check, _) = ping_cache.check(now, node, || -> Option<Ping> { None });
            assert!(!check);
        }
    }

    #[test]
    #[allow(clippy::needless_collect)]
    fn test_handle_ping_messages() {
        let mut rng = rand::thread_rng();
        let this_node = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&this_node.pubkey(), timestamp()),
            this_node.clone(),
            SocketAddrSpace::Unspecified,
        );
        let remote_nodes: Vec<(Keypair, SocketAddr)> =
            repeat_with(|| new_rand_remote_node(&mut rng))
                .take(128)
                .collect();
        let pings: Vec<_> = remote_nodes
            .iter()
            .map(|(keypair, _)| Ping::new_rand(&mut rng, keypair).unwrap())
            .collect();
        let pongs: Vec<_> = pings
            .iter()
            .map(|ping| Pong::new(ping, &this_node).unwrap())
            .collect();
        let recycler = PacketsRecycler::default();
        let packets = cluster_info
            .handle_ping_messages(
                remote_nodes
                    .iter()
                    .map(|(_, socket)| *socket)
                    .zip(pings.into_iter()),
                &recycler,
            )
            .unwrap()
            .packets;
        assert_eq!(remote_nodes.len(), packets.len());
        for (packet, (_, socket), pong) in izip!(
            packets.into_iter(),
            remote_nodes.into_iter(),
            pongs.into_iter()
        ) {
            assert_eq!(packet.meta.addr(), socket);
            let bytes = serialize(&pong).unwrap();
            match limited_deserialize(&packet.data[..packet.meta.size]).unwrap() {
                Protocol::PongMessage(pong) => assert_eq!(serialize(&pong).unwrap(), bytes),
                _ => panic!("invalid packet!"),
            }
        }
    }

    fn test_crds_values(pubkey: Pubkey) -> Vec<CrdsValue> {
        let entrypoint = ContactInfo::new_localhost(&pubkey, timestamp());
        let entrypoint_crdsvalue = CrdsValue::new_unsigned(CrdsData::ContactInfo(entrypoint));
        vec![entrypoint_crdsvalue]
    }

    #[test]
    fn test_max_snapshot_hashes_with_push_messages() {
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let snapshot_hash = SnapshotHash::new_rand(&mut rng, None);
            let crds_value =
                CrdsValue::new_signed(CrdsData::SnapshotHashes(snapshot_hash), &Keypair::new());
            let message = Protocol::PushMessage(Pubkey::new_unique(), vec![crds_value]);
            let socket = new_rand_socket_addr(&mut rng);
            assert!(Packet::from_data(Some(&socket), message).is_ok());
        }
    }

    #[test]
    fn test_max_snapshot_hashes_with_pull_responses() {
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let snapshot_hash = SnapshotHash::new_rand(&mut rng, None);
            let crds_value =
                CrdsValue::new_signed(CrdsData::AccountsHashes(snapshot_hash), &Keypair::new());
            let response = Protocol::PullResponse(Pubkey::new_unique(), vec![crds_value]);
            let socket = new_rand_socket_addr(&mut rng);
            assert!(Packet::from_data(Some(&socket), response).is_ok());
        }
    }

    #[test]
    fn test_max_prune_data_pubkeys() {
        let mut rng = rand::thread_rng();
        for _ in 0..64 {
            let self_keypair = Keypair::new();
            let prune_data =
                PruneData::new_rand(&mut rng, &self_keypair, Some(MAX_PRUNE_DATA_NODES));
            let prune_message = Protocol::PruneMessage(self_keypair.pubkey(), prune_data);
            let socket = new_rand_socket_addr(&mut rng);
            assert!(Packet::from_data(Some(&socket), prune_message).is_ok());
        }
        // Assert that MAX_PRUNE_DATA_NODES is highest possible.
        let self_keypair = Keypair::new();
        let prune_data =
            PruneData::new_rand(&mut rng, &self_keypair, Some(MAX_PRUNE_DATA_NODES + 1));
        let prune_message = Protocol::PruneMessage(self_keypair.pubkey(), prune_data);
        let socket = new_rand_socket_addr(&mut rng);
        assert!(Packet::from_data(Some(&socket), prune_message).is_err());
    }

    #[test]
    fn test_push_message_max_payload_size() {
        let header = Protocol::PushMessage(Pubkey::default(), Vec::default());
        assert_eq!(
            PUSH_MESSAGE_MAX_PAYLOAD_SIZE,
            PACKET_DATA_SIZE - serialized_size(&header).unwrap() as usize
        );
    }

    #[test]
    fn test_duplicate_shred_max_payload_size() {
        let mut rng = rand::thread_rng();
        let leader = Arc::new(Keypair::new());
        let keypair = Keypair::new();
        let (slot, parent_slot, reference_tick, version) = (53084024, 53084023, 0, 0);
        let shredder = Shredder::new(slot, parent_slot, reference_tick, version).unwrap();
        let next_shred_index = rng.gen();
        let shred = new_rand_shred(&mut rng, next_shred_index, &shredder, &leader);
        let other_payload = new_rand_shred(&mut rng, next_shred_index, &shredder, &leader).payload;
        let leader_schedule = |s| {
            if s == slot {
                Some(leader.pubkey())
            } else {
                None
            }
        };
        let chunks: Vec<_> = duplicate_shred::from_shred(
            shred,
            keypair.pubkey(),
            other_payload,
            Some(leader_schedule),
            timestamp(),
            DUPLICATE_SHRED_MAX_PAYLOAD_SIZE,
        )
        .unwrap()
        .collect();
        assert!(chunks.len() > 1);
        for chunk in chunks {
            let data = CrdsData::DuplicateShred(MAX_DUPLICATE_SHREDS - 1, chunk);
            let value = CrdsValue::new_signed(data, &keypair);
            let pull_response = Protocol::PullResponse(keypair.pubkey(), vec![value.clone()]);
            assert!(serialized_size(&pull_response).unwrap() < PACKET_DATA_SIZE as u64);
            let push_message = Protocol::PushMessage(keypair.pubkey(), vec![value.clone()]);
            assert!(serialized_size(&push_message).unwrap() < PACKET_DATA_SIZE as u64);
        }
    }

    #[test]
    fn test_pull_response_min_serialized_size() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let crds_values = vec![CrdsValue::new_rand(&mut rng, None)];
            let pull_response = Protocol::PullResponse(Pubkey::new_unique(), crds_values);
            let size = serialized_size(&pull_response).unwrap();
            assert!(
                PULL_RESPONSE_MIN_SERIALIZED_SIZE as u64 <= size,
                "pull-response serialized size: {}",
                size
            );
        }
    }

    #[test]
    fn test_cluster_spy_gossip() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        //check that gossip doesn't try to push to invalid addresses
        let node = Node::new_localhost();
        let (spy, _, _) = ClusterInfo::spy_node(solana_sdk::pubkey::new_rand(), 0);
        let cluster_info = Arc::new(ClusterInfo::new(
            node.info,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        cluster_info.insert_info(spy);
        cluster_info.gossip.refresh_push_active_set(
            &cluster_info.id(),
            cluster_info.my_shred_version(),
            &HashMap::new(), // stakes
            None,            // gossip validators
            &SocketAddrSpace::Unspecified,
        );
        let reqs = cluster_info.generate_new_gossip_requests(
            &thread_pool,
            None,            // gossip_validators
            &HashMap::new(), // stakes
            true,            // generate_pull_requests
            false,           // require_stake_for_gossip
        );
        //assert none of the addrs are invalid.
        reqs.iter().all(|(addr, _)| {
            let res = ContactInfo::is_valid_address(addr, &SocketAddrSpace::Unspecified);
            assert!(res);
            res
        });
    }

    #[test]
    fn test_cluster_info_new() {
        let d = ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), timestamp());
        let cluster_info = ClusterInfo::new(
            d.clone(),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        );
        assert_eq!(d.id, cluster_info.id());
    }

    #[test]
    fn insert_info_test() {
        let d = ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), timestamp());
        let cluster_info =
            ClusterInfo::new(d, Arc::new(Keypair::new()), SocketAddrSpace::Unspecified);
        let d = ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), timestamp());
        let label = CrdsValueLabel::ContactInfo(d.id);
        cluster_info.insert_info(d);
        let gossip_crds = cluster_info.gossip.crds.read().unwrap();
        assert!(gossip_crds.get::<&CrdsValue>(&label).is_some());
    }

    fn assert_in_range(x: u16, range: (u16, u16)) {
        assert!(x >= range.0);
        assert!(x < range.1);
    }

    fn check_sockets(sockets: &[UdpSocket], ip: IpAddr, range: (u16, u16)) {
        assert!(sockets.len() > 1);
        let port = sockets[0].local_addr().unwrap().port();
        for socket in sockets.iter() {
            check_socket(socket, ip, range);
            assert_eq!(socket.local_addr().unwrap().port(), port);
        }
    }

    fn check_socket(socket: &UdpSocket, ip: IpAddr, range: (u16, u16)) {
        let local_addr = socket.local_addr().unwrap();
        assert_eq!(local_addr.ip(), ip);
        assert_in_range(local_addr.port(), range);
    }

    fn check_node_sockets(node: &Node, ip: IpAddr, range: (u16, u16)) {
        check_socket(&node.sockets.gossip, ip, range);
        check_socket(&node.sockets.repair, ip, range);

        check_sockets(&node.sockets.tvu, ip, range);
        check_sockets(&node.sockets.tpu, ip, range);
    }

    #[test]
    fn new_with_external_ip_test_random() {
        let ip = Ipv4Addr::from(0);
        let node = Node::new_with_external_ip(
            &solana_sdk::pubkey::new_rand(),
            &socketaddr!(ip, 0),
            VALIDATOR_PORT_RANGE,
            IpAddr::V4(ip),
        );

        check_node_sockets(&node, IpAddr::V4(ip), VALIDATOR_PORT_RANGE);
    }

    #[test]
    fn new_with_external_ip_test_gossip() {
        // Can't use VALIDATOR_PORT_RANGE because if this test runs in parallel with others, the
        // port returned by `bind_in_range()` might be snatched up before `Node::new_with_external_ip()` runs
        let port_range = (VALIDATOR_PORT_RANGE.1 + 10, VALIDATOR_PORT_RANGE.1 + 20);

        let ip = IpAddr::V4(Ipv4Addr::from(0));
        let port = bind_in_range(ip, port_range).expect("Failed to bind").0;
        let node = Node::new_with_external_ip(
            &solana_sdk::pubkey::new_rand(),
            &socketaddr!(0, port),
            port_range,
            ip,
        );

        check_node_sockets(&node, ip, port_range);

        assert_eq!(node.sockets.gossip.local_addr().unwrap().port(), port);
    }

    //test that all cluster_info objects only generate signed messages
    //when constructed with keypairs
    #[test]
    fn test_gossip_signature_verification() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        //create new cluster info, leader, and peer
        let keypair = Keypair::new();
        let peer_keypair = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let peer = ContactInfo::new_localhost(&peer_keypair.pubkey(), 0);
        let cluster_info = ClusterInfo::new(
            contact_info,
            Arc::new(keypair),
            SocketAddrSpace::Unspecified,
        );
        cluster_info
            .ping_cache
            .lock()
            .unwrap()
            .mock_pong(peer.id, peer.gossip, Instant::now());
        cluster_info.insert_info(peer);
        cluster_info.gossip.refresh_push_active_set(
            &cluster_info.id(),
            cluster_info.my_shred_version(),
            &HashMap::new(), // stakes
            None,            // gossip validators
            &SocketAddrSpace::Unspecified,
        );
        //check that all types of gossip messages are signed correctly
        let push_messages = cluster_info
            .gossip
            .new_push_messages(cluster_info.drain_push_queue(), timestamp());
        // there should be some pushes ready
        assert!(!push_messages.is_empty());
        push_messages
            .values()
            .for_each(|v| v.par_iter().for_each(|v| assert!(v.verify())));

        let mut pings = Vec::new();
        cluster_info
            .gossip
            .new_pull_request(
                &thread_pool,
                cluster_info.keypair().deref(),
                cluster_info.my_shred_version(),
                timestamp(),
                None,
                &HashMap::new(),
                MAX_BLOOM_SIZE,
                &cluster_info.ping_cache,
                &mut pings,
                &cluster_info.socket_addr_space,
            )
            .ok()
            .unwrap();
    }

    #[test]
    fn test_refresh_vote() {
        let keys = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keys.pubkey(), 0);
        let cluster_info = ClusterInfo::new(
            contact_info,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        );

        // Construct and push a vote for some other slot
        let unrefresh_slot = 5;
        let unrefresh_tower = vec![1, 3, unrefresh_slot];
        let unrefresh_vote = Vote::new(unrefresh_tower.clone(), Hash::new_unique());
        let unrefresh_ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            unrefresh_vote,
        );
        let unrefresh_tx = Transaction::new_with_payer(
            &[unrefresh_ix], // instructions
            None,            // payer
        );
        cluster_info.push_vote(&unrefresh_tower, unrefresh_tx.clone());
        let mut cursor = Cursor::default();
        let (_, votes) = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes, vec![unrefresh_tx.clone()]);

        // Now construct vote for the slot to be refreshed later
        let refresh_slot = 7;
        let refresh_tower = vec![1, 3, unrefresh_slot, refresh_slot];
        let refresh_vote = Vote::new(refresh_tower.clone(), Hash::new_unique());
        let refresh_ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            refresh_vote.clone(),
        );
        let refresh_tx = Transaction::new_with_payer(
            &[refresh_ix], // instructions
            None,          // payer
        );

        // Trying to refresh vote when it doesn't yet exist in gossip
        // shouldn't add the vote
        cluster_info.refresh_vote(refresh_tx.clone(), refresh_slot);
        let (_, votes) = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes, vec![]);
        let (_, votes) = cluster_info.get_votes(&mut Cursor::default());
        assert_eq!(votes.len(), 1);
        assert!(votes.contains(&unrefresh_tx));

        // Push the new vote for `refresh_slot`
        cluster_info.push_vote(&refresh_tower, refresh_tx.clone());

        // Should be two votes in gossip
        let (_, votes) = cluster_info.get_votes(&mut Cursor::default());
        assert_eq!(votes.len(), 2);
        assert!(votes.contains(&unrefresh_tx));
        assert!(votes.contains(&refresh_tx));

        // Refresh a few times, we should only have the latest update
        let mut latest_refresh_tx = refresh_tx;
        for _ in 0..10 {
            let latest_refreshed_recent_blockhash = Hash::new_unique();
            let new_signer = Keypair::new();
            let refresh_ix = vote_instruction::vote(
                &new_signer.pubkey(), // vote_pubkey
                &new_signer.pubkey(), // authorized_voter_pubkey
                refresh_vote.clone(),
            );
            latest_refresh_tx = Transaction::new_signed_with_payer(
                &[refresh_ix],
                None,
                &[&new_signer],
                latest_refreshed_recent_blockhash,
            );
            cluster_info.refresh_vote(latest_refresh_tx.clone(), refresh_slot);
        }
        // The diff since `max_ts` should only be the latest refreshed vote
        let (_, votes) = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes.len(), 1);
        assert_eq!(votes[0], latest_refresh_tx);

        // Should still be two votes in gossip
        let (_, votes) = cluster_info.get_votes(&mut Cursor::default());
        assert_eq!(votes.len(), 2);
        assert!(votes.contains(&unrefresh_tx));
        assert!(votes.contains(&latest_refresh_tx));
    }

    #[test]
    fn test_push_vote() {
        let mut rng = rand::thread_rng();
        let keys = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keys.pubkey(), 0);
        let cluster_info = ClusterInfo::new(
            contact_info,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        );

        // make sure empty crds is handled correctly
        let mut cursor = Cursor::default();
        let (_, votes) = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes, vec![]);

        // add a vote
        let vote = Vote::new(
            vec![1, 3, 7], // slots
            solana_sdk::hash::new_rand(&mut rng),
        );
        let ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            vote,
        );
        let tx = Transaction::new_with_payer(
            &[ix], // instructions
            None,  // payer
        );
        let tower = vec![7]; // Last slot in the vote.
        cluster_info.push_vote(&tower, tx.clone());

        let (labels, votes) = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes, vec![tx]);
        assert_eq!(labels.len(), 1);
        match labels[0] {
            CrdsValueLabel::Vote(_, pubkey) => {
                assert_eq!(pubkey, keys.pubkey());
            }

            _ => panic!("Bad match"),
        }
        // make sure timestamp filter works
        let (_, votes) = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes, vec![]);
    }

    fn new_vote_transaction<R: Rng>(rng: &mut R, slots: Vec<Slot>) -> Transaction {
        let vote = Vote::new(slots, solana_sdk::hash::new_rand(rng));
        let ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            vote,
        );
        Transaction::new_with_payer(
            &[ix], // instructions
            None,  // payer
        )
    }

    #[test]
    fn test_push_votes_with_tower() {
        let get_vote_slots = |cluster_info: &ClusterInfo| -> Vec<Slot> {
            let (labels, _) = cluster_info.get_votes(&mut Cursor::default());
            let gossip_crds = cluster_info.gossip.crds.read().unwrap();
            let mut vote_slots = HashSet::new();
            for label in labels {
                match &gossip_crds.get::<&CrdsData>(&label).unwrap() {
                    CrdsData::Vote(_, vote) => {
                        assert!(vote_slots.insert(vote.slot().unwrap()));
                    }
                    _ => panic!("this should not happen!"),
                }
            }
            vote_slots.into_iter().collect()
        };
        let mut rng = rand::thread_rng();
        let keys = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keys.pubkey(), 0);
        let cluster_info = ClusterInfo::new(
            contact_info,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        );
        let mut tower = Vec::new();
        for k in 0..MAX_LOCKOUT_HISTORY {
            let slot = k as Slot;
            tower.push(slot);
            let vote = new_vote_transaction(&mut rng, vec![slot]);
            cluster_info.push_vote(&tower, vote);
        }
        let vote_slots = get_vote_slots(&cluster_info);
        assert_eq!(vote_slots.len(), MAX_LOCKOUT_HISTORY);
        for vote_slot in vote_slots {
            assert!(vote_slot < MAX_LOCKOUT_HISTORY as u64);
        }
        // Push a new vote evicting one.
        let slot = MAX_LOCKOUT_HISTORY as Slot;
        tower.push(slot);
        tower.remove(23);
        let vote = new_vote_transaction(&mut rng, vec![slot]);
        cluster_info.push_vote(&tower, vote);
        let vote_slots = get_vote_slots(&cluster_info);
        assert_eq!(vote_slots.len(), MAX_LOCKOUT_HISTORY);
        for vote_slot in vote_slots {
            assert!(vote_slot <= slot);
            assert!(vote_slot != 23);
        }
        // Push a new vote evicting two.
        // Older one should be evicted from the crds table.
        let slot = slot + 1;
        tower.push(slot);
        tower.remove(17);
        tower.remove(5);
        let vote = new_vote_transaction(&mut rng, vec![slot]);
        cluster_info.push_vote(&tower, vote);
        let vote_slots = get_vote_slots(&cluster_info);
        assert_eq!(vote_slots.len(), MAX_LOCKOUT_HISTORY);
        for vote_slot in vote_slots {
            assert!(vote_slot <= slot);
            assert!(vote_slot != 23);
            assert!(vote_slot != 5);
        }
    }

    #[test]
    fn test_push_epoch_slots() {
        let keys = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keys.pubkey(), 0);
        let cluster_info = ClusterInfo::new(
            contact_info,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        );
        let slots = cluster_info.get_epoch_slots(&mut Cursor::default());
        assert!(slots.is_empty());
        cluster_info.push_epoch_slots(&[0]);

        let mut cursor = Cursor::default();
        let slots = cluster_info.get_epoch_slots(&mut cursor);
        assert_eq!(slots.len(), 1);

        let slots = cluster_info.get_epoch_slots(&mut cursor);
        assert!(slots.is_empty());

        // Test with different shred versions.
        let mut rng = rand::thread_rng();
        let node_pubkey = Pubkey::new_unique();
        let mut node = ContactInfo::new_rand(&mut rng, Some(node_pubkey));
        node.shred_version = 42;
        let epoch_slots = EpochSlots::new_rand(&mut rng, Some(node_pubkey));
        let entries = vec![
            CrdsValue::new_unsigned(CrdsData::ContactInfo(node)),
            CrdsValue::new_unsigned(CrdsData::EpochSlots(0, epoch_slots)),
        ];
        {
            let mut gossip_crds = cluster_info.gossip.crds.write().unwrap();
            for entry in entries {
                assert!(gossip_crds.insert(entry, /*now=*/ 0).is_ok());
            }
        }
        // Should exclude other node's epoch-slot because of different
        // shred-version.
        let slots = cluster_info.get_epoch_slots(&mut Cursor::default());
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].from, cluster_info.id());
        // Match shred versions.
        {
            let mut node = cluster_info.my_contact_info.write().unwrap();
            node.shred_version = 42;
        }
        cluster_info.push_self(
            &HashMap::default(), // stakes
            None,                // gossip validators
        );
        cluster_info.flush_push_queue();
        // Should now include both epoch slots.
        let slots = cluster_info.get_epoch_slots(&mut Cursor::default());
        assert_eq!(slots.len(), 2);
        assert_eq!(slots[0].from, cluster_info.id());
        assert_eq!(slots[1].from, node_pubkey);
    }

    #[test]
    fn test_append_entrypoint_to_pulls() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        );
        let entrypoint_pubkey = solana_sdk::pubkey::new_rand();
        let entrypoint = ContactInfo::new_localhost(&entrypoint_pubkey, timestamp());
        cluster_info.set_entrypoint(entrypoint.clone());
        let (pings, pulls) = cluster_info.new_pull_requests(&thread_pool, None, &HashMap::new());
        assert!(pings.is_empty());
        assert_eq!(pulls.len(), MIN_NUM_BLOOM_FILTERS);
        for (addr, msg) in pulls {
            assert_eq!(addr, entrypoint.gossip);
            match msg {
                Protocol::PullRequest(_, value) => {
                    assert!(value.verify());
                    assert_eq!(value.pubkey(), cluster_info.id())
                }
                _ => panic!("wrong protocol"),
            }
        }
        // now add this message back to the table and make sure after the next pull, the entrypoint is unset
        let entrypoint_crdsvalue =
            CrdsValue::new_unsigned(CrdsData::ContactInfo(entrypoint.clone()));
        let cluster_info = Arc::new(cluster_info);
        let timeouts = cluster_info.gossip.make_timeouts(
            cluster_info.id(),
            &HashMap::default(), // stakes,
            Duration::from_millis(cluster_info.gossip.pull.crds_timeout),
        );
        ClusterInfo::handle_pull_response(
            &cluster_info,
            &entrypoint_pubkey,
            vec![entrypoint_crdsvalue],
            &timeouts,
        );
        let (pings, pulls) = cluster_info.new_pull_requests(&thread_pool, None, &HashMap::new());
        assert_eq!(pings.len(), 1);
        assert_eq!(pulls.len(), MIN_NUM_BLOOM_FILTERS);
        assert_eq!(*cluster_info.entrypoints.read().unwrap(), vec![entrypoint]);
    }

    #[test]
    fn test_split_messages_small() {
        let value = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::default()));
        test_split_messages(value);
    }

    #[test]
    fn test_split_messages_large() {
        let value = CrdsValue::new_unsigned(CrdsData::LowestSlot(
            0,
            LowestSlot::new(Pubkey::default(), 0, 0),
        ));
        test_split_messages(value);
    }

    #[test]
    fn test_split_gossip_messages() {
        const NUM_CRDS_VALUES: usize = 2048;
        let mut rng = rand::thread_rng();
        let values: Vec<_> = repeat_with(|| CrdsValue::new_rand(&mut rng, None))
            .take(NUM_CRDS_VALUES)
            .collect();
        let splits: Vec<_> =
            ClusterInfo::split_gossip_messages(PUSH_MESSAGE_MAX_PAYLOAD_SIZE, values.clone())
                .collect();
        let self_pubkey = solana_sdk::pubkey::new_rand();
        assert!(splits.len() * 3 < NUM_CRDS_VALUES);
        // Assert that all messages are included in the splits.
        assert_eq!(NUM_CRDS_VALUES, splits.iter().map(Vec::len).sum::<usize>());
        splits
            .iter()
            .flat_map(|s| s.iter())
            .zip(values)
            .for_each(|(a, b)| assert_eq!(*a, b));
        let socket = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(rng.gen(), rng.gen(), rng.gen(), rng.gen()),
            rng.gen(),
        ));
        let header_size = PACKET_DATA_SIZE - PUSH_MESSAGE_MAX_PAYLOAD_SIZE;
        for values in splits {
            // Assert that sum of parts equals the whole.
            let size: u64 = header_size as u64
                + values
                    .iter()
                    .map(|v| serialized_size(v).unwrap())
                    .sum::<u64>();
            let message = Protocol::PushMessage(self_pubkey, values);
            assert_eq!(serialized_size(&message).unwrap(), size);
            // Assert that the message fits into a packet.
            assert!(Packet::from_data(Some(&socket), message).is_ok());
        }
    }

    #[test]
    #[allow(clippy::needless_collect)]
    fn test_split_messages_packet_size() {
        // Test that if a value is smaller than payload size but too large to be wrapped in a vec
        // that it is still dropped
        let mut value = CrdsValue::new_unsigned(CrdsData::SnapshotHashes(SnapshotHash {
            from: Pubkey::default(),
            hashes: vec![],
            wallclock: 0,
        }));

        let mut i = 0;
        while value.size() < PUSH_MESSAGE_MAX_PAYLOAD_SIZE as u64 {
            value.data = CrdsData::SnapshotHashes(SnapshotHash {
                from: Pubkey::default(),
                hashes: vec![(0, Hash::default()); i],
                wallclock: 0,
            });
            i += 1;
        }
        let split: Vec<_> =
            ClusterInfo::split_gossip_messages(PUSH_MESSAGE_MAX_PAYLOAD_SIZE, vec![value])
                .collect();
        assert_eq!(split.len(), 0);
    }

    fn test_split_messages(value: CrdsValue) {
        const NUM_VALUES: u64 = 30;
        let value_size = value.size();
        let num_values_per_payload = (PUSH_MESSAGE_MAX_PAYLOAD_SIZE as u64 / value_size).max(1);

        // Expected len is the ceiling of the division
        let expected_len = (NUM_VALUES + num_values_per_payload - 1) / num_values_per_payload;
        let msgs = vec![value; NUM_VALUES as usize];

        assert!(
            ClusterInfo::split_gossip_messages(PUSH_MESSAGE_MAX_PAYLOAD_SIZE, msgs).count() as u64
                <= expected_len
        );
    }

    #[test]
    fn test_crds_filter_size() {
        //sanity test to ensure filter size never exceeds MTU size
        check_pull_request_size(CrdsFilter::new_rand(1000, 10));
        check_pull_request_size(CrdsFilter::new_rand(1000, 1000));
        check_pull_request_size(CrdsFilter::new_rand(100_000, 1000));
        check_pull_request_size(CrdsFilter::new_rand(100_000, MAX_BLOOM_SIZE));
    }

    fn check_pull_request_size(filter: CrdsFilter) {
        let value = CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::default()));
        let protocol = Protocol::PullRequest(filter, value);
        assert!(serialized_size(&protocol).unwrap() <= PACKET_DATA_SIZE as u64);
    }

    #[test]
    fn test_tvu_peers_and_stakes() {
        let d = ContactInfo::new_localhost(&Pubkey::new(&[0; 32]), timestamp());
        let cluster_info = ClusterInfo::new(
            d.clone(),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        );
        let mut stakes = HashMap::new();

        // no stake
        let id = Pubkey::new(&[1u8; 32]);
        let contact_info = ContactInfo::new_localhost(&id, timestamp());
        cluster_info.insert_info(contact_info);

        // normal
        let id2 = Pubkey::new(&[2u8; 32]);
        let mut contact_info = ContactInfo::new_localhost(&id2, timestamp());
        cluster_info.insert_info(contact_info.clone());
        stakes.insert(id2, 10);

        // duplicate
        contact_info.wallclock = timestamp() + 1;
        cluster_info.insert_info(contact_info);

        // no tvu
        let id3 = Pubkey::new(&[3u8; 32]);
        let mut contact_info = ContactInfo::new_localhost(&id3, timestamp());
        contact_info.tvu = "0.0.0.0:0".parse().unwrap();
        cluster_info.insert_info(contact_info);
        stakes.insert(id3, 10);

        // normal but with different shred version
        let id4 = Pubkey::new(&[4u8; 32]);
        let mut contact_info = ContactInfo::new_localhost(&id4, timestamp());
        contact_info.shred_version = 1;
        assert_ne!(contact_info.shred_version, d.shred_version);
        cluster_info.insert_info(contact_info);
        stakes.insert(id4, 10);
    }

    #[test]
    fn test_pull_from_entrypoint_if_not_present() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        );
        let entrypoint_pubkey = solana_sdk::pubkey::new_rand();
        let mut entrypoint = ContactInfo::new_localhost(&entrypoint_pubkey, timestamp());
        entrypoint.gossip = socketaddr!("127.0.0.2:1234");
        cluster_info.set_entrypoint(entrypoint.clone());

        let mut stakes = HashMap::new();

        let other_node_pubkey = solana_sdk::pubkey::new_rand();
        let other_node = ContactInfo::new_localhost(&other_node_pubkey, timestamp());
        assert_ne!(other_node.gossip, entrypoint.gossip);
        cluster_info.ping_cache.lock().unwrap().mock_pong(
            other_node.id,
            other_node.gossip,
            Instant::now(),
        );
        cluster_info.insert_info(other_node.clone());
        stakes.insert(other_node_pubkey, 10);

        // Pull request 1:  `other_node` is present but `entrypoint` was just added (so it has a
        // fresh timestamp).  There should only be one pull request to `other_node`
        let (pings, pulls) = cluster_info.new_pull_requests(&thread_pool, None, &stakes);
        assert!(pings.is_empty());
        assert_eq!(pulls.len(), MIN_NUM_BLOOM_FILTERS);
        assert!(pulls.into_iter().all(|(addr, _)| addr == other_node.gossip));

        // Pull request 2: pretend it's been a while since we've pulled from `entrypoint`.  There should
        // now be two pull requests
        cluster_info.entrypoints.write().unwrap()[0].wallclock = 0;
        let (pings, pulls) = cluster_info.new_pull_requests(&thread_pool, None, &stakes);
        assert!(pings.is_empty());
        assert_eq!(pulls.len(), 2 * MIN_NUM_BLOOM_FILTERS);
        assert!(pulls
            .iter()
            .take(MIN_NUM_BLOOM_FILTERS)
            .all(|(addr, _)| *addr == other_node.gossip));
        assert!(pulls
            .iter()
            .skip(MIN_NUM_BLOOM_FILTERS)
            .all(|(addr, _)| *addr == entrypoint.gossip));

        // Pull request 3:  `other_node` is present and `entrypoint` was just pulled from.  There should
        // only be one pull request to `other_node`
        let (pings, pulls) = cluster_info.new_pull_requests(&thread_pool, None, &stakes);
        assert!(pings.is_empty());
        assert_eq!(pulls.len(), MIN_NUM_BLOOM_FILTERS);
        assert!(pulls.into_iter().all(|(addr, _)| addr == other_node.gossip));
    }

    #[test]
    fn test_repair_peers() {
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        );
        for i in 0..10 {
            // make these invalid for the upcoming repair request
            let peer_lowest = if i >= 5 { 10 } else { 0 };
            let other_node_pubkey = solana_sdk::pubkey::new_rand();
            let other_node = ContactInfo::new_localhost(&other_node_pubkey, timestamp());
            cluster_info.insert_info(other_node.clone());
            let value = CrdsValue::new_unsigned(CrdsData::LowestSlot(
                0,
                LowestSlot::new(other_node_pubkey, peer_lowest, timestamp()),
            ));
            let mut gossip_crds = cluster_info.gossip.crds.write().unwrap();
            let _ = gossip_crds.insert(value, timestamp());
        }
        // only half the visible peers should be eligible to serve this repair
        assert_eq!(cluster_info.repair_peers(5).len(), 5);
    }

    #[test]
    fn test_max_bloom_size() {
        // check that the constant fits into the dynamic size
        assert!(MAX_BLOOM_SIZE <= max_bloom_size());
    }

    #[test]
    fn test_protocol_sanitize() {
        let pd = PruneData {
            wallclock: MAX_WALLCLOCK,
            ..PruneData::default()
        };
        let msg = Protocol::PruneMessage(Pubkey::default(), pd);
        assert_eq!(msg.sanitize(), Err(SanitizeError::ValueOutOfBounds));
    }

    #[test]
    fn test_protocol_prune_message_sanitize() {
        let keypair = Keypair::new();
        let mut prune_data = PruneData {
            pubkey: keypair.pubkey(),
            prunes: vec![],
            signature: Signature::default(),
            destination: Pubkey::new_unique(),
            wallclock: timestamp(),
        };
        prune_data.sign(&keypair);
        let prune_message = Protocol::PruneMessage(keypair.pubkey(), prune_data.clone());
        assert_eq!(prune_message.sanitize(), Ok(()));
        let prune_message = Protocol::PruneMessage(Pubkey::new_unique(), prune_data);
        assert_eq!(prune_message.sanitize(), Err(SanitizeError::InvalidValue));
    }

    // computes the maximum size for pull request blooms
    fn max_bloom_size() -> usize {
        let filter_size = serialized_size(&CrdsFilter::default())
            .expect("unable to serialize default filter") as usize;
        let protocol = Protocol::PullRequest(
            CrdsFilter::default(),
            CrdsValue::new_unsigned(CrdsData::ContactInfo(ContactInfo::default())),
        );
        let protocol_size =
            serialized_size(&protocol).expect("unable to serialize gossip protocol") as usize;
        PACKET_DATA_SIZE - (protocol_size - filter_size)
    }

    #[test]
    #[allow(clippy::same_item_push)]
    fn test_push_epoch_slots_large() {
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        );
        let mut range: Vec<Slot> = vec![];
        //random should be hard to compress
        for _ in 0..32000 {
            let last = *range.last().unwrap_or(&0);
            range.push(last + rand::thread_rng().gen_range(1, 32));
        }
        cluster_info.push_epoch_slots(&range[..16000]);
        cluster_info.push_epoch_slots(&range[16000..]);
        let slots = cluster_info.get_epoch_slots(&mut Cursor::default());
        let slots: Vec<_> = slots.iter().flat_map(|x| x.to_slots(0)).collect();
        assert_eq!(slots, range);
    }

    #[test]
    fn test_vote_size() {
        let slots = vec![1; 32];
        let vote = Vote::new(slots, Hash::default());
        let keypair = Arc::new(Keypair::new());

        // Create the biggest possible vote transaction
        let vote_ix = vote_instruction::vote_switch(
            &keypair.pubkey(),
            &keypair.pubkey(),
            vote,
            Hash::default(),
        );
        let mut vote_tx = Transaction::new_with_payer(&[vote_ix], Some(&keypair.pubkey()));

        vote_tx.partial_sign(&[keypair.as_ref()], Hash::default());
        vote_tx.partial_sign(&[keypair.as_ref()], Hash::default());

        let vote = CrdsVote::new(
            keypair.pubkey(),
            vote_tx,
            0, // wallclock
        );
        let vote = CrdsValue::new_signed(CrdsData::Vote(1, vote), &Keypair::new());
        assert!(bincode::serialized_size(&vote).unwrap() <= PUSH_MESSAGE_MAX_PAYLOAD_SIZE as u64);
    }

    #[test]
    fn test_process_entrypoint_adopt_shred_version() {
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        ));
        assert_eq!(cluster_info.my_shred_version(), 0);

        // Simulating starting up with two entrypoints, no known id, only a gossip
        // address
        let entrypoint1_gossip_addr = socketaddr!("127.0.0.2:1234");
        let mut entrypoint1 = ContactInfo::new_localhost(&Pubkey::default(), timestamp());
        entrypoint1.gossip = entrypoint1_gossip_addr;
        assert_eq!(entrypoint1.shred_version, 0);

        let entrypoint2_gossip_addr = socketaddr!("127.0.0.2:5678");
        let mut entrypoint2 = ContactInfo::new_localhost(&Pubkey::default(), timestamp());
        entrypoint2.gossip = entrypoint2_gossip_addr;
        assert_eq!(entrypoint2.shred_version, 0);
        cluster_info.set_entrypoints(vec![entrypoint1, entrypoint2]);

        // Simulate getting entrypoint ContactInfo from gossip with an entrypoint1 shred version of
        // 0
        let mut gossiped_entrypoint1_info =
            ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), timestamp());
        gossiped_entrypoint1_info.gossip = entrypoint1_gossip_addr;
        gossiped_entrypoint1_info.shred_version = 0;
        cluster_info.insert_info(gossiped_entrypoint1_info.clone());
        assert!(!cluster_info
            .entrypoints
            .read()
            .unwrap()
            .iter()
            .any(|entrypoint| *entrypoint == gossiped_entrypoint1_info));

        // Adopt the entrypoint's gossiped contact info and verify
        let entrypoints_processed = ClusterInfo::process_entrypoints(&cluster_info);
        assert_eq!(cluster_info.entrypoints.read().unwrap().len(), 2);
        assert!(cluster_info
            .entrypoints
            .read()
            .unwrap()
            .iter()
            .any(|entrypoint| *entrypoint == gossiped_entrypoint1_info));

        assert!(!entrypoints_processed); // <--- entrypoint processing incomplete because shred adoption still pending
        assert_eq!(cluster_info.my_shred_version(), 0); // <-- shred version still 0

        // Simulate getting entrypoint ContactInfo from gossip with an entrypoint2 shred version of
        // !0
        let mut gossiped_entrypoint2_info =
            ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), timestamp());
        gossiped_entrypoint2_info.gossip = entrypoint2_gossip_addr;
        gossiped_entrypoint2_info.shred_version = 1;
        cluster_info.insert_info(gossiped_entrypoint2_info.clone());
        assert!(!cluster_info
            .entrypoints
            .read()
            .unwrap()
            .iter()
            .any(|entrypoint| *entrypoint == gossiped_entrypoint2_info));

        // Adopt the entrypoint's gossiped contact info and verify
        error!("Adopt the entrypoint's gossiped contact info and verify");
        let entrypoints_processed = ClusterInfo::process_entrypoints(&cluster_info);
        assert_eq!(cluster_info.entrypoints.read().unwrap().len(), 2);
        assert!(cluster_info
            .entrypoints
            .read()
            .unwrap()
            .iter()
            .any(|entrypoint| *entrypoint == gossiped_entrypoint2_info));

        assert!(entrypoints_processed);
        assert_eq!(cluster_info.my_shred_version(), 1); // <-- shred version now adopted from entrypoint2
    }

    #[test]
    fn test_process_entrypoint_without_adopt_shred_version() {
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = Arc::new(ClusterInfo::new(
            {
                let mut contact_info =
                    ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp());
                contact_info.shred_version = 2;
                contact_info
            },
            node_keypair,
            SocketAddrSpace::Unspecified,
        ));
        assert_eq!(cluster_info.my_shred_version(), 2);

        // Simulating starting up with default entrypoint, no known id, only a gossip
        // address
        let entrypoint_gossip_addr = socketaddr!("127.0.0.2:1234");
        let mut entrypoint = ContactInfo::new_localhost(&Pubkey::default(), timestamp());
        entrypoint.gossip = entrypoint_gossip_addr;
        assert_eq!(entrypoint.shred_version, 0);
        cluster_info.set_entrypoint(entrypoint);

        // Simulate getting entrypoint ContactInfo from gossip
        let mut gossiped_entrypoint_info =
            ContactInfo::new_localhost(&solana_sdk::pubkey::new_rand(), timestamp());
        gossiped_entrypoint_info.gossip = entrypoint_gossip_addr;
        gossiped_entrypoint_info.shred_version = 1;
        cluster_info.insert_info(gossiped_entrypoint_info.clone());

        // Adopt the entrypoint's gossiped contact info and verify
        let entrypoints_processed = ClusterInfo::process_entrypoints(&cluster_info);
        assert_eq!(cluster_info.entrypoints.read().unwrap().len(), 1);
        assert_eq!(
            cluster_info.entrypoints.read().unwrap()[0],
            gossiped_entrypoint_info
        );
        assert!(entrypoints_processed);
        assert_eq!(cluster_info.my_shred_version(), 2); // <--- No change to shred version
    }

    #[test]
    fn test_compute_retransmit_peers_small() {
        const FANOUT: usize = 3;
        let index = vec![
            14, 15, 28, // 1st layer
            // 2nd layer
            29, 4, 5, // 1st neighborhood
            9, 16, 7, // 2nd neighborhood
            26, 23, 2, // 3rd neighborhood
            // 3rd layer
            31, 3, 17, // 1st neighborhood
            20, 25, 0, // 2nd neighborhood
            13, 30, 18, // 3rd neighborhood
            19, 21, 22, // 4th neighborhood
            6, 8, 11, // 5th neighborhood
            27, 1, 10, // 6th neighborhood
            12, 24, 34, // 7th neighborhood
            33, 32, // 8th neighborhood
        ];
        // 1st layer
        assert_eq!(
            compute_retransmit_peers(FANOUT, 0, &index),
            (vec![14, 15, 28], vec![29, 9, 26])
        );
        assert_eq!(
            compute_retransmit_peers(FANOUT, 1, &index),
            (vec![14, 15, 28], vec![4, 16, 23])
        );
        assert_eq!(
            compute_retransmit_peers(FANOUT, 2, &index),
            (vec![14, 15, 28], vec![5, 7, 2])
        );
        // 2nd layer, 1st neighborhood
        assert_eq!(
            compute_retransmit_peers(FANOUT, 3, &index),
            (vec![29, 4, 5], vec![31, 20, 13])
        );
        assert_eq!(
            compute_retransmit_peers(FANOUT, 4, &index),
            (vec![29, 4, 5], vec![3, 25, 30])
        );
        assert_eq!(
            compute_retransmit_peers(FANOUT, 5, &index),
            (vec![29, 4, 5], vec![17, 0, 18])
        );
        // 2nd layer, 2nd neighborhood
        assert_eq!(
            compute_retransmit_peers(FANOUT, 6, &index),
            (vec![9, 16, 7], vec![19, 6, 27])
        );
        assert_eq!(
            compute_retransmit_peers(FANOUT, 7, &index),
            (vec![9, 16, 7], vec![21, 8, 1])
        );
        assert_eq!(
            compute_retransmit_peers(FANOUT, 8, &index),
            (vec![9, 16, 7], vec![22, 11, 10])
        );
        // 2nd layer, 3rd neighborhood
        assert_eq!(
            compute_retransmit_peers(FANOUT, 9, &index),
            (vec![26, 23, 2], vec![12, 33])
        );
        assert_eq!(
            compute_retransmit_peers(FANOUT, 10, &index),
            (vec![26, 23, 2], vec![24, 32])
        );
        assert_eq!(
            compute_retransmit_peers(FANOUT, 11, &index),
            (vec![26, 23, 2], vec![34])
        );
        // 3rd layer
        let num_nodes = index.len();
        for k in (12..num_nodes).step_by(3) {
            let end = num_nodes.min(k + 3);
            let neighbors = index[k..end].to_vec();
            for i in k..end {
                assert_eq!(
                    compute_retransmit_peers(FANOUT, i, &index),
                    (neighbors.clone(), vec![])
                );
            }
        }
    }

    #[test]
    fn test_compute_retransmit_peers_with_fanout_five() {
        const FANOUT: usize = 5;
        const NUM_NODES: usize = 2048;
        const SEED: [u8; 32] = [0x55; 32];
        let mut rng = ChaChaRng::from_seed(SEED);
        let mut index: Vec<_> = (0..NUM_NODES).collect();
        index.shuffle(&mut rng);
        let (neighbors, children) = compute_retransmit_peers(FANOUT, 17, &index);
        assert_eq!(neighbors, vec![1410, 1293, 1810, 552, 512]);
        assert_eq!(children, vec![511, 1989, 283, 1606, 1154]);
    }

    #[test]
    fn test_compute_retransmit_peers_large() {
        const FANOUT: usize = 7;
        const NUM_NODES: usize = 512;
        let mut rng = rand::thread_rng();
        let mut index: Vec<_> = (0..NUM_NODES).collect();
        index.shuffle(&mut rng);
        let pos: HashMap<usize, usize> = index
            .iter()
            .enumerate()
            .map(|(i, node)| (*node, i))
            .collect();
        let mut seen = vec![0; NUM_NODES];
        for i in 0..NUM_NODES {
            let node = index[i];
            let (neighbors, children) = compute_retransmit_peers(FANOUT, i, &index);
            assert!(neighbors.len() <= FANOUT);
            assert!(children.len() <= FANOUT);
            // If x is neighbor of y then y is also neighbor of x.
            for other in &neighbors {
                let j = pos[other];
                let (other_neighbors, _) = compute_retransmit_peers(FANOUT, j, &index);
                assert!(other_neighbors.contains(&node));
            }
            for i in children {
                seen[i] += 1;
            }
        }
        // Except for the first layer, each node
        // is child of exactly one other node.
        let (seed, _) = compute_retransmit_peers(FANOUT, 0, &index);
        for (i, k) in seen.into_iter().enumerate() {
            if seed.contains(&i) {
                assert_eq!(k, 0);
            } else {
                assert_eq!(k, 1);
            }
        }
    }

    #[test]
    #[ignore] // TODO: debug why this is flaky on buildkite!
    fn test_pull_request_time_pruning() {
        let node = Node::new_localhost();
        let cluster_info = Arc::new(ClusterInfo::new(
            node.info,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        let entrypoint_pubkey = solana_sdk::pubkey::new_rand();
        let entrypoint = ContactInfo::new_localhost(&entrypoint_pubkey, timestamp());
        cluster_info.set_entrypoint(entrypoint);

        let mut rng = rand::thread_rng();
        let shred_version = cluster_info.my_shred_version();
        let mut peers: Vec<Pubkey> = vec![];

        const NO_ENTRIES: usize = CRDS_UNIQUE_PUBKEY_CAPACITY + 128;
        let data: Vec<_> = repeat_with(|| {
            let keypair = Keypair::new();
            peers.push(keypair.pubkey());
            let mut rand_ci = ContactInfo::new_rand(&mut rng, Some(keypair.pubkey()));
            rand_ci.shred_version = shred_version;
            rand_ci.wallclock = timestamp();
            CrdsValue::new_signed(CrdsData::ContactInfo(rand_ci), &keypair)
        })
        .take(NO_ENTRIES)
        .collect();
        let mut timeouts = HashMap::new();
        timeouts.insert(Pubkey::default(), CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS * 4);
        assert_eq!(
            (0, 0, NO_ENTRIES),
            cluster_info.handle_pull_response(&entrypoint_pubkey, data, &timeouts)
        );

        let now = timestamp();
        for peer in peers {
            cluster_info
                .gossip
                .mark_pull_request_creation_time(peer, now);
        }
        assert_eq!(
            cluster_info.gossip.pull.pull_request_time().len(),
            CRDS_UNIQUE_PUBKEY_CAPACITY
        );
    }

    #[test]
    fn test_get_epoch_millis_no_bank() {
        assert_eq!(
            get_epoch_duration(/*bank_forks=*/ None).as_millis() as u64,
            DEFAULT_SLOTS_PER_EPOCH * DEFAULT_MS_PER_SLOT // 48 hours
        );
    }
}
