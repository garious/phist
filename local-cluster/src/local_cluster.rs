use crate::{
    cluster::{Cluster, ClusterValidatorInfo, ValidatorInfo},
    cluster_tests,
    validator_configs::*,
};
use itertools::izip;
use log::*;
use solana_client::thin_client::{create_client, ThinClient};
use solana_core::validator::{Validator, ValidatorConfig, ValidatorStartProgress};
use solana_gossip::{
    cluster_info::{Node, VALIDATOR_PORT_RANGE},
    contact_info::ContactInfo,
    gossip_service::discover_cluster,
};
use solana_ledger::create_new_tmp_ledger;
use solana_runtime::genesis_utils::{
    create_genesis_config_with_vote_accounts_and_cluster_type, GenesisConfigInfo,
    ValidatorVoteKeypairs,
};
use solana_sdk::{
    account::Account,
    account::AccountSharedData,
    client::SyncClient,
    clock::{DEFAULT_DEV_SLOTS_PER_EPOCH, DEFAULT_TICKS_PER_SLOT},
    commitment_config::CommitmentConfig,
    epoch_schedule::EpochSchedule,
    genesis_config::{ClusterType, GenesisConfig},
    message::Message,
    poh_config::PohConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    stake::{
        config as stake_config, instruction as stake_instruction,
        state::{Authorized, Lockup},
    },
    system_transaction,
    transaction::Transaction,
};
use solana_stake_program::{config::create_account as create_stake_config_account, stake_state};
use solana_vote_program::{
    vote_instruction,
    vote_state::{VoteInit, VoteState},
};
use std::{
    collections::HashMap,
    io::{Error, ErrorKind, Result},
    iter,
    sync::{Arc, RwLock},
};

pub struct ClusterConfig {
    /// The validator config that should be applied to every node in the cluster
    pub validator_configs: Vec<ValidatorConfig>,
    /// Number of nodes that are unstaked and not voting (a.k.a listening)
    pub num_listeners: u64,
    /// List of tuples (pubkeys, in_genesis) of each node if specified. If
    /// `in_genesis` == true, the validator's vote and stake accounts
    //  will be inserted into the genesis block instead of warming up through
    // creating the vote accounts. The first validator (bootstrap leader) automatically
    // is assumed to be `in_genesis` == true.
    pub validator_keys: Option<Vec<(Arc<Keypair>, bool)>>,
    /// The stakes of each node
    pub node_stakes: Vec<u64>,
    /// The total lamports available to the cluster
    pub cluster_lamports: u64,
    pub ticks_per_slot: u64,
    pub slots_per_epoch: u64,
    pub stakers_slot_offset: u64,
    pub skip_warmup_slots: bool,
    pub native_instruction_processors: Vec<(String, Pubkey)>,
    pub cluster_type: ClusterType,
    pub poh_config: PohConfig,
    pub additional_accounts: Vec<(Pubkey, AccountSharedData)>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            validator_configs: vec![],
            num_listeners: 0,
            validator_keys: None,
            node_stakes: vec![],
            cluster_lamports: 0,
            ticks_per_slot: DEFAULT_TICKS_PER_SLOT,
            slots_per_epoch: DEFAULT_DEV_SLOTS_PER_EPOCH,
            stakers_slot_offset: DEFAULT_DEV_SLOTS_PER_EPOCH,
            native_instruction_processors: vec![],
            cluster_type: ClusterType::Development,
            poh_config: PohConfig::default(),
            skip_warmup_slots: false,
            additional_accounts: vec![],
        }
    }
}

pub struct LocalCluster {
    /// Keypair with funding to participate in the network
    pub funding_keypair: Keypair,
    /// Entry point from which the rest of the network can be discovered
    pub entry_point_info: ContactInfo,
    pub validators: HashMap<Pubkey, ClusterValidatorInfo>,
    pub genesis_config: GenesisConfig,
}

impl LocalCluster {
    pub fn new_with_equal_stakes(
        num_nodes: usize,
        cluster_lamports: u64,
        lamports_per_node: u64,
    ) -> Self {
        let stakes: Vec<_> = (0..num_nodes).map(|_| lamports_per_node).collect();
        let mut config = ClusterConfig {
            node_stakes: stakes,
            cluster_lamports,
            validator_configs: make_identical_validator_configs(
                &ValidatorConfig::default(),
                num_nodes,
            ),
            ..ClusterConfig::default()
        };
        Self::new(&mut config)
    }

    pub fn new(config: &mut ClusterConfig) -> Self {
        assert_eq!(config.validator_configs.len(), config.node_stakes.len());
        let mut validator_keys = {
            if let Some(ref keys) = config.validator_keys {
                assert_eq!(config.validator_configs.len(), keys.len());
                keys.clone()
            } else {
                iter::repeat_with(|| (Arc::new(Keypair::new()), false))
                    .take(config.validator_configs.len())
                    .collect()
            }
        };

        // Bootstrap leader should always be in genesis block
        validator_keys[0].1 = true;
        let (keys_in_genesis, stakes_in_genesis): (Vec<ValidatorVoteKeypairs>, Vec<u64>) =
            validator_keys
                .iter()
                .zip(&config.node_stakes)
                .filter_map(|((node_keypair, in_genesis), stake)| {
                    if *in_genesis {
                        Some((
                            ValidatorVoteKeypairs {
                                node_keypair: Keypair::from_bytes(&node_keypair.to_bytes())
                                    .unwrap(),
                                vote_keypair: Keypair::new(),
                                stake_keypair: Keypair::new(),
                            },
                            stake,
                        ))
                    } else {
                        None
                    }
                })
                .unzip();
        let leader_keypair = &keys_in_genesis[0].node_keypair;
        let leader_vote_keypair = &keys_in_genesis[0].vote_keypair;
        let leader_pubkey = leader_keypair.pubkey();
        let leader_node = Node::new_localhost_with_pubkey(&leader_pubkey);
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_vote_accounts_and_cluster_type(
            config.cluster_lamports,
            &keys_in_genesis,
            stakes_in_genesis,
            config.cluster_type,
        );
        genesis_config.accounts.extend(
            config
                .additional_accounts
                .drain(..)
                .map(|(key, account)| (key, Account::from(account))),
        );
        genesis_config.ticks_per_slot = config.ticks_per_slot;
        genesis_config.epoch_schedule = EpochSchedule::custom(
            config.slots_per_epoch,
            config.stakers_slot_offset,
            !config.skip_warmup_slots,
        );
        genesis_config.poh_config = config.poh_config.clone();
        genesis_config
            .native_instruction_processors
            .extend_from_slice(&config.native_instruction_processors);

        // Replace staking config
        genesis_config.add_account(
            stake_config::id(),
            create_stake_config_account(
                1,
                &stake_config::Config {
                    warmup_cooldown_rate: 1_000_000_000.0f64,
                    slash_penalty: std::u8::MAX,
                },
            ),
        );

        let (leader_ledger_path, _blockhash) = create_new_tmp_ledger!(&genesis_config);
        let leader_contact_info = leader_node.info.clone();
        let mut leader_config = safe_clone_config(&config.validator_configs[0]);
        leader_config.rpc_addrs = Some((leader_node.info.rpc, leader_node.info.rpc_pubsub));
        leader_config.account_paths = vec![leader_ledger_path.join("accounts")];
        let leader_keypair = Arc::new(Keypair::from_bytes(&leader_keypair.to_bytes()).unwrap());
        let leader_vote_keypair =
            Arc::new(Keypair::from_bytes(&leader_vote_keypair.to_bytes()).unwrap());

        let leader_server = Validator::new(
            leader_node,
            leader_keypair.clone(),
            &leader_ledger_path,
            &leader_vote_keypair.pubkey(),
            Arc::new(RwLock::new(vec![leader_vote_keypair.clone()])),
            vec![],
            &leader_config,
            true, // should_check_duplicate_instance
            Arc::new(RwLock::new(ValidatorStartProgress::default())),
        );

        let mut validators = HashMap::new();
        let leader_info = ValidatorInfo {
            keypair: leader_keypair,
            voting_keypair: leader_vote_keypair,
            ledger_path: leader_ledger_path,
            contact_info: leader_contact_info.clone(),
        };
        let cluster_leader = ClusterValidatorInfo::new(
            leader_info,
            safe_clone_config(&config.validator_configs[0]),
            leader_server,
        );

        validators.insert(leader_pubkey, cluster_leader);

        let mut cluster = Self {
            funding_keypair: mint_keypair,
            entry_point_info: leader_contact_info,
            validators,
            genesis_config,
        };

        let node_pubkey_to_vote_key: HashMap<Pubkey, Arc<Keypair>> = keys_in_genesis
            .into_iter()
            .map(|keypairs| {
                (
                    keypairs.node_keypair.pubkey(),
                    Arc::new(Keypair::from_bytes(&keypairs.vote_keypair.to_bytes()).unwrap()),
                )
            })
            .collect();
        for (stake, validator_config, (key, _)) in izip!(
            (&config.node_stakes[1..]).iter(),
            config.validator_configs[1..].iter(),
            validator_keys[1..].iter(),
        ) {
            cluster.add_validator(
                validator_config,
                *stake,
                key.clone(),
                node_pubkey_to_vote_key.get(&key.pubkey()).cloned(),
            );
        }

        let mut listener_config = safe_clone_config(&config.validator_configs[0]);
        listener_config.voting_disabled = true;
        (0..config.num_listeners).for_each(|_| {
            cluster.add_validator(&listener_config, 0, Arc::new(Keypair::new()), None);
        });

        discover_cluster(
            &cluster.entry_point_info.gossip,
            config.node_stakes.len() + config.num_listeners as usize,
        )
        .unwrap();

        discover_cluster(&cluster.entry_point_info.gossip, config.node_stakes.len()).unwrap();

        cluster
    }

    pub fn exit(&mut self) {
        for node in self.validators.values_mut() {
            if let Some(ref mut v) = node.validator {
                v.exit();
            }
        }
    }

    pub fn close_preserve_ledgers(&mut self) {
        self.exit();
        for (_, node) in self.validators.iter_mut() {
            if let Some(v) = node.validator.take() {
                v.join();
            }
        }
    }

    pub fn add_validator(
        &mut self,
        validator_config: &ValidatorConfig,
        stake: u64,
        validator_keypair: Arc<Keypair>,
        mut voting_keypair: Option<Arc<Keypair>>,
    ) -> Pubkey {
        let client = create_client(
            self.entry_point_info.client_facing_addr(),
            VALIDATOR_PORT_RANGE,
        );

        // Must have enough tokens to fund vote account and set delegate
        let should_create_vote_pubkey = voting_keypair.is_none();
        if voting_keypair.is_none() {
            voting_keypair = Some(Arc::new(Keypair::new()));
        }
        let validator_pubkey = validator_keypair.pubkey();
        let validator_node = Node::new_localhost_with_pubkey(&validator_keypair.pubkey());
        let contact_info = validator_node.info.clone();
        let (ledger_path, _blockhash) = create_new_tmp_ledger!(&self.genesis_config);

        if validator_config.voting_disabled {
            // setup as a listener
            info!("listener {} ", validator_pubkey,);
        } else {
            // Give the validator some lamports to setup vote accounts
            if should_create_vote_pubkey {
                let validator_balance = Self::transfer_with_client(
                    &client,
                    &self.funding_keypair,
                    &validator_pubkey,
                    stake * 2 + 2,
                );
                info!(
                    "validator {} balance {}",
                    validator_pubkey, validator_balance
                );
                Self::setup_vote_and_stake_accounts(
                    &client,
                    voting_keypair.as_ref().unwrap(),
                    &validator_keypair,
                    stake,
                )
                .unwrap();
            }
        }

        let mut config = safe_clone_config(validator_config);
        config.rpc_addrs = Some((validator_node.info.rpc, validator_node.info.rpc_pubsub));
        config.account_paths = vec![ledger_path.join("accounts")];
        let voting_keypair = voting_keypair.unwrap();
        let validator_server = Validator::new(
            validator_node,
            validator_keypair.clone(),
            &ledger_path,
            &voting_keypair.pubkey(),
            Arc::new(RwLock::new(vec![voting_keypair.clone()])),
            vec![self.entry_point_info.clone()],
            &config,
            true, // should_check_duplicate_instance
            Arc::new(RwLock::new(ValidatorStartProgress::default())),
        );

        let validator_pubkey = validator_keypair.pubkey();
        let validator_info = ClusterValidatorInfo::new(
            ValidatorInfo {
                keypair: validator_keypair,
                voting_keypair,
                ledger_path,
                contact_info,
            },
            safe_clone_config(validator_config),
            validator_server,
        );

        self.validators.insert(validator_pubkey, validator_info);
        validator_pubkey
    }

    pub fn ledger_path(&self, validator_pubkey: &Pubkey) -> std::path::PathBuf {
        self.validators
            .get(validator_pubkey)
            .unwrap()
            .info
            .ledger_path
            .clone()
    }

    fn close(&mut self) {
        self.close_preserve_ledgers();
    }

    pub fn transfer(&self, source_keypair: &Keypair, dest_pubkey: &Pubkey, lamports: u64) -> u64 {
        let client = create_client(
            self.entry_point_info.client_facing_addr(),
            VALIDATOR_PORT_RANGE,
        );
        Self::transfer_with_client(&client, source_keypair, dest_pubkey, lamports)
    }

    pub fn check_for_new_roots(&self, num_new_roots: usize, test_name: &str) {
        let alive_node_contact_infos: Vec<_> = self
            .validators
            .values()
            .map(|v| v.info.contact_info.clone())
            .collect();
        assert!(!alive_node_contact_infos.is_empty());
        info!("{} discovering nodes", test_name);
        let cluster_nodes = discover_cluster(
            &alive_node_contact_infos[0].gossip,
            alive_node_contact_infos.len(),
        )
        .unwrap();
        info!("{} discovered {} nodes", test_name, cluster_nodes.len());
        info!("{} looking for new roots on all nodes", test_name);
        cluster_tests::check_for_new_roots(num_new_roots, &alive_node_contact_infos, test_name);
        info!("{} done waiting for roots", test_name);
    }

    pub fn check_no_new_roots(&self, num_slots_to_wait: usize, test_name: &str) {
        let alive_node_contact_infos: Vec<_> = self
            .validators
            .values()
            .map(|v| v.info.contact_info.clone())
            .collect();
        assert!(!alive_node_contact_infos.is_empty());
        info!("{} discovering nodes", test_name);
        let cluster_nodes = discover_cluster(
            &alive_node_contact_infos[0].gossip,
            alive_node_contact_infos.len(),
        )
        .unwrap();
        info!("{} discovered {} nodes", test_name, cluster_nodes.len());
        info!("{} making sure no new roots on any nodes", test_name);
        cluster_tests::check_no_new_roots(num_slots_to_wait, &alive_node_contact_infos, test_name);
        info!("{} done waiting for roots", test_name);
    }

    fn transfer_with_client(
        client: &ThinClient,
        source_keypair: &Keypair,
        dest_pubkey: &Pubkey,
        lamports: u64,
    ) -> u64 {
        trace!("getting leader blockhash");
        let (blockhash, _fee_calculator, _last_valid_slot) = client
            .get_recent_blockhash_with_commitment(CommitmentConfig::processed())
            .unwrap();
        let mut tx = system_transaction::transfer(source_keypair, dest_pubkey, lamports, blockhash);
        info!(
            "executing transfer of {} from {} to {}",
            lamports,
            source_keypair.pubkey(),
            *dest_pubkey
        );
        client
            .retry_transfer(source_keypair, &mut tx, 10)
            .expect("client transfer");
        client
            .wait_for_balance_with_commitment(
                dest_pubkey,
                Some(lamports),
                CommitmentConfig::processed(),
            )
            .expect("get balance")
    }

    fn setup_vote_and_stake_accounts(
        client: &ThinClient,
        vote_account: &Keypair,
        from_account: &Arc<Keypair>,
        amount: u64,
    ) -> Result<()> {
        let vote_account_pubkey = vote_account.pubkey();
        let node_pubkey = from_account.pubkey();
        info!(
            "setup_vote_and_stake_accounts: {}, {}",
            node_pubkey, vote_account_pubkey
        );
        let stake_account_keypair = Keypair::new();
        let stake_account_pubkey = stake_account_keypair.pubkey();

        // Create the vote account if necessary
        if client
            .poll_get_balance_with_commitment(&vote_account_pubkey, CommitmentConfig::processed())
            .unwrap_or(0)
            == 0
        {
            // 1) Create vote account

            let instructions = vote_instruction::create_account(
                &from_account.pubkey(),
                &vote_account_pubkey,
                &VoteInit {
                    node_pubkey,
                    authorized_voter: vote_account_pubkey,
                    authorized_withdrawer: vote_account_pubkey,
                    commission: 0,
                },
                amount,
            );
            let message = Message::new(&instructions, Some(&from_account.pubkey()));
            let mut transaction = Transaction::new(
                &[from_account.as_ref(), vote_account],
                message,
                client
                    .get_recent_blockhash_with_commitment(CommitmentConfig::processed())
                    .unwrap()
                    .0,
            );
            client
                .retry_transfer(from_account, &mut transaction, 10)
                .expect("fund vote");
            client
                .wait_for_balance_with_commitment(
                    &vote_account_pubkey,
                    Some(amount),
                    CommitmentConfig::processed(),
                )
                .expect("get balance");

            let instructions = stake_instruction::create_account_and_delegate_stake(
                &from_account.pubkey(),
                &stake_account_pubkey,
                &vote_account_pubkey,
                &Authorized::auto(&stake_account_pubkey),
                &Lockup::default(),
                amount,
            );
            let message = Message::new(&instructions, Some(&from_account.pubkey()));
            let mut transaction = Transaction::new(
                &[from_account.as_ref(), &stake_account_keypair],
                message,
                client
                    .get_recent_blockhash_with_commitment(CommitmentConfig::processed())
                    .unwrap()
                    .0,
            );

            client
                .send_and_confirm_transaction(
                    &[from_account.as_ref(), &stake_account_keypair],
                    &mut transaction,
                    5,
                    0,
                )
                .expect("delegate stake");
            client
                .wait_for_balance_with_commitment(
                    &stake_account_pubkey,
                    Some(amount),
                    CommitmentConfig::processed(),
                )
                .expect("get balance");
        } else {
            warn!(
                "{} vote_account already has a balance?!?",
                vote_account_pubkey
            );
        }
        info!("Checking for vote account registration of {}", node_pubkey);
        match (
            client
                .get_account_with_commitment(&stake_account_pubkey, CommitmentConfig::processed()),
            client.get_account_with_commitment(&vote_account_pubkey, CommitmentConfig::processed()),
        ) {
            (Ok(Some(stake_account)), Ok(Some(vote_account))) => {
                match (
                    stake_state::stake_from(&stake_account),
                    VoteState::from(&vote_account),
                ) {
                    (Some(stake_state), Some(vote_state)) => {
                        if stake_state.delegation.voter_pubkey != vote_account_pubkey
                            || stake_state.delegation.stake != amount
                        {
                            Err(Error::new(ErrorKind::Other, "invalid stake account state"))
                        } else if vote_state.node_pubkey != node_pubkey {
                            Err(Error::new(ErrorKind::Other, "invalid vote account state"))
                        } else {
                            info!("node {} {:?} {:?}", node_pubkey, stake_state, vote_state);

                            Ok(())
                        }
                    }
                    (None, _) => Err(Error::new(ErrorKind::Other, "invalid stake account data")),
                    (_, None) => Err(Error::new(ErrorKind::Other, "invalid vote account data")),
                }
            }
            (Ok(None), _) | (Err(_), _) => Err(Error::new(
                ErrorKind::Other,
                "unable to retrieve stake account data",
            )),
            (_, Ok(None)) | (_, Err(_)) => Err(Error::new(
                ErrorKind::Other,
                "unable to retrieve vote account data",
            )),
        }
    }
}

impl Cluster for LocalCluster {
    fn get_node_pubkeys(&self) -> Vec<Pubkey> {
        self.validators.keys().cloned().collect()
    }

    fn get_validator_client(&self, pubkey: &Pubkey) -> Option<ThinClient> {
        self.validators.get(pubkey).map(|f| {
            create_client(
                f.info.contact_info.client_facing_addr(),
                VALIDATOR_PORT_RANGE,
            )
        })
    }

    fn exit_node(&mut self, pubkey: &Pubkey) -> ClusterValidatorInfo {
        let mut node = self.validators.remove(pubkey).unwrap();

        // Shut down the validator
        let mut validator = node.validator.take().expect("Validator must be running");
        validator.exit();
        validator.join();
        node
    }

    fn create_restart_context(
        &mut self,
        pubkey: &Pubkey,
        cluster_validator_info: &mut ClusterValidatorInfo,
    ) -> (Node, Option<ContactInfo>) {
        // Update the stored ContactInfo for this node
        let node = Node::new_localhost_with_pubkey(pubkey);
        cluster_validator_info.info.contact_info = node.info.clone();
        cluster_validator_info.config.rpc_addrs = Some((node.info.rpc, node.info.rpc_pubsub));

        let entry_point_info = {
            if *pubkey == self.entry_point_info.id {
                self.entry_point_info = node.info.clone();
                None
            } else {
                Some(self.entry_point_info.clone())
            }
        };

        (node, entry_point_info)
    }

    fn restart_node(&mut self, pubkey: &Pubkey, mut cluster_validator_info: ClusterValidatorInfo) {
        let restart_context = self.create_restart_context(pubkey, &mut cluster_validator_info);
        let cluster_validator_info =
            Self::restart_node_with_context(cluster_validator_info, restart_context);
        self.add_node(pubkey, cluster_validator_info);
    }

    fn add_node(&mut self, pubkey: &Pubkey, cluster_validator_info: ClusterValidatorInfo) {
        self.validators.insert(*pubkey, cluster_validator_info);
    }

    fn restart_node_with_context(
        mut cluster_validator_info: ClusterValidatorInfo,
        (node, entry_point_info): (Node, Option<ContactInfo>),
    ) -> ClusterValidatorInfo {
        // Restart the node
        let validator_info = &cluster_validator_info.info;
        cluster_validator_info.config.account_paths =
            vec![validator_info.ledger_path.join("accounts")];
        let restarted_node = Validator::new(
            node,
            validator_info.keypair.clone(),
            &validator_info.ledger_path,
            &validator_info.voting_keypair.pubkey(),
            Arc::new(RwLock::new(vec![validator_info.voting_keypair.clone()])),
            entry_point_info
                .map(|entry_point_info| vec![entry_point_info])
                .unwrap_or_default(),
            &safe_clone_config(&cluster_validator_info.config),
            true, // should_check_duplicate_instance
            Arc::new(RwLock::new(ValidatorStartProgress::default())),
        );
        cluster_validator_info.validator = Some(restarted_node);
        cluster_validator_info
    }

    fn exit_restart_node(&mut self, pubkey: &Pubkey, validator_config: ValidatorConfig) {
        let mut cluster_validator_info = self.exit_node(pubkey);
        cluster_validator_info.config = validator_config;
        self.restart_node(pubkey, cluster_validator_info);
    }

    fn get_contact_info(&self, pubkey: &Pubkey) -> Option<&ContactInfo> {
        self.validators.get(pubkey).map(|v| &v.info.contact_info)
    }
}

impl Drop for LocalCluster {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use solana_sdk::epoch_schedule::MINIMUM_SLOTS_PER_EPOCH;

    #[test]
    fn test_local_cluster_start_and_exit() {
        solana_logger::setup();
        let num_nodes = 1;
        let cluster = LocalCluster::new_with_equal_stakes(num_nodes, 100, 3);
        assert_eq!(cluster.validators.len(), num_nodes);
    }

    #[test]
    fn test_local_cluster_start_and_exit_with_config() {
        solana_logger::setup();
        const NUM_NODES: usize = 1;
        let mut config = ClusterConfig {
            validator_configs: make_identical_validator_configs(
                &ValidatorConfig::default(),
                NUM_NODES,
            ),
            node_stakes: vec![3; NUM_NODES],
            cluster_lamports: 100,
            ticks_per_slot: 8,
            slots_per_epoch: MINIMUM_SLOTS_PER_EPOCH as u64,
            stakers_slot_offset: MINIMUM_SLOTS_PER_EPOCH as u64,
            ..ClusterConfig::default()
        };
        let cluster = LocalCluster::new(&mut config);
        assert_eq!(cluster.validators.len(), NUM_NODES);
    }
}
