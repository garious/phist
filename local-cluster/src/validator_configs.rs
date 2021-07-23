use solana_core::validator::ValidatorConfig;
use solana_sdk::exit::Exit;
use std::sync::{Arc, RwLock};

pub fn safe_clone_config(config: &ValidatorConfig) -> ValidatorConfig {
    ValidatorConfig {
        dev_halt_at_slot: config.dev_halt_at_slot,
        expected_genesis_hash: config.expected_genesis_hash,
        expected_bank_hash: config.expected_bank_hash,
        expected_shred_version: config.expected_shred_version,
        voting_disabled: config.voting_disabled,
        account_paths: config.account_paths.clone(),
        account_shrink_paths: config.account_shrink_paths.clone(),
        rpc_config: config.rpc_config.clone(),
        rpc_addrs: config.rpc_addrs,
        pubsub_config: config.pubsub_config.clone(),
        snapshot_config: config.snapshot_config.clone(),
        max_ledger_shreds: config.max_ledger_shreds,
        broadcast_stage_type: config.broadcast_stage_type.clone(),
        enable_partition: config.enable_partition.clone(),
        enforce_ulimit_nofile: config.enforce_ulimit_nofile,
        fixed_leader_schedule: config.fixed_leader_schedule.clone(),
        wait_for_supermajority: config.wait_for_supermajority,
        new_hard_forks: config.new_hard_forks.clone(),
        trusted_validators: config.trusted_validators.clone(),
        repair_validators: config.repair_validators.clone(),
        gossip_validators: config.gossip_validators.clone(),
        halt_on_trusted_validators_accounts_hash_mismatch: config
            .halt_on_trusted_validators_accounts_hash_mismatch,
        accounts_hash_fault_injection_slots: config.accounts_hash_fault_injection_slots,
        frozen_accounts: config.frozen_accounts.clone(),
        no_rocksdb_compaction: config.no_rocksdb_compaction,
        rocksdb_compaction_interval: config.rocksdb_compaction_interval,
        rocksdb_max_compaction_jitter: config.rocksdb_max_compaction_jitter,
        accounts_hash_interval_slots: config.accounts_hash_interval_slots,
        max_genesis_archive_unpacked_size: config.max_genesis_archive_unpacked_size,
        wal_recovery_mode: config.wal_recovery_mode.clone(),
        poh_verify: config.poh_verify,
        cuda: config.cuda,
        require_tower: config.require_tower,
        tower_storage: config.tower_storage.clone(),
        debug_keys: config.debug_keys.clone(),
        contact_debug_interval: config.contact_debug_interval,
        contact_save_interval: config.contact_save_interval,
        bpf_jit: config.bpf_jit,
        send_transaction_retry_ms: config.send_transaction_retry_ms,
        send_transaction_leader_forward_count: config.send_transaction_leader_forward_count,
        no_poh_speed_test: config.no_poh_speed_test,
        poh_pinned_cpu_core: config.poh_pinned_cpu_core,
        account_indexes: config.account_indexes.clone(),
        accounts_db_caching_enabled: config.accounts_db_caching_enabled,
        warp_slot: config.warp_slot,
        accounts_db_test_hash_calculation: config.accounts_db_test_hash_calculation,
        accounts_db_use_index_hash_calculation: config.accounts_db_use_index_hash_calculation,
        tpu_coalesce_ms: config.tpu_coalesce_ms,
        validator_exit: Arc::new(RwLock::new(Exit::default())),
        poh_hashes_per_batch: config.poh_hashes_per_batch,
        no_wait_for_vote_to_start_leader: config.no_wait_for_vote_to_start_leader,
        accounts_shrink_ratio: config.accounts_shrink_ratio,
    }
}

pub fn make_identical_validator_configs(
    config: &ValidatorConfig,
    num: usize,
) -> Vec<ValidatorConfig> {
    let mut configs = vec![];
    for _ in 0..num {
        configs.push(safe_clone_config(config));
    }
    configs
}
