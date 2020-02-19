use crate::{
    bank_forks::SnapshotConfig,
    blockstore::Blockstore,
    blockstore_processor::{self, BlockstoreProcessorResult, ProcessOptions},
    entry::VerifyRecyclers,
    snapshot_utils,
};
use log::*;
use solana_runtime::bank::Bank;
use solana_sdk::genesis_config::GenesisConfig;
use std::{fs, path::PathBuf, sync::Arc};

pub fn bank_from_snapshot(
    snapshot_config: Option<&SnapshotConfig>,
    account_paths: &[PathBuf],
) -> Option<Bank> {
    if let Some(snapshot_config) = snapshot_config.as_ref() {
        info!(
            "Initializing snapshot path: {:?}",
            snapshot_config.snapshot_path
        );
        let _ = fs::remove_dir_all(&snapshot_config.snapshot_path);
        fs::create_dir_all(&snapshot_config.snapshot_path)
            .expect("Couldn't create snapshot directory");

        let tar = snapshot_utils::get_snapshot_archive_path(
            &snapshot_config.snapshot_package_output_path,
        );
        if tar.exists() {
            info!("Loading snapshot package: {:?}", tar);
            // Fail hard here if snapshot fails to load, don't silently continue

            if account_paths.is_empty() {
                panic!("Account paths not present when booting from snapshot")
            }

            let deserialized_bank = snapshot_utils::bank_from_archive(
                account_paths,
                &snapshot_config.snapshot_path,
                &tar,
            )
            .expect("Load from snapshot failed");

            if let Some(ref snapshot_info) = snapshot_config.expected_snapshot_info {
                if snapshot_info.slot != deserialized_bank.slot() {
                    panic!(
                        "Snapshot bank slot mismatch: expected={} actual={}",
                        snapshot_info.slot,
                        deserialized_bank.slot()
                    );
                }
                if snapshot_info.bank_hash != deserialized_bank.hash() {
                    panic!(
                        "Snapshot bank bank_hash mismatch: expected={} actual={}",
                        snapshot_info.bank_hash,
                        deserialized_bank.hash()
                    );
                }
            }
            Some(deserialized_bank)
        } else {
            info!("Snapshot package does not exist: {:?}", tar);
            None
        }
    } else {
        info!("Snapshots disabled");
        None
    }
}

pub fn load_ledger(
    genesis_config: &GenesisConfig,
    blockstore: &Blockstore,
    account_paths: Vec<PathBuf>,
    bank: Option<Bank>,
    process_options: ProcessOptions,
) -> BlockstoreProcessorResult {
    if let Some(bank) = bank {
        info!("Processing ledger from slot {}", bank.slot());
        blockstore_processor::process_blockstore_from_root(
            genesis_config,
            blockstore,
            Arc::new(bank),
            &process_options,
            &VerifyRecyclers::default(),
        )
    } else {
        info!("Processing ledger from genesis");
        blockstore_processor::process_blockstore(
            &genesis_config,
            &blockstore,
            account_paths,
            process_options,
        )
    }
}

pub fn load(
    genesis_config: &GenesisConfig,
    blockstore: &Blockstore,
    account_paths: Vec<PathBuf>,
    snapshot_config: Option<&SnapshotConfig>,
    process_options: ProcessOptions,
) -> BlockstoreProcessorResult {
    let bank = bank_from_snapshot(snapshot_config, &account_paths);
    load_ledger(
        genesis_config,
        blockstore,
        account_paths,
        bank,
        process_options,
    )
}
