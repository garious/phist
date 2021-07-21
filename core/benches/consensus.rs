#![feature(test)]

extern crate solana_core;
extern crate test;

use solana_core::{
    consensus::{FileTowerStorage, Tower},
    vote_simulator::VoteSimulator,
};
use solana_runtime::bank::Bank;
use solana_runtime::bank_forks::BankForks;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tempfile::TempDir;
use test::Bencher;
use trees::tr;

#[bench]
fn bench_save_tower(bench: &mut Bencher) {
    let dir = TempDir::new().unwrap();

    let vote_account_pubkey = &Pubkey::default();
    let node_keypair = Arc::new(Keypair::new());
    let heaviest_bank = BankForks::new(Bank::default()).working_bank();
    let tower_storage = FileTowerStorage::new(dir.path().to_path_buf());
    let tower = Tower::new(
        &node_keypair.pubkey(),
        vote_account_pubkey,
        0,
        &heaviest_bank,
    );

    bench.iter(move || {
        tower.save(&tower_storage, &node_keypair).unwrap();
    });
}

#[bench]
#[ignore]
fn bench_generate_ancestors_descendants(bench: &mut Bencher) {
    let vote_account_pubkey = &Pubkey::default();
    let node_keypair = Arc::new(Keypair::new());
    let heaviest_bank = BankForks::new(Bank::default()).working_bank();
    let mut tower = Tower::new(
        &node_keypair.pubkey(),
        vote_account_pubkey,
        0,
        &heaviest_bank,
    );

    let num_banks = 500;
    let forks = tr(0);
    let mut vote_simulator = VoteSimulator::new(2);
    vote_simulator.fill_bank_forks(forks, &HashMap::new());
    vote_simulator.create_and_vote_new_branch(
        0,
        num_banks,
        &HashMap::new(),
        &HashSet::new(),
        &Pubkey::new_unique(),
        &mut tower,
    );

    bench.iter(move || {
        for _ in 0..num_banks {
            let _ancestors = vote_simulator.bank_forks.read().unwrap().ancestors();
            let _descendants = vote_simulator
                .bank_forks
                .read()
                .unwrap()
                .descendants()
                .clone();
        }
    });
}
