use log::*;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use solana_runtime::{
    accounts_db::{AccountsDb, LoadHint},
    ancestors::Ancestors,
};
use solana_sdk::genesis_config::ClusterType;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount, WritableAccount},
    clock::Slot,
    pubkey::Pubkey,
};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[test]
fn test_shrink_and_clean() {
    solana_logger::setup();

    // repeat the whole test scenario
    for _ in 0..5 {
        let accounts = Arc::new(AccountsDb::new_single());
        let accounts_for_shrink = accounts.clone();

        // spawn the slot shrinking background thread
        let exit = Arc::new(AtomicBool::default());
        let exit_for_shrink = exit.clone();
        let shrink_thread = std::thread::spawn(move || loop {
            if exit_for_shrink.load(Ordering::Relaxed) {
                break;
            }
            accounts_for_shrink.process_stale_slot_v1();
        });

        let mut alive_accounts = vec![];
        let owner = Pubkey::default();

        // populate the AccountsDb with plenty of food for slot shrinking
        // also this simulates realistic some heavy spike account updates in the wild
        for current_slot in 0..100 {
            while alive_accounts.len() <= 10 {
                alive_accounts.push((
                    solana_sdk::pubkey::new_rand(),
                    AccountSharedData::new(thread_rng().gen_range(0, 50), 0, &owner),
                ));
            }

            alive_accounts.retain(|(_pubkey, account)| account.lamports() >= 1);

            for (pubkey, account) in alive_accounts.iter_mut() {
                account.checked_sub_lamports(1).unwrap();
                accounts.store_uncached(current_slot, &[(pubkey, account)]);
            }
            accounts.add_root(current_slot);
        }

        // let's dance.
        for _ in 0..10 {
            accounts.clean_accounts(None, false, None);
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        // cleanup
        exit.store(true, Ordering::Relaxed);
        shrink_thread.join().unwrap();
    }
}

#[test]
fn test_bad_bank_hash() {
    solana_logger::setup();
    use solana_sdk::signature::{Keypair, Signer};
    let db = AccountsDb::new(Vec::new(), &ClusterType::Development);

    let some_slot: Slot = 0;
    let ancestors = Ancestors::from(vec![some_slot]);

    let max_accounts = 200;
    let mut accounts_keys: Vec<_> = (0..max_accounts)
        .into_par_iter()
        .map(|_| {
            let key = Keypair::new().pubkey();
            let lamports = thread_rng().gen_range(0, 100);
            let some_data_len = thread_rng().gen_range(0, 1000);
            let account = AccountSharedData::new(lamports, some_data_len, &key);
            (key, account)
        })
        .collect();

    let mut existing = HashSet::new();
    let mut last_print = Instant::now();
    for i in 0..5_000 {
        if last_print.elapsed().as_millis() > 5000 {
            info!("i: {}", i);
            last_print = Instant::now();
        }
        let num_accounts = thread_rng().gen_range(0, 100);
        (0..num_accounts).into_iter().for_each(|_| {
            let mut idx;
            loop {
                idx = thread_rng().gen_range(0, max_accounts);
                if existing.contains(&idx) {
                    continue;
                }
                existing.insert(idx);
                break;
            }
            accounts_keys[idx]
                .1
                .set_lamports(thread_rng().gen_range(0, 1000));
        });

        let account_refs: Vec<_> = existing
            .iter()
            .map(|idx| (&accounts_keys[*idx].0, &accounts_keys[*idx].1))
            .collect();
        db.store_uncached(some_slot, &account_refs);

        for (key, account) in &account_refs {
            assert_eq!(
                db.load_account_hash(&ancestors, key, None, LoadHint::Unspecified)
                    .unwrap(),
                AccountsDb::hash_account(some_slot, *account, key)
            );
        }
        existing.clear();
    }
}
