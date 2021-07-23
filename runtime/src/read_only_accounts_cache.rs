//! ReadOnlyAccountsCache used to store accounts, such as executable accounts,
//! which can be large, loaded many times, and rarely change.
use dashmap::{mapref::entry::Entry, DashMap};
//use mapref::entry::{Entry, OccupiedEntry, VacantEntry};
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    clock::Slot,
    pubkey::Pubkey,
};
use std::thread::{sleep, Builder, JoinHandle};
use std::time::Duration;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    time::Instant,
};

type ReadOnlyCacheKey = (Pubkey, Slot);
type LruEntry = (Instant, ReadOnlyCacheKey);

#[derive(Debug)]
pub struct ReadOnlyAccountCacheEntry {
    pub account: AccountSharedData,
    pub last_used: Arc<RwLock<Instant>>,
}

#[derive(Debug)]
pub struct ReadOnlyAccountsCache {
    cache: Arc<DashMap<ReadOnlyCacheKey, ReadOnlyAccountCacheEntry>>,
    max_data_size: usize,
    data_size: Arc<AtomicUsize>,
    hits: AtomicU64,
    misses: AtomicU64,
    per_account_size: usize,
    stop: Arc<AtomicBool>,
    background: Option<JoinHandle<()>>,
}

impl Drop for ReadOnlyAccountsCache {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(background) = self.background.take() {
            background.join().unwrap();
        }
    }
}

impl ReadOnlyAccountsCache {
    pub fn new(max_data_size: usize) -> Self {
        let mut result = Self::new_test(max_data_size);

        let bg = Self {
            max_data_size,
            cache: result.cache.clone(),
            data_size: result.data_size.clone(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            per_account_size: Self::per_account_size(),
            stop: result.stop.clone(),
            background: None,
        };

        result.background = Some(
            Builder::new()
                .name("solana-readonly-accounts-cache".to_string())
                .spawn(move || {
                    bg.bg_purge_lru_items(false);
                })
                .unwrap(),
        );

        result
    }

    fn new_test(max_data_size: usize) -> Self {
        Self {
            max_data_size,
            cache: Arc::new(DashMap::default()),
            data_size: Arc::new(AtomicUsize::new(0)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            per_account_size: Self::per_account_size(),
            stop: Arc::new(AtomicBool::new(false)),
            background: None,
        }
    }

    fn per_account_size() -> usize {
        // size_of(arc(x)) does not return the size of x, so we have to add the size of RwLock...
        std::mem::size_of::<ReadOnlyAccountCacheEntry>() + std::mem::size_of::<RwLock<Instant>>()
    }

    pub fn in_cache(&self, pubkey: &Pubkey, slot: Slot) -> bool {
        self.cache.contains_key(&(*pubkey, slot))
    }

    pub fn load(&self, pubkey: &Pubkey, slot: Slot) -> Option<AccountSharedData> {
        self.cache
            .get(&(*pubkey, slot))
            .map(|account_ref| {
                self.hits.fetch_add(1, Ordering::Relaxed);
                let value = account_ref.value();
                // remember last use
                let now = Instant::now();
                *value.last_used.write().unwrap() = now;
                value.account.clone()
            })
            .or_else(|| {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            })
    }

    fn account_size(&self, account: &AccountSharedData) -> usize {
        account.data().len() + self.per_account_size
    }

    pub fn store(&self, pubkey: &Pubkey, slot: Slot, account: &AccountSharedData) {
        let len = self.account_size(account);
        let previous_len = if let Some(previous) = self.cache.insert(
            (*pubkey, slot),
            ReadOnlyAccountCacheEntry {
                account: account.clone(),
                last_used: Arc::new(RwLock::new(Instant::now())),
            },
        ) {
            self.account_size(&previous.account)
        } else {
            0
        };

        match len.cmp(&previous_len) {
            std::cmp::Ordering::Greater => {
                self.data_size
                    .fetch_add(len - previous_len, Ordering::Relaxed);
            }
            std::cmp::Ordering::Less => {
                self.data_size
                    .fetch_sub(previous_len - len, Ordering::Relaxed);
            }
            std::cmp::Ordering::Equal => {
                // no change in size
            }
        };
    }

    pub fn remove(&self, pubkey: &Pubkey, slot: Slot) {
        if let Some((_, value)) = self.cache.remove(&(*pubkey, slot)) {
            self.data_size
                .fetch_sub(self.account_size(&value.account), Ordering::Relaxed);
        }
    }

    fn purge_lru_list(&self, lru: &[LruEntry], lru_index: &mut usize) -> bool {
        let mut freed_bytes = 0;
        let start = *lru_index;
        let mut done = false;
        let current_size = self.data_size.load(Ordering::Relaxed);
        for (timestamp, key) in lru.iter().skip(start) {
            if current_size.saturating_sub(freed_bytes) <= self.max_data_size {
                done = true;
                break;
            }
            *lru_index += 1;
            match self.cache.entry(*key) {
                Entry::Vacant(_entry) => (),
                Entry::Occupied(entry) => {
                    if *timestamp == *entry.get().last_used.read().unwrap() {
                        let size = self.account_size(&entry.get().account);
                        freed_bytes += size;
                        entry.remove();
                    }
                }
            }
        }
        if freed_bytes > 0 {
            // if this overflows, we'll have a really big data size, so we'll clean everything, scan all, and reset the size. Not ideal, but not terrible.
            self.data_size.fetch_sub(freed_bytes, Ordering::Relaxed);
        }
        done
    }

    fn calculate_lru_list(&self, lru: &mut Vec<LruEntry>) -> usize {
        lru.clear();
        lru.reserve(self.cache.len());
        let mut new_size = 0;
        for item in self.cache.iter() {
            let value = item.value();
            let item_len = self.account_size(&value.account);
            new_size += item_len;
            lru.push((*value.last_used.read().unwrap(), *item.key()));
        }
        new_size
    }

    fn bg_purge_lru_items(&self, once: bool) {
        let mut lru = Vec::new();
        let mut lru_index = 0;
        let mut stop = false;
        loop {
            if !once {
                sleep(Duration::from_millis(200));
            } else {
                if stop {
                    break;
                }
                stop = true;
            }

            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            // purge from the lru list we last made
            if self.purge_lru_list(&lru, &mut lru_index) {
                continue;
            }

            // we didn't get enough, so calculate a new list and keep purging
            let new_size = self.calculate_lru_list(&mut lru);
            lru_index = 0;
            self.data_size.store(new_size, Ordering::Relaxed);
            lru.sort();
            self.purge_lru_list(&lru, &mut lru_index);
        }
    }

    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    pub fn data_size(&self) -> usize {
        self.data_size.load(Ordering::Relaxed)
    }

    pub fn get_and_reset_stats(&self) -> (u64, u64) {
        let hits = self.hits.swap(0, Ordering::Relaxed);
        let misses = self.misses.swap(0, Ordering::Relaxed);
        (hits, misses)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use solana_sdk::account::{accounts_equal, Account, WritableAccount};
    #[test]
    fn test_accountsdb_sizeof() {
        // size_of(arc(x)) does not return the size of x
        assert!(std::mem::size_of::<Arc<u64>>() == std::mem::size_of::<Arc<u8>>());
        assert!(std::mem::size_of::<Arc<u64>>() == std::mem::size_of::<Arc<[u8; 32]>>());
    }

    #[test]
    fn test_read_only_accounts_cache_drop() {
        solana_logger::setup();
        let cache = ReadOnlyAccountsCache::new_test(100);
        let stop = cache.stop.clone();
        drop(cache);
        assert!(stop.load(Ordering::Relaxed));
    }

    #[test]
    fn test_read_only_accounts_cache() {
        solana_logger::setup();
        let per_account_size = ReadOnlyAccountsCache::per_account_size();
        let data_size = 100;
        let max = data_size + per_account_size;
        let cache = ReadOnlyAccountsCache::new_test(max);
        let slot = 0;
        assert!(cache.load(&Pubkey::default(), slot).is_none());
        assert_eq!(0, cache.cache_len());
        assert_eq!(0, cache.data_size());
        cache.remove(&Pubkey::default(), slot); // assert no panic
        let key1 = Pubkey::new_unique();
        let key2 = Pubkey::new_unique();
        let key3 = Pubkey::new_unique();
        let account1 = AccountSharedData::from(Account {
            data: vec![0; data_size],
            ..Account::default()
        });
        let mut account2 = account1.clone();
        account2.checked_add_lamports(1).unwrap(); // so they compare differently
        let mut account3 = account1.clone();
        account3.checked_add_lamports(4).unwrap(); // so they compare differently
        cache.store(&key1, slot, &account1);
        assert_eq!(100 + per_account_size, cache.data_size());
        assert!(accounts_equal(&cache.load(&key1, slot).unwrap(), &account1));
        assert_eq!(1, cache.cache_len());
        cache.store(&key2, slot, &account2);
        cache.bg_purge_lru_items(true);
        assert_eq!(100 + per_account_size, cache.data_size());
        assert!(accounts_equal(&cache.load(&key2, slot).unwrap(), &account2));
        assert_eq!(1, cache.cache_len());
        cache.store(&key2, slot, &account1); // overwrite key2 with account1
        assert_eq!(100 + per_account_size, cache.data_size());
        assert!(accounts_equal(&cache.load(&key2, slot).unwrap(), &account1));
        assert_eq!(1, cache.cache_len());
        cache.remove(&key2, slot);
        assert_eq!(0, cache.data_size());
        assert_eq!(0, cache.cache_len());

        // can store 2 items, 3rd item kicks oldest item out
        let max = (data_size + per_account_size) * 2;
        let cache = ReadOnlyAccountsCache::new_test(max);
        cache.store(&key1, slot, &account1);
        assert_eq!(100 + per_account_size, cache.data_size());
        assert!(accounts_equal(&cache.load(&key1, slot).unwrap(), &account1));
        assert_eq!(1, cache.cache_len());
        cache.store(&key2, slot, &account2);
        assert_eq!(max, cache.data_size());
        assert!(accounts_equal(&cache.load(&key1, slot).unwrap(), &account1));
        assert!(accounts_equal(&cache.load(&key2, slot).unwrap(), &account2));
        assert_eq!(2, cache.cache_len());
        cache.store(&key2, slot, &account1); // overwrite key2 with account1
        assert_eq!(max, cache.data_size());
        assert!(accounts_equal(&cache.load(&key1, slot).unwrap(), &account1));
        assert!(accounts_equal(&cache.load(&key2, slot).unwrap(), &account1));
        assert_eq!(2, cache.cache_len());
        cache.store(&key3, slot, &account3);
        cache.bg_purge_lru_items(true);
        assert_eq!(max, cache.data_size());
        assert!(cache.load(&key1, slot).is_none()); // was lru purged
        assert!(accounts_equal(&cache.load(&key2, slot).unwrap(), &account1));
        assert!(accounts_equal(&cache.load(&key3, slot).unwrap(), &account3));
        assert_eq!(2, cache.cache_len());
    }
}
