//! The `ledger_cleanup_service` drops older ledger data to limit disk space usage

use rand::{thread_rng, Rng};
use solana_ledger::blockstore::{Blockstore, PurgeType};
use solana_ledger::blockstore_db::Result as BlockstoreResult;
use solana_measure::measure::Measure;
use solana_sdk::clock::{Slot, DEFAULT_TICKS_PER_SLOT, TICKS_PER_DAY};
use std::string::ToString;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::thread;
use std::thread::{sleep, Builder, JoinHandle};
use std::time::Duration;

// Notes for calculations below:
//  Using 2000b / shred (seeing about 1600b / shred; use 2000b for margin)
//  Using 5k shreds / slot for 50k tps load
//  Using 50 shreds / slot for idle load
//  Using 400ms / slot as noted by constant SLOT_MS

// Cap at 200m shreds, this is chosen to allow enough time to
// - Ensure a validator can download a snapshot from a peer and boot from it
// - Ensure that if a validator needs to reboot from its own snapshot, it has enough
//   slots locally to catch back up to where it was when it stopped
// 200m shred max gives ...
//   ~400gb max ledger size
//   200m / 5k = 40k slots (nearly 4.5 hours) at 50k tps load
//   200m / 60 = 3.333m slots (15.43 days) at idle load
pub const DEFAULT_MAX_LEDGER_SHREDS: u64 = 200_000_000;

// Allow down to 50m shreds; 50m shred max gives ...
//   ~100gb max ledger size
//   50m / 5k = 10k slots (just over 1 hour) at 50k tps load
//   50m / 60 = 833k slots (3.85 days) at idle load
pub const DEFAULT_MIN_MAX_LEDGER_SHREDS: u64 = 50_000_000;

// Check for flushing shreds from cache to disk at this interval. We want recent
// shreds in cache for quick access for replay, retransmit, etc. But, we also need
// to bound the cache size to avoid eating too much RAM.
// - Shreds are at most 1228 bytes, use 1500 bytes for margin
// - 5k shreds/slot (50k TPS) * 1500 bytes = 7.5 MB / slot
// - 7.5 MB / slot * 128 slots = 960 MB cache size
pub const DEFAULT_FLUSH_SLOT_INTERVAL: u64 = 128;

// Check for removing slots at this interval so we don't purge too often
// and starve other blockstore users.
pub const DEFAULT_PURGE_SLOT_INTERVAL: u64 = 512;

// Compacting at a slower interval than purging helps keep IOPS down.
// Once a day should be ample
const DEFAULT_COMPACTION_SLOT_INTERVAL: u64 = TICKS_PER_DAY / DEFAULT_TICKS_PER_SLOT;

pub struct LedgerCleanupService {
    t_cleanup: JoinHandle<()>,
    t_compact: JoinHandle<()>,
}

impl LedgerCleanupService {
    pub fn new(
        new_root_receiver: Receiver<Slot>,
        blockstore: Arc<Blockstore>,
        max_ledger_shreds: u64,
        exit: &Arc<AtomicBool>,
        compaction_interval: Option<u64>,
        max_compaction_jitter: Option<u64>,
    ) -> Self {
        let exit = exit.clone();
        let mut last_flush_slot = 0;
        let mut last_purge_slot = 0;
        let mut last_compaction_slot = 0;
        let mut compaction_jitter = 0;
        let compaction_interval = compaction_interval.unwrap_or(DEFAULT_COMPACTION_SLOT_INTERVAL);
        let last_compact_slot = Arc::new(AtomicU64::new(0));
        let last_compact_slot2 = last_compact_slot.clone();

        info!(
            "LedgerCleanupService active. max ledger shreds={}, compaction interval={}",
            max_ledger_shreds, compaction_interval,
        );

        let exit_compact = exit.clone();
        let blockstore_compact = blockstore.clone();

        let t_cleanup = Builder::new()
            .name("sol-led-cleanup".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(e) = Self::cleanup_ledger(
                    &new_root_receiver,
                    &blockstore,
                    max_ledger_shreds,
                    &mut last_flush_slot,
                    DEFAULT_FLUSH_SLOT_INTERVAL,
                    &mut last_purge_slot,
                    DEFAULT_PURGE_SLOT_INTERVAL,
                    &last_compact_slot,
                ) {
                    match e {
                        RecvTimeoutError::Disconnected => break,
                        RecvTimeoutError::Timeout => (),
                    }
                }
            })
            .unwrap();

        let t_compact = Builder::new()
            .name("sol-led-compact".to_string())
            .spawn(move || loop {
                if exit_compact.load(Ordering::Relaxed) {
                    break;
                }
                Self::compact_ledger(
                    &blockstore_compact,
                    &mut last_compaction_slot,
                    compaction_interval,
                    &last_compact_slot2,
                    &mut compaction_jitter,
                    max_compaction_jitter,
                );
                sleep(Duration::from_secs(1));
            })
            .unwrap();

        Self {
            t_cleanup,
            t_compact,
        }
    }

    fn flush_slots(
        blockstore: &Arc<Blockstore>,
        root: Slot,
        last_flush_slot: &mut u64,
        flush_interval: u64,
    ) -> BlockstoreResult<()> {
        let slots_to_flush: Vec<Slot> = blockstore
            .data_shred_cache_slots
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .take_while(|slot| root > *slot + flush_interval)
            .collect();
        if slots_to_flush.is_empty() {
            trace!("No slots found to flush");
            return Ok(());
        }
        info!(
            "flushing shreds from slot {} to {}",
            slots_to_flush.first().unwrap(),
            slots_to_flush.last().unwrap()
        );

        let mut flush_time = Measure::start("flush_time");
        for slot in slots_to_flush.iter() {
            blockstore.flush_data_shreds_for_slot_to_fs(*slot)?;
        }
        flush_time.stop();
        *last_flush_slot = root;
        info!(
            "flushed {} slots of shreds to disk, {}",
            slots_to_flush.len(),
            flush_time
        );
        Ok(())
    }

    fn find_slots_to_clean(
        blockstore: &Arc<Blockstore>,
        root: Slot,
        max_ledger_shreds: u64,
    ) -> (bool, Slot, Slot, u64) {
        let mut total_slots = Vec::new();
        let mut iterate_time = Measure::start("iterate_time");
        let mut total_shreds = 0;
        let mut first_slot = 0;
        for (i, (slot, meta)) in blockstore.slot_meta_iterator(0).unwrap().enumerate() {
            if i == 0 {
                first_slot = slot;
                debug!("purge: searching from slot: {}", slot);
            }
            // Not exact since non-full slots will have holes
            total_shreds += meta.received;
            total_slots.push((slot, meta.received));
            if slot > root {
                break;
            }
        }
        iterate_time.stop();
        info!(
            "first_slot={} total_slots={} total_shreds={} max_ledger_shreds={}, {}",
            first_slot,
            total_slots.len(),
            total_shreds,
            max_ledger_shreds,
            iterate_time
        );
        if (total_shreds as u64) < max_ledger_shreds {
            return (false, 0, 0, total_shreds);
        }
        let mut num_shreds_to_keep = 0;
        let mut lowest_cleanup_slot = total_slots[0].0;
        // Iterate through total_slots in reverse so we keep most recent slots
        for (slot, num_shreds) in total_slots.iter().rev() {
            num_shreds_to_keep += *num_shreds as u64;
            if num_shreds_to_keep > max_ledger_shreds {
                lowest_cleanup_slot = *slot;
                break;
            }
        }

        (true, first_slot, lowest_cleanup_slot, total_shreds)
    }

    fn receive_new_roots(new_root_receiver: &Receiver<Slot>) -> Result<Slot, RecvTimeoutError> {
        let mut root = new_root_receiver.recv_timeout(Duration::from_secs(1))?;
        // Empty out the channel and return the newest root
        while let Ok(new_root) = new_root_receiver.try_recv() {
            root = new_root;
        }
        Ok(root)
    }

    pub fn cleanup_ledger(
        new_root_receiver: &Receiver<Slot>,
        blockstore: &Arc<Blockstore>,
        max_ledger_shreds: u64,
        last_flush_slot: &mut u64,
        flush_interval: u64,
        last_purge_slot: &mut u64,
        purge_interval: u64,
        last_compact_slot: &Arc<AtomicU64>,
    ) -> Result<(), RecvTimeoutError> {
        let root = Self::receive_new_roots(new_root_receiver)?;

        // Flush interval is shorter than purge interval, so check this one first
        if root - *last_flush_slot > flush_interval {
            let _ = Self::flush_slots(&blockstore.clone(), root, last_flush_slot, flush_interval);
        }

        if root - *last_purge_slot <= purge_interval {
            return Ok(());
        }

        let disk_utilization_pre = blockstore.storage_size();
        info!(
            "purge: last_root={}, last_purge_slot={}, purge_interval={}, disk_utilization={:?}",
            root, last_purge_slot, purge_interval, disk_utilization_pre
        );

        *last_purge_slot = root;

        let (slots_to_clean, purge_first_slot, lowest_cleanup_slot, total_shreds) =
            Self::find_slots_to_clean(blockstore, root, max_ledger_shreds);

        if slots_to_clean {
            let purge_complete = Arc::new(AtomicBool::new(false));
            let blockstore = blockstore.clone();
            let purge_complete1 = purge_complete.clone();
            let last_compact_slot1 = last_compact_slot.clone();
            let _t_purge = Builder::new()
                .name("solana-ledger-purge".to_string())
                .spawn(move || {
                    let mut slot_update_time = Measure::start("slot_update");
                    *blockstore.lowest_cleanup_slot.write().unwrap() = lowest_cleanup_slot;
                    slot_update_time.stop();

                    info!(
                        "purging data from slots {} to {}",
                        purge_first_slot, lowest_cleanup_slot
                    );

                    let mut purge_time = Measure::start("purge_slots");

                    blockstore.purge_slots(
                        purge_first_slot,
                        lowest_cleanup_slot,
                        PurgeType::CompactionFilter,
                    );
                    // Update only after purge operation.
                    // Safety: This value can be used by compaction_filters shared via Arc<AtomicU64>.
                    // Compactions are async and run as a multi-threaded background job. However, this
                    // shouldn't cause consistency issues for iterators and getters because we have
                    // already expired all affected keys (older than or equal to lowest_cleanup_slot)
                    // by the above `purge_slots`. According to the general RocksDB design where SST
                    // files are immutable, even running iterators aren't affected; the database grabs
                    // a snapshot of the live set of sst files at iterator's creation.
                    // Also, we passed the PurgeType::CompactionFilter, meaning no delete_range for
                    // transaction_status and address_signatures CFs. These are fine because they
                    // don't require strong consistent view for their operation.
                    blockstore.set_max_expired_slot(lowest_cleanup_slot);

                    purge_time.stop();
                    info!("{}", purge_time);

                    last_compact_slot1.store(lowest_cleanup_slot, Ordering::Relaxed);

                    purge_complete1.store(true, Ordering::Relaxed);
                })
                .unwrap();

            // Keep pulling roots off `new_root_receiver` while purging to avoid channel buildup
            while !purge_complete.load(Ordering::Relaxed) {
                if let Err(err) = Self::receive_new_roots(new_root_receiver) {
                    debug!("receive_new_roots: {}", err);
                }
                thread::sleep(Duration::from_secs(1));
            }
        }

        let disk_utilization_post = blockstore.storage_size();
        Self::report_disk_metrics(disk_utilization_pre, disk_utilization_post, total_shreds);

        Ok(())
    }

    pub fn compact_ledger(
        blockstore: &Arc<Blockstore>,
        last_compaction_slot: &mut u64,
        compaction_interval: u64,
        highest_compact_slot: &Arc<AtomicU64>,
        compaction_jitter: &mut u64,
        max_jitter: Option<u64>,
    ) {
        let highest_compaction_slot = highest_compact_slot.load(Ordering::Relaxed);
        if highest_compaction_slot.saturating_sub(*last_compaction_slot)
            > (compaction_interval + *compaction_jitter)
        {
            info!(
                "compacting data from slots {} to {}",
                *last_compaction_slot, highest_compaction_slot,
            );
            if let Err(err) =
                blockstore.compact_storage(*last_compaction_slot, highest_compaction_slot)
            {
                // This error is not fatal and indicates an internal error?
                error!(
                    "Error: {:?}; Couldn't compact storage from {:?} to {:?}",
                    err, last_compaction_slot, highest_compaction_slot,
                );
            }
            *last_compaction_slot = highest_compaction_slot;
            let jitter = max_jitter.unwrap_or(0);
            if jitter > 0 {
                *compaction_jitter = thread_rng().gen_range(0, jitter);
            }
        }
    }

    fn report_disk_metrics(
        pre: BlockstoreResult<u64>,
        post: BlockstoreResult<u64>,
        total_shreds: u64,
    ) {
        if let (Ok(pre), Ok(post)) = (pre, post) {
            datapoint_info!(
                "ledger_disk_utilization",
                ("disk_utilization_pre", pre as i64, i64),
                ("disk_utilization_post", post as i64, i64),
                ("disk_utilization_delta", (pre as i64 - post as i64), i64),
                ("total_shreds", total_shreds, i64),
            );
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_cleanup.join()?;
        self.t_compact.join()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use solana_ledger::blockstore::make_many_slot_entries;
    use solana_ledger::get_tmp_ledger_path;
    use std::sync::mpsc::channel;

    #[test]
    fn test_cleanup1() {
        solana_logger::setup();
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();
        let (shreds, _) = make_many_slot_entries(0, 50, 5);
        blockstore.insert_shreds(shreds, None, false).unwrap();
        let blockstore = Arc::new(blockstore);
        let (sender, receiver) = channel();

        //send a signal to kill all but 5 shreds, which will be in the newest slots
        let mut last_flush_slot = 0;
        let mut last_purge_slot = 0;
        let highest_compaction_slot = Arc::new(AtomicU64::new(0));
        sender.send(50).unwrap();
        LedgerCleanupService::cleanup_ledger(
            &receiver,
            &blockstore,
            5,
            &mut last_flush_slot,
            20, // Flush interval > purge interval so we don't flush
            &mut last_purge_slot,
            10,
            &highest_compaction_slot,
        )
        .unwrap();
        assert_eq!(last_purge_slot, 50);
        assert_eq!(highest_compaction_slot.load(Ordering::Relaxed), 44);

        //check that 0-40 don't exist
        blockstore
            .slot_meta_iterator(0)
            .unwrap()
            .for_each(|(slot, _)| assert!(slot > 40));

        let mut last_compaction_slot = 0;
        let mut jitter = 0;
        LedgerCleanupService::compact_ledger(
            &blockstore,
            &mut last_compaction_slot,
            10,
            &highest_compaction_slot,
            &mut jitter,
            None,
        );
        assert_eq!(jitter, 0);

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_cleanup_speed() {
        solana_logger::setup();
        let blockstore_path = get_tmp_ledger_path!();
        let mut blockstore = Blockstore::open(&blockstore_path).unwrap();
        blockstore.set_no_compaction(true);
        let blockstore = Arc::new(blockstore);
        let (sender, receiver) = channel();

        let mut first_insert = Measure::start("first_insert");
        let initial_slots = 50;
        let initial_entries = 5;
        let (shreds, _) = make_many_slot_entries(0, initial_slots, initial_entries);
        blockstore.insert_shreds(shreds, None, false).unwrap();
        first_insert.stop();
        info!("{}", first_insert);

        let mut last_flush_slot = 0;
        let mut last_purge_slot = 0;
        let last_compaction_slot = Arc::new(AtomicU64::new(0));
        let mut slot = initial_slots;
        let mut num_slots = 6;
        for _ in 0..5 {
            let mut insert_time = Measure::start("insert time");
            let batch_size = 2;
            let batches = num_slots / batch_size;
            for i in 0..batches {
                let (shreds, _) = make_many_slot_entries(slot + i * batch_size, batch_size, 5);
                blockstore.insert_shreds(shreds, None, false).unwrap();
                if i % 100 == 0 {
                    info!("inserting..{} of {}", i, batches);
                }
            }
            insert_time.stop();

            let mut time = Measure::start("purge time");
            sender.send(slot + num_slots).unwrap();
            LedgerCleanupService::cleanup_ledger(
                &receiver,
                &blockstore,
                initial_slots,
                &mut last_flush_slot,
                20, // Flush interval > purge interval so we don't flush
                &mut last_purge_slot,
                10,
                &last_compaction_slot,
            )
            .unwrap();
            time.stop();
            info!(
                "slot: {} size: {} {} {}",
                slot, num_slots, insert_time, time
            );
            slot += num_slots;
            num_slots *= 2;
        }

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }
}
