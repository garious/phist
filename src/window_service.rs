//! The `window_service` provides a thread for maintaining a window (tail of the ledger).
//!
use crate::cluster_info::ClusterInfo;
use crate::counter::Counter;
use crate::db_ledger::DbLedger;
use crate::db_window::*;

use crate::leader_scheduler::LeaderScheduler;
use crate::result::{Error, Result};
use crate::streamer::{BlobReceiver, BlobSender};
use log::Level;
use rand::{thread_rng, Rng};
use solana_metrics::{influxdb, submit};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::timing::duration_as_ms;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, RwLock};
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, Instant};

pub const MAX_REPAIR_BACKOFF: usize = 128;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WindowServiceReturnType {
    LeaderRotation(u64),
}

fn repair_backoff(last: &mut u64, times: &mut usize, consumed: u64) -> bool {
    //exponential backoff
    if *last != consumed {
        //start with a 50% chance of asking for repairs
        *times = 1;
    }
    *last = consumed;
    *times += 1;

    // Experiment with capping repair request duration.
    // Once nodes are too far behind they can spend many
    // seconds without asking for repair
    if *times > MAX_REPAIR_BACKOFF {
        // 50% chance that a request will fire between 64 - 128 tries
        *times = MAX_REPAIR_BACKOFF / 2;
    }

    //if we get lucky, make the request, which should exponentially get less likely
    thread_rng().gen_range(0, *times as u64) == 0
}

#[allow(clippy::too_many_arguments)]
fn recv_window(
    db_ledger: &Arc<DbLedger>,
    id: &Pubkey,
    leader_scheduler: &Arc<RwLock<LeaderScheduler>>,
    tick_height: &mut u64,
    max_ix: u64,
    r: &BlobReceiver,
    retransmit: &BlobSender,
    done: &Arc<AtomicBool>,
) -> Result<()> {
    let timer = Duration::from_millis(200);
    let mut dq = r.recv_timeout(timer)?;

    while let Ok(mut nq) = r.try_recv() {
        dq.append(&mut nq)
    }
    let now = Instant::now();
    inc_new_counter_info!("streamer-recv_window-recv", dq.len(), 100);

    submit(
        influxdb::Point::new("recv-window")
            .add_field("count", influxdb::Value::Integer(dq.len() as i64))
            .to_owned(),
    );

    retransmit_all_leader_blocks(&dq, leader_scheduler, retransmit, id)?;

    //send a contiguous set of blocks
    let mut consume_queue = Vec::new();

    trace!("{} num blobs received: {}", id, dq.len());

    for b in dq {
        let (pix, meta_size) = {
            let p = b.read().unwrap();
            (p.index(), p.meta.size)
        };

        trace!("{} window pix: {} size: {}", id, pix, meta_size);

        let _ = process_blob(
            leader_scheduler,
            db_ledger,
            &b,
            max_ix,
            &mut consume_queue,
            tick_height,
            done,
        );
    }

    trace!(
        "Elapsed processing time in recv_window(): {}",
        duration_as_ms(&now.elapsed())
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn window_service(
    db_ledger: Arc<DbLedger>,
    cluster_info: Arc<RwLock<ClusterInfo>>,
    tick_height: u64,
    entry_height: u64,
    max_entry_height: u64,
    r: BlobReceiver,
    retransmit: BlobSender,
    repair_socket: Arc<UdpSocket>,
    leader_scheduler: Arc<RwLock<LeaderScheduler>>,
    done: Arc<AtomicBool>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("solana-window".to_string())
        .spawn(move || {
            let mut tick_height_ = tick_height;
            let mut last = entry_height;
            let mut times = 0;
            let id = cluster_info.read().unwrap().id();
            trace!("{}: RECV_WINDOW started", id);
            loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(e) = recv_window(
                    &db_ledger,
                    &id,
                    &leader_scheduler,
                    &mut tick_height_,
                    max_entry_height,
                    &r,
                    &retransmit,
                    &done,
                ) {
                    match e {
                        Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
                        Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                        _ => {
                            inc_new_counter_info!("streamer-window-error", 1, 1);
                            error!("window error: {:?}", e);
                        }
                    }
                }

                let meta = db_ledger.meta();

                if let Ok(Some(meta)) = meta {
                    let received = meta.received;
                    let consumed = meta.consumed;

                    // Consumed should never be bigger than received
                    assert!(consumed <= received);
                    if received == consumed {
                        trace!(
                            "{} we have everything received: {} consumed: {}",
                            id,
                            received,
                            consumed
                        );
                        continue;
                    }

                    //exponential backoff
                    if !repair_backoff(&mut last, &mut times, consumed) {
                        trace!("{} !repair_backoff() times = {}", id, times);
                        continue;
                    }
                    trace!("{} let's repair! times = {}", id, times);

                    let reqs = repair(
                        &db_ledger,
                        &cluster_info,
                        &id,
                        times,
                        tick_height_,
                        max_entry_height,
                        &leader_scheduler,
                    );

                    if let Ok(reqs) = reqs {
                        for (to, req) in reqs {
                            repair_socket.send_to(&req, to).unwrap_or_else(|e| {
                                info!("{} repair req send_to({}) error {:?}", id, to, e);
                                0
                            });
                        }
                    }
                }
            }
        })
        .unwrap()
}

#[cfg(test)]
mod test {
    use crate::cluster_info::{ClusterInfo, Node};
    use crate::db_ledger::get_tmp_ledger_path;
    use crate::db_ledger::DbLedger;
    use crate::entry::make_consecutive_blobs;
    use crate::leader_scheduler::LeaderScheduler;

    use crate::streamer::{blob_receiver, responder};
    use crate::window_service::{repair_backoff, window_service};
    use solana_sdk::hash::Hash;
    use std::fs::remove_dir_all;
    use std::net::UdpSocket;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::{Arc, RwLock};
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    pub fn window_send_test() {
        solana_logger::setup();
        // setup a leader whose id is used to generates blobs and a validator
        // node whose window service will retransmit leader blobs.
        let leader_node = Node::new_localhost();
        let validator_node = Node::new_localhost();
        let exit = Arc::new(AtomicBool::new(false));
        let mut cluster_info_me = ClusterInfo::new(validator_node.info.clone());
        let me_id = leader_node.info.id;
        cluster_info_me.set_leader(me_id);
        let subs = Arc::new(RwLock::new(cluster_info_me));

        let (s_reader, r_reader) = channel();
        let t_receiver =
            blob_receiver(Arc::new(leader_node.sockets.gossip), exit.clone(), s_reader);
        let (s_retransmit, r_retransmit) = channel();
        let done = Arc::new(AtomicBool::new(false));
        let db_ledger_path = get_tmp_ledger_path("window_send_test");
        let db_ledger = Arc::new(
            DbLedger::open(&db_ledger_path).expect("Expected to be able to open database ledger"),
        );
        let mut leader_schedule = LeaderScheduler::default();
        leader_schedule.set_leader_schedule(vec![me_id]);
        let t_window = window_service(
            db_ledger,
            subs,
            0,
            0,
            0,
            r_reader,
            s_retransmit,
            Arc::new(leader_node.sockets.repair),
            Arc::new(RwLock::new(leader_schedule)),
            done,
            exit.clone(),
        );
        let t_responder = {
            let (s_responder, r_responder) = channel();
            let blob_sockets: Vec<Arc<UdpSocket>> =
                leader_node.sockets.tvu.into_iter().map(Arc::new).collect();

            let t_responder = responder("window_send_test", blob_sockets[0].clone(), r_responder);
            let num_blobs_to_make = 10;
            let gossip_address = &leader_node.info.gossip;
            let msgs = make_consecutive_blobs(
                &me_id,
                num_blobs_to_make,
                0,
                Hash::default(),
                &gossip_address,
            )
            .into_iter()
            .rev()
            .collect();;
            s_responder.send(msgs).expect("send");
            t_responder
        };

        let max_attempts = 10;
        let mut num_attempts = 0;
        loop {
            assert!(num_attempts != max_attempts);
            let mut q = r_retransmit.recv().unwrap();
            while let Ok(mut nq) = r_retransmit.try_recv() {
                q.append(&mut nq);
            }
            if q.len() != 10 {
                sleep(Duration::from_millis(100));
            } else {
                break;
            }
            num_attempts += 1;
        }

        exit.store(true, Ordering::Relaxed);
        t_receiver.join().expect("join");
        t_responder.join().expect("join");
        t_window.join().expect("join");
        DbLedger::destroy(&db_ledger_path).expect("Expected successful database destruction");
        let _ignored = remove_dir_all(&db_ledger_path);
    }

    #[test]
    pub fn window_send_leader_test2() {
        solana_logger::setup();
        // setup a leader whose id is used to generates blobs and a validator
        // node whose window service will retransmit leader blobs.
        let leader_node = Node::new_localhost();
        let validator_node = Node::new_localhost();
        let exit = Arc::new(AtomicBool::new(false));
        let cluster_info_me = ClusterInfo::new(validator_node.info.clone());
        let me_id = leader_node.info.id;
        let subs = Arc::new(RwLock::new(cluster_info_me));

        let (s_reader, r_reader) = channel();
        let t_receiver =
            blob_receiver(Arc::new(leader_node.sockets.gossip), exit.clone(), s_reader);
        let (s_retransmit, r_retransmit) = channel();
        let done = Arc::new(AtomicBool::new(false));
        let db_ledger_path = get_tmp_ledger_path("window_send_late_leader_test");
        let db_ledger = Arc::new(
            DbLedger::open(&db_ledger_path).expect("Expected to be able to open database ledger"),
        );
        let mut leader_schedule = LeaderScheduler::default();
        leader_schedule.set_leader_schedule(vec![me_id]);
        let t_window = window_service(
            db_ledger,
            subs.clone(),
            0,
            0,
            0,
            r_reader,
            s_retransmit,
            Arc::new(leader_node.sockets.repair),
            Arc::new(RwLock::new(leader_schedule)),
            done,
            exit.clone(),
        );
        let t_responder = {
            let (s_responder, r_responder) = channel();
            let blob_sockets: Vec<Arc<UdpSocket>> =
                leader_node.sockets.tvu.into_iter().map(Arc::new).collect();
            let t_responder = responder("window_send_test", blob_sockets[0].clone(), r_responder);
            let mut msgs = Vec::new();
            let blobs = make_consecutive_blobs(
                &me_id,
                14u64,
                0,
                Default::default(),
                &leader_node.info.gossip,
            );

            for v in 0..10 {
                let i = 9 - v;
                msgs.push(blobs[i].clone());
            }
            s_responder.send(msgs).expect("send");

            subs.write().unwrap().set_leader(me_id);
            let mut msgs1 = Vec::new();
            for v in 1..5 {
                let i = 9 + v;
                msgs1.push(blobs[i].clone());
            }
            s_responder.send(msgs1).expect("send");
            t_responder
        };
        let mut q = r_retransmit.recv().unwrap();
        while let Ok(mut nq) = r_retransmit.recv_timeout(Duration::from_millis(100)) {
            q.append(&mut nq);
        }
        assert!(q.len() > 10);
        exit.store(true, Ordering::Relaxed);
        t_receiver.join().expect("join");
        t_responder.join().expect("join");
        t_window.join().expect("join");
        DbLedger::destroy(&db_ledger_path).expect("Expected successful database destruction");
        let _ignored = remove_dir_all(&db_ledger_path);
    }

    #[test]
    pub fn test_repair_backoff() {
        let num_tests = 100;
        let res: usize = (0..num_tests)
            .map(|_| {
                let mut last = 0;
                let mut times = 0;
                let total: usize = (0..127)
                    .map(|x| {
                        let rv = repair_backoff(&mut last, &mut times, 1) as usize;
                        assert_eq!(times, x + 2);
                        rv
                    })
                    .sum();
                assert_eq!(times, 128);
                assert_eq!(last, 1);
                repair_backoff(&mut last, &mut times, 1);
                assert_eq!(times, 64);
                repair_backoff(&mut last, &mut times, 2);
                assert_eq!(times, 2);
                assert_eq!(last, 2);
                total
            })
            .sum();
        let avg = res / num_tests;
        assert!(avg >= 3);
        assert!(avg <= 5);
    }
}
