use crate::consensus::{SavedTower, TowerStorage};
use solana_gossip::cluster_info::ClusterInfo;
use solana_measure::measure::Measure;
use solana_poh::poh_recorder::PohRecorder;
use solana_sdk::{clock::Slot, transaction::Transaction};
use std::{
    sync::{mpsc::Receiver, Arc, Mutex},
    thread::{self, Builder, JoinHandle},
};

pub enum VoteOp {
    PushVote {
        tx: Transaction,
        tower_slots: Vec<Slot>,
        saved_tower: SavedTower,
    },
    RefreshVote {
        tx: Transaction,
        last_voted_slot: Slot,
    },
}

impl VoteOp {
    fn tx(&self) -> &Transaction {
        match self {
            VoteOp::PushVote { tx, .. } => tx,
            VoteOp::RefreshVote { tx, .. } => tx,
        }
    }
}

pub struct VotingService {
    thread_hdl: JoinHandle<()>,
}

impl VotingService {
    pub fn new(
        vote_receiver: Receiver<VoteOp>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<Mutex<PohRecorder>>,
        tower_storage: Arc<dyn TowerStorage>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("sol-vote-service".to_string())
            .spawn(move || {
                for vote_op in vote_receiver.iter() {
                    Self::handle_vote(
                        &cluster_info,
                        &poh_recorder,
                        tower_storage.as_ref(),
                        vote_op,
                    );
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    pub fn handle_vote(
        cluster_info: &ClusterInfo,
        poh_recorder: &Mutex<PohRecorder>,
        tower_storage: &dyn TowerStorage,
        vote_op: VoteOp,
    ) {
        if let VoteOp::PushVote { saved_tower, .. } = &vote_op {
            let mut measure = Measure::start("tower_save-ms");
            if let Err(err) = tower_storage.store(saved_tower) {
                error!("Unable to save tower to storage: {:?}", err);
                std::process::exit(1);
            }
            measure.stop();
            inc_new_counter_info!("tower_save-ms", measure.as_ms() as usize);
        }

        let _ = cluster_info.send_transaction(
            vote_op.tx(),
            crate::banking_stage::next_leader_tpu(cluster_info, poh_recorder),
        );

        match vote_op {
            VoteOp::PushVote {
                tx, tower_slots, ..
            } => {
                cluster_info.push_vote(&tower_slots, tx);
            }
            VoteOp::RefreshVote {
                tx,
                last_voted_slot,
            } => {
                cluster_info.refresh_vote(tx, last_voted_slot);
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
