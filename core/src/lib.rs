#![cfg_attr(RUSTC_WITH_SPECIALIZATION, feature(min_specialization))]
#![allow(clippy::integer_arithmetic)]
//! The `solana` library implements the Solana high-performance blockchain architecture.
//! It includes a full Rust implementation of the architecture (see
//! [Validator](server/struct.Validator.html)) as well as hooks to GPU implementations of its most
//! paralellizable components (i.e. [SigVerify](sigverify/index.html)).  It also includes
//! command-line tools to spin up validators and a Rust library
//!

pub mod accounts_hash_verifier;
pub mod ancestor_hashes_service;
pub mod banking_stage;
pub mod broadcast_stage;
pub mod cache_block_meta_service;
pub mod cluster_info_vote_listener;
pub mod cluster_nodes;
pub mod cluster_slot_state_verifier;
pub mod cluster_slots;
pub mod cluster_slots_service;
pub mod commitment_service;
pub mod completed_data_sets_service;
pub mod consensus;
pub mod cost_model;
pub mod cost_tracker;
pub mod cost_update_service;
pub mod duplicate_repair_status;
pub mod execute_cost_table;
pub mod fetch_stage;
pub mod fork_choice;
pub mod gen_keys;
pub mod heaviest_subtree_fork_choice;
pub mod latest_validator_votes_for_frozen_banks;
pub mod ledger_cleanup_service;
pub mod optimistic_confirmation_verifier;
pub mod outstanding_requests;
pub mod packet_hasher;
pub mod progress_map;
pub mod repair_response;
pub mod repair_service;
pub mod repair_weight;
pub mod repair_weighted_traversal;
pub mod replay_stage;
pub mod request_response;
mod result;
pub mod retransmit_stage;
pub mod rewards_recorder_service;
pub mod sample_performance_service;
pub mod serve_repair;
pub mod serve_repair_service;
pub mod shred_fetch_stage;
pub mod sigverify;
pub mod sigverify_shreds;
pub mod sigverify_stage;
pub mod snapshot_packager_service;
pub mod test_validator;
pub mod tpu;
pub mod tree_diff;
pub mod tvu;
pub mod unfrozen_gossip_verified_vote_hashes;
pub mod validator;
pub mod verified_vote_packets;
pub mod vote_simulator;
pub mod vote_stake_tracker;
pub mod voting_service;
pub mod window_service;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate solana_metrics;

#[macro_use]
extern crate solana_frozen_abi_macro;

#[cfg(test)]
#[macro_use]
extern crate matches;
