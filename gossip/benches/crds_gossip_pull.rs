#![feature(test)]

extern crate test;

use {
    rand::{thread_rng, Rng},
    rayon::ThreadPoolBuilder,
    solana_gossip::{
        cluster_info::MAX_BLOOM_SIZE,
        crds::Crds,
        crds_gossip_pull::{CrdsFilter, CrdsGossipPull},
        crds_value::CrdsValue,
        sync::RwLock,
    },
    solana_sdk::hash,
    test::Bencher,
};

#[bench]
fn bench_hash_as_u64(bencher: &mut Bencher) {
    let mut rng = thread_rng();
    let hashes: Vec<_> = std::iter::repeat_with(|| hash::new_rand(&mut rng))
        .take(1000)
        .collect();
    bencher.iter(|| {
        hashes
            .iter()
            .map(CrdsFilter::hash_as_u64)
            .collect::<Vec<_>>()
    });
}

#[bench]
fn bench_build_crds_filters(bencher: &mut Bencher) {
    let thread_pool = ThreadPoolBuilder::new().build().unwrap();
    let mut rng = thread_rng();
    let crds_gossip_pull = CrdsGossipPull::default();
    let mut crds = Crds::default();
    let mut num_inserts = 0;
    for _ in 0..90_000 {
        if crds
            .insert(CrdsValue::new_rand(&mut rng, None), rng.gen())
            .is_ok()
        {
            num_inserts += 1;
        }
    }
    assert_eq!(num_inserts, 90_000);
    let crds = RwLock::new(crds);
    bencher.iter(|| {
        let filters = crds_gossip_pull.build_crds_filters(&thread_pool, &crds, MAX_BLOOM_SIZE);
        assert_eq!(filters.len(), 128);
    });
}
