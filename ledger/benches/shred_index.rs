#![feature(test)]

extern crate test;
use rand::seq::SliceRandom;
use solana_ledger::blockstore_meta::ShredIndex2;
use test::Bencher;

fn get_shuffled_vector(size: usize) -> Vec<u64> {
    let mut values: Vec<u64> = Vec::with_capacity(size);
    for i in 0..size {
        values.push(i as u64);
    }
    values.shuffle(&mut rand::thread_rng());
    values
}

#[bench]
fn bench_shred_index_is_present(bencher: &mut Bencher) {
    let values = get_shuffled_vector(500);
    bencher.iter(|| {
        let index = ShredIndex2::default();
        for value in values.iter() {
            index.is_present(*value);
        }
    })
}

#[bench]
fn bench_shred_index_set_present(bencher: &mut Bencher) {
    let values = get_shuffled_vector(500);
    bencher.iter(|| {
        let mut index = ShredIndex2::default();
        for value in values.iter() {
            index.set_present(*value, true);
        }
    })
}
