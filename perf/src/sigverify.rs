//! The `sigverify` module provides digital signature verification functions.
//! By default, signatures are verified in parallel using all available CPU
//! cores.  When perf-libs are available signature verification is offloaded
//! to the GPU.
//!

use crate::cuda_runtime::PinnedVec;
use crate::packet::{Packet, Packets};
use crate::perf_libs;
use crate::recycler::Recycler;
use rayon::ThreadPool;
use solana_metrics::inc_new_counter_debug;
use solana_rayon_threadlimit::get_thread_count;
use solana_sdk::message::MESSAGE_HEADER_LENGTH;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::short_vec::decode_shortu16_len;
use solana_sdk::signature::Signature;
#[cfg(test)]
use solana_sdk::transaction::Transaction;
use std::convert::TryFrom;
use std::mem::size_of;

// Representing key tKeYE4wtowRb8yRroZShTipE18YVnqwXjsSAoNsFU6g
const TRACER_KEY_BYTES: [u8; 32] = [
    13, 37, 180, 170, 252, 137, 36, 194, 183, 143, 161, 193, 201, 207, 211, 23, 189, 93, 33, 110,
    155, 90, 30, 39, 116, 115, 238, 38, 126, 21, 232, 133,
];
const TRACER_KEY: Pubkey = Pubkey::new_from_array(TRACER_KEY_BYTES);

lazy_static! {
    static ref PAR_THREAD_POOL: ThreadPool = rayon::ThreadPoolBuilder::new()
        .num_threads(get_thread_count())
        .thread_name(|ix| format!("sigverify_{}", ix))
        .build()
        .unwrap();
}

pub type TxOffset = PinnedVec<u32>;

type TxOffsets = (TxOffset, TxOffset, TxOffset, TxOffset, Vec<Vec<u32>>);

#[derive(Debug, PartialEq, Eq)]
struct PacketOffsets {
    pub sig_len: u32,
    pub sig_start: u32,
    pub msg_start: u32,
    pub pubkey_start: u32,
}

impl PacketOffsets {
    pub fn new(sig_len: u32, sig_start: u32, msg_start: u32, pubkey_start: u32) -> Self {
        Self {
            sig_len,
            sig_start,
            msg_start,
            pubkey_start,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum PacketError {
    InvalidLen,
    InvalidPubkeyLen,
    InvalidShortVec,
    InvalidSignatureLen,
    MismatchSignatureLen,
    PayerNotWritable,
}

impl std::convert::From<std::boxed::Box<bincode::ErrorKind>> for PacketError {
    fn from(_e: std::boxed::Box<bincode::ErrorKind>) -> PacketError {
        PacketError::InvalidShortVec
    }
}

impl std::convert::From<std::num::TryFromIntError> for PacketError {
    fn from(_e: std::num::TryFromIntError) -> Self {
        Self::InvalidLen
    }
}

pub fn init() {
    if let Some(api) = perf_libs::api() {
        unsafe {
            (api.ed25519_set_verbose)(true);
            if !(api.ed25519_init)() {
                panic!("ed25519_init() failed");
            }
            (api.ed25519_set_verbose)(false);
        }
    }
}

fn verify_packet(packet: &mut Packet) {
    let packet_offsets = get_packet_offsets(packet, 0);
    let mut sig_start = packet_offsets.sig_start as usize;
    let mut pubkey_start = packet_offsets.pubkey_start as usize;
    let msg_start = packet_offsets.msg_start as usize;

    // If this packet was already marked as discard, drop it
    if packet.meta.discard {
        return;
    }

    if packet_offsets.sig_len == 0 {
        packet.meta.discard = true;
        return;
    }

    if packet.meta.size <= msg_start {
        packet.meta.discard = true;
        return;
    }

    let msg_end = packet.meta.size;
    for _ in 0..packet_offsets.sig_len {
        let pubkey_end = pubkey_start.saturating_add(size_of::<Pubkey>());
        let sig_end = sig_start.saturating_add(size_of::<Signature>());

        // get_packet_offsets should ensure pubkey_end and sig_end do
        // not overflow packet.meta.size

        let signature = Signature::new(&packet.data[sig_start..sig_end]);

        if !signature.verify(
            &packet.data[pubkey_start..pubkey_end],
            &packet.data[msg_start..msg_end],
        ) {
            packet.meta.discard = true;
            return;
        }

        // Check for tracer pubkey
        if !packet.meta.is_tracer_tx
            && &packet.data[pubkey_start..pubkey_end] == TRACER_KEY.as_ref()
        {
            packet.meta.is_tracer_tx = true;
        }

        pubkey_start = pubkey_end;
        sig_start = sig_end;
    }
}

pub fn batch_size(batches: &[Packets]) -> usize {
    batches.iter().map(|p| p.packets.len()).sum()
}

// internal function to be unit-tested; should be used only by get_packet_offsets
fn do_get_packet_offsets(
    packet: &Packet,
    current_offset: usize,
) -> Result<PacketOffsets, PacketError> {
    // should have at least 1 signature, sig lengths and the message header
    let _ = 1usize
        .checked_add(size_of::<Signature>())
        .and_then(|v| v.checked_add(MESSAGE_HEADER_LENGTH))
        .filter(|v| *v <= packet.meta.size)
        .ok_or(PacketError::InvalidLen)?;

    // read the length of Transaction.signatures (serialized with short_vec)
    let (sig_len_untrusted, sig_size) =
        decode_shortu16_len(&packet.data).map_err(|_| PacketError::InvalidShortVec)?;

    // Using msg_start_offset which is based on sig_len_untrusted introduces uncertainty.
    // Ultimately, the actual sigverify will determine the uncertainty.
    let msg_start_offset = sig_len_untrusted
        .checked_mul(size_of::<Signature>())
        .and_then(|v| v.checked_add(sig_size))
        .ok_or(PacketError::InvalidLen)?;

    let msg_start_offset_plus_one = msg_start_offset
        .checked_add(1)
        .ok_or(PacketError::InvalidLen)?;

    // Packet should have data at least for signatures, MessageHeader, 1 byte for Message.account_keys.len
    let _ = msg_start_offset_plus_one
        .checked_add(MESSAGE_HEADER_LENGTH)
        .filter(|v| *v <= packet.meta.size)
        .ok_or(PacketError::InvalidSignatureLen)?;

    // read MessageHeader.num_required_signatures (serialized with u8)
    let sig_len_maybe_trusted = packet.data[msg_start_offset];

    let message_account_keys_len_offset = msg_start_offset
        .checked_add(MESSAGE_HEADER_LENGTH)
        .ok_or(PacketError::InvalidLen)?;

    // This reads and compares the MessageHeader num_required_signatures and
    // num_readonly_signed_accounts bytes. If num_required_signatures is not larger than
    // num_readonly_signed_accounts, the first account is not debitable, and cannot be charged
    // required transaction fees.
    let readonly_signer_offset = msg_start_offset_plus_one;
    if sig_len_maybe_trusted <= packet.data[readonly_signer_offset] {
        return Err(PacketError::PayerNotWritable);
    }

    if usize::from(sig_len_maybe_trusted) != sig_len_untrusted {
        return Err(PacketError::MismatchSignatureLen);
    }

    // read the length of Message.account_keys (serialized with short_vec)
    let (pubkey_len, pubkey_len_size) =
        decode_shortu16_len(&packet.data[message_account_keys_len_offset..])
            .map_err(|_| PacketError::InvalidShortVec)?;

    let pubkey_start = message_account_keys_len_offset
        .checked_add(pubkey_len_size)
        .ok_or(PacketError::InvalidPubkeyLen)?;

    let _ = pubkey_len
        .checked_mul(size_of::<Pubkey>())
        .and_then(|v| v.checked_add(pubkey_start))
        .filter(|v| *v <= packet.meta.size)
        .ok_or(PacketError::InvalidPubkeyLen)?;

    if pubkey_len < sig_len_untrusted {
        return Err(PacketError::InvalidPubkeyLen);
    }

    let sig_start = current_offset
        .checked_add(sig_size)
        .ok_or(PacketError::InvalidLen)?;
    let msg_start = current_offset
        .checked_add(msg_start_offset)
        .ok_or(PacketError::InvalidLen)?;
    let pubkey_start = current_offset
        .checked_add(pubkey_start)
        .ok_or(PacketError::InvalidLen)?;

    Ok(PacketOffsets::new(
        u32::try_from(sig_len_untrusted)?,
        u32::try_from(sig_start)?,
        u32::try_from(msg_start)?,
        u32::try_from(pubkey_start)?,
    ))
}

fn get_packet_offsets(packet: &Packet, current_offset: usize) -> PacketOffsets {
    let unsanitized_packet_offsets = do_get_packet_offsets(packet, current_offset);
    if let Ok(offsets) = unsanitized_packet_offsets {
        offsets
    } else {
        // force sigverify to fail by returning zeros
        PacketOffsets::new(0, 0, 0, 0)
    }
}

pub fn generate_offsets(batches: &[Packets], recycler: &Recycler<TxOffset>) -> TxOffsets {
    debug!("allocating..");
    let mut signature_offsets: PinnedVec<_> = recycler.allocate("sig_offsets");
    signature_offsets.set_pinnable();
    let mut pubkey_offsets: PinnedVec<_> = recycler.allocate("pubkey_offsets");
    pubkey_offsets.set_pinnable();
    let mut msg_start_offsets: PinnedVec<_> = recycler.allocate("msg_start_offsets");
    msg_start_offsets.set_pinnable();
    let mut msg_sizes: PinnedVec<_> = recycler.allocate("msg_size_offsets");
    msg_sizes.set_pinnable();
    let mut current_offset: usize = 0;
    let mut v_sig_lens = Vec::new();
    batches.iter().for_each(|p| {
        let mut sig_lens = Vec::new();
        p.packets.iter().for_each(|packet| {
            let packet_offsets = get_packet_offsets(packet, current_offset);

            sig_lens.push(packet_offsets.sig_len);

            trace!("pubkey_offset: {}", packet_offsets.pubkey_start);

            let mut pubkey_offset = packet_offsets.pubkey_start;
            let mut sig_offset = packet_offsets.sig_start;
            let msg_size = current_offset.saturating_add(packet.meta.size) as u32;
            for _ in 0..packet_offsets.sig_len {
                signature_offsets.push(sig_offset);
                sig_offset = sig_offset.saturating_add(size_of::<Signature>() as u32);

                pubkey_offsets.push(pubkey_offset);
                pubkey_offset = pubkey_offset.saturating_add(size_of::<Pubkey>() as u32);

                msg_start_offsets.push(packet_offsets.msg_start);

                let msg_size = msg_size.saturating_sub(packet_offsets.msg_start);
                msg_sizes.push(msg_size);
            }
            current_offset = current_offset.saturating_add(size_of::<Packet>());
        });
        v_sig_lens.push(sig_lens);
    });
    (
        signature_offsets,
        pubkey_offsets,
        msg_start_offsets,
        msg_sizes,
        v_sig_lens,
    )
}

pub fn ed25519_verify_cpu(batches: &mut [Packets]) {
    use rayon::prelude::*;
    let count = batch_size(batches);
    debug!("CPU ECDSA for {}", batch_size(batches));
    PAR_THREAD_POOL.install(|| {
        batches
            .into_par_iter()
            .for_each(|p| p.packets.par_iter_mut().for_each(|p| verify_packet(p)))
    });
    inc_new_counter_debug!("ed25519_verify_cpu", count);
}

pub fn ed25519_verify_disabled(batches: &mut [Packets]) {
    use rayon::prelude::*;
    let count = batch_size(batches);
    debug!("disabled ECDSA for {}", batch_size(batches));
    batches.into_par_iter().for_each(|p| {
        p.packets
            .par_iter_mut()
            .for_each(|p| p.meta.discard = false)
    });
    inc_new_counter_debug!("ed25519_verify_disabled", count);
}

pub fn copy_return_values(sig_lens: &[Vec<u32>], out: &PinnedVec<u8>, rvs: &mut Vec<Vec<u8>>) {
    let mut num = 0;
    for (vs, sig_vs) in rvs.iter_mut().zip(sig_lens.iter()) {
        for (v, sig_v) in vs.iter_mut().zip(sig_vs.iter()) {
            if *sig_v == 0 {
                *v = 0;
            } else {
                let mut vout = 1;
                for _ in 0..*sig_v {
                    if 0 == out[num] {
                        vout = 0;
                    }
                    num = num.saturating_add(1);
                }
                *v = vout;
            }
            if *v != 0 {
                trace!("VERIFIED PACKET!!!!!");
            }
        }
    }
}

// return true for success, i.e ge unpacks and !ge.is_small_order()
pub fn check_packed_ge_small_order(ge: &[u8; 32]) -> bool {
    if let Some(api) = perf_libs::api() {
        unsafe {
            // Returns 1 == fail, 0 == success
            let res = (api.ed25519_check_packed_ge_small_order)(ge.as_ptr());

            return res == 0;
        }
    }
    false
}

pub fn get_checked_scalar(scalar: &[u8; 32]) -> Result<[u8; 32], PacketError> {
    let mut out = [0u8; 32];
    if let Some(api) = perf_libs::api() {
        unsafe {
            let res = (api.ed25519_get_checked_scalar)(out.as_mut_ptr(), scalar.as_ptr());
            if res == 0 {
                return Ok(out);
            } else {
                return Err(PacketError::InvalidLen);
            }
        }
    }
    Ok(out)
}

pub fn mark_disabled(batches: &mut [Packets], r: &[Vec<u8>]) {
    batches.iter_mut().zip(r).for_each(|(b, v)| {
        b.packets.iter_mut().zip(v).for_each(|(p, f)| {
            p.meta.discard = *f == 0;
        })
    });
}

pub fn ed25519_verify(
    batches: &mut [Packets],
    recycler: &Recycler<TxOffset>,
    recycler_out: &Recycler<PinnedVec<u8>>,
) {
    let api = perf_libs::api();
    if api.is_none() {
        return ed25519_verify_cpu(batches);
    }
    let api = api.unwrap();

    use crate::packet::PACKET_DATA_SIZE;
    let count = batch_size(batches);

    // micro-benchmarks show GPU time for smallest batch around 15-20ms
    // and CPU speed for 64-128 sigverifies around 10-20ms. 64 is a nice
    // power-of-two number around that accounting for the fact that the CPU
    // may be busy doing other things while being a real validator
    // TODO: dynamically adjust this crossover
    if count < 64 {
        return ed25519_verify_cpu(batches);
    }

    let (signature_offsets, pubkey_offsets, msg_start_offsets, msg_sizes, sig_lens) =
        generate_offsets(batches, recycler);

    debug!("CUDA ECDSA for {}", batch_size(batches));
    debug!("allocating out..");
    let mut out = recycler_out.allocate("out_buffer");
    out.set_pinnable();
    let mut elems = Vec::new();
    let mut rvs = Vec::new();

    let mut num_packets: usize = 0;
    for p in batches.iter() {
        elems.push(perf_libs::Elems {
            elems: p.packets.as_ptr(),
            num: p.packets.len() as u32,
        });
        let mut v = Vec::new();
        v.resize(p.packets.len(), 0);
        rvs.push(v);
        num_packets = num_packets.saturating_add(p.packets.len());
    }
    out.resize(signature_offsets.len(), 0);
    trace!("Starting verify num packets: {}", num_packets);
    trace!("elem len: {}", elems.len() as u32);
    trace!("packet sizeof: {}", size_of::<Packet>() as u32);
    trace!("len offset: {}", PACKET_DATA_SIZE as u32);
    const USE_NON_DEFAULT_STREAM: u8 = 1;
    unsafe {
        let res = (api.ed25519_verify_many)(
            elems.as_ptr(),
            elems.len() as u32,
            size_of::<Packet>() as u32,
            num_packets as u32,
            signature_offsets.len() as u32,
            msg_sizes.as_ptr(),
            pubkey_offsets.as_ptr(),
            signature_offsets.as_ptr(),
            msg_start_offsets.as_ptr(),
            out.as_mut_ptr(),
            USE_NON_DEFAULT_STREAM,
        );
        if res != 0 {
            trace!("RETURN!!!: {}", res);
        }
    }
    trace!("done verify");
    copy_return_values(&sig_lens, &out, &mut rvs);
    mark_disabled(batches, &rvs);
    inc_new_counter_debug!("ed25519_verify_gpu", count);
}

#[cfg(test)]
pub fn make_packet_from_transaction(tx: Transaction) -> Packet {
    use bincode::serialize;

    let tx_bytes = serialize(&tx).unwrap();
    let mut packet = Packet::default();
    packet.meta.size = tx_bytes.len();
    packet.data[..packet.meta.size].copy_from_slice(&tx_bytes);
    packet
}

#[cfg(test)]
#[allow(clippy::integer_arithmetic)]
mod tests {
    use super::*;
    use crate::packet::{Packet, Packets};
    use crate::sigverify;
    use crate::sigverify::PacketOffsets;
    use crate::test_tx::{test_multisig_tx, test_tx};
    use bincode::{deserialize, serialize};
    use solana_sdk::hash::Hash;
    use solana_sdk::message::{Message, MessageHeader};
    use solana_sdk::signature::Signature;
    use solana_sdk::transaction::Transaction;

    const SIG_OFFSET: usize = 1;

    pub fn memfind<A: Eq>(a: &[A], b: &[A]) -> Option<usize> {
        assert!(a.len() >= b.len());
        let end = a.len() - b.len() + 1;
        for i in 0..end {
            if a[i..i + b.len()] == b[..] {
                return Some(i);
            }
        }
        None
    }

    #[test]
    fn test_mark_disabled() {
        let mut batch = Packets::default();
        batch.packets.push(Packet::default());
        let mut batches: Vec<Packets> = vec![batch];
        mark_disabled(&mut batches, &[vec![0]]);
        assert!(batches[0].packets[0].meta.discard);
        mark_disabled(&mut batches, &[vec![1]]);
        assert!(!batches[0].packets[0].meta.discard);
    }

    #[test]
    fn test_layout() {
        let tx = test_tx();
        let tx_bytes = serialize(&tx).unwrap();
        let packet = serialize(&tx).unwrap();
        assert_matches!(memfind(&packet, &tx_bytes), Some(0));
        assert_matches!(memfind(&packet, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), None);
    }

    #[test]
    fn test_system_transaction_layout() {
        let tx = test_tx();
        let tx_bytes = serialize(&tx).unwrap();
        let message_data = tx.message_data();
        let packet = sigverify::make_packet_from_transaction(tx.clone());

        let packet_offsets = sigverify::get_packet_offsets(&packet, 0);

        assert_eq!(
            memfind(&tx_bytes, tx.signatures[0].as_ref()),
            Some(SIG_OFFSET)
        );
        assert_eq!(
            memfind(&tx_bytes, tx.message().account_keys[0].as_ref()),
            Some(packet_offsets.pubkey_start as usize)
        );
        assert_eq!(
            memfind(&tx_bytes, &message_data),
            Some(packet_offsets.msg_start as usize)
        );
        assert_eq!(
            memfind(&tx_bytes, tx.signatures[0].as_ref()),
            Some(packet_offsets.sig_start as usize)
        );
        assert_eq!(packet_offsets.sig_len, 1);
    }

    fn packet_from_num_sigs(required_num_sigs: u8, actual_num_sigs: usize) -> Packet {
        let message = Message {
            header: MessageHeader {
                num_required_signatures: required_num_sigs,
                num_readonly_signed_accounts: 12,
                num_readonly_unsigned_accounts: 11,
            },
            account_keys: vec![],
            recent_blockhash: Hash::default(),
            instructions: vec![],
        };
        let mut tx = Transaction::new_unsigned(message);
        tx.signatures = vec![Signature::default(); actual_num_sigs as usize];
        sigverify::make_packet_from_transaction(tx)
    }

    #[test]
    fn test_untrustworthy_sigs() {
        let required_num_sigs = 14;
        let actual_num_sigs = 5;

        let packet = packet_from_num_sigs(required_num_sigs, actual_num_sigs);

        let unsanitized_packet_offsets = sigverify::do_get_packet_offsets(&packet, 0);

        assert_eq!(
            unsanitized_packet_offsets,
            Err(PacketError::MismatchSignatureLen)
        );
    }

    #[test]
    fn test_large_sigs() {
        // use any large number to be misinterpreted as 2 bytes when decoded as short_vec
        let required_num_sigs = 214;
        let actual_num_sigs = 5;

        let packet = packet_from_num_sigs(required_num_sigs, actual_num_sigs);

        let unsanitized_packet_offsets = sigverify::do_get_packet_offsets(&packet, 0);

        assert_eq!(
            unsanitized_packet_offsets,
            Err(PacketError::MismatchSignatureLen)
        );
    }

    #[test]
    fn test_small_packet() {
        let tx = test_tx();
        let mut packet = sigverify::make_packet_from_transaction(tx);

        packet.data[0] = 0xff;
        packet.data[1] = 0xff;
        packet.meta.size = 2;

        let res = sigverify::do_get_packet_offsets(&packet, 0);
        assert_eq!(res, Err(PacketError::InvalidLen));
    }

    #[test]
    fn test_pubkey_too_small() {
        solana_logger::setup();
        let mut tx = test_tx();
        let sig = tx.signatures[0];
        const NUM_SIG: usize = 18;
        tx.signatures = vec![sig; NUM_SIG];
        tx.message.account_keys = vec![];
        tx.message.header.num_required_signatures = NUM_SIG as u8;
        let mut packet = sigverify::make_packet_from_transaction(tx);

        let res = sigverify::do_get_packet_offsets(&packet, 0);
        assert_eq!(res, Err(PacketError::InvalidPubkeyLen));

        verify_packet(&mut packet);
        assert!(packet.meta.discard);

        packet.meta.discard = false;
        let mut batches = generate_packet_vec(&packet, 1, 1);
        ed25519_verify(&mut batches);
        assert!(batches[0].packets[0].meta.discard);
    }

    #[test]
    fn test_pubkey_len() {
        // See that the verify cannot walk off the end of the packet
        // trying to index into the account_keys to access pubkey.
        use solana_sdk::signer::{keypair::Keypair, Signer};
        solana_logger::setup();

        const NUM_SIG: usize = 17;
        let keypair1 = Keypair::new();
        let pubkey1 = keypair1.pubkey();
        let mut message = Message::new(&[], Some(&pubkey1));
        message.account_keys.push(pubkey1);
        message.account_keys.push(pubkey1);
        message.header.num_required_signatures = NUM_SIG as u8;
        message.recent_blockhash = Hash(pubkey1.to_bytes());
        let mut tx = Transaction::new_unsigned(message);

        info!("message: {:?}", tx.message_data());
        info!("tx: {:?}", tx);
        let sig = keypair1.try_sign_message(&tx.message_data()).unwrap();
        tx.signatures = vec![sig; NUM_SIG];

        let mut packet = sigverify::make_packet_from_transaction(tx);

        let res = sigverify::do_get_packet_offsets(&packet, 0);
        assert_eq!(res, Err(PacketError::InvalidPubkeyLen));

        verify_packet(&mut packet);
        assert!(packet.meta.discard);

        packet.meta.discard = false;
        let mut batches = generate_packet_vec(&packet, 1, 1);
        ed25519_verify(&mut batches);
        assert!(batches[0].packets[0].meta.discard);
    }

    #[test]
    fn test_large_sig_len() {
        let tx = test_tx();
        let mut packet = sigverify::make_packet_from_transaction(tx);

        // Make the signatures len huge
        packet.data[0] = 0x7f;

        let res = sigverify::do_get_packet_offsets(&packet, 0);
        assert_eq!(res, Err(PacketError::InvalidSignatureLen));
    }

    #[test]
    fn test_really_large_sig_len() {
        let tx = test_tx();
        let mut packet = sigverify::make_packet_from_transaction(tx);

        // Make the signatures len huge
        packet.data[0] = 0xff;
        packet.data[1] = 0xff;
        packet.data[2] = 0xff;
        packet.data[3] = 0xff;

        let res = sigverify::do_get_packet_offsets(&packet, 0);
        assert_eq!(res, Err(PacketError::InvalidShortVec));
    }

    #[test]
    fn test_invalid_pubkey_len() {
        let tx = test_tx();
        let mut packet = sigverify::make_packet_from_transaction(tx);

        let res = sigverify::do_get_packet_offsets(&packet, 0);

        // make pubkey len huge
        packet.data[res.unwrap().pubkey_start as usize - 1] = 0x7f;

        let res = sigverify::do_get_packet_offsets(&packet, 0);
        assert_eq!(res, Err(PacketError::InvalidPubkeyLen));
    }

    #[test]
    fn test_fee_payer_is_debitable() {
        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![],
            recent_blockhash: Hash::default(),
            instructions: vec![],
        };
        let mut tx = Transaction::new_unsigned(message);
        tx.signatures = vec![Signature::default()];
        let packet = sigverify::make_packet_from_transaction(tx);
        let res = sigverify::do_get_packet_offsets(&packet, 0);

        assert_eq!(res, Err(PacketError::PayerNotWritable));
    }

    #[test]
    fn test_system_transaction_data_layout() {
        use crate::packet::PACKET_DATA_SIZE;
        let mut tx0 = test_tx();
        tx0.message.instructions[0].data = vec![1, 2, 3];
        let message0a = tx0.message_data();
        let tx_bytes = serialize(&tx0).unwrap();
        assert!(tx_bytes.len() <= PACKET_DATA_SIZE);
        assert_eq!(
            memfind(&tx_bytes, tx0.signatures[0].as_ref()),
            Some(SIG_OFFSET)
        );
        let tx1 = deserialize(&tx_bytes).unwrap();
        assert_eq!(tx0, tx1);
        assert_eq!(tx1.message().instructions[0].data, vec![1, 2, 3]);

        tx0.message.instructions[0].data = vec![1, 2, 4];
        let message0b = tx0.message_data();
        assert_ne!(message0a, message0b);
    }

    // Just like get_packet_offsets, but not returning redundant information.
    fn get_packet_offsets_from_tx(tx: Transaction, current_offset: u32) -> PacketOffsets {
        let packet = sigverify::make_packet_from_transaction(tx);
        let packet_offsets = sigverify::get_packet_offsets(&packet, current_offset as usize);
        PacketOffsets::new(
            packet_offsets.sig_len,
            packet_offsets.sig_start - current_offset,
            packet_offsets.msg_start - packet_offsets.sig_start,
            packet_offsets.pubkey_start - packet_offsets.msg_start,
        )
    }

    #[test]
    fn test_get_packet_offsets() {
        assert_eq!(
            get_packet_offsets_from_tx(test_tx(), 0),
            PacketOffsets::new(1, 1, 64, 4)
        );
        assert_eq!(
            get_packet_offsets_from_tx(test_tx(), 100),
            PacketOffsets::new(1, 1, 64, 4)
        );

        // Ensure we're not indexing packet by the `current_offset` parameter.
        assert_eq!(
            get_packet_offsets_from_tx(test_tx(), 1_000_000),
            PacketOffsets::new(1, 1, 64, 4)
        );

        // Ensure we're returning sig_len, not sig_size.
        assert_eq!(
            get_packet_offsets_from_tx(test_multisig_tx(), 0),
            PacketOffsets::new(2, 1, 128, 4)
        );
    }

    fn generate_packet_vec(
        packet: &Packet,
        num_packets_per_batch: usize,
        num_batches: usize,
    ) -> Vec<Packets> {
        // generate packet vector
        let batches: Vec<_> = (0..num_batches)
            .map(|_| {
                let mut packets = Packets::default();
                packets.packets.resize(0, Packet::default());
                for _ in 0..num_packets_per_batch {
                    packets.packets.push(packet.clone());
                }
                assert_eq!(packets.packets.len(), num_packets_per_batch);
                packets
            })
            .collect();
        assert_eq!(batches.len(), num_batches);

        batches
    }

    fn test_verify_n(n: usize, modify_data: bool) {
        let tx = test_tx();
        let mut packet = sigverify::make_packet_from_transaction(tx);

        // jumble some data to test failure
        if modify_data {
            packet.data[20] = packet.data[20].wrapping_add(10);
        }

        let mut batches = generate_packet_vec(&packet, n, 2);

        // verify packets
        ed25519_verify(&mut batches);

        // check result
        let should_discard = modify_data;
        assert!(batches
            .iter()
            .flat_map(|p| &p.packets)
            .all(|p| p.meta.discard == should_discard));
    }

    fn ed25519_verify(batches: &mut [Packets]) {
        let recycler = Recycler::default();
        let recycler_out = Recycler::default();
        sigverify::ed25519_verify(batches, &recycler, &recycler_out);
    }

    #[test]
    fn test_verify_tampered_sig_len() {
        let mut tx = test_tx();
        // pretend malicious leader dropped a signature...
        tx.signatures.pop();
        let packet = sigverify::make_packet_from_transaction(tx);

        let mut batches = generate_packet_vec(&packet, 1, 1);

        // verify packets
        ed25519_verify(&mut batches);
        assert!(batches
            .iter()
            .flat_map(|p| &p.packets)
            .all(|p| p.meta.discard));
    }

    #[test]
    fn test_verify_zero() {
        test_verify_n(0, false);
    }

    #[test]
    fn test_verify_one() {
        test_verify_n(1, false);
    }

    #[test]
    fn test_verify_seventy_one() {
        test_verify_n(71, false);
    }

    #[test]
    fn test_verify_multisig() {
        solana_logger::setup();

        let tx = test_multisig_tx();
        let mut packet = sigverify::make_packet_from_transaction(tx);

        let n = 4;
        let num_batches = 3;
        let mut batches = generate_packet_vec(&packet, n, num_batches);

        packet.data[40] = packet.data[40].wrapping_add(8);

        batches[0].packets.push(packet);

        // verify packets
        ed25519_verify(&mut batches);

        // check result
        let ref_ans = 1u8;
        let mut ref_vec = vec![vec![ref_ans; n]; num_batches];
        ref_vec[0].push(0u8);
        assert!(batches
            .iter()
            .flat_map(|p| &p.packets)
            .zip(ref_vec.into_iter().flatten())
            .all(|(p, discard)| {
                if discard == 0 {
                    p.meta.discard
                } else {
                    !p.meta.discard
                }
            }));
    }

    #[test]
    fn test_verify_fuzz() {
        use rand::{thread_rng, Rng};
        solana_logger::setup();

        let tx = test_multisig_tx();
        let packet = sigverify::make_packet_from_transaction(tx);

        let recycler = Recycler::default();
        let recycler_out = Recycler::default();
        for _ in 0..50 {
            let n = thread_rng().gen_range(1, 30);
            let num_batches = thread_rng().gen_range(2, 30);
            let mut batches = generate_packet_vec(&packet, n, num_batches);

            let num_modifications = thread_rng().gen_range(0, 5);
            for _ in 0..num_modifications {
                let batch = thread_rng().gen_range(0, batches.len());
                let packet = thread_rng().gen_range(0, batches[batch].packets.len());
                let offset = thread_rng().gen_range(0, batches[batch].packets[packet].meta.size);
                let add = thread_rng().gen_range(0, 255);
                batches[batch].packets[packet].data[offset] =
                    batches[batch].packets[packet].data[offset].wrapping_add(add);
            }

            // verify from GPU verification pipeline (when GPU verification is enabled) are
            // equivalent to the CPU verification pipeline.
            let mut batches_cpu = batches.clone();
            sigverify::ed25519_verify(&mut batches, &recycler, &recycler_out);
            ed25519_verify_cpu(&mut batches_cpu);

            // check result
            batches
                .iter()
                .flat_map(|p| &p.packets)
                .zip(batches_cpu.iter().flat_map(|p| &p.packets))
                .for_each(|(p1, p2)| assert_eq!(p1, p2));
        }
    }

    #[test]
    fn test_verify_fail() {
        test_verify_n(5, true);
    }

    #[test]
    fn test_get_checked_scalar() {
        solana_logger::setup();
        use curve25519_dalek::scalar::Scalar;
        use rand::{thread_rng, Rng};
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        if perf_libs::api().is_none() {
            return;
        }

        let passed_g = AtomicU64::new(0);
        let failed_g = AtomicU64::new(0);
        (0..4).into_par_iter().for_each(|_| {
            let mut input = [0u8; 32];
            let mut passed = 0;
            let mut failed = 0;
            for _ in 0..1_000_000 {
                thread_rng().fill(&mut input);
                let ans = get_checked_scalar(&input);
                let ref_ans = Scalar::from_canonical_bytes(input);
                if let Some(ref_ans) = ref_ans {
                    passed += 1;
                    assert_eq!(ans.unwrap(), ref_ans.to_bytes());
                } else {
                    failed += 1;
                    assert!(ans.is_err());
                }
            }
            passed_g.fetch_add(passed, Ordering::Relaxed);
            failed_g.fetch_add(failed, Ordering::Relaxed);
        });
        info!(
            "passed: {} failed: {}",
            passed_g.load(Ordering::Relaxed),
            failed_g.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn test_ge_small_order() {
        solana_logger::setup();
        use curve25519_dalek::edwards::CompressedEdwardsY;
        use rand::{thread_rng, Rng};
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        if perf_libs::api().is_none() {
            return;
        }

        let passed_g = AtomicU64::new(0);
        let failed_g = AtomicU64::new(0);
        (0..4).into_par_iter().for_each(|_| {
            let mut input = [0u8; 32];
            let mut passed = 0;
            let mut failed = 0;
            for _ in 0..1_000_000 {
                thread_rng().fill(&mut input);
                let ans = check_packed_ge_small_order(&input);
                let ref_ge = CompressedEdwardsY::from_slice(&input);
                if let Some(ref_element) = ref_ge.decompress() {
                    if ref_element.is_small_order() {
                        assert!(!ans);
                    } else {
                        assert!(ans);
                    }
                } else {
                    assert!(!ans);
                }
                if ans {
                    passed += 1;
                } else {
                    failed += 1;
                }
            }
            passed_g.fetch_add(passed, Ordering::Relaxed);
            failed_g.fetch_add(failed, Ordering::Relaxed);
        });
        info!(
            "passed: {} failed: {}",
            passed_g.load(Ordering::Relaxed),
            failed_g.load(Ordering::Relaxed)
        );
    }
}
