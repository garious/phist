use solana_runtime::bank::Bank;
use solana_runtime::loader_utils::{create_invoke_instruction, load_program};
use solana_runtime::sync_client::SyncClient;
use solana_sdk::genesis_block::GenesisBlock;
use solana_sdk::native_loader;
use solana_sdk::signature::KeypairUtil;

#[test]
fn test_program_native_noop() {
    solana_logger::setup();

    let (genesis_block, alice_keypair) = GenesisBlock::new(50);
    let bank = Bank::new(&genesis_block);

    let program = "noop".as_bytes().to_vec();
    let program_id = load_program(&bank, &alice_keypair, &native_loader::id(), program);

    // Call user program
    let instruction = create_invoke_instruction(alice_keypair.pubkey(), program_id, &1u8);
    bank.send_instruction(&[&alice_keypair], instruction)
        .unwrap();
}
