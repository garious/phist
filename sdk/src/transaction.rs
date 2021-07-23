//! Defines a Transaction type to package an atomic sequence of instructions.

#![cfg(feature = "full")]

use crate::sanitize::{Sanitize, SanitizeError};
use crate::secp256k1_instruction::verify_eth_addresses;
use crate::{
    hash::Hash,
    instruction::{CompiledInstruction, Instruction, InstructionError},
    message::Message,
    program_utils::limited_deserialize,
    pubkey::Pubkey,
    short_vec,
    signature::{Signature, SignerError},
    signers::Signers,
    system_instruction::SystemInstruction,
    system_program,
};
use std::result;
use thiserror::Error;

/// Reasons a transaction might be rejected.
#[derive(
    Error, Serialize, Deserialize, Debug, PartialEq, Eq, Clone, AbiExample, AbiEnumVisitor,
)]
pub enum TransactionError {
    /// An account is already being processed in another transaction in a way
    /// that does not support parallelism
    #[error("Account in use")]
    AccountInUse,

    /// A `Pubkey` appears twice in the transaction's `account_keys`.  Instructions can reference
    /// `Pubkey`s more than once but the message must contain a list with no duplicate keys
    #[error("Account loaded twice")]
    AccountLoadedTwice,

    /// Attempt to debit an account but found no record of a prior credit.
    #[error("Attempt to debit an account but found no record of a prior credit.")]
    AccountNotFound,

    /// Attempt to load a program that does not exist
    #[error("Attempt to load a program that does not exist")]
    ProgramAccountNotFound,

    /// The from `Pubkey` does not have sufficient balance to pay the fee to schedule the transaction
    #[error("Insufficient funds for fee")]
    InsufficientFundsForFee,

    /// This account may not be used to pay transaction fees
    #[error("This account may not be used to pay transaction fees")]
    InvalidAccountForFee,

    /// The bank has seen this transaction before. This can occur under normal operation
    /// when a UDP packet is duplicated, as a user error from a client not updating
    /// its `recent_blockhash`, or as a double-spend attack.
    #[error("This transaction has already been processed")]
    AlreadyProcessed,

    /// The bank has not seen the given `recent_blockhash` or the transaction is too old and
    /// the `recent_blockhash` has been discarded.
    #[error("Blockhash not found")]
    BlockhashNotFound,

    /// An error occurred while processing an instruction. The first element of the tuple
    /// indicates the instruction index in which the error occurred.
    #[error("Error processing Instruction {0}: {1}")]
    InstructionError(u8, InstructionError),

    /// Loader call chain is too deep
    #[error("Loader call chain is too deep")]
    CallChainTooDeep,

    /// Transaction requires a fee but has no signature present
    #[error("Transaction requires a fee but has no signature present")]
    MissingSignatureForFee,

    /// Transaction contains an invalid account reference
    #[error("Transaction contains an invalid account reference")]
    InvalidAccountIndex,

    /// Transaction did not pass signature verification
    #[error("Transaction did not pass signature verification")]
    SignatureFailure,

    /// This program may not be used for executing instructions
    #[error("This program may not be used for executing instructions")]
    InvalidProgramForExecution,

    /// Transaction failed to sanitize accounts offsets correctly
    /// implies that account locks are not taken for this TX, and should
    /// not be unlocked.
    #[error("Transaction failed to sanitize accounts offsets correctly")]
    SanitizeFailure,

    #[error("Transactions are currently disabled due to cluster maintenance")]
    ClusterMaintenance,

    /// Transaction processing left an account with an outstanding borrowed reference
    #[error("Transaction processing left an account with an outstanding borrowed reference")]
    AccountBorrowOutstanding,
}

pub type Result<T> = result::Result<T, TransactionError>;

impl From<SanitizeError> for TransactionError {
    fn from(_: SanitizeError) -> Self {
        Self::SanitizeFailure
    }
}

/// An atomic transaction
#[frozen_abi(digest = "AJho2K4eY1U3LuMJS9HuUqJBiLY4AHx4ivxXfA2KrrcC")]
#[derive(Debug, PartialEq, Default, Eq, Clone, Serialize, Deserialize, AbiExample)]
pub struct Transaction {
    /// A set of digital signatures of `account_keys`, `program_ids`, `recent_blockhash`, and `instructions`, signed by the first
    /// signatures.len() keys of account_keys
    /// NOTE: Serialization-related changes must be paired with the direct read at sigverify.
    #[serde(with = "short_vec")]
    pub signatures: Vec<Signature>,

    /// The message to sign.
    pub message: Message,
}

impl Sanitize for Transaction {
    fn sanitize(&self) -> std::result::Result<(), SanitizeError> {
        if self.message.header.num_required_signatures as usize > self.signatures.len() {
            return Err(SanitizeError::IndexOutOfBounds);
        }
        if self.signatures.len() > self.message.account_keys.len() {
            return Err(SanitizeError::IndexOutOfBounds);
        }
        self.message.sanitize()
    }
}

impl Transaction {
    pub fn new_unsigned(message: Message) -> Self {
        Self {
            signatures: vec![Signature::default(); message.header.num_required_signatures as usize],
            message,
        }
    }

    pub fn new_with_payer(instructions: &[Instruction], payer: Option<&Pubkey>) -> Self {
        let message = Message::new(instructions, payer);
        Self::new_unsigned(message)
    }

    /// Create a signed transaction with the given payer.
    ///
    /// # Panics
    ///
    /// Panics when signing fails.
    pub fn new_signed_with_payer<T: Signers>(
        instructions: &[Instruction],
        payer: Option<&Pubkey>,
        signing_keypairs: &T,
        recent_blockhash: Hash,
    ) -> Self {
        let message = Message::new(instructions, payer);
        Self::new(signing_keypairs, message, recent_blockhash)
    }

    /// Create a signed transaction.
    ///
    /// # Panics
    ///
    /// Panics when signing fails.
    pub fn new<T: Signers>(
        from_keypairs: &T,
        message: Message,
        recent_blockhash: Hash,
    ) -> Transaction {
        let mut tx = Self::new_unsigned(message);
        tx.sign(from_keypairs, recent_blockhash);
        tx
    }

    /// Create a signed transaction
    /// * `from_keypairs` - The keys used to sign the transaction.
    /// * `keys` - The keys for the transaction.  These are the program state
    ///    instances or lamport recipient keys.
    /// * `recent_blockhash` - The PoH hash.
    /// * `program_ids` - The keys that identify programs used in the `instruction` vector.
    /// * `instructions` - Instructions that will be executed atomically.
    ///
    /// # Panics
    ///
    /// Panics when signing fails.
    pub fn new_with_compiled_instructions<T: Signers>(
        from_keypairs: &T,
        keys: &[Pubkey],
        recent_blockhash: Hash,
        program_ids: Vec<Pubkey>,
        instructions: Vec<CompiledInstruction>,
    ) -> Self {
        let mut account_keys = from_keypairs.pubkeys();
        let from_keypairs_len = account_keys.len();
        account_keys.extend_from_slice(keys);
        account_keys.extend(&program_ids);
        let message = Message::new_with_compiled_instructions(
            from_keypairs_len as u8,
            0,
            program_ids.len() as u8,
            account_keys,
            Hash::default(),
            instructions,
        );
        Transaction::new(from_keypairs, message, recent_blockhash)
    }

    pub fn data(&self, instruction_index: usize) -> &[u8] {
        &self.message.instructions[instruction_index].data
    }

    fn key_index(&self, instruction_index: usize, accounts_index: usize) -> Option<usize> {
        self.message
            .instructions
            .get(instruction_index)
            .and_then(|instruction| instruction.accounts.get(accounts_index))
            .map(|&account_keys_index| account_keys_index as usize)
    }
    pub fn key(&self, instruction_index: usize, accounts_index: usize) -> Option<&Pubkey> {
        self.key_index(instruction_index, accounts_index)
            .and_then(|account_keys_index| self.message.account_keys.get(account_keys_index))
    }
    pub fn signer_key(&self, instruction_index: usize, accounts_index: usize) -> Option<&Pubkey> {
        match self.key_index(instruction_index, accounts_index) {
            None => None,
            Some(signature_index) => {
                if signature_index >= self.signatures.len() {
                    return None;
                }
                self.message.account_keys.get(signature_index)
            }
        }
    }

    /// Return a message containing all data that should be signed.
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// Return the serialized message data to sign.
    pub fn message_data(&self) -> Vec<u8> {
        self.message().serialize()
    }

    /// Check keys and keypair lengths, then sign this transaction.
    ///
    /// # Panics
    ///
    /// Panics when signing fails, use [`Transaction::try_sign`] to handle the error.
    pub fn sign<T: Signers>(&mut self, keypairs: &T, recent_blockhash: Hash) {
        if let Err(e) = self.try_sign(keypairs, recent_blockhash) {
            panic!("Transaction::sign failed with error {:?}", e);
        }
    }

    /// Sign using some subset of required keys
    ///  if recent_blockhash is not the same as currently in the transaction,
    ///  clear any prior signatures and update recent_blockhash
    ///
    /// # Panics
    ///
    /// Panics when signing fails, use [`Transaction::try_partial_sign`] to handle the error.
    pub fn partial_sign<T: Signers>(&mut self, keypairs: &T, recent_blockhash: Hash) {
        if let Err(e) = self.try_partial_sign(keypairs, recent_blockhash) {
            panic!("Transaction::partial_sign failed with error {:?}", e);
        }
    }

    /// Sign the transaction and place the signatures in their associated positions in `signatures`
    /// without checking that the positions are correct.
    ///
    /// # Panics
    ///
    /// Panics when signing fails, use [`Transaction::try_partial_sign_unchecked`] to handle the error.
    pub fn partial_sign_unchecked<T: Signers>(
        &mut self,
        keypairs: &T,
        positions: Vec<usize>,
        recent_blockhash: Hash,
    ) {
        if let Err(e) = self.try_partial_sign_unchecked(keypairs, positions, recent_blockhash) {
            panic!(
                "Transaction::partial_sign_unchecked failed with error {:?}",
                e
            );
        }
    }

    /// Check keys and keypair lengths, then sign this transaction, returning any signing errors
    /// encountered
    pub fn try_sign<T: Signers>(
        &mut self,
        keypairs: &T,
        recent_blockhash: Hash,
    ) -> result::Result<(), SignerError> {
        self.try_partial_sign(keypairs, recent_blockhash)?;

        if !self.is_signed() {
            Err(SignerError::NotEnoughSigners)
        } else {
            Ok(())
        }
    }

    ///  Sign using some subset of required keys, returning any signing errors encountered. If
    ///  recent_blockhash is not the same as currently in the transaction, clear any prior
    ///  signatures and update recent_blockhash
    pub fn try_partial_sign<T: Signers>(
        &mut self,
        keypairs: &T,
        recent_blockhash: Hash,
    ) -> result::Result<(), SignerError> {
        let positions = self.get_signing_keypair_positions(&keypairs.pubkeys())?;
        if positions.iter().any(|pos| pos.is_none()) {
            return Err(SignerError::KeypairPubkeyMismatch);
        }
        let positions: Vec<usize> = positions.iter().map(|pos| pos.unwrap()).collect();
        self.try_partial_sign_unchecked(keypairs, positions, recent_blockhash)
    }

    /// Sign the transaction, returning any signing errors encountered, and place the
    /// signatures in their associated positions in `signatures` without checking that the
    /// positions are correct.
    pub fn try_partial_sign_unchecked<T: Signers>(
        &mut self,
        keypairs: &T,
        positions: Vec<usize>,
        recent_blockhash: Hash,
    ) -> result::Result<(), SignerError> {
        // if you change the blockhash, you're re-signing...
        if recent_blockhash != self.message.recent_blockhash {
            self.message.recent_blockhash = recent_blockhash;
            self.signatures
                .iter_mut()
                .for_each(|signature| *signature = Signature::default());
        }

        let signatures = keypairs.try_sign_message(&self.message_data())?;
        for i in 0..positions.len() {
            self.signatures[positions[i]] = signatures[i];
        }
        Ok(())
    }

    /// Verify the transaction
    pub fn verify(&self) -> Result<()> {
        let message_bytes = self.message_data();
        if !self
            ._verify_with_results(&message_bytes)
            .iter()
            .all(|verify_result| *verify_result)
        {
            Err(TransactionError::SignatureFailure)
        } else {
            Ok(())
        }
    }

    /// Verify the length of signatures matches the value in the message header
    pub fn verify_signatures_len(&self) -> bool {
        self.signatures.len() == self.message.header.num_required_signatures as usize
    }

    /// Verify the transaction and hash its message
    pub fn verify_and_hash_message(&self) -> Result<Hash> {
        let message_bytes = self.message_data();
        if !self
            ._verify_with_results(&message_bytes)
            .iter()
            .all(|verify_result| *verify_result)
        {
            Err(TransactionError::SignatureFailure)
        } else {
            Ok(Message::hash_raw_message(&message_bytes))
        }
    }

    pub fn verify_with_results(&self) -> Vec<bool> {
        self._verify_with_results(&self.message_data())
    }

    pub(crate) fn _verify_with_results(&self, message_bytes: &[u8]) -> Vec<bool> {
        self.signatures
            .iter()
            .zip(&self.message.account_keys)
            .map(|(signature, pubkey)| signature.verify(pubkey.as_ref(), message_bytes))
            .collect()
    }

    pub fn verify_precompiles(&self, libsecp256k1_0_5_upgrade_enabled: bool) -> Result<()> {
        for instruction in &self.message().instructions {
            // The Transaction may not be sanitized at this point
            if instruction.program_id_index as usize >= self.message().account_keys.len() {
                return Err(TransactionError::AccountNotFound);
            }
            let program_id = &self.message().account_keys[instruction.program_id_index as usize];
            if crate::secp256k1_program::check_id(program_id) {
                let instruction_datas: Vec<_> = self
                    .message()
                    .instructions
                    .iter()
                    .map(|instruction| instruction.data.as_ref())
                    .collect();
                let data = &instruction.data;
                let e = verify_eth_addresses(
                    data,
                    &instruction_datas,
                    libsecp256k1_0_5_upgrade_enabled,
                );
                e.map_err(|_| TransactionError::InvalidAccountIndex)?;
            }
        }
        Ok(())
    }

    /// Get the positions of the pubkeys in `account_keys` associated with signing keypairs
    pub fn get_signing_keypair_positions(&self, pubkeys: &[Pubkey]) -> Result<Vec<Option<usize>>> {
        if self.message.account_keys.len() < self.message.header.num_required_signatures as usize {
            return Err(TransactionError::InvalidAccountIndex);
        }
        let signed_keys =
            &self.message.account_keys[0..self.message.header.num_required_signatures as usize];

        Ok(pubkeys
            .iter()
            .map(|pubkey| signed_keys.iter().position(|x| x == pubkey))
            .collect())
    }

    /// Replace all the signatures and pubkeys
    pub fn replace_signatures(&mut self, signers: &[(Pubkey, Signature)]) -> Result<()> {
        let num_required_signatures = self.message.header.num_required_signatures as usize;
        if signers.len() != num_required_signatures
            || self.signatures.len() != num_required_signatures
            || self.message.account_keys.len() < num_required_signatures
        {
            return Err(TransactionError::InvalidAccountIndex);
        }

        signers
            .iter()
            .enumerate()
            .for_each(|(i, (pubkey, signature))| {
                self.signatures[i] = *signature;
                self.message.account_keys[i] = *pubkey;
            });

        self.verify()
    }

    pub fn is_signed(&self) -> bool {
        self.signatures
            .iter()
            .all(|signature| *signature != Signature::default())
    }
}

pub fn uses_durable_nonce(tx: &Transaction) -> Option<&CompiledInstruction> {
    let message = tx.message();
    message
        .instructions
        .get(0)
        .filter(|maybe_ix| {
            let prog_id_idx = maybe_ix.program_id_index as usize;
            match message.account_keys.get(prog_id_idx) {
                Some(program_id) => system_program::check_id(program_id),
                _ => false,
            }
        } && matches!(limited_deserialize(&maybe_ix.data), Ok(SystemInstruction::AdvanceNonceAccount))
        )
}

pub fn get_nonce_pubkey_from_instruction<'a>(
    ix: &CompiledInstruction,
    tx: &'a Transaction,
) -> Option<&'a Pubkey> {
    ix.accounts.get(0).and_then(|idx| {
        let idx = *idx as usize;
        tx.message().account_keys.get(idx)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        hash::hash,
        instruction::AccountMeta,
        signature::{Keypair, Presigner, Signer},
        system_instruction,
    };
    use bincode::{deserialize, serialize, serialized_size};
    use std::mem::size_of;

    fn get_program_id(tx: &Transaction, instruction_index: usize) -> &Pubkey {
        let message = tx.message();
        let instruction = &message.instructions[instruction_index];
        instruction.program_id(&message.account_keys)
    }

    #[test]
    fn test_refs() {
        let key = Keypair::new();
        let key1 = solana_sdk::pubkey::new_rand();
        let key2 = solana_sdk::pubkey::new_rand();
        let prog1 = solana_sdk::pubkey::new_rand();
        let prog2 = solana_sdk::pubkey::new_rand();
        let instructions = vec![
            CompiledInstruction::new(3, &(), vec![0, 1]),
            CompiledInstruction::new(4, &(), vec![0, 2]),
        ];
        let tx = Transaction::new_with_compiled_instructions(
            &[&key],
            &[key1, key2],
            Hash::default(),
            vec![prog1, prog2],
            instructions,
        );
        assert!(tx.sanitize().is_ok());

        assert_eq!(tx.key(0, 0), Some(&key.pubkey()));
        assert_eq!(tx.signer_key(0, 0), Some(&key.pubkey()));

        assert_eq!(tx.key(1, 0), Some(&key.pubkey()));
        assert_eq!(tx.signer_key(1, 0), Some(&key.pubkey()));

        assert_eq!(tx.key(0, 1), Some(&key1));
        assert_eq!(tx.signer_key(0, 1), None);

        assert_eq!(tx.key(1, 1), Some(&key2));
        assert_eq!(tx.signer_key(1, 1), None);

        assert_eq!(tx.key(2, 0), None);
        assert_eq!(tx.signer_key(2, 0), None);

        assert_eq!(tx.key(0, 2), None);
        assert_eq!(tx.signer_key(0, 2), None);

        assert_eq!(*get_program_id(&tx, 0), prog1);
        assert_eq!(*get_program_id(&tx, 1), prog2);
    }
    #[test]
    fn test_refs_invalid_program_id() {
        let key = Keypair::new();
        let instructions = vec![CompiledInstruction::new(1, &(), vec![])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&key],
            &[],
            Hash::default(),
            vec![],
            instructions,
        );
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));
    }
    #[test]
    fn test_refs_invalid_account() {
        let key = Keypair::new();
        let instructions = vec![CompiledInstruction::new(1, &(), vec![2])];
        let tx = Transaction::new_with_compiled_instructions(
            &[&key],
            &[],
            Hash::default(),
            vec![Pubkey::default()],
            instructions,
        );
        assert_eq!(*get_program_id(&tx, 0), Pubkey::default());
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));
    }

    #[test]
    fn test_sanitize_txs() {
        let key = Keypair::new();
        let id0 = Pubkey::default();
        let program_id = solana_sdk::pubkey::new_rand();
        let ix = Instruction::new_with_bincode(
            program_id,
            &0,
            vec![
                AccountMeta::new(key.pubkey(), true),
                AccountMeta::new(id0, true),
            ],
        );
        let mut tx = Transaction::new_with_payer(&[ix], Some(&key.pubkey()));
        let o = tx.clone();
        assert_eq!(tx.sanitize(), Ok(()));
        assert_eq!(tx.message.account_keys.len(), 3);

        tx = o.clone();
        tx.message.header.num_required_signatures = 3;
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));

        tx = o.clone();
        tx.message.header.num_readonly_signed_accounts = 4;
        tx.message.header.num_readonly_unsigned_accounts = 0;
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));

        tx = o.clone();
        tx.message.header.num_readonly_signed_accounts = 2;
        tx.message.header.num_readonly_unsigned_accounts = 2;
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));

        tx = o.clone();
        tx.message.header.num_readonly_signed_accounts = 0;
        tx.message.header.num_readonly_unsigned_accounts = 4;
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));

        tx = o.clone();
        tx.message.instructions[0].program_id_index = 3;
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));

        tx = o.clone();
        tx.message.instructions[0].accounts[0] = 3;
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));

        tx = o.clone();
        tx.message.instructions[0].program_id_index = 0;
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));

        tx = o.clone();
        tx.message.header.num_readonly_signed_accounts = 2;
        tx.message.header.num_readonly_unsigned_accounts = 3;
        tx.message.account_keys.resize(4, Pubkey::default());
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));

        tx = o;
        tx.message.header.num_readonly_signed_accounts = 2;
        tx.message.header.num_required_signatures = 1;
        assert_eq!(tx.sanitize(), Err(SanitizeError::IndexOutOfBounds));
    }

    fn create_sample_transaction() -> Transaction {
        let keypair = Keypair::from_bytes(&[
            48, 83, 2, 1, 1, 48, 5, 6, 3, 43, 101, 112, 4, 34, 4, 32, 255, 101, 36, 24, 124, 23,
            167, 21, 132, 204, 155, 5, 185, 58, 121, 75, 156, 227, 116, 193, 215, 38, 142, 22, 8,
            14, 229, 239, 119, 93, 5, 218, 161, 35, 3, 33, 0, 36, 100, 158, 252, 33, 161, 97, 185,
            62, 89, 99,
        ])
        .unwrap();
        let to = Pubkey::new(&[
            1, 1, 1, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 8, 7, 6, 5, 4,
            1, 1, 1,
        ]);

        let program_id = Pubkey::new(&[
            2, 2, 2, 4, 5, 6, 7, 8, 9, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 8, 7, 6, 5, 4,
            2, 2, 2,
        ]);
        let account_metas = vec![
            AccountMeta::new(keypair.pubkey(), true),
            AccountMeta::new(to, false),
        ];
        let instruction =
            Instruction::new_with_bincode(program_id, &(1u8, 2u8, 3u8), account_metas);
        let message = Message::new(&[instruction], Some(&keypair.pubkey()));
        Transaction::new(&[&keypair], message, Hash::default())
    }

    #[test]
    fn test_transaction_serialize() {
        let tx = create_sample_transaction();
        let ser = serialize(&tx).unwrap();
        let deser = deserialize(&ser).unwrap();
        assert_eq!(tx, deser);
    }

    /// Detect changes to the serialized size of payment transactions, which affects TPS.
    #[test]
    fn test_transaction_minimum_serialized_size() {
        let alice_keypair = Keypair::new();
        let alice_pubkey = alice_keypair.pubkey();
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let ix = system_instruction::transfer(&alice_pubkey, &bob_pubkey, 42);

        let expected_data_size = size_of::<u32>() + size_of::<u64>();
        assert_eq!(expected_data_size, 12);
        assert_eq!(
            ix.data.len(),
            expected_data_size,
            "unexpected system instruction size"
        );

        let expected_instruction_size = 1 + 1 + ix.accounts.len() + 1 + expected_data_size;
        assert_eq!(expected_instruction_size, 17);

        let message = Message::new(&[ix], Some(&alice_pubkey));
        assert_eq!(
            serialized_size(&message.instructions[0]).unwrap() as usize,
            expected_instruction_size,
            "unexpected Instruction::serialized_size"
        );

        let tx = Transaction::new(&[&alice_keypair], message, Hash::default());

        let len_size = 1;
        let num_required_sigs_size = 1;
        let num_readonly_accounts_size = 2;
        let blockhash_size = size_of::<Hash>();
        let expected_transaction_size = len_size
            + (tx.signatures.len() * size_of::<Signature>())
            + num_required_sigs_size
            + num_readonly_accounts_size
            + len_size
            + (tx.message.account_keys.len() * size_of::<Pubkey>())
            + blockhash_size
            + len_size
            + expected_instruction_size;
        assert_eq!(expected_transaction_size, 215);

        assert_eq!(
            serialized_size(&tx).unwrap() as usize,
            expected_transaction_size,
            "unexpected serialized transaction size"
        );
    }

    /// Detect binary changes in the serialized transaction data, which could have a downstream
    /// affect on SDKs and applications
    #[test]
    fn test_sdk_serialize() {
        assert_eq!(
            serialize(&create_sample_transaction()).unwrap(),
            vec![
                1, 71, 59, 9, 187, 190, 129, 150, 165, 21, 33, 158, 72, 87, 110, 144, 120, 79, 238,
                132, 134, 105, 39, 102, 116, 209, 29, 229, 154, 36, 105, 44, 172, 118, 131, 22,
                124, 131, 179, 142, 176, 27, 117, 160, 89, 102, 224, 204, 1, 252, 141, 2, 136, 0,
                37, 218, 225, 129, 92, 154, 250, 59, 97, 178, 10, 1, 0, 1, 3, 156, 227, 116, 193,
                215, 38, 142, 22, 8, 14, 229, 239, 119, 93, 5, 218, 161, 35, 3, 33, 0, 36, 100,
                158, 252, 33, 161, 97, 185, 62, 89, 99, 1, 1, 1, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9,
                9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 8, 7, 6, 5, 4, 1, 1, 1, 2, 2, 2, 4, 5, 6, 7, 8, 9, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 8, 7, 6, 5, 4, 2, 2, 2, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2,
                2, 0, 1, 3, 1, 2, 3
            ]
        );
    }

    #[test]
    #[should_panic]
    fn test_transaction_missing_key() {
        let keypair = Keypair::new();
        let message = Message::new(&[], None);
        Transaction::new_unsigned(message).sign(&[&keypair], Hash::default());
    }

    #[test]
    #[should_panic]
    fn test_partial_sign_mismatched_key() {
        let keypair = Keypair::new();
        let fee_payer = solana_sdk::pubkey::new_rand();
        let ix = Instruction::new_with_bincode(
            Pubkey::default(),
            &0,
            vec![AccountMeta::new(fee_payer, true)],
        );
        let message = Message::new(&[ix], Some(&fee_payer));
        Transaction::new_unsigned(message).partial_sign(&[&keypair], Hash::default());
    }

    #[test]
    fn test_partial_sign() {
        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let ix = Instruction::new_with_bincode(
            Pubkey::default(),
            &0,
            vec![
                AccountMeta::new(keypair0.pubkey(), true),
                AccountMeta::new(keypair1.pubkey(), true),
                AccountMeta::new(keypair2.pubkey(), true),
            ],
        );
        let message = Message::new(&[ix], Some(&keypair0.pubkey()));
        let mut tx = Transaction::new_unsigned(message);

        tx.partial_sign(&[&keypair0, &keypair2], Hash::default());
        assert!(!tx.is_signed());
        tx.partial_sign(&[&keypair1], Hash::default());
        assert!(tx.is_signed());

        let hash = hash(&[1]);
        tx.partial_sign(&[&keypair1], hash);
        assert!(!tx.is_signed());
        tx.partial_sign(&[&keypair0, &keypair2], hash);
        assert!(tx.is_signed());
    }

    #[test]
    #[should_panic]
    fn test_transaction_missing_keypair() {
        let program_id = Pubkey::default();
        let keypair0 = Keypair::new();
        let id0 = keypair0.pubkey();
        let ix = Instruction::new_with_bincode(program_id, &0, vec![AccountMeta::new(id0, true)]);
        let message = Message::new(&[ix], Some(&id0));
        Transaction::new_unsigned(message).sign(&Vec::<&Keypair>::new(), Hash::default());
    }

    #[test]
    #[should_panic]
    fn test_transaction_wrong_key() {
        let program_id = Pubkey::default();
        let keypair0 = Keypair::new();
        let wrong_id = Pubkey::default();
        let ix =
            Instruction::new_with_bincode(program_id, &0, vec![AccountMeta::new(wrong_id, true)]);
        let message = Message::new(&[ix], Some(&wrong_id));
        Transaction::new_unsigned(message).sign(&[&keypair0], Hash::default());
    }

    #[test]
    fn test_transaction_correct_key() {
        let program_id = Pubkey::default();
        let keypair0 = Keypair::new();
        let id0 = keypair0.pubkey();
        let ix = Instruction::new_with_bincode(program_id, &0, vec![AccountMeta::new(id0, true)]);
        let message = Message::new(&[ix], Some(&id0));
        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&[&keypair0], Hash::default());
        assert_eq!(
            tx.message.instructions[0],
            CompiledInstruction::new(1, &0, vec![0])
        );
        assert!(tx.is_signed());
    }

    #[test]
    fn test_transaction_instruction_with_duplicate_keys() {
        let program_id = Pubkey::default();
        let keypair0 = Keypair::new();
        let id0 = keypair0.pubkey();
        let id1 = solana_sdk::pubkey::new_rand();
        let ix = Instruction::new_with_bincode(
            program_id,
            &0,
            vec![
                AccountMeta::new(id0, true),
                AccountMeta::new(id1, false),
                AccountMeta::new(id0, false),
                AccountMeta::new(id1, false),
            ],
        );
        let message = Message::new(&[ix], Some(&id0));
        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&[&keypair0], Hash::default());
        assert_eq!(
            tx.message.instructions[0],
            CompiledInstruction::new(2, &0, vec![0, 1, 0, 1])
        );
        assert!(tx.is_signed());
    }

    #[test]
    fn test_try_sign_dyn_keypairs() {
        let program_id = Pubkey::default();
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey();
        let presigner_keypair = Keypair::new();
        let presigner_pubkey = presigner_keypair.pubkey();

        let ix = Instruction::new_with_bincode(
            program_id,
            &0,
            vec![
                AccountMeta::new(pubkey, true),
                AccountMeta::new(presigner_pubkey, true),
            ],
        );
        let message = Message::new(&[ix], Some(&pubkey));
        let mut tx = Transaction::new_unsigned(message);

        let presigner_sig = presigner_keypair.sign_message(&tx.message_data());
        let presigner = Presigner::new(&presigner_pubkey, &presigner_sig);

        let signers: Vec<&dyn Signer> = vec![&keypair, &presigner];

        let res = tx.try_sign(&signers, Hash::default());
        assert_eq!(res, Ok(()));
        assert_eq!(tx.signatures[0], keypair.sign_message(&tx.message_data()));
        assert_eq!(tx.signatures[1], presigner_sig);

        // Wrong key should error, not panic
        let another_pubkey = solana_sdk::pubkey::new_rand();
        let ix = Instruction::new_with_bincode(
            program_id,
            &0,
            vec![
                AccountMeta::new(another_pubkey, true),
                AccountMeta::new(presigner_pubkey, true),
            ],
        );
        let message = Message::new(&[ix], Some(&another_pubkey));
        let mut tx = Transaction::new_unsigned(message);

        let res = tx.try_sign(&signers, Hash::default());
        assert!(res.is_err());
        assert_eq!(
            tx.signatures,
            vec![Signature::default(), Signature::default()]
        );
    }

    fn nonced_transfer_tx() -> (Pubkey, Pubkey, Transaction) {
        let from_keypair = Keypair::new();
        let from_pubkey = from_keypair.pubkey();
        let nonce_keypair = Keypair::new();
        let nonce_pubkey = nonce_keypair.pubkey();
        let instructions = [
            system_instruction::advance_nonce_account(&nonce_pubkey, &nonce_pubkey),
            system_instruction::transfer(&from_pubkey, &nonce_pubkey, 42),
        ];
        let message = Message::new(&instructions, Some(&nonce_pubkey));
        let tx = Transaction::new(&[&from_keypair, &nonce_keypair], message, Hash::default());
        (from_pubkey, nonce_pubkey, tx)
    }

    #[test]
    fn tx_uses_nonce_ok() {
        let (_, _, tx) = nonced_transfer_tx();
        assert!(uses_durable_nonce(&tx).is_some());
    }

    #[test]
    fn tx_uses_nonce_empty_ix_fail() {
        assert!(uses_durable_nonce(&Transaction::default()).is_none());
    }

    #[test]
    fn tx_uses_nonce_bad_prog_id_idx_fail() {
        let (_, _, mut tx) = nonced_transfer_tx();
        tx.message.instructions.get_mut(0).unwrap().program_id_index = 255u8;
        assert!(uses_durable_nonce(&tx).is_none());
    }

    #[test]
    fn tx_uses_nonce_first_prog_id_not_nonce_fail() {
        let from_keypair = Keypair::new();
        let from_pubkey = from_keypair.pubkey();
        let nonce_keypair = Keypair::new();
        let nonce_pubkey = nonce_keypair.pubkey();
        let instructions = [
            system_instruction::transfer(&from_pubkey, &nonce_pubkey, 42),
            system_instruction::advance_nonce_account(&nonce_pubkey, &nonce_pubkey),
        ];
        let message = Message::new(&instructions, Some(&from_pubkey));
        let tx = Transaction::new(&[&from_keypair, &nonce_keypair], message, Hash::default());
        assert!(uses_durable_nonce(&tx).is_none());
    }

    #[test]
    fn tx_uses_nonce_wrong_first_nonce_ix_fail() {
        let from_keypair = Keypair::new();
        let from_pubkey = from_keypair.pubkey();
        let nonce_keypair = Keypair::new();
        let nonce_pubkey = nonce_keypair.pubkey();
        let instructions = [
            system_instruction::withdraw_nonce_account(
                &nonce_pubkey,
                &nonce_pubkey,
                &from_pubkey,
                42,
            ),
            system_instruction::transfer(&from_pubkey, &nonce_pubkey, 42),
        ];
        let message = Message::new(&instructions, Some(&nonce_pubkey));
        let tx = Transaction::new(&[&from_keypair, &nonce_keypair], message, Hash::default());
        assert!(uses_durable_nonce(&tx).is_none());
    }

    #[test]
    fn get_nonce_pub_from_ix_ok() {
        let (_, nonce_pubkey, tx) = nonced_transfer_tx();
        let nonce_ix = uses_durable_nonce(&tx).unwrap();
        assert_eq!(
            get_nonce_pubkey_from_instruction(nonce_ix, &tx),
            Some(&nonce_pubkey),
        );
    }

    #[test]
    fn get_nonce_pub_from_ix_no_accounts_fail() {
        let (_, _, tx) = nonced_transfer_tx();
        let nonce_ix = uses_durable_nonce(&tx).unwrap();
        let mut nonce_ix = nonce_ix.clone();
        nonce_ix.accounts.clear();
        assert_eq!(get_nonce_pubkey_from_instruction(&nonce_ix, &tx), None,);
    }

    #[test]
    fn get_nonce_pub_from_ix_bad_acc_idx_fail() {
        let (_, _, tx) = nonced_transfer_tx();
        let nonce_ix = uses_durable_nonce(&tx).unwrap();
        let mut nonce_ix = nonce_ix.clone();
        nonce_ix.accounts[0] = 255u8;
        assert_eq!(get_nonce_pubkey_from_instruction(&nonce_ix, &tx), None,);
    }

    #[test]
    fn tx_keypair_pubkey_mismatch() {
        let from_keypair = Keypair::new();
        let from_pubkey = from_keypair.pubkey();
        let to_pubkey = Pubkey::new_unique();
        let instructions = [system_instruction::transfer(&from_pubkey, &to_pubkey, 42)];
        let mut tx = Transaction::new_with_payer(&instructions, Some(&from_pubkey));
        let unused_keypair = Keypair::new();
        let err = tx
            .try_partial_sign(&[&from_keypair, &unused_keypair], Hash::default())
            .unwrap_err();
        assert_eq!(err, SignerError::KeypairPubkeyMismatch);
    }
}
