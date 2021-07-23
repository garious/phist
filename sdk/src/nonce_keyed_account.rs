#![cfg(feature = "full")]

use crate::{
    account::{ReadableAccount, WritableAccount},
    account_utils::State as AccountUtilsState,
    ic_msg,
    keyed_account::KeyedAccount,
    nonce_account::create_account,
    process_instruction::InvokeContext,
};
use solana_program::{
    instruction::{checked_add, InstructionError},
    nonce::{self, state::Versions, State},
    pubkey::Pubkey,
    system_instruction::NonceError,
    sysvar::rent::Rent,
};
#[allow(deprecated)]
use solana_sdk::sysvar::recent_blockhashes::RecentBlockhashes;
use std::collections::HashSet;

#[allow(deprecated)]
pub trait NonceKeyedAccount {
    fn advance_nonce_account(
        &self,
        recent_blockhashes: &RecentBlockhashes,
        signers: &HashSet<Pubkey>,
        invoke_context: &dyn InvokeContext,
    ) -> Result<(), InstructionError>;
    fn withdraw_nonce_account(
        &self,
        lamports: u64,
        to: &KeyedAccount,
        recent_blockhashes: &RecentBlockhashes,
        rent: &Rent,
        signers: &HashSet<Pubkey>,
        invoke_context: &dyn InvokeContext,
    ) -> Result<(), InstructionError>;
    fn initialize_nonce_account(
        &self,
        nonce_authority: &Pubkey,
        recent_blockhashes: &RecentBlockhashes,
        rent: &Rent,
        invoke_context: &dyn InvokeContext,
    ) -> Result<(), InstructionError>;
    fn authorize_nonce_account(
        &self,
        nonce_authority: &Pubkey,
        signers: &HashSet<Pubkey>,
        invoke_context: &dyn InvokeContext,
    ) -> Result<(), InstructionError>;
}

impl<'a> NonceKeyedAccount for KeyedAccount<'a> {
    #[allow(deprecated)]
    fn advance_nonce_account(
        &self,
        recent_blockhashes: &RecentBlockhashes,
        signers: &HashSet<Pubkey>,
        invoke_context: &dyn InvokeContext,
    ) -> Result<(), InstructionError> {
        if recent_blockhashes.is_empty() {
            ic_msg!(
                invoke_context,
                "Advance nonce account: recent blockhash list is empty",
            );
            return Err(NonceError::NoRecentBlockhashes.into());
        }

        let state = AccountUtilsState::<Versions>::state(self)?.convert_to_current();
        match state {
            State::Initialized(data) => {
                if !signers.contains(&data.authority) {
                    ic_msg!(
                        invoke_context,
                        "Advance nonce account: Account {} must be a signer",
                        data.authority
                    );
                    return Err(InstructionError::MissingRequiredSignature);
                }
                let recent_blockhash = recent_blockhashes[0].blockhash;
                if data.blockhash == recent_blockhash {
                    ic_msg!(
                        invoke_context,
                        "Advance nonce account: nonce can only advance once per slot"
                    );
                    return Err(NonceError::NotExpired.into());
                }

                let new_data = nonce::state::Data {
                    blockhash: recent_blockhash,
                    fee_calculator: recent_blockhashes[0].fee_calculator.clone(),
                    ..data
                };
                self.set_state(&Versions::new_current(State::Initialized(new_data)))
            }
            _ => {
                ic_msg!(
                    invoke_context,
                    "Advance nonce account: Account {} state is invalid",
                    self.unsigned_key()
                );
                Err(NonceError::BadAccountState.into())
            }
        }
    }

    #[allow(deprecated)]
    fn withdraw_nonce_account(
        &self,
        lamports: u64,
        to: &KeyedAccount,
        recent_blockhashes: &RecentBlockhashes,
        rent: &Rent,
        signers: &HashSet<Pubkey>,
        invoke_context: &dyn InvokeContext,
    ) -> Result<(), InstructionError> {
        let signer = match AccountUtilsState::<Versions>::state(self)?.convert_to_current() {
            State::Uninitialized => {
                if lamports > self.lamports()? {
                    ic_msg!(
                        invoke_context,
                        "Withdraw nonce account: insufficient lamports {}, need {}",
                        self.lamports()?,
                        lamports,
                    );
                    return Err(InstructionError::InsufficientFunds);
                }
                *self.unsigned_key()
            }
            State::Initialized(ref data) => {
                if lamports == self.lamports()? {
                    if data.blockhash == recent_blockhashes[0].blockhash {
                        ic_msg!(
                            invoke_context,
                            "Withdraw nonce account: nonce can only advance once per slot"
                        );
                        return Err(NonceError::NotExpired.into());
                    }
                    self.set_state(&Versions::new_current(State::Uninitialized))?;
                } else {
                    let min_balance = rent.minimum_balance(self.data_len()?);
                    let amount = checked_add(lamports, min_balance)?;
                    if amount > self.lamports()? {
                        ic_msg!(
                            invoke_context,
                            "Withdraw nonce account: insufficient lamports {}, need {}",
                            self.lamports()?,
                            amount,
                        );
                        return Err(InstructionError::InsufficientFunds);
                    }
                }
                data.authority
            }
        };

        if !signers.contains(&signer) {
            ic_msg!(
                invoke_context,
                "Withdraw nonce account: Account {} must sign",
                signer
            );
            return Err(InstructionError::MissingRequiredSignature);
        }

        let nonce_balance = self.try_account_ref_mut()?.lamports();
        self.try_account_ref_mut()?.set_lamports(
            nonce_balance
                .checked_sub(lamports)
                .ok_or(InstructionError::ArithmeticOverflow)?,
        );
        let to_balance = to.try_account_ref_mut()?.lamports();
        to.try_account_ref_mut()?.set_lamports(
            to_balance
                .checked_add(lamports)
                .ok_or(InstructionError::ArithmeticOverflow)?,
        );

        Ok(())
    }

    #[allow(deprecated)]
    fn initialize_nonce_account(
        &self,
        nonce_authority: &Pubkey,
        recent_blockhashes: &RecentBlockhashes,
        rent: &Rent,
        invoke_context: &dyn InvokeContext,
    ) -> Result<(), InstructionError> {
        if recent_blockhashes.is_empty() {
            ic_msg!(
                invoke_context,
                "Initialize nonce account: recent blockhash list is empty",
            );
            return Err(NonceError::NoRecentBlockhashes.into());
        }

        match AccountUtilsState::<Versions>::state(self)?.convert_to_current() {
            State::Uninitialized => {
                let min_balance = rent.minimum_balance(self.data_len()?);
                if self.lamports()? < min_balance {
                    ic_msg!(
                        invoke_context,
                        "Initialize nonce account: insufficient lamports {}, need {}",
                        self.lamports()?,
                        min_balance
                    );
                    return Err(InstructionError::InsufficientFunds);
                }
                let data = nonce::state::Data {
                    authority: *nonce_authority,
                    blockhash: recent_blockhashes[0].blockhash,
                    fee_calculator: recent_blockhashes[0].fee_calculator.clone(),
                };
                self.set_state(&Versions::new_current(State::Initialized(data)))
            }
            _ => {
                ic_msg!(
                    invoke_context,
                    "Initialize nonce account: Account {} state is invalid",
                    self.unsigned_key()
                );
                Err(NonceError::BadAccountState.into())
            }
        }
    }

    fn authorize_nonce_account(
        &self,
        nonce_authority: &Pubkey,
        signers: &HashSet<Pubkey>,
        invoke_context: &dyn InvokeContext,
    ) -> Result<(), InstructionError> {
        match AccountUtilsState::<Versions>::state(self)?.convert_to_current() {
            State::Initialized(data) => {
                if !signers.contains(&data.authority) {
                    ic_msg!(
                        invoke_context,
                        "Authorize nonce account: Account {} must sign",
                        data.authority
                    );
                    return Err(InstructionError::MissingRequiredSignature);
                }
                let new_data = nonce::state::Data {
                    authority: *nonce_authority,
                    ..data
                };
                self.set_state(&Versions::new_current(State::Initialized(new_data)))
            }
            _ => {
                ic_msg!(
                    invoke_context,
                    "Authorize nonce account: Account {} state is invalid",
                    self.unsigned_key()
                );
                Err(NonceError::BadAccountState.into())
            }
        }
    }
}

/// Convenience function for working with keyed accounts in tests
pub fn with_test_keyed_account<F>(lamports: u64, signer: bool, f: F)
where
    F: Fn(&KeyedAccount),
{
    let pubkey = Pubkey::new_unique();
    let account = create_account(lamports);
    let keyed_account = KeyedAccount::new(&pubkey, signer, &account);
    f(&keyed_account)
}

#[cfg(test)]
mod test {
    use super::*;
    #[allow(deprecated)]
    use crate::sysvar::recent_blockhashes::create_test_recent_blockhashes;
    use crate::{
        account::ReadableAccount,
        account_utils::State as AccountUtilsState,
        keyed_account::KeyedAccount,
        nonce::{self, State},
        nonce_account::verify_nonce_account,
        process_instruction::MockInvokeContext,
        system_instruction::NonceError,
    };
    use solana_program::hash::Hash;

    #[test]
    fn default_is_uninitialized() {
        assert_eq!(State::default(), State::Uninitialized)
    }

    #[test]
    fn keyed_account_expected_behavior() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |keyed_account| {
            let data = nonce::state::Data {
                authority: *keyed_account.unsigned_key(),
                ..nonce::state::Data::default()
            };
            let mut signers = HashSet::new();
            signers.insert(*keyed_account.signer_key().unwrap());
            let state = AccountUtilsState::<Versions>::state(keyed_account)
                .unwrap()
                .convert_to_current();
            // New is in Uninitialzed state
            assert_eq!(state, State::Uninitialized);
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(95);
            let authorized = keyed_account.unsigned_key();
            keyed_account
                .initialize_nonce_account(
                    authorized,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            let state = AccountUtilsState::<Versions>::state(keyed_account)
                .unwrap()
                .convert_to_current();
            #[allow(deprecated)]
            let data = nonce::state::Data {
                blockhash: recent_blockhashes[0].blockhash,
                fee_calculator: recent_blockhashes[0].fee_calculator.clone(),
                ..data
            };
            // First nonce instruction drives state from Uninitialized to Initialized
            assert_eq!(state, State::Initialized(data.clone()));
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(63);
            keyed_account
                .advance_nonce_account(
                    &recent_blockhashes,
                    &signers,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            let state = AccountUtilsState::<Versions>::state(keyed_account)
                .unwrap()
                .convert_to_current();
            #[allow(deprecated)]
            let data = nonce::state::Data {
                blockhash: recent_blockhashes[0].blockhash,
                fee_calculator: recent_blockhashes[0].fee_calculator.clone(),
                ..data
            };
            // Second nonce instruction consumes and replaces stored nonce
            assert_eq!(state, State::Initialized(data.clone()));
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(31);
            keyed_account
                .advance_nonce_account(
                    &recent_blockhashes,
                    &signers,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            let state = AccountUtilsState::<Versions>::state(keyed_account)
                .unwrap()
                .convert_to_current();
            #[allow(deprecated)]
            let data = nonce::state::Data {
                blockhash: recent_blockhashes[0].blockhash,
                fee_calculator: recent_blockhashes[0].fee_calculator.clone(),
                ..data
            };
            // Third nonce instruction for fun and profit
            assert_eq!(state, State::Initialized(data));
            with_test_keyed_account(42, false, |to_keyed| {
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(0);
                let withdraw_lamports = keyed_account.account.borrow().lamports();
                let expect_nonce_lamports =
                    keyed_account.account.borrow().lamports() - withdraw_lamports;
                let expect_to_lamports = to_keyed.account.borrow().lamports() + withdraw_lamports;
                keyed_account
                    .withdraw_nonce_account(
                        withdraw_lamports,
                        to_keyed,
                        &recent_blockhashes,
                        &rent,
                        &signers,
                        &MockInvokeContext::new(vec![]),
                    )
                    .unwrap();
                // Empties Account balance
                assert_eq!(
                    keyed_account.account.borrow().lamports(),
                    expect_nonce_lamports
                );
                // Account balance goes to `to`
                assert_eq!(to_keyed.account.borrow().lamports(), expect_to_lamports);
                let state = AccountUtilsState::<Versions>::state(keyed_account)
                    .unwrap()
                    .convert_to_current();
                // Empty balance deinitializes data
                assert_eq!(state, State::Uninitialized);
            })
        })
    }

    #[test]
    fn nonce_inx_initialized_account_not_signer_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_account| {
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(31);
            let authority = *nonce_account.unsigned_key();
            nonce_account
                .initialize_nonce_account(
                    &authority,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            let pubkey = *nonce_account.account.borrow().owner();
            let nonce_account = KeyedAccount::new(&pubkey, false, nonce_account.account);
            let state = AccountUtilsState::<Versions>::state(&nonce_account)
                .unwrap()
                .convert_to_current();
            #[allow(deprecated)]
            let data = nonce::state::Data {
                authority,
                blockhash: recent_blockhashes[0].blockhash,
                fee_calculator: recent_blockhashes[0].fee_calculator.clone(),
            };
            assert_eq!(state, State::Initialized(data));
            let signers = HashSet::new();
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(0);
            let result = nonce_account.advance_nonce_account(
                &recent_blockhashes,
                &signers,
                &MockInvokeContext::new(vec![]),
            );
            assert_eq!(result, Err(InstructionError::MissingRequiredSignature),);
        })
    }

    #[test]
    fn nonce_inx_with_empty_recent_blockhashes_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |keyed_account| {
            let mut signers = HashSet::new();
            signers.insert(*keyed_account.signer_key().unwrap());
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(0);
            let authorized = *keyed_account.unsigned_key();
            keyed_account
                .initialize_nonce_account(
                    &authorized,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            let recent_blockhashes = vec![].into_iter().collect();
            let result = keyed_account.advance_nonce_account(
                &recent_blockhashes,
                &signers,
                &MockInvokeContext::new(vec![]),
            );
            assert_eq!(result, Err(NonceError::NoRecentBlockhashes.into()));
        })
    }

    #[test]
    fn nonce_inx_too_early_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |keyed_account| {
            let mut signers = HashSet::new();
            signers.insert(*keyed_account.signer_key().unwrap());
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(63);
            let authorized = *keyed_account.unsigned_key();
            keyed_account
                .initialize_nonce_account(
                    &authorized,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            let result = keyed_account.advance_nonce_account(
                &recent_blockhashes,
                &signers,
                &MockInvokeContext::new(vec![]),
            );
            assert_eq!(result, Err(NonceError::NotExpired.into()));
        })
    }

    #[test]
    fn nonce_inx_uninitialized_account_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |keyed_account| {
            let mut signers = HashSet::new();
            signers.insert(*keyed_account.signer_key().unwrap());
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(63);
            let result = keyed_account.advance_nonce_account(
                &recent_blockhashes,
                &signers,
                &MockInvokeContext::new(vec![]),
            );
            assert_eq!(result, Err(NonceError::BadAccountState.into()));
        })
    }

    #[test]
    fn nonce_inx_independent_nonce_authority_ok() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_account| {
            with_test_keyed_account(42, true, |nonce_authority| {
                let mut signers = HashSet::new();
                signers.insert(*nonce_account.signer_key().unwrap());
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(63);
                let authorized = *nonce_authority.unsigned_key();
                nonce_account
                    .initialize_nonce_account(
                        &authorized,
                        &recent_blockhashes,
                        &rent,
                        &MockInvokeContext::new(vec![]),
                    )
                    .unwrap();
                let mut signers = HashSet::new();
                signers.insert(*nonce_authority.signer_key().unwrap());
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(31);
                let result = nonce_account.advance_nonce_account(
                    &recent_blockhashes,
                    &signers,
                    &MockInvokeContext::new(vec![]),
                );
                assert_eq!(result, Ok(()));
            });
        });
    }

    #[test]
    fn nonce_inx_no_nonce_authority_sig_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_account| {
            with_test_keyed_account(42, false, |nonce_authority| {
                let mut signers = HashSet::new();
                signers.insert(*nonce_account.signer_key().unwrap());
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(63);
                let authorized = *nonce_authority.unsigned_key();
                nonce_account
                    .initialize_nonce_account(
                        &authorized,
                        &recent_blockhashes,
                        &rent,
                        &MockInvokeContext::new(vec![]),
                    )
                    .unwrap();
                let result = nonce_account.advance_nonce_account(
                    &recent_blockhashes,
                    &signers,
                    &MockInvokeContext::new(vec![]),
                );
                assert_eq!(result, Err(InstructionError::MissingRequiredSignature),);
            });
        });
    }

    #[test]
    fn withdraw_inx_unintialized_acc_ok() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_keyed| {
            let state = AccountUtilsState::<Versions>::state(nonce_keyed)
                .unwrap()
                .convert_to_current();
            assert_eq!(state, State::Uninitialized);
            with_test_keyed_account(42, false, |to_keyed| {
                let mut signers = HashSet::new();
                signers.insert(*nonce_keyed.signer_key().unwrap());
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(0);
                let withdraw_lamports = nonce_keyed.account.borrow().lamports();
                let expect_nonce_lamports =
                    nonce_keyed.account.borrow().lamports() - withdraw_lamports;
                let expect_to_lamports = to_keyed.account.borrow().lamports() + withdraw_lamports;
                nonce_keyed
                    .withdraw_nonce_account(
                        withdraw_lamports,
                        to_keyed,
                        &recent_blockhashes,
                        &rent,
                        &signers,
                        &MockInvokeContext::new(vec![]),
                    )
                    .unwrap();
                let state = AccountUtilsState::<Versions>::state(nonce_keyed)
                    .unwrap()
                    .convert_to_current();
                // Withdraw instruction...
                // Deinitializes Account state
                assert_eq!(state, State::Uninitialized);
                // Empties Account balance
                assert_eq!(
                    nonce_keyed.account.borrow().lamports(),
                    expect_nonce_lamports
                );
                // Account balance goes to `to`
                assert_eq!(to_keyed.account.borrow().lamports(), expect_to_lamports);
            })
        })
    }

    #[test]
    fn withdraw_inx_unintialized_acc_unsigned_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, false, |nonce_keyed| {
            let state = AccountUtilsState::<Versions>::state(nonce_keyed)
                .unwrap()
                .convert_to_current();
            assert_eq!(state, State::Uninitialized);
            with_test_keyed_account(42, false, |to_keyed| {
                let signers = HashSet::new();
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(0);
                let lamports = nonce_keyed.account.borrow().lamports();
                let result = nonce_keyed.withdraw_nonce_account(
                    lamports,
                    to_keyed,
                    &recent_blockhashes,
                    &rent,
                    &signers,
                    &MockInvokeContext::new(vec![]),
                );
                assert_eq!(result, Err(InstructionError::MissingRequiredSignature),);
            })
        })
    }

    #[test]
    fn withdraw_inx_unintialized_acc_insuff_funds_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_keyed| {
            let state = AccountUtilsState::<Versions>::state(nonce_keyed)
                .unwrap()
                .convert_to_current();
            assert_eq!(state, State::Uninitialized);
            with_test_keyed_account(42, false, |to_keyed| {
                let mut signers = HashSet::new();
                signers.insert(*nonce_keyed.signer_key().unwrap());
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(0);
                let lamports = nonce_keyed.account.borrow().lamports() + 1;
                let result = nonce_keyed.withdraw_nonce_account(
                    lamports,
                    to_keyed,
                    &recent_blockhashes,
                    &rent,
                    &signers,
                    &MockInvokeContext::new(vec![]),
                );
                assert_eq!(result, Err(InstructionError::InsufficientFunds));
            })
        })
    }

    #[test]
    fn withdraw_inx_uninitialized_acc_two_withdraws_ok() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_keyed| {
            with_test_keyed_account(42, false, |to_keyed| {
                let mut signers = HashSet::new();
                signers.insert(*nonce_keyed.signer_key().unwrap());
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(0);
                let withdraw_lamports = nonce_keyed.account.borrow().lamports() / 2;
                let nonce_expect_lamports =
                    nonce_keyed.account.borrow().lamports() - withdraw_lamports;
                let to_expect_lamports = to_keyed.account.borrow().lamports() + withdraw_lamports;
                nonce_keyed
                    .withdraw_nonce_account(
                        withdraw_lamports,
                        to_keyed,
                        &recent_blockhashes,
                        &rent,
                        &signers,
                        &MockInvokeContext::new(vec![]),
                    )
                    .unwrap();
                let state = AccountUtilsState::<Versions>::state(nonce_keyed)
                    .unwrap()
                    .convert_to_current();
                assert_eq!(state, State::Uninitialized);
                assert_eq!(
                    nonce_keyed.account.borrow().lamports(),
                    nonce_expect_lamports
                );
                assert_eq!(to_keyed.account.borrow().lamports(), to_expect_lamports);
                let withdraw_lamports = nonce_keyed.account.borrow().lamports();
                let nonce_expect_lamports =
                    nonce_keyed.account.borrow().lamports() - withdraw_lamports;
                let to_expect_lamports = to_keyed.account.borrow().lamports() + withdraw_lamports;
                nonce_keyed
                    .withdraw_nonce_account(
                        withdraw_lamports,
                        to_keyed,
                        &recent_blockhashes,
                        &rent,
                        &signers,
                        &MockInvokeContext::new(vec![]),
                    )
                    .unwrap();
                let state = AccountUtilsState::<Versions>::state(nonce_keyed)
                    .unwrap()
                    .convert_to_current();
                assert_eq!(state, State::Uninitialized);
                assert_eq!(
                    nonce_keyed.account.borrow().lamports(),
                    nonce_expect_lamports
                );
                assert_eq!(to_keyed.account.borrow().lamports(), to_expect_lamports);
            })
        })
    }

    #[test]
    fn withdraw_inx_initialized_acc_two_withdraws_ok() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_keyed| {
            let mut signers = HashSet::new();
            signers.insert(*nonce_keyed.signer_key().unwrap());
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(31);
            let authority = *nonce_keyed.unsigned_key();
            nonce_keyed
                .initialize_nonce_account(
                    &authority,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            let state = AccountUtilsState::<Versions>::state(nonce_keyed)
                .unwrap()
                .convert_to_current();
            #[allow(deprecated)]
            let data = nonce::state::Data {
                authority,
                blockhash: recent_blockhashes[0].blockhash,
                fee_calculator: recent_blockhashes[0].fee_calculator.clone(),
            };
            assert_eq!(state, State::Initialized(data.clone()));
            with_test_keyed_account(42, false, |to_keyed| {
                let withdraw_lamports = nonce_keyed.account.borrow().lamports() - min_lamports;
                let nonce_expect_lamports =
                    nonce_keyed.account.borrow().lamports() - withdraw_lamports;
                let to_expect_lamports = to_keyed.account.borrow().lamports() + withdraw_lamports;
                nonce_keyed
                    .withdraw_nonce_account(
                        withdraw_lamports,
                        to_keyed,
                        &recent_blockhashes,
                        &rent,
                        &signers,
                        &MockInvokeContext::new(vec![]),
                    )
                    .unwrap();
                let state = AccountUtilsState::<Versions>::state(nonce_keyed)
                    .unwrap()
                    .convert_to_current();
                #[allow(deprecated)]
                let data = nonce::state::Data {
                    blockhash: recent_blockhashes[0].blockhash,
                    fee_calculator: recent_blockhashes[0].fee_calculator.clone(),
                    ..data.clone()
                };
                assert_eq!(state, State::Initialized(data));
                assert_eq!(
                    nonce_keyed.account.borrow().lamports(),
                    nonce_expect_lamports
                );
                assert_eq!(to_keyed.account.borrow().lamports(), to_expect_lamports);
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(0);
                let withdraw_lamports = nonce_keyed.account.borrow().lamports();
                let nonce_expect_lamports =
                    nonce_keyed.account.borrow().lamports() - withdraw_lamports;
                let to_expect_lamports = to_keyed.account.borrow().lamports() + withdraw_lamports;
                nonce_keyed
                    .withdraw_nonce_account(
                        withdraw_lamports,
                        to_keyed,
                        &recent_blockhashes,
                        &rent,
                        &signers,
                        &MockInvokeContext::new(vec![]),
                    )
                    .unwrap();
                let state = AccountUtilsState::<Versions>::state(nonce_keyed)
                    .unwrap()
                    .convert_to_current();
                assert_eq!(state, State::Uninitialized);
                assert_eq!(
                    nonce_keyed.account.borrow().lamports(),
                    nonce_expect_lamports
                );
                assert_eq!(to_keyed.account.borrow().lamports(), to_expect_lamports);
            })
        })
    }

    #[test]
    fn withdraw_inx_initialized_acc_nonce_too_early_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_keyed| {
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(0);
            let authorized = *nonce_keyed.unsigned_key();
            nonce_keyed
                .initialize_nonce_account(
                    &authorized,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            with_test_keyed_account(42, false, |to_keyed| {
                let mut signers = HashSet::new();
                signers.insert(*nonce_keyed.signer_key().unwrap());
                let withdraw_lamports = nonce_keyed.account.borrow().lamports();
                let result = nonce_keyed.withdraw_nonce_account(
                    withdraw_lamports,
                    to_keyed,
                    &recent_blockhashes,
                    &rent,
                    &signers,
                    &MockInvokeContext::new(vec![]),
                );
                assert_eq!(result, Err(NonceError::NotExpired.into()));
            })
        })
    }

    #[test]
    fn withdraw_inx_initialized_acc_insuff_funds_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_keyed| {
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(95);
            let authorized = *nonce_keyed.unsigned_key();
            nonce_keyed
                .initialize_nonce_account(
                    &authorized,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            with_test_keyed_account(42, false, |to_keyed| {
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(63);
                let mut signers = HashSet::new();
                signers.insert(*nonce_keyed.signer_key().unwrap());
                let withdraw_lamports = nonce_keyed.account.borrow().lamports() + 1;
                let result = nonce_keyed.withdraw_nonce_account(
                    withdraw_lamports,
                    to_keyed,
                    &recent_blockhashes,
                    &rent,
                    &signers,
                    &MockInvokeContext::new(vec![]),
                );
                assert_eq!(result, Err(InstructionError::InsufficientFunds));
            })
        })
    }

    #[test]
    fn withdraw_inx_initialized_acc_insuff_rent_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_keyed| {
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(95);
            let authorized = *nonce_keyed.unsigned_key();
            nonce_keyed
                .initialize_nonce_account(
                    &authorized,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            with_test_keyed_account(42, false, |to_keyed| {
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(63);
                let mut signers = HashSet::new();
                signers.insert(*nonce_keyed.signer_key().unwrap());
                let withdraw_lamports = nonce_keyed.account.borrow().lamports() - min_lamports + 1;
                let result = nonce_keyed.withdraw_nonce_account(
                    withdraw_lamports,
                    to_keyed,
                    &recent_blockhashes,
                    &rent,
                    &signers,
                    &MockInvokeContext::new(vec![]),
                );
                assert_eq!(result, Err(InstructionError::InsufficientFunds));
            })
        })
    }

    #[test]
    fn withdraw_inx_overflow() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_keyed| {
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(95);
            let authorized = *nonce_keyed.unsigned_key();
            nonce_keyed
                .initialize_nonce_account(
                    &authorized,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            with_test_keyed_account(55, false, |to_keyed| {
                #[allow(deprecated)]
                let recent_blockhashes = create_test_recent_blockhashes(63);
                let mut signers = HashSet::new();
                signers.insert(*nonce_keyed.signer_key().unwrap());
                let withdraw_lamports = u64::MAX - 54;
                let result = nonce_keyed.withdraw_nonce_account(
                    withdraw_lamports,
                    to_keyed,
                    &recent_blockhashes,
                    &rent,
                    &signers,
                    &MockInvokeContext::new(vec![]),
                );
                assert_eq!(result, Err(InstructionError::InsufficientFunds));
            })
        })
    }

    #[test]
    fn initialize_inx_ok() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |keyed_account| {
            let state = AccountUtilsState::<Versions>::state(keyed_account)
                .unwrap()
                .convert_to_current();
            assert_eq!(state, State::Uninitialized);
            let mut signers = HashSet::new();
            signers.insert(*keyed_account.signer_key().unwrap());
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(0);
            let authority = *keyed_account.unsigned_key();
            let result = keyed_account.initialize_nonce_account(
                &authority,
                &recent_blockhashes,
                &rent,
                &MockInvokeContext::new(vec![]),
            );
            #[allow(deprecated)]
            let data = nonce::state::Data {
                authority,
                blockhash: recent_blockhashes[0].blockhash,
                fee_calculator: recent_blockhashes[0].fee_calculator.clone(),
            };
            assert_eq!(result, Ok(()));
            let state = AccountUtilsState::<Versions>::state(keyed_account)
                .unwrap()
                .convert_to_current();
            assert_eq!(state, State::Initialized(data));
        })
    }

    #[test]
    fn initialize_inx_empty_recent_blockhashes_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |keyed_account| {
            let mut signers = HashSet::new();
            signers.insert(*keyed_account.signer_key().unwrap());
            let recent_blockhashes = vec![].into_iter().collect();
            let authorized = *keyed_account.unsigned_key();
            let result = keyed_account.initialize_nonce_account(
                &authorized,
                &recent_blockhashes,
                &rent,
                &MockInvokeContext::new(vec![]),
            );
            assert_eq!(result, Err(NonceError::NoRecentBlockhashes.into()));
        })
    }

    #[test]
    fn initialize_inx_initialized_account_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |keyed_account| {
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(31);
            let authorized = *keyed_account.unsigned_key();
            keyed_account
                .initialize_nonce_account(
                    &authorized,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(0);
            let result = keyed_account.initialize_nonce_account(
                &authorized,
                &recent_blockhashes,
                &rent,
                &MockInvokeContext::new(vec![]),
            );
            assert_eq!(result, Err(NonceError::BadAccountState.into()));
        })
    }

    #[test]
    fn initialize_inx_uninitialized_acc_insuff_funds_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports - 42, true, |keyed_account| {
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(63);
            let authorized = *keyed_account.unsigned_key();
            let result = keyed_account.initialize_nonce_account(
                &authorized,
                &recent_blockhashes,
                &rent,
                &MockInvokeContext::new(vec![]),
            );
            assert_eq!(result, Err(InstructionError::InsufficientFunds));
        })
    }

    #[test]
    fn authorize_inx_ok() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_account| {
            let mut signers = HashSet::new();
            signers.insert(*nonce_account.signer_key().unwrap());
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(31);
            let authorized = *nonce_account.unsigned_key();
            nonce_account
                .initialize_nonce_account(
                    &authorized,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            let authority = Pubkey::default();
            #[allow(deprecated)]
            let data = nonce::state::Data {
                authority,
                blockhash: recent_blockhashes[0].blockhash,
                fee_calculator: recent_blockhashes[0].fee_calculator.clone(),
            };
            let result = nonce_account.authorize_nonce_account(
                &Pubkey::default(),
                &signers,
                &MockInvokeContext::new(vec![]),
            );
            assert_eq!(result, Ok(()));
            let state = AccountUtilsState::<Versions>::state(nonce_account)
                .unwrap()
                .convert_to_current();
            assert_eq!(state, State::Initialized(data));
        })
    }

    #[test]
    fn authorize_inx_uninitialized_state_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_account| {
            let mut signers = HashSet::new();
            signers.insert(*nonce_account.signer_key().unwrap());
            let result = nonce_account.authorize_nonce_account(
                &Pubkey::default(),
                &signers,
                &MockInvokeContext::new(vec![]),
            );
            assert_eq!(result, Err(NonceError::BadAccountState.into()));
        })
    }

    #[test]
    fn authorize_inx_bad_authority_fail() {
        let rent = Rent {
            lamports_per_byte_year: 42,
            ..Rent::default()
        };
        let min_lamports = rent.minimum_balance(State::size());
        with_test_keyed_account(min_lamports + 42, true, |nonce_account| {
            let mut signers = HashSet::new();
            signers.insert(*nonce_account.signer_key().unwrap());
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(31);
            let authorized = &Pubkey::default().clone();
            nonce_account
                .initialize_nonce_account(
                    authorized,
                    &recent_blockhashes,
                    &rent,
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            let result = nonce_account.authorize_nonce_account(
                &Pubkey::default(),
                &signers,
                &MockInvokeContext::new(vec![]),
            );
            assert_eq!(result, Err(InstructionError::MissingRequiredSignature));
        })
    }

    #[test]
    fn verify_nonce_ok() {
        with_test_keyed_account(42, true, |nonce_account| {
            let mut signers = HashSet::new();
            signers.insert(nonce_account.signer_key().unwrap());
            let state: State = nonce_account.state().unwrap();
            // New is in Uninitialzed state
            assert_eq!(state, State::Uninitialized);
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(0);
            let authorized = nonce_account.unsigned_key();
            nonce_account
                .initialize_nonce_account(
                    authorized,
                    &recent_blockhashes,
                    &Rent::free(),
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            assert!(verify_nonce_account(
                &nonce_account.account.borrow(),
                #[allow(deprecated)]
                &recent_blockhashes[0].blockhash,
            ));
        });
    }

    #[test]
    fn verify_nonce_bad_acc_state_fail() {
        with_test_keyed_account(42, true, |nonce_account| {
            assert!(!verify_nonce_account(
                &nonce_account.account.borrow(),
                &Hash::default()
            ));
        });
    }

    #[test]
    fn verify_nonce_bad_query_hash_fail() {
        with_test_keyed_account(42, true, |nonce_account| {
            let mut signers = HashSet::new();
            signers.insert(nonce_account.signer_key().unwrap());
            let state: State = nonce_account.state().unwrap();
            // New is in Uninitialzed state
            assert_eq!(state, State::Uninitialized);
            #[allow(deprecated)]
            let recent_blockhashes = create_test_recent_blockhashes(0);
            let authorized = nonce_account.unsigned_key();
            nonce_account
                .initialize_nonce_account(
                    authorized,
                    &recent_blockhashes,
                    &Rent::free(),
                    &MockInvokeContext::new(vec![]),
                )
                .unwrap();
            assert!(!verify_nonce_account(
                &nonce_account.account.borrow(),
                #[allow(deprecated)]
                &recent_blockhashes[1].blockhash,
            ));
        });
    }
}
