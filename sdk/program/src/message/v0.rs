#![allow(clippy::integer_arithmetic)]

use super::MessageHeader;
use crate::{
    hash::Hash,
    instruction::CompiledInstruction,
    pubkey::Pubkey,
    sanitize::{Sanitize, SanitizeError},
    short_vec,
};

/// Alternative version of `Message` that supports succinct account loading
/// through an on-chain address map.
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq, Clone, AbiExample)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    /// The message header, identifying signed and read-only `account_keys`
    pub header: MessageHeader,

    /// List of accounts loaded by this transaction.
    #[serde(with = "short_vec")]
    pub account_keys: Vec<Pubkey>,

    /// List of address maps used to succinctly load additional accounts for
    /// this transaction.
    ///
    /// # Notes
    ///
    /// The last `address_maps.len()` accounts of the read-only unsigned
    /// accounts are loaded as address maps.
    #[serde(with = "short_vec")]
    pub address_maps: Vec<AddressMap>,

    /// The blockhash of a recent block.
    pub recent_blockhash: Hash,

    /// Instructions that invoke a designated program, are executed in sequence,
    /// and committed in one atomic transaction if all succeed.
    ///
    /// # Notes
    ///
    /// Account indices will index into the list of addresses constructed from
    /// the concatenation of `account_keys` and the `entries` of each address
    /// map in sequential order.
    #[serde(with = "short_vec")]
    pub instructions: Vec<CompiledInstruction>,
}

impl Sanitize for Message {
    fn sanitize(&self) -> Result<(), SanitizeError> {
        // signing area and read-only non-signing area should not
        // overlap
        if self.header.num_required_signatures as usize
            + self.header.num_readonly_unsigned_accounts as usize
            > self.account_keys.len()
        {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        // there should be at least 1 RW fee-payer account.
        if self.header.num_readonly_signed_accounts >= self.header.num_required_signatures {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        // there cannot be more address maps than read-only unsigned accounts.
        if self.address_maps.len() > self.header.num_readonly_unsigned_accounts as usize {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        // each map must load at least one entry
        let mut num_loaded_accounts = self.account_keys.len();
        for map in &self.address_maps {
            let num_loaded_map_entries = map
                .read_only_entries
                .len()
                .saturating_add(map.writable_entries.len());

            if num_loaded_map_entries == 0 {
                return Err(SanitizeError::InvalidValue);
            }

            num_loaded_accounts = num_loaded_accounts.saturating_add(num_loaded_map_entries);
        }

        // the number of loaded accounts must be <= 256 since account indices are
        // encoded as `u8`
        if num_loaded_accounts > 256 {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        for ci in &self.instructions {
            if ci.program_id_index as usize >= num_loaded_accounts {
                return Err(SanitizeError::IndexOutOfBounds);
            }
            // A program cannot be a payer.
            if ci.program_id_index == 0 {
                return Err(SanitizeError::IndexOutOfBounds);
            }
            for ai in &ci.accounts {
                if *ai as usize >= num_loaded_accounts {
                    return Err(SanitizeError::IndexOutOfBounds);
                }
            }
        }

        Ok(())
    }
}

/// Address map specifies read-only and writable entries
/// to load for the transaction.
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq, Clone, AbiExample)]
#[serde(rename_all = "camelCase")]
pub struct AddressMap {
    /// List of map entries to load as read-only.
    #[serde(with = "short_vec")]
    pub read_only_entries: Vec<u8>,
    /// List of map entries to load as read-write.
    #[serde(with = "short_vec")]
    pub writable_entries: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_message() -> Message {
        Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![Pubkey::new_unique(), Pubkey::new_unique()],
            address_maps: vec![AddressMap {
                read_only_entries: vec![0],
                writable_entries: vec![],
            }],
            ..Message::default()
        }
    }

    fn two_map_message() -> Message {
        Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 2,
            },
            account_keys: vec![
                Pubkey::new_unique(),
                Pubkey::new_unique(),
                Pubkey::new_unique(),
            ],
            address_maps: vec![
                AddressMap {
                    read_only_entries: vec![0],
                    writable_entries: vec![1],
                },
                AddressMap {
                    read_only_entries: vec![1],
                    writable_entries: vec![0],
                },
            ],
            ..Message::default()
        }
    }

    #[test]
    fn test_sanitize_account_indices() {
        assert!(Message {
            account_keys: (0..=u8::MAX).map(|_| Pubkey::new_unique()).collect(),
            address_maps: vec![],
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![u8::MAX],
                data: vec![],
            }],
            ..simple_message()
        }
        .sanitize()
        .is_ok());

        assert!(Message {
            account_keys: (0..u8::MAX).map(|_| Pubkey::new_unique()).collect(),
            address_maps: vec![],
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![u8::MAX],
                data: vec![],
            }],
            ..simple_message()
        }
        .sanitize()
        .is_err());

        assert!(Message {
            account_keys: (0..u8::MAX).map(|_| Pubkey::new_unique()).collect(),
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![u8::MAX],
                data: vec![],
            }],
            ..simple_message()
        }
        .sanitize()
        .is_ok());

        assert!(Message {
            account_keys: (0..u8::MAX - 1).map(|_| Pubkey::new_unique()).collect(),
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![u8::MAX],
                data: vec![],
            }],
            ..simple_message()
        }
        .sanitize()
        .is_err());

        assert!(Message {
            address_maps: vec![
                AddressMap {
                    read_only_entries: (0..200).step_by(2).collect(),
                    writable_entries: (1..200).step_by(2).collect(),
                },
                AddressMap {
                    read_only_entries: (0..53).step_by(2).collect(),
                    writable_entries: (1..53).step_by(2).collect(),
                },
            ],
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![u8::MAX],
                data: vec![],
            }],
            ..two_map_message()
        }
        .sanitize()
        .is_ok());

        assert!(Message {
            address_maps: vec![
                AddressMap {
                    read_only_entries: (0..200).step_by(2).collect(),
                    writable_entries: (1..200).step_by(2).collect(),
                },
                AddressMap {
                    read_only_entries: (0..52).step_by(2).collect(),
                    writable_entries: (1..52).step_by(2).collect(),
                },
            ],
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![u8::MAX],
                data: vec![],
            }],
            ..two_map_message()
        }
        .sanitize()
        .is_err());
    }

    #[test]
    fn test_sanitize_excessive_loaded_accounts() {
        assert!(Message {
            account_keys: (0..=u8::MAX).map(|_| Pubkey::new_unique()).collect(),
            address_maps: vec![],
            ..simple_message()
        }
        .sanitize()
        .is_ok());

        assert!(Message {
            account_keys: (0..257).map(|_| Pubkey::new_unique()).collect(),
            address_maps: vec![],
            ..simple_message()
        }
        .sanitize()
        .is_err());

        assert!(Message {
            account_keys: (0..u8::MAX).map(|_| Pubkey::new_unique()).collect(),
            ..simple_message()
        }
        .sanitize()
        .is_ok());

        assert!(Message {
            account_keys: (0..256).map(|_| Pubkey::new_unique()).collect(),
            ..simple_message()
        }
        .sanitize()
        .is_err());

        assert!(Message {
            address_maps: vec![
                AddressMap {
                    read_only_entries: (0..200).step_by(2).collect(),
                    writable_entries: (1..200).step_by(2).collect(),
                },
                AddressMap {
                    read_only_entries: (0..53).step_by(2).collect(),
                    writable_entries: (1..53).step_by(2).collect(),
                }
            ],
            ..two_map_message()
        }
        .sanitize()
        .is_ok());

        assert!(Message {
            address_maps: vec![
                AddressMap {
                    read_only_entries: (0..200).step_by(2).collect(),
                    writable_entries: (1..200).step_by(2).collect(),
                },
                AddressMap {
                    read_only_entries: (0..200).step_by(2).collect(),
                    writable_entries: (1..200).step_by(2).collect(),
                }
            ],
            ..two_map_message()
        }
        .sanitize()
        .is_err());
    }

    #[test]
    fn test_sanitize_excessive_maps() {
        assert!(Message {
            header: MessageHeader {
                num_readonly_unsigned_accounts: 1,
                ..simple_message().header
            },
            ..simple_message()
        }
        .sanitize()
        .is_ok());

        assert!(Message {
            header: MessageHeader {
                num_readonly_unsigned_accounts: 0,
                ..simple_message().header
            },
            ..simple_message()
        }
        .sanitize()
        .is_err());
    }

    #[test]
    fn test_sanitize_address_map() {
        assert!(Message {
            address_maps: vec![AddressMap {
                read_only_entries: vec![],
                writable_entries: vec![0],
            }],
            ..simple_message()
        }
        .sanitize()
        .is_ok());

        assert!(Message {
            address_maps: vec![AddressMap {
                read_only_entries: vec![0],
                writable_entries: vec![],
            }],
            ..simple_message()
        }
        .sanitize()
        .is_ok());

        assert!(Message {
            address_maps: vec![AddressMap {
                read_only_entries: vec![],
                writable_entries: vec![],
            }],
            ..simple_message()
        }
        .sanitize()
        .is_err());
    }
}
