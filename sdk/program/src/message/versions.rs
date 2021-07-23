use {
    crate::{
        hash::Hash,
        instruction::CompiledInstruction,
        message::{v0, Message, MessageHeader},
        pubkey::Pubkey,
        sanitize::{Sanitize, SanitizeError},
        short_vec,
    },
    serde::{
        de::{self, Deserializer, SeqAccess, Visitor},
        ser::{SerializeTuple, Serializer},
        {Deserialize, Serialize},
    },
    std::fmt,
};

/// Bit mask that indicates whether a serialized message is versioned.
const VERSION_PREFIX: u8 = 0x80;

/// Message versions supported by the Solana runtime.
///
/// # Serialization
///
/// If the first bit is set, the remaining 7 bits will be used to determine
/// which message version is serialized starting from version `0`. If the first
/// is bit is not set, all bytes are used to encode the original `Message`
/// format.
#[frozen_abi(digest = "DeuuRstA25YgNwvzBmuBjK3jZobPA7Fz83DxnNi6cBX1")]
#[derive(Debug, PartialEq, Eq, Clone, AbiEnumVisitor, AbiExample)]
pub enum MessageVersions {
    Original(Message),
    V0(v0::Message),
}

impl Sanitize for MessageVersions {
    fn sanitize(&self) -> Result<(), SanitizeError> {
        match self {
            Self::Original(message) => message.sanitize(),
            Self::V0(message) => message.sanitize(),
        }
    }
}

impl Serialize for MessageVersions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Original(message) => {
                let mut seq = serializer.serialize_tuple(1)?;
                seq.serialize_element(message)?;
                seq.end()
            }
            Self::V0(message) => {
                let mut seq = serializer.serialize_tuple(2)?;
                seq.serialize_element(&VERSION_PREFIX)?;
                seq.serialize_element(message)?;
                seq.end()
            }
        }
    }
}

enum MessagePrefix {
    Original(u8),
    Versioned(u8),
}

impl<'de> Deserialize<'de> for MessagePrefix {
    fn deserialize<D>(deserializer: D) -> Result<MessagePrefix, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PrefixVisitor;

        impl<'de> Visitor<'de> for PrefixVisitor {
            type Value = MessagePrefix;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("message prefix byte")
            }

            fn visit_u8<E>(self, byte: u8) -> Result<MessagePrefix, E> {
                if byte & VERSION_PREFIX != 0 {
                    Ok(MessagePrefix::Versioned(byte ^ VERSION_PREFIX))
                } else {
                    Ok(MessagePrefix::Original(byte))
                }
            }
        }

        deserializer.deserialize_u8(PrefixVisitor)
    }
}

impl<'de> Deserialize<'de> for MessageVersions {
    fn deserialize<D>(deserializer: D) -> Result<MessageVersions, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MessageVisitor;

        impl<'de> Visitor<'de> for MessageVisitor {
            type Value = MessageVersions;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("message bytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<MessageVersions, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let prefix: MessagePrefix = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;

                match prefix {
                    MessagePrefix::Original(num_required_signatures) => {
                        // The remaining fields of the original Message struct after the first byte.
                        #[derive(Serialize, Deserialize)]
                        struct RemainingMessage {
                            pub num_readonly_signed_accounts: u8,
                            pub num_readonly_unsigned_accounts: u8,
                            #[serde(with = "short_vec")]
                            pub account_keys: Vec<Pubkey>,
                            pub recent_blockhash: Hash,
                            #[serde(with = "short_vec")]
                            pub instructions: Vec<CompiledInstruction>,
                        }

                        let rm: RemainingMessage = seq.next_element()?.ok_or_else(|| {
                            // will never happen since tuple length is always 2
                            de::Error::invalid_length(1, &self)
                        })?;

                        Ok(MessageVersions::Original(Message {
                            header: MessageHeader {
                                num_required_signatures,
                                num_readonly_signed_accounts: rm.num_readonly_signed_accounts,
                                num_readonly_unsigned_accounts: rm.num_readonly_unsigned_accounts,
                            },
                            account_keys: rm.account_keys,
                            recent_blockhash: rm.recent_blockhash,
                            instructions: rm.instructions,
                        }))
                    }
                    MessagePrefix::Versioned(version) => {
                        if version == 0 {
                            Ok(MessageVersions::V0(seq.next_element()?.ok_or_else(
                                || {
                                    // will never happen since tuple length is always 2
                                    de::Error::invalid_length(1, &self)
                                },
                            )?))
                        } else {
                            Err(de::Error::invalid_value(
                                de::Unexpected::Unsigned(version as u64),
                                &"supported versions: [0]",
                            ))
                        }
                    }
                }
            }
        }

        deserializer.deserialize_tuple(2, MessageVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        instruction::{AccountMeta, Instruction},
        message::v0::AddressMap,
    };

    #[test]
    fn test_original_message_serialization() {
        let program_id0 = Pubkey::new_unique();
        let program_id1 = Pubkey::new_unique();
        let id0 = Pubkey::new_unique();
        let id1 = Pubkey::new_unique();
        let id2 = Pubkey::new_unique();
        let id3 = Pubkey::new_unique();
        let instructions = vec![
            Instruction::new_with_bincode(program_id0, &0, vec![AccountMeta::new(id0, false)]),
            Instruction::new_with_bincode(program_id0, &0, vec![AccountMeta::new(id1, true)]),
            Instruction::new_with_bincode(
                program_id1,
                &0,
                vec![AccountMeta::new_readonly(id2, false)],
            ),
            Instruction::new_with_bincode(
                program_id1,
                &0,
                vec![AccountMeta::new_readonly(id3, true)],
            ),
        ];

        let mut message = Message::new(&instructions, Some(&id1));
        message.recent_blockhash = Hash::new_unique();

        let bytes1 = bincode::serialize(&message).unwrap();
        let bytes2 = bincode::serialize(&MessageVersions::Original(message.clone())).unwrap();

        assert_eq!(bytes1, bytes2);

        let message1: Message = bincode::deserialize(&bytes1).unwrap();
        let message2: MessageVersions = bincode::deserialize(&bytes2).unwrap();

        if let MessageVersions::Original(message2) = message2 {
            assert_eq!(message, message1);
            assert_eq!(message1, message2);
        } else {
            panic!("should deserialize to original message");
        }
    }

    #[test]
    fn test_versioned_message_serialization() {
        let message = v0::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 2,
            },
            recent_blockhash: Hash::new_unique(),
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
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![],
            }],
        };

        let bytes = bincode::serialize(&MessageVersions::V0(message.clone())).unwrap();
        let message_from_bytes: MessageVersions = bincode::deserialize(&bytes).unwrap();

        if let MessageVersions::V0(message_from_bytes) = message_from_bytes {
            assert_eq!(message, message_from_bytes);
        } else {
            panic!("should deserialize to versioned message");
        }
    }
}
