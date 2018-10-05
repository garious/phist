use pubkey::Pubkey;

/// An Account with userdata that is stored on chain
#[repr(C)]
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Account {
    /// tokens in the account
    pub tokens: i64,
    /// user data
    /// A transaction can write to its userdata
    pub userdata: Vec<u8>,
    /// interpreter that owns this account
    pub interpreter_id: Pubkey,
}

impl Account {
    pub fn new(tokens: i64, space: usize, interpreter_id: Pubkey) -> Account {
        Account {
            tokens,
            userdata: vec![0u8; space],
            interpreter_id,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct KeyedAccount<'a> {
    pub key: &'a Pubkey,
    pub account: &'a mut Account,
}
