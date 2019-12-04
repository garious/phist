<<<<<<< HEAD
=======
pub mod address_generator;
pub mod genesis_accounts;
pub mod stakes;
pub mod unlocks;
pub mod validators;

>>>>>>> 369f37a0a... genesis validators (#7235)
use serde::{Deserialize, Serialize};

/// An account where the data is encoded as a Base64 string.
#[derive(Serialize, Deserialize, Debug)]
pub struct Base64Account {
    pub balance: u64,
    pub owner: String,
    pub data: String,
    pub executable: bool,
}
