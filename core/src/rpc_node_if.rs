use solana_sdk::{account::Account, clock::Slot};

#[derive(Serialize, Deserialize)]
pub struct AccountsUpdate {
    slot: Slot,
    accounts: Vec<Account>,
}

#[derive(Serialize, Deserialize)]
pub enum SlotUpdateType {
    Rooted,
    OptimisticallyConfirmed,
}

#[derive(Serialize, Deserialize)]
pub struct SlotUpdate {
    slot: Slot,
    update: SlotUpdateType,
}

#[derive(Serialize, Deserialize)]
pub enum RpcNodePacket {
    AccountsUpdate(AccountsUpdate),
    SlotUpdate(SlotUpdate),
}