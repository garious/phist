use solana_sdk::{account::Account, clock::Slot};

#[derive(Serialize, Deserialize, Debug)]
pub struct AccountsUpdate {
    slot: Slot,
    accounts: Vec<Account>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SlotUpdateType {
    Rooted,
    OptimisticallyConfirmed,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SlotUpdate {
    slot: Slot,
    update: SlotUpdateType,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RpcNodePacket {
    AccountsUpdate(AccountsUpdate),
    SlotUpdate(SlotUpdate),
}