use crate::{
    parse_account_data::{ParsableAccount, ParseAccountError},
    StringAmount, UiFeeCalculator,
};
use bincode::deserialize;
use bv::BitVec;
#[allow(deprecated)]
use solana_sdk::sysvar::recent_blockhashes::RecentBlockhashes;
use solana_sdk::{
    clock::{Clock, Epoch, Slot, UnixTimestamp},
    epoch_schedule::EpochSchedule,
    pubkey::Pubkey,
    rent::Rent,
    slot_hashes::SlotHashes,
    slot_history::{self, SlotHistory},
    stake_history::{StakeHistory, StakeHistoryEntry},
    sysvar::{self, fees::Fees, rewards::Rewards},
};

pub fn parse_sysvar(data: &[u8], pubkey: &Pubkey) -> Result<SysvarAccountType, ParseAccountError> {
    #[allow(deprecated)]
    let parsed_account = {
        if pubkey == &sysvar::clock::id() {
            deserialize::<Clock>(data)
                .ok()
                .map(|clock| SysvarAccountType::Clock(clock.into()))
        } else if pubkey == &sysvar::epoch_schedule::id() {
            deserialize(data).ok().map(SysvarAccountType::EpochSchedule)
        } else if pubkey == &sysvar::fees::id() {
            deserialize::<Fees>(data)
                .ok()
                .map(|fees| SysvarAccountType::Fees(fees.into()))
        } else if pubkey == &sysvar::recent_blockhashes::id() {
            deserialize::<RecentBlockhashes>(data)
                .ok()
                .map(|recent_blockhashes| {
                    let recent_blockhashes = recent_blockhashes
                        .iter()
                        .map(|entry| UiRecentBlockhashesEntry {
                            blockhash: entry.blockhash.to_string(),
                            fee_calculator: entry.fee_calculator.clone().into(),
                        })
                        .collect();
                    SysvarAccountType::RecentBlockhashes(recent_blockhashes)
                })
        } else if pubkey == &sysvar::rent::id() {
            deserialize::<Rent>(data)
                .ok()
                .map(|rent| SysvarAccountType::Rent(rent.into()))
        } else if pubkey == &sysvar::rewards::id() {
            deserialize::<Rewards>(data)
                .ok()
                .map(|rewards| SysvarAccountType::Rewards(rewards.into()))
        } else if pubkey == &sysvar::slot_hashes::id() {
            deserialize::<SlotHashes>(data).ok().map(|slot_hashes| {
                let slot_hashes = slot_hashes
                    .iter()
                    .map(|slot_hash| UiSlotHashEntry {
                        slot: slot_hash.0,
                        hash: slot_hash.1.to_string(),
                    })
                    .collect();
                SysvarAccountType::SlotHashes(slot_hashes)
            })
        } else if pubkey == &sysvar::slot_history::id() {
            deserialize::<SlotHistory>(data).ok().map(|slot_history| {
                SysvarAccountType::SlotHistory(UiSlotHistory {
                    next_slot: slot_history.next_slot,
                    bits: format!("{:?}", SlotHistoryBits(slot_history.bits)),
                })
            })
        } else if pubkey == &sysvar::stake_history::id() {
            deserialize::<StakeHistory>(data).ok().map(|stake_history| {
                let stake_history = stake_history
                    .iter()
                    .map(|entry| UiStakeHistoryEntry {
                        epoch: entry.0,
                        stake_history: entry.1.clone(),
                    })
                    .collect();
                SysvarAccountType::StakeHistory(stake_history)
            })
        } else {
            None
        }
    };
    parsed_account.ok_or(ParseAccountError::AccountNotParsable(
        ParsableAccount::Sysvar,
    ))
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type", content = "info")]
pub enum SysvarAccountType {
    Clock(UiClock),
    EpochSchedule(EpochSchedule),
    Fees(UiFees),
    RecentBlockhashes(Vec<UiRecentBlockhashesEntry>),
    Rent(UiRent),
    Rewards(UiRewards),
    SlotHashes(Vec<UiSlotHashEntry>),
    SlotHistory(UiSlotHistory),
    StakeHistory(Vec<UiStakeHistoryEntry>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UiClock {
    pub slot: Slot,
    pub epoch: Epoch,
    pub epoch_start_timestamp: UnixTimestamp,
    pub leader_schedule_epoch: Epoch,
    pub unix_timestamp: UnixTimestamp,
}

impl From<Clock> for UiClock {
    fn from(clock: Clock) -> Self {
        Self {
            slot: clock.slot,
            epoch: clock.epoch,
            epoch_start_timestamp: clock.epoch_start_timestamp,
            leader_schedule_epoch: clock.leader_schedule_epoch,
            unix_timestamp: clock.unix_timestamp,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UiFees {
    pub fee_calculator: UiFeeCalculator,
}
impl From<Fees> for UiFees {
    fn from(fees: Fees) -> Self {
        Self {
            fee_calculator: fees.fee_calculator.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UiRent {
    pub lamports_per_byte_year: StringAmount,
    pub exemption_threshold: f64,
    pub burn_percent: u8,
}

impl From<Rent> for UiRent {
    fn from(rent: Rent) -> Self {
        Self {
            lamports_per_byte_year: rent.lamports_per_byte_year.to_string(),
            exemption_threshold: rent.exemption_threshold,
            burn_percent: rent.burn_percent,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UiRewards {
    pub validator_point_value: f64,
}

impl From<Rewards> for UiRewards {
    fn from(rewards: Rewards) -> Self {
        Self {
            validator_point_value: rewards.validator_point_value,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UiRecentBlockhashesEntry {
    pub blockhash: String,
    pub fee_calculator: UiFeeCalculator,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UiSlotHashEntry {
    pub slot: Slot,
    pub hash: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UiSlotHistory {
    pub next_slot: Slot,
    pub bits: String,
}

struct SlotHistoryBits(BitVec<u64>);

impl std::fmt::Debug for SlotHistoryBits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in 0..slot_history::MAX_ENTRIES {
            if self.0.get(i) {
                write!(f, "1")?;
            } else {
                write!(f, "0")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UiStakeHistoryEntry {
    pub epoch: Epoch,
    pub stake_history: StakeHistoryEntry,
}

#[cfg(test)]
mod test {
    use super::*;
    #[allow(deprecated)]
    use solana_sdk::sysvar::recent_blockhashes::IterItem;
    use solana_sdk::{account::create_account_for_test, fee_calculator::FeeCalculator, hash::Hash};

    #[test]
    fn test_parse_sysvars() {
        let clock_sysvar = create_account_for_test(&Clock::default());
        assert_eq!(
            parse_sysvar(&clock_sysvar.data, &sysvar::clock::id()).unwrap(),
            SysvarAccountType::Clock(UiClock::default()),
        );

        let epoch_schedule = EpochSchedule {
            slots_per_epoch: 12,
            leader_schedule_slot_offset: 0,
            warmup: false,
            first_normal_epoch: 1,
            first_normal_slot: 12,
        };
        let epoch_schedule_sysvar = create_account_for_test(&epoch_schedule);
        assert_eq!(
            parse_sysvar(&epoch_schedule_sysvar.data, &sysvar::epoch_schedule::id()).unwrap(),
            SysvarAccountType::EpochSchedule(epoch_schedule),
        );

        let fees_sysvar = create_account_for_test(&Fees::default());
        assert_eq!(
            parse_sysvar(&fees_sysvar.data, &sysvar::fees::id()).unwrap(),
            SysvarAccountType::Fees(UiFees::default()),
        );

        let hash = Hash::new(&[1; 32]);
        let fee_calculator = FeeCalculator {
            lamports_per_signature: 10,
        };
        #[allow(deprecated)]
        let recent_blockhashes: RecentBlockhashes = vec![IterItem(0, &hash, &fee_calculator)]
            .into_iter()
            .collect();
        let recent_blockhashes_sysvar = create_account_for_test(&recent_blockhashes);
        assert_eq!(
            parse_sysvar(
                &recent_blockhashes_sysvar.data,
                &sysvar::recent_blockhashes::id()
            )
            .unwrap(),
            SysvarAccountType::RecentBlockhashes(vec![UiRecentBlockhashesEntry {
                blockhash: hash.to_string(),
                fee_calculator: fee_calculator.into(),
            }]),
        );

        let rent = Rent {
            lamports_per_byte_year: 10,
            exemption_threshold: 2.0,
            burn_percent: 5,
        };
        let rent_sysvar = create_account_for_test(&rent);
        assert_eq!(
            parse_sysvar(&rent_sysvar.data, &sysvar::rent::id()).unwrap(),
            SysvarAccountType::Rent(rent.into()),
        );

        let rewards_sysvar = create_account_for_test(&Rewards::default());
        assert_eq!(
            parse_sysvar(&rewards_sysvar.data, &sysvar::rewards::id()).unwrap(),
            SysvarAccountType::Rewards(UiRewards::default()),
        );

        let mut slot_hashes = SlotHashes::default();
        slot_hashes.add(1, hash);
        let slot_hashes_sysvar = create_account_for_test(&slot_hashes);
        assert_eq!(
            parse_sysvar(&slot_hashes_sysvar.data, &sysvar::slot_hashes::id()).unwrap(),
            SysvarAccountType::SlotHashes(vec![UiSlotHashEntry {
                slot: 1,
                hash: hash.to_string(),
            }]),
        );

        let mut slot_history = SlotHistory::default();
        slot_history.add(42);
        let slot_history_sysvar = create_account_for_test(&slot_history);
        assert_eq!(
            parse_sysvar(&slot_history_sysvar.data, &sysvar::slot_history::id()).unwrap(),
            SysvarAccountType::SlotHistory(UiSlotHistory {
                next_slot: slot_history.next_slot,
                bits: format!("{:?}", SlotHistoryBits(slot_history.bits)),
            }),
        );

        let mut stake_history = StakeHistory::default();
        let stake_history_entry = StakeHistoryEntry {
            effective: 10,
            activating: 2,
            deactivating: 3,
        };
        stake_history.add(1, stake_history_entry.clone());
        let stake_history_sysvar = create_account_for_test(&stake_history);
        assert_eq!(
            parse_sysvar(&stake_history_sysvar.data, &sysvar::stake_history::id()).unwrap(),
            SysvarAccountType::StakeHistory(vec![UiStakeHistoryEntry {
                epoch: 1,
                stake_history: stake_history_entry,
            }]),
        );

        let bad_pubkey = solana_sdk::pubkey::new_rand();
        assert!(parse_sysvar(&stake_history_sysvar.data, &bad_pubkey).is_err());

        let bad_data = vec![0; 4];
        assert!(parse_sysvar(&bad_data, &sysvar::stake_history::id()).is_err());
    }
}
