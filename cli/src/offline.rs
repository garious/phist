use clap::{App, Arg, ArgMatches};
use solana_clap_utils::{
    input_parsers::value_of,
    input_validators::{is_hash, is_pubkey_sig},
    ArgConstant,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{fee_calculator::FeeCalculator, hash::Hash};

pub const BLOCKHASH_ARG: ArgConstant<'static> = ArgConstant {
    name: "blockhash",
    long: "blockhash",
    help: "Use the supplied blockhash",
};

pub const SIGN_ONLY_ARG: ArgConstant<'static> = ArgConstant {
    name: "sign_only",
    long: "sign-only",
    help: "Sign the transaction offline",
};

pub const SIGNER_ARG: ArgConstant<'static> = ArgConstant {
    name: "signer",
    long: "signer",
    help: "Provid a public-key/signature pair for the transaction",
};

#[derive(Clone, Debug, PartialEq)]
pub enum BlockhashSpec {
    Full(Hash, FeeCalculator),
    Partial(Hash),
    Undeclared,
}

impl BlockhashSpec {
    pub fn new(blockhash: Option<Hash>, sign_only: bool) -> Self {
        match blockhash {
            Some(hash) if sign_only => Self::Full(hash, FeeCalculator::default()),
            Some(hash) if !sign_only => Self::Partial(hash),
            None if !sign_only => Self::Undeclared,
            _ => panic!("Cannot resolve blockhash"),
        }
    }

    pub fn new_from_matches(matches: &ArgMatches<'_>) -> Self {
        let blockhash = value_of(matches, BLOCKHASH_ARG.name);
        let sign_only = matches.is_present(SIGN_ONLY_ARG.name);
        BlockhashSpec::new(blockhash, sign_only)
    }

    pub fn get_blockhash_fee_calculator(
        &self,
        rpc_client: &RpcClient,
    ) -> Result<(Hash, FeeCalculator), Box<dyn std::error::Error>> {
        let (hash, fee_calc) = match self {
            BlockhashSpec::Full(hash, fee_calc) => (Some(hash), Some(fee_calc)),
            BlockhashSpec::Partial(hash) => (Some(hash), None),
            BlockhashSpec::Undeclared => (None, None),
        };
        if None == fee_calc {
            let (cluster_hash, fee_calc) = rpc_client.get_recent_blockhash()?;
            Ok((*hash.unwrap_or(&cluster_hash), fee_calc))
        } else {
            Ok((*hash.unwrap(), fee_calc.unwrap().clone()))
        }
    }
}

impl Default for BlockhashSpec {
    fn default() -> Self {
        BlockhashSpec::Undeclared
    }
}

fn blockhash_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name(BLOCKHASH_ARG.name)
        .long(BLOCKHASH_ARG.long)
        .takes_value(true)
        .value_name("BLOCKHASH")
        .validator(is_hash)
        .help(BLOCKHASH_ARG.help)
}

fn sign_only_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name(SIGN_ONLY_ARG.name)
        .long(SIGN_ONLY_ARG.long)
        .takes_value(false)
        .requires(BLOCKHASH_ARG.name)
        .help(SIGN_ONLY_ARG.help)
}

fn signer_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name(SIGNER_ARG.name)
        .long(SIGNER_ARG.long)
        .takes_value(true)
        .value_name("BASE58_PUBKEY=BASE58_SIG")
        .validator(is_pubkey_sig)
        .requires(BLOCKHASH_ARG.name)
        .multiple(true)
        .help(SIGNER_ARG.help)
}

pub trait OfflineArgs {
    fn offline_args(self) -> Self;
}

impl OfflineArgs for App<'_, '_> {
    fn offline_args(self) -> Self {
        self.arg(blockhash_arg())
            .arg(sign_only_arg())
            .arg(signer_arg())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::App;
    use serde_json::{self, json, Value};
    use solana_client::{
        rpc_request::RpcRequest,
        rpc_response::{Response, RpcResponseContext},
    };
    use solana_sdk::{fee_calculator::FeeCalculator, hash::hash};
    use std::collections::HashMap;

    #[test]
    fn test_blockhashspec_new_ok() {
        let blockhash = hash(&[1u8]);

        assert_eq!(
            BlockhashSpec::new(Some(blockhash), true),
            BlockhashSpec::Full(blockhash, FeeCalculator::default()),
        );
        assert_eq!(
            BlockhashSpec::new(Some(blockhash), false),
            BlockhashSpec::Partial(blockhash),
        );
        assert_eq!(
            BlockhashSpec::new(None, false),
            BlockhashSpec::Undeclared,
        );
    }

    #[test]
    #[should_panic]
    fn test_blockhashspec_new_fail() {
        BlockhashSpec::new(None, true);
    }

    #[test]
    fn test_blockhashspec_new_from_matches_ok() {
        let test_commands = App::new("blockhashspec_test").offline_args();
        let blockhash = hash(&[1u8]);
        let blockhash_string = blockhash.to_string();

        let matches = test_commands.clone().get_matches_from(vec![
            "blockhashspec_test",
            "--blockhash",
            &blockhash_string,
            "--sign-only",
        ]);
        assert_eq!(
            BlockhashSpec::new_from_matches(&matches),
            BlockhashSpec::Full(blockhash, FeeCalculator::default()),
        );

        let matches = test_commands.clone().get_matches_from(vec![
            "blockhashspec_test",
            "--blockhash",
            &blockhash_string,
        ]);
        assert_eq!(
            BlockhashSpec::new_from_matches(&matches),
            BlockhashSpec::Partial(blockhash),
        );

        let matches = test_commands
            .clone()
            .get_matches_from(vec!["blockhashspec_test"]);
        assert_eq!(
            BlockhashSpec::new_from_matches(&matches),
            BlockhashSpec::Undeclared,
        );
    }

    #[test]
    #[should_panic]
    fn test_blockhashspec_new_from_matches_fail() {
        let test_commands = App::new("blockhashspec_test")
            .arg(blockhash_arg())
            // We can really only hit this case unless the arg requirements
            // are broken, so unset the requires() to recreate that condition
            .arg(sign_only_arg().requires(""));

        let matches = test_commands
            .clone()
            .get_matches_from(vec!["blockhashspec_test", "--sign-only"]);
        BlockhashSpec::new_from_matches(&matches);
    }

    #[test]
    fn test_blockhashspec_get_blockhash_fee_calc() {
        let test_blockhash = hash(&[0u8]);
        let rpc_blockhash = hash(&[1u8]);
        let rpc_fee_calc = FeeCalculator::new(42, 42);
        let get_recent_blockhash_response = json!(Response {
            context: RpcResponseContext { slot: 1 },
            value: json!((
                Value::String(rpc_blockhash.to_string()),
                serde_json::to_value(rpc_fee_calc.clone()).unwrap()
            )),
        });
        let mut mocks = HashMap::new();
        mocks.insert(
            RpcRequest::GetRecentBlockhash,
            get_recent_blockhash_response.clone(),
        );
        let rpc_client = RpcClient::new_mock_with_mocks("".to_string(), mocks);
        assert_eq!(
            BlockhashSpec::Undeclared
                .get_blockhash_fee_calculator(&rpc_client)
                .unwrap(),
            (rpc_blockhash, rpc_fee_calc.clone()),
        );
        let mut mocks = HashMap::new();
        mocks.insert(
            RpcRequest::GetRecentBlockhash,
            get_recent_blockhash_response.clone(),
        );
        let rpc_client = RpcClient::new_mock_with_mocks("".to_string(), mocks);
        assert_eq!(
            BlockhashSpec::Partial(test_blockhash)
                .get_blockhash_fee_calculator(&rpc_client)
                .unwrap(),
            (test_blockhash, rpc_fee_calc.clone()),
        );
        let mut mocks = HashMap::new();
        mocks.insert(
            RpcRequest::GetRecentBlockhash,
            get_recent_blockhash_response.clone(),
        );
        let rpc_client = RpcClient::new_mock_with_mocks("".to_string(), mocks);
        assert_eq!(
            BlockhashSpec::Full(test_blockhash, FeeCalculator::default())
                .get_blockhash_fee_calculator(&rpc_client)
                .unwrap(),
            (test_blockhash, FeeCalculator::default()),
        );
        let rpc_client = RpcClient::new_mock("fails".to_string());
        assert!(BlockhashSpec::Undeclared
            .get_blockhash_fee_calculator(&rpc_client)
            .is_err());
    }
}
