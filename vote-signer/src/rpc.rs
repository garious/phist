//! The `rpc` module implements the Vote signing service RPC interface.

use bs58;
use jsonrpc_core::*;
use jsonrpc_http_server::*;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, KeypairUtil, Signature};
use std::collections::HashMap;
use std::mem;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, sleep, Builder, JoinHandle};
use std::time::Duration;

pub struct VoteSignerRpcService {
    thread_hdl: JoinHandle<()>,
    exit: Arc<AtomicBool>,
}

impl VoteSignerRpcService {
    pub fn new(rpc_addr: SocketAddr) -> Self {
        let request_processor = VoteSignRequestProcessor::default();
        let exit = Arc::new(AtomicBool::new(false));
        let exit_ = exit.clone();
        let thread_hdl = Builder::new()
            .name("solana-vote-signer-jsonrpc".to_string())
            .spawn(move || {
                let mut io = MetaIoHandler::default();
                let rpc = VoteSignerRpcImpl;
                io.extend_with(rpc.to_delegate());

                let server =
                    ServerBuilder::with_meta_extractor(io, move |_req: &hyper::Request<hyper::Body>| Meta {
                        request_processor: request_processor.clone(),
                    }).threads(4)
                        .cors(DomainsValidation::AllowOnly(vec![
                            AccessControlAllowOrigin::Any,
                        ]))
                        .start_http(&rpc_addr);
                if server.is_err() {
                    warn!("JSON RPC service unavailable: unable to bind to RPC port {}. \nMake sure this port is not already in use by another application", rpc_addr.port());
                    return;
                }
                while !exit_.load(Ordering::Relaxed) {
                    sleep(Duration::from_millis(100));
                }
                server.unwrap().close();
            })
            .unwrap();
        Self { thread_hdl, exit }
    }

    pub fn exit(&self) {
        self.exit.store(true, Ordering::Relaxed);
    }

    pub fn close(self) -> thread::Result<()> {
        self.exit();
        self.join()
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[derive(Clone)]
pub struct Meta {
    pub request_processor: VoteSignRequestProcessor,
}
impl Metadata for Meta {}

build_rpc_trait! {
    pub trait VoteSignerRpc {
        type Metadata;

        #[rpc(meta, name = "registerNode")]
        fn register(&self, Self::Metadata, String, Signature, Vec<u8>) -> Result<String>;

        #[rpc(meta, name = "signVote")]
        fn sign(&self, Self::Metadata, String, Signature, Vec<u8>) -> Result<String>;

        #[rpc(meta, name = "deregisterNode")]
        fn deregister(&self, Self::Metadata, String, Signature, Vec<u8>) -> Result<()>;
    }
}

pub struct VoteSignerRpcImpl;
impl VoteSignerRpc for VoteSignerRpcImpl {
    type Metadata = Meta;

    fn register(
        &self,
        meta: Self::Metadata,
        id: String,
        sig: Signature,
        signed_msg: Vec<u8>,
    ) -> Result<String> {
        info!("register rpc request received: {:?}", id);
        let pubkey = verify_pubkey(id)?;
        verify_signature(&sig, &pubkey, &signed_msg)?;
        meta.request_processor.register(pubkey)
    }

    fn sign(
        &self,
        meta: Self::Metadata,
        id: String,
        sig: Signature,
        signed_msg: Vec<u8>,
    ) -> Result<String> {
        info!("sign rpc request received: {:?}", id);
        let pubkey = verify_pubkey(id)?;
        verify_signature(&sig, &pubkey, &signed_msg)?;
        meta.request_processor.sign(pubkey, &signed_msg)
    }

    fn deregister(
        &self,
        meta: Self::Metadata,
        id: String,
        sig: Signature,
        signed_msg: Vec<u8>,
    ) -> Result<()> {
        info!("deregister rpc request received: {:?}", id);
        let pubkey = verify_pubkey(id)?;
        verify_signature(&sig, &pubkey, &signed_msg)?;
        meta.request_processor.deregister(pubkey)
    }
}

fn verify_signature(sig: &Signature, pubkey: &Pubkey, msg: &[u8]) -> Result<()> {
    if sig.verify(pubkey.as_ref(), msg) {
        Ok(())
    } else {
        Err(Error::invalid_request())
    }
}

#[derive(Clone)]
pub struct VoteSignRequestProcessor {
    nodes: Arc<RwLock<HashMap<Pubkey, Keypair>>>,
}
impl VoteSignRequestProcessor {
    /// Process JSON-RPC request items sent via JSON-RPC.
    pub fn register(&self, pubkey: Pubkey) -> Result<String> {
        {
            if let Some(voting_keypair) = self.nodes.read().unwrap().get(&pubkey) {
                return Ok(bs58::encode(voting_keypair.pubkey()).into_string());
            }
        }
        let voting_keypair = Keypair::new();
        let voting_pubkey = voting_keypair.pubkey();
        self.nodes.write().unwrap().insert(pubkey, voting_keypair);
        Ok(bs58::encode(voting_pubkey).into_string())
    }
    pub fn sign(&self, pubkey: Pubkey, msg: &[u8]) -> Result<String> {
        match self.nodes.read().unwrap().get(&pubkey) {
            Some(voting_keypair) => {
                let sig = Signature::new(&voting_keypair.sign(&msg).as_ref());
                Ok(bs58::encode(sig).into_string())
            }
            None => Err(Error::invalid_request()),
        }
    }
    pub fn deregister(&self, pubkey: Pubkey) -> Result<()> {
        self.nodes.write().unwrap().remove(&pubkey);
        Ok(())
    }
}

impl Default for VoteSignRequestProcessor {
    fn default() -> Self {
        VoteSignRequestProcessor {
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

pub fn verify_pubkey(input: String) -> Result<Pubkey> {
    let pubkey_vec = bs58::decode(input).into_vec().map_err(|err| {
        info!("verify_pubkey: invalid input: {:?}", err);
        Error::invalid_request()
    })?;
    if pubkey_vec.len() != mem::size_of::<Pubkey>() {
        info!(
            "verify_pubkey: invalid pubkey_vec length: {}",
            pubkey_vec.len()
        );
        Err(Error::invalid_request())
    } else {
        Ok(Pubkey::new(&pubkey_vec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpc_core::Response;
    use solana_sdk::signature::{Keypair, KeypairUtil};

    fn start_rpc_handler() -> (MetaIoHandler<Meta>, Meta) {
        let request_processor = VoteSignRequestProcessor::default();
        let mut io = MetaIoHandler::default();
        let rpc = VoteSignerRpcImpl;
        io.extend_with(rpc.to_delegate());
        let meta = Meta { request_processor };
        (io, meta)
    }

    #[test]
    fn test_rpc_register_node() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let sig = Signature::new(&node_keypair.sign(msg.as_bytes()).as_ref());
        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "registerNode",
           "params": [node_pubkey.to_string(), sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Success(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
                assert!(succ.result.is_string());
                let _pubkey = verify_pubkey(succ.result.as_str().unwrap().to_string()).unwrap();
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_register_node_invalid_sig() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let msg1 = "This is a Test1";
        let sig = Signature::new(&node_keypair.sign(msg.as_bytes()).as_ref());
        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "registerNode",
           "params": [node_pubkey.to_string(), sig, msg1.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Failure(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_deregister_node() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let sig = Signature::new(&node_keypair.sign(msg.as_bytes()).as_ref());
        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "deregisterNode",
           "params": [node_pubkey.to_string(), sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Success(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_deregister_node_invalid_sig() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let msg1 = "This is a Test1";
        let sig = Signature::new(&node_keypair.sign(msg.as_bytes()).as_ref());
        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "deregisterNode",
           "params": [node_pubkey.to_string(), sig, msg1.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Failure(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_sign_vote() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let sig = Signature::new(&node_keypair.sign(msg.as_bytes()).as_ref());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "registerNode",
           "params": [node_pubkey.to_string(), sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta.clone());
        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");
        let mut vote_pubkey = Keypair::new().pubkey();
        if let Response::Single(out) = result {
            if let Output::Success(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
                assert!(succ.result.is_string());
                vote_pubkey = verify_pubkey(succ.result.as_str().unwrap().to_string()).unwrap();
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "signVote",
           "params": [node_pubkey.to_string(), sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Success(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
                assert!(succ.result.is_string());
                let sig_vec = bs58::decode(succ.result.as_str().unwrap().to_string())
                    .into_vec()
                    .map_err(|err| {
                        info!("invalid input: {:?}", err);
                        Error::invalid_request()
                    })
                    .unwrap();
                assert_eq!(sig_vec.len(), mem::size_of::<Signature>());
                let sig = Signature::new(&sig_vec);
                assert_eq!(verify_signature(&sig, &vote_pubkey, msg.as_bytes()), Ok(()));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_sign_vote_before_register() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let sig = Signature::new(&node_keypair.sign(msg.as_bytes()).as_ref());
        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "signVote",
           "params": [node_pubkey.to_string(), sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Failure(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_sign_vote_after_deregister() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let sig = Signature::new(&node_keypair.sign(msg.as_bytes()).as_ref());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "registerNode",
           "params": [node_pubkey.to_string(), sig, msg.as_bytes()],
        });
        let _res = io.handle_request_sync(&req.to_string(), meta.clone());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "deregisterNode",
           "params": [node_pubkey.to_string(), sig, msg.as_bytes()],
        });
        let _res = io.handle_request_sync(&req.to_string(), meta.clone());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "signVote",
           "params": [node_pubkey.to_string(), sig, msg.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Failure(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_rpc_sign_vote_invalid_sig() {
        let (io, meta) = start_rpc_handler();

        let node_keypair = Keypair::new();
        let node_pubkey = node_keypair.pubkey();
        let msg = "This is a test";
        let msg1 = "This is a Test";
        let sig = Signature::new(&node_keypair.sign(msg.as_bytes()).as_ref());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "registerNode",
           "params": [node_pubkey.to_string(), sig, msg.as_bytes()],
        });
        let _res = io.handle_request_sync(&req.to_string(), meta.clone());

        let req = json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": "signVote",
           "params": [node_pubkey.to_string(), sig, msg1.as_bytes()],
        });
        let res = io.handle_request_sync(&req.to_string(), meta);

        let result: Response = serde_json::from_str(&res.expect("actual response"))
            .expect("actual response deserialization");

        if let Response::Single(out) = result {
            if let Output::Failure(succ) = out {
                assert_eq!(succ.jsonrpc.unwrap(), Version::V2);
                assert_eq!(succ.id, Id::Num(1));
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }
}
