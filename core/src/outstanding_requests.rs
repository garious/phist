use crate::request_response::RequestResponse;
use lru::LruCache;
use rand::{thread_rng, Rng};
use solana_ledger::shred::Nonce;

pub const DEFAULT_REQUEST_EXPIRATION_MS: u64 = 60_000;

pub struct OutstandingRequests<T> {
    requests: LruCache<Nonce, RequestStatus<T>>,
}

impl<T, S> OutstandingRequests<T>
where
    T: RequestResponse<Response = S>,
{
    // Returns boolean indicating whether sufficient time has passed for a request with
    // the given timestamp to be made
    pub fn add_request(&mut self, request: T, now: u64) -> Nonce {
        let num_expected_responses = request.num_expected_responses();
        let nonce = thread_rng().gen_range(0, Nonce::MAX);
        self.requests.put(
            nonce,
            RequestStatus {
                expire_timestamp: now + DEFAULT_REQUEST_EXPIRATION_MS,
                num_expected_responses,
                request,
            },
        );
        nonce
    }

    pub fn register_response<R>(
        &mut self,
        nonce: u32,
        response: &S,
        now: u64,
        // runs if the response was valid
        success_fn: impl Fn(&T) -> R,
    ) -> Option<R> {
        let (response, should_delete) = self
            .requests
            .get_mut(&nonce)
            .map(|status| {
                if status.num_expected_responses > 0
                    && now < status.expire_timestamp
                    && status.request.verify_response(response)
                {
                    status.num_expected_responses -= 1;
                    (
                        Some(success_fn(&status.request)),
                        status.num_expected_responses == 0,
                    )
                } else {
                    (None, true)
                }
            })
            .unwrap_or((None, false));

        if should_delete {
            self.requests
                .pop(&nonce)
                .expect("Delete must delete existing object");
        }

        response
    }
}

impl<T> Default for OutstandingRequests<T> {
    fn default() -> Self {
        Self {
            requests: LruCache::new(16 * 1024),
        }
    }
}

pub struct RequestStatus<T> {
    expire_timestamp: u64,
    num_expected_responses: u32,
    request: T,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::serve_repair::ShredRepairType;
    use solana_ledger::shred::Shred;
    use solana_sdk::timing::timestamp;

    #[test]
    fn test_add_request() {
        let repair_type = ShredRepairType::Orphan(9);
        let mut outstanding_requests = OutstandingRequests::default();
        let nonce = outstanding_requests.add_request(repair_type, timestamp());
        let request_status = outstanding_requests.requests.get(&nonce).unwrap();
        assert_eq!(request_status.request, repair_type);
        assert_eq!(
            request_status.num_expected_responses,
            repair_type.num_expected_responses()
        );
    }

    #[test]
    fn test_timeout_expired_remove() {
        let repair_type = ShredRepairType::Orphan(9);
        let mut outstanding_requests = OutstandingRequests::default();
        let nonce = outstanding_requests.add_request(repair_type, timestamp());
        let shred = Shred::new_empty_data_shred();

        let expire_timestamp = outstanding_requests
            .requests
            .get(&nonce)
            .unwrap()
            .expire_timestamp;

        assert!(outstanding_requests
            .register_response(nonce, &shred, expire_timestamp + 1, |_| ())
            .is_none());
        assert!(outstanding_requests.requests.get(&nonce).is_none());
    }

    #[test]
    fn test_register_response() {
        let repair_type = ShredRepairType::Orphan(9);
        let mut outstanding_requests = OutstandingRequests::default();
        let nonce = outstanding_requests.add_request(repair_type, timestamp());

        let shred = Shred::new_empty_data_shred();
        let mut expire_timestamp = outstanding_requests
            .requests
            .get(&nonce)
            .unwrap()
            .expire_timestamp;
        let mut num_expected_responses = outstanding_requests
            .requests
            .get(&nonce)
            .unwrap()
            .num_expected_responses;
        assert!(num_expected_responses > 1);

        // Response that passes all checks should decrease num_expected_responses
        assert!(outstanding_requests
            .register_response(nonce, &shred, expire_timestamp - 1, |_| ())
            .is_some());
        num_expected_responses -= 1;
        assert_eq!(
            outstanding_requests
                .requests
                .get(&nonce)
                .unwrap()
                .num_expected_responses,
            num_expected_responses
        );

        // Response with incorrect nonce is ignored
        assert!(outstanding_requests
            .register_response(nonce + 1, &shred, expire_timestamp - 1, |_| ())
            .is_none());
        assert!(outstanding_requests
            .register_response(nonce + 1, &shred, expire_timestamp, |_| ())
            .is_none());
        assert_eq!(
            outstanding_requests
                .requests
                .get(&nonce)
                .unwrap()
                .num_expected_responses,
            num_expected_responses
        );

        // Response with timestamp over limit should remove status, preventing late
        // responses from being accepted
        assert!(outstanding_requests
            .register_response(nonce, &shred, expire_timestamp, |_| ())
            .is_none());
        assert!(outstanding_requests.requests.get(&nonce).is_none());

        // If number of outstanding requests hits zero, should also remove the entry
        let nonce = outstanding_requests.add_request(repair_type, timestamp());
        expire_timestamp = outstanding_requests
            .requests
            .get(&nonce)
            .unwrap()
            .expire_timestamp;
        num_expected_responses = outstanding_requests
            .requests
            .get(&nonce)
            .unwrap()
            .num_expected_responses;
        assert!(num_expected_responses > 1);
        for _ in 0..num_expected_responses {
            assert!(outstanding_requests.requests.get(&nonce).is_some());
            assert!(outstanding_requests
                .register_response(nonce, &shred, expire_timestamp - 1, |_| ())
                .is_some());
        }
        assert!(outstanding_requests.requests.get(&nonce).is_none());
    }
}
