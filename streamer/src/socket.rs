use std::net::SocketAddr;

// TODO: remove these once IpAddr::is_global is stable.

#[cfg(any(test, feature = "allow_private_addr"))]
pub fn is_global(_: &SocketAddr) -> bool {
    true
}

#[cfg(not(any(test, feature = "allow_private_addr")))]
pub fn is_global(addr: &SocketAddr) -> bool {
    use std::net::IpAddr;

    match addr.ip() {
        IpAddr::V4(addr) => {
            // TODO: Consider excluding:
            //    addr.is_loopback() || addr.is_link_local()
            // || addr.is_broadcast() || addr.is_documentation()
            // || addr.is_unspecified()
            !addr.is_private()
        }
        IpAddr::V6(_) => {
            // TODO: Consider excluding:
            // addr.is_loopback() || addr.is_unspecified(),
            true
        }
    }
}
