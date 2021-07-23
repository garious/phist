//! The `sendmmsg` module provides sendmmsg() API implementation

use std::io;
use std::net::{SocketAddr, UdpSocket};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SendPktsError {
    /// IO Error during send: first error, num failed packets
    #[error("IO Error, some packets could not be sent")]
    IoError(io::Error, usize),
}

#[cfg(not(target_os = "linux"))]
pub fn batch_send(sock: &UdpSocket, packets: &[(&[u8], &SocketAddr)]) -> Result<(), SendPktsError> {
    let mut num_failed = 0;
    let mut erropt = None;
    for (p, a) in packets {
        if let Err(e) = sock.send_to(p, *a) {
            num_failed += 1;
            if erropt.is_none() {
                erropt = Some(e);
            }
        }
    }

    if let Some(err) = erropt {
        Err(SendPktsError::IoError(err, num_failed))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
use libc::{iovec, mmsghdr, sockaddr_in, sockaddr_in6, sockaddr_storage};

#[cfg(target_os = "linux")]
fn mmsghdr_for_packet(
    packet: &[u8],
    dest: &SocketAddr,
    iovs: &mut Vec<iovec>,
    addrs: &mut Vec<sockaddr_storage>,
    hdrs: &mut Vec<mmsghdr>,
) {
    use libc::c_void;
    use nix::sys::socket::InetAddr;
    use std::mem;

    const SIZE_OF_SOCKADDR_IN: usize = mem::size_of::<sockaddr_in>();
    const SIZE_OF_SOCKADDR_IN6: usize = mem::size_of::<sockaddr_in6>();

    let index = hdrs.len();

    debug_assert!(index < hdrs.capacity());
    debug_assert!(index < addrs.capacity());
    debug_assert!(index < iovs.capacity());
    debug_assert_eq!(hdrs.len(), addrs.len());
    debug_assert_eq!(hdrs.len(), iovs.len());

    iovs.push(iovec {
        iov_base: packet.as_ptr() as *mut c_void,
        iov_len: packet.len(),
    });

    let hdr: mmsghdr = unsafe { mem::zeroed() };
    hdrs.push(hdr);

    let addr_storage: sockaddr_storage = unsafe { mem::zeroed() };
    addrs.push(addr_storage);

    debug_assert!(index < hdrs.len());

    hdrs[index].msg_hdr.msg_iov = &mut iovs[index];
    hdrs[index].msg_hdr.msg_iovlen = 1;

    match InetAddr::from_std(dest) {
        InetAddr::V4(addr) => {
            unsafe {
                core::ptr::write(&mut addrs[index] as *mut _ as *mut _, addr);
            }
            hdrs[index].msg_hdr.msg_name = &mut addrs[index] as *mut _ as *mut _;
            hdrs[index].msg_hdr.msg_namelen = SIZE_OF_SOCKADDR_IN as u32;
        }
        InetAddr::V6(addr) => {
            unsafe {
                core::ptr::write(&mut addrs[index] as *mut _ as *mut _, addr);
            }
            hdrs[index].msg_hdr.msg_name = &mut addrs[index] as *mut _ as *mut _;
            hdrs[index].msg_hdr.msg_namelen = SIZE_OF_SOCKADDR_IN6 as u32;
        }
    };
}

#[cfg(target_os = "linux")]
fn sendmmsg_retry(sock: &UdpSocket, hdrs: &mut Vec<mmsghdr>) -> Result<(), SendPktsError> {
    use libc::sendmmsg;
    use std::os::unix::io::AsRawFd;

    let sock_fd = sock.as_raw_fd();
    let mut total_sent = 0;
    let mut erropt = None;

    let mut pkts = &mut hdrs[..];
    while !pkts.is_empty() {
        let npkts = match unsafe { sendmmsg(sock_fd, &mut pkts[0], pkts.len() as u32, 0) } {
            -1 => {
                if erropt.is_none() {
                    erropt = Some(io::Error::last_os_error());
                }
                // skip over the failing packet
                1_usize
            }
            n => {
                // if we fail to send all packets we advance to the failing
                // packet and retry in order to capture the error code
                total_sent += n as usize;
                n as usize
            }
        };
        pkts = &mut pkts[npkts..];
    }

    if let Some(err) = erropt {
        Err(SendPktsError::IoError(err, hdrs.len() - total_sent))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
pub fn batch_send(sock: &UdpSocket, packets: &[(&[u8], &SocketAddr)]) -> Result<(), SendPktsError> {
    // The vectors are allocated with capacity, as later code inserts elements
    // at specific indices, and uses the address of the vector index in hdrs
    let mut iovs: Vec<iovec> = Vec::with_capacity(packets.len());
    let mut addrs: Vec<sockaddr_storage> = Vec::with_capacity(packets.len());
    let mut hdrs: Vec<mmsghdr> = Vec::with_capacity(packets.len());

    for (pkt, dest) in packets.iter() {
        mmsghdr_for_packet(pkt, dest, &mut iovs, &mut addrs, &mut hdrs);
    }

    sendmmsg_retry(sock, &mut hdrs)
}

pub fn multi_target_send(
    sock: &UdpSocket,
    packet: &[u8],
    dests: &[&SocketAddr],
) -> Result<(), SendPktsError> {
    let pkts: Vec<_> = dests.iter().map(|addr| (packet, *addr)).collect();
    batch_send(sock, &pkts[..])
}

#[cfg(test)]
mod tests {
    use crate::packet::Packet;
    use crate::recvmmsg::recv_mmsg;
    use crate::sendmmsg::{batch_send, multi_target_send, SendPktsError};
    use solana_sdk::packet::PACKET_DATA_SIZE;
    use std::{
        io::ErrorKind,
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket},
    };

    #[test]
    pub fn test_send_mmsg_one_dest() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");

        let packets: Vec<_> = (0..32).map(|_| vec![0u8; PACKET_DATA_SIZE]).collect();
        let packet_refs: Vec<_> = packets.iter().map(|p| (&p[..], &addr)).collect();

        let sent = batch_send(&sender, &packet_refs[..]).ok();
        assert_eq!(sent, Some(()));

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader, &mut packets[..], 1_000).unwrap().1;
        assert_eq!(32, recv);
    }

    #[test]
    pub fn test_send_mmsg_multi_dest() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();

        let reader2 = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr2 = reader2.local_addr().unwrap();

        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");

        let packets: Vec<_> = (0..32).map(|_| vec![0u8; PACKET_DATA_SIZE]).collect();
        let packet_refs: Vec<_> = packets
            .iter()
            .enumerate()
            .map(|(i, p)| {
                if i < 16 {
                    (&p[..], &addr)
                } else {
                    (&p[..], &addr2)
                }
            })
            .collect();

        let sent = batch_send(&sender, &packet_refs[..]).ok();
        assert_eq!(sent, Some(()));

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader, &mut packets[..], 1_000).unwrap().1;
        assert_eq!(16, recv);

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader2, &mut packets[..], 1_000).unwrap().1;
        assert_eq!(16, recv);
    }

    #[test]
    pub fn test_multicast_msg() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();

        let reader2 = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr2 = reader2.local_addr().unwrap();

        let reader3 = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr3 = reader3.local_addr().unwrap();

        let reader4 = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr4 = reader4.local_addr().unwrap();

        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");

        let packet = Packet::default();

        let sent = multi_target_send(
            &sender,
            &packet.data[..packet.meta.size],
            &[&addr, &addr2, &addr3, &addr4],
        )
        .ok();
        assert_eq!(sent, Some(()));

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader, &mut packets[..], 1_000).unwrap().1;
        assert_eq!(1, recv);

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader2, &mut packets[..], 1_000).unwrap().1;
        assert_eq!(1, recv);

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader3, &mut packets[..], 1_000).unwrap().1;
        assert_eq!(1, recv);

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader4, &mut packets[..], 1_000).unwrap().1;
        assert_eq!(1, recv);
    }

    #[test]
    fn test_intermediate_failures_mismatched_bind() {
        let packets: Vec<_> = (0..3).map(|_| vec![0u8; PACKET_DATA_SIZE]).collect();
        let ip4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        let ip6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 8080);
        let packet_refs: Vec<_> = vec![
            (&packets[0][..], &ip4),
            (&packets[1][..], &ip6),
            (&packets[2][..], &ip4),
        ];
        let dest_refs: Vec<_> = vec![&ip4, &ip6, &ip4];

        let sender = UdpSocket::bind("0.0.0.0:0").expect("bind");
        if let Err(SendPktsError::IoError(_, num_failed)) = batch_send(&sender, &packet_refs[..]) {
            assert_eq!(num_failed, 1);
        }
        if let Err(SendPktsError::IoError(_, num_failed)) =
            multi_target_send(&sender, &packets[0], &dest_refs)
        {
            assert_eq!(num_failed, 1);
        }
    }

    #[test]
    fn test_intermediate_failures_unreachable_address() {
        let packets: Vec<_> = (0..5).map(|_| vec![0u8; PACKET_DATA_SIZE]).collect();
        let ipv4local = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        let ipv4broadcast = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), 8080);
        let sender = UdpSocket::bind("0.0.0.0:0").expect("bind");

        // test intermediate failures for batch_send
        let packet_refs: Vec<_> = vec![
            (&packets[0][..], &ipv4local),
            (&packets[1][..], &ipv4broadcast),
            (&packets[2][..], &ipv4local),
            (&packets[3][..], &ipv4broadcast),
            (&packets[4][..], &ipv4local),
        ];
        if let Err(SendPktsError::IoError(ioerror, num_failed)) =
            batch_send(&sender, &packet_refs[..])
        {
            assert!(matches!(ioerror.kind(), ErrorKind::PermissionDenied));
            assert_eq!(num_failed, 2);
        }

        // test leading and trailing failures for batch_send
        let packet_refs: Vec<_> = vec![
            (&packets[0][..], &ipv4broadcast),
            (&packets[1][..], &ipv4local),
            (&packets[2][..], &ipv4broadcast),
            (&packets[3][..], &ipv4local),
            (&packets[4][..], &ipv4broadcast),
        ];
        if let Err(SendPktsError::IoError(ioerror, num_failed)) =
            batch_send(&sender, &packet_refs[..])
        {
            assert!(matches!(ioerror.kind(), ErrorKind::PermissionDenied));
            assert_eq!(num_failed, 3);
        }

        // test consecutive intermediate failures for batch_send
        let packet_refs: Vec<_> = vec![
            (&packets[0][..], &ipv4local),
            (&packets[1][..], &ipv4local),
            (&packets[2][..], &ipv4broadcast),
            (&packets[3][..], &ipv4broadcast),
            (&packets[4][..], &ipv4local),
        ];
        if let Err(SendPktsError::IoError(ioerror, num_failed)) =
            batch_send(&sender, &packet_refs[..])
        {
            assert!(matches!(ioerror.kind(), ErrorKind::PermissionDenied));
            assert_eq!(num_failed, 2);
        }

        // test intermediate failures for multi_target_send
        let dest_refs: Vec<_> = vec![
            &ipv4local,
            &ipv4broadcast,
            &ipv4local,
            &ipv4broadcast,
            &ipv4local,
        ];
        if let Err(SendPktsError::IoError(ioerror, num_failed)) =
            multi_target_send(&sender, &packets[0], &dest_refs)
        {
            assert!(matches!(ioerror.kind(), ErrorKind::PermissionDenied));
            assert_eq!(num_failed, 2);
        }

        // test leading and trailing failures for multi_target_send
        let dest_refs: Vec<_> = vec![
            &ipv4broadcast,
            &ipv4local,
            &ipv4broadcast,
            &ipv4local,
            &ipv4broadcast,
        ];
        if let Err(SendPktsError::IoError(ioerror, num_failed)) =
            multi_target_send(&sender, &packets[0], &dest_refs)
        {
            assert!(matches!(ioerror.kind(), ErrorKind::PermissionDenied));
            assert_eq!(num_failed, 3);
        }
    }
}
