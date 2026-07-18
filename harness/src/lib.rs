//! Phase-0 conformance harness for the SQD Portal.
//!
//! Implements the build order of `spec/13-conformance.md`: dependency stubs per
//! IB-7 with ledgers, a toy-world generator, the reference model (the oracle),
//! the six structural validators, an HTTP client driver, and the
//! quiescence-gated gauge audit. The portal runs as a black-box child process.

pub mod artifact;
pub mod driver;
pub mod dummy_chain;
pub mod keys;
pub mod metrics_audit;
pub mod model;
pub mod portal;
pub mod stubs;
pub mod validators;
pub mod world;

pub use world::ToyWorld;

/// Free TCP port on loopback. Bind-then-drop has a benign race; ports are used
/// immediately after.
pub fn free_tcp_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Free UDP port on loopback (for the stub worker's QUIC listener).
pub fn free_udp_port() -> u16 {
    let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    s.local_addr().unwrap().port()
}
