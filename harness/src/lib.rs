//! Phase-0 conformance harness for the SQD Portal.
//!
//! Implements the build order of `spec/13-conformance.md`: dependency stubs per
//! IB-7 with ledgers, a toy-world generator, the reference model (the oracle),
//! the six structural validators, an HTTP client driver, and the
//! quiescence-gated gauge audit. The portal runs as a black-box child process.

pub mod artifact;
pub mod driver;
pub mod dummy_chain;
pub mod fixture;
pub mod keys;
pub mod metrics_audit;
pub mod model;
pub mod portal;
pub mod stubs;
pub mod validators;
pub mod world;

pub use world::ToyWorld;

/// Ports handed out in this process, and never handed out twice.
///
/// Binding `:0` and reading the port back leaves the port free until the caller's
/// stub actually binds it, and the OS will happily offer the same one to whoever
/// asks in between. The loser then dies with `EADDRINUSE` long after allocation,
/// in a different test, looking like a product fault — which is how this was
/// found, as a CT-10 failure on CI that no laptop reproduced. Tests within a
/// binary run as threads of one process, so remembering what was issued closes
/// the window where it actually bites.
static ISSUED_PORTS: std::sync::Mutex<std::collections::BTreeSet<u16>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

/// Rejected candidates are kept bound until one is accepted, so a single pass
/// cannot be offered the same port twice either.
fn reserve(mut probe: impl FnMut() -> (u16, Box<dyn std::any::Any>)) -> u16 {
    let mut held = Vec::new();
    loop {
        let (port, socket) = probe();
        if ISSUED_PORTS.lock().unwrap().insert(port) {
            return port;
        }
        held.push(socket);
        assert!(
            held.len() < 64,
            "no unissued loopback port after 64 tries: the ephemeral range is exhausted"
        );
    }
}

/// Free TCP port on loopback, never one already given out in this process.
pub fn free_tcp_port() -> u16 {
    reserve(|| {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        (l.local_addr().unwrap().port(), Box::new(l))
    })
}

/// Free UDP port on loopback (for the stub worker's QUIC listener).
pub fn free_udp_port() -> u16 {
    reserve(|| {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        (s.local_addr().unwrap().port(), Box::new(s))
    })
}

#[cfg(test)]
mod port_tests {
    /// The guarantee, tested against the OS's worst behaviour rather than its
    /// usual one. A probe that keeps offering a port already handed out stands
    /// in for the window the real fault lives in — a listener dropped, its
    /// owner not yet bound, the port free as far as the kernel is concerned.
    /// Provoking that for real needs an allocator that reuses ports promptly,
    /// which Linux does and macOS does not, so the condition is injected
    /// instead of waited for.
    #[test]
    fn a_port_already_issued_is_refused_however_often_it_is_offered() {
        let issued = super::free_tcp_port();
        let mut offers = 0;
        let port = super::reserve(|| {
            offers += 1;
            if offers <= 3 {
                (issued, Box::new(()))
            } else {
                let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                (l.local_addr().unwrap().port(), Box::new(l))
            }
        });

        assert_ne!(port, issued, "a port was handed out twice");
        assert_eq!(offers, 4, "every repeat offer must be refused, not taken");
    }
}
