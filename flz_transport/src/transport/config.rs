use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::OnceLock;
use std::{env, fmt, io};

#[derive(Debug, Clone, PartialEq)]
pub enum TransportType {
    Uds,
    Quic,
}

/// Default maximum accepted wire-frame payload length (64 MiB) — far above any
/// legitimate frame, small enough that a hostile length prefix cannot force an
/// OOM. Override with `FLEZHA_MAX_FRAME_BYTES`.
const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Default per-stream send-buffer cap (8 MiB) — a few seconds of peak
/// per-stream throughput, so a slow peer cannot grow the buffered-write `Vec`
/// without bound. Override with `FLEZHA_MAX_WRITE_BUFFER_BYTES`.
const DEFAULT_MAX_WRITE_BUFFER_BYTES: usize = 8 * 1024 * 1024;

/// The bound every frame decoder enforces on a declared length prefix BEFORE
/// allocating for it. Cached after the first read: the check sits on the
/// per-frame hot path, so the knob is boot-time configuration.
pub fn max_frame_bytes() -> usize {
    static CACHE: OnceLock<usize> = OnceLock::new();
    *CACHE.get_or_init(|| {
        parse_positive_bytes(
            env::var("FLEZHA_MAX_FRAME_BYTES").ok().as_deref(),
            DEFAULT_MAX_FRAME_BYTES,
        )
    })
}

/// Per-stream send-buffer cap shared by the UDS and QUIC drivers.
pub fn max_write_buffer_bytes() -> usize {
    parse_positive_bytes(
        env::var("FLEZHA_MAX_WRITE_BUFFER_BYTES").ok().as_deref(),
        DEFAULT_MAX_WRITE_BUFFER_BYTES,
    )
}

fn parse_positive_bytes(raw: Option<&str>, default: usize) -> usize {
    raw.and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

pub fn get_transport_type() -> TransportType {
    match env::var("FLEZHA_TRANSPORT").as_deref() {
        Ok("quic") => TransportType::Quic,
        _ => TransportType::Uds,
    }
}

/// Comma-separated controller endpoints a QUIC runner dials, each a literal
/// address or a name, each with an optional port.
pub const QUIC_ADDRESSES_VAR: &str = "FLEZHA_QUIC_ADDRESSES";

/// Replaced by `QUIC_ADDRESSES_VAR`, and not aliased to it: a single hardcoded
/// address is the failure this list exists to remove, so honouring the old
/// name silently would preserve it.
pub const RETIRED_QUIC_ADDRESS_VAR: &str = "FLEZHA_QUIC_ADDRESS";

const DEFAULT_QUIC_PORT: u16 = 4433;
pub const DEFAULT_QUIC_ADDRESSES: &str = "127.0.0.1:4433";

/// One entry of the candidate list, still unresolved: a name is resolved at
/// the point of use, never here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicEntry {
    pub host: String,
    pub port: u16,
}

impl QuicEntry {
    /// Parse one list entry: a literal address or a name, with an optional
    /// port. Public because a non-Rust SDK reaches it through `flz_nif` with a
    /// single entry in hand.
    pub fn parse(entry: &str) -> Result<QuicEntry, String> {
        let invalid = |what: &str| format!("{} entry {:?} is {}", QUIC_ADDRESSES_VAR, entry, what);

        // `[::1]:4433` and `[::1]` — the only spelling that separates an IPv6
        // literal from its port.
        if let Some(rest) = entry.strip_prefix('[') {
            let (host, tail) = rest
                .split_once(']')
                .ok_or_else(|| invalid("a bracketed address with no closing bracket"))?;
            let port = match tail.strip_prefix(':') {
                Some(port) => port.parse().map_err(|_| invalid("not a valid port"))?,
                None if tail.is_empty() => DEFAULT_QUIC_PORT,
                None => return Err(invalid("not a valid address")),
            };
            return Ok(QuicEntry {
                host: host.to_string(),
                port,
            });
        }

        // A bare IPv6 literal carries colons of its own, so it is recognised
        // before the host:port split.
        if entry.parse::<IpAddr>().is_ok() {
            return Ok(QuicEntry {
                host: entry.to_string(),
                port: DEFAULT_QUIC_PORT,
            });
        }

        match entry.rsplit_once(':') {
            Some((host, port)) if !host.is_empty() => Ok(QuicEntry {
                host: host.to_string(),
                port: port.parse().map_err(|_| invalid("not a valid port"))?,
            }),
            Some(_) => Err(invalid("not a valid address")),
            None => Ok(QuicEntry {
                host: entry.to_string(),
                port: DEFAULT_QUIC_PORT,
            }),
        }
    }

    /// Resolve this entry to every address it names. Called on every
    /// connection attempt — a cached resolution reproduces the hardcoded
    /// address on a delay.
    pub fn resolve(&self) -> io::Result<Vec<SocketAddr>> {
        (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map(|addrs| addrs.collect())
    }
}

impl fmt::Display for QuicEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// The controller endpoints this runner may dial, in declared order.
pub fn quic_addresses() -> Result<Vec<QuicEntry>, String> {
    quic_addresses_with(None)
}

/// The candidate list, with the caller's own view of the list variable — see
/// `path_helper::resolve_socket_dir_with` for why a host supplies one.
pub fn quic_addresses_with(list: Option<&str>) -> Result<Vec<QuicEntry>, String> {
    let from_env = env::var(QUIC_ADDRESSES_VAR).ok();
    let list = list
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .or(from_env.as_deref());

    parse_quic_addresses(list, env::var(RETIRED_QUIC_ADDRESS_VAR).ok().as_deref())
}

fn parse_quic_addresses(
    raw: Option<&str>,
    retired: Option<&str>,
) -> Result<Vec<QuicEntry>, String> {
    if raw.is_none() && retired.is_some() {
        return Err(format!(
            "{} is no longer honoured; set {} instead — it takes a comma-separated list \
             whose entries may be names or literal addresses",
            RETIRED_QUIC_ADDRESS_VAR, QUIC_ADDRESSES_VAR
        ));
    }

    let list = raw.map(str::trim).filter(|v| !v.is_empty());
    let entries: Result<Vec<QuicEntry>, String> = list
        .unwrap_or(DEFAULT_QUIC_ADDRESSES)
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(QuicEntry::parse)
        .collect();

    let entries = entries?;
    if entries.is_empty() {
        return Err(format!("{} names no endpoint", QUIC_ADDRESSES_VAR));
    }

    Ok(entries)
}

pub fn ca_cert_path() -> String {
    env::var("FLEZHA_CA_CERT_PATH").unwrap_or_else(|_| "app_data/certs/ca.pem".to_string())
}

/// Client certificate presented to a QUIC server that requires peer
/// authentication. Defaults to the controller-generated pair so a local
/// runner sharing `app_data/` authenticates out of the box; remote runners
/// point these at operator-provisioned certs.
pub fn quic_client_cert_path() -> String {
    env::var("FLEZHA_QUIC_CLIENT_CERT").unwrap_or_else(|_| "app_data/certs/client.pem".to_string())
}

pub fn quic_client_key_path() -> String {
    env::var("FLEZHA_QUIC_CLIENT_KEY").unwrap_or_else(|_| "app_data/certs/client.key".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The pure parser is tested instead of the env accessors themselves: the
    // frame-limit accessor's OnceLock cache is process-global, so an
    // env-mutating test would race other tests in the same binary.
    #[test]
    fn byte_knob_override_honoured_and_default_otherwise() {
        assert_eq!(
            parse_positive_bytes(Some("1024"), DEFAULT_MAX_FRAME_BYTES),
            1024
        );
        assert_eq!(
            parse_positive_bytes(None, DEFAULT_MAX_FRAME_BYTES),
            DEFAULT_MAX_FRAME_BYTES
        );
        assert_eq!(
            parse_positive_bytes(Some("nonsense"), DEFAULT_MAX_FRAME_BYTES),
            DEFAULT_MAX_FRAME_BYTES
        );
        assert_eq!(
            parse_positive_bytes(Some("0"), DEFAULT_MAX_FRAME_BYTES),
            DEFAULT_MAX_FRAME_BYTES
        );
        assert_eq!(max_write_buffer_bytes(), DEFAULT_MAX_WRITE_BUFFER_BYTES);
    }

    fn entries(raw: &str) -> Vec<QuicEntry> {
        parse_quic_addresses(Some(raw), None).expect("parses")
    }

    #[test]
    fn a_list_entry_may_be_a_name_or_a_literal_and_its_port_defaults() {
        assert_eq!(
            entries("controller.internal,10.0.0.7:5000,10.0.0.8,[::1]:4500,::1"),
            vec![
                QuicEntry {
                    host: "controller.internal".into(),
                    port: DEFAULT_QUIC_PORT
                },
                QuicEntry {
                    host: "10.0.0.7".into(),
                    port: 5000
                },
                QuicEntry {
                    host: "10.0.0.8".into(),
                    port: DEFAULT_QUIC_PORT
                },
                QuicEntry {
                    host: "::1".into(),
                    port: 4500
                },
                QuicEntry {
                    host: "::1".into(),
                    port: DEFAULT_QUIC_PORT
                },
            ]
        );
    }

    #[test]
    fn whitespace_and_empty_entries_are_forgiven_but_a_bad_port_is_not() {
        assert_eq!(
            entries(" a.example , , b.example:9 "),
            vec![
                QuicEntry {
                    host: "a.example".into(),
                    port: DEFAULT_QUIC_PORT
                },
                QuicEntry {
                    host: "b.example".into(),
                    port: 9
                },
            ]
        );

        let refusal = parse_quic_addresses(Some("a.example:http"), None).unwrap_err();
        assert!(refusal.contains("not a valid port"), "{}", refusal);
    }

    #[test]
    fn an_unset_list_keeps_the_local_default() {
        assert_eq!(
            parse_quic_addresses(None, None).expect("default"),
            vec![QuicEntry {
                host: "127.0.0.1".into(),
                port: DEFAULT_QUIC_PORT
            }]
        );
    }

    #[test]
    fn the_retired_variable_alone_is_refused_naming_its_replacement() {
        let refusal = parse_quic_addresses(None, Some("127.0.0.1:4433")).unwrap_err();
        assert!(refusal.contains(RETIRED_QUIC_ADDRESS_VAR), "{}", refusal);
        assert!(refusal.contains(QUIC_ADDRESSES_VAR), "{}", refusal);

        // Set alongside the replacement it is simply ignored: an operator
        // mid-migration is not stopped by the variable they already replaced.
        assert!(parse_quic_addresses(Some("a.example"), Some("127.0.0.1:4433")).is_ok());
    }

    #[test]
    fn an_entry_renders_as_the_address_it_dials() {
        assert_eq!(
            QuicEntry {
                host: "a.example".into(),
                port: 4433
            }
            .to_string(),
            "a.example:4433"
        );
        assert_eq!(
            QuicEntry {
                host: "::1".into(),
                port: 4433
            }
            .to_string(),
            "[::1]:4433"
        );
    }

    #[test]
    fn a_name_that_does_not_resolve_reports_an_error_rather_than_panicking() {
        let entry = QuicEntry {
            host: "no-such-host.invalid".into(),
            port: 4433,
        };
        assert!(entry.resolve().is_err());

        let local = QuicEntry {
            host: "127.0.0.1".into(),
            port: 4433,
        };
        assert_eq!(
            local.resolve().expect("literal resolves"),
            vec!["127.0.0.1:4433".parse::<SocketAddr>().unwrap()]
        );
    }
}
