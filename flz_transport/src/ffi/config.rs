use std::ffi::CStr;
use std::os::raw::c_char;

use super::types::FlezhaTransport;

/// Configuration struct passed to `flezha_context_new`.
///
/// All string fields are borrowed pointers that must remain valid only
/// for the duration of the `flezha_context_new` call. The library copies
/// all string data internally.
#[repr(C)]
pub struct FlezhaConfig {
    /// Transport type: UDS or QUIC.
    pub transport: FlezhaTransport,
    /// QUIC server address (e.g., "127.0.0.1:4433"). Ignored for UDS.
    pub quic_address: *const c_char,
    /// Path to CA certificate PEM file. Ignored for UDS.
    pub ca_cert_path: *const c_char,
    /// Path to server certificate PEM file (server mode only).
    pub cert_path: *const c_char,
    /// Path to server private key PEM file (server mode only).
    pub key_path: *const c_char,
    /// Base path for UDS socket files. Ignored for QUIC.
    pub socket_base_path: *const c_char,
    /// Number of worker pool threads. 0 = auto-detect from CPU cores.
    pub worker_pool_size: u16,
    /// Number of concurrent I/O channels.
    pub concurrent_channel_count: u32,
}

/// Internal owned representation of the configuration.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct FlezhaConfigOwned {
    pub transport: FlezhaTransport,
    pub quic_address: String,
    pub ca_cert_path: String,
    pub cert_path: String,
    pub key_path: String,
    pub socket_base_path: String,
    pub worker_pool_size: u16,
    pub concurrent_channel_count: u32,
}

impl FlezhaConfigOwned {
    /// Convert from the C config struct, copying all string data.
    ///
    /// # Safety
    /// All non-null `*const c_char` fields must point to valid UTF-8 C strings.
    pub(crate) unsafe fn from_c(config: &FlezhaConfig) -> Self {
        FlezhaConfigOwned {
            transport: config.transport,
            quic_address: ptr_to_string(config.quic_address, "127.0.0.1:4433"),
            ca_cert_path: ptr_to_string(config.ca_cert_path, "app_data/certs/ca.pem"),
            cert_path: ptr_to_string(config.cert_path, "app_data/certs/cert.pem"),
            key_path: ptr_to_string(config.key_path, "app_data/certs/key.pem"),
            socket_base_path: ptr_to_string(config.socket_base_path, "app_data/sockets"),
            worker_pool_size: config.worker_pool_size,
            concurrent_channel_count: config.concurrent_channel_count,
        }
    }
}

unsafe fn ptr_to_string(ptr: *const c_char, default: &str) -> String {
    if ptr.is_null() {
        default.to_string()
    } else {
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    }
}

/// Create a default FlezhaConfig with sensible defaults.
///
/// All pointer fields are null (library uses built-in defaults).
/// Transport defaults to UDS.
#[no_mangle]
pub extern "C" fn flezha_config_default() -> FlezhaConfig {
    FlezhaConfig {
        transport: FlezhaTransport::Uds,
        quic_address: std::ptr::null(),
        ca_cert_path: std::ptr::null(),
        cert_path: std::ptr::null(),
        key_path: std::ptr::null(),
        socket_base_path: std::ptr::null(),
        worker_pool_size: 0,
        concurrent_channel_count: 1,
    }
}
