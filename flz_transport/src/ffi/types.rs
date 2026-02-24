use std::os::raw::c_char;

/// Status codes returned by all flz_transport functions.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlezhaStatus {
    Ok = 0,
    InvalidArgument = 1,
    InvalidState = 2,
    TransportError = 3,
    IoError = 4,
    AlreadyRunning = 5,
    NotRunning = 6,
    InternalError = 7,
}

/// Transport type selection.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlezhaTransport {
    Uds = 0,
    Quic = 1,
}

/// Log severity levels.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlezhaLogLevel {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
    Trace = 4,
}

/// Callback type for logging. Called from library-managed threads.
///
/// # Safety
/// - Must not call back into the library (no re-entrancy).
/// - `message` is valid only for the duration of the call.
/// - `user_data` is the pointer provided at registration time.
pub type FlezhaLogFn = Option<
    unsafe extern "C" fn(level: FlezhaLogLevel, message: *const c_char, user_data: *mut std::ffi::c_void),
>;

/// Callbacks for server mode (agent/Elixir NIF).
#[repr(C)]
pub struct FlezhaServerCallbacks {
    /// Called when a new connection is established.
    pub on_connection: Option<
        unsafe extern "C" fn(conn_id: u64, user_data: *mut std::ffi::c_void),
    >,
    /// Called when data arrives on a stream.
    pub on_data: Option<
        unsafe extern "C" fn(
            conn_id: u64,
            stream_id: u64,
            data: *const u8,
            data_len: usize,
            user_data: *mut std::ffi::c_void,
        ),
    >,
    /// Called when a stream is closed.
    pub on_stream_closed: Option<
        unsafe extern "C" fn(conn_id: u64, stream_id: u64, user_data: *mut std::ffi::c_void),
    >,
    /// Called when a connection is lost.
    pub on_connection_lost: Option<
        unsafe extern "C" fn(conn_id: u64, user_data: *mut std::ffi::c_void),
    >,
    pub user_data: *mut std::ffi::c_void,
}

// Safety: callback pointers + user_data are intentionally shared across threads
unsafe impl Send for FlezhaServerCallbacks {}
unsafe impl Sync for FlezhaServerCallbacks {}
