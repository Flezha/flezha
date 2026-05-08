//! Context-lifecycle JNI entry points.
//!
//! The Java-side handle is just a `jlong` whose bit pattern is a
//! `Box::into_raw(Box::new(JniContext{...}))` pointer. The bridge owns all
//! transport state — a `QuicClient` when in QUIC mode, a registry of open
//! streams, and a `Dispatcher` callback reference that surfaces `on_data`,
//! `on_stream_closed`, and `on_connection_lost` events onto Java via
//! `AttachCurrentThread`.

use std::collections::HashMap;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::{protocol, FlezhaStream};
#[cfg(unix)]
use flz_transport::utility::path_helper;
use jni::objects::{GlobalRef, JByteArray, JClass, JObject, JString, JValue};
use jni::sys::{jint, jlong};
use jni::{JNIEnv, JavaVM};

use crate::callbacks::{dispatch_connection_lost, dispatch_data, dispatch_stream_closed};
use crate::throw_native;

// reason: STREAM_TYPE_* are documentation constants mirroring flz_transport::protocol; consumed by Java via JNI
/// Stream type constants — kept in sync with
/// `flz_transport::transport::protocol::STREAM_TYPE_*`. Exposed for
/// documentation; the Java side carries its own mirror.
#[allow(dead_code)]
pub const STREAM_TYPE_CONTROL: i32 = 0;
#[allow(dead_code)]
pub const STREAM_TYPE_PRIMARY: i32 = protocol::STREAM_TYPE_PRIMARY as i32;
#[allow(dead_code)]
pub const STREAM_TYPE_SECONDARY: i32 = protocol::STREAM_TYPE_SECONDARY as i32;

pub(crate) struct JniContext {
    pub(crate) config: Config,
    pub(crate) jvm: JavaVM,
    pub(crate) dispatcher: Mutex<Option<GlobalRef>>,
    pub(crate) log_callback: Mutex<Option<GlobalRef>>,
    pub(crate) quic_client: Mutex<Option<Arc<QuicClient>>>,
    pub(crate) streams: Mutex<HashMap<u64, Arc<StreamEntry>>>,
    pub(crate) next_stream_id: AtomicU64,
    pub(crate) destroyed: AtomicBool,
}

pub(crate) struct StreamEntry {
    pub(crate) stream: Arc<FlezhaStream>,
    pub(crate) shutdown: Arc<AtomicBool>,
    pub(crate) reader: Mutex<Option<JoinHandle<()>>>,
    pub(crate) write_lock: Mutex<()>,
}

#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) transport: Transport,
    pub(crate) quic_address: String,
    pub(crate) ca_cert_path: String,
    #[cfg_attr(not(unix), allow(dead_code))]
    pub(crate) socket_base_path: String,
    pub(crate) executor_id: Mutex<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transport {
    Uds,
    Quic,
}

fn ctx_from_handle<'h>(handle: jlong) -> Option<&'h JniContext> {
    if handle == 0 {
        None
    } else {
        unsafe { Some(&*(handle as *const JniContext)) }
    }
}

fn jstring_to_string(env: &mut JNIEnv, jstr: &JString) -> Option<String> {
    if jstr.is_null() {
        return None;
    }
    env.get_string(jstr).ok().map(|s| s.into())
}

/// `long contextNew(String transport, String quicAddress, String caCertPath, String socketBasePath)`
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_contextNew<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    transport_jstr: JString<'local>,
    quic_address_jstr: JString<'local>,
    ca_cert_path_jstr: JString<'local>,
    socket_base_path_jstr: JString<'local>,
) -> jlong {
    let transport_str = jstring_to_string(&mut env, &transport_jstr).unwrap_or_default();
    let transport = match transport_str.as_str() {
        "quic" => Transport::Quic,
        _ => Transport::Uds,
    };

    let quic_address = jstring_to_string(&mut env, &quic_address_jstr)
        .unwrap_or_else(|| "127.0.0.1:4433".to_string());
    let ca_cert_path = jstring_to_string(&mut env, &ca_cert_path_jstr)
        .unwrap_or_else(|| "app_data/certs/ca.pem".to_string());
    let socket_base_path = jstring_to_string(&mut env, &socket_base_path_jstr)
        .unwrap_or_else(|| "app_data/sockets".to_string());

    let jvm = match env.get_java_vm() {
        Ok(jvm) => jvm,
        Err(e) => {
            throw_native(&mut env, 7, &format!("JavaVM handle unavailable: {}", e));
            return 0;
        }
    };

    let ctx = JniContext {
        config: Config {
            transport,
            quic_address,
            ca_cert_path,
            socket_base_path,
            executor_id: Mutex::new(String::from("undefined")),
        },
        jvm,
        dispatcher: Mutex::new(None),
        log_callback: Mutex::new(None),
        quic_client: Mutex::new(None),
        streams: Mutex::new(HashMap::new()),
        next_stream_id: AtomicU64::new(1),
        destroyed: AtomicBool::new(false),
    };

    let boxed = Box::new(ctx);
    Box::into_raw(boxed) as jlong
}

/// `int contextDestroy(long handle)`
///
/// Returns `FlezhaStatus` (0 on success, 2=InvalidState on double-destroy).
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_contextDestroy<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    if handle == 0 {
        return 1; // InvalidArgument
    }

    unsafe {
        let ctx_ptr = handle as *mut JniContext;
        if (*ctx_ptr).destroyed.swap(true, Ordering::SeqCst) {
            return 2; // InvalidState — already destroyed
        }

        // Shut down all streams before dropping the context.
        let streams: Vec<Arc<StreamEntry>> = {
            let mut guard = (*ctx_ptr).streams.lock().unwrap();
            guard.drain().map(|(_, v)| v).collect()
        };
        let current_tid = thread::current().id();
        for entry in streams {
            entry.shutdown.store(true, Ordering::SeqCst);
            let _ = entry.stream.shutdown(std::net::Shutdown::Both);
            if let Some(handle) = entry.reader.lock().unwrap().take() {
                // Never block-join the thread that called us (happens when a
                // reader panics into on_connection_lost → Java → contextDestroy).
                if handle.thread().id() != current_tid {
                    let _ = handle.join();
                }
            }
        }

        {
            let mut guard = (*ctx_ptr).quic_client.lock().unwrap();
            guard.take(); // Drop QuicClient → triggers driver shutdown.
        }

        drop(Box::from_raw(ctx_ptr));
    }

    0
}

/// `void setDispatcher(long handle, Object dispatcher)`
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_setDispatcher<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    dispatcher: JObject<'local>,
) {
    let ctx = match ctx_from_handle(handle) {
        Some(c) => c,
        None => {
            throw_native(&mut env, 1, "null context handle");
            return;
        }
    };

    let global = match env.new_global_ref(dispatcher) {
        Ok(g) => g,
        Err(e) => {
            throw_native(&mut env, 7, &format!("failed to take global ref: {}", e));
            return;
        }
    };

    *ctx.dispatcher.lock().unwrap() = Some(global);
}

/// `void setExecutorId(long handle, String id)`
///
/// Called by the Java SDK after receiving `AssignExecutorId`. The JNI
/// side needs the id to build per-stream UDS socket paths during
/// `openStream(PRIMARY/SECONDARY, ...)`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_setExecutorId<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    id_jstr: JString<'local>,
) {
    let ctx = match ctx_from_handle(handle) {
        Some(c) => c,
        None => {
            throw_native(&mut env, 1, "null context handle");
            return;
        }
    };
    let id = jstring_to_string(&mut env, &id_jstr).unwrap_or_default();
    *ctx.config.executor_id.lock().unwrap() = id;
}

/// `long openControlStream(long handle)`
///
/// Opens the control channel (UDS control socket or QUIC first bidi
/// stream) and spawns a reader thread that pumps incoming bytes to the
/// registered dispatcher via `on_data`. On disconnect or error, emits
/// `on_connection_lost`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_openControlStream<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let ctx = match ctx_from_handle(handle) {
        Some(c) => c,
        None => {
            throw_native(&mut env, 1, "null context handle");
            return 0;
        }
    };

    let stream = match open_control(ctx) {
        Ok(s) => s,
        Err(e) => {
            throw_native(&mut env, 3, &format!("control stream open failed: {}", e));
            return 0;
        }
    };

    register_and_spawn_reader(ctx, stream)
}

/// `long openStream(long handle, int streamType, int index)`
///
/// Opens a primary (`0x01`) or secondary (`0x02`) stream. Primary streams
/// spawn a reader thread; secondary streams do not (they are write-only
/// from the SDK's perspective, though the type is bidi at the transport
/// layer). The returned long is the Java-visible stream id.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_openStream<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    stream_type: jint,
    index: jint,
) -> jlong {
    let ctx = match ctx_from_handle(handle) {
        Some(c) => c,
        None => {
            throw_native(&mut env, 1, "null context handle");
            return 0;
        }
    };

    let stream_type_u8 = stream_type as u8;
    if stream_type_u8 != protocol::STREAM_TYPE_PRIMARY
        && stream_type_u8 != protocol::STREAM_TYPE_SECONDARY
    {
        throw_native(
            &mut env,
            1,
            &format!("unsupported stream type: {}", stream_type),
        );
        return 0;
    }

    let stream = match open_data_stream(ctx, stream_type_u8, index as u8) {
        Ok(s) => s,
        Err(e) => {
            throw_native(&mut env, 3, &format!("data stream open failed: {}", e));
            return 0;
        }
    };

    if stream_type_u8 == protocol::STREAM_TYPE_PRIMARY {
        register_and_spawn_reader(ctx, stream)
    } else {
        register_without_reader(ctx, stream)
    }
}

/// `int streamWrite(long handle, long streamId, byte[] data)`
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_streamWrite<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    stream_id: jlong,
    data: JByteArray<'local>,
) -> jint {
    let ctx = match ctx_from_handle(handle) {
        Some(c) => c,
        None => {
            throw_native(&mut env, 1, "null context handle");
            return 1;
        }
    };

    let bytes = match env.convert_byte_array(&data) {
        Ok(v) => v,
        Err(e) => {
            throw_native(&mut env, 1, &format!("byte[] conversion failed: {}", e));
            return 1;
        }
    };

    let entry = {
        let guard = ctx.streams.lock().unwrap();
        match guard.get(&(stream_id as u64)) {
            Some(e) => Arc::clone(e),
            None => {
                throw_native(&mut env, 1, "unknown stream id");
                return 1;
            }
        }
    };

    let _write_lock = entry.write_lock.lock().unwrap();
    match (&*entry.stream).write_all(&bytes) {
        Ok(_) => 0,
        Err(e) => {
            throw_native(&mut env, 4, &format!("stream write failed: {}", e));
            4 // IoError
        }
    }
}

/// `void streamClose(long handle, long streamId)`
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_streamClose<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    stream_id: jlong,
) {
    let ctx = match ctx_from_handle(handle) {
        Some(c) => c,
        None => {
            throw_native(&mut env, 1, "null context handle");
            return;
        }
    };

    let entry = {
        let mut guard = ctx.streams.lock().unwrap();
        guard.remove(&(stream_id as u64))
    };

    if let Some(entry) = entry {
        entry.shutdown.store(true, Ordering::SeqCst);
        let _ = entry.stream.shutdown(std::net::Shutdown::Both);
        if let Some(handle) = entry.reader.lock().unwrap().take() {
            // Never block-join the current thread (e.g., when a
            // ExecutorDisconnect handler on the control reader thread
            // closes the control stream in-line).
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn open_control(ctx: &JniContext) -> std::io::Result<Arc<FlezhaStream>> {
    match ctx.config.transport {
        #[cfg(unix)]
        Transport::Uds => {
            let path = format!(
                "{}/executor_control_channel.sock",
                ctx.config.socket_base_path.trim_end_matches('/')
            );
            let _ = path_helper::control_socket_path(); // reserved for future use
            let unix = UnixStream::connect(&path)?;
            Ok(Arc::new(FlezhaStream::Unix(unix)))
        }
        #[cfg(not(unix))]
        Transport::Uds => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "UDS transport is not supported on this platform; use QUIC",
        )),
        Transport::Quic => {
            let client = QuicClient::connect(&ctx.config.quic_address, &ctx.config.ca_cert_path)
                .map_err(std::io::Error::other)?;
            let client = Arc::new(client);
            let control = client.open_bidi_stream().map_err(std::io::Error::other)?;
            *ctx.quic_client.lock().unwrap() = Some(client);
            Ok(Arc::new(FlezhaStream::Quic(control)))
        }
    }
}

fn open_data_stream(
    ctx: &JniContext,
    stream_type: u8,
    index: u8,
) -> std::io::Result<Arc<FlezhaStream>> {
    match ctx.config.transport {
        #[cfg(unix)]
        Transport::Uds => {
            let kind = match stream_type {
                protocol::STREAM_TYPE_PRIMARY => "primary",
                protocol::STREAM_TYPE_SECONDARY => "secondary",
                _ => unreachable!(),
            };
            let id = ctx.config.executor_id.lock().unwrap().clone();
            let path = format!(
                "{}/{}_stream_{}.sock",
                ctx.config.socket_base_path.trim_end_matches('/'),
                kind,
                id
            );
            let unix = UnixStream::connect(&path)?;
            Ok(Arc::new(FlezhaStream::Unix(unix)))
        }
        #[cfg(not(unix))]
        Transport::Uds => {
            let _ = stream_type;
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "UDS transport is not supported on this platform; use QUIC",
            ))
        }
        Transport::Quic => {
            let client = {
                let guard = ctx.quic_client.lock().unwrap();
                guard.clone()
            }
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "QUIC client not connected",
                )
            })?;

            let mut stream = client.open_bidi_stream().map_err(std::io::Error::other)?;
            stream.write_all(&[stream_type, index])?;
            Ok(Arc::new(FlezhaStream::Quic(stream)))
        }
    }
}

fn register_without_reader(ctx: &JniContext, stream: Arc<FlezhaStream>) -> jlong {
    let id = ctx.next_stream_id.fetch_add(1, Ordering::SeqCst);
    let entry = Arc::new(StreamEntry {
        stream,
        shutdown: Arc::new(AtomicBool::new(false)),
        reader: Mutex::new(None),
        write_lock: Mutex::new(()),
    });
    ctx.streams.lock().unwrap().insert(id, entry);
    id as jlong
}

fn register_and_spawn_reader(ctx: &JniContext, stream: Arc<FlezhaStream>) -> jlong {
    let id = ctx.next_stream_id.fetch_add(1, Ordering::SeqCst);
    let shutdown = Arc::new(AtomicBool::new(false));
    let entry = Arc::new(StreamEntry {
        stream: Arc::clone(&stream),
        shutdown: Arc::clone(&shutdown),
        reader: Mutex::new(None),
        write_lock: Mutex::new(()),
    });
    ctx.streams.lock().unwrap().insert(id, Arc::clone(&entry));

    let ctx_ptr = ctx as *const JniContext as usize;
    let shutdown_clone = Arc::clone(&shutdown);
    let reader_stream = Arc::clone(&stream);

    let join = thread::Builder::new()
        .name(format!("flz-jni-reader-{}", id))
        .spawn(move || {
            // SAFETY: the JniContext is pinned for the lifetime of the reader
            // by the Java-side contract: the JVM guarantees it will call
            // contextDestroy (which joins all readers) before releasing the
            // handle.
            let ctx: &JniContext = unsafe { &*(ctx_ptr as *const JniContext) };
            reader_loop(ctx, id, reader_stream, shutdown_clone);
        })
        .expect("failed to spawn reader thread");

    *entry.reader.lock().unwrap() = Some(join);
    id as jlong
}

fn reader_loop(
    ctx: &JniContext,
    stream_id: u64,
    stream: Arc<FlezhaStream>,
    shutdown: Arc<AtomicBool>,
) {
    let mut buf = [0u8; 8192];
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        match (&*stream).read(&mut buf) {
            Ok(0) => {
                dispatch_stream_closed(ctx, stream_id);
                dispatch_connection_lost(ctx, stream_id);
                break;
            }
            Ok(n) => {
                dispatch_data(ctx, stream_id, &buf[..n]);
            }
            Err(e) if matches!(e.kind(), std::io::ErrorKind::Interrupted) => continue,
            Err(_) => {
                dispatch_stream_closed(ctx, stream_id);
                dispatch_connection_lost(ctx, stream_id);
                break;
            }
        }
    }
}

// reason: log_callback is written by logging.rs; dispatcher by setDispatcher; rustc can't see those cross-module writes as field reads
#[allow(dead_code)]
fn _retain_fields(_ctx: &JniContext) {
    let _ = &_ctx.log_callback;
    let _ = &_ctx.dispatcher;
}

// Helpful unused-value suppression for JValue — re-exported so callbacks.rs can use.
pub(crate) fn _jvalue_sink<'a>(_v: JValue<'a, 'a>) {}
