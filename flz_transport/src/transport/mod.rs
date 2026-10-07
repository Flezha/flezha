pub mod close_reason;
pub mod config;
pub mod liveness;
pub mod protocol;
pub mod quic;
/// The in-memory `quinn_proto` pair both send-buffer test suites drive. Test
/// code, compiled into the library only for `flz_nif`'s tests, which reach it
/// through the `test-support` feature.
#[cfg(any(test, feature = "test-support"))]
#[path = "quic_pair_tests.rs"]
pub mod quic_pair;
pub mod send_gate;
pub mod tls;

use std::io::{self, Read, Write};
use std::net::Shutdown;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, TryLockError};

use quic::QuicBidiStream;

/// Where a frame written to a [`FlezhaStream::Local`] goes: the in-process
/// owner of the run parses it and delivers it, instead of a socket carrying
/// it to a controller. Installed only by the local driver; the dialling path
/// never builds one.
pub type LocalFrameSink = Arc<dyn Fn(&[u8]) -> io::Result<()> + Send + Sync>;

pub enum FlezhaStream {
    #[cfg(unix)]
    Unix(UnixWire),
    Quic(QuicBidiStream),
    /// A run with no controller: writes are handed to the owner in process
    /// and reads are at end of stream, so every writer on this seam keeps the
    /// one code path it has.
    Local(LocalFrameSink),
}

/// A Unix socket and the lock its frames are written under, shared by every
/// handle cloned from it. The kernel may queue one large write in pieces, and
/// another thread's frame landing between them corrupts the stream for good.
#[cfg(unix)]
pub struct UnixWire {
    stream: UnixStream,
    frames: Arc<Mutex<()>>,
}

#[cfg(unix)]
impl std::ops::Deref for UnixWire {
    type Target = UnixStream;

    fn deref(&self) -> &UnixStream {
        &self.stream
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const SEND_NOWAIT: libc::c_int = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
const SEND_NOWAIT: libc::c_int = libc::MSG_DONTWAIT;

#[cfg(unix)]
impl UnixWire {
    fn clone_handle(&self) -> io::Result<UnixWire> {
        Ok(UnixWire {
            stream: self.stream.try_clone()?,
            frames: Arc::clone(&self.frames),
        })
    }

    // The lock guards no data, so a writer that panicked holding it left
    // nothing to repair.
    fn write_frame(&self, frame: &[u8]) -> io::Result<()> {
        let _held = self.frames.lock().unwrap_or_else(|p| p.into_inner());
        (&self.stream).write_all(frame)
    }

    fn try_write_frame(&self, frame: &[u8]) -> io::Result<()> {
        let _held = match self.frames.try_lock() {
            Ok(held) => held,
            Err(TryLockError::Poisoned(p)) => p.into_inner(),
            Err(TryLockError::WouldBlock) => return Err(io::ErrorKind::WouldBlock.into()),
        };
        // SAFETY: the fd is the stream's own, open for as long as `self` is
        // borrowed, and `frame` is readable for its whole length.
        let sent = unsafe {
            libc::send(
                self.stream.as_raw_fd(),
                frame.as_ptr().cast(),
                frame.len(),
                SEND_NOWAIT,
            )
        };
        match usize::try_from(sent) {
            Ok(n) if n == frame.len() => Ok(()),
            Ok(n) if n > 0 => (&self.stream).write_all(&frame[n..]),
            Ok(_) => Err(io::ErrorKind::WouldBlock.into()),
            Err(_) => Err(io::Error::last_os_error()),
        }
    }
}

impl FlezhaStream {
    #[cfg(unix)]
    pub fn unix(stream: UnixStream) -> FlezhaStream {
        FlezhaStream::Unix(UnixWire {
            stream,
            frames: Arc::default(),
        })
    }

    pub fn clone_handle(&self) -> io::Result<FlezhaStream> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(wire) => Ok(FlezhaStream::Unix(wire.clone_handle()?)),
            FlezhaStream::Quic(stream) => Ok(FlezhaStream::Quic(stream.clone_handle())),
            FlezhaStream::Local(sink) => Ok(FlezhaStream::Local(Arc::clone(sink))),
        }
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => stream.set_nonblocking(nonblocking),
            FlezhaStream::Quic(_) => {
                let _ = nonblocking;
                Ok(()) // QUIC manages this internally
            }
            FlezhaStream::Local(_) => Ok(()),
        }
    }

    pub fn set_read_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => stream.set_read_timeout(timeout),
            // QUIC reads block on a crossbeam channel; honor the deadline
            // there (a no-op here would silently skip control-channel liveness
            // when FLEZHA_TRANSPORT=quic).
            FlezhaStream::Quic(stream) => stream.set_read_timeout(timeout),
            FlezhaStream::Local(_) => Ok(()),
        }
    }

    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => stream.shutdown(how),
            FlezhaStream::Quic(_) => {
                let _ = how;
                Ok(()) // QUIC streams are closed when dropped
            }
            FlezhaStream::Local(_) => Ok(()),
        }
    }

    /// Write one whole frame, never interleaved with another writer's.
    pub fn write_frame(&self, frame: &[u8]) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(wire) => wire.write_frame(frame),
            FlezhaStream::Quic(stream) => (&*stream).write_all(frame),
            FlezhaStream::Local(sink) => sink(frame),
        }
    }

    /// Write one whole frame only if the stream takes it without waiting for
    /// the peer or another writer; `WouldBlock` otherwise, with nothing
    /// written. A frame the socket took part of is finished with a blocking
    /// write, since a partial frame would corrupt every frame behind it.
    pub fn try_write_frame(&self, frame: &[u8]) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(wire) => wire.try_write_frame(frame),
            FlezhaStream::Quic(stream) => stream.try_write_frame(frame).map(|_| ()),
            FlezhaStream::Local(sink) => sink(frame),
        }
    }

    #[cfg(unix)]
    pub fn as_raw_fd(&self) -> Option<i32> {
        match self {
            FlezhaStream::Unix(stream) => Some(stream.as_raw_fd()),
            FlezhaStream::Quic(_) | FlezhaStream::Local(_) => None,
        }
    }
}

impl Read for FlezhaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(wire) => wire.stream.read(buf),
            FlezhaStream::Quic(stream) => stream.read(buf),
            FlezhaStream::Local(_) => Ok(0),
        }
    }
}

impl Write for FlezhaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(wire) => wire.stream.write(buf),
            FlezhaStream::Quic(stream) => stream.write(buf),
            FlezhaStream::Local(sink) => sink(buf).map(|()| buf.len()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(wire) => wire.stream.flush(),
            FlezhaStream::Quic(stream) => stream.flush(),
            FlezhaStream::Local(_) => Ok(()),
        }
    }
}

// Implement Read/Write for &FlezhaStream (needed for Arc<FlezhaStream>)
impl Read for &FlezhaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(wire) => (&wire.stream).read(buf),
            FlezhaStream::Quic(stream) => (&*stream).read(buf),
            FlezhaStream::Local(_) => Ok(0),
        }
    }
}

impl Write for &FlezhaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(wire) => (&wire.stream).write(buf),
            FlezhaStream::Quic(stream) => (&*stream).write(buf),
            FlezhaStream::Local(sink) => sink(buf).map(|()| buf.len()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(wire) => (&wire.stream).flush(),
            FlezhaStream::Quic(stream) => (&*stream).flush(),
            FlezhaStream::Local(_) => Ok(()),
        }
    }
}
