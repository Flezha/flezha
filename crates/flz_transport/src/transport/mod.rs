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
use std::sync::Arc;
#[cfg(unix)]
use std::sync::{Mutex, TryLockError};
use std::time::Duration;

use quic::QuicBidiStream;

/// One client transport's stream, so a new transport is one impl. A knob a
/// wire lacks keeps its no-op default: QUIC's driver owns blocking and closes a
/// stream when its last handle drops, and a local run has no medium at all.
pub trait Wire: Sized {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize>;
    fn write(&self, buf: &[u8]) -> io::Result<usize>;

    /// Write one whole frame, never interleaved with another writer's.
    fn write_frame(&self, frame: &[u8]) -> io::Result<()>;

    /// Write one whole frame only if the stream takes it without waiting for
    /// the peer or another writer; `WouldBlock` otherwise, with nothing
    /// written. A frame the medium took part of is finished with a blocking
    /// write, since a partial frame would corrupt every frame behind it.
    fn try_write_frame(&self, frame: &[u8]) -> io::Result<()>;

    fn clone_handle(&self) -> io::Result<Self>;

    fn set_nonblocking(&self, _nonblocking: bool) -> io::Result<()> {
        Ok(())
    }

    fn set_read_timeout(&self, _timeout: Option<Duration>) -> io::Result<()> {
        Ok(())
    }

    fn shutdown(&self, _how: Shutdown) -> io::Result<()> {
        Ok(())
    }

    fn raw_fd(&self) -> Option<i32> {
        None
    }
}

/// Where a frame written to a [`FlezhaStream::Local`] goes: the in-process
/// owner of the run parses it and delivers it, instead of a socket carrying
/// it to a controller. Installed only by the local driver; the dialling path
/// never builds one.
pub type LocalFrameSink = Arc<dyn Fn(&[u8]) -> io::Result<()> + Send + Sync>;

/// A stream on whichever wire the run uses. An enum rather than a boxed
/// [`Wire`], so each call is one match and a direct call, never a virtual one.
pub enum FlezhaStream {
    #[cfg(unix)]
    Unix(UnixWire),
    Quic(QuicBidiStream),
    /// A run with no controller: writes are handed to the owner in process
    /// and reads are at end of stream, so every writer on this seam keeps the
    /// one code path it has.
    Local(LocalFrameSink),
}

/// Call `$call` on the variant's wire, bound as `$wire`.
macro_rules! on_wire {
    ($stream:expr, $wire:ident => $call:expr) => {
        match $stream {
            #[cfg(unix)]
            FlezhaStream::Unix($wire) => $call,
            FlezhaStream::Quic($wire) => $call,
            FlezhaStream::Local($wire) => $call,
        }
    };
}

/// A Unix socket and the lock its frames are written under, shared by every
/// handle cloned from it. The kernel may queue one large write in pieces, and
/// another thread's frame landing between them corrupts the stream for good.
#[cfg(unix)]
pub struct UnixWire {
    stream: UnixStream,
    frames: Arc<Mutex<()>>,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const SEND_NOWAIT: libc::c_int = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
const SEND_NOWAIT: libc::c_int = libc::MSG_DONTWAIT;

#[cfg(unix)]
impl Wire for UnixWire {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        (&self.stream).read(buf)
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        (&self.stream).write(buf)
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

    fn clone_handle(&self) -> io::Result<UnixWire> {
        Ok(UnixWire {
            stream: self.stream.try_clone()?,
            frames: Arc::clone(&self.frames),
        })
    }

    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.stream.set_nonblocking(nonblocking)
    }

    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.stream.shutdown(how)
    }

    fn raw_fd(&self) -> Option<i32> {
        Some(self.stream.as_raw_fd())
    }
}

impl Wire for LocalFrameSink {
    fn read(&self, _buf: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        self(buf).map(|()| buf.len())
    }

    fn write_frame(&self, frame: &[u8]) -> io::Result<()> {
        self(frame)
    }

    fn try_write_frame(&self, frame: &[u8]) -> io::Result<()> {
        self(frame)
    }

    fn clone_handle(&self) -> io::Result<Self> {
        Ok(Arc::clone(self))
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
        Ok(match self {
            #[cfg(unix)]
            FlezhaStream::Unix(wire) => FlezhaStream::Unix(wire.clone_handle()?),
            FlezhaStream::Quic(wire) => FlezhaStream::Quic(wire.clone_handle()?),
            FlezhaStream::Local(wire) => FlezhaStream::Local(wire.clone_handle()?),
        })
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        on_wire!(self, wire => wire.set_nonblocking(nonblocking))
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        on_wire!(self, wire => wire.set_read_timeout(timeout))
    }

    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        on_wire!(self, wire => wire.shutdown(how))
    }

    pub fn write_frame(&self, frame: &[u8]) -> io::Result<()> {
        on_wire!(self, wire => wire.write_frame(frame))
    }

    pub fn try_write_frame(&self, frame: &[u8]) -> io::Result<()> {
        on_wire!(self, wire => wire.try_write_frame(frame))
    }

    #[cfg(unix)]
    pub fn as_raw_fd(&self) -> Option<i32> {
        on_wire!(self, wire => wire.raw_fd())
    }
}

impl Read for FlezhaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buf)
    }
}

impl Write for FlezhaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// What an `Arc<FlezhaStream>` reads and writes through.
impl Read for &FlezhaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        on_wire!(*self, wire => wire.read(buf))
    }
}

// No wire buffers in user space, so a flush has nothing to push.
impl Write for &FlezhaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        on_wire!(*self, wire => wire.write(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
