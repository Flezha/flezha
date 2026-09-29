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

use quic::QuicBidiStream;

/// Where a frame written to a [`FlezhaStream::Local`] goes: the in-process
/// owner of the run parses it and delivers it, instead of a socket carrying
/// it to a controller. Installed only by the local driver; the dialling path
/// never builds one.
pub type LocalFrameSink = Arc<dyn Fn(&[u8]) -> io::Result<()> + Send + Sync>;

pub enum FlezhaStream {
    #[cfg(unix)]
    Unix(UnixStream),
    Quic(QuicBidiStream),
    /// A run with no controller: writes are handed to the owner in process
    /// and reads are at end of stream, so every writer on this seam keeps the
    /// one code path it has.
    Local(LocalFrameSink),
}

impl FlezhaStream {
    pub fn clone_handle(&self) -> io::Result<FlezhaStream> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => Ok(FlezhaStream::Unix(stream.try_clone()?)),
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

    /// Write one whole frame only if the stream takes it without waiting for
    /// the peer; `WouldBlock` otherwise, with nothing written. A frame the
    /// socket took part of is finished with a blocking write, since a partial
    /// frame would corrupt every frame behind it.
    pub fn try_write_frame(&self, frame: &[u8]) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => try_send_unix(stream, frame),
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

#[cfg(any(target_os = "linux", target_os = "android"))]
const SEND_NOWAIT: libc::c_int = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
const SEND_NOWAIT: libc::c_int = libc::MSG_DONTWAIT;

#[cfg(unix)]
fn try_send_unix(stream: &UnixStream, frame: &[u8]) -> io::Result<()> {
    // SAFETY: the fd is the stream's own, open for as long as `stream` is
    // borrowed, and `frame` is readable for its whole length.
    let sent = unsafe {
        libc::send(
            stream.as_raw_fd(),
            frame.as_ptr().cast(),
            frame.len(),
            SEND_NOWAIT,
        )
    };
    match usize::try_from(sent) {
        Ok(n) if n == frame.len() => Ok(()),
        Ok(n) if n > 0 => (&*stream).write_all(&frame[n..]),
        Ok(_) => Err(io::ErrorKind::WouldBlock.into()),
        Err(_) => Err(io::Error::last_os_error()),
    }
}

impl Read for FlezhaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => stream.read(buf),
            FlezhaStream::Quic(stream) => stream.read(buf),
            FlezhaStream::Local(_) => Ok(0),
        }
    }
}

impl Write for FlezhaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => stream.write(buf),
            FlezhaStream::Quic(stream) => stream.write(buf),
            FlezhaStream::Local(sink) => sink(buf).map(|()| buf.len()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => stream.flush(),
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
            FlezhaStream::Unix(stream) => (&*stream).read(buf),
            FlezhaStream::Quic(stream) => (&*stream).read(buf),
            FlezhaStream::Local(_) => Ok(0),
        }
    }
}

impl Write for &FlezhaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => (&*stream).write(buf),
            FlezhaStream::Quic(stream) => (&*stream).write(buf),
            FlezhaStream::Local(sink) => sink(buf).map(|()| buf.len()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => (&*stream).flush(),
            FlezhaStream::Quic(stream) => (&*stream).flush(),
            FlezhaStream::Local(_) => Ok(()),
        }
    }
}
