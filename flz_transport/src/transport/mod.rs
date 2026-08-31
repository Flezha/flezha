pub mod config;
pub mod liveness;
pub mod protocol;
pub mod quic;
pub mod send_gate;
pub mod tls;

use std::io::{self, Read, Write};
use std::net::Shutdown;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;

use quic::QuicBidiStream;

pub enum FlezhaStream {
    #[cfg(unix)]
    Unix(UnixStream),
    Quic(QuicBidiStream),
}

impl FlezhaStream {
    pub fn clone_handle(&self) -> io::Result<FlezhaStream> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => Ok(FlezhaStream::Unix(stream.try_clone()?)),
            FlezhaStream::Quic(stream) => Ok(FlezhaStream::Quic(stream.clone_handle())),
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
        }
    }

    #[cfg(unix)]
    pub fn as_raw_fd(&self) -> Option<i32> {
        match self {
            FlezhaStream::Unix(stream) => Some(stream.as_raw_fd()),
            FlezhaStream::Quic(_) => None,
        }
    }
}

impl Read for FlezhaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => stream.read(buf),
            FlezhaStream::Quic(stream) => stream.read(buf),
        }
    }
}

impl Write for FlezhaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => stream.write(buf),
            FlezhaStream::Quic(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => stream.flush(),
            FlezhaStream::Quic(stream) => stream.flush(),
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
        }
    }
}

impl Write for &FlezhaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => (&*stream).write(buf),
            FlezhaStream::Quic(stream) => (&*stream).write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            FlezhaStream::Unix(stream) => (&*stream).flush(),
            FlezhaStream::Quic(stream) => (&*stream).flush(),
        }
    }
}
