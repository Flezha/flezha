//! Nothing of this crate's own: the gem's native extension IS `flz_core_ffi` built with one
//! feature, and this re-export is what puts its entry points in a `cdylib`'s
//! export table. A `cdylib` exports no `#[no_mangle]` symbol of a dependency
//! it never names. Measured, not assumed: without the glob the binary
//! links and exports nothing at all.

pub use ::veneer::*;
