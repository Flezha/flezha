//! Build script for the ABI crate. A no-op unless the `napi` feature is on.

fn main() {
    // The N-API addon needs linker arguments the default `cdylib` link does
    // not supply: undefined-symbol dynamic lookup on macOS, and the import
    // library on Windows. `napi_build::setup()` emits them. Gated on the
    // feature so the `cdylib` / `staticlib` / `rlib` the other SDKs consume
    // link exactly as they did before.
    #[cfg(feature = "napi")]
    napi_build::setup();
}
