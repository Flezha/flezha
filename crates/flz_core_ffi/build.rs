//! Build script for the ABI crate. A no-op unless a language-veneer feature
//! is on: each one needs link flags the plain `cdylib` does not supply.

fn main() {
    // The N-API addon needs linker arguments the default `cdylib` link does
    // not supply: undefined-symbol dynamic lookup on macOS, and the import
    // library on Windows. `napi_build::setup()` emits them. Gated on the
    // feature so the `cdylib` / `staticlib` / `rlib` the other SDKs consume
    // link exactly as they did before.
    #[cfg(feature = "napi")]
    napi_build::setup();

    // magnus needs the Ruby version cfgs and the link flags rb-sys derives
    // from the `ruby` on PATH. Gated so a build without the feature never
    // looks for an interpreter.
    #[cfg(feature = "ruby")]
    rb_sys_env::activate().expect("rb-sys could not read the Ruby on PATH");

    // pyo3 derives `Py_GIL_DISABLED`, `Py_3_13` and the rest from the
    // interpreter it is building against, and the veneer reads the first of
    // them to report its wheel tier. Without this the cfg is unknown to
    // rustc and the build warns on its own answer.
    #[cfg(feature = "python")]
    pyo3_build_config::use_pyo3_cfgs();
}
