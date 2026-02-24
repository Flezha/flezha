mod atoms;
mod frame_buffer;
mod resources;
mod server;
mod stream;
mod uds_driver;

fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    let _ = rustler::resource!(resources::TransportServer, env);
    let _ = rustler::resource!(resources::StreamHandle, env);
    true
}

rustler::init!(
    "Elixir.Flezha.Transport.Native",
    load = load
);
