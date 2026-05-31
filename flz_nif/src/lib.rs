mod atoms;
mod driver_msg;
mod frame_buffer;
mod quic_driver;
mod resources;
mod server;
mod stream;
mod uds_driver;

#[allow(non_local_definitions)]
fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    let _ = rustler::resource!(resources::TransportServer, env);
    let _ = rustler::resource!(resources::StreamHandle, env);
    true
}

rustler::init!("Elixir.Flezha.Transport.Native", load = load);
