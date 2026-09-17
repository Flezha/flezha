mod atoms;
mod driver;
mod driver_msg;
mod driver_stats;
mod frame_buffer;
mod instance_claim;
mod resources;
mod server;
mod socket_dir;
mod stream;

#[allow(non_local_definitions)]
fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    let _ = rustler::resource!(resources::TransportServer, env);
    let _ = rustler::resource!(resources::StreamHandle, env);
    true
}

rustler::init!("Elixir.Flezha.Transport.Native", load = load);
