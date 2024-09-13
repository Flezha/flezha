use std::env;

pub fn stream_socket_path(engine_id: &str) -> String {
    let base_path: String = sockets_base_path();

    format!("{}stream_{}.sock", base_path, engine_id)
}

pub fn control_socket_path() -> String {
    let base_path: String = sockets_base_path();

    format!("{}control_engine.sock", base_path)
}

fn sockets_base_path() -> String {
    let key = "socket_files_path";
    match env::var(key) {
        Ok(val) => val,
        Err(_) => "app_data/sockets/".to_string()
    }
}