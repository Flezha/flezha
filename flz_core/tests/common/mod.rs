use std::path::PathBuf;

/// A socket directory of this process's own, with the runner's environment
/// pointed at it: the explicit rung of the socket ladder wins outright, so
/// nothing a test does can reach a real controller's directory.
pub fn isolated_socket_dir(tag: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!("flezha-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("socket dir");

    std::env::set_var("FLEZHA_TRANSPORT", "uds");
    std::env::set_var("FLEZHA_SOCKET_FILES_PATH", &dir);
    std::env::set_var("FLEZHA_RUNNER_ID_PATH", dir.join("runner_id"));
    dir
}
