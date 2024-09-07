use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::os::unix::net::UnixStream;
use std::fs;

use crate::service::payloader;

use super::ThreadItem;


pub fn start_command_thread(
    rx: Arc<Mutex<mpsc::Sender<ThreadItem>>>
) -> (thread::JoinHandle<()>, Arc<Mutex<UnixStream>>) {
    let socket_path = "app_data/sockets/command_default.sock"; // TODO: the name should be dynamic

    let path = Path::new(&socket_path);
    if path.exists() {
        fs::remove_file(path).expect("Failed to remove existing socket file");
    }

    let listener = Arc::new(Mutex::new(UnixListener::bind(socket_path).expect("Failed to bind socket")));
    let listener_clone = Arc::clone(&listener);
    let (tx, rx_conn) = mpsc::channel();

    let thread = thread::spawn(move || {
        let listener = listener_clone.lock().unwrap();

        // Accept the first connection and send it to the main thread
        if let Ok((stream, _)) = listener.accept() {
            let stream = Arc::new(Mutex::new(stream));
            tx.send(Arc::clone(&stream)).expect("Failed to send the connection");

            loop {
                let stream = stream.lock().unwrap();
                match payloader::extract_payload(&*stream) {
                    Ok((payload_id, message)) => {
                        rx.lock().unwrap().send(ThreadItem::Command(payload_id, message)).unwrap();
                    }
                    Err(e) => {
                        eprintln!("Failed to extract payload: {}", e);
                    }
                }

                // Sleep for a while to avoid busy waiting
                thread::sleep(Duration::from_millis(10));
            }
        } else {
            eprintln!("Failed to accept the first connection");
        }
    });

    // Receive the first connection and return it
    let stream = rx_conn.recv().expect("Failed to receive the first client");

    (thread, stream)
}
