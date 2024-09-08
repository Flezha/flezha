use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::os::unix::net::UnixStream;

use crate::service::payloader;

use super::ThreadItem;


pub fn start_command_thread(
    tx: Arc<Mutex<mpsc::Sender<ThreadItem>>>
) -> (thread::JoinHandle<()>, Arc<UnixStream>) {
    let socket_path = "app_data/sockets/command_default.sock"; // TODO: the name should be dynamic
    let stream_boxed = Arc::new(UnixStream::connect(socket_path).expect("Failed to connect to the socket"));
    let stream_boxed_clone = Arc::clone(&stream_boxed);

    let thread = thread::spawn(move || {
        loop {
            match payloader::extract_payload(&*stream_boxed_clone) {
                Ok((payload_id, message)) => {
                    tx.lock().unwrap().send(ThreadItem::Command(payload_id, message)).unwrap();
                }
                Err(_e) => {
                    // TODO: log error
                    break;
                }
            }

            // Sleep for a while to avoid busy waiting
            thread::sleep(Duration::from_millis(10));
        }
    });

    (thread, stream_boxed)
}
