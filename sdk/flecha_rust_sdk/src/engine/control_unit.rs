use std::io::Write;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::os::unix::net::UnixStream;

use crate::service::payloader;

use super::ThreadItem;

pub fn connect_stream_socket(
    engine_id: String,
    tx: Arc<Mutex<mpsc::Sender<ThreadItem>>>
) -> (thread::JoinHandle<()>, Arc<UnixStream>) {
    let socket_path = "app_data/sockets/control_default.sock"; // TODO: should be configurable
    let stream_boxed = Arc::new(UnixStream::connect(socket_path).expect("Failed to connect to the socket"));
    let stream_boxed_clone = Arc::clone(&stream_boxed);

    let thread = thread::spawn(move || {
        let payload = payloader::build_payload("0", &engine_id);
        stream_boxed_clone.try_clone().unwrap().write_all(&payload).unwrap();
        tx.lock().unwrap().send(ThreadItem::EngineIdConfirmation).unwrap();

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
