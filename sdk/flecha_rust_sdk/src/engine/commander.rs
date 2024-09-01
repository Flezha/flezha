use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::io::{self};

use crate::service::payloader;

use super::ThreadItem;


pub fn start_command_thread(rx: Arc<Mutex<mpsc::Sender<ThreadItem>>>) -> thread::JoinHandle<()> {
    // Create new thread to listen for commands.
    thread::spawn(move || {
        // Create a buffered reader for stdin.
        let stdin = io::stdin();
        let mut reader = stdin.lock();

        loop {
            match payloader::extract_payload(&mut reader) {
                Ok((payload_id, message)) => {
                    rx.lock().unwrap().send(ThreadItem::Command(payload_id, message)).unwrap();
                }
                Err(e) => {
                    eprintln!("Failed to extract payload: {}", e);
                    break;
                }
            }
        }
    })
}