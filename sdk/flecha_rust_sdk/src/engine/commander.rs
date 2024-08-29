use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::io::{self, BufRead};

use super::ThreadItem;


pub fn start_command_thread(rx: Arc<Mutex<mpsc::Sender<ThreadItem>>>) -> thread::JoinHandle<()> {
    // Create new thread to listen for commands.
    thread::spawn(move || {
        // Create a buffered reader for stdin.
        let stdin = io::stdin();
        let reader = stdin.lock();

        // Read lines from stdin in a blocking way.
        for line in reader.lines() {
            match line {
                Ok(command_data) => {
                    rx.lock().unwrap().send(ThreadItem::Command(command_data)).unwrap();
                }
                Err(e) => {
                    eprintln!("Failed to read line: {}", e);
                }
            }
        } 
    })
}