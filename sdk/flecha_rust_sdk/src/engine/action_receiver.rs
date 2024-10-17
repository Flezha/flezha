use std::thread;
use std::sync::{mpsc, Arc, Mutex};

use crate::command::action;

use super::{ComputationEngine, EngineAction};


pub fn start_action_receiver(
    engine: Arc<Mutex<ComputationEngine>>,
    rx: Arc<Mutex<mpsc::Receiver<EngineAction>>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            let rx = rx.lock().unwrap();
            match rx.recv() {
                Ok(received) => {
                    match received {
                        EngineAction::ControlCommand(payload_id, payload) => {
                            let payload_message: String = String::from_utf8_lossy(&payload).to_string();
                            action::handle_action(&engine, &payload_id, &payload_message);
                        }
                        _ => panic!("Unexpected message received on worker channel"),
                    }
                }
                Err(_) => {
                    eprint!("Action receiver channel closed");
                    
                    // TODO: we should probably break here and try to reconnect.
                    return;
                }
            }
        }
    })
}
