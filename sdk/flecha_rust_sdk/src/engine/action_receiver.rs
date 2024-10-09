use std::thread;
use std::sync::{mpsc, Arc, Mutex};

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
                            engine.lock().unwrap().handle_command(&payload_id, &payload);
                        }
                        _ => panic!("Unexpected message received on worker channel"),
                    }
                }
                Err(_) => {
                    eprint!("Failed to receive message on worker channel");
                }
            }
        }
    })
}
