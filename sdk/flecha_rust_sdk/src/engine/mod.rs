pub mod worker;
mod control_unit;

use crate::computation::ComputationType;

use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self};
use std::io::{self, Read};
use std::time::Duration;

pub struct ComputationEngine {
    _engine_id: String,
    _worker_thread: Option<thread::JoinHandle<()>>,
    _control_thread: Option<thread::JoinHandle<()>>,
    _rx: Arc<Mutex<mpsc::Receiver<ThreadItem>>>,
    tx: Arc<Mutex<mpsc::Sender<ThreadItem>>>,
    _stdio_thread: thread::JoinHandle<()>,
}

pub enum ThreadItem {
    Command(String, String),
    Computation((String, fn() -> ComputationType)),
    EngineIdConfirmation
}

impl ComputationEngine {
    pub fn new(engine_id: String) -> ComputationEngine {
        let (tx, rx): (mpsc::Sender<ThreadItem>, mpsc::Receiver<ThreadItem>) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        let rx = Arc::new(Mutex::new(rx));
        let (control_thread, control_stream) = control_unit::connect_control_socket(
            engine_id.clone(),
            Arc::clone(&tx)
        );
        let worker_thread = worker::start_worker_thread(
            engine_id.clone(),
            Arc::clone(&rx),
             control_stream
        );
        let stdio_thread = start_stdio_thread();

        // Make sure the engine id is passed to the Flecha core.
        match rx.lock().unwrap().recv().expect("Failed to engine id sent confirmation") {
            ThreadItem::EngineIdConfirmation => {},
            _ => panic!("Wrong order of messages"),
        }

        ComputationEngine {
            _engine_id: engine_id,
            _worker_thread: Some(worker_thread),
            _control_thread: Some(control_thread),
            _rx: rx,
            tx,
            _stdio_thread: stdio_thread,
        }
    }

    pub fn register_factory(&mut self, computation_id: String, factory: fn() -> ComputationType) {
        self.tx.lock().unwrap().send(
            ThreadItem::Computation((computation_id, factory))
        ).unwrap();
    }
}

// Start a thread to monitor stdout and exit when stdin is closed.
fn start_stdio_thread() -> thread::JoinHandle<()> {
       // Start a thread to monitor stdout
       thread::spawn(|| {
        let mut buf = [0u8; 1]; // Buffer to read from stdout

        loop {
            let result = io::stdin().read(&mut buf); // Attempt to read from stdin
            match result {
                Ok(0) => {
                    // TODO: Log that stdin has been closed and exit.
                    std::process::exit(0);
                }
                Ok(_) => {
                    // Data available; continue monitoring
                    println!("Data available: {:?}", buf);
                }
                Err(_e) => {
                    // TODO: Log the error.
                }
            }

            thread::sleep(Duration::from_secs(1));
        }
    })
}