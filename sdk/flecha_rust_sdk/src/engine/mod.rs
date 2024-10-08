pub mod worker;
mod control_unit;

use crate::computation::{self, ComputationInput, ComputationResult, ComputationType};

use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex, RwLock};
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
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>>,
}

pub enum ThreadItem {
    ControlCommand(String, Vec<u8>),
    RegisterFactory((String, fn() -> ComputationType)),
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
        let computations = Arc::new(RwLock::new(HashMap::new()));
        let worker_thread = worker::start_worker_thread(
            engine_id.clone(),
            Arc::clone(&rx),
            computations.clone(),   
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
            computations,
            _stdio_thread: stdio_thread,
        }
    }

    pub fn register_factory(&mut self, factory_id: String, factory: fn() -> ComputationType) {
        self.tx.lock().unwrap().send(
            ThreadItem::RegisterFactory((factory_id, factory))
        ).unwrap();
    }

    // TODO: Continue from here. We need to trigger the compute somehow differently becuase the engine needs to know about the stream channels(secondary)
    pub fn compute(&mut self, computation_id: String, input: ComputationInput) {
        let computations_read = self.computations.read().unwrap();
                
        if let Some(c) = computations_read.get(&computation_id) {
            let mut comp = c.lock().unwrap();

        // TODO: reduce duplication. with the connect streams module code.
        match &mut *comp {
            ComputationType::TextComputation(comp) => {
                match input {
                    ComputationInput::TextInput(input_text) => {
                        let result = comp.run(input_text);

                        match result {
                            ComputationResult::NoReply => {}
                            ComputationResult::Reply(reply) => {
                                computation::send_result(&mut stream, &computation_id, &reply);
                            }
                        }
                    }
                    _ => panic!("Wrong input type for text computation"),
                }
            }
            ComputationType::BinaryComputation(comp) => {
                match input {
                    ComputationInput::BinaryInput(input_binary) => {
                        let result = comp.run(input_binary);

                        match result {
                            ComputationResult::NoReply => {}
                            ComputationResult::Reply(reply) => {
                                computation::send_result(&mut stream, &computation_id, &reply);
                            }
                        }
                    }
                    _ => panic!("Wrong input type for binary computation"),
                }
            }
        };              
        } else {
            eprintln!("Computation with given ID not found {}", computation_id);
        }
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