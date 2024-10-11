pub mod action_receiver;
mod control_unit;

use crate::computation::ComputationType;
use crate::service::payloader;

use std::io::Write;
use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::{self};
use std::io::{self, Read};
use std::time::Duration;

pub struct ComputationEngine {
    engine_id: String,
    computation_factory: HashMap<String, fn() -> ComputationType>,
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>>,
    control_stream: Arc<UnixStream>,
    io_streams: Arc<Mutex<HashMap<u8, UnixStream>>>,
}

pub enum EngineAction {
    ControlCommand(String, Vec<u8>),
    EngineIdConfirmation
}

impl ComputationEngine {
    pub fn new(engine_id: String) -> Arc<Mutex<ComputationEngine>> {
        let (tx, rx): (mpsc::Sender<EngineAction>, mpsc::Receiver<EngineAction>) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        let rx = Arc::new(Mutex::new(rx));
    
        let (_control_thread, control_stream) = control_unit::connect_control_socket(
            engine_id.clone(),
            Arc::clone(&tx)
        );
    
        let engine = ComputationEngine {
            engine_id,
            control_stream,
            computation_factory: HashMap::new(),
            computations: Arc::new(RwLock::new(HashMap::new())),
            io_streams: Arc::new(Mutex::new(HashMap::new())),
        };
    
        let engine_arc = Arc::new(Mutex::new(engine));
            
        let _action_receiver = action_receiver::start_action_receiver(
            Arc::clone(&engine_arc),
            Arc::clone(&rx)
        );
    
        let _stdio_thread = start_stdio_thread();
    
        // Make sure the engine id is passed to the Flecha core.
        match rx.lock().unwrap().recv().expect("Failed to engine id sent confirmation") {
            EngineAction::EngineIdConfirmation => {},
            _ => panic!("Wrong order of messages"),
        }
    
        engine_arc
    }

    pub fn register_factory(
        &mut self,
        factory_id: String,
        factory: fn() -> ComputationType) {
        let mut core_data = HashMap::new();
        
        core_data.insert("factory_id".to_string(), factory_id.clone());

        let json = serde_json::to_string(&core_data).unwrap();
        let payload = payloader::build_payload(&"register_factory".to_string(), &json);

        self.control_stream.try_clone().unwrap().write_all(&payload).unwrap();
        self.computation_factory.insert(factory_id.clone(), factory);
    }

    pub fn get_factory(&self, factory_id: String) -> Option<&fn() -> ComputationType> {
        self.computation_factory.get(&factory_id)
    }

    pub fn register_computation(&self, id: String, computation: ComputationType) {
        let computation = Arc::new(Mutex::new(computation));
        self.computations.write().unwrap().insert(id, computation);
    }

    pub fn get_control_stream(&self) -> Arc<UnixStream> {
        Arc::clone(&self.control_stream)
    }

    pub fn get_engine_id(&self) -> String {
        self.engine_id.clone()
    }

    pub fn get_computations(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>> {
        Arc::clone(&self.computations)
    }

    pub fn get_io_streams(&self) -> Arc<Mutex<HashMap<u8, UnixStream>>> {
        Arc::clone(&self.io_streams)
    }

    // TODO: Continue from here. We need to trigger the compute somehow differently becuase the engine needs to know about the stream channels(secondary)
    // pub fn run(&mut self, computation_id: String, input: ComputationInput) {
        // let computations_read = self.computations.read().unwrap();
                
        // if let Some(c) = computations_read.get(&computation_id) {
        //     let mut comp = c.lock().unwrap();

        // // TODO: reduce duplication. with the connect streams module code.
        // match &mut *comp {
        //     ComputationType::TextComputation(comp) => {
        //         match input {
        //             ComputationInput::TextInput(input_text) => {
        //                 let result = comp.run(input_text);

        //                 match result {
        //                     ComputationResult::NoReply => {}
        //                     ComputationResult::Reply(reply) => {
        //                         computation::send_result(&mut stream, &computation_id, &reply);
        //                     }
        //                 }
        //             }
        //             _ => panic!("Wrong input type for text computation"),
        //         }
        //     }
        //     ComputationType::BinaryComputation(comp) => {
        //         match input {
        //             ComputationInput::BinaryInput(input_binary) => {
        //                 let result = comp.run(input_binary);

        //                 match result {
        //                     ComputationResult::NoReply => {}
        //                     ComputationResult::Reply(reply) => {
        //                         computation::send_result(&mut stream, &computation_id, &reply);
        //                     }
        //                 }
        //             }
        //             _ => panic!("Wrong input type for binary computation"),
        //         }
        //     }
        // };              
        // } else {
        //     eprintln!("Computation with given ID not found {}", computation_id);
        // }
    // }
}

// Start a thread to monitor stdout and exit when stdin is closed. TODO: Maybe remove this?
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