mod computation;
mod engine;
mod service;
mod command;
mod utility;

use std::{collections::HashMap, os::unix::net::UnixStream, sync::{Arc, Mutex, RwLock}};

use engine::ComputationEngine;

pub use computation::{
    ComputationType, 
    TextComputation, 
    BinaryComputation, 
    ComputationResult,
    ComputationInput,
    ComputationOutput
};

pub struct Flecha {
    _id: String,
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>>,
    shared_io_streams: Arc<RwLock<HashMap<u8, UnixStream>>>,
    engine: Arc<Mutex<ComputationEngine>>,
}

impl Flecha {
    fn new(id: String) -> Flecha {
        let computations =  Arc::new(RwLock::new(HashMap::new()));
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        Flecha {
            _id: id.clone(),
            computations: Arc::clone(&computations),
            shared_io_streams: Arc::clone(&shared_io_streams),
            engine: ComputationEngine::new(
                id.clone(),
                computations,
                shared_io_streams,
            ),
        }
    }
    
    pub fn register_factory(&mut self, name: String, factory: fn() -> ComputationType) {
        self.engine.lock().unwrap().register_factory(name, factory);
    }

    pub fn run(&mut self, computation_id: String, input: ComputationInput) -> Result<ComputationOutput, String> {
        // Take random io_stream.
        let mut io_stream = self.shared_io_streams.write().unwrap().values().next().unwrap().try_clone().unwrap();

        computation::sync_exec(&computation_id, &self.computations, &mut io_stream, &input)
    }
}

pub fn new(id: String) -> Flecha {
    Flecha::new(id)
}