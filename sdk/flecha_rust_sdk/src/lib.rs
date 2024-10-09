mod computation;
mod engine;
mod service;
mod command;
mod utility;

use engine::ComputationEngine;

pub use computation::{
    ComputationType, 
    TextComputation, 
    BinaryComputation, 
    ComputationResult,
    ComputationInput
};

pub struct Flecha {
    _id: String,
    engine: ComputationEngine,
}

impl Flecha {
    fn new(id: String) -> Flecha {
        Flecha {
            _id: id.clone(),
            engine: ComputationEngine::new(id.clone()),
        }
    }
    
    pub fn register_factory(&mut self, name: String, factory: fn() -> ComputationType) {
        self.engine.register_factory(name, factory);
    }

    pub fn run(&mut self, computation_id: String, input: ComputationInput) {
        self.engine.run(computation_id, input);
    }
}

pub fn new(id: String) -> Flecha {
    Flecha::new(id)
}