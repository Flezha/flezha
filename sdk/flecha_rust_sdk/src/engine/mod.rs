use crate::computation::Computation;

use std::collections::HashMap;
use std::thread;
use std::io::{self, BufRead};

pub struct Engine {
    computations: HashMap<String, Box<dyn Computation>>,
}

impl Engine {
    pub fn new() -> Engine {
        // Create new thread to listen for commands.
        let _engine_thread = thread::spawn(|| {
            // Create a buffered reader for stdin.
            let stdin = io::stdin();
            let reader = stdin.lock();
    
            // Read lines from stdin in a blocking way.
            for line in reader.lines() {
                match line {
                    Ok(_command_data) => {
                        
                    }
                    Err(e) => {
                        eprintln!("Failed to read line: {}", e);
                    }
                }
            } 
        });

        Engine {
            computations: HashMap::new(),
        }
    }

    pub fn register_computation(&mut self, name: String, computation: Box<dyn Computation>) {
        self.computations.insert(name, computation);
    }
}