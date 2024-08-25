use crate::computation::Computation;
use crate::service::std_out;

use serde_json;
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
        let schema_json = serde_json::to_string(&computation.get_schema()).unwrap();
        let mut core_data = HashMap::new();
        core_data.insert("template_name".to_string(), name.clone());
        core_data.insert("schema".to_string(), schema_json); 

        let json = serde_json::to_string(&core_data).unwrap();
        std_out::write_stdout(20, &json);

        self.computations.insert(name, computation);
    }
}