use std::sync::{Arc, Mutex, RwLock};
use std::{collections::HashMap, os::unix::net::UnixStream};
use std::io::Write;
use serde::{Serialize, Deserialize};
use serde_json::{from_value, to_value, Value};

use crate::service::payloader;

pub enum ComputationResult {
    None,
    ThreadHandle(ComputationOutput),
    Forward(ComputationOutput),
    Return(ComputationOutput),
}

pub enum ComputationOutput {
    Text(String),
    Binary(Vec<u8>),
    ThreadHandle(std::thread::JoinHandle<()>),
}

pub enum ComputationType {
    TextComputation(Box<dyn TextComputationTrait>),
    BinaryComputation(Box<dyn BinaryComputationTrait>),
}

pub enum ComputationInput<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
}

pub trait TextComputation: ComputationSerializer {
    fn run(&mut self, input: String) -> ComputationResult;
}

pub trait TextComputationTrait: ComputationTrait  {
    fn run(&mut self, input: String) -> ComputationResult;
}

impl<T: TextComputation + 'static> TextComputationTrait for T {
    fn run(&mut self, input: String) -> ComputationResult {
        self.run(input)
    }
}

pub trait BinaryComputation: ComputationSerializer  {
    fn run(&mut self, input: Vec<u8>) -> ComputationResult;
}

pub trait BinaryComputationTrait: ComputationTrait  {
    fn run(&mut self, input: Vec<u8>) -> ComputationResult;
}

impl<T: BinaryComputation + 'static> BinaryComputationTrait for T {
    fn run(&mut self, input: Vec<u8>) -> ComputationResult {
        self.run(input)
    }
}

impl<T> ComputationSerializer for T 
where
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
{}

pub trait ComputationTrait: Send + Sync  {
    fn update_computation(&mut self, map: &HashMap<String, Value>);
    fn serialize_computation(&self) -> Vec<u8>;
    fn deserialize_computation(&self, data: &[u8]) -> Box<dyn ComputationTrait>;
}

impl<T: ComputationSerializer + 'static> ComputationTrait for T {
    fn update_computation(&mut self, map: &HashMap<String, Value>) {
        self.update_from_map(map)
    }

    fn serialize_computation(&self) -> Vec<u8> {
        ComputationSerializer::serialize(self)
    }

    fn deserialize_computation(&self, data: &[u8]) -> Box<dyn ComputationTrait> {
        let deserialized_computation = <T as ComputationSerializer>::deserialize(data);
        Box::new(deserialized_computation)
    }
}

pub trait ComputationSerializer: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync {
    fn update_from_map(&mut self, map: &HashMap<String, Value>) {
        let mut value = to_value(self.clone()).expect("Failed to convert to serde_json::Value");

        if let Value::Object(ref mut obj) = value {
            for (key, val) in map {
                obj.insert(key.clone(), val.clone());
            }
        }

        *self = from_value(value).expect("Failed to convert from serde_json::Value");
    }

    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize")
    }

    fn deserialize(data: &[u8]) -> Self
    where
        Self: Sized,
    {
        bincode::deserialize(data).expect("Failed to deserialize")
    }
}

// TODO: remove duplication
pub fn async_exec(
    computation_id: String, 
    computations: &Arc<RwLock<HashMap<String, Arc<Mutex<(ComputationType, HashMap<String, String>)>>>>>,
    stream: &mut UnixStream,
    payload: Vec<u8>,
) {
    let computations_read = computations.read().unwrap();
                
    if let Some(c) = computations_read.get(&computation_id) {
        let mut comp = c.lock().unwrap();

        // TODO: reduce duplication.
        // TODO from here we should spawn new thread if computationMetaData is having task type.
        match &mut *comp {
            (ComputationType::TextComputation(comp), _) => {
                let payload_message: String = String::from_utf8_lossy(&payload).to_string();
                let result = comp.run(payload_message);

                match result {
                    ComputationResult::None | ComputationResult::Return(_) | ComputationResult::ThreadHandle(_) => {}
                    ComputationResult::Forward(res) => {
                        send_result(stream, &computation_id, &res);
                    }
                }
            }
            (ComputationType::BinaryComputation(comp), _) => {
                let result = comp.run(payload);

                match result {
                    ComputationResult::None | ComputationResult::Return(_) | ComputationResult::ThreadHandle(_) => {}
                    ComputationResult::Forward(res) => {
                        send_result(stream, &computation_id, &res);
                    }
                }
            }
        };              
    } else {
        eprintln!("Computation with given ID not found {}", computation_id);
    }
}

pub fn sync_exec<'a>(
    computation_id: &String, 
    computations: &Arc<RwLock<HashMap<String, Arc<Mutex<(ComputationType, HashMap<String, String>)>>>>>,
    stream: &mut UnixStream,
    input: &ComputationInput,
) -> Result<ComputationOutput, String> {
    let computations_read = computations.read().unwrap();
                
    if let Some(c) = computations_read.get(computation_id) {
        let mut comp = c.lock().unwrap();


        let computation_result = match &mut *comp {
            (ComputationType::TextComputation(comp), _) => {
                match input {
                    ComputationInput::Text(text) => comp.run(text.to_string()),
                    _ => panic!("Invalid input type"),
                    
                }
            }

            (ComputationType::BinaryComputation(comp), _) => {
                match input {
                    ComputationInput::Binary(binary) => comp.run(binary.to_vec()),
                    _ => panic!("Invalid input type")
                }
            }
        };

        match computation_result {
            ComputationResult::None => Err("No result returned from the computation".to_string()),
            ComputationResult::ThreadHandle(join_handle) => Ok(join_handle),
            ComputationResult::Return(res) => Ok(res),
            ComputationResult::Forward(res) => {
                send_result(stream, &computation_id, &res);
                Ok(res)
            }
        }
    } else {
        Err(format!("Computation with given ID not found {}", computation_id))
    }
}

fn send_result(stream: &mut UnixStream, computation_id: &String, result: &ComputationOutput) {
    let payload = match result {
        ComputationOutput::Text(text) => {
            payloader::build_from_str(&computation_id, text)
        }
        ComputationOutput::Binary(binary) => {
            payloader::build_from_binary(&computation_id, binary)
        },
        ComputationOutput::ThreadHandle(_) => {
            eprintln!("Thread handle is not supported for sync execution");
            return;
        }
    };

    if let Err(err) = stream.write_all(&payload) {
        eprintln!("Error writing to stream: {}", err);
    }
}