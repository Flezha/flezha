use std::sync::{Arc, Mutex, RwLock};
use std::{collections::HashMap, os::unix::net::UnixStream};
use std::io::Write;
use serde::{Serialize, Deserialize};
use serde_json::{from_value, to_value, Value};

use crate::service::payloader;

pub enum ComputationResult {
    None,
    Forward(ComputationOutput),
    Return(ComputationOutput),
}

pub enum ComputationOutput {
    Text(String),
    Binary(Vec<u8>),
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
    computations: &Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>>,
    stream: &mut UnixStream,
    payload: Vec<u8>,
) {
    let computations_read = computations.read().unwrap();
                
    if let Some(c) = computations_read.get(&computation_id) {
        let mut comp = c.lock().unwrap();

        // TODO: reduce duplication.
        match &mut *comp {
            ComputationType::TextComputation(comp) => {
                let payload_message: String = String::from_utf8_lossy(&payload).to_string();
                let result = comp.run(payload_message);

                match result {
                    ComputationResult::None | ComputationResult::Return(_) => {}
                    ComputationResult::Forward(res) => {
                        send_result(stream, &computation_id, &res);
                    }
                }
            }
            ComputationType::BinaryComputation(comp) => {
                let result = comp.run(payload);

                match result {
                    ComputationResult::None | ComputationResult::Return(_) => {}
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
    computations: &Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>>,
    stream: &mut UnixStream,
    input: &ComputationInput,
) -> Result<ComputationOutput, String> {
    let computations_read = computations.read().unwrap();
                
    if let Some(c) = computations_read.get(computation_id) {
        let mut comp = c.lock().unwrap();

        match input {
            ComputationInput::Text(text) => {
                match &mut *comp {
                    ComputationType::TextComputation(comp) => {
                        let result = comp.run(text.to_string());

                        match result {
                            ComputationResult::None => Err("No result returned from the computation".to_string()),
                            ComputationResult::Return(res) => Ok(res),
                            ComputationResult::Forward(res) => {
                                send_result(stream, &computation_id, &res);
                                Ok(res)
                            }
                        }
                    }
                    ComputationType::BinaryComputation(_) => {
                        Err(format!("Computation with given ID is not a text computation {}", computation_id))
                    }
                }
            }
            ComputationInput::Binary(binary) => {
                match &mut *comp {
                    ComputationType::BinaryComputation(comp) => {
                        let result = comp.run(binary.to_vec());

                        match result {
                            ComputationResult::None => Err("No result returned from the computation".to_string()),
                            ComputationResult::Return(res) => Ok(res),
                            ComputationResult::Forward(res) => {
                                send_result(stream, &computation_id, &res);
                                Ok(res)
                            }
                        }
                    }
                    ComputationType::TextComputation(_) => {
                        Err(format!("Computation with given ID is not a binary computation {}", computation_id))
                    }
                }
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
        }
    };

    if let Err(err) = stream.write_all(&payload) {
        eprintln!("Error writing to stream: {}", err);
    }
}