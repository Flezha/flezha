use std::sync::{Arc, Mutex, RwLock};
use std::{collections::HashMap, os::unix::net::UnixStream};
use std::io::Write;
use serde::{Serialize, Deserialize};
use serde_json::{from_value, to_value, Value};

use crate::service::payloader;

pub enum ComputationInput {
    Text(String),
    Binary(Vec<u8>),
}

pub enum ComputationOutput {
    None,
    Forward(ComputationResult),
    Return(ComputationResult),
}

pub enum ComputationResult {
    Text(String),
    Binary(Vec<u8>),
}

pub trait ComputationTrait: Send + Sync {
    fn run(&mut self, input: &ComputationInput) -> ComputationOutput;
    fn update_computation(&mut self, map: &HashMap<String, Value>);
    fn serialize_computation(&self) -> Vec<u8>;
    fn deserialize_computation(&self, data: &[u8]) -> Box<dyn ComputationTrait>;
}

impl<T> ComputationSerializer for T 
where
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
{}

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

pub fn exec(
    computation_id: &String, 
    computations: &Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn ComputationTrait>>>>>>,
    stream: &mut UnixStream,
    input: &ComputationInput,
) -> Option<ComputationResult> {
    let computations_read = computations.read().unwrap();
                
    if let Some(c) = computations_read.get(computation_id) {
        let mut computation = c.lock().unwrap();
        let computation_result= computation.run(input);

        handle_computation_result(computation_result, stream, computation_id)
    } else {
        panic!("Computation with given ID not found {}", computation_id);
    }
}

fn handle_computation_result(computation_result: ComputationOutput, stream: &mut UnixStream, computation_id: &String) -> Option<ComputationResult> {
    match computation_result {
        ComputationOutput::None => None,
        ComputationOutput::Return(res) => Some(res),
        ComputationOutput::Forward(res) => {
            send_result(stream, &computation_id, &res);
            Some(res)
        }
    }
}

fn send_result(stream: &mut UnixStream, computation_id: &String, result: &ComputationResult) {
    let payload = match result {
        ComputationResult::Text(text) => {
            payloader::build_from_str(&computation_id, text)
        }
        ComputationResult::Binary(binary) => {
            payloader::build_from_binary(&computation_id, binary)
        }
    };

    if let Err(err) = stream.write_all(&payload) {
        eprintln!("Error writing to stream: {}", err);
    }
}