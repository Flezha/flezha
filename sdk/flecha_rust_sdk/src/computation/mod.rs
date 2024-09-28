use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::{from_value, to_value, Value};

pub enum ComputationResult {
    NoReply,
    Reply(String),
}

pub trait Computation: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync  {
    fn run(&self, input: String) -> ComputationResult;

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

pub trait ComputationTrait: Send + Sync  {
    fn run(&self, input: String) -> ComputationResult;
    fn update_computation(&mut self, map: &HashMap<String, Value>);
    fn serialize_computation(&self) -> Vec<u8>;
    fn deserialize_computation(&self, data: &[u8]) -> Box<dyn ComputationTrait>;
}

// Implement ComputationTrait for all types that implement Computation.
impl<T: Computation + 'static> ComputationTrait for T {
    fn run(&self, input: String) -> ComputationResult {
        self.run(input)
    }

    fn update_computation(&mut self, map: &HashMap<String, Value>) {
        self.update_from_map(map)
    }

    fn serialize_computation(&self) -> Vec<u8> {
        // Use the fully qualified syntax to avoid ambiguity
        Computation::serialize(self)
    }

    fn deserialize_computation(&self, data: &[u8]) -> Box<dyn ComputationTrait> {
        let deserialized_computation = <T as Computation>::deserialize(data);
        Box::new(deserialized_computation)
    }
}
