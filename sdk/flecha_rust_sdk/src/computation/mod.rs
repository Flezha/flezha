use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::{from_value, to_value, Value};

pub enum ComputationResult {
    NoReply,
    Reply(String),
}

pub enum ComputationInput {
    StringInput(String),
    U32Input(u32)
}

pub trait Blueprint: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync  {
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

pub trait BlueprintTrait: Send + Sync  {
    fn run(&self, input: String) -> ComputationResult;
    fn update_blueprint(&mut self, map: &HashMap<String, Value>);
    fn serialize_blueprint(&self) -> Vec<u8>;
    fn deserialize_blueprint(&self, data: &[u8]) -> Box<dyn BlueprintTrait>;
}

pub struct Computation {
    pub id: String,
    pub blueprint: Box<dyn BlueprintTrait>,
}

pub struct BlueprintManager<T: Blueprint + 'static> {
    pub blueprint: T,
}

impl<T: Blueprint> BlueprintTrait for BlueprintManager<T> {
    fn run(&self, input: String) -> ComputationResult {
        self.blueprint.run(input)
    }

    fn update_blueprint(&mut self, map: &HashMap<String, Value>) {
        self.blueprint.update_from_map(map)
    }

    fn serialize_blueprint(&self) -> Vec<u8> {
        Blueprint::serialize(&self.blueprint)
    }

    fn deserialize_blueprint(&self, data: &[u8]) -> Box<dyn BlueprintTrait> {
        let deserialized_blueprint = <T as Blueprint>::deserialize(data);
        Box::new(BlueprintManager { blueprint: deserialized_blueprint })
    }
}