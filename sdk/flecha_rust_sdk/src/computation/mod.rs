use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::{from_value, to_value, Value};

pub enum ComputationResult {
    NoReply,
    Reply(String),
}

pub enum ComputationDataType {
    String
}

pub trait Template: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync  {
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

pub trait TemplateTrait: Send + Sync  {
    fn run(&self, input: String) -> ComputationResult;
    fn update_template(&mut self, map: &HashMap<String, Value>);
    fn serialize_template(&self) -> Vec<u8>;
    fn deserialize_template(&self, data: &[u8]) -> Box<dyn TemplateTrait>;
}

pub struct Computation {
    pub id: String,
    pub template: Box<dyn TemplateTrait>,
}

pub struct TemplateManager<T: Template + 'static> {
    pub template: T,
}

impl<T: Template> TemplateTrait for TemplateManager<T> {
    fn run(&self, input: String) -> ComputationResult {
        self.template.run(input)
    }

    fn update_template(&mut self, map: &HashMap<String, Value>) {
        self.template.update_from_map(map)
    }

    fn serialize_template(&self) -> Vec<u8> {
        Template::serialize(&self.template)
    }

    fn deserialize_template(&self, data: &[u8]) -> Box<dyn TemplateTrait> {
        let deserialized_template = <T as Template>::deserialize(data);
        Box::new(TemplateManager { template: deserialized_template })
    }
}