use std::collections::HashMap;

pub enum ComputationResult {
    NoReply,
    Reply(String),
}

pub struct Computation {
    pub id: String,
    pub template: Box<dyn ComputationTemplate>,
}

pub trait ComputationTemplate: Send + Sync {
    fn compute(&self, input: String) -> ComputationResult;
    fn get_schema(&self) -> HashMap<String, String>;
}