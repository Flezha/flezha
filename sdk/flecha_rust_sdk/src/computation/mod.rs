use std::collections::HashMap;

pub struct Computation {
    pub id: String,
    pub template: Box<dyn ComputationTemplate>,
}

pub trait ComputationTemplate: Send + Sync {
    fn compute(&self, input: String);
    fn get_schema(&self) -> HashMap<String, String>;
}