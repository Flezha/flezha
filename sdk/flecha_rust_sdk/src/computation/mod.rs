use std::collections::HashMap;

pub trait Computation {
    fn compute(&self, input: String);
    fn get_schema(&self) -> HashMap<String, String>;
}