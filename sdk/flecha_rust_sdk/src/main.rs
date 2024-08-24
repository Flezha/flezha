mod computation;
mod engine;

use engine::Engine;

fn main() {
    let mut engine = Engine::new();
    engine.register_computation("do_nothing".to_string(), Box::new(DoNothingComputation));
}

struct DoNothingComputation;

impl computation::Computation for DoNothingComputation {
    fn compute(&self, input: String) {
        println!("Do nothing with input: {}", input);
    }
}