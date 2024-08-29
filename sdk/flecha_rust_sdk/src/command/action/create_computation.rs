use super::CommandAction;

use serde::Deserialize;

use crate::service::std_out;
use crate::computation::Computation;
use crate::engine::worker::EngineWorker;

#[derive(Deserialize)]
pub struct CreateComputationCommand {
    computation_id: String,
    template_name: String,
}

impl CommandAction for CreateComputationCommand {
    fn handle(
        &self,
        engine_worker: &mut EngineWorker,
        _message: &String,
        reference_id: String,
    ) {
        let template_factory = engine_worker.get_template_factory(self.template_name.clone()).unwrap();

        let computation = Computation{
            id: self.computation_id.clone(),
            template: template_factory(),
        };
    
        engine_worker.register_computation(computation);
        
        std_out::write_stdout(21, &format!("COMMAND_REF:{}", reference_id));
    }
}