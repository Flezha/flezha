use std::{collections::HashMap, thread};
use std::sync::{mpsc, Arc, Mutex, RwLock};

use crate::computation::{Computation, ComputationTemplate};
use crate::service::std_out;
use crate::command::action;

use super::ThreadItem;

pub struct EngineWorker {
    templates_factory: HashMap<String, fn() -> Box<dyn ComputationTemplate>>,
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<Computation>>>>>,
    thread_map: Arc<Mutex<HashMap<String, thread::JoinHandle<()>>>>
}

pub fn start_worker_thread(rx: Arc<Mutex<mpsc::Receiver<ThreadItem>>>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut engine_worker = EngineWorker {
            templates_factory: HashMap::new(),
            computations: Arc::new(RwLock::new(HashMap::new())),
            thread_map: Arc::new(Mutex::new(HashMap::new()))
        };

        loop {
            let rx = rx.lock().unwrap();
            match rx.recv() {
                Ok(received) => {
                    match received {
                        ThreadItem::Command(_command) => {
                            action::handle_action(&mut engine_worker, &_command);
                        }
                        ThreadItem::ComputationTemplate((template_id, factory)) => {
                            engine_worker.register_template(template_id, factory)
                        }
                    }
                }
                Err(_) => {
                    eprint!("Failed to receive message on worker channel");
                }
            }
        }
    })
}

impl EngineWorker {
    pub fn get_computations(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<Computation>>>>> {
        Arc::clone(&self.computations)
    }

    pub fn get_template_factory(&self, template_id: String) -> Option<&fn() -> Box<dyn ComputationTemplate>> {
        self.templates_factory.get(&template_id)
    }

    pub fn get_thread_map(&self) -> Arc<Mutex<HashMap<String, thread::JoinHandle<()>>>> {
        Arc::clone(&self.thread_map)
    }

    pub fn register_template(&mut self, template_id: String, factory: fn() -> Box<dyn ComputationTemplate>) {
        let temp_template = factory();
        let schema_json = serde_json::to_string(&temp_template.get_schema()).unwrap();
        let mut core_data = HashMap::new();
        core_data.insert("template_name".to_string(), template_id.clone());
        core_data.insert("schema".to_string(), schema_json); 

        let json = serde_json::to_string(&core_data).unwrap();
        std_out::write_w_code(39, &json);

        self.templates_factory.insert(template_id.clone(), factory);
    }

    pub fn register_computation(&self, computation: Computation) {
        let id = computation.id.clone();
        let computation = Arc::new(Mutex::new(computation));
        self.computations.write().unwrap().insert(id, computation);
    }
}

