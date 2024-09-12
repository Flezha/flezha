use std::io::Write;
use std::os::unix::net::UnixStream;
use std::{collections::HashMap, thread};
use std::sync::{mpsc, Arc, Mutex, RwLock};

use crate::computation::{Computation, TemplateTrait};
use crate::service::payloader;
use crate::command::action;

use super::ThreadItem;

pub struct EngineWorker {
    engine_id: String,
    templates_factory: HashMap<String, fn() -> Box<dyn TemplateTrait>>,
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<Computation>>>>>,
    thread_map: Arc<Mutex<HashMap<u8, thread::JoinHandle<()>>>>,
    control_stream: Arc<UnixStream>,
}

pub fn start_worker_thread(
    engine_id: String,
    rx: Arc<Mutex<mpsc::Receiver<ThreadItem>>>,
    control_stream: Arc<UnixStream>
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut engine_worker = EngineWorker {
            engine_id: engine_id,
            templates_factory: HashMap::new(),
            computations: Arc::new(RwLock::new(HashMap::new())),
            thread_map: Arc::new(Mutex::new(HashMap::new())),
            control_stream: control_stream,
        };

        loop {
            let rx = rx.lock().unwrap();
            match rx.recv() {
                Ok(received) => {
                    match received {
                        ThreadItem::Command(payload_id, command) => {
                            action::handle_action(&mut engine_worker, &payload_id,&command);
                        }
                        ThreadItem::ComputationTemplate((template_id, factory)) => {
                            engine_worker.register_template(template_id, factory, engine_worker.control_stream.clone());
                        }
                    }
                }
                Err(_) => {
                    // TODO: remove later
                    print!("FAIL");

                    eprint!("Failed to receive message on worker channel");
                }
            }
        }
    })
}

impl EngineWorker {
    pub fn get_engine_id(&self) -> String {
        self.engine_id.clone()
    }

    pub fn get_computations(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<Computation>>>>> {
        Arc::clone(&self.computations)
    }

    pub fn get_template_factory(&self, template_id: String) -> Option<&fn() -> Box<dyn TemplateTrait>> {
        self.templates_factory.get(&template_id)
    }

    pub fn get_thread_map(&self) -> Arc<Mutex<HashMap<u8, thread::JoinHandle<()>>>> {
        Arc::clone(&self.thread_map)
    }

    pub fn get_control_stream(&self) -> Arc<UnixStream> {
        Arc::clone(&self.control_stream)
    }

    pub fn register_template(
        &mut self,
        template_id: String,
        factory: fn() -> Box<dyn TemplateTrait>,
        control_stream: Arc<UnixStream>
    ) {
        let temp_template = factory();
        let schema_json = serde_json::to_string(&temp_template.get_schema()).unwrap();
        let mut core_data = HashMap::new();
        
        core_data.insert("template_id".to_string(), template_id.clone());
        core_data.insert("schema".to_string(), schema_json); 

        let json = serde_json::to_string(&core_data).unwrap();
        let payload = payloader::build_payload(&"39".to_string(), &json);

        control_stream.try_clone().unwrap().write_all(&payload).unwrap();

        self.templates_factory.insert(template_id.clone(), factory);
    }

    pub fn register_computation(&self, computation: Computation) {
        let id = computation.id.clone();
        let computation = Arc::new(Mutex::new(computation));
        self.computations.write().unwrap().insert(id, computation);
    }
}

