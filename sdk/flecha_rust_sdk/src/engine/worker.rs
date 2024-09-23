use std::io::Write;
use std::os::unix::net::UnixStream;
use std::{collections::HashMap, thread};
use std::sync::{mpsc, Arc, Mutex, RwLock};

use crate::computation::{Computation, BlueprintTrait};
use crate::service::payloader;
use crate::command::action;

use super::ThreadItem;

pub struct EngineWorker {
    engine_id: String,
    blueprints_factory: HashMap<String, fn() -> Box<dyn BlueprintTrait>>,
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
            blueprints_factory: HashMap::new(),
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
                        ThreadItem::ComputationBlueprint((blueprint_id, factory)) => {
                            engine_worker.register_blueprint(blueprint_id, factory, engine_worker.control_stream.clone());
                        },
                        _ => panic!("Unexpected message received on worker channel"),
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
    pub fn get_engine_id(&self) -> String {
        self.engine_id.clone()
    }

    pub fn get_computations(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<Computation>>>>> {
        Arc::clone(&self.computations)
    }

    pub fn get_blueprint_factory(&self, blueprint_id: String) -> Option<&fn() -> Box<dyn BlueprintTrait>> {
        self.blueprints_factory.get(&blueprint_id)
    }

    pub fn get_thread_map(&self) -> Arc<Mutex<HashMap<u8, thread::JoinHandle<()>>>> {
        Arc::clone(&self.thread_map)
    }

    pub fn get_control_stream(&self) -> Arc<UnixStream> {
        Arc::clone(&self.control_stream)
    }

    pub fn register_blueprint(
        &mut self,
        blueprint_id: String,
        factory: fn() -> Box<dyn BlueprintTrait>,
        control_stream: Arc<UnixStream>
    ) {
        let mut core_data = HashMap::new();
        
        core_data.insert("blueprint_id".to_string(), blueprint_id.clone());

        let json = serde_json::to_string(&core_data).unwrap();
        let payload = payloader::build_payload(&"39".to_string(), &json);

        control_stream.try_clone().unwrap().write_all(&payload).unwrap();

        self.blueprints_factory.insert(blueprint_id.clone(), factory);
    }

    pub fn register_computation(&self, computation: Computation) {
        let id = computation.id.clone();
        let computation = Arc::new(Mutex::new(computation));
        self.computations.write().unwrap().insert(id, computation);
    }
}

