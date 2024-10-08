use std::io::Write;
use std::os::unix::net::UnixStream;
use std::{collections::HashMap, thread};
use std::sync::{mpsc, Arc, Mutex, RwLock};

use crate::computation::ComputationType;
use crate::service::payloader;
use crate::command::action;

use super::ThreadItem;

pub struct EngineWorker {
    engine_id: String,
    computation_factory: HashMap<String, fn() -> ComputationType>,
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>>,
    thread_map: Arc<Mutex<HashMap<u8, thread::JoinHandle<()>>>>,
    control_stream: Arc<UnixStream>,
}

pub fn start_worker_thread(
    engine_id: String,
    rx: Arc<Mutex<mpsc::Receiver<ThreadItem>>>,
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>>,
    control_stream: Arc<UnixStream>
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut engine_worker = EngineWorker {
            engine_id: engine_id,
            computation_factory: HashMap::new(),
            computations: computations,
            thread_map: Arc::new(Mutex::new(HashMap::new())),
            control_stream: control_stream,
        };

        loop {
            let rx = rx.lock().unwrap();
            match rx.recv() {
                Ok(received) => {
                    match received {
                        ThreadItem::ControlCommand(payload_id, payload) => {
                            let payload_message: String = String::from_utf8_lossy(&payload).to_string();
                            action::handle_action(&mut engine_worker, &payload_id,&payload_message);
                        }
                        ThreadItem::RegisterFactory((factory_id, factory)) => {
                            engine_worker.register_factory(factory_id, factory, engine_worker.control_stream.clone());
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

    pub fn get_computations(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>> {
        Arc::clone(&self.computations)
    }

    pub fn get_factory(&self, factory_id: String) -> Option<&fn() -> ComputationType> {
        self.computation_factory.get(&factory_id)
    }

    pub fn get_thread_map(&self) -> Arc<Mutex<HashMap<u8, thread::JoinHandle<()>>>> {
        Arc::clone(&self.thread_map)
    }

    pub fn get_control_stream(&self) -> Arc<UnixStream> {
        Arc::clone(&self.control_stream)
    }

    pub fn register_factory(
        &mut self,
        factory_id: String,
        factory: fn() -> ComputationType,
        control_stream: Arc<UnixStream>
    ) {
        let mut core_data = HashMap::new();
        
        core_data.insert("factory_id".to_string(), factory_id.clone());

        let json = serde_json::to_string(&core_data).unwrap();
        let payload = payloader::build_payload(&"register_factory".to_string(), &json);

        control_stream.try_clone().unwrap().write_all(&payload).unwrap();

        self.computation_factory.insert(factory_id.clone(), factory);
    }

    pub fn register_computation(&self, id: String, computation: ComputationType) {
        let computation = Arc::new(Mutex::new(computation));
        self.computations.write().unwrap().insert(id, computation);
    }
}

