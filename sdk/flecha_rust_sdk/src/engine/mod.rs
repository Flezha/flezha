pub mod worker;
mod commander;

use crate::computation::TemplateTrait;

use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self};

pub struct ComputationEngine {
    _worker_thread: Option<thread::JoinHandle<()>>,
    _command_thread: Option<thread::JoinHandle<()>>,
    _rx: Arc<Mutex<mpsc::Receiver<ThreadItem>>>,
    tx: Arc<Mutex<mpsc::Sender<ThreadItem>>>,
}

pub enum ThreadItem {
    Command(String, String),
    ComputationTemplate((String, fn() -> Box<dyn TemplateTrait>)),
}

impl ComputationEngine {
    pub fn new() -> ComputationEngine {
        let (tx, rx): (mpsc::Sender<ThreadItem>, mpsc::Receiver<ThreadItem>) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        let rx = Arc::new(Mutex::new(rx));
        let (command_thread, command_stream) = commander::start_command_thread(Arc::clone(&tx));
        let worker_thread = worker::start_worker_thread(Arc::clone(&rx), command_stream);

        ComputationEngine {
            _worker_thread: Some(worker_thread),
            _command_thread: Some(command_thread),
            _rx: rx,
            tx,
        }
    }

    pub fn register_template(&mut self, template_id: String, factory: fn() -> Box<dyn TemplateTrait>) {
        self.tx.lock().unwrap().send(
            ThreadItem::ComputationTemplate((template_id, factory))
        ).unwrap();
    }
}