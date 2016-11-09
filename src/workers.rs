use std::{time, thread};
use std::sync::mpsc;
use std::boxed::FnBox;
use fabric::FabricMsg;
use database::{NodeId, Token};
use rand::{thread_rng, Rng};
use resp::RespValue;

#[derive(Debug)]
pub enum WorkerMsg {
    Fabric(NodeId, FabricMsg),
    Tick(time::Instant),
    Command(Token, RespValue),
    DHTChange,
    Exit,
}

pub struct WorkerSender {
    cursor: usize,
    channels: Vec<mpsc::Sender<WorkerMsg>>,
}

pub struct WorkerManager {
    ticker_interval: time::Duration,
    ticker_thread: Option<thread::JoinHandle<()>>,
    ticker_chan: Option<mpsc::Sender<()>>,
    thread_count: usize,
    threads: Vec<thread::JoinHandle<()>>,
    channels: Vec<mpsc::Sender<WorkerMsg>>,
    node: NodeId,
}

impl WorkerManager {
    pub fn new(node: NodeId, thread_count: usize, ticker_interval: time::Duration) -> Self {
        assert!(thread_count > 0);
        WorkerManager {
            ticker_interval: ticker_interval,
            ticker_thread: None,
            ticker_chan: None,
            thread_count: thread_count,
            threads: Vec::new(),
            channels: Vec::new(),
            node: node,
        }
    }

    pub fn start<F>(&mut self, mut worker_fn_gen: F)
        where F: FnMut() -> Box<FnBox(mpsc::Receiver<WorkerMsg>) + Send>
    {
        assert!(self.channels.is_empty());
        for i in 0..self.thread_count {
            let worker_fn = worker_fn_gen();
            let (tx, rx) = mpsc::channel();
            self.threads.push(thread::Builder::new()
                .name(format!("Worker:{}-{}", self.node, i))
                .spawn(move || worker_fn(rx))
                .unwrap());
            self.channels.push(tx);
        }

        let (ticker_tx, ticker_rx) = mpsc::channel();
        self.ticker_chan = Some(ticker_tx);
        let ticker_interval = self.ticker_interval;
        let mut sender = self.sender();
        self.ticker_thread = Some(thread::Builder::new()
            .name(format!("WorkerTicker:{}", -1))
            .spawn(move || {
                loop {
                    thread::sleep(ticker_interval);
                    match ticker_rx.try_recv() {
                        Err(mpsc::TryRecvError::Empty) => (),
                        _ => break,
                    }
                    let _ = sender.try_send(WorkerMsg::Tick(time::Instant::now()));
                }
            })
            .unwrap());
    }

    pub fn sender(&self) -> WorkerSender {
        assert!(!self.channels.is_empty());
        WorkerSender {
            cursor: thread_rng().gen(),
            channels: self.channels.clone(),
        }
    }
}

impl WorkerSender {
    pub fn send(&mut self, msg: WorkerMsg) {
        // right now only possible error is disconected, so no need to do anything
        let _ = self.try_send(msg);
    }
    pub fn try_send(&mut self, msg: WorkerMsg) -> Result<(), mpsc::SendError<WorkerMsg>> {
        let i = self.cursor % self.channels.len();
        self.cursor = self.cursor.wrapping_add(1);
        self.channels[i].send(msg)
    }
}

impl Drop for WorkerManager {
    fn drop(&mut self) {
        if let Some(chan) = self.ticker_chan.take() {
            let _ = chan.send(());
        }
        for chan in self.channels.drain(..) {
            let _ = chan.send(WorkerMsg::Exit);
        }
        if let Some(t) = self.ticker_thread.take() {
            t.join().unwrap();
        }
        for t in self.threads.drain(..) {
            t.join().unwrap();
        }
    }
}
