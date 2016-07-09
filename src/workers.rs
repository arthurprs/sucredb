use std::{time, thread};
use std::sync::mpsc;
use fabric::{NodeId, FabricMsg};
use rand::{weak_rng, Rng, XorShiftRng};

pub enum WorkerMsg {
    Fabric(NodeId, FabricMsg),
    Tick(time::SystemTime),
    Request,
    Exit,
}

pub struct WorkerSender {
    rng: XorShiftRng,
    channels: Vec<mpsc::Sender<WorkerMsg>>,
}

pub struct WorkerManager {
    ticker_interval: time::Duration,
    ticker_thread: Option<thread::JoinHandle<()>>,
    ticker_chan: Option<mpsc::Sender<()>>,
    thread_count: usize,
    threads: Vec<thread::JoinHandle<()>>,
    channels: Vec<mpsc::Sender<WorkerMsg>>,
}

impl WorkerManager {
    pub fn new(thread_count: usize, ticker_interval: time::Duration) -> Self {
        assert!(thread_count > 0);
        WorkerManager {
            ticker_interval: ticker_interval,
            ticker_thread: None,
            ticker_chan: None,
            thread_count: thread_count,
            threads: Vec::new(),
            channels: Vec::new(),
        }
    }

    pub fn start<F>(&mut self, fun: F) where F: Fn() -> Box<FnMut(mpsc::Receiver<WorkerMsg>) + Send> {
        assert!(self.channels.is_empty());
        for _ in 0..self.thread_count {
            let mut fun_cloned = fun();
            let (tx, rx) = mpsc::channel();
            self.threads.push(thread::spawn(move || fun_cloned(rx)));
            self.channels.push(tx);
        }
        let channels = self.channels.clone();

        let (ticker_tx, ticker_rx) = mpsc::channel();
        self.ticker_chan = Some(ticker_tx);
        let ticker_interval = self.ticker_interval;

        self.ticker_thread = Some(thread::spawn(move || {
            loop {
                thread::sleep(ticker_interval);
                if ticker_rx.try_recv().is_ok() {
                    break;
                }
                let now = time::SystemTime::now();
                for chan in &channels {
                    let _ = chan.send(WorkerMsg::Tick(now));
                }
            }
        }));
    }

    pub fn sender(&self) -> WorkerSender {
        assert!(!self.channels.is_empty());
        WorkerSender {
            rng: weak_rng(),
            channels: self.channels.clone(),
        }
    }
}

impl WorkerSender {
    pub fn send(&mut self, msg: WorkerMsg) {
        self.rng.choose(&self.channels).unwrap().send(msg).unwrap();
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
        for to in self.threads.drain(..) {
            to.join().unwrap();
        }
    }
}
