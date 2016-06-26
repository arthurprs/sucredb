use std::{time, thread};
use std::sync::mpsc;
use fabric::FabricMsg;

pub enum WorkerMsg {
    Fabric(FabricMsg),
    Tick(time::SystemTime),
    Done,
}

pub struct VNodeWorkers {
    ticker_interval: time::Duration,
    ticker_thread: Option<thread::JoinHandle<()>>,
    ticker_chan: mpsc::Sender<()>,
    threads: Vec<Option<thread::JoinHandle<()>>>,
    channels: Vec<mpsc::Sender<WorkerMsg>>,
}

impl VNodeWorkers {
    pub fn new<F>(thread_count: usize, fun: F, ticker_interval: time::Duration) -> Self
    where F: FnOnce(mpsc::Receiver<WorkerMsg>) + Clone + Send + 'static {
        let mut threads = Vec::new();
        let mut channels = Vec::new();
        for _ in 0..thread_count {
            let fun_cloned = fun.clone();
            let (tx, rx) = mpsc::channel();
            threads.push(Some(thread::spawn(move || fun_cloned(rx))));
            channels.push(tx);
        }
        let channels_cloned = channels.clone();
        let (ticker_tx, ticker_rx) = mpsc::channel();
        let ticker_thread = thread::spawn(move || {
            loop {
                thread::sleep(ticker_interval);
                if ticker_rx.try_recv().is_ok() {
                    break;
                }
                let now = time::SystemTime::now();
                for chan in &channels {
                    chan.send(WorkerMsg::Tick(now)).unwrap();
                }
            };
        });
        VNodeWorkers {
            ticker_interval: ticker_interval,
            ticker_thread: Some(ticker_thread),
            ticker_chan: ticker_tx,
            threads: threads,
            channels: channels_cloned,
        }
    }

    pub fn send(&self, vnode: u16, msg: WorkerMsg) {
        self.channels[vnode as usize % self.channels.len()].send(msg).unwrap();
    }
}

impl Drop for VNodeWorkers {
    fn drop(&mut self) {
        self.ticker_chan.send(()).unwrap();
        for chan in &self.channels {
            chan.send(WorkerMsg::Done).unwrap();
        }
        self.ticker_thread.take().map(|t| t.join());
        for to in &mut self.threads {
            to.take().map(|t| t.join());
        }
    }
}
