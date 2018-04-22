use rand::{thread_rng, Rng};
use std::sync::mpsc;
use std::{thread, time};

pub trait ExitMsg {
    fn exit_msg() -> Self;
    fn is_exit(&self) -> bool;
}

/// A Sender attached to a WorkerManager
/// messages are distributed to threads in a Round-Robin manner.
pub struct WorkerSender<T: Send + 'static> {
    cursor: usize,
    channels: Vec<mpsc::Sender<T>>,
}

/// A thread pool containing threads prepared to receive WorkerMsg's
pub struct WorkerManager<T: ExitMsg + Send + 'static> {
    thread_count: usize,
    threads: Vec<thread::JoinHandle<()>>,
    channels: Vec<mpsc::Sender<T>>,
    name: String,
}

impl<T: ExitMsg + Send + 'static> WorkerManager<T> {
    pub fn new(name: String, thread_count: usize) -> Self {
        assert!(thread_count > 0);
        WorkerManager {
            thread_count: thread_count,
            threads: Default::default(),
            channels: Default::default(),
            name: name,
        }
    }

    pub fn start<F>(&mut self, mut worker_fn_gen: F)
    where
        F: FnMut() -> Box<FnMut(T) + Send>,
    {
        assert!(self.channels.is_empty());
        for i in 0..self.thread_count {
            // since neither closure cloning or Box<FnOnce> are stable use Box<FnMut>
            let mut worker_fn = worker_fn_gen();
            let (tx, rx) = mpsc::channel();
            self.channels.push(tx);
            self.threads.push(
                thread::Builder::new()
                    .name(format!("Worker:{}:{}", i, self.name))
                    .spawn(move || {
                        for m in rx {
                            if m.is_exit() {
                                break;
                            }
                            worker_fn(m);
                        }
                        info!("Exiting worker");
                    })
                    .unwrap(),
            );
        }
    }

    pub fn sender(&self) -> WorkerSender<T> {
        assert!(!self.channels.is_empty());
        WorkerSender {
            cursor: thread_rng().gen(),
            channels: self.channels.clone(),
        }
    }
}

impl<T: Send + 'static> WorkerSender<T> {
    pub fn send(&mut self, msg: T) {
        // right now only possible error is disconected, so no need to do anything
        let _ = self.try_send(msg);
    }
    pub fn try_send(&mut self, msg: T) -> Result<(), mpsc::SendError<T>> {
        self.cursor = self.cursor.wrapping_add(1);
        self.channels[self.cursor % self.channels.len()].send(msg)
    }
}

impl<T: ExitMsg + Send + 'static> Drop for WorkerManager<T> {
    fn drop(&mut self) {
        for c in &*self.channels {
            let _ = c.send(T::exit_msg());
        }
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

pub fn timer_fn<F>(
    name: String,
    interval: time::Duration,
    mut callback: F,
) -> thread::JoinHandle<()>
where
    F: FnMut(time::Instant) -> bool + Send + 'static,
{
    thread::Builder::new()
        .name(format!("Timer:{}", name))
        .spawn(move || loop {
            thread::sleep(interval);
            if !callback(time::Instant::now()) {
                break;
            }
        })
        .expect("Can't start timer")
}
