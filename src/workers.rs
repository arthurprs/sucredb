use crossbeam_channel as chan;
use std::{thread, time};
use std::sync::atomic::{Ordering, AtomicUsize};

pub trait ExitMsg {
    fn exit_msg() -> Self;
    fn is_exit(&self) -> bool;
}

/// A Sender attached to a WorkerManager
/// messages are distributed to threads in a Round-Robin manner.
pub struct WorkerSender<T: ExitMsg + Send + 'static> {
    cursor: AtomicUsize,
    channels: Vec<chan::Sender<T>>,
}

impl<T: ExitMsg + Send + 'static> Clone for WorkerSender<T> {
    fn clone(&self) -> Self {
        WorkerSender {
            cursor: Default::default(),
            channels: self.channels.clone(),
        }
    }
}

/// A thread pool containing threads prepared to receive WorkerMsg's
pub struct WorkerManager<T: ExitMsg + Send + 'static> {
    thread_count: usize,
    threads: Vec<thread::JoinHandle<()>>,
    name: String,
    channels: Vec<chan::Sender<T>>,
}

impl<T: ExitMsg + Send + 'static> WorkerManager<T> {
    pub fn new(name: String, thread_count: usize) -> Self {
        assert!(thread_count > 0);
        WorkerManager {
            thread_count: thread_count,
            threads: Default::default(),
            name: name,
            channels: Default::default(),
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
            let (tx, rx) = chan::unbounded();
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
            cursor: Default::default(),
            channels: self.channels.clone(),
        }
    }
}

impl<T: ExitMsg + Send + 'static> WorkerSender<T> {
    pub fn send(&self, msg: T) {
        // right now only possible error is disconected, so no need to do anything
        let _ = self.try_send(msg);
    }

    pub fn try_send(&self, msg: T) -> Result<(), chan::SendError<T>> {
        let cursor = self.cursor.fetch_add(1, Ordering::Relaxed);
        self.try_send_to(cursor, msg)
    }

    pub fn try_send_to(&self, seed: usize, msg: T) -> Result<(), chan::SendError<T>> {
        self.channels[seed % self.channels.len()].send(msg)
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
