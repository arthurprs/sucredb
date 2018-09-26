use crossbeam_channel as chan;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{thread, time};

pub trait ExitMsg {
    fn exit_msg() -> Self;
    fn is_exit(&self) -> bool;
}

/// A Sender attached to a WorkerManager
/// messages are distributed to threads in a Round-Robin manner.
pub struct WorkerSender<T: ExitMsg + Send + 'static> {
    cursor: AtomicUsize,
    alive_threads: Arc<AtomicUsize>,
    channels: Vec<chan::Sender<T>>,
}

impl<T: ExitMsg + Send + 'static> Clone for WorkerSender<T> {
    fn clone(&self) -> Self {
        WorkerSender {
            cursor: Default::default(),
            channels: self.channels.clone(),
            alive_threads: self.alive_threads.clone(),
        }
    }
}

/// A thread pool containing threads prepared to receive WorkerMsg's
pub struct WorkerManager<T: ExitMsg + Send + 'static> {
    thread_count: usize,
    threads: Vec<thread::JoinHandle<()>>,
    name: String,
    alive_threads: Arc<AtomicUsize>,
    channels: Vec<chan::Sender<T>>,
}

impl<T: ExitMsg + Send + 'static> WorkerManager<T> {
    pub fn new(name: String, thread_count: usize) -> Self {
        assert!(thread_count > 0);
        WorkerManager {
            thread_count: thread_count,
            threads: Default::default(),
            name: name,
            alive_threads: Default::default(),
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
            let alive_handle = self.alive_threads.clone();
            self.channels.push(tx);
            self.threads.push(
                thread::Builder::new()
                    .name(format!("Worker:{}:{}", i, self.name))
                    .spawn(move || {
                        alive_handle.fetch_add(1, Ordering::SeqCst);
                        for m in rx {
                            if m.is_exit() {
                                break;
                            }
                            worker_fn(m);
                        }
                        alive_handle.fetch_sub(1, Ordering::SeqCst);
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
            alive_threads: self.alive_threads.clone(),
        }
    }
}

impl<T: ExitMsg + Send + 'static> WorkerSender<T> {
    pub fn send(&self, msg: T) -> bool {
        let cursor = self.cursor.fetch_add(1, Ordering::Relaxed);
        self.send_to(cursor, msg)
    }

    pub fn send_to(&self, seed: usize, msg: T) -> bool {
        self.channels[seed % self.channels.len()].send(msg);
        self.alive_threads.load(Ordering::SeqCst) > 0
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
