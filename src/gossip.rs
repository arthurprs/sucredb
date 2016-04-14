use std::{cmp, thread, io, net, time, mem};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use rand::{thread_rng, Rng};
use inflightmap::InFlightMap;
use serde::{Serialize, Deserialize};
use serde_json;

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
enum NodeStatus {
    Alive,
    Suspect,
    Dead,
}

type Seq = u32;

type StateTriple = (net::SocketAddr, Seq, NodeStatus);

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    Ping {
        seq: Seq,
    },
    PingReq {
        seq: Seq,
        node: net::SocketAddr,
    },
    PingAck {
        seq: Seq,
    },
    Suspect {
        from: net::SocketAddr,
        node: net::SocketAddr,
        incarnation: Seq,
    },
    Dead {
        from: net::SocketAddr,
        node: net::SocketAddr,
        incarnation: Seq,
    },
    Alive {
        incarnation: Seq,
        node: net::SocketAddr,
    },
    Sync {
        state: Vec<StateTriple>,
    },
    SyncAck {
        state: Vec<StateTriple>,
    },
}

impl Message {
    fn encode<'a>(&self, mut buffer: &'a mut [u8]) -> Result<usize, serde_json::Error> {
        let buffer_len = buffer.len();
        match serde_json::to_writer(&mut buffer, &self) {
            Ok(_) => Ok(buffer_len - buffer.len()),
            Err(err) => {
                debug!("{:?}", err);
                Err(err)
            }
        }
    }

    fn decode(buffer: &[u8]) -> Result<Message, serde_json::Error> {
        match serde_json::from_slice(buffer) {
            Ok(msg) => Ok(msg),
            Err(err) => {
                debug!("{:?}", err);
                Err(err)
            }
        }
    }
}

#[derive(Debug)]
struct Node {
    incarnation: Seq,
    status_change: time::Instant,
    status: NodeStatus,
}

struct Inner {
    addr: net::SocketAddr,
    nodes: HashMap<net::SocketAddr, Node>,
    next_alive_probe: time::Instant,
    next_dead_probe: time::Instant,
    seq: Seq,
    incarnation: Seq,
    pingreq_inflight: InFlightMap<Seq, (net::SocketAddr, net::SocketAddr), time::Instant>,
    ping_inflight: InFlightMap<Seq, net::SocketAddr, time::Instant>,
    suspect_inflight: InFlightMap<net::SocketAddr, time::Instant, time::Instant>,
    send_queue: VecDeque<(net::SocketAddr, Message)>,
    broadcast_queue: Vec<(u32, Message)>,
    running: bool,
}

pub struct Gossiper(Arc<Mutex<Inner>>);

impl Node {
    fn new(status: NodeStatus, incarnation: Seq) -> Node {
        Node {
            incarnation: incarnation,
            status_change: time::Instant::now(),
            status: status,
        }
    }

    fn set_status(&mut self, status: NodeStatus, incarnation: Seq) -> bool {
        debug!("{:?} set_status {:?} {:?}", self, status, incarnation);
        if self.status != status {
            self.status = status;
            self.status_change = time::Instant::now();
            self.incarnation = incarnation;
            true
        } else if self.incarnation != incarnation {
            self.incarnation = incarnation;
            true
        } else {
            false
        }
    }
}

impl Inner {
    fn run(socket: net::UdpSocket, g: Arc<Mutex<Inner>>) {
        let mut stack_buffer = [0u8; 1500];
        let mut messages = Vec::new();
        let addr = g.lock().unwrap().addr.clone();
        socket.set_read_timeout(Some(time::Duration::from_millis(5))).unwrap();
        socket.set_write_timeout(Some(time::Duration::from_millis(5))).unwrap();
        while g.lock().unwrap().running {
            while let Ok((buffer_len, remote_addr)) = socket.recv_from(&mut stack_buffer) {
                if let Ok(msg) = Message::decode(&stack_buffer[..buffer_len]) {
                    g.lock().unwrap().on_message(remote_addr, msg);
                } else {
                    warn!("{} cant parse message", addr);
                }
            }

            let now = time::Instant::now();
            {
                let mut g = g.lock().unwrap();
                // drain send queue
                messages.extend(g.send_queue.drain(..));
                // drain broadcast queue
                let node_count = g.nodes.len();
                for &(counter, ref msg) in &g.broadcast_queue {
                    for (&addr, _) in g.nodes.keys().zip(0..counter) {
                        messages.push((addr, msg.clone()));
                    }
                }
                for cm in &mut g.broadcast_queue {
                    cm.0 -= cmp::min(node_count as u32, cm.0);
                }
                g.broadcast_queue.retain(|&(c, _)| c > 0);
                // gossip to alive nodes
                g.maybe_gossip_alive(now);
                // gossip to dead nodes possibly resolving partitions, etc
                g.maybe_gossip_dead(now);
                // expire pings and fire indirect pings
                while let Some((seq, node)) = g.ping_inflight.pop_expired(now) {
                    if g.send_ping_reqs(seq, node) == 0 {
                        g.pingreq_inflight.insert(seq, (addr, node), now);
                    }
                }
                // expire pingreqs and mark as suspect if we are the originating node
                while let Some((_, (from, node))) = g.pingreq_inflight.pop_expired(now) {
                    let msg = match g.nodes.get(&node) {
                        Some(n) if from == addr => {
                            Message::Suspect {
                                node: node,
                                incarnation: n.incarnation,
                                from: addr,
                            }
                        }
                        _ => continue,
                    };
                    g.on_message(addr, msg);
                }
                // expire suspicious and mark dead if status didnt change
                while let Some((node, status_change)) = g.suspect_inflight.pop_expired(now) {
                    let msg = match g.nodes.get(&node) {
                        Some(n) if n.status_change == status_change => {
                            Message::Dead {
                                node: node,
                                incarnation: n.incarnation,
                                from: addr,
                            }
                        }
                        _ => continue,
                    };
                    g.on_message(addr, msg);
                }

                if !messages.is_empty() {
                    debug!("{} sending: {:?}", g.addr, messages);
                }
            }

            // send serialized messages
            for (remote_addr, msg) in messages.drain(..) {
                if let Ok(buffer_len) = msg.encode(&mut stack_buffer) {
                    let _ = socket.send_to(&stack_buffer[..buffer_len], remote_addr);
                }
            }
        }
    }

    fn send(&mut self, to: net::SocketAddr, msg: Message) {
        self.send_queue.push_back((to, msg));
    }

    fn broadcast(&mut self, msg: Message) {
        // self.nodes dont include self, so + 2
        let n = ((self.nodes.len() + 2) as f32).log10().ceil() as u32 * 4;
        self.broadcast_queue.push((n, msg));
    }

    fn generate_ping_msg(&mut self) -> (Seq, Message) {
        let seq = self.seq;
        self.seq += 1;
        (seq, Message::Ping { seq: seq })
    }

    fn get_candidates(&self, alive: bool, limit: usize) -> Vec<net::SocketAddr> {
        let mut candidates: Vec<_> = self.nodes
                                         .iter()
                                         .filter_map(|(&k, v)| {
                                             if (alive && v.status != NodeStatus::Dead) ||
                                                (!alive && v.status == NodeStatus::Dead) {
                                                 Some(k)
                                             } else {
                                                 None
                                             }
                                         })
                                         .collect();
        if candidates.len() > limit {
            thread_rng().shuffle(&mut candidates);
            candidates.truncate(limit);
        }
        info!("{:?} nodes are {:?}, returning {} candidates",
              self.addr,
              self.nodes,
              candidates.len());
        candidates
    }

    fn send_ping_reqs(&mut self, seq: Seq, node: net::SocketAddr) -> usize {
        let now = time::Instant::now();
        let candidates = self.get_candidates(true, 3);
        for &k in &candidates {
            self.pingreq_inflight
                .insert(seq, (self.addr, k), now + time::Duration::from_millis(500));
            self.send(k,
                      Message::PingReq {
                          seq: seq,
                          node: node,
                      });
        }

        candidates.len()
    }

    fn maybe_gossip_alive(&mut self, now: time::Instant) -> usize {
        if now < self.next_alive_probe {
            return 0;
        }
        info!("pass");
        self.next_alive_probe = now + time::Duration::from_secs(1);
        self.get_candidates(true, 3)
            .iter()
            .map(|&k| {
                info!("sending ping");
                let (seq, msg) = self.generate_ping_msg();
                self.ping_inflight.insert(seq, k, now + time::Duration::from_millis(500));
                self.send(k, msg)
            })
            .count()
    }

    fn maybe_gossip_dead(&mut self, now: time::Instant) -> usize {
        if now < self.next_dead_probe {
            return 0;
        }
        self.next_dead_probe = now + time::Duration::from_secs(1);
        self.get_candidates(false, 3)
            .iter()
            .map(|&k| {
                let (seq, msg) = self.generate_ping_msg();
                self.ping_inflight.insert(seq, k, now + time::Duration::from_millis(500));
                self.send(k, msg)
            })
            .count()
    }

    fn on_message(&mut self, sender: net::SocketAddr, msg: Message) {
        debug!("{} on_message: {:?}", self.addr, msg);
        match msg {
            Message::Ping { seq } => {
                self.send(sender, Message::PingAck { seq: seq });
            }
            Message::PingReq { seq, node } => {
                self.pingreq_inflight.insert(seq,
                                             (sender, node),
                                             time::Instant::now() +
                                             time::Duration::from_millis(500));
                self.send(node, Message::Ping { seq: seq });
            }
            Message::PingAck { seq } => {
                if let Some((from, _)) = self.pingreq_inflight.remove(&seq) {
                    self.send(from, msg);
                } else if let Some(_) = self.ping_inflight.remove(&seq) {
                    //
                } else {
                    return;
                };

                self.nodes
                    .get_mut(&sender)
                    .and_then(|n| {
                        let incarnation = n.incarnation;
                        if n.set_status(NodeStatus::Alive, incarnation) {
                            Some(Message::Alive {
                                incarnation: incarnation,
                                node: sender,
                            })
                        } else {
                            None
                        }
                    })
                    .map(|msg| self.broadcast(msg));
            }
            Message::Alive { mut incarnation, node } => {
                if node == self.addr {
                    if incarnation <= self.incarnation {
                        return;
                    }
                    self.incarnation = cmp::max(self.incarnation, incarnation) + 1;
                    incarnation = self.incarnation;
                } else {
                    let mut existing = true;
                    let n = self.nodes
                                .entry(node)
                                .or_insert_with(|| {
                                    existing = false;
                                    Node::new(NodeStatus::Dead, 0)
                                });
                    if existing && incarnation <= n.incarnation {
                        return;
                    }
                    n.set_status(NodeStatus::Alive, incarnation);
                }
                self.broadcast(Message::Alive {
                    incarnation: incarnation,
                    node: node,
                });
            }
            Message::Suspect { incarnation, from, node } => {
                if node == self.addr {
                    if incarnation < self.incarnation {
                        return;
                    }
                    // refute
                    self.incarnation = cmp::max(self.incarnation, incarnation) + 1;
                    let msg = Message::Alive {
                        incarnation: self.incarnation,
                        node: node,
                    };
                    self.broadcast(msg);
                    return;
                } else if let Some(n) = self.nodes.get_mut(&sender) {
                    if incarnation < n.incarnation || n.status != NodeStatus::Alive {
                        return;
                    }
                    n.set_status(NodeStatus::Suspect, incarnation);
                    self.suspect_inflight.insert(node,
                                                 n.status_change,
                                                 time::Instant::now() +
                                                 time::Duration::from_secs(1));
                } else {
                    return;
                }

                self.broadcast(Message::Suspect {
                    incarnation: incarnation,
                    from: from,
                    node: node,
                });
            }
            Message::Dead { incarnation, from, node } => {
                if node == self.addr {
                    if incarnation < self.incarnation {
                        return;
                    }
                    // refute
                    self.incarnation = cmp::max(self.incarnation, incarnation) + 1;
                    let msg = Message::Alive {
                        incarnation: self.incarnation,
                        node: node,
                    };
                    self.broadcast(msg);
                    return;
                } else if let Some(n) = self.nodes.get_mut(&sender) {
                    if incarnation < n.incarnation || n.status == NodeStatus::Dead {
                        return;
                    }
                    n.set_status(NodeStatus::Dead, incarnation);
                } else {
                    return;
                }
                self.broadcast(Message::Dead {
                    incarnation: incarnation,
                    from: from,
                    node: node,
                });
            }
            Message::Sync { state } => {
                self.do_sync(state);
                let ack_state = self.generate_sync_state();
                self.send(sender, Message::SyncAck { state: ack_state });
            }
            Message::SyncAck { state } => {
                self.do_sync(state);
            }
        }
    }

    fn generate_sync_state(&mut self) -> Vec<StateTriple> {
        let mut state: Vec<_> = self.nodes
                                    .iter()
                                    .map(|(k, n)| (k.clone(), n.incarnation, n.status))
                                    .collect();
        state.push((self.addr, self.incarnation, NodeStatus::Alive));
        state
    }

    fn do_sync(&mut self, state: Vec<StateTriple>) {
        let sender = self.addr;
        for (addr, incarnation, status) in state {
            self.on_message(sender,
                            if status == NodeStatus::Alive {
                                Message::Alive {
                                    node: addr,
                                    incarnation: incarnation,
                                }
                            } else {
                                Message::Suspect {
                                    from: sender,
                                    node: addr,
                                    incarnation: incarnation,
                                }
                            });
        }
    }

    pub fn join(&mut self, seeds: &[&str]) {
        let state = self.generate_sync_state();
        for &seed in seeds {
            if let Ok(addrs) = net::ToSocketAddrs::to_socket_addrs(seed) {
                for addr in addrs {
                    self.send(addr, Message::Sync { state: state.clone() });
                }
            }
        }
    }
}

impl Gossiper {
    pub fn new(listen_addr: &str) -> Result<Gossiper, io::Error> {
        let socket = try!(net::UdpSocket::bind(listen_addr));
        let inner = Arc::new(Mutex::new(Inner {
            addr: socket.local_addr().unwrap(),
            nodes: Default::default(),
            incarnation: 0,
            seq: 0,
            next_alive_probe: time::Instant::now(),
            next_dead_probe: time::Instant::now(),
            ping_inflight: InFlightMap::new(),
            pingreq_inflight: InFlightMap::new(),
            suspect_inflight: InFlightMap::new(),
            send_queue: Default::default(),
            broadcast_queue: Default::default(),
            running: true,
        }));
        let inner2 = inner.clone();
        let _ = thread::Builder::new()
                    .name("Gossiper".into())
                    .spawn(move || Inner::run(socket, inner2));
        Ok(Gossiper(inner))
    }

    pub fn join(&mut self, seeds: &[&str]) {
        self.0.lock().unwrap().join(seeds)
    }
}

impl Drop for Gossiper {
    fn drop(&mut self) {
        self.0.lock().unwrap().running = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        use env_logger;
        use std::{time, thread};
        env_logger::init().unwrap();
        let mut g0 = Gossiper::new("0.0.0.0:8998").unwrap();
        let mut g1 = Gossiper::new("0.0.0.0:8999").unwrap();
        g1.join(&["0.0.0.0:8998"]);
        thread::sleep(time::Duration::from_secs(5));
    }
}
