use std::{cmp, thread, io, net, time, mem};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use rand::{thread_rng, Rng};
use inflightmap::InFlightMap;
use serde::{Serialize, Deserialize};
use serde_json;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum NodeStatus {
    Alive,
    Suspect,
    Dead,
}

type Seq = u32;

#[derive(Debug, Serialize, Deserialize, Clone)]
enum Message {
    Ping {
        seq: Seq,
    },
    PingReq {
        seq: Seq,
        to: net::SocketAddr,
    },
    Ack {
        seq: Seq,
    },
    Suspect {
        incarnation: Seq,
        from: net::SocketAddr,
        node: net::SocketAddr,
    },
    Dead {
        incarnation: Seq,
        from: net::SocketAddr,
        node: net::SocketAddr,
    },
    Alive {
        incarnation: Seq,
        node: net::SocketAddr,
    },
}

impl Message {
    fn encode<'a>(&self, mut buffer: &'a mut [u8]) -> Result<usize, serde_json::Error> {
        let buffer_len = buffer.len();
        match serde_json::to_writer(&mut buffer, &self) {
            Ok(_) => {
                Ok(buffer_len - buffer.len())
            },
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
    pingreq_inflight: InFlightMap<Seq, net::SocketAddr, time::Instant>,
    ping_inflight: InFlightMap<Seq, net::SocketAddr, time::Instant>,
    suspicious_inflight: InFlightMap<net::SocketAddr, (), time::Instant>,
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
                    info!("{} cant parse message", addr);
                }
            }

            {
                let mut g = g.lock().unwrap();
                // drain send queue
                messages.extend(g.send_queue.drain(..));
                // drain broadcast queue
                let node_count = g.nodes.len();
                for cm in &g.broadcast_queue {
                    for (n, _) in g.nodes.keys().zip(0..cm.0) {
                        messages.push((n.clone(), cm.1.clone()));
                    }
                }
                for cm in &mut g.broadcast_queue {
                    cm.0 -= cmp::min(node_count as u32, cm.0);
                }
                g.broadcast_queue.retain(|&(c, _)| c > 0);
                // gossip to alive nodes
                if let Some(addr) = g.should_gossip_alive() {
                    messages.push((addr, g.generate_ping_msg()));
                }
                // gossip to dead nodes possibly resolving partitions
                if let Some(addr) = g.should_gossip_dead() {
                    messages.push((addr, g.generate_ping_msg()));
                }
                // expire pings and mark suspicous
                while let Some((_, node)) = g.ping_inflight.pop_expired(time::Instant::now()) {
                    {
                        let n = g.nodes.get_mut(&node).unwrap();
                        let incarnation = n.incarnation;
                        n.set_status(NodeStatus::Suspect, incarnation);
                    }
                    let msg = g.generate_suspect_msg(node);
                    g.broadcast(msg);
                }
                // expire pingeqs
                while let Some(_) = g.pingreq_inflight.pop_expired(time::Instant::now()) {}
                // expire suspicous and mark dead
                while let Some((node, _)) = g.suspicious_inflight.pop_expired(time::Instant::now()) {
                    {
                        let n = g.nodes.get_mut(&node).unwrap();
                        let incarnation = n.incarnation;
                        n.set_status(NodeStatus::Dead, incarnation);
                    }
                    let msg = g.generate_dead_msg(node);
                    g.broadcast(msg);
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
        let n = self.nodes.len();
        self.broadcast_queue.push((n as u32, msg));
    }

    fn generate_suspect_msg(&mut self, node: net::SocketAddr) -> Message {
        Message::Suspect {
            incarnation: self.nodes.get(&node).unwrap().incarnation,
            from: self.addr.clone(),
            node: node,
        }
    }

    fn generate_dead_msg(&mut self, node: net::SocketAddr) -> Message {
        Message::Suspect {
            incarnation: self.nodes.get(&node).unwrap().incarnation,
            from: self.addr.clone(),
            node: node,
        }
    }

    fn generate_pingreq_msg(&mut self, seq: Seq, to: net::SocketAddr) -> Message {
        Message::PingReq { seq: seq, to: to }
    }

    fn generate_ping_msg_raw(&mut self, seq: Seq) -> Message {
        Message::Ping { seq: seq }
    }

    fn generate_ack_msg(&mut self, seq: Seq) -> Message {
        Message::Ack { seq: seq }
    }

    fn generate_alive_msg(&mut self) -> Message {
        Message::Alive {
            incarnation: self.incarnation,
            node: self.addr.clone(),
        }
    }

    fn generate_ping_msg(&mut self) -> Message {
        let seq = self.seq;
        self.seq += 1;
        Message::Ping { seq: seq }
    }

    fn should_gossip_alive(&mut self) -> Option<net::SocketAddr> {
        let now = time::Instant::now();
        if now < self.next_alive_probe {
            return None;
        }
        self.next_alive_probe = now + time::Duration::from_secs(1);
        let alive = self.nodes
                        .iter()
                        .filter(|&(_, v)| v.status != NodeStatus::Dead)
                        .collect::<Vec<_>>();
        thread_rng().choose(&alive).map(|kv| kv.0.clone())
    }

    fn should_gossip_dead(&mut self) -> Option<net::SocketAddr> {
        let now = time::Instant::now();
        if now < self.next_dead_probe {
            return None;
        }
        self.next_dead_probe = now + time::Duration::from_secs(1);
        let dead = self.nodes
                       .iter()
                       .filter(|&(_, v)| v.status == NodeStatus::Dead)
                       .collect::<Vec<_>>();
        thread_rng().choose(&dead).map(|kv| kv.0.clone())
    }

    fn on_message(&mut self, sender: net::SocketAddr, msg: Message) {
        debug!("{} on_message: {:?}", self.addr, msg);
        match msg {
            Message::Ping { seq } => {
                let msg = self.generate_ack_msg(seq);
                self.send(sender, msg);
            }
            Message::PingReq { seq, to } => {
                self.pingreq_inflight.insert(seq,
                                             sender,
                                             time::Instant::now() +
                                             time::Duration::from_millis(500));
                let msg = self.generate_ping_msg_raw(seq);
                self.send(to, msg);
            }
            Message::Ack { seq } => {
                if let Some(from) = self.pingreq_inflight.remove(&seq) {
                    self.send(from, msg);
                } else if let Some(_) = self.ping_inflight.remove(&seq) {
                    //
                } else {
                    return
                };

                {
                    let n = self.nodes
                            .entry(sender)
                            .or_insert_with(|| Node::new(NodeStatus::Dead, 0));
                    let incarnation = n.incarnation;
                    if n.set_status(NodeStatus::Alive, incarnation) {
                        Some(Message::Alive {
                            incarnation: incarnation,
                            node: sender,
                        })
                    } else {
                        None
                    }
                }.map(|msg| self.broadcast(msg));
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
                        return
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
        }
    }

    pub fn join(&mut self, seeds: &[&str]) {
        for &seed in seeds {
            if let Ok(addrs) = net::ToSocketAddrs::to_socket_addrs(seed) {
                for addr in addrs {
                    self.nodes.insert(addr.clone(), Node::new(NodeStatus::Dead, 0));
                    let msg = self.generate_alive_msg();
                    self.send(addr, msg);
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
            suspicious_inflight: InFlightMap::new(),
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
