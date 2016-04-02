use std::{cmp, thread, io, net, time};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use rand::{thread_rng, Rng};
use inflightmap::InFlightMap;
use serde::{Serialize, Deserialize};
use rmp_serde;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum NodeStatus {
    Alive,
    Suspect,
    Dead,
}

type Seq = u32;

#[derive(Debug, Serialize, Deserialize)]
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
    fn encode<'a>(&self, mut buffer: &'a mut [u8]) -> Result<&'a mut [u8], ()> {
        self.serialize(&mut rmp_serde::Serializer::new(&mut buffer))
            .map(|_| buffer)
            .map_err(|_| ())
    }

    fn decode(buffer: &[u8]) -> Result<Message, ()> {
        Deserialize::deserialize(&mut rmp_serde::Deserializer::new(buffer)).map_err(|_| ())
    }
}

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

    fn set_status(&mut self, status: NodeStatus, incarnation: Seq) {
        if self.status != status {
            self.status = NodeStatus::Alive;
            self.status_change = time::Instant::now();
        }
        self.incarnation = incarnation;
    }
}

impl Inner {
    fn run(socket: net::UdpSocket, g: Arc<Mutex<Inner>>) {
        let mut stack_buffer = [0u8; 1500];
        let mut messages = Vec::new();
        socket.set_read_timeout(Some(time::Duration::from_millis(5))).unwrap();
        socket.set_write_timeout(Some(time::Duration::from_millis(5))).unwrap();
        loop {
            while let Ok((buffer_len, remote_addr)) = socket.recv_from(&mut stack_buffer) {
                if let Ok(msg) = Message::decode(&stack_buffer[..buffer_len]) {
                    g.lock().unwrap().on_message(remote_addr, msg);
                }
            }

            {
                let mut g = g.lock().unwrap();
                if let Some(addr) = g.should_gossip_alive() {
                    messages.push((addr, g.generate_ping_msg()));
                }
                if let Some(addr) = g.should_gossip_dead() {
                    messages.push((addr, g.generate_ping_msg()));
                }

                while let Some((_, node)) = g.ping_inflight.pop_expired(time::Instant::now()) {
                    {
                        let n = g.nodes.get_mut(&node).unwrap();
                        let incarnation = n.incarnation;
                        n.set_status(NodeStatus::Suspect, incarnation);
                    }
                    let msg = g.generate_suspect_msg(node);
                    g.broadcast(msg);
                }

                while let Some(_) = g.pingreq_inflight.pop_expired(time::Instant::now()) {}

                while let Some((node, _)) = g.suspicious_inflight.pop_expired(time::Instant::now()) {
                    {
                        let n = g.nodes.get_mut(&node).unwrap();
                        let incarnation = n.incarnation;
                        n.set_status(NodeStatus::Dead, incarnation);
                    }
                    let msg = g.generate_dead_msg(node);
                    g.broadcast(msg);
                }

                messages.extend(g.send_queue.drain(..));
            }

            for (remote_addr, msg) in messages.drain(..) {
                if let Ok(buffer) = msg.encode(&mut stack_buffer) {
                    let _ = socket.send_to(buffer, remote_addr);
                }
            }
        }
    }

    fn send(&mut self, to: net::SocketAddr, msg: Message) {
        self.send_queue.push_back((to, msg));
    }

    fn broadcast(&mut self, msg: Message) {
        let n = self.nodes.len();
        self.broadcast_queue.push((n as Seq, msg));
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
                let on_time = if let Some(from) = self.pingreq_inflight.remove(&seq) {
                    self.send(from, msg);
                    true
                } else if let Some(_) = self.ping_inflight.remove(&seq) {
                    true
                } else {
                    false
                };

                if on_time {
                    let n = self.nodes
                                .entry(sender)
                                .or_insert_with(|| Node::new(NodeStatus::Alive, 0));
                    let incarnation = n.incarnation;
                    n.set_status(NodeStatus::Alive, incarnation);
                }
            }
            Message::Alive { mut incarnation, node } => {
                if node == self.addr {
                    if incarnation <= self.incarnation {
                        return;
                    }
                    self.incarnation = cmp::max(self.incarnation, incarnation) + 1;
                    incarnation = self.incarnation;
                } else {
                    let n = self.nodes
                                .entry(node)
                                .or_insert_with(|| Node::new(NodeStatus::Alive, incarnation));
                    if incarnation <= n.incarnation {
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
}

impl Gossiper {
    pub fn new() -> Result<Gossiper, io::Error> {
        let socket = try!(net::UdpSocket::bind("0.0.0.0:9939"));
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
        }));
        let inner2 = inner.clone();
        let _ = thread::Builder::new()
                    .name("Gossiper".into())
                    .spawn(move || Inner::run(socket, inner2));
        Ok(Gossiper(inner))
    }

    pub fn join(&mut self) {
        unimplemented!()
    }
}
