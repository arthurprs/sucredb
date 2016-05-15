use std::{cmp, thread, io, net, time, fmt};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use rand::{thread_rng, Rng};
use inflightmap::InFlightMap;
use serde::{Serialize, Deserialize};
use serde_json;

const PACKET_SIZE: usize = 1400;
const SUSPECT_TIMEOUT_MS: u64 = 1000;
const PING_TIMEOUT_MS: u64 = 500;
const PING_PERIOD_MS: u64 = 250;
const PING_SYNC_CHANCE: f32 = 0.15f32;
const PING_CANDIDATES: usize = 3;
const PINGREQ_CANDIDATES: usize = 3;

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
enum NodeStatus {
    Alive,
    Suspect,
    Dead,
}

pub trait Metadata: Serialize + Deserialize + Clone + Send + fmt::Debug {}

impl<T: Serialize + Deserialize + Clone + Send + fmt::Debug> Metadata for T {}

type Seq = u32;

type StateTriple<T> = (net::SocketAddr, Seq, NodeStatus, T);

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message<T: Metadata> {
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
        meta: T,
    },
    Sync {
        state: Vec<StateTriple<T>>,
    },
    SyncAck {
        state: Vec<StateTriple<T>>,
    },
}

impl<T: Metadata> Message<T> {
    fn encode(&self, mut buffer: &mut [u8]) -> Result<usize, serde_json::Error> {
        let buffer_len = buffer.len();
        match serde_json::to_writer(&mut buffer, &self) {
            Ok(_) => Ok(buffer_len - buffer.len()),
            Err(err) => {
                warn!("json encode err: {:?}", err);
                Err(err)
            }
        }
    }

    fn decode(buffer: &[u8]) -> Result<Message<T>, serde_json::Error> {
        warn!("decoding {:?}", buffer);
        match serde_json::from_slice(buffer) {
            Ok(msg) => Ok(msg),
            Err(err) => {
                warn!("json decode err: {:?}", err);
                Err(err)
            }
        }
    }
}

#[derive(Debug)]
struct Node<T: Metadata> {
    incarnation: Seq,
    status_change: time::Instant,
    status: NodeStatus,
    meta: T,
}

struct Inner<T: Metadata> {
    addr: net::SocketAddr,
    seq: Seq,
    incarnation: Seq,
    meta: T,
    nodes: HashMap<net::SocketAddr, Node<T>>,
    next_alive_probe: time::Instant,
    next_dead_probe: time::Instant,
    pingreq_inflight: InFlightMap<Seq, (net::SocketAddr, net::SocketAddr), time::Instant>,
    ping_inflight: InFlightMap<Seq, net::SocketAddr, time::Instant>,
    suspect_inflight: InFlightMap<net::SocketAddr, time::Instant, time::Instant>,
    send_queue: VecDeque<(net::SocketAddr, Message<T>)>,
    broadcast_queue: Vec<(u32, Message<T>)>,
    running: isize,
}

pub struct Gossiper<T: Metadata>(Arc<Mutex<Inner<T>>>);

impl<T: Metadata> Node<T> {
    fn new(status: NodeStatus, incarnation: Seq, meta: T) -> Node<T> {
        Node {
            incarnation: incarnation,
            status_change: time::Instant::now(),
            status: status,
            meta: meta,
        }
    }

    fn set_status(&mut self, status: NodeStatus, incarnation: Seq) -> bool {
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

impl<T: Metadata> Inner<T> {
    fn run(socket: net::UdpSocket, g: Arc<Mutex<Inner<T>>>) {
        let mut stack_buffer = [0u8; PACKET_SIZE];
        let mut messages = Vec::new();
        let addr = g.lock().unwrap().addr.clone();
        let mut broadcast_counter: usize = thread_rng().gen();
        while g.lock().unwrap().running > 0 {
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
                let candidates = g.get_candidates(true, !0 as usize);
                for &mut (ref mut counter, ref msg) in &mut g.broadcast_queue {
                    let n = cmp::min(candidates.len(), *counter as usize);
                    for _ in 0..n {
                        messages.push((candidates[broadcast_counter % candidates.len()],
                                       msg.clone()));
                    }
                    broadcast_counter = broadcast_counter.wrapping_add(n);
                    *counter -= n as u32;
                }
                g.broadcast_queue.retain(|&(c, _)| c > 0);
                // gossip to alive nodes
                g.maybe_gossip_alive(now);
                // gossip to dead nodes possibly resolving partitions, etc
                g.maybe_gossip_dead(now);
                // expire pings and fire indirect pings
                while let Some((seq, node)) = g.ping_inflight.pop_expired(now) {
                    debug!("{:?} pingreq to {:?}", addr, node);
                    if g.send_ping_reqs(seq, node) == 0 {
                        g.pingreq_inflight.insert(seq, (addr, node), now);
                    }
                }
                // expire pingreqs and mark as suspect if we are the originating node
                while let Some((_, (from, node))) = g.pingreq_inflight.pop_expired(now) {
                    debug!("pingreq expired {:?} {:?} - {:?}", from, node, addr);
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
            }

            // send serialized messages
            for (remote_addr, msg) in messages.drain(..) {
                if let Ok(buffer_len) = msg.encode(&mut stack_buffer) {
                    warn!("buffer is {:?}", &stack_buffer[..buffer_len]);
                    trace!("{} sending to {} {:?}", addr, remote_addr, msg);
                    let _ = socket.send_to(&stack_buffer[..buffer_len], remote_addr);
                }
            }
        }

        drop(socket);
        g.lock().unwrap().running = -1;
    }

    fn send(&mut self, to: net::SocketAddr, msg: Message<T>) {
        self.send_queue.push_back((to, msg));
    }

    fn broadcast(&mut self, msg: Message<T>) {
        // self.nodes dont include self, so + 2
        let n = ((self.nodes.len() + 2) as f32).log10().ceil() as u32 * 4;
        self.broadcast_queue.push((n, msg));
    }

    fn generate_ping_msg(&mut self) -> (Seq, Message<T>) {
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
        trace!("{:?} nodes are {:?}, returning {} candidates",
               self.addr,
               self.nodes,
               candidates.len());
        candidates
    }

    fn send_ping_reqs(&mut self, seq: Seq, node: net::SocketAddr) -> usize {
        let now = time::Instant::now();
        let candidates = self.get_candidates(true, PINGREQ_CANDIDATES);
        debug!("{} sending indirect pings to {} through {} other nodes",
               self.addr,
               node,
               candidates.len());
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
        self.next_alive_probe = now + time::Duration::from_millis(PING_PERIOD_MS);
        let candidates = self.get_candidates(true, PING_CANDIDATES);
        if candidates.len() != 0 {
            debug!("{} gossiping to {} alive nodes",
                   self.addr,
                   candidates.len());
            for &k in &candidates {
                let (seq, msg) = self.generate_ping_msg();
                self.ping_inflight
                    .insert(seq, k, now + time::Duration::from_millis(PING_TIMEOUT_MS));
                self.send(k, msg);
                // chance to fire a sync message as well
                if thread_rng().gen::<f32>() < PING_SYNC_CHANCE {
                    let sync_state = self.generate_sync_state();
                    self.send(k, Message::Sync { state: sync_state });
                }
            }
        }
        candidates.len()
    }

    fn maybe_gossip_dead(&mut self, now: time::Instant) -> usize {
        if now < self.next_dead_probe {
            return 0;
        }
        self.next_dead_probe = now + time::Duration::from_secs(PING_PERIOD_MS);

        let candidates = self.get_candidates(false, PING_CANDIDATES);
        if candidates.len() != 0 {
            debug!("{} gossiping to {} dead nodes", self.addr, candidates.len());
            for &k in &candidates {
                let (seq, msg) = self.generate_ping_msg();
                self.ping_inflight
                    .insert(seq, k, now + time::Duration::from_millis(PING_TIMEOUT_MS));
                self.send(k, msg)
            }
        }
        candidates.len()
    }

    fn on_message(&mut self, sender: net::SocketAddr, msg: Message<T>) {
        trace!("{} on_message: {:?}", self.addr, msg);
        match msg {
            Message::Ping { seq } => {
                self.send(sender, Message::PingAck { seq: seq });
            }
            Message::PingReq { seq, node } => {
                self.pingreq_inflight.insert(seq,
                                             (sender, node),
                                             time::Instant::now() +
                                             time::Duration::from_millis(PING_TIMEOUT_MS));
                self.send(node, Message::Ping { seq: seq });
            }
            Message::PingAck { seq } => {
                if let Some((from, _)) = self.pingreq_inflight.remove(&seq) {
                    self.send(from, msg);
                } else if let Some(_) = self.ping_inflight.remove(&seq) {
                    //
                } else {
                    // do nothing if we dont have it in state
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
                                meta: n.meta.clone(),
                            })
                        } else {
                            None
                        }
                    })
                    .map(|msg| self.broadcast(msg));
            }
            Message::Alive { mut incarnation, node, mut meta } => {
                if node == self.addr {
                    if incarnation <= self.incarnation {
                        return;
                    }
                    // refute
                    debug!("node {:?} REFUTE ALIVE", node);
                    self.incarnation = cmp::max(self.incarnation, incarnation) + 1;
                    incarnation = self.incarnation;
                    meta = self.meta.clone();
                } else {
                    let mut existing = true;
                    let n = self.nodes
                                .entry(node)
                                .or_insert_with(|| {
                                    existing = false;
                                    Node::new(NodeStatus::Dead, 0, meta.clone())
                                });
                    if existing && incarnation <= n.incarnation {
                        return;
                    }
                    debug!("{:?} node {:?} IS ALIVE", self.addr, node);
                    n.set_status(NodeStatus::Alive, incarnation);
                }
                self.broadcast(Message::Alive {
                    incarnation: incarnation,
                    node: node,
                    meta: meta,
                });
            }
            Message::Suspect { incarnation, from, node } => {
                if node == self.addr {
                    if incarnation < self.incarnation {
                        return;
                    }
                    // refute
                    debug!("node {:?} REFUTE SUSPECT", node);
                    self.incarnation = cmp::max(self.incarnation, incarnation) + 1;
                    let msg = Message::Alive {
                        incarnation: self.incarnation,
                        node: node,
                        meta: self.meta.clone(),
                    };
                    self.broadcast(msg);
                    return;
                } else if let Some(n) = self.nodes.get_mut(&node) {
                    if incarnation < n.incarnation || n.status != NodeStatus::Alive {
                        return;
                    }
                    debug!("{:?} node {:?} IS SUSPECT", self.addr, node);
                    n.set_status(NodeStatus::Suspect, incarnation);
                    self.suspect_inflight.insert(node,
                                                 n.status_change,
                                                 time::Instant::now() +
                                                 time::Duration::from_millis(SUSPECT_TIMEOUT_MS));
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
                    debug!("node {:?} REFUTE DEAD", node);
                    self.incarnation = cmp::max(self.incarnation, incarnation) + 1;
                    let msg = Message::Alive {
                        incarnation: self.incarnation,
                        node: node,
                        meta: self.meta.clone(),
                    };
                    self.broadcast(msg);
                    return;
                } else if let Some(n) = self.nodes.get_mut(&node) {
                    if incarnation < n.incarnation || n.status == NodeStatus::Dead {
                        return;
                    }
                    debug!("{:?} node {:?} IS DEAD", self.addr, node);
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

    fn generate_sync_state(&mut self) -> Vec<StateTriple<T>> {
        let mut state: Vec<_> = self.nodes
                                    .iter()
                                    .map(|(k, n)| {
                                        (k.clone(), n.incarnation, n.status, n.meta.clone())
                                    })
                                    .collect();
        state.push((self.addr, self.incarnation, NodeStatus::Alive, self.meta.clone()));
        state.truncate(25);
        state
    }

    fn do_sync(&mut self, state: Vec<StateTriple<T>>) {
        let sender = self.addr;
        for (addr, incarnation, status, meta) in state {
            let msg = match status {
                NodeStatus::Alive => {
                    Message::Alive {
                        node: addr,
                        incarnation: incarnation,
                        meta: meta,
                    }
                }
                // threat suspect and dead the same
                NodeStatus::Suspect | NodeStatus::Dead => {
                    Message::Suspect {
                        from: sender,
                        node: addr,
                        incarnation: incarnation,
                    }
                }
            };
            self.on_message(sender, msg);
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

impl<T: Metadata + 'static> Gossiper<T> {
    pub fn new(listen_addr: &str, meta: T) -> Result<Gossiper<T>, io::Error> {
        let socket = try!(net::UdpSocket::bind(listen_addr));
        try!(socket.set_nonblocking(true));
        let inner = Arc::new(Mutex::new(Inner {
            addr: socket.local_addr().unwrap(),
            nodes: Default::default(),
            incarnation: 0,
            seq: 0,
            meta: meta,
            next_alive_probe: time::Instant::now(),
            next_dead_probe: time::Instant::now(),
            ping_inflight: InFlightMap::new(),
            pingreq_inflight: InFlightMap::new(),
            suspect_inflight: InFlightMap::new(),
            send_queue: Default::default(),
            broadcast_queue: Default::default(),
            running: 1,
        }));
        let inner2 = inner.clone();
        let _ = thread::Builder::new()
                    .name(format!("Gossiper {}", listen_addr))
                    .spawn(move || Inner::run(socket, inner2));
        Ok(Gossiper(inner))
    }

    pub fn join(&self, seeds: &[&str]) {
        self.0.lock().unwrap().join(seeds)
    }

    pub fn node_count(&self) -> usize {
        self.0.lock().unwrap().nodes.len() + 1
    }

    pub fn alive_count(&self) -> usize {
        self.0.lock().unwrap().nodes.values().filter(|n| n.status != NodeStatus::Dead).count() + 1
    }
}

impl<T: Metadata> Drop for Gossiper<T> {
    fn drop(&mut self) {
        self.0.lock().unwrap().running = 0;
        while self.0.lock().unwrap().running >= 0 {
            thread::sleep(time::Duration::from_millis(100));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;
    use std::{time, thread};

    fn test_converge(n: usize) -> Vec<Gossiper<()>> {
        let _ = env_logger::init();
        let g: Vec<_> = (0..n)
                            .map(|i| Gossiper::new(&format!("0.0.0.0:{}", 9000 + i), ()).unwrap())
                            .collect();
        let start = time::Instant::now();
        for (i, g0) in (&g[1..]).iter().enumerate() {
            g0.join(&[&format!("0.0.0.0:{}", 9000 + i)]);
        }
        for _ in 0..(n * 1000) {
            if g.iter().all(|g| g.alive_count() == n) {
                break;
            }
            thread::sleep(time::Duration::from_millis(1));
        }
        warn!("{:?} has passed", time::Instant::now() - start);
        assert!(g.iter().all(|g| g.alive_count() == n),
                "{} {:?}",
                n,
                g.iter().map(|g| g.alive_count()).collect::<Vec<_>>());
        g
    }

    macro_rules! test_converge_n {
        ($fn_name: ident, $n: expr) => (
            #[test]
            fn $fn_name() {
                test_converge($n);
            }
        );
    }

    test_converge_n!(test_converge_1, 1);
    test_converge_n!(test_converge_2, 2);
    test_converge_n!(test_converge_3, 3);
    test_converge_n!(test_converge_5, 5);
    test_converge_n!(test_converge_10, 10);
    test_converge_n!(test_converge_20, 20);
    test_converge_n!(test_converge_30, 30);
    test_converge_n!(test_converge_50, 50);

    fn test_dead(n: usize) {
        let _ = env_logger::init();
        let mut g = test_converge(n);
        g.pop();
        let start = time::Instant::now();
        for _ in 0..(n * 2000) {
            if g.iter().all(|g| g.alive_count() == n - 1) {
                break;
            }
            thread::sleep(time::Duration::from_millis(1));
        }
        warn!("{:?} has passed", time::Instant::now() - start);
        assert!(g.iter().all(|g| g.alive_count() == n - 1),
                "{} {:?}",
                n - 1,
                g.iter().map(|g| g.alive_count()).collect::<Vec<_>>());
    }

    macro_rules! test_dead_n {
        ($fn_name: ident, $n: expr) => (
            #[test]
            fn $fn_name() {
                test_dead($n);
            }
        );
    }

    test_dead_n!(test_dead_1, 1);
    test_dead_n!(test_dead_2, 2);
    test_dead_n!(test_dead_3, 3);
    test_dead_n!(test_dead_5, 5);
    test_dead_n!(test_dead_10, 10);
    test_dead_n!(test_dead_20, 20);
    test_dead_n!(test_dead_30, 30);
    test_dead_n!(test_dead_50, 50);
}
