use std::{cmp, thread, io, time, fmt};
use std::time::{Duration, Instant};
use std::net::SocketAddr;
use std::marker::PhantomData;
use std::sync::{mpsc, Arc, Mutex};
use std::collections::HashMap;

use rand::{thread_rng, Rng};
use serde::Serialize;
use serde::de::DeserializeOwned;
use bincode;
use futures::{Future, Stream, Sink};
use futures::sync::mpsc as fmpsc;
use futures::sync::oneshot as foneshot;
use tokio_core as tokio;

use inflightmap::InFlightMap;
use utils::into_io_error;

const PACKET_SIZE: usize = 1400;
const PING_PERIOD_MS: u64 = 500;
const PING_TIMEOUT_MS: u64 = 1000;
const SUSPECT_TIMEOUT_MS: u64 = 5 * PING_TIMEOUT_MS;
const PING_SYNC_CHANCE: f32 = 0.05f32;
const PING_CANDIDATES: usize = 3;
const PINGREQ_CANDIDATES: usize = 3;
const TIMER_RESOLUTION_MS: u64 = 150;

// quick implementation of SWIM
// has various limitations
// TODO: piggyback
pub struct Gossiper<T: Metadata> {
    context: Arc<Mutex<Inner<T>>>,
    loop_thread: Option<(foneshot::Sender<()>, thread::JoinHandle<io::Result<()>>)>,
}

pub enum GossiperMsg<T: Metadata> {
    New(SocketAddr, T),
    Alive(SocketAddr, T),
    Dead(SocketAddr),
    // Left(SocketAddr),
}

pub type GossiperCallback<T> = Box<FnMut(GossiperMsg<T>) + Send>;

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
enum NodeStatus {
    Alive,
    Suspect,
    Dead,
}

type Seq = u32;

pub trait Metadata
    : Serialize + DeserializeOwned + Clone + PartialEq + Send + fmt::Debug + 'static
    {
}

impl<T: Serialize + DeserializeOwned + Clone + PartialEq + Send + fmt::Debug + 'static> Metadata
    for T {
}

#[derive(Debug)]
struct Node<T: Metadata> {
    incarnation: Seq,
    status_change: Instant,
    status: NodeStatus,
    meta: T,
}

struct Inner<T: Metadata> {
    addr: SocketAddr,
    seq: Seq,
    incarnation: Seq,
    meta: T,
    nodes: HashMap<SocketAddr, Node<T>>,
    next_alive_probe: Instant,
    next_dead_probe: Instant,
    pingreq_inflight: InFlightMap<Seq, (SocketAddr, SocketAddr), Instant>,
    ping_inflight: InFlightMap<Seq, SocketAddr, Instant>,
    suspect_inflight: InFlightMap<SocketAddr, Instant, Instant>,
    send_queue: fmpsc::UnboundedSender<(SocketAddr, Message<T>)>,
    broadcast_queue: Vec<(u32, Message<T>)>,
    callback: GossiperCallback<T>,
    leaving: bool,
    bootstraping: bool,
}

type State<T> = (SocketAddr, Seq, NodeStatus, T);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "T: DeserializeOwned")]
enum Message<T: Metadata> {
    Ping { seq: Seq },
    PingReq { seq: Seq, node: SocketAddr },
    PingAck { seq: Seq },
    Suspect {
        from: SocketAddr,
        node: SocketAddr,
        incarnation: Seq,
    },
    Dead {
        // named Confirm in original paper
        from: SocketAddr,
        node: SocketAddr,
        incarnation: Seq,
    },
    Alive {
        incarnation: Seq,
        node: SocketAddr,
        meta: T,
    },
    Sync { state: Vec<State<T>> },
    SyncAck { state: Vec<State<T>> },
}

struct UdpCodec<T: Metadata>(PhantomData<T>);

impl<T: Metadata> tokio::net::UdpCodec for UdpCodec<T> {
    type In = (SocketAddr, Message<T>);
    type Out = (SocketAddr, Message<T>);

    fn decode(&mut self, addr: &SocketAddr, buf: &[u8]) -> io::Result<Self::In> {
        trace!("decoding {:?}", buf);
        match bincode::deserialize(buf) {
            Ok(msg) => Ok((*addr, msg)),
            Err(err) => {
                warn!("decode err: {:?}", err);
                Err(into_io_error(err))
            }
        }
    }

    fn encode(&mut self, addr_msg: Self::Out, buf: &mut Vec<u8>) -> SocketAddr {
        let (addr, msg) = addr_msg;
        trace!("encoding {:?}", msg);
        match bincode::serialize_into(buf, &msg, bincode::Infinite) {
            Ok(_) => addr,
            Err(err) => {
                panic!("encode err: {:?}", err);
            }
        }
    }
}

impl<T: Metadata> Node<T> {
    fn new(status: NodeStatus, incarnation: Seq, meta: T) -> Node<T> {
        Node {
            incarnation: incarnation,
            status_change: Instant::now(),
            status: status,
            meta: meta,
        }
    }

    fn set_status(&mut self, status: NodeStatus, incarnation: Seq) -> bool {
        if self.status != status {
            self.status = status;
            self.status_change = Instant::now();
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

type InitType<T> = io::Result<(Arc<Mutex<Inner<T>>>, foneshot::Sender<()>)>;

impl<T: Metadata> Inner<T> {
    fn init(
        handle: tokio::reactor::Handle,
        addr: SocketAddr,
        meta: T,
        callback: GossiperCallback<T>,
    ) -> io::Result<Arc<Mutex<Inner<T>>>> {
        let (chan_tx, chan_rx) = fmpsc::unbounded::<(SocketAddr, Message<T>)>();

        let context = Arc::new(Mutex::new(Inner {
            addr: addr,
            nodes: Default::default(),
            incarnation: 0,
            seq: 0,
            meta: meta,
            next_alive_probe: Instant::now(),
            next_dead_probe: Instant::now(),
            ping_inflight: InFlightMap::new(),
            pingreq_inflight: InFlightMap::new(),
            suspect_inflight: InFlightMap::new(),
            send_queue: chan_tx,
            broadcast_queue: Default::default(),
            callback: callback,
            leaving: false,
            bootstraping: false,
        }));

        let socket = tokio::net::UdpSocket::bind(&addr, &handle)?;
        let (s_tx, s_rx) = socket.framed(UdpCodec::<T>(PhantomData)).split();

        let fut_tx = s_tx.send_all(chan_rx.map_err(|_| io::Error::from(io::ErrorKind::Other)))
            .map(|_| ());

        let context2 = context.clone();
        let interval =
            tokio::reactor::Interval::new(Duration::from_millis(TIMER_RESOLUTION_MS), &handle)
                .expect("Can't create Interval");
        let fut_timer = interval
            .for_each(move |_| {
                context2.lock().unwrap().on_timer();
                Ok(())
            })
            .then(|r| {
                info!("timer fut {:?}", r);
                Ok(())
            });

        let context3 = context.clone();
        let fut_rx = s_rx.for_each(move |(a, m)| {
            context3.lock().unwrap().on_message(a, m);
            Ok(())
        });

        let fut_socket = fut_tx.select(fut_rx).map(|_| ()).map_err(|(e, _)| e).then(
            |r| {
                info!("socket fut: {:?}", r);
                Ok(())
            },
        );

        handle.spawn(fut_timer);
        handle.spawn(fut_socket);

        Ok(context)
    }

    fn on_timer(self: &mut Inner<T>) {
        let now = Instant::now();

        // gossip to alive nodes
        self.maybe_gossip_alive(now);
        // gossip to dead nodes possibly resolving partitions, etc
        self.maybe_gossip_dead(now);

        // expire pings and fire indirect pings
        while let Some((seq, node)) = self.ping_inflight.pop_expired(now) {
            debug!("{:?} pingreq to {:?}", self.addr, node);
            if self.send_ping_reqs(seq, node) == 0 {
                // nobody to pingreq!?
                self.pingreq_inflight.insert(seq, (self.addr, node), now);
            }
        }
        // expire pingreqs and mark as suspect if we are the originating node
        while let Some((_, (from, node))) = self.pingreq_inflight.pop_expired(now) {
            debug!("pingreq expired {:?} {:?} - {:?}", from, node, self.addr);
            let msg = match self.nodes.get(&node) {
                Some(n) if from == self.addr => {
                    Message::Suspect {
                        node: node,
                        incarnation: n.incarnation,
                        from: self.addr,
                    }
                }
                _ => continue,
            };
            let addr = self.addr;
            self.on_message(addr, msg);
        }

        // expire suspicious and mark dead if status didnt change
        while let Some((node, status_change)) = self.suspect_inflight.pop_expired(now) {
            let msg = match self.nodes.get(&node) {
                Some(n) if n.status_change == status_change => {
                    Message::Dead {
                        node: node,
                        incarnation: n.incarnation,
                        from: self.addr,
                    }
                }
                _ => continue,
            };
            let addr = self.addr;
            self.on_message(addr, msg);
        }

        // drain broadcast queue
        if !self.broadcast_queue.is_empty() {
            let candidates = self.get_candidates(true, !0);
            let mut messages = Vec::new();
            let mut counter = 0;
            for &mut (ref mut rem, ref msg) in &mut self.broadcast_queue {
                let n = cmp::min(candidates.len(), *rem as usize);
                *rem -= n as u32;
                for _ in 0..n {
                    messages.push((candidates[counter % candidates.len()], msg.clone()));
                    counter += 1;
                }
            }
            self.broadcast_queue.retain(|&(r, _)| r > 0);
            for (addr, msg) in messages {
                self.send(addr, msg);
            }
        }
    }

    fn refute(&mut self, incarnation: Seq) {
        self.incarnation = cmp::max(self.incarnation, incarnation) + 1;
        let msg = Message::Alive {
            incarnation: self.incarnation,
            node: self.addr,
            meta: self.meta.clone(),
        };
        self.broadcast(msg);
    }

    fn get_candidates(&self, alive: bool, limit: usize) -> Vec<SocketAddr> {
        let mut candidates: Vec<_> = self.nodes
            .iter()
            .filter_map(|(&k, v)| if (alive && v.status != NodeStatus::Dead) ||
                (!alive && v.status == NodeStatus::Dead)
            {
                Some(k)
            } else {
                None
            })
            .collect();
        if candidates.len() > limit {
            thread_rng().shuffle(&mut candidates);
            candidates.truncate(limit);
        }
        trace!(
            "{:?} nodes are {:?}, returning {} candidates",
            self.addr,
            self.nodes,
            candidates.len()
        );
        candidates
    }

    fn send_ping_reqs(&mut self, seq: Seq, node: SocketAddr) -> usize {
        let now = Instant::now();
        let candidates = self.get_candidates(true, PINGREQ_CANDIDATES);
        debug!(
            "{} sending indirect pings to {} through {} other nodes",
            self.addr,
            node,
            candidates.len()
        );
        for &k in &candidates {
            self.pingreq_inflight.insert(
                seq,
                (self.addr, k),
                now + time::Duration::from_millis(PING_TIMEOUT_MS),
            );
            self.send(
                k,
                Message::PingReq {
                    seq: seq,
                    node: node,
                },
            );
        }
        candidates.len()
    }

    fn maybe_gossip_alive(&mut self, now: Instant) -> usize {
        if now < self.next_alive_probe {
            return 0;
        }
        self.next_alive_probe = now + time::Duration::from_millis(PING_PERIOD_MS);
        let candidates = self.get_candidates(true, PING_CANDIDATES);
        if !candidates.is_empty() {
            debug!(
                "{} gossiping to {} alive nodes",
                self.addr,
                candidates.len()
            );
            for &k in &candidates {
                // TODO: in case a node is suspect,
                // it'd be best to probe with a Suspect msg
                let (seq, msg) = self.generate_ping_msg();
                self.ping_inflight.insert(
                    seq,
                    k,
                    now +
                        time::Duration::from_millis(
                            PING_TIMEOUT_MS,
                        ),
                );
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

    fn maybe_gossip_dead(&mut self, now: Instant) -> usize {
        // TODO: maybe sync instead
        if now < self.next_dead_probe {
            return 0;
        }
        self.next_dead_probe = now + time::Duration::from_secs(PING_PERIOD_MS);

        let candidates = self.get_candidates(false, PING_CANDIDATES);
        if candidates.len() != 0 {
            debug!("{} gossiping to {} dead nodes", self.addr, candidates.len());
            for &k in &candidates {
                // probe with a dead msg so it does have a chance to refute
                let msg = Message::Dead {
                    node: k,
                    incarnation: self.nodes[&k].incarnation,
                    from: self.addr,
                };
                self.send(k, msg);
            }
        }
        candidates.len()
    }

    fn on_message(&mut self, sender: SocketAddr, msg: Message<T>) {
        trace!("{} on_message: {:?}", self.addr, msg);
        match msg {
            Message::Ping { seq } => {
                self.send(sender, Message::PingAck { seq: seq });
            }
            Message::PingReq { seq, node } => {
                self.pingreq_inflight.insert(
                    seq,
                    (sender, node),
                    Instant::now() + time::Duration::from_millis(PING_TIMEOUT_MS),
                );
                self.send(node, Message::Ping { seq: seq });
            }
            Message::PingAck { seq } => {
                if let Some(_) = self.ping_inflight.remove(&seq) {
                    // good
                } else if let Some((from, _)) = self.pingreq_inflight.remove(&seq) {
                    // send to original sender
                    self.send(from, msg);
                } else {
                    // do nothing if we dont have it in state
                };
            }
            Message::Alive {
                incarnation,
                node,
                meta,
            } => {
                if node == self.addr {
                    if incarnation < self.incarnation ||
                        (incarnation == self.incarnation && meta == self.meta)
                    {
                        return;
                    }
                    if self.leaving {
                        // TODO!
                        return;
                    }
                    // refute
                    debug!("node {:?} REFUTE ALIVE", node);
                    self.refute(incarnation);
                    return;
                }

                {
                    let mut existing = true;
                    let n = self.nodes.entry(node).or_insert_with(|| {
                        existing = false;
                        Node::new(NodeStatus::Dead, 0, meta.clone())
                    });
                    if existing && incarnation <= n.incarnation {
                        return;
                    }
                    debug!("{:?} node {:?} IS ALIVE", self.addr, node);
                    if existing {
                        (self.callback)(GossiperMsg::Alive(node, meta.clone()));
                    } else {
                        (self.callback)(GossiperMsg::New(node, meta.clone()));
                    }
                    n.set_status(NodeStatus::Alive, incarnation);
                }

                // help broadcast
                self.broadcast(Message::Alive {
                    incarnation: incarnation,
                    node: node,
                    meta: meta,
                });
            }
            Message::Suspect {
                incarnation,
                from,
                node,
            } => {
                if node == self.addr {
                    // ignore old info
                    if incarnation < self.incarnation {
                        return;
                    }
                    // refute & broadcast
                    debug!("node {:?} REFUTE SUSPECT", node);
                    self.refute(incarnation);
                    return;
                }

                if let Some(n) = self.nodes.get_mut(&node) {
                    // ignore old info or irrelevant
                    if incarnation < n.incarnation || n.status != NodeStatus::Alive {
                        return;
                    }
                    debug!("{:?} node {:?} IS SUSPECT", self.addr, node);
                    n.set_status(NodeStatus::Suspect, incarnation);
                    self.suspect_inflight.insert(
                        node,
                        n.status_change,
                        Instant::now() +
                            time::Duration::from_millis(SUSPECT_TIMEOUT_MS),
                    );
                } else {
                    // about an unknown node!?
                    return;
                }

                // help broadcast
                self.broadcast(Message::Suspect {
                    incarnation: incarnation,
                    from: from,
                    node: node,
                });
            }
            Message::Dead {
                incarnation,
                from,
                node,
            } => {
                if node == self.addr {
                    // ignore old info
                    if incarnation < self.incarnation {
                        return;
                    }
                    if self.leaving {
                        // TODO!
                        return;
                    }
                    // refute & broadcast
                    debug!("node {:?} REFUTE DEAD", node);
                    self.refute(incarnation);
                    return;
                }

                if let Some(n) = self.nodes.get_mut(&node) {
                    // ignore old info or irrelevant
                    if incarnation < n.incarnation || n.status == NodeStatus::Dead {
                        return;
                    }
                    debug!("{:?} node {:?} IS DEAD", self.addr, node);
                    (self.callback)(GossiperMsg::Dead(node));
                    n.set_status(NodeStatus::Dead, incarnation);
                } else {
                    // about an unknown node!?
                    return;
                }

                // help broadcast
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

    fn generate_sync_state(&mut self) -> Vec<State<T>> {
        let mut state: Vec<_> = self.nodes
            .iter()
            .map(|(&k, n)| (k, n.incarnation, n.status, n.meta.clone()))
            .collect();
        state.push((
            self.addr,
            self.incarnation,
            NodeStatus::Alive,
            self.meta.clone(),
        ));
        // TODO: worry about size
        if state.len() > 20 {
            thread_rng().shuffle(&mut state);
            state.truncate(20);
        }
        state
    }

    fn do_sync(&mut self, state: Vec<State<T>>) {
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

    fn send(&mut self, to: SocketAddr, msg: Message<T>) {
        let _ = self.send_queue.unbounded_send((to, msg));
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

    pub fn update_meta(&mut self, meta: T) {
        self.incarnation += 1;
        self.meta = meta;
        let msg = Message::Alive {
            node: self.addr,
            incarnation: self.incarnation,
            meta: self.meta.clone(),
        };
        self.broadcast(msg);
    }

    pub fn join(&mut self, seeds: &[SocketAddr]) {
        let state = self.generate_sync_state();
        for &seed in seeds {
            self.send(seed, Message::Sync { state: state.clone() });
        }
    }
}

impl<T: Metadata> Gossiper<T> {
    pub fn new(
        listen_addr: SocketAddr,
        meta: T,
        callback: GossiperCallback<T>,
    ) -> io::Result<Gossiper<T>> {
        let (init_tx, init_rx) = mpsc::channel();
        let thread = thread::Builder::new()
            .name(format!("Gossiper:{}", listen_addr))
            .spawn(move || {
                let mut core = tokio::reactor::Core::new().unwrap();
                let (completer_tx, completer_rx) = foneshot::channel();
                init_tx
                    .send(
                        Inner::init(core.handle(), listen_addr, meta, callback)
                            .map(|c| (c, completer_tx)),
                    )
                    .map_err(into_io_error)?;
                core.run(completer_rx).map_err(into_io_error)
            })?;

        let (context, completer) = init_rx.recv().map_err(into_io_error)??;
        Ok(Gossiper {
            context: context,
            loop_thread: Some((completer, thread)),
        })
    }

    pub fn join(&self, seeds: &[SocketAddr]) {
        self.context.lock().unwrap().join(seeds)
    }

    pub fn node_count(&self) -> usize {
        self.context.lock().unwrap().nodes.len() + 1
    }

    pub fn alive_count(&self) -> usize {
        self.context
            .lock()
            .unwrap()
            .nodes
            .values()
            .filter(|n| n.status != NodeStatus::Dead)
            .count() + 1
    }
}

impl<T: Metadata> Drop for Gossiper<T> {
    fn drop(&mut self) {
        if let Some((completer, thread)) = self.loop_thread.take() {
            let _ = completer.send(());
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;
    use std::{time, thread};

    fn test_converge(n: usize) -> Vec<Gossiper<()>> {
        let _ = env_logger::try_init();
        let g: Vec<_> = (0..n)
            .map(|i| {
                Gossiper::new(format!("0.0.0.0:{}", 9000 + i).parse().unwrap(), ()).unwrap()
            })
            .collect();
        let start = Instant::now();
        for (i, g0) in (&g[1..]).iter().enumerate() {
            g0.join(&[format!("0.0.0.0:{}", 9000 + i).parse().unwrap()]);
        }
        for _ in 0..(n * 1000) {
            if g.iter().all(|g| g.alive_count() == n) {
                break;
            }
            thread::sleep(time::Duration::from_millis(1));
        }
        warn!("{:?} has passed", Instant::now() - start);
        assert!(
            g.iter().all(|g| g.alive_count() == n),
            "{} {:?}",
            n,
            g.iter().map(|g| g.alive_count()).collect::<Vec<_>>()
        );
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
        let _ = env_logger::try_init();
        let mut g = test_converge(n);
        g.pop();
        let start = Instant::now();
        for _ in 0..(n * 2000) {
            if g.iter().all(|g| g.alive_count() == n - 1) {
                break;
            }
            thread::sleep(time::Duration::from_millis(1));
        }
        warn!("{:?} has passed", Instant::now() - start);
        assert!(
            g.iter().all(|g| g.alive_count() == n - 1),
            "{} {:?}",
            n - 1,
            g.iter().map(|g| g.alive_count()).collect::<Vec<_>>()
        );
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
