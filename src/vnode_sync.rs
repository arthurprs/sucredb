use std::time::{Instant, Duration};
use std::collections::{hash_set, HashSet};
use vnode::VNodeState;
use fabric::*;
use version_vector::*;
use database::*;
use inflightmap::InFlightMap;
use bincode;
use utils::IdHasherBuilder;
use metrics::{self, Meter};
use bytes::Bytes;

#[derive(Debug, Copy, Clone, PartialEq)]
#[must_use]
pub enum SyncResult {
    Continue,
    Done,
    Error,
}

impl<S, E> From<Result<S, E>> for SyncResult {
    fn from(result: Result<S, E>) -> Self {
        if result.is_ok() {
            SyncResult::Continue
        } else {
            SyncResult::Error
        }
    }
}

macro_rules! stry {
    ($expr: expr) => {{
        let conv = $expr.into();
        if let SyncResult::Continue = conv {
            conv
        } else {
            return conv;
        }
    }};
}

#[derive(Debug)]
pub enum SyncDirection {
    Incomming,
    Outgoing,
}

// TODO: take &mut buffers instead of returning them
type IteratorFn =
    Box<FnMut(&VNodeState) -> Result<(Bytes, DottedCausalContainer<Bytes>), Result<(), ()>> + Send>;
type InFlightSyncMsgMap = InFlightMap<u64,
                                      (Bytes, DottedCausalContainer<Bytes>),
                                      Instant,
                                      IdHasherBuilder>;

struct SyncKeysIterator {
    dots_delta: BitmappedVersionVectorDelta,
    keys: hash_set::IntoIter<Bytes>,
    broken: bool,
}

pub enum Synchronization {
    SyncSender {
        // bvv in peer at the time of sync start
        clocks_in_peer: BitmappedVersionVector,
        // local bvv at the time of sync start
        clocks_snapshot: BitmappedVersionVector,
        iterator: IteratorFn,
        // TODO: only store keys as resends should be rare
        inflight: InFlightSyncMsgMap,
        cookie: Cookie,
        peer: NodeId,
        // count of sent keys (includes inflight)
        count: u64,
        last_recv: Instant,
        last_send: Instant,
    },
    SyncReceiver {
        // local bvv at the time of sync start
        clocks_in_peer: BitmappedVersionVector,
        cookie: Cookie,
        peer: NodeId,
        // aprox count of received keys (includes dups)
        recv_count: u64,
        last_recv: Instant,
        last_send: Instant,
    },
    BootstrapSender {
        clocks_snapshot: BitmappedVersionVector,
        iterator: IteratorFn,
        inflight: InFlightSyncMsgMap,
        cookie: Cookie,
        peer: NodeId,
        // count of sent keys (includes inflight)
        count: u64,
        last_recv: Instant,
        last_send: Instant,
    },
    BootstrapReceiver {
        cookie: Cookie,
        peer: NodeId,
        // aprox count of received keys (includes dups)
        recv_count: u64,
        last_recv: Instant,
        last_send: Instant,
    },
}

impl SyncKeysIterator {
    fn new(dots_delta: BitmappedVersionVectorDelta) -> Self {
        SyncKeysIterator {
            dots_delta: dots_delta,
            keys: HashSet::new().into_iter(),
            broken: false,
        }
    }

    fn next(&mut self, state: &VNodeState) -> Result<Bytes, Result<(), ()>> {
        loop {
            if let Some(key) = self.keys.next() {
                return Ok(key);
            }
            if self.broken {
                return Err(Err(()));
            }
            // fetch log in 10_000 key batches
            // reserve a dedup hashmap with half of iter len + 1
            // this way it will only resize once at the worst case
            let mut keys = HashSet::with_capacity(self.dots_delta.size_hint().0 / 2 + 1);
            for (n, v) in self.dots_delta.by_ref() {
                if let Some(key) = state.logs.get(&n).and_then(|log| log.get(v)).cloned() {
                    keys.insert(key);
                    if keys.len() >= 10_000 {
                        break;
                    }
                } else {
                    warn!("Can't find key for ({}, {}), stopping sync iterator", n, v);
                    self.broken = true;
                    return Err(Err(()));
                }
            }
            debug_assert!(!self.broken);
            if keys.is_empty() {
                return Err(Ok(()));
            }
            debug!("sync will send key batch {:?}", keys);
            self.keys = keys.into_iter();
        }
    }
}

use self::Synchronization::*;

impl Synchronization {
    pub fn new_bootstrap_receiver(_db: &Database, _state: &mut VNodeState, peer: NodeId,
                                  cookie: Cookie)
                                  -> Self {
        BootstrapReceiver {
            cookie: cookie,
            peer: peer,
            recv_count: 0,
            last_recv: Instant::now(),
            last_send: Instant::now(),
        }
    }

    pub fn new_bootstrap_sender(_db: &Database, state: &mut VNodeState, peer: NodeId,
                                msg: MsgSyncStart)
                                -> Self {
        let mut storage_iterator = state.storage.iterator();
        let iterator: IteratorFn = Box::new(move |_| {
            storage_iterator.iter()
                .map(|(k, v)| {
                         (k.into(), bincode::deserialize::<DottedCausalContainer<_>>(&v).unwrap())
                     })
                .next()
                .ok_or(Ok(()))
        });

        BootstrapSender {
            cookie: msg.cookie,
            clocks_snapshot: state.clocks.clone(),
            iterator: iterator,
            inflight: InFlightMap::new(),
            peer: peer,
            count: 0,
            last_recv: Instant::now(),
            last_send: Instant::now(),
        }
    }

    pub fn new_sync_receiver(_db: &Database, state: &mut VNodeState, peer: NodeId, cookie: Cookie)
                             -> Self {
        SyncReceiver {
            clocks_in_peer: state.clocks.clone(),
            peer: peer,
            cookie: cookie,
            recv_count: 0,
            last_recv: Instant::now(),
            last_send: Instant::now(),
        }
    }

    pub fn new_sync_sender(db: &Database, state: &mut VNodeState, peer: NodeId, msg: MsgSyncStart)
                           -> Self {
        let MsgSyncStart { target, cookie, clocks_in_peer, .. } = msg;
        assert_eq!(target, Some(db.dht.node()));

        debug!("Creating SyncSender {:?} from {:?} to {:?}",
               cookie,
               state.clocks,
               clocks_in_peer);

        let clocks_snapshot = state.clocks.clone();
        let clocks_snapshot2 = state.clocks.clone();
        let clocks_in_peer2 = clocks_in_peer.clone();

        let dots_delta = state.clocks.delta(&clocks_in_peer);
        debug!("Delta from {:?} to {:?}", state.clocks, clocks_in_peer);
        let log_uptodate =
            dots_delta.min_versions().iter().all(|&(n, v)| {
                state.logs.get(&n).and_then(|log| log.min_version()).unwrap_or(0) <= v
            });

        let iterator: IteratorFn = if log_uptodate {
            let mut sync_keys = SyncKeysIterator::new(dots_delta);
            Box::new(move |state| {
                loop {
                    match sync_keys.next(state) {
                        Ok(k) => {
                            if let Some(v) = state.storage.get(&k, |v| Bytes::from(v)) {
                                let mut dcc: DottedCausalContainer<_> = bincode::deserialize(&v)
                                    .unwrap();
                                // TODO: fill should be done in the remote?
                                dcc.fill(&clocks_snapshot);
                                return Ok((k, dcc));
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
            })
        } else {
            warn!("SyncSender {:?} using a scan", cookie);
            let mut storage_iterator = state.storage.iterator();
            Box::new(move |_| {
                storage_iterator.iter()
                    .filter_map(|(k, v)| {
                        let mut dcc = bincode::deserialize::<DottedCausalContainer<_>>(&v).unwrap();
                        if !dcc.contained(&clocks_in_peer) {
                            // TODO: fill should be done in the remote?
                            dcc.fill(&clocks_snapshot);
                            Some((k.into(), dcc))
                        } else {
                            None
                        }
                    })
                    .next()
                    .ok_or(Ok(()))
            })
        };

        SyncSender {
            clocks_in_peer: clocks_in_peer2,
            clocks_snapshot: clocks_snapshot2,
            iterator: iterator,
            inflight: InFlightMap::new(),
            cookie: cookie,
            peer: peer,
            count: 0,
            last_recv: Instant::now(),
            last_send: Instant::now(),
        }
    }

    // send SyncStart message, only valid for Receivers
    fn send_start(&mut self, db: &Database, state: &mut VNodeState) -> SyncResult {
        let (peer, cookie, target, clocks_in_peer) = match *self {
            SyncReceiver { cookie, peer, ref mut last_send, ref clocks_in_peer, .. } => {
                *last_send = Instant::now();
                (peer, cookie, Some(peer), clocks_in_peer.clone())
            }
            BootstrapReceiver { peer, cookie, ref mut last_send, .. } => {
                *last_send = Instant::now();
                (peer, cookie, None, BitmappedVersionVector::new())
            }
            _ => unreachable!(),
        };

        info!("Sending start for {:?}", cookie);
        db.fabric
            .send_msg(peer,
                      MsgSyncStart {
                          cookie: cookie,
                          vnode: state.num(),
                          clocks_in_peer: clocks_in_peer,
                          target: target,
                      })
            .into()
    }

    // Sending Errors always result in Error
    fn send_error_fin(&mut self, db: &Database, state: &mut VNodeState, e: FabricMsgError)
                      -> SyncResult {
        match *self {
            SyncReceiver { peer, cookie, ref mut last_send, .. } |
            BootstrapReceiver { peer, cookie, ref mut last_send, .. } |
            SyncSender { peer, cookie, ref mut last_send, .. } |
            BootstrapSender { peer, cookie, ref mut last_send, .. } => {
                *last_send = Instant::now();
                let _ = db.fabric.send_msg(peer,
                                           MsgSyncFin {
                                               cookie: cookie,
                                               vnode: state.num(),
                                               result: Err(e),
                                           });
                SyncResult::Error
            }
        }
    }

    // Senders wait for the Receivers to reply => Continue
    // unless there's no route the peer => Error
    fn send_sender_success_fin(&mut self, db: &Database, state: &mut VNodeState) -> SyncResult {
        match *self {
            SyncSender { peer, cookie, ref clocks_snapshot, ref mut last_send, .. } |
            BootstrapSender { peer, cookie, ref clocks_snapshot, ref mut last_send, .. } => {
                *last_send = Instant::now();
                db.fabric
                    .send_msg(peer,
                              MsgSyncFin {
                                  cookie: cookie,
                                  vnode: state.num(),
                                  result: Ok(clocks_snapshot.clone()),
                              })
                    .into()
            }
            _ => unreachable!(),
        }
    }

    // send (possibly multiple) SyncSend messages and eventual SyncFin
    // (also takes care of expired SyncSend)
    fn send_next(&mut self, db: &Database, state: &mut VNodeState) -> SyncResult {
        let now = Instant::now();
        let timeout = now + Duration::from_millis(db.config.sync_msg_timeout as _);
        let (error, inflight_empty) = match *self {
            SyncSender { peer,
                         cookie,
                         ref mut iterator,
                         ref mut count,
                         ref mut inflight,
                         ref mut last_send,
                         .. } |
            BootstrapSender { peer,
                              cookie,
                              ref mut iterator,
                              ref mut count,
                              ref mut inflight,
                              ref mut last_send,
                              .. } => {
                while let Some((seq, &(ref k, ref dcc))) = inflight.touch_expired(now, timeout) {
                    debug!("resending seq {} for sync/bootstrap {:?}", seq, cookie);
                    let _ = stry!(db.fabric.send_msg(peer,
                                                     MsgSyncSend {
                                                         cookie: cookie,
                                                         vnode: state.num(),
                                                         seq: seq,
                                                         key: k.clone(),
                                                         container: dcc.clone(),
                                                     }));
                    metrics::SYNC_RESEND.mark(1);
                }
                let mut error = false;
                while inflight.len() < db.config.sync_msg_inflight as usize {
                    match iterator(&state) {
                        Ok((k, dcc)) => {
                            let _ = stry!(db.fabric.send_msg(peer,
                                                             MsgSyncSend {
                                                                 cookie: cookie,
                                                                 vnode: state.num(),
                                                                 seq: *count,
                                                                 key: k.clone(),
                                                                 container: dcc.clone(),
                                                             }));
                            inflight.insert(*count, (k, dcc), timeout);
                            *count += 1;
                            *last_send = now;
                            metrics::SYNC_SEND.mark(1);
                            continue;
                        }
                        Err(e) => {
                            error = e.is_err();
                            break;
                        }
                    }
                }
                (error, inflight.is_empty())
            }
            _ => unreachable!(),
        };

        if error {
            self.send_error_fin(db, state, FabricMsgError::SyncInterrupted)
        } else if inflight_empty {
            // do not trottle success fin as we don't know if last_send
            // was set by MsgSend or MsgFin
            self.send_sender_success_fin(db, state)
        } else {
            SyncResult::Continue
        }
    }

    // called by vnode when node is transition to an incompatible state
    // only valid for Receivers right now
    pub fn on_cancel(&mut self, db: &Database, state: &mut VNodeState) {
        match *self {
            BootstrapReceiver { .. } |
            SyncReceiver { .. } => {
                let _ = self.send_error_fin(db, state, FabricMsgError::BadVNodeStatus);
            }
            _ => unreachable!(),
        }
    }

    // called by vnode as soon as the sync is unregistered
    pub fn on_remove(self, db: &Database, state: &mut VNodeState) {
        match self {
            SyncReceiver { peer, .. } => {
                state.sync_nodes.remove(&peer);
            }
            _ => (),
        }

        db.signal_sync_end(self.direction());
    }

    pub fn on_tick(&mut self, db: &Database, state: &mut VNodeState) -> SyncResult {
        match *self {
            SyncSender { last_recv, cookie, .. } |
            BootstrapSender { last_recv, cookie, .. } => {
                if last_recv.elapsed() > Duration::from_millis(db.config.sync_timeout as _) {
                    warn!("sync/boostrap sender timed out {:?}", cookie);
                    SyncResult::Error
                } else {
                    self.send_next(db, state)
                }
            }
            SyncReceiver { last_recv, recv_count, last_send, cookie, .. } |
            BootstrapReceiver { last_recv, recv_count, last_send, cookie, .. } => {
                if last_recv.elapsed() > Duration::from_millis(db.config.sync_timeout as _) {
                    warn!("sync/boostrap receiver timed out {:?}", cookie);
                    SyncResult::Error
                } else if recv_count == 0 &&
                          last_send.elapsed() >
                          Duration::from_millis(db.config.sync_msg_timeout as _) {
                    self.send_start(db, state)
                } else {
                    SyncResult::Continue
                }
            }
        }
    }

    // called by vnode as soon as the sync is registered (after creation)
    pub fn on_start(&mut self, db: &Database, state: &mut VNodeState) {
        let _ = match *self {
            SyncReceiver { .. } |
            BootstrapReceiver { .. } => self.send_start(db, state),
            SyncSender { .. } |
            BootstrapSender { .. } => self.send_next(db, state),
        };
    }

    pub fn on_msg_fin(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncFin)
                      -> SyncResult {
        match *self {
            SyncReceiver { peer, .. } => {
                if msg.result.is_ok() {
                    state.clocks.join(msg.result.as_ref().unwrap());
                    state.save(db, false);
                    state.storage.sync();
                    // send it back as a form of ack-ack
                    let _ = db.fabric.send_msg(peer, msg);
                    SyncResult::Done
                } else if msg.result.err() == Some(FabricMsgError::NotReady) {
                    SyncResult::Continue
                } else {
                    SyncResult::Error
                }
            }
            BootstrapReceiver { peer, .. } => {
                if msg.result.is_ok() {
                    state.clocks.merge(msg.result.as_ref().unwrap());
                    state.save(db, false);
                    state.storage.sync();
                    // send it back as a form of ack-ack
                    let _ = db.fabric.send_msg(peer, msg);
                    SyncResult::Done
                } else if msg.result.err() == Some(FabricMsgError::NotReady) {
                    SyncResult::Continue
                } else {
                    SyncResult::Error
                }
            }
            // Senders are always Done on SyncFin messages
            SyncSender { .. } |
            BootstrapSender { .. } => SyncResult::Done,
        }
    }

    pub fn on_msg_send(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncSend) {
        match *self {
            SyncReceiver { peer, ref mut recv_count, ref mut last_recv, ref mut last_send, .. } |
            BootstrapReceiver { peer,
                                ref mut recv_count,
                                ref mut last_recv,
                                ref mut last_send,
                                .. } => {
                // FIXME: these can create a big region of 0s in the start of bitmap
                //        until the bvv join on Fin
                state.storage_set_remote(db, &msg.key, msg.container);

                let _ = db.fabric.send_msg(peer,
                                           MsgSyncAck {
                                               cookie: msg.cookie,
                                               vnode: state.num(),
                                               seq: msg.seq,
                                           });

                *recv_count += 1;
                let now = Instant::now();
                *last_recv = now;
                *last_send = now;
                metrics::SYNC_RECV.mark(1);
            }
            _ => unreachable!(),
        }
    }

    pub fn on_msg_ack(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncAck) {
        match *self {
            SyncSender { ref mut inflight, ref mut last_recv, .. } |
            BootstrapSender { ref mut inflight, ref mut last_recv, .. } => {
                inflight.remove(&msg.seq);
                *last_recv = Instant::now();
            }
            _ => unreachable!(),
        }
        let _ = self.send_next(db, state);
    }

    pub fn direction(&self) -> SyncDirection {
        match *self {
            BootstrapReceiver { .. } |
            SyncReceiver { .. } => SyncDirection::Incomming,
            BootstrapSender { .. } |
            SyncSender { .. } => SyncDirection::Outgoing,
        }
    }
}
