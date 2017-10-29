use std::time::{Duration, Instant};
use std::collections::{hash_set, HashSet};
use vnode::VNodeState;
use fabric::*;
use version_vector::*;
use cubes::Cube;
use database::*;
use inflightmap::InFlightMap;
use bincode;
use utils::{IdHasherBuilder, split_u64};
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

type IteratorFn = Box<FnMut(&VNodeState) -> Result<Option<(Bytes, Cube)>, ()> + Send>;

type InFlightSyncMsgMap = InFlightMap<u64, (Bytes, Cube), Instant, IdHasherBuilder>;

struct SyncKeysIterator {
    dots_delta: BitmappedVersionVectorDelta,
    keys: hash_set::IntoIter<Bytes>,
}

// TODO: Refactor into trait objects
// trait Synchronization { fn on_.., .. }
// new_sync_sender -> Box<Synchronization>
pub enum Synchronization {
    SyncSender {
        // bvv in peer at the time of sync start
        clocks_in_peer: BitmappedVersionVector,
        // partial copy of the local bvv at the time of sync start
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
        }
    }

    fn next(&mut self, state: &VNodeState) -> Result<Option<Bytes>, ()> {
        loop {
            if let Some(key) = self.keys.next() {
                return Ok(Some(key));
            }
            // fetch log in batches of ~1_000 keys
            let hint_size = self.dots_delta.size_hint().0.min(1_000);
            let mut keys = HashSet::with_capacity(hint_size);
            // consider up to 90% of the actual capacity as an alternative limit
            let limit = (keys.capacity() * 9 / 10).max(1_000);
            for (n, v) in self.dots_delta.by_ref() {
                let key = state
                    .storage
                    .log_get((n, v), |x| Bytes::from(x))
                    .map_err(|_| ())?;
                if let Some(key) = key {
                    keys.insert(key);
                    if keys.len() >= limit {
                        break;
                    }
                } else {
                    warn!("Can't find log key for ({}, {})", n, v);
                }
            }
            if keys.is_empty() {
                return Ok(None);
            }
            debug!("Sync will send key batch with {:?} keys", keys.len());
            self.keys = keys.into_iter();
        }
    }
}

use self::Synchronization::*;

impl Synchronization {
    pub fn new_bootstrap_receiver(
        _db: &Database,
        _state: &mut VNodeState,
        peer: NodeId,
        cookie: Cookie,
    ) -> Self {
        BootstrapReceiver {
            cookie: cookie,
            peer: peer,
            recv_count: 0,
            last_recv: Instant::now(),
            last_send: Instant::now(),
        }
    }

    pub fn new_bootstrap_sender(
        _db: &Database,
        state: &mut VNodeState,
        peer: NodeId,
        msg: MsgSyncStart,
    ) -> Self {
        let mut storage_iterator = state.storage.iterator();
        let iterator_fn: IteratorFn = Box::new(move |_| {
            let next = storage_iterator
                .iter()
                .map(|(k, v)| {
                    let cube = bincode::deserialize::<Cube>(v).map_err(|_| ())?;
                    Ok((Bytes::from(k), cube))
                })
                .next();

            match next {
                Some(Ok(r)) => Ok(Some(r)),
                None => Ok(None),
                Some(Err(e)) => Err(e),
            }
        });

        BootstrapSender {
            cookie: msg.cookie,
            clocks_snapshot: state.clocks.clone(),
            iterator: iterator_fn,
            inflight: InFlightMap::new(),
            peer: peer,
            count: 0,
            last_recv: Instant::now(),
            last_send: Instant::now(),
        }
    }

    pub fn new_sync_receiver(
        _db: &Database,
        state: &mut VNodeState,
        peer: NodeId,
        cookie: Cookie,
    ) -> Self {
        SyncReceiver {
            clocks_in_peer: state.clocks.clone(),
            peer: peer,
            cookie: cookie,
            recv_count: 0,
            last_recv: Instant::now(),
            last_send: Instant::now(),
        }
    }

    pub fn new_sync_sender(
        db: &Database,
        state: &mut VNodeState,
        peer: NodeId,
        msg: MsgSyncStart,
    ) -> Self {
        let MsgSyncStart {
            target,
            cookie,
            clocks_in_peer,
            ..
        } = msg;
        assert_eq!(target, Some(db.dht.node()));

        debug!(
            "Creating SyncSender {:?} from {:?} to {:?}",
            cookie,
            state.clocks,
            clocks_in_peer
        );

        let dots_delta = state.clocks.delta(&clocks_in_peer);
        debug!("Delta from {:?} to {:?}", state.clocks, clocks_in_peer);

        let mut sync_keys = SyncKeysIterator::new(dots_delta);
        let iterator_fn: IteratorFn =
            Box::new(move |state| if let Some(key) = sync_keys.next(state)? {
                let cube = state.storage_get(&key)?;
                Ok(Some((key, cube)))
            } else {
                Ok(None)
            });

        // Only send the part of the bvv corresponding to things that this node coordinated.
        // Even if the dots are registered in the bvv,
	// there's no guarantee that the dot->key log had the dot.
        let physical_id = split_u64(db.dht.node()).0;
        let clocks_snapshot = state.clocks.clone_if(|i| split_u64(i).0 == physical_id);

        SyncSender {
            clocks_in_peer: clocks_in_peer,
            clocks_snapshot: clocks_snapshot,
            iterator: iterator_fn,
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
            SyncReceiver {
                cookie,
                peer,
                ref mut last_send,
                ref clocks_in_peer,
                ..
            } => {
                *last_send = Instant::now();
                (peer, cookie, Some(peer), clocks_in_peer.clone())
            }
            BootstrapReceiver {
                peer,
                cookie,
                ref mut last_send,
                ..
            } => {
                *last_send = Instant::now();
                (peer, cookie, None, BitmappedVersionVector::new())
            }
            _ => unreachable!(),
        };

        info!("Sending start for {:?}", cookie);
        db.fabric
            .send_msg(
                peer,
                MsgSyncStart {
                    cookie: cookie,
                    vnode: state.num(),
                    clocks_in_peer: clocks_in_peer,
                    target: target,
                },
            )
            .into()
    }

    // Sending Errors always result in Error
    fn send_error_fin(
        &mut self,
        db: &Database,
        state: &mut VNodeState,
        error: FabricError,
    ) -> SyncResult {
        match *self {
            SyncReceiver {
                peer,
                cookie,
                ref mut last_send,
                ..
            } |
            BootstrapReceiver {
                peer,
                cookie,
                ref mut last_send,
                ..
            } |
            SyncSender {
                peer,
                cookie,
                ref mut last_send,
                ..
            } |
            BootstrapSender {
                peer,
                cookie,
                ref mut last_send,
                ..
            } => {
                *last_send = Instant::now();
                let _ = db.fabric.send_msg(
                    peer,
                    MsgSyncFin {
                        cookie: cookie,
                        vnode: state.num(),
                        result: Err(error),
                    },
                );
                SyncResult::Error
            }
        }
    }

    // Senders wait for the Receivers to reply => Continue
    // unless there's no route the peer => Error
    fn send_sender_success_fin(&mut self, db: &Database, state: &mut VNodeState) -> SyncResult {
        match *self {
            SyncSender {
                peer,
                cookie,
                ref clocks_snapshot,
                ref mut last_send,
                ..
            } |
            BootstrapSender {
                peer,
                cookie,
                ref clocks_snapshot,
                ref mut last_send,
                ..
            } => {
                *last_send = Instant::now();
                db.fabric
                    .send_msg(
                        peer,
                        MsgSyncFin {
                            cookie: cookie,
                            vnode: state.num(),
                            result: Ok(clocks_snapshot.clone()),
                        },
                    )
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
            SyncSender {
                peer,
                cookie,
                ref mut iterator,
                ref mut count,
                ref mut inflight,
                ref mut last_send,
                ..
            } |
            BootstrapSender {
                peer,
                cookie,
                ref mut iterator,
                ref mut count,
                ref mut inflight,
                ref mut last_send,
                ..
            } => {
                while let Some((seq, &(ref k, ref v))) = inflight.touch_expired(now, timeout) {
                    debug!("resending seq {} for sync/bootstrap {:?}", seq, cookie);
                    let _ = stry!(db.fabric.send_msg(
                        peer,
                        MsgSyncSend {
                            cookie: cookie,
                            vnode: state.num(),
                            seq: seq,
                            key: k.clone(),
                            value: v.clone(),
                        },
                    ));
                    metrics::SYNC_RESEND.mark(1);
                }
                let mut error = false;
                while inflight.len() < db.config.sync_msg_inflight as usize {
                    match iterator(state) {
                        Ok(Some((k, v))) => {
                            let _ = stry!(db.fabric.send_msg(
                                peer,
                                MsgSyncSend {
                                    cookie: cookie,
                                    vnode: state.num(),
                                    seq: *count,
                                    key: k.clone(),
                                    value: v.clone(),
                                },
                            ));
                            inflight.insert(*count, (k, v), timeout);
                            *count += 1;
                            *last_send = now;
                            metrics::SYNC_SEND.mark(1);
                            continue;
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(_) => {
                            error = true;
                            break;
                        }
                    }
                }
                (error, inflight.is_empty())
            }
            _ => unreachable!(),
        };

        if error {
            self.send_error_fin(db, state, FabricError::SyncInterrupted)
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
            BootstrapReceiver { .. } | SyncReceiver { .. } => {
                let _ = self.send_error_fin(db, state, FabricError::BadVNodeStatus);
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
            SyncSender {
                last_recv, cookie, ..
            } |
            BootstrapSender {
                last_recv, cookie, ..
            } => if last_recv.elapsed() > Duration::from_millis(db.config.sync_timeout as _) {
                warn!("sync/boostrap sender timed out {:?}", cookie);
                SyncResult::Error
            } else {
                self.send_next(db, state)
            },
            SyncReceiver {
                last_recv,
                recv_count,
                last_send,
                cookie,
                ..
            } |
            BootstrapReceiver {
                last_recv,
                recv_count,
                last_send,
                cookie,
                ..
            } => if last_recv.elapsed() > Duration::from_millis(db.config.sync_timeout as _) {
                warn!("sync/boostrap receiver timed out {:?}", cookie);
                SyncResult::Error
            } else if recv_count == 0
                && last_send.elapsed() > Duration::from_millis(db.config.sync_msg_timeout as _)
            {
                self.send_start(db, state)
            } else {
                SyncResult::Continue
            },
        }
    }

    // called by vnode as soon as the sync is registered (after creation)
    pub fn on_start(&mut self, db: &Database, state: &mut VNodeState) {
        let _ = match *self {
            SyncReceiver { .. } | BootstrapReceiver { .. } => self.send_start(db, state),
            SyncSender { .. } | BootstrapSender { .. } => self.send_next(db, state),
        };
    }

    pub fn on_msg_fin(
        &mut self,
        db: &Database,
        state: &mut VNodeState,
        msg: MsgSyncFin,
    ) -> SyncResult {
        match *self {
            SyncReceiver { peer, .. } | BootstrapReceiver { peer, .. } => {
                if msg.result.is_ok() {
                    state.clocks.merge(msg.result.as_ref().unwrap());
                    state.save(db, false);
                    // send it back as a form of ack-ack
                    let _ = db.fabric.send_msg(peer, msg);
                    SyncResult::Done
                } else if msg.result.err() == Some(FabricError::NotReady) {
                    SyncResult::Continue
                } else {
                    SyncResult::Error
                }
            }
            SyncSender { .. } | BootstrapSender { .. } => {
                // Senders are always Done on SyncFin messages
                SyncResult::Done
            }
        }
    }

    pub fn on_msg_send(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncSend) {
        match *self {
            SyncReceiver {
                peer,
                ref mut recv_count,
                ref mut last_recv,
                ref mut last_send,
                ..
            } |
            BootstrapReceiver {
                peer,
                ref mut recv_count,
                ref mut last_recv,
                ref mut last_send,
                ..
            } => {
                // TODO: what to do with errors here?
                state
                    .storage_set_remote(db, &msg.key, msg.value, false)
                    .unwrap();

                let _ = db.fabric.send_msg(
                    peer,
                    MsgSyncAck {
                        cookie: msg.cookie,
                        vnode: state.num(),
                        seq: msg.seq,
                    },
                );

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
            SyncSender {
                ref mut inflight,
                ref mut last_recv,
                ..
            } |
            BootstrapSender {
                ref mut inflight,
                ref mut last_recv,
                ..
            } => {
                inflight.remove(&msg.seq);
                *last_recv = Instant::now();
            }
            _ => unreachable!(),
        }
        let _ = self.send_next(db, state);
    }

    pub fn direction(&self) -> SyncDirection {
        match *self {
            BootstrapReceiver { .. } | SyncReceiver { .. } => SyncDirection::Incomming,
            BootstrapSender { .. } | SyncSender { .. } => SyncDirection::Outgoing,
        }
    }
}
