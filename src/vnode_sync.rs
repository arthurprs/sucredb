use std::collections::HashSet;
use vnode::{VNodeState, VNodeStatus};
use fabric::*;
use version_vector::*;
use database::*;
use storage::Storage;
use inflightmap::InFlightMap;
use std::time::{Instant, Duration};
use bincode::serde as bincode_serde;
use utils::IdHasherBuilder;

const SYNC_INFLIGHT_MAX: usize = 5;
const SYNC_INFLIGHT_TIMEOUT_MS: u64 = 2000;
const SYNC_TIMEOUT_MS: u64 = SYNC_INFLIGHT_TIMEOUT_MS * 51 / 10;

#[derive(Debug)]
pub enum SyncResult {
    // it's done, just remove entry
    Done,
    // do nothing
    Continue,
    // remove then retry bootstrap
    RetryBoostrap,
}

// TODO: take mut buffers instead of returning them
type IteratorFn = Box<FnMut(&Storage) -> Option<(Vec<u8>, DottedCausalContainer<Vec<u8>>)> + Send>;
type InFlightSyncMsgMap = InFlightMap<u64,
                                      (Vec<u8>, DottedCausalContainer<Vec<u8>>),
                                      Instant,
                                      IdHasherBuilder>;

pub enum Synchronization {
    SyncSender {
        clocks_in_peer: BitmappedVersionVector,
        clocks_snapshot: BitmappedVersionVector,
        iterator: IteratorFn,
        inflight: InFlightSyncMsgMap,
        cookie: Cookie,
        peer: NodeId,
        target: NodeId,
        count: u64,
        last_receive: Instant,
    },
    SyncReceiver {
        clocks_in_peer: BitmappedVersionVector,
        cookie: Cookie,
        peer: NodeId,
        target: NodeId,
        count: u64,
        last_receive: Instant,
        starts_sent: u32,
    },
    BootstrapSender {
        clocks_snapshot: BitmappedVersionVector,
        iterator: IteratorFn,
        inflight: InFlightSyncMsgMap,
        cookie: Cookie,
        peer: NodeId,
        count: u64,
        last_receive: Instant,
    },
    BootstrapReceiver {
        cookie: Cookie,
        peer: NodeId,
        count: u64,
        last_receive: Instant,
        starts_sent: u32,
    },
}

impl Synchronization {
    pub fn new_sync_receiver(db: &Database, state: &mut VNodeState, peer: NodeId, target: NodeId,
                             cookie: Cookie)
                             -> Self {
        let mut sync = Synchronization::SyncReceiver {
            clocks_in_peer: state.clocks.clone(),
            peer: peer,
            cookie: cookie,
            count: 0,
            target: target,
            last_receive: Instant::now(),
            starts_sent: 0,
        };
        sync.send_start(db, state);
        sync
    }
    pub fn new_bootstrap_receiver(db: &Database, state: &mut VNodeState, peer: NodeId,
                                  cookie: Cookie)
                                  -> Self {
        let mut sync = Synchronization::BootstrapReceiver {
            cookie: cookie,
            peer: peer,
            count: 0,
            last_receive: Instant::now(),
            starts_sent: 0,
        };
        sync.send_start(db, state);
        sync
    }

    pub fn new_bootstrap_sender(_db: &Database, state: &mut VNodeState, peer: NodeId,
                                msg: MsgSyncStart)
                                -> Self {
        let cookie = msg.cookie;
        let mut storage_iterator = state.storage.iterator();
        let mut iter = (0..)
            .map(move |_| {
                storage_iterator.iter().next().map(|(k, v)| {
                    (k.into(), bincode_serde::deserialize::<DottedCausalContainer<_>>(&v).unwrap())
                })
            })
            .take_while(|i| i.is_some())
            .filter_map(|i| i);

        Synchronization::BootstrapSender {
            cookie: cookie,
            clocks_snapshot: state.clocks.clone(),
            iterator: Box::new(move |_| iter.next()),
            inflight: InFlightMap::new(),
            peer: peer,
            count: 0,
            last_receive: Instant::now(),
        }
    }

    pub fn new_sync_sender(db: &Database, state: &mut VNodeState, peer: NodeId, msg: MsgSyncStart)
                           -> Self {
        let iterator: IteratorFn;
        let mut clocks_snapshot: BitmappedVersionVector;

        let clocks = state.clocks.clone();
        let target = msg.target.unwrap();
        let clocks_in_peer = msg.clocks_in_peer.clone();
        let clock_in_peer = clocks_in_peer.get(target).cloned().unwrap_or_default();

        debug!("Creating SyncSender {:?} for target {:?} from {:?} to {:?}",
               msg.cookie,
               target,
               clocks_in_peer,
               state.clocks);

        let target_log_base = if target == db.dht.node() {
            state.log.clone().min_version()
        } else {
            state.peers.get(&target).cloned().unwrap_or_default().min_version()
        };
        if target_log_base.is_some() && target_log_base.unwrap() <= clock_in_peer.base() {
            clocks_snapshot = BitmappedVersionVector::new();
            let mut keys: HashSet<Vec<u8>> = HashSet::new();
            let empty_bv = BitmappedVersion::new(0, 0);

            for (&node, log) in state.peers
                .iter()
                .chain(::std::iter::once((&db.dht.node(), &state.log))) {
                let n_bv = clocks.get(node).unwrap_or(&empty_bv);
                let p_bv = clocks_in_peer.get(node).unwrap_or(&empty_bv);
                clocks_snapshot.add_bv(node, n_bv);
                let dots_delta = n_bv.delta(p_bv);
                keys.reserve(dots_delta.size_hint().0);
                for dot in dots_delta {
                    if let Some(k) = log.get(dot) {
                        keys.insert(k.into());
                    }
                }
            }

            let mut keys = keys.into_iter();
            iterator = Box::new(move |storage| {
                keys.by_ref()
                    .filter_map(|k| {
                        storage.get_vec(&k)
                            .map(|v| {
                                let mut dcc: DottedCausalContainer<_> =
                                    bincode_serde::deserialize(&v).unwrap();
                                // TODO: fill should be done in the remote
                                dcc.fill(&clocks);
                                (k, dcc)
                            })
                    })
                    .next()
            });
        } else {
            debug!("SyncSender {:?} using a scan", msg.cookie);
            clocks_snapshot = clocks.clone();
            let mut storage_iterator = state.storage.iterator();
            let mut iter = (0..)
                .map(move |_| {
                    storage_iterator.iter().next().map(|(k, v)| {
                        (k.into(),
                         bincode_serde::deserialize::<DottedCausalContainer<_>>(&v).unwrap())
                    })
                })
                .take_while(|i| i.is_some())
                .filter_map(|i| i)
                .filter_map(move |(k, mut dcc)| {
                    if !dcc.contained(&clocks_in_peer) {
                        // TODO: fill should be done in the remote
                        dcc.fill(&clocks);
                        Some((k, dcc))
                    } else {
                        None
                    }
                });
            iterator = Box::new(move |_| iter.next());
        }

        Synchronization::SyncSender {
            clocks_in_peer: msg.clocks_in_peer,
            clocks_snapshot: clocks_snapshot,
            iterator: iterator,
            inflight: InFlightMap::new(),
            cookie: msg.cookie,
            target: target,
            peer: peer,
            count: 0,
            last_receive: Instant::now(),
        }
    }

    fn send_start(&mut self, db: &Database, state: &mut VNodeState) {
        let (peer, cookie, target, clocks_in_peer) = match *self {
            Synchronization::SyncReceiver { cookie,
                                            peer,
                                            target,
                                            ref mut last_receive,
                                            ref clocks_in_peer,
                                            .. } => {
                // reset last receives
                *last_receive = Instant::now();
                (peer, cookie, Some(target), clocks_in_peer.clone())
            }
            Synchronization::BootstrapReceiver { peer, cookie, ref mut last_receive, .. } => {
                // reset last receives
                *last_receive = Instant::now();
                (peer, cookie, None, BitmappedVersionVector::new())
            }
            _ => unreachable!(),
        };
        db.fabric
            .send_msg(peer,
                      MsgSyncStart {
                          cookie: cookie,
                          vnode: state.num(),
                          clocks_in_peer: clocks_in_peer,
                          target: target,
                      })
            .unwrap();
    }

    fn send_success_fin(&mut self, db: &Database, state: &mut VNodeState) {
        match *self {
            Synchronization::SyncSender { peer, cookie, ref clocks_snapshot, .. } |
            Synchronization::BootstrapSender { peer, cookie, ref clocks_snapshot, .. } => {
                db.fabric
                    .send_msg(peer,
                              MsgSyncFin {
                                  cookie: cookie,
                                  vnode: state.num(),
                                  result: Ok(clocks_snapshot.clone()),
                              })
                    .unwrap();
            }
            _ => unreachable!(),
        };

    }

    fn send_next(&mut self, db: &Database, state: &mut VNodeState) {
        let exausted = match *self {
            Synchronization::SyncSender { peer,
                                          cookie,
                                          ref mut iterator,
                                          ref mut count,
                                          ref mut inflight,
                                          .. } |
            Synchronization::BootstrapSender { peer,
                                               cookie,
                                               ref mut iterator,
                                               ref mut count,
                                               ref mut inflight,
                                               .. } => {
                let now = Instant::now();
                let timeout = now + Duration::from_millis(SYNC_INFLIGHT_TIMEOUT_MS);
                while let Some((seq, &(ref k, ref dcc))) = inflight.touch_expired(now, timeout) {
                    debug!("resending seq {} for sync/bootstrap {:?}", seq, cookie);
                    db.fabric
                        .send_msg(peer,
                                  MsgSyncSend {
                                      cookie: cookie,
                                      vnode: state.num(),
                                      seq: seq,
                                      key: k.clone(),
                                      container: dcc.clone(),
                                  })
                        .unwrap();
                }
                while inflight.len() < SYNC_INFLIGHT_MAX {
                    if let Some((k, dcc)) = iterator(&state.storage) {
                        db.fabric
                            .send_msg(peer,
                                      MsgSyncSend {
                                          cookie: cookie,
                                          vnode: state.num(),
                                          seq: *count,
                                          key: k.clone(),
                                          container: dcc.clone(),
                                      })
                            .unwrap();
                        inflight.insert(*count, (k, dcc), timeout);
                        *count += 1;
                    } else {
                        break;
                    }
                }
                inflight.is_empty()
            }
            _ => unreachable!(),
        };

        if exausted {
            // FIXME: check when we sent the last one
            self.send_success_fin(db, state);
        }
    }

    pub fn on_cancel(&mut self, db: &Database, state: &mut VNodeState) {
        match *self {
            Synchronization::BootstrapReceiver { peer, cookie, .. } |
            Synchronization::SyncReceiver { peer, cookie, .. } => {
                fabric_send_error!(db,
                                   peer,
                                   state.num(),
                                   cookie,
                                   MsgSyncFin,
                                   FabricMsgError::BadVNodeStatus);
            }
            _ => unreachable!(),
        }
    }

    pub fn on_remove(self, db: &Database, state: &mut VNodeState) {
        match self {
            Synchronization::SyncReceiver { peer, target, .. } => {
                if target == peer {
                    state.sync_nodes.remove(&peer);
                } else {
                    assert_eq!(state.status(), VNodeStatus::Recover);
                    state.pending_recoveries -= 1;
                    if state.pending_recoveries == 0 {
                        state.set_status(db, VNodeStatus::Ready);
                    }
                }
            }
            _ => (),
        }
    }

    pub fn on_tick(&mut self, db: &Database, state: &mut VNodeState) -> SyncResult {
        match *self {
            Synchronization::SyncSender { last_receive, cookie, .. } |
            Synchronization::BootstrapSender { last_receive, cookie, .. } => {
                if last_receive.elapsed() > Duration::from_millis(SYNC_TIMEOUT_MS) {
                    warn!("sync/boostrap sender timed out {:?}", cookie);
                    return SyncResult::Done;
                }
                self.send_next(db, state);
            }
            Synchronization::SyncReceiver { last_receive, count, cookie, .. } |
            Synchronization::BootstrapReceiver { last_receive, count, cookie, .. } => {
                if last_receive.elapsed() > Duration::from_millis(SYNC_TIMEOUT_MS) {
                    warn!("sync/boostrap receiver timed out {:?}", cookie);
                    match *self {
                        Synchronization::BootstrapReceiver { .. } => {
                            return SyncResult::RetryBoostrap;
                        }
                        _ => return SyncResult::Done,
                    }
                } else if count == 0 &&
                          last_receive.elapsed() > Duration::from_millis(SYNC_INFLIGHT_TIMEOUT_MS) {
                    self.send_start(db, state);
                }
            }
        }
        SyncResult::Continue
    }

    pub fn on_start(&mut self, db: &Database, state: &mut VNodeState) {
        match *self {
            Synchronization::SyncSender { .. } => self.send_next(db, state),
            Synchronization::BootstrapSender { .. } => self.send_next(db, state),
            _ => unreachable!(),
        }
    }

    pub fn on_fin(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncFin) -> SyncResult {
        match *self {
            Synchronization::SyncReceiver { peer, .. } => {
                if msg.result.is_ok() {
                    state.clocks.join(msg.result.as_ref().unwrap());
                    state.save(db, false);
                    state.storage.sync();
                    // send it back as a form of ack-ack
                    db.fabric.send_msg(peer, msg).unwrap();
                }
            }
            Synchronization::SyncSender { /*ref clock_in_peer, peer,*/ .. } => {
                // TODO: advance to the snapshot base instead? Is it safe to do so?
                // FIXME: this won't happen if we don't get the ack-ack back
                // if msg.result.is_ok() {
                //     state
                //         .peers
                //         .entry(peer)
                //         .or_insert_with(Default::default)
                //         .advance_knowledge(clock_in_peer.base());
                // }
            }
            Synchronization::BootstrapReceiver { peer, .. } => {
                if msg.result.is_ok() {
                    state.clocks.merge(msg.result.as_ref().unwrap());
                    state.save(db, false);
                    state.storage.sync();
                    // now we're ready!
                    db.dht.promote_pending_node(db.dht.node(), state.num()).unwrap();
                    // send it back as a form of ack-ack
                    db.fabric.send_msg(peer, msg).unwrap();
                } else {
                    return SyncResult::RetryBoostrap;
                }
            }
            Synchronization::BootstrapSender { .. } => (),
        }
        SyncResult::Done
    }

    pub fn on_send(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncSend) {
        match *self {
            Synchronization::SyncReceiver { peer, ref mut count, ref mut last_receive, .. } |
            Synchronization::BootstrapReceiver { peer,
                                                 ref mut count,
                                                 ref mut last_receive,
                                                 .. } => {
                state.storage_set_remote(db, &msg.key, msg.container);

                db.fabric
                    .send_msg(peer,
                              MsgSyncAck {
                                  cookie: msg.cookie,
                                  vnode: state.num(),
                                  seq: msg.seq,
                              })
                    .unwrap();

                *count += 1;
                *last_receive = Instant::now();
            }
            _ => unreachable!(),
        }
    }

    pub fn on_ack(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncAck) {
        match *self {
            Synchronization::SyncSender { ref mut inflight, ref mut last_receive, .. } |
            Synchronization::BootstrapSender { ref mut inflight, ref mut last_receive, .. } => {
                inflight.remove(&msg.seq);
                *last_receive = Instant::now();
            }
            _ => unreachable!(),
        }
        self.send_next(db, state);
    }
}
