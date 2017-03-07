use std::time::{Instant, Duration};
use std::collections::HashSet;
use vnode::{VNodeStatus, VNodeState};
use fabric::*;
use version_vector::*;
use database::*;
use storage::Storage;
use inflightmap::InFlightMap;
use bincode;
use utils::IdHasherBuilder;

#[derive(Debug)]
#[must_use]
pub enum SyncResult {
    // it's done, just remove entry
    Done,
    // do nothing
    Continue,
    // remove then retry bootstrap
    RetryBoostrap,
}

// TODO: take &mut buffers instead of returning them
type IteratorFn = Box<FnMut(&Storage) -> Option<(Vec<u8>, DottedCausalContainer<Vec<u8>>)> + Send>;
type InFlightSyncMsgMap = InFlightMap<u64,
                                      (Vec<u8>, DottedCausalContainer<Vec<u8>>),
                                      Instant,
                                      IdHasherBuilder>;

pub enum Synchronization {
    SyncSender {
        // bvv in peer at the time of sync start
        clocks_in_peer: BitmappedVersionVector,
        // local bvv at the time of sync start
        clocks_snapshot: BitmappedVersionVector,
        iterator: IteratorFn,
        inflight: InFlightSyncMsgMap,
        cookie: Cookie,
        peer: NodeId,
        // count of sent keys (includes inflight)
        count: u64,
        last_recv: Instant,
    },
    SyncReceiver {
        // local bvv at the time of sync start
        clocks_in_peer: BitmappedVersionVector,
        cookie: Cookie,
        peer: NodeId,
        // aprox count of received keys (includes dups)
        recv_count: u64,
        last_recv: Instant,
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
    },
    BootstrapReceiver {
        cookie: Cookie,
        peer: NodeId,
        // aprox count of received keys (includes dups)
        recv_count: u64,
        last_recv: Instant,
    },
}

impl Synchronization {
    pub fn new_sync_receiver(db: &Database, state: &mut VNodeState, peer: NodeId, cookie: Cookie)
                             -> Self {
        let mut sync = Synchronization::SyncReceiver {
            clocks_in_peer: state.clocks.clone(),
            peer: peer,
            cookie: cookie,
            recv_count: 0,
            last_recv: Instant::now(),
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
            recv_count: 0,
            last_recv: Instant::now(),
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
                    (k.into(), bincode::deserialize::<DottedCausalContainer<_>>(&v).unwrap())
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
            last_recv: Instant::now(),
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
        debug!("Delta from {:?} to {:?}",  state.clocks, clocks_in_peer);
        let log_uptodate = dots_delta.min_versions()
            .iter()
            .all(|&(n, v)| state.logs.get(&n).and_then(|log| log.min_version()).unwrap_or(0) <= v);

        let iterator: IteratorFn = if log_uptodate {
            // FIXME: this is really bad but it works
            // otherwise be might risk the log to overflow and dots to go missing
            let mut keys = HashSet::new();
            for (n, v) in dots_delta {
                let key = state.logs.get(&n).and_then(|log| log.get(v)).cloned().unwrap();
                keys.insert(key);
            }
            let mut keys = keys.into_iter();

            Box::new(move |storage| {
                keys.by_ref()
                    .filter_map(|k| {
                        storage.get_vec(&k)
                            .map(|v| {
                                let mut dcc: DottedCausalContainer<_> =
                                    bincode::deserialize(&v).unwrap();
                                // TODO: fill should be done in the remote?
                                dcc.fill(&clocks_snapshot);
                                (k, dcc)
                            })
                    })
                    .next()
            })
        } else {
            debug!("SyncSender {:?} using a scan", msg.cookie);
            let mut storage_iterator = state.storage.iterator();
            let mut iter = (0..)
                .map(move |_| {
                    storage_iterator.iter().next().map(|(k, v)| {
                        (k.into(),
                         bincode::deserialize::<DottedCausalContainer<_>>(&v).unwrap())
                    })
                })
                .take_while(|i| i.is_some())
                .filter_map(|i| i)
                .filter_map(move |(k, mut dcc)| {
                    if !dcc.contained(&clocks_in_peer) {
                        // TODO: fill should be done in the remote?
                        dcc.fill(&clocks_snapshot);
                        Some((k, dcc))
                    } else {
                        None
                    }
                });
            Box::new(move |_| iter.next())
        };

        Synchronization::SyncSender {
            clocks_in_peer: clocks_in_peer2,
            clocks_snapshot: clocks_snapshot2,
            iterator: iterator,
            inflight: InFlightMap::new(),
            cookie: cookie,
            peer: peer,
            count: 0,
            last_recv: Instant::now(),
        }
    }

    fn send_start(&mut self, db: &Database, state: &mut VNodeState) {
        let (peer, cookie, target, clocks_in_peer) = match *self {
            Synchronization::SyncReceiver { cookie,
                                            peer,
                                            ref mut last_recv,
                                            ref clocks_in_peer,
                                            .. } => {
                // reset last receives
                *last_recv = Instant::now();
                (peer, cookie, Some(peer), clocks_in_peer.clone())
            }
            Synchronization::BootstrapReceiver { peer, cookie, ref mut last_recv, .. } => {
                // reset last receives
                *last_recv = Instant::now();
                (peer, cookie, None, BitmappedVersionVector::new())
            }
            _ => unreachable!(),
        };

        info!("Sending start for {:?}", cookie);
        let _ = db.fabric
            .send_msg(peer,
                      MsgSyncStart {
                          cookie: cookie,
                          vnode: state.num(),
                          clocks_in_peer: clocks_in_peer,
                          target: target,
                      });
    }

    fn send_success_fin(&mut self, db: &Database, state: &mut VNodeState) {
        match *self {
            Synchronization::SyncSender { peer, cookie, ref clocks_snapshot, .. } |
            Synchronization::BootstrapSender { peer, cookie, ref clocks_snapshot, .. } => {
                let _ = db.fabric
                    .send_msg(peer,
                              MsgSyncFin {
                                  cookie: cookie,
                                  vnode: state.num(),
                                  result: Ok(clocks_snapshot.clone()),
                              });
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
                let timeout = now + Duration::from_millis(db.config.sync_msg_timeout as _);
                while let Some((seq, &(ref k, ref dcc))) = inflight.touch_expired(now, timeout) {
                    debug!("resending seq {} for sync/bootstrap {:?}", seq, cookie);
                    let _ = db.fabric
                        .send_msg(peer,
                                  MsgSyncSend {
                                      cookie: cookie,
                                      vnode: state.num(),
                                      seq: seq,
                                      key: k.clone(),
                                      container: dcc.clone(),
                                  });
                }
                while inflight.len() < db.config.sync_msg_inflight as usize {
                    if let Some((k, dcc)) = iterator(&state.storage) {
                        let _ = db.fabric
                            .send_msg(peer,
                                      MsgSyncSend {
                                          cookie: cookie,
                                          vnode: state.num(),
                                          seq: *count,
                                          key: k.clone(),
                                          container: dcc.clone(),
                                      });
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
                let _ = fabric_send_error!(db,
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
            Synchronization::SyncReceiver { peer, .. } => {
                state.sync_nodes.remove(&peer);
            }
            _ => (),
        }

        let incomming = match self {
            Synchronization::BootstrapReceiver { .. } |
            Synchronization::SyncReceiver { .. } => true,
            Synchronization::BootstrapSender { .. } |
            Synchronization::SyncSender { .. } => false,
        };
        db.signal_sync_end(incomming);
    }

    pub fn on_tick(&mut self, db: &Database, state: &mut VNodeState) -> SyncResult {
        match *self {
            Synchronization::SyncSender { last_recv, cookie, .. } |
            Synchronization::BootstrapSender { last_recv, cookie, .. } => {
                if last_recv.elapsed() > Duration::from_millis(db.config.sync_timeout as _) {
                    warn!("sync/boostrap sender timed out {:?}", cookie);
                    return SyncResult::Done;
                }
                self.send_next(db, state);
            }
            Synchronization::SyncReceiver { last_recv, recv_count, cookie, .. } |
            Synchronization::BootstrapReceiver { last_recv, recv_count, cookie, .. } => {
                if last_recv.elapsed() > Duration::from_millis(db.config.sync_timeout as _) {
                    warn!("sync/boostrap receiver timed out {:?}", cookie);
                    match *self {
                        Synchronization::BootstrapReceiver { .. } => {
                            return SyncResult::RetryBoostrap;
                        }
                        _ => return SyncResult::Done,
                    }
                } else if recv_count == 0 &&
                          last_recv.elapsed() >
                          Duration::from_millis(db.config.sync_msg_timeout as _) {
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
                    let _ = db.fabric.send_msg(peer, msg);
                } else if msg.result.err() == Some(FabricMsgError::NotReady) {
                    return SyncResult::Continue;
                }
            }
            Synchronization::SyncSender { /*ref clock_in_peer, peer,*/ .. } => {
                // TODO: gc log
                // TODO: advance to the snapshot base instead? Is it safe to do so?
                // FIXME: this won't happen if the ack-ack is dropped
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
                    match db.dht.promote_pending_node(db.dht.node(), state.num()) {
                        Ok(_) => {
                            // send it back as a form of ack-ack
                            let _ = db.fabric.send_msg(peer, msg);
                            state.set_status(db, VNodeStatus::Ready);
                        }
                        Err(e) => {
                            warn!("Can't retire node {} vnode {}: {}", db.dht.node(), state.num(), e);
                            return SyncResult::Continue;
                        }
                    }
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
            Synchronization::SyncReceiver { peer, ref mut recv_count, ref mut last_recv, .. } |
            Synchronization::BootstrapReceiver { peer,
                                                 ref mut recv_count,
                                                 ref mut last_recv,
                                                 .. } => {
                // FIXME: these can create a big region of 0s in the start of bitmap
                //        until the bvv join on Fin
                state.storage_set_remote(db, &msg.key, msg.container);

                let _ = db.fabric
                    .send_msg(peer,
                              MsgSyncAck {
                                  cookie: msg.cookie,
                                  vnode: state.num(),
                                  seq: msg.seq,
                              });

                *recv_count += 1;
                *last_recv = Instant::now();
            }
            _ => unreachable!(),
        }
    }

    pub fn on_ack(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncAck) {
        match *self {
            Synchronization::SyncSender { ref mut inflight, ref mut last_recv, .. } |
            Synchronization::BootstrapSender { ref mut inflight, ref mut last_recv, .. } => {
                inflight.remove(&msg.seq);
                *last_recv = Instant::now();
            }
            _ => unreachable!(),
        }
        self.send_next(db, state);
    }
}
