# Sucredb

Sugar, it's everywhere. It happens that I'm allergic to it, really.

# Background

A forever work in progress, the idea is to build a highly available database
using vector clocks, similar to Riak. The actual algorithm is heavily
inspired by [1].

[1] Gonçalves, Ricardo, et al. "Concise server-wide causality management for eventually consistent data stores."

### checklist

* [x] store node id
* [x] store vnode state (skip logs unless it's shutting down)
* [x] rebuild vnode state on startup (after crash)
* [x] when joining ask nodes for their version of the log
* [x] resp server
* [x] convert resp into command to the database
* [x] send responses to the resp server
* [ ] proper resp protocol
* [x] support specifying ConsistencyLevel on request
* [x] worker threads
* [x] handling timeouts for crud
* [x] resending sync/bootstrap data from sender side
* [x] resending sync/bootstrap start from receiver side
* [x] generic sync/bootstrap code?
* [x] when log isn't enough to satisfy sync, iter the database sending necessary dots
* [x] propagate dht notifications to vnodes and take actions
* [ ] dead node handling
* [ ] pruning old nodes from node clocks?
* [ ] track deleted keys pending physical deletion
* [ ] limit # of syncs on both directions by config
* [x] cluster name
* [ ] cluster handling api (join/leave/rebalance)
* [ ] smarter rebalance
* [x] cluster tests
* [ ] more cluster tests
* [ ] configurations everywhere
* [ ] stats everywhere
* [ ] config file
