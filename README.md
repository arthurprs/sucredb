# Sucredb

Sugar, it's everywhere. It happens that I'm allergic to it, really.

# Background

A forever work in progress, the idea is to build a highly available database using vector clocks, similar to Riak.
The actual algorithm is heavily inspired by [1].

I've been working on this for several months during my freetime, including some big rewrites.
Code quality and commit messages could be of subpar quality, I'll try to improve that in the future.

[1] Gonçalves, Ricardo, et al. "Concise server-wide causality management for eventually consistent data stores."

### please ignore, dev checklist

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
* [x] when log isn't enough to satisfy sync, iter the database sending missing dots
* [x] propagate dht notifications to vnodes and take actions
* [ ] dead node handling (either with gossip or smarter fabric)
* [x] prevent log overflow from crashing ongoing syncs
* [x] limit # of syncs on both directions by config
* [x] gc dot->key log
* [x] cluster name
* [ ] cluster handling api (join/leave/rebalance)
* [x] cluster tests
* [ ] more cluster tests
* [x] proper storage module
* [x] metrics
* [ ] expose metrics
* [x] config file
* [ ] better logging
* [x] CI like Travis
* [x] modernize tokio servers (client and fabric)
* [ ] configurations everywhere
* [ ] track deleted keys pending physical deletion (is this a good idea? maybe a compaction filter?)
* [ ] persisted dot->key log (is this a good idea?)
* [ ] pruning old nodes from node clocks (is it possible?)
* [ ] inner vnode parallelism
