# Sucredb

> *A database made of sugar cubes*

[![Build Status](https://travis-ci.org/arthurprs/sucredb.svg?branch=master)](https://travis-ci.org/arthurprs/sucredb)

Sucredb is a multi-master key-value distributed database, it provides a dynamo style tunable consistent and causality tracking.

Any node that owns a partition (replication factor) can serve both reads and writes. The database tracks causality using vector-clocks and will NOT drop any conflicting writes unlike LWW (last write wins) and other strategies. Conflicts can and will happen due to races between clients and network partitions.

Status: Alpha quality with missing pieces.

# API & Clients

You can use Sucredb with any Redis Cluster client.

*Get* like most commands bellow can be done with various commands, you can choose any that makes you client library happy. Consistency level is optional.

Results are returned as an array containing the values (zero, one or more if there's conflicting versions), the last item of that array (always present) is a binary string representing the causal context.

\> `get/hget/mget key {consistency}`

< `[{value1}, {value2}, .., causal_context]`

*Set*, in addition to the key and value, also takes the causal context. If you're sure it don't exist you can actually omit the context, if you're wrong it'll create a conflicting version.

\> `set/hset key value {context} {consistency}`

< `OK`

*Getset* is similar to set, but returns the updated value(s) and a new context. Despite the name and the semantics in Redis, the get is always done *after* the set.

\> `getset key value context {consistency}`

< `[{value1}, {value2}, .., causal_context]`

*Delete* is like set and also requires a context. Note that the server always returns 1 regardless of the key situation.

\> `del/hdel/mdel key context {consistency}`

< `1`

`{consistency}` follows the dynamo/cassandra/riak style:

* `1`, `o`, `O`: One
* `q`, `Q`: Quorum
* `a`, `A`: All

Quick example using *redis-cli*

```
➜  ~ redis-cli
127.0.0.1:6379> GET there
1) "\x00\x00\x00\x00\x00\x00\x00\x00"
127.0.0.1:6379> SET there 1 "\x00\x00\x00\x00\x00\x00\x00\x00"
OK
127.0.0.1:6379> GET there
1) "1"
2) "\x01\x00\x00\x00\x00\x00\x00\x00P\xb0n\x83g\xef`\n\x01\x00\x00\x00\x00\x00\x00\x00"
127.0.0.1:6379> SET there 2
OK
127.0.0.1:6379> GET there 1
1) "1"
2) "2"
3) "\x01\x00\x00\x00\x00\x00\x00\x00P\xb0n\x83g\xef`\n\x02\x00\x00\x00\x00\x00\x00\x00"
127.0.0.1:6379> SET there 3 "\x01\x00\x00\x00\x00\x00\x00\x00P\xb0n\x83g\xef`\n\x02\x00\x00\x00\x00\x00\x00\x00"
OK
127.0.0.1:6379> GET there
1) "3"
2) "\x01\x00\x00\x00\x00\x00\x00\x00P\xb0n\x83g\xef`\n\x03\x00\x00\x00\x00\x00\x00\x00"
127.0.0.1:6379> GETSET there 4 "\x01\x00\x00\x00\x00\x00\x00\x00P\xb0n\x83g\xef`\n\x03\x00\x00\x00\x00\x00\x00\x00"
1) "4"
2) "\x01\x00\x00\x00\x00\x00\x00\x00P\xb0n\x83g\xef`\n\x04\x00\x00\x00\x00\x00\x00\x00"
127.0.0.1:6379> DEL there "\x01\x00\x00\x00\x00\x00\x00\x00P\xb0n\x83g\xef`\n\x04\x00\x00\x00\x00\x00\x00\x00"
1
127.0.0.1:6379> GET there q
1) "\x01\x00\x00\x00\x00\x00\x00\x00P\xb0n\x83g\xef`\n\x05\x00\x00\x00\x00\x00\x00\x00
```

# Running

**Requirements**

* Needs a reasonably recent Rust (nightly[1])
* C++ compiler (for Rocksdb).

**Running**

* The following setup will use the default settings.
* Clone the repo and enter repository root
* `cargo install .` [2]
* `sucredb --help`

Single/First Node

`sucredb -d datadir1 -l 127.0.0.1:6379 -f 127.0.0.1:16379 init`

Second node

`sucredb -d datadir2 -l 127.0.0.1:6378 -f 127.0.0.1:16378 -s 127.0.0.1:16379`

[1] Mostly due to the try_from feature that should be stable soon.

[2] Be patient as rocksdb takes a long time to compile.

# Configuration

See `sucredb.yaml`

To use configuration file use: `sucredb -c sucredb.yaml`

# CAP theorem

It behaves mostly like an AP system but not exactly.

Sucredb doesn't use sloppy quorum or hinted handoff so it can't serve requests that don't satisfy the requested/default consistency level.

# Performance

Almost every single new thing claims to be fast or blazing fast. Sucredb makes no claims at this point, but it's probably fast.

There's some obvious things I didn't explore that could make it faster, notably using merge operator on the storage engine.

# Ideas worth exploring

* Implement some of Redis data types that work reasonably as CRDTs, like sets and HyperLogLog.

* Take advantage of UDFs (user defined functions) to prevent extra round-trips and conflicts.

* Improve the data model with a range/clustering key.

# Background

Storage takes advantage of RocksDB.

It uses a variant of version clocks to track causality. The actual algorithm is heavily inspired by [1].

[1] Gonçalves, Ricardo, et al. "Concise server-wide causality management for eventually consistent data stores."
