# Sucredb

> *A database made of sugar cubes*

[![Build Status](https://travis-ci.org/arthurprs/sucredb.svg?branch=master)](https://travis-ci.org/arthurprs/sucredb)

Sucredb is a multi-master key-value distributed database, it provides a dynamo style tunable consistent and causality tracking.

Any node that owns a partition (replicas) can serve both reads and writes. The database tracks causality using vector-clocks and will NOT drop any conflicting writes unlike LWW (last write wins) and other strategies. Conflicts can and do happen due to races between clients and network partitions.

Status: Alpha quality with missing pieces.

# API & Clients

Theoretically you can use Sucredb with any Redis Cluster clients.

It implements a tiny subset of Redis commands. Only basic Key-Value/Sets/Hashes operations are supported at this point.

### Key Value

#### GET

*GET* result(s) is/are returned as an array containing the values (zero, one or more if there's conflicting versions) plus the causal context. The context is an binary string and is always returned as the last item of the array even if no values are present.

`> GET key {consistency}`

`< [{value1}, {value2}, .., context]`

#### MGET

*MGET* takes the # of keys (N) followed by N keys. Results are returned as an array.


`> MGET key_count {key1} {key2} {..} {consistency}`

`< [[{value1_1}, {value1_2}, .., context], [{value2_1}, {value2_2}, .., context]]`

#### SET

*SET*, in addition to the key and value, also takes the causal context. If you're sure it don't exist you can actually omit the context, if you're wrong it'll create a conflicting version.

`> SET key value {context} {consistency}`

`< OK`

#### GETSET

*GETSET* is similar to set, but returns the updated value(s) and a new context. Despite the name and the semantics in Redis, the get is always done *after* the set.

`> GETSET key value context {consistency}`

`< [{value1}, {value2}, .., context]`

#### DEL

*DEL* is like set and also requires a context when dealing with basic values.
Following Redis api *del* works for keys with any datastructure, in these cases the context is ignored (you can use an empty string instead).

`> DEL key context {consistency}`

`< 1 OR 0 (if not found)`

### Data structures

Sucredb also supports a tiny subset of commands for Hash and Set datatypes. These types are [CRDTs](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) and don't require a context to be sent along the operation. Mutations depend on the coordinator version of the value and conflicts are handled as follow:

* Hash: On values conflict the latest write wins.
* Set: On values conflict add wins.
* Counter: Deletes may erase non observed increments.

#### CSET

Sets the value for a counter.

`> SET key int_value {consistency}`

`< OK`

### INCRBY

Increments the value for a counter, the delta can be either positive or negative.

`> INCRBY key delta_value {consistency}`

`< resulting_int_value`

#### HGETALL

Gets all key value pairs from a hash.

`> HGETALL key {consistency}`

`< [{KA, VA}, {KB, VB}, ...]`

#### HSET

Set key a value pair in a hash.

`> HSET key hash_key value {consistency}`

`< 1 OR 0 (if hash_key already existed) `

#### HDEL

Deletes a key from a hash.

`> HDEL key hash_key {consistency}`

`< 1 OR 0 (if hash_key didn't exist)`

#### SMEMBERS

Gets all values from a set.

`> SMEMBERS key {consistency}`

`< [{KA}, {KB}, ...]`

#### SADD

Adds a value from the set.

`> SADD key value {consistency}`

`< 1 OR 0 (if value already existed) `

#### SREM

Removes a value from the set.

`> SREM key value {consistency}`

`< 1 OR 0 (if value didn't exist) `

### MULTI/EXEC Batches

todo

### Other parameters

#### `context` parameter

If you don't have a context (from a previous get or getset) you can send an empty string.

#### `consistency` parameter

`{consistency}` follows the dynamo/cassandra/riak style:

* `1`, `o`, `O`: One
* `q`, `Q`: Quorum
* `a`, `A`: All

# Running

**Requirements**

* Needs a reasonably recent Rust (nightly[2])
* C++ compiler (for Rocksdb).

**Running**

* The following setup will use the default settings.
* Clone the repo and enter repository root
* `cargo install .` [3]
* `sucredb --help`

Single/First instance

`sucredb -d datadir1 -l 127.0.0.1:6379 -f 127.0.0.1:16379 init`

The command above will initialize a new cluster containing this node. The cluster will have the default name, partition count and replication factor.

Second instance

`sucredb -d datadir2 -l 127.0.0.1:6378 -f 127.0.0.1:16378 -s 127.0.0.1:16379`

The second instance joins the cluster using the first instance as a seed.

Quick test

`redis-cli CLUSTER SLOTS`

#### Example

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

# Configuration

See `sucredb.yaml`

To use configuration file use: `sucredb -c sucredb.yaml`

# CAP theorem

It behaves mostly like an AP system but not exactly.

Sucredb doesn't use sloppy quorum or hinted handoff so it can't serve requests that don't satisfy the requested/default consistency level.

# Performance

Almost every single new thing claims to be fast or blazing fast. Sucredb makes no claims at this point, but it's probably fast.

The data structure operations move the entire collection around the cluster so it's *not* suitable for large values/collections.

# Ideas worth exploring

* Improve the data model with a range/clustering key.

# Background

Storage takes advantage of RocksDB.

It uses a variant of version clocks to track causality. The actual algorithm is heavily inspired by [1].

----

[1] Gonçalves, Ricardo, et al. "Concise server-wide causality management for eventually consistent data stores."

[2] Mostly due to the try_from and impl trait features that should be stable soon.

[3] Be patient.
