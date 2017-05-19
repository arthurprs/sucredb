# Sucredb

> *A database made of sugar cubes*

Sucredb is a multi-master key-value distributed database, it providers a dynamo style tunable consistent and causality tracking.

Any node that owns a partition (usually 3) can serve both reads and writes. The database tracks causality using vector-clocks and will NOT drop any conflicting writes unlike LWW (last write wins) and other strategies. Conflicts can and will happen due to races between clients and network partitions.

# API & Clients

You can use Sucredb with any redis cluter client.

*Get* like most commands bellow can be done with various commands, you can choose any that makes you client library happy. Consistency level is optional.

Results are returned as an array containing the values (zero, one or more if there's conflicting versions), the last item of that array (always present) is a binary string representing the causal context.

> `get/hget/mget key {consistency}`
>
> `[{value1}, {value2}, .., causal_context]`

*SET*, note that you need to use the context that you got from the get along the value. If you're sure it don't exist you can omit the context, if you're wrong it'll create a conflicting version.

> `set/hset key value {context} {consistency}`
>
> `OK`

*Getset* Like set, but returns the new value (and possibly more, if conflicting) and the new context, consistency level is optional. Despite the name and the semantics in redis, the get is done *after* the set.

> `getset key value context {consistency}`
>
> `[{value1}, {value2}, .., causal_context]`

*Delete*, consistency level is optional. Note that the server always returns 1, even if the key isn't present.

> `del/hdel/mdel key context {consistency}`
>
> `1`

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
OK
127.0.0.1:6379> GET there q
1) "\x01\x00\x00\x00\x00\x00\x00\x00P\xb0n\x83g\xef`\n\x05\x00\x00\x00\x00\x00\x00\x00
```

# Running

TODO

# CAP theorem

It behaves mostly like an AP system but not exactly.

Sucredb doesn't use sloppy quorum or hinted handoff so it can't serve requests that don't satisfy the requested/default consistency level.

# Performance

No claims at this point, but it's probably fast.

There's some obvious things I didn't explore that could make it faster, notably using merge operator on the storage engine.

# Ideas worth exploring

* Take advantage of UDFs (user defined functions) to prevent extra round-trips and conflicts.

* Improve the data model with a range/clustering key.

# Background

Storage takes advantage of RocksDB.

It uses a variant of version clocks to track causality. The actual algorithm is heavily inspired by [1].

[1] Gonçalves, Ricardo, et al. "Concise server-wide causality management for eventually consistent data stores."
