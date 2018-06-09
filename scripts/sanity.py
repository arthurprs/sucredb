import subprocess
import time
import sys
import random
from itertools import chain
from redis import StrictRedis
from rediscluster import StrictRedisCluster
from funcy import retry
from collections import defaultdict
import shutil

VERBOSE = False


class Instance(object):
    BIND = "127.0.0.1"
    PORT = 6379
    FPORT = 16379

    def __init__(self, i, ii):
        super(Instance, self).__init__()
        self.i = i
        self.ii = ii
        self.process = None
        self.listen_addr = "{}:{}".format(self.BIND, self.PORT + self.i)
        self.fabric_addr = "{}:{}".format(self.BIND, self.FPORT + self.i)
        self.data_dir = "n{}".format(self.i)

    @property
    def client(self):
        return StrictRedis(self.BIND, self.PORT + self.i)

    def clear_data(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def cluster_init(self):
        self.clear_data()
        self.start("init")

    def cluster_join(self):
        self.clear_data()
        self.start()

    def wait_ready(self, callback=lambda c: c.ping(),
                   timeout=5, sleep=0.1):
        @retry(int(timeout / float(sleep) + 0.5), timeout=sleep)
        def inner():
            assert callback(self.client)
        inner()

    def start(self, *args):
        assert not self.process
        self.process = subprocess.Popen(
            ["cargo", "run", "--",
             "-l", self.listen_addr,
             "-f", self.fabric_addr,
             "-d", self.data_dir]
            + list(chain.from_iterable(
                ["-s", "{}:{}".format(self.BIND, self.FPORT + i)]
                for i in range(self.ii)
                if i != self.i
            ))
            + list(args),
            stdin=sys.stdin if VERBOSE else None,
            stdout=sys.stdout if VERBOSE else None,
            stderr=sys.stderr if VERBOSE else None,
            )
        self.wait_ready()

    def __del__(self):
        if self.process:
            self.process.kill()

    def kill(self):
        assert self.process
        self.process.kill()
        self.process.wait()
        self.process = None

    def restart(self):
        self.kill()
        self.start()

    @property
    def running(self):
        return bool(self.process)

    def execute(self, *args, **kwargs):
        self.client.execute_command(*args, **kwargs)


def main():
    global VERBOSE
    VERBOSE = "verbose" in sys.argv[1:]
    subprocess.check_call(["cargo", "build"])
    cluster_sz = 3
    cluster = [Instance(i, cluster_sz) for i in range(cluster_sz)]
    cluster[0].cluster_init()
    cluster[1].cluster_join()
    cluster[2].cluster_join()
    cluster[0].execute("CLUSTER", "REBALANCE")
    time.sleep(5)

    client = StrictRedisCluster(
        startup_nodes=[
            {"host": n.listen_addr.partition(":")[0],
             "port": int(n.listen_addr.partition(":")[2])}
            for n in cluster
        ],
        decode_responses=False,
        socket_timeout=0.5,
    )

    check_map = defaultdict(set)
    items = 1000
    groups = 100
    for i in xrange(items):
        k = str(i % groups)
        v = str(i)
        client.execute_command("SET", k, v, "", "Q")
        check_map[k].add(v)
        if random.random() < 0.1:
            n = random.choice(cluster)
            n.restart()
            n.wait_ready(lambda c: c.execute_command("CLUSTER", "CONNECTIONS"))

    time.sleep(5)
    for k, expected in check_map.items():
        values = set(client.get(k)[:-1])
        assert values == expected, "%s %s %s" % (k, expected, values)
        for c in cluster:
            values = set(c.client.execute_command("GET", k, "1")[:-1])
            assert values == expected, "%s %s %s" % (k, expected, values)


if __name__ == '__main__':
    main()
