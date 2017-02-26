# -*- coding: utf-8 -*-

# python std lib
import time

# 3rd party imports
from docopt import docopt
from redis._compat import xrange


def resp(aa):
    if len(aa) == 1:
        return [], aa[0]
    if len(aa) == 2:
        return aa[0], aa[1]
    raise Exception("multiple values returned ~ " + str(aa))

def loop(rc, reset_last_key=None):
    """
    Regular debug loop that can be used to test how redis behaves during changes in the cluster.
    """
    _, last_ctx = resp(rc.get("__last__"))
    if reset_last_key:
        rc.hset("__last__", 0, last_ctx)

    last = False
    while last is False:
        try:
            last, last_ctx = resp(rc.get("__last__"))
            print "last is %s" % last
            last = 0 if not last else int(last)
            print("starting at foo{0}".format(last))
        except Exception as e:
            print("error1 {0}".format(repr(e)))
            time.sleep(1)

    for i in xrange(last, 1000000000):  # noqa
        try:
            print("SET foo{} {}".format(i, i))
            rc.set("foo{}".format(i), str(i))
            got, got_ctx = resp(rc.get("foo{}".format(i)))
            print("GET foo{} {}".format(i, got))
            assert got == str(i), "%s != %s" % (got, i)
            rc.hset("__last__", i, last_ctx)
        except Exception as e:
            print("error2 {}".format(repr(e)))


def timeit(rc, itterations=50000):
    """
    Time how long it take to run a number of set/get:s
    """
    t0 = time.time()
    for i in xrange(0, itterations):  # noqa
        s = "foo{0}".format(i)
        rc.set(s, i)
        rc.get(s)

    t1 = time.time() - t0
    print("{0}k SET/GET operations took: {1} seconds... {2} operations per second".format((itterations / 1000) * 2, t1, (itterations / t1) * 2))


def timeit_pipeline(rc, itterations=50000):
    """
    Time how long it takes to run a number of set/get:s inside a cluster pipeline
    """
    t0 = time.time()
    for i in xrange(0, itterations):  # noqa
        s = "foo{0}".format(i)

        p = rc.pipeline()
        p.set(s, i)
        p.get(s)
        p.execute()

    t1 = time.time() - t0
    print("{0}k SET/GET operations inside pipelines took: {1} seconds... {2} operations per second".format(
        (itterations / 1000) * 2, t1, (itterations / t1) * 2)
    )


if __name__ == "__main__":
    __docopt__ = """
Usage:
  simple [--host IP] [--port PORT] [--nocluster] [--timeit] [--pipeline] [--resetlastkey] [-h] [--version]

Options:
  --nocluster        If flag is set then StrictRedis will be used instead of cluster lib
  --host IP          Redis server to test against [default: 127.0.0.1]
  --port PORT        Port on redis server [default: 7000]
  --timeit           run a mini benchmark to test performance
  --pipeline         Only usable with --timeit flag. Runs SET/GET inside pipelines.
  --resetlastkey     reset __last__ key
  -h --help          show this help and exit
  -v --version       show version and exit
    """

    args = docopt(__docopt__, version="0.3.0")

    startup_nodes = [{"host": args["--host"], "port": args["--port"]}]

    if not args["--nocluster"]:
        from rediscluster import StrictRedisCluster
        rc = StrictRedisCluster(startup_nodes=startup_nodes,
            max_connections=32, socket_timeout=0.5,
            decode_responses=False, skip_full_coverage_check=True)
    else:
        from redis import StrictRedis
        rc = StrictRedis(host=args["--host"], port=args["--port"],
            socket_timeout=0.5, decode_responses=False)

    if args["--timeit"]:
        test_itterstions = [
            5000,
            10000,
            20000,
        ]

        if args["--pipeline"]:
            for itterations in test_itterstions:
                timeit_pipeline(rc, itterations=itterations)
        else:
            for itterations in test_itterstions:
                timeit(rc, itterations=itterations)
    else:
        loop(rc, reset_last_key=args["--resetlastkey"])
