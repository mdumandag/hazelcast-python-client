import os
import random
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
# Making sure that hazelcast directory is in the sys.path so we can import modules from there in the command line.

import argparse
import threading
import time

from hazelcast.config import ClientConfig, NearCacheConfig, IN_MEMORY_FORMAT, EVICTION_POLICY
from hazelcast.client import HazelcastClient
from hdrh.histogram import HdrHistogram

parser = argparse.ArgumentParser()
parser.add_argument("--thread-count", default=16, type=int, help="number of threads")
parser.add_argument("--entry-count", default=10000, type=int, help="number of entry counts")
parser.add_argument("--key-size", default=10, type=int, help="size of the keys in bytes")
parser.add_argument("--value-size", default=1024, type=int, help="size of the values in bytes")
parser.add_argument("--addresses", default="", type=str, help="host:port pairs seperated by -")
parser.add_argument("--warmup-time", default=1.0, type=float, help="benchmark warmup time in minutes")
parser.add_argument("--test-time", default=5.0, type=float, help="benchmark test time in minutes")
parser.add_argument("--cache-ratio", default=0.2, type=float, help="Near cache ratio to the entry count")

args = parser.parse_args()

THREAD_COUNT = args.thread_count
ENTRY_COUNT = args.entry_count
KEY_SIZE = args.key_size
VALUE_SIZE = args.value_size
ADDRESSES = args.addresses
WARM_UP_TIME = args.warmup_time
TEST_TIME = args.test_time
CACHE_RATIO = args.cache_ratio


config = ClientConfig()
for address in ADDRESSES.split("-"):
    config.network_config.addresses.append(address)

nearcache_config = NearCacheConfig("map")
nearcache_config.in_memory_format = IN_MEMORY_FORMAT.OBJECT
nearcache_config.eviction_policy = EVICTION_POLICY.LRU
nearcache_config.eviction_max_size = int(ENTRY_COUNT * CACHE_RATIO)
config.add_near_cache_config(nearcache_config)

client = HazelcastClient(config)

test_map = client.get_map("map").blocking()
test_map.destroy()

key_format = '{:0<%d}' % KEY_SIZE
value_data = "x" * VALUE_SIZE


# histogram __init__ values
LOWEST = 1
HIGHEST = 3600 * 1000 * 1000
SIGNIFICANT = 3
histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)


def do_benchmark():
    class ClientThread(threading.Thread):
        def __init__(self, name):
            threading.Thread.__init__(self, name=name)
            self.count = 0
            self.setDaemon(True)
            self.histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)

        def run(self):
            while True:
                key = key_format.format(int(random.random() * ENTRY_COUNT))
                start = time.time_ns()
                test_map.get(key)
                end = time.time_ns()
                self.histogram.record_value(end - start)
                self.count += 1

        def reset(self):
            count = self.count
            self.count = 0
            return count

    fill_map()
    warm_up()

    throughputs = []
    benchmark_lock = threading.Lock()

    threads = [ClientThread("client-thread-%d" % i) for i in range(0, THREAD_COUNT)]
    for t in threads:
        t.start()

    test_end_time = time.time() + TEST_TIME * 60
    while time.time() < test_end_time:
        time.sleep(1)
        with benchmark_lock:
            throughput = sum(thread.reset() for thread in threads)
            throughputs.append(throughput)

    for thread in threads:
        histogram.add(thread.histogram)

    with open("hz-{}t-get-th-1client-nearcache.txt".format(THREAD_COUNT), "w") as out:
        out.write(str(throughputs))

    with open("hz-{}t-get-hist-1client-nearcache.txt".format(THREAD_COUNT), "wb") as out:
        histogram.output_percentile_distribution(out, 1000000)

    print("hit", test_map._near_cache._cache_hit, "miss", test_map._near_cache._cache_miss)


def warm_up():
    start_time = time.time_ns()
    warm_up_time = WARM_UP_TIME * 60 * 1e9
    end_time = int(start_time + warm_up_time)

    while end_time > time.time_ns():
        key = key_format.format(int(random.random() * ENTRY_COUNT))
        test_map.get(key)


def fill_map():
    for i in range(ENTRY_COUNT):
        test_map.set(key_format.format(i), value_data)


do_benchmark()
