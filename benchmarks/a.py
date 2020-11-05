import cProfile
import io
import os
import pstats
import random
import sys
import threading
import time
from collections import deque
from itertools import count

import yappi

dirname = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dirname)
sys.path.append(os.path.join(dirname, '..'))

from hazelcast.six.moves import range
from hazelcast import six

import hazelcast


sentinel = object()
REQ_COUNT = 100000
ENTRY_COUNT = 10 * 1000
VALUE_SIZE = 1000
GET_PERCENTAGE = 40
PUT_PERCENTAGE = 40
VALUE = "x" * VALUE_SIZE
c = False
y = False


def do_benchmark():
    client = hazelcast.HazelcastClient()

    class Test(object):
        def __init__(self):
            self.num_started = count()
            self.num_finished = count()
            self.event = threading.Event()
            self.my_map = client.get_map("default")
            self.my_map.clear().result()

        def run(self):
            futures = deque()

            for i in range(REQ_COUNT):
                if i > 0 and i % 1000 == 0:
                    # clear the existing queue
                    while True:
                        try:
                            futures.popleft().result()
                        except IndexError:
                            break

                key = int(random.random() * ENTRY_COUNT)
                operation = int(random.random() * 100)
                if operation < GET_PERCENTAGE:
                    future = self.my_map.get(key)
                elif operation < GET_PERCENTAGE + PUT_PERCENTAGE:
                    future = self.my_map.set(key, VALUE)
                else:
                    future = self.my_map.delete(key)
                futures.append(future)

            while True:
                try:
                    futures.popleft().result()
                except IndexError:
                    break

    p = start_profiling()

    t = Test()
    start = time.time()
    t.run()

    time_taken = time.time() - start
    six.print_("Took {} seconds for {} requests".format(time_taken, REQ_COUNT))
    six.print_("ops per second: {}".format(REQ_COUNT // time_taken))

    finish_profiling(p)


def start_profiling():
    if c:
        pr = cProfile.Profile()
        pr.enable()
        return pr

    if y:
        yappi.set_clock_type("cpu")  # Use set_clock_type("wall") for wall time
        yappi.start()

    return None


def finish_profiling(p=None):
    if c:
        p.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(p, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())

    if y:
        yappi.stop()
        yappi.get_func_stats().print_all(columns={
                 0: ("name", 80),
                 1: ("ncall", 10),
                 2: ("tsub", 10),
                 3: ("ttot", 10),
                 4: ("tavg", 10)
             })
        yappi.get_thread_stats().print_all()


if __name__ == '__main__':
    do_benchmark()
