import logging
import queue
import random
import sys
import threading
import time
from itertools import count
from os.path import dirname
from hazelcast import six
from hazelcast.six.moves import range
import yappi
import cProfile, pstats, io

sys.path.append(dirname(dirname(dirname(__file__))))

import hazelcast


sentinel = object()

def do_benchmark():
    REQ_COUNT = 5000
    ENTRY_COUNT = 10 * 1000
    VALUE_SIZE = 1000
    GET_PERCENTAGE = 40
    PUT_PERCENTAGE = 40
    VALUE = "x" * VALUE_SIZE
    client = hazelcast.HazelcastClient()

    class Test(object):

        def __init__(self):
            self.my_map = client.get_map("default")
            self.my_map.clear().result()

        def run(self):
            for _ in range(REQ_COUNT):
                key = int(random.random() * ENTRY_COUNT)
                operation = int(random.random() * 100)
                if operation < GET_PERCENTAGE:
                    self.my_map.get(key).result()
                elif operation < GET_PERCENTAGE + PUT_PERCENTAGE:
                    self.my_map.set(key, VALUE).result()
                else:
                    self.my_map.delete(key).result()

    t = Test()
    start = time.time()

    # pr = cProfile.Profile()
    # pr.enable()

    # yappi.set_clock_type("wall") # Use set_clock_type("wall") for wall time
    # yappi.start()

    t.run()

    #
    # pr.disable()
    # s = io.StringIO()
    # sortby = 'cumulative'
    # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    # ps.print_stats()
    # print(s.getvalue())

    # yappi.stop()
    # yappi.get_func_stats().print_all(columns={
    #         0: ("name", 80),
    #         1: ("ncall", 10),
    #         2: ("tsub", 10),
    #         3: ("ttot", 10),
    #         4: ("tavg", 10)
    #     })
    # yappi.get_thread_stats().print_all()


    time_taken = time.time() - start
    six.print_("Took {} seconds for {} requests".format(time_taken, REQ_COUNT))
    six.print_("ops per second: {}".format(REQ_COUNT // time_taken))


if __name__ == '__main__':
    do_benchmark()
    time.sleep(100)
