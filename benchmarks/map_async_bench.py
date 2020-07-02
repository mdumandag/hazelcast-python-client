import logging
import random
import sys
import threading
import time
from os.path import dirname
from hazelcast import six
from hazelcast.six.moves import range
import yappi
import cProfile, pstats, io
from pstats import SortKey

sys.path.append(dirname(dirname(dirname(__file__))))

import hazelcast

# yappi.set_clock_type("cpu") # Use set_clock_type("wall") for wall time
# yappi.start()


# pr = cProfile.Profile()
# pr.enable()

def do_benchmark():
    REQ_COUNT = 20000
    ENTRY_COUNT = 10 * 1000
    VALUE_SIZE = 1000
    GET_PERCENTAGE = 40
    PUT_PERCENTAGE = 40
    VALUE = "x" * VALUE_SIZE
    client = hazelcast.HazelcastClient()

    class Test(object):

        def __init__(self):
            self.ops = 0
            self.event = threading.Event()

        def incr(self, _):
            self.ops += 1
            if self.ops == REQ_COUNT:
                self.event.set()

        def run(self):
            my_map = client.get_map("default")
            my_map.clear().result()
            for _ in range(0, REQ_COUNT):
                key = int(random.random() * ENTRY_COUNT)
                operation = int(random.random() * 100)
                if operation < GET_PERCENTAGE:
                    my_map.get(key).add_done_callback(self.incr)
                elif operation < GET_PERCENTAGE + PUT_PERCENTAGE:
                    my_map.set(key, VALUE).add_done_callback(self.incr)
                else:
                    my_map.remove(key).add_done_callback(self.incr)

    t = Test()
    start = time.time()
    t.run()
    t.event.wait()

    # yappi.stop()
    # yappi.get_func_stats().print_all()
    # yappi.get_thread_stats().print_all()

    # pr.disable()
    # s = io.StringIO()
    # sortby = SortKey.CUMULATIVE
    # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    # ps.print_stats()
    # print(s.getvalue())

    time_taken = time.time() - start
    six.print_("Took {} seconds for {} requests".format(time_taken, REQ_COUNT))
    six.print_("ops per second: {}".format(t.ops // time_taken))


if __name__ == '__main__':
    do_benchmark()
