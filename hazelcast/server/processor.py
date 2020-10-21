class SomeProcessor(object):
    def process(self, entry):
        return self._process(entry)

    def _process(self, entry):
        val = entry.value
        key = entry.key
        entry.value = "%s maps to %s" % (key, val)
        return "result_of_the_call for %s, %s" %(key, val)


class SomeCallable(object):
    def __init__(self):
        self._a = 0
        self._b = 1

    def call(self):
        a = self._a
        b = self._b

        while b < 10:
            a, b = b, a + b

        return b
