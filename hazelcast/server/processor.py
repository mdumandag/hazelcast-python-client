class SomeProcessor(object):
    def __init__(self):
        self.state = "."

    def process(self, entry):
        val = entry.value
        entry.value = val + "hey" + self.state
        self.state += "."
        return "ok"


class SomeCallable(object):
    def call(self):
        return 12
