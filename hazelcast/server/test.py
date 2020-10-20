import hazelcast
from hazelcast.serialization.serializer import EntryProcessor, Callable
from hazelcast.server.processor import SomeProcessor, SomeCallable

client = hazelcast.HazelcastClient()

m = client.get_map("test").blocking()
m.set("a", "b")
m.set("c", "d")
m.set("e", "f")

print("here")


m.execute_on_entries(EntryProcessor(SomeProcessor()))
print(m.entry_set())

e = client.get_executor("e").blocking()
print(e.execute_on_all_members(Callable(SomeCallable())))

