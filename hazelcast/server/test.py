import hazelcast
from hazelcast.server.processor import SomeProcessor, SomeCallable

client = hazelcast.HazelcastClient()

m = client.get_map("test").blocking()
m.set("a", "b")
m.set("c", "d")
m.set("e", "f")

print("before execute on entries")
print(m.entry_set())
print("Result: ", m.execute_on_entries(SomeProcessor()))
print("after execute on entries")
print(m.entry_set())

e = client.get_executor("e").blocking()
print(e.execute_on_all_members(SomeCallable()))

client.shutdown()