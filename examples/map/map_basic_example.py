import hazelcast
import threading

if __name__ == "__main__":
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("my-map").blocking()

    # Fill the map
    my_map.set("1", "c")
    my_map.put("2", "Paris")
    my_map.put("3", "Istanbul3")
    my_map.put("4", "Istanbul4")
    my_map.put("5", "Istanbul5")
    my_map.put("6", "Istanbul6")
    my_map.put("7", "Istanbul7")
    my_map.put("8", "Istanbul8")
    my_map.put("9", "Istanbul9")
    my_map.put("0", "Istanbul0")

    print("Entry with key 3: {}".format(my_map.get("3").result()))

    print("Map size: {}".format(my_map.size().result()))

    # Print the map
    print("\nIterating over the map: \n")

    entries = my_map.entry_set().result()
    for key, value in entries:
        print("{} -> {}".format(key, value))

    client.shutdown()
