# SoakTest Test Description
The test setup at Hazelcast lab environment is:

1. Use 4 member cluster on 2 servers (2 members each).
2. The client test program exercises the following API calls. At most 32 active operations are allowed at any time.
    + Put/Gets
    + Predicates
    + Map Listeners
    + Entry Processors
    
3. Run 10 clients on one lab machine and this machine only runs clients (i.e. no server at this machine)

4. Run the tests for 48 hours. Verify that: 
    + Make sure that all the client processes are up and running before killing the clients after 48 hours.
    + Analyse the outputs: Make sure that there are no errors printed.

# Usage
1. Run start-server.sh on 2 servers 2 times to get 4 members.
2. Run start-client.sh on client lab machine 10 times with the following way:
    + sh start-client.sh [hour-limit] [addresses] [log-file-name] [log-file-index]
        + hour-limit: a float for the time limit of soak test; e.g. 48.0
        + addresses: list of addresses separated by -; e.g. 10.212.1.111:5701-10.212.1.111:5702
        + log-file-name: name of the log file; e.g. client-log
        + log-file-index: index of log file; e.g. 3
        
# Success Criteria
1. No errors printed.
2. All client processes are up and running after 48 hours with no problem.