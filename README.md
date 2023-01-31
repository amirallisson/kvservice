#Key-Value Storage Service

<h2>Introduction</h2>

<p>
  This is a fault-tolerant key-value storage
  service that uses Raft consensus protocol.
  The key-value service will be structured as a replicated state machine 
  with several key-value servers that coordinate their activities
  through the Raft log. The key/value service can process client requests as long as a majority of the servers
  are alive and can communicate, in spite of other failures or
  network partitions.
</p>

<p>
  The system consists of clients and key/value servers,
  where each key/value server also acts as a Raft peer. Clients
  send <tt>Put()</tt>, <tt>Append()</tt>, and <tt>Get()</tt> RPCs
  to key/value servers (called kvraft servers), who then place
  those calls into the Raft log and execute them in order. A
  client can send an RPC to any of the kvraft servers, but if that 
  server is not currently a Raft leader, or if there's a failure, the 
  client retries by sending to a different server. If the 
  operation is committed to the Raft log (and hence applied to
  the key/value state machine), its result is reported to the
  client. If the operation failed to commit (for example, if the
  leader was replaced), the server reports an error, and the
  client retries with a different server.
</p>

<p>
  In the face of unreliable connections and node failures,
  clients may send RPCs multiple times until it finds a kvraft
  server that replies positively. The service ensures that each application call to
  <tt>Put()</tt> or <tt>Append()</tt> must appear in
  that order just once (i.e., write the key/value database just
  once) by implementing exactly-once semantics.
</p>


<p>
src/client.go contains the implementation of the client API for interacting with the server.
src/server.go contains the implementation of the server functionality.
 To test the service, run the following command:
<pre>
$ go test -v
=== RUN   TestBasic
Test: One client ...
  ... Passed
--- PASS: TestBasic (15.22s)
=== RUN   TestConcurrent
Test: concurrent clients ...
  ... Passed
--- PASS: TestConcurrent (15.83s)
=== RUN   TestUnreliable
Test: unreliable ...
  ... Passed
--- PASS: TestUnreliable (16.68s)
=== RUN   TestUnreliableOneKey
Test: Concurrent Append to same key, unreliable ...
  ... Passed
--- PASS: TestUnreliableOneKey (1.40s)
=== RUN   TestOnePartition
Test: Progress in majority ...
  ... Passed
Test: No progress in minority ...
  ... Passed
Test: Completion after heal ...
  ... Passed
--- PASS: TestOnePartition (2.54s)
=== RUN   TestManyPartitionsOneClient
Test: many partitions ...
  ... Passed
--- PASS: TestManyPartitionsOneClient (24.08s)
=== RUN   TestManyPartitionsManyClients
Test: many partitions, many clients ...
  ... Passed
--- PASS: TestManyPartitionsManyClients (26.12s)
=== RUN   TestPersistOneClient
Test: persistence with one client ...
  ... Passed
--- PASS: TestPersistOneClient (18.68s)
=== RUN   TestPersistConcurrent
Test: persistence with concurrent clients ...
  ... Passed
--- PASS: TestPersistConcurrent (19.34s)
=== RUN   TestPersistConcurrentUnreliable
Test: persistence with concurrent clients, unreliable ...
  ... Passed
--- PASS: TestPersistConcurrentUnreliable (20.37s)
=== RUN   TestPersistPartition
Test: persistence with concurrent clients and repartitioning servers...
  ... Passed
--- PASS: TestPersistPartition (26.91s)
=== RUN   TestPersistPartitionUnreliable
Test: persistence with concurrent clients and repartitioning servers, unreliable...
  ... Passed
--- PASS: TestPersistPartitionUnreliable (26.89s)
PASS
ok  kvraft 214.069s</pre>
</p>

