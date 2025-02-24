# High availability

## Number of servers

The recommended number of servers is 3 to 7 to balance between latency and
availability. This recommendation aligns with the principles of the Raft
consensus algorithm, which is designed to manage a replicated log across
multiple servers. 

In Raft, having an odd number of servers (typically 3, 5, or 7) is preferred to
ensure that a majority can be reached for consensus. This majority is crucial
for maintaining data consistency and fault tolerance. With 3 to 7 servers, the
system can tolerate up to 1 or 3 server failures respectively, while still
maintaining availability and ensuring that the remaining servers can continue to
operate and reach consensus.

## Raft parameters

The Raft parameters, such as heartbeat timeout and election timeout, can be
tuned to accommodate different scenarios. Adjusting these parameters allows for
optimization based on network conditions and server performance, ensuring
efficient leader election and stable cluster operation.