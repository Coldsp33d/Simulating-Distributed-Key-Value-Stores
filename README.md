## Simulating Distributed Key-Value Stores
_Written in Python 3_
***

Non-children nodes are data nodes...
Child nodes of data nodes are the ephmeral nodes

DONE
* Automatic port allocation needed - don't hardcode ports (DONE)
* Integate with zookeeper - zookeeper coordination as part of the cluster or as part of the client (DONE)
* Designation of masters - how should this be done? (DONE - it's random)
* Distributing the range of keys across servers  		----\
* Hadoop's hashing function for keys... should make things more sane (DONE - just need to apply it)
* Simulate server ready and cluster ready ... variable sleep in Server class? (DONE - its random sleep)
* Designation of backup servers for appropriate range	----- } these two are being done in one step, and this 
is where the error's happening
* Implement Cluster class -- as a controller class to carry out cluster level initialization (DONE)
    * has a list of servers which form the cluster 
    * cluster related properties 

TODO
* nd_service_registry is a very buggy zookeeper client. It doesn't work half the time. It's time to look for alternatives.
* Implement a socket for the cluster, let the cluster make all changes to the zookeeper structure
* Master uploads data to zookeeper's /cluster node 
* Implement Data integrity
* Simulate server death
