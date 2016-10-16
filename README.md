## Simulating Distributed Key-Value Stores
_Written in Python 3_
***

Non-children nodes are data nodes...
Child nodes of data nodes are the ephmeral nodes

DONE
* Automatic port allocation needed - don't hardcode ports (DONE)
* Implement Cluster class -- as a controller class to carry out cluster level initialization (DONE)
    * has a list of servers which form the cluster 
    * cluster related properties 
* Integate with zookeeper - zookeeper coordination as part of the cluster or as part of the client (DONE)
* Designation of masters - how should this be done? (DONE - it's random)
* Hadoop's hashing function for keys... should make things more sane (DONE - just need to apply it)
* Simulate server ready and cluster ready ... variable sleep in Server class? (DONE - its random sleep)
* Distributing the range of keys across servers  		----\
* Designation of backup servers for appropriate range	----- } (DONE) 
* Migration from multithreading to multiprocessing - less flaky now (DONE)
* Master uploads data to zookeeper's /cluster node  (DONE - actually every server uploads their own server mapping)

TODO
* Implement Data integrity
* Simulate server death
