# Simulating-Distributed-Key-Value-Stores

TODO: 

* Automatic port allocation needed - don't hardcode ports
* Integate with zookeeper - zookeeper coordination as part of the cluster or as part of the client
* Designation of masters - how should this be done?
* Distributing the range of keys across servers
* Designation of backup servers for appropriate range
* Hadoop's hashing function for keys... should make things more sane
* Implement Data integrity
* Simulate server ready and cluster ready ... variable sleep in Server class?
* simulate server death
* Implement Cluster class -- as a controller class to carry out cluster level initialization
    ..* has a list of servers
    
