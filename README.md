## Simulating Distributed Key-Value Stores
_Written in Python 3_
***

Non-children nodes are data nodes...
Child nodes of data nodes are the ephmeral nodes


Current Status (Output for 10 server cluster):

{'key': 0,
 'primary': '192.168.1.117:59628',
 'secondary': '192.168.1.117:59624'}
{'key': 1,
 'primary': '192.168.1.117:59622',
 'secondary': '192.168.1.117:59629'}
{'key': 2,
 'primary': '192.168.1.117:59631',
 'secondary': '192.168.1.117:59630'}
{'key': 3,
 'primary': '192.168.1.117:59627',
 'secondary': '192.168.1.117:59623'}
{'key': 4,
 'primary': '192.168.1.117:59623',
 'secondary': '192.168.1.117:59631'}
{'key': 5,
 'primary': '192.168.1.117:59630',
 'secondary': '192.168.1.117:59622'}
{'key': 6,
 'primary': '192.168.1.117:59624',
 'secondary': '192.168.1.117:59626'}
{'key': 7,
 'primary': '192.168.1.117:59625',
 'secondary': '192.168.1.117:59627'}
{'key': 8,
 'primary': '192.168.1.117:59626',
 'secondary': '192.168.1.117:59628'}
{'key': 9,
 'primary': '192.168.1.117:59629',
 'secondary': '192.168.1.117:59625'}

(Data extracted from zookeeper using zookeeper handle)

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
* Implement Data integrity (this is only half-done)
* Simulate server death - either use interactive shell to stop a service, or use a flag and send a "resource temporarily unavailable for some time limit"
