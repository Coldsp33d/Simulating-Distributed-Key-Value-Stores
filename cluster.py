#! usr/bin/python
from nd_service_registry import KazooServiceRegistry

from server import Server
import logging
import pprint
import dill
from pathos.multiprocessing import ThreadPool as Pool

logging.basicConfig(level=logging.INFO)

class Cluster:

	def __init__(self, num_servers=4):
		self.nd = KazooServiceRegistry()
		
		self.nd.set_data('/cluster/', data={'master': '', 'status' :  ''})
		self.nd.set_data('/cluster/servers')
		self.nd.set_data('/cluster/mapping')
		
		pool = Pool(num_servers)
		server_list = pool.map(lambda x: Server(server_id=x, zookeeper_handler=self.nd), list(range(num_servers)))
		
		self.server_list = { i : server_list[i] for i in range(len(server_list)) }


if __name__ == "__main__":
	cluster = Cluster()
	k1 = cluster.nd.get('/cluster/')
	k2 = cluster.nd.get('/cluster/servers/')
	k3 = cluster.nd.get('/cluster/mapping/')

	print 

	pprint.pprint(k1)
	pprint.pprint(k2)
	pprint.pprint(k3)