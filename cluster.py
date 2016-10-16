#! usr/bin/python
from nd_service_registry import KazooServiceRegistry

from server import Server
import logging
import pprint
import dill
from pathos.multiprocessing import ProcessingPool as Pool #ThreadingPool as Pool

logging.basicConfig(level=logging.INFO)

class Cluster:

	def __init__(self, num_servers=4):
		self.nd = KazooServiceRegistry()
		
		self.nd.set_data('/cluster', data={'master': '', 'status' :  ''})
		self.nd.set_data('/cluster/servers')
		self.nd.set_data('/cluster/mapping')
		
		pool = Pool(num_servers)
		server_list = pool.map(lambda x: Server(server_id=x, zookeeper_handler=self.nd), list(range(num_servers)))
		
		self.server_list = { i : server_list[i] for i in range(len(server_list)) }


if __name__ == "__main__":
	cluster = Cluster()
	pprint.pprint(cluster.nd.get('/cluster/mapping/0'))
	pprint.pprint(cluster.nd.get('/cluster/mapping/1'))
	pprint.pprint(cluster.nd.get('/cluster/mapping/2'))
	pprint.pprint(cluster.nd.get('/cluster/mapping/3'))