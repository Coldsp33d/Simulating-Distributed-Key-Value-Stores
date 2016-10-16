#! usr/bin/python
from nd_service_registry import KazooServiceRegistry

from server import Server
import logging
import pprint
import dill
from pathos.multiprocessing import ProcessPool as Pool #ThreadingPool as Pool #ProcessPool as Pool

logging.basicConfig(level=logging.INFO)

class Cluster:

	def __init__(self, num_servers=4):
		self.nd = KazooServiceRegistry(rate_limit_calls=None)
		
		self.nd.set_data('/cluster/meta', data={'master': '', 'status' :  'offline'})


		
		pool = Pool(num_servers)
		server_list = pool.map(lambda x: Server(server_id=x, zookeeper_handler=self.nd), list(range(num_servers)))
		
		#self.server_list = { i : server_list[i] for i in range(len(server_list)) }


if __name__ == "__main__":
	num_servers = 10
	cluster = Cluster(num_servers)

	for i in range(num_servers):
		primary = ':'.join(list(map(str, cluster.nd.get('/cluster/mapping/%d/primary' %i)['data']['address'])))
		secondary = ':'.join(list(map(str, cluster.nd.get('/cluster/mapping/%d/secondary' %i)['data']['address'])))
		
		pprint.pprint({'key' : i, 'primary' : primary, 'secondary' : secondary } )

