#! usr/bin/python
from nd_service_registry import KazooServiceRegistry

from server import Server

import sys
import numpy as np
import logging
import pprint
import dill
from pathos.multiprocessing import ProcessPool as Pool #ThreadingPool as Pool #ProcessPool as Pool

logging.basicConfig(level=logging.INFO)

INT_MAX = np.int64(2 ** 63 - 1)

class Cluster:
	def __init__(self, num_servers=4):
		self.num_servers = num_servers

		self.nd = KazooServiceRegistry(rate_limit_calls=None)
		self.nd.set_data('/cluster/meta', data={'master': '', 'status' :  'offline', 'size' : num_servers})
		
		pool = Pool(num_servers)
		server_list = pool.map(lambda x: Server(server_id=x), list(range(num_servers)))
		self.server_list = { i : server_list[i] for i in range(len(server_list)) }

		self.server_mapping = {} # for debuging purposes
		for i in range(num_servers):
			primary = ':'.join(list(map(str, self.nd.get('/cluster/mapping/%d/primary' %i)['data']['address'])))
			secondary = ':'.join(list(map(str, self.nd.get('/cluster/mapping/%d/secondary' %i)['data']['address'])))
			
			self.server_mapping[i] = { 'primary' : primary, 'secondary' : secondary }


	def get_master(self):
		return tuple(self.nd.get('/cluster/meta')['data']['master'])


	def get_hash_partition(self, string):
		return (np.int64(hash(string)) & INT_MAX) % self.num_servers



if __name__ == "__main__":
	if len(sys.argv) > 1:
		num_servers = int(sys.argv[1])
	else:
		num_servers = 3
		 
	cluster = Cluster(num_servers)

	for i in range(num_servers):
		primary = ':'.join(list(map(str, cluster.nd.get('/cluster/mapping/%d/primary' %i)['data']['address'])))
		secondary = ':'.join(list(map(str, cluster.nd.get('/cluster/mapping/%d/secondary' %i)['data']['address'])))
		
		pprint.pprint({'key' : i, 'primary' : primary, 'secondary' : secondary } )


