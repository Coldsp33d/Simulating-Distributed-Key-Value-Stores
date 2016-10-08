#! usr/bin/python

from nd_service_registry import KazooServiceRegistry
import logging
import server

logging.basicConfig(level=logging.DEBUG)

class Cluster:
	def __init__(self):
		self.nd = nd_service_registry.KazooServiceRegistry()
		self.server_list = { i + 1 : server() for i in range(4) }
		
		for num, server in self.server_list.items():
			#np.set_node('/services/ssh/')
			pass
			
if __name__ == "__main__":
	pass