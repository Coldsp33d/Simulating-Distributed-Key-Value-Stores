#! usr/bin/python

from nd_service_registry import KazooServiceRegistry
from server import Server
import logging
import pprint

logging.basicConfig(level=logging.DEBUG)

def split(a, n):
    k, m = len(a) / n, len(a) % n
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in xrange(n))

class Cluster:
	def __init__(self):
		self.nd = KazooServiceRegistry()
		self.server_list = { i + 1 : Server() for i in range(4) }
		
		chunks = list(split(range(99999), len(self.server_list))) # hardcoding range
		for num, server in self.server_list.items():

			self.nd.set_node('/services/ssh/' + ':'.join(list(map(str, server.server_address))), data={'range' : str(min(chunks[0])) + '-' + str(max(chunks[0])) })

if __name__ == "__main__":
	cluster = Cluster()

	pprint.pprint(cluster.nd.get('/services/ssh'))