import cluster
import socket
import random
import json

SERVER_ADDRESS = (IP, PORT) = '', 8888
BUFFER_SIZE = 1024

def get_socket(server_address=('', 0), sock_type='tcp'):
    if sock_type == 'tcp':
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_socket.bind(server_address)
        return tcp_socket
    
    else:
        pass # not implemented, nor will there be a need

class Client:
	__buf_size = 1024

	def __init__(self, buf_size=None):
		self.buf_size = buf_size if buf_size else self.__buf_size
		self.socket = get_socket()
		
		self.cluster = cluster.Cluster()
		self.cluster.master = self.cluster.get_master()

	def __del__(self):
		self.socket.close()

	def get_server(self, type='primary'):
		self.socket.connect(self.cluster.master)
		request = { 'op' 	: 'GET MAPPING', 
					'type'	: 'primary' 
					'key'	: str(key) 
				  }

		self.socket.sendall(json.dumps(request).encode('utf-8'))

		response = json.loads(self.socket.recv(self.buf_size).decode('utf-8'))
		return tuple(response['data']['address'])
		
	def put(self, key, value):


		request = { 'op' 	: 'PUT', 
					'data' 	: { 
								str(key) : str(value) 
								} 
				  }
		self.socket.sendall(json.dumps(request).encode('utf-8'))
		
		response = json.loads(self.socket.recv(self.buf_size).decode('utf-8'))
		
		return response

	def get(self, key):
		request = 	{	'op' 	: 	'GET', 
						'data' 	: 	str(key)
					}
		self.socket.sendall(json.dumps(request).encode('utf-8'))

		response = json.loads(self.socket.recv(self.buf_size).decode('utf-8'))
		
		return response

	def shutdown(self):
		request = {'op' : 'CLOSE'}

		self.socket.send(json.dumps(request).encode('utf-8'))	

if __name__ == "__main__":
	client = Client(server_address=SERVER_ADDRESS)


