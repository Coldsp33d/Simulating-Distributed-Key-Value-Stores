import cluster
import socket
import random
import json

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

		self.cluster = cluster.Cluster()
		self.cluster.master = self.cluster.get_master()

	def __del__(self):
		self.socket.close()

	def __get_server(self, key, dtype='primary'):
		socket = get_socket()
		socket.connect(self.cluster.master)
		request = { 'op' 	: 'MAP', 
					'type'	: dtype, 
					'key'	: str(key) 
				  }

		socket.sendall(json.dumps(request).encode('utf-8'))

		response = json.loads(socket.recv(self.buf_size).decode('utf-8'))
		socket.close()

		return tuple(response['data']['address'])
		
	def put(self, key, value, address):
		socket = get_socket()
		address = self.__get_server(key, dtype='primary')

		try:
			socket.connect(address)
		except:
			secondary_address = self.__get_server(key, dtype='secondary')
			socket.connect(secondary_address)

		request = { 'op' 	: 'PUT', 
					'data' 	: { 
							str(key) : str(value) 
							} 
				  }
		socket.sendall(json.dumps(request).encode('utf-8'))		
		response = json.loads(socket.recv(self.buf_size).decode('utf-8'))
		socket.close()

		return response
		
	def get(self, key):
		socket = get_socket()
		address = self.__get_server(key, dtype='primary')

		try:
			socket.connect(address)
		except:
			secondary_address = self.__get_server(key, dtype='secondary')
			socket.connect(secondary_address)

		request = 	{	'op' 	: 	'GET', 
						'data' 	: 	str(key)
					}
		socket.sendall(json.dumps(request).encode('utf-8'))
		response = json.loads(socket.recv(self.buf_size).decode('utf-8'))
		socket.close()
		
		return response

if __name__ == "__main__":
	client = Client()


