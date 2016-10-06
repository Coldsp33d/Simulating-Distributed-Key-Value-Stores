import socket
import random
import json

SERVER_ADDRESS = (IP, PORT) = '', 8888
BUFFER_SIZE = 1024


class Client:
	__buf_size = 1024

	def __init__(self, server_address, buf_size=None):
		self.buf_size = buf_size if buf_size else self.__buf_size
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.socket.connect(server_address)

	def __del__(self):
		self.socket.close()
		
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

	data = {int(random.random() * 819) :  int(random.random() * 821309) for _ in range(100)}

	for k, v in data.items():
		client.put(key=k, value=v)

	keylist = list(data.keys())
	random.shuffle(keylist)

	print(client.get(keylist[0]))
	client.shutdown()

