from nd_service_registry import KazooServiceRegistry

import socket
import random
import json
import time
import pprint
import sys

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

		self.master = tuple(KazooServiceRegistry().get('/cluster/meta')['data']['master'])

	def __get_server(self, key, dtype='primary'):
		socket = get_socket()
		socket.connect(self.master)
		request = { 'op' 	: 'MAP', 
					'type'	: dtype, 
					'key'	: str(key) 
				  }

		socket.sendall(json.dumps(request).encode('utf-8'))

		response = json.loads(socket.recv(self.buf_size).decode('utf-8'))
		socket.close()

		try:
			return tuple(response['data']['address'])
		except:
			raise Exception(str(response['status']))

	def put(self, key, value):
		socket = get_socket()
		address = self.__get_server(key, dtype='primary')

		try:
			socket.connect(address)
		except:
			socket.close()
			socket = get_socket()
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
			socket.close()
			socket = get_socket()
			secondary_address = self.__get_server(key, dtype='secondary')
			print secondary_address
			socket.connect(secondary_address)

		request = 	{	'op' 	: 	'GET', 
						'key' 	: 	str(key)
					}
		socket.sendall(json.dumps(request).encode('utf-8'))
		response = json.loads(socket.recv(self.buf_size).decode('utf-8'))
		socket.close()
		
		return response

if __name__ == "__main__":
	client = Client()

	states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming',

	    'AB': 'Alberta',
	    'BC': 'British Columbia',
	    'MB': 'Manitoba',
	    'NB': 'New Brunswick',
	    'NL': 'Newfoundland and Labrador',
	    'NT': 'Northwest Territories',
	    'NS': 'Nova Scotia',
	    'NU': 'Nunavut',
	    'ON': 'Ontario',
	    'PE': 'Prince Edward Island',
	    'QC': 'Quebec',
	    'SK': 'Saskatchewan',
	    'YT': 'Yukon'

}

if sys.argv[1] in ['--put', '-p']:
	for k, v in states.items():
		print(client.put(k, v))

elif sys.argv[1] in ['--get', '-g']:
	#print(client.get('SD'))
	print(client.get('AR'))
	#print(client.get('MD'))


