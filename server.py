from nd_service_registry import KazooServiceRegistry

import socket
import pprint
import json
import threading
import time
import numpy as np
import random
import os


def get_socket(server_address=('', 0), sock_type='tcp'):
    if sock_type == 'tcp':
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_socket.bind(server_address)
        return tcp_socket
    
    else:
        pass # not implemented, nor will there be a need

# Pass an existing object in order to circumvent the overhead of re-creation and connection
def get_primary_server(key, root='/cluster', zk_handler=KazooServiceRegistry()):
    return tuple(zk_handler.get(root + '/mapping/' + str(key) + '/primary')['data']['address'])

def get_secondary_server(key, root='/cluster', zk_handler=KazooServiceRegistry()):
    return tuple(zk_handler.get(root + '/mapping/' + str(key) + '/secondary')['data']['address'])

def get_hash_partition(val, mod):
    return (np.int64(hash(val)) & INT_MAX) % mod

class Server:
    __buf_size = 1024

    __SUCCESS = "200 OK"
    __BAD_REQ = "400 Bad Request"
    __NOT_FOUND = "404 Not Found"
    __INT_ERROR = "500 Internal Server Error"

    __timeout = 15 # registration timeout


    def __init__(self, server_id, server_address=('', 0), buf_size=None):
        self.data_dict = {{ 'primary' : {} }, 'secondary' : {} }
        self.backup_data_dict = { 'primary' : {}, 'secondary' : {} }
        self.server_id = server_id
        self.zookeeper_handler = KazooServiceRegistry()
        self.buf_size = buf_size if buf_size else self.__buf_size
        self.
        self.socket = get_socket(server_address)
        self.socket.listen(100)

        self.config_list = None
        
        self.server_address = (self.IP, self.port) = socket.gethostbyname(
                                                                        socket.gethostname()
                                                                        ), \
                                                            self.socket.getsockname()[1]
        self.name = self.IP + ":" + str(self.port)

        self.zookeeper_handler.set_data('/cluster/servers/' + ':'.join(list(map(str, self.server_address))) )
        
        np.random.seed(os.getpid())
        time.sleep(np.random.randint(low=0, high=10)) # random sleep for x seconds 

        self.is_master = False

        with self.zookeeper_handler.get_lock('/cluster/lock', wait=2) as lock:
            master_address = self.zookeeper_handler.get('/cluster/meta')['data']['master']

            if master_address == '':
                self.zookeeper_handler.set_node('/cluster/meta', data=
                        {   'master' : list(self.server_address) , 
                            'status' :  'initializing' 
                        }) # better practice to update the existing dict rather than reinitialize it with a possible loss of data
                self.is_master = True

        if self.is_master:  
            print("{server}: Assigned role of MASTER".format(server=self.name)) 
            self.name = '[MASTER] ' + self.name  
            self.slave_list = []
            self.__invoke_master_routine()

        else: 
            print("{server}: Assigned role of SLAVE".format(server=self.name)) 
            self.name = '[SLAVE]  ' + self.name
            self.__invoke_slave_routine(tuple(master_address))
        
        time.sleep(np.random.randint(low=0, high=3)) 

        print("{server}: Updating mapping...".format(server=self.name)) 

        data = {'address' : list(self.server_address) }
        self.zookeeper_handler.set_data('/cluster/mapping/' + str(self.primary_key) + '/primary', data=data)
        self.zookeeper_handler.set_data('/cluster/mapping/' + str(self.secondary_key) + '/secondary', data=data)

        self.cluster_size = self.zookeeper_handler.get('/cluster/meta')['data']['size']


    def __del__(self):
        print("{server}: Shutting down, thanks!".format(server=self.name))
        self.socket.close()


    def __invoke_master_routine(self):
        # now listen on a socket for signals from other servers... write a new function to do this and in a separate thread

        """ 1.  call Threading.Thread on the function, wait for x minutes
            2.  call join
            3.  set cluster status to ready
            4.  for each slave that registered, 
                    send a start signal with info on which [ps]keys to store and where 
                    
                    set the key to server mapping in zookeeper too
                    self.nd.set_node('/cluster/mapping/' + str(num), data={'primary': ..., 'secondary', ... })
        """
        self.op_list = ["__reg__", "__config__", "get mapping", "get", "put"]

        self.start() # deploy master and listen for registrations

        print("{server}: Beginning registrations for slave nodes".format(server=self.name))
        
        self.accept_reg = True
        start = time.time()
        while  time.time() <= start + self.__timeout:
            pass
        self.accept_reg = False

        print("{server}: Stopping registration".format(server=self.name))

        self.zookeeper_handler.set_data('/cluster/meta', data=self.zookeeper_handler.get('/cluster/meta')['data'].update({'status' :  'ready'}))

        x, y = [i for i in range(len(self.slave_list) + 1)], [(i + 1) % (len(self.slave_list) + 1) for i in range(len(self.slave_list) + 1)]

        while True:
            random.shuffle(x), random.shuffle(y)
            reshuffle = False

            for i in range(len(x)):
                if x[i] == y[i]:
                    reshuffle = True
                    break

            if not reshuffle: 
                break

        key_list = zip(x, y)

        self.primary_key = key_list[0][0]
        self.secondary_key = key_list[0][1]

        for i, slave_address in enumerate(self.slave_list):
            temp_socket = get_socket() # will shutdown the existing socket and recreate it with the same address
            
            print("{server}: Configuring {client}".format(server=self.name, client=':'.join(list(map(str, self.server_address))) ))

            temp_socket.connect(slave_address)

            data = {'op' : '__config__', 'primary' : key_list[i + 1][0], 'secondary' : key_list[i + 1][1] }
            temp_socket.sendall(json.dumps(data).encode('utf-8'))
            _ = temp_socket.recv(self.buf_size) # don't continue until the slave acknowledges the request and closes the connection
            temp_socket.close() # just to be on the safe side

        self.zookeeper_handler.set_data('/cluster/meta', data=self.zookeeper_handler.get('/cluster/meta')['data'].update({'size' : len(self.slave_list) + 1 }))



    def __invoke_slave_routine(self, master_address):
        # get the master address, and send a signal to the master that the other server is ready
        # wait for the ready signal, get server mappings from master and note it down
        self.op_list = ["__config__", "get mapping", "get", "put"]

        temp_socket = get_socket()
        temp_socket.connect(master_address)

        data = {'op' : '__reg__', 'address' : [self.IP, self.port] }
        
        temp_socket.sendall(json.dumps(data).encode('utf-8'))
        _ = temp_socket.recv(self.buf_size)
        temp_socket.close()

        self.start()

        print("{server}: Waiting for config info from master...".format(server=self.name))

        self.configured = False

        while not self.configured:
            pass


    def __listen_forever(self):
        while self.__keep_alive:
            (client_socket, client_address) = self.socket.accept()

            data = client_socket.recv(self.buf_size)
            try:
                data = json.loads(data.decode('utf-8'))
            except:
                response = {'status' : self.__BAD_REQ }
                client_socket.sendall(json.dumps(response).encode('utf-8'))
                continue

            if data['op'].lower() not in self.op_list:
                response = {'status' : self.__BAD_REQ }
                client_socket.sendall(json.dumps(response).encode('utf-8')) 
                continue

            op_code = data['op'].lower()

            # This code only applies to the master
            if op_code == "__reg__": 
                try:
                    if self.is_master and self.accept_reg:
                        self.slave_list.append(tuple(data['address']))
                        
                        print("{server}: Registering {client}".format(server=self.name, client=':'.join(list(map(str, data['address'])))))

                        response = {'status' : self.__SUCCESS }
                        client_socket.sendall(json.dumps(response).encode('utf-8')) 
                    else:
                        raise Exception
                
                except:
                    response = {'status' : self.__BAD_REQ }
                    client_socket.sendall(json.dumps(response).encode('utf-8')) 

            if op_code == "get mapping": 
                try:
                    if self.is_master:
                        response = {    'status'    :   self.__SUCCESS, 
                                        'data'      :   {'address' : '' } 
                                    }
                        if data['type'] == 'primary':
                            response['data']['address'] = list(get_primary_server(data['key']))
                        elif data['type'] == 'secondary':
                            response['data']['address'] = list(get_secondary_server(data['key']))
                        else:
                            raise Exception
                        client_socket.sendall(json.dumps(response).encode('utf-8'))         
                    else:
                        raise Exception

                except:
                    response = {'status' : self.__BAD_REQ }
                    client_socket.sendall(json.dumps(response).encode('utf-8')) 

            # This code only applies to the slave
            elif op_code == "__config__":  
                try:
                    print("{server}: Received config info from master".format(server=self.name))
                    self.primary_key = data['primary']
                    self.secondary_key = data['secondary']
                    response = {'status' : self.__SUCCESS }
                    client_socket.sendall(json.dumps(response).encode('utf-8')) 
                    self.configured = True
                except:
                    response = {'status' : self.__INT_ERROR }
                    client_socket.sendall(json.dumps(response).encode('utf-8'))                    

            elif op_code == "put":
                """ Format: { 'op' : ..., 'key' : ..., 'value' : ... } """
                    try:
                        if data
                        hashed_key = get_hash_partition(data['key'], self.cluster_size)

                        if hashed_key == self.primary_key:
                            # save it... queue it to be sent to secondary server
                            # if the size of the saved dict exceeds some value, create a thread and send everything... requests to be blocked or... ? that depends on how much i'm willing to code


                        elif hashed_key == self.secondary_key:
                            pass # for now

                        else:
                            raise Exception
                        
                        self.data_dict.update(data['data'])
                        response = {'status' : self.__SUCCESS }
                        client_socket.sendall(json.dumps(response).encode('utf-8')) 
                    
                    except:
                        response = {'status' : self.__BAD_REQ, 'message' : 'not responsible for key' }
                        client_socket.sendall(json.dumps(response).encode('utf-8'))

            elif op_code == "get":
                pass



            client_socket.close()


    def start(self):
        self.__keep_alive = True
        self.service_task = threading.Thread(target=self.__listen_forever)
        self.service_task.daemon = True
        self.service_task.start()
     

    def stop(self):
        self.__keep_alive = False
        self.service_task.join(3)

# -------------------------------------------------------------------------------------------

if __name__ == "__main__":
    SERVER_ADDRESS = (IP, PORT) = '', 8888

    server = Server(SERVER_ADDRESS)
    server.start(silence=False)
