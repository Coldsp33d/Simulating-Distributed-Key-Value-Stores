from nd_service_registry import KazooServiceRegistry

import socket
import pprint
import json
import threading
import time
import numpy as np
import random
import os

INT_MAX = np.int64(2 ** 63 - 1)


def get_socket(server_address=('', 0), sock_type='tcp'):
    if sock_type == 'tcp':
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_socket.bind(server_address)
        return tcp_socket
    
    else:
        pass # not implemented, nor will there be a need


def get_partition(string, num_servers):
    return (np.int64(hash(string)) & INT_MAX) % num_servers




class Server:
    __buf_size = 1024

    __SUCCESS = "200 OK"
    __BAD_REQ = "400 Bad Request"
    __NOT_FOUND = "404 Not Found"
    __INT_ERROR = "500 Internal Server Error"

    __timeout = 8 # registration timeout


    def __init__(self, server_id, zookeeper_handler, server_address=('', 0), buf_size=None):
        self.data_dict = {}
        self.server_id = server_id
        self.zookeeper_handler = zookeeper_handler
        self.buf_size = buf_size if buf_size else self.__buf_size

        self.socket = get_socket(server_address)
        self.socket.listen(100)
        
        self.server_address = (self.IP, self.port) = socket.gethostbyname(
                                                                        socket.gethostname()
                                                                        ), \
                                                            self.socket.getsockname()[1]
        self.name = self.IP + ":" + str(self.port)

        self.zookeeper_handler.set_node('/cluster/servers/' + ':'.join(list(map(str, self.server_address))) )
        
        np.random.seed(os.getpid())

        if(self.server_id == 1):
            self.zookeeper_handler.set_node('/cluster', data=self.zookeeper_handler.get('/cluster')['data'].update(
                        {   'master' : list(self.server_address) , 
                            'status' :  'initializing' 
                        }))
            pprint.pprint(self.zookeeper_handler.get('/cluster')['data'])
        elif(self.server_id == 2):
            time.sleep(5)
            pprint.pprint(self.zookeeper_handler.get('/cluster')['data'])

        #time.sleep(np.random.randint(low=0, high=5)) # random sleep for x seconds 

        time.sleep(100)
        is_master = False

        with self.zookeeper_handler.get_lock('/cluster/lock'):
            pprint.pprint(self.zookeeper_handler.get('/cluster')['data']['master'])
            master_address = self.zookeeper_handler.get('/cluster')['data']['master']
           
            if master_address == '':
                self.zookeeper_handler.set_node('/cluster', data=self.zookeeper_handler.get('/cluster')['data'].update(
                        {   'master' : list(self.server_address) , 
                            'status' :  'initializing' 
                        })) # better practice to update the existing dict rather than reinitialize it with a possible loss of data
                is_master = True


        if is_master:  
            print("{server}: Assigned role of MASTER".format(server=self.name)) 
            self.name = '[MASTER] ' + self.name  
            self.slave_list = []
            self.__invoke_master_routine()

        else: 
            print("{server}: Assigned role of SLAVE".format(server=self.name)) 
            self.name = '[SLAVE]  ' + self.name
            self.__invoke_slave_routine(tuple(master_address))

        time.sleep(np.random.randint(low=0, high=5)) # random sleep for x seconds 

        with self.zookeeper_handler.get_lock('/cluster/lock'):
            data = self.zookeeper_handler.get('/cluster/mapping/' + str(self.primary_key))['data']
            data = data if data is not None else {}    
            data.update({'primary' : ':'.join(list(map(str, self.server_address)))})
            self.zookeeper_handler.set_node('/cluster/mapping/' + str(self.primary_key), data=data)

            data = self.zookeeper_handler.get('/cluster/mapping/' + str(self.secondary_key))['data']
            data = data if data is not None else {}
            data.update({'secondary' : ':'.join(list(map(str, self.server_address)))})
            self.zookeeper_handler.set_node('/cluster/mapping/' + str(self.secondary_key), data=data)


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
        self.start() # deploy master and listen for registrations

        print("{server}: Beginning registrations for slave nodes".format(server=self.name))
        
        self.accept_reg = True
        start = time.time()
        while  time.time() <= start + self.__timeout:
            pass
        self.accept_reg = False

        print("{server}: Stopping registration".format(server=self.name))

        self.zookeeper_handler.set_data('/cluster', data=self.zookeeper_handler.get('/cluster')['data'].update({'status' :  'ready'}))

        x, y = [i for i in range(len(self.slave_list) + 1)], [(i + 1) % len(self.slave_list) for i in range(len(self.slave_list) + 1)]

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


    def __invoke_slave_routine(self, master_address):
        # get the master address, and send a signal to the master that the other server is ready
        # wait for the ready signal, get server mappings from master and note it down
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

            if data['op'].lower() not in ["__reg__", "__config__", "get", "put"]:
                response = {'status' : self.__BAD_REQ }
                client_socket.sendall(json.dumps(response).encode('utf-8')) 
                continue

            op_code = data['op'].lower()

            # This code only applies to the master
            if op_code == "__reg__": 
                try:
                    if self.accept_reg is True:
                        self.slave_list.append(tuple(data['address']))
                        
                        print("{server}: Registering {client}".format(server=self.name, client=':'.join(list(map(str, data['address'])))))

                        response = {'status' : self.__SUCCESS }
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
                try:
                    self.data_dict.update(data['data'])
                    response = {'status' : self.__SUCCESS }
                    client_socket.sendall(json.dumps(response).encode('utf-8')) 
                
                except:
                    response = {'status' : self.__INT_ERROR }
                    client_socket.sendall(json.dumps(response).encode('utf-8'))
            
            elif op_code == "get":
                try:
                    if data['data'] in self.data_dict.keys():
                        response = {    'status'    :   self.__SUCCESS, 
                                        'data'      :   self.data_dict[data['data']] 
                                    }    
                    else:
                        response = {    'status'    :   self.__NOT_FOUND } 
                    client_socket.sendall(json.dumps(response).encode("utf-8"))
                
                except:
                    response = {'status' : self.__INT_ERROR }
                    client_socket.sendall(json.dumps(response).encode('utf-8'))

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
