from nd_service_registry import KazooServiceRegistry

import socket
import pprint
import json
import threading
import time
import numpy as np
import random
import os
import copy
import sys

INT_MAX = np.int64(2 ** 63 - 1)

def get_socket(address=('', 0), sock_type='tcp'):
    if sock_type == 'tcp':
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_socket.bind(address)
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

""" ------------------------------------------------------------------------------------------- """

class KeyNotFoundException(Exception):
    pass

""" ------------------------------------------------------------------------------------------- """

class NotResponsibleForKeyException(Exception):
    pass

""" ------------------------------------------------------------------------------------------- """

class Server:
    __buf_size = 1024

    __SUCCESS = "200 OK"
    __BAD_REQ = "400 Bad Request"
    __NOT_FOUND = "404 Not Found"
    __INT_ERROR = "500 Internal Server Error"

    __timeout = 8 # registration timeout
    __backup_delay = 5 # delay between successive backups


    def __init__(self, server_id, server_address=('', 0), buf_size=None):
        self.data_dict = { 'primary' : {}, 'secondary' : {} }
        # primary:      the keys for which the given server is primary
        # secondary:    the keys for which the given server is secondary

        self.backup_data_dict = { 'primary' : {}, 'secondary' : {} }
        # primary:      the keys which have to be sent to the primary server when possible
        # secondary:    the keys which have to be sent to the secondary server when a limit is reached
        
        self.server_id = server_id
        self.zookeeper_handler = KazooServiceRegistry()
        self.buf_size = buf_size if buf_size else self.__buf_size

        self.config_list = None

        self.socket = get_socket(server_address)
        self.socket.listen(100)
        
        self.server_address = (self.IP, self.port) = socket.gethostbyname(
                                                                        socket.gethostname()
                                                                        ), \
                                                            self.socket.getsockname()[1]
        self.name = self.IP + ":" + str(self.port)

        self.zookeeper_handler.set_data('/cluster/servers/' + ':'.join(list(map(str, self.server_address))) )
        
        np.random.seed(os.getpid())
        time.sleep(np.random.randint(low=0, high=2)) # random sleep for x seconds 

        self.is_master = False

        with self.zookeeper_handler.get_lock('/cluster/lock', wait=2) as lock:
            master_address = self.zookeeper_handler.get('/cluster/meta')['data']['master']

            if master_address == '':
                data = {    'master' : list(self.server_address) , 
                            'status' : 'initializing', 
                            'size'   : self.zookeeper_handler.get('/cluster/meta')['data']['size']
                        }
                self.zookeeper_handler.set_node('/cluster/meta', data=data) 
                self.is_master = True
                print("{server}: Setting status to INITIALIZING".format(server=self.name))

        if self.is_master:  
            print("{server}: Assigned role of MASTER".format(server=self.name)) 
            self.name = '[MASTER] ' + self.name  
            self.slave_list = []
            
            Server.__master_routine(self)
            
        else: 
            self.name = '[SLAVE]  ' + self.name
            
            Server.__slave_routine(self, tuple(master_address))
        
        time.sleep(np.random.randint(low=0, high=3)) 

        print("{server}: Updating mapping".format(server=self.name)) 

        self.cluster_size = self.zookeeper_handler.get('/cluster/meta')['data']['size']
    
    # ------------------------------------------------------------------------------------- 

    def __del__(self):
        print("{server}: Shutting down, thanks!".format(server=self.name))
        self.socket.close()
    
    # -------------------------------------------------------------------------------------
    
    def __master_routine(self):
        # now listen on a socket for signals from other servers... write a new function to do this and in a separate thread

        """ 1.  call Threading.Thread on the function, wait for x minutes
            2.  call join
            3.  set cluster status to ready
            4.  for each slave that registered, 
                    send a start signal with info on which [ps]keys to store and where 
                    
                    set the key to server mapping in zookeeper too
                    self.nd.set_node('/cluster/mapping/' + str(num), data={'primary': ..., 'secondary', ... })
        """
        self.op_list = ["__reg__", "__config__", "map", "get", "put"]

        self.keep_alive = True
        self.service_task = threading.Thread(target=self.__listen_forever)
        self.service_task.start() # deploy master and listen for registrations

        print("{server}: Beginning registration".format(server=self.name))
        
        self.accept_reg = True
        start = time.time()
        while  time.time() <= start + self.__timeout:
            pass
        self.accept_reg = False

        print("{server}: Setting status to READY".format(server=self.name))

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

        for i, slave_address in enumerate([self.server_address] + self.slave_list):
            temp_socket = get_socket() # will shutdown the existing socket and recreate it with the same address
            
            print("{server}: Configuring {client}".format(server=self.name, client=':'.join(list(map(str, slave_address))) ))

            temp_socket.connect(slave_address)

            data = {'op' : '__config__', 'primary' : key_list[i][0], 'secondary' : key_list[i][1] }
            temp_socket.sendall(json.dumps(data).encode('utf-8'))
            _ = temp_socket.recv(self.buf_size) # don't continue until the slave acknowledges the request and closes the connection
            temp_socket.close() # just to be on the safe side

    # ------------------------------------------------------------------------------------- 

    def __slave_routine(self, master_address):
        # get the master address, and send a signal to the master that the other server is ready
        # wait for the ready signal, get server mappings from master and note it down
        self.op_list = ["__notify__", "__config__", "get", "put"]

        temp_socket = get_socket()
        temp_socket.connect(master_address)

        data = {'op' : '__reg__', 'address' : [self.IP, self.port] }
        
        temp_socket.sendall(json.dumps(data).encode('utf-8'))
        _ = temp_socket.recv(self.buf_size)
        temp_socket.close()

        self.keep_alive = True
        self.service_task = threading.Thread(target=self.__listen_forever)
        self.service_task.start() 

        self.configured = False

        while not self.configured:
            pass # wait for master to configure slave

    # -------------------------------------------------------------------------------------  

    def __listen_forever(self):
        def run_backup_service():
            def backup_data(entries, server_address, dtype):
                try:
                    socket = get_socket()
                    request = { 'op'    : 'PUT', 
                                'type'  : dtype,
                                'data'  :  entries  
                               }
                    socket.connect(server_address)                    
                    socket.sendall(json.dumps(request).encode('utf-8'))
                    _ = json.loads(socket.recv(self.buf_size).decode('utf-8'))
                except:
                    self.backup_data_dict[dtype].update(entries) # request couldn't be processed, put it back and keep it to send later
                finally:
                    socket.close()
                    return
            
            last_backup_timestamp = time.time() # setup backup 
            while True:
                if time.time() > last_backup_timestamp + self.__backup_delay:
                    if len(self.backup_data_dict['secondary']) > 0:
                        print("{server}: Backing up to secondary".format(server=self.name))  
                        entries = copy.copy(self.backup_data_dict['secondary'])
                        server_address = get_secondary_server(get_hash_partition(list(entries.keys())[0], self.cluster_size))
                        self.backup_data_dict['secondary'] = {}
                        backup_data(entries, server_address, 'secondary')

                    last_backup_timestamp = time.time()
            
        backup_service_thread = threading.Thread(target=run_backup_service)
        backup_service_thread.start()


        """ The giant mainloop begins here """
        while self.keep_alive: 
            (client_socket, client_address) = self.socket.accept()
            # print("{server}: Connected to {client}".format(server=self.name, client=client_address)) 

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

            elif op_code == "map": 
                print("{server}: MAP from {client}".format(server=self.name, client=client_address)) 
                try:
                    if self.is_master:
                        response = {    'status'    :   self.__SUCCESS, 
                                        'data'      :   {'address' : '' } 
                                    }
                        hashed_key = get_hash_partition(data['key'], self.cluster_size)

                        if data['type'] == 'primary':
                            response['data']['address'] = list(get_primary_server(hashed_key))
                        elif data['type'] == 'secondary':
                            response['data']['address'] = list(get_secondary_server(hashed_key))
                        else:
                            raise Exception
                        client_socket.sendall(json.dumps(response).encode('utf-8'))         
                    
                    else:
                        raise Exception

                except:
                    print "{server}:".format(server=self.name), sys.exc_info()[1]
                    response = {'status' : self.__BAD_REQ }
                    client_socket.sendall(json.dumps(response).encode('utf-8')) 

            elif op_code == "__config__":  
                try:
                    self.primary_key = data['primary']
                    self.secondary_key = data['secondary']

                    data = {'address' : list(self.server_address) }
                    self.zookeeper_handler.set_data('/cluster/mapping/' + str(self.primary_key) + '/primary', data=data)
                    self.zookeeper_handler.set_data('/cluster/mapping/' + str(self.secondary_key) + '/secondary', data=data)

                    response = {'status' : self.__SUCCESS }
                    client_socket.sendall(json.dumps(response).encode('utf-8')) 
                    self.configured = True
                except:
                    print "{server}:".format(server=self.name), sys.exc_info()[1]
                    response = {'status' : self.__INT_ERROR }
                    client_socket.sendall(json.dumps(response).encode('utf-8'))                    

            elif op_code == "put":
                """ Format: { 'op' : ..., 'key' : ..., 'value' : ... } """
                print("{server}: PUT from {client}".format(server=self.name, client=client_address)) 
                try:
                    # Assume one of the cluster servers sent this request
                    if data.has_key('type'):
                        if data['type'] == 'secondary': # data is meant to be backup
                            self.data_dict['secondary'].update(data['data']) 
                        elif data['type'] == 'primary': # happens if this server was down earlier, the secondary server is now sending this data 
                            self.data_dict['primary'].update(data['data'])
                    
                    else:
                        entry = copy.copy(data['data'])
                        hashed_key = get_hash_partition(data['data'].popitem()[0], self.cluster_size)

                        if hashed_key == self.primary_key:
                            self.data_dict['primary'].update(entry) # update it in the primary key list
                            self.backup_data_dict['secondary'].update(entry) # mark it for backup later 
                        elif hashed_key == self.secondary_key:
                            print("{server}: Secondary server servicing PUT request".format(server=self.name, client=client_address)) 
                            self.data_dict['secondary'].update(entry)
                            self.backup_data_dict['primary'].update(entry)
                        else:
                            raise NotResponsibleForKeyException
                    
                    response = {'status' : self.__SUCCESS }
                
                except NotResponsibleForKeyException:
                    response = {'status' : self.__BAD_REQ, 'message' : 'not responsible for key' }
                    
                except:
                    print "{server}:".format(server=self.name), sys.exc_info()[1]
                    response = {'status' : self.__INT_ERROR }
                    
                finally:
                    client_socket.sendall(json.dumps(response).encode('utf-8'))

            elif op_code == "get":
                print self.data_dict
                print("{server}: GET from {client}".format(server=self.name, client=client_address)) 
                try:
                    response = {    'status'    :   self.__SUCCESS, 
                                    'data'      :   ''
                                }
                    print data['key']
                    if self.data_dict['primary'].has_key(data['key']):
                        response['data'] = self.data_dict['primary'][data['key']] 
                    
                    elif self.data_dict['secondary'].has_key(data['key']):
                        response['data'] = self.data_dict['secondary'][data['key']] 
                                        
                    else:
                        raise KeyNotFoundException
                        
                except KeyNotFoundException:
                    response = {    'status'    :   self.__NOT_FOUND, 'message' : 'key not located' }   
                
                except:
                    print "{server}:".format(server=self.name), sys.exc_info()[1]
                    response = {'status' : self.__INT_ERROR }
                
                finally:
                    client_socket.sendall(json.dumps(response).encode('utf-8'))

            client_socket.close()   # finally, close the connection once everything's done

        backup_service_thread.join(0)

    # ------------------------------------------------------------------------------------- 

    def stop(self):
        if self.keep_alive:
            self.keep_alive = False

            self.socket.close()
            self.service_task.join(1)

""" ------------------------------------------------------------------------------------------- """
