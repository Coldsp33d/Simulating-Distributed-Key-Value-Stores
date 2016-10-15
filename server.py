import socket
import pprint
import json
import threading
import time
import numpy as np

INT_MAX = np.int64(2 ** 63 - 1)


def create_socket(server_address, sock=None):
    try:
        sock.close()    
    except: 
        pass

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(server_address)
    return sock


def get_partition(string, num_servers):
    return (np.int64(hash(string)) & INT_MAX) % num_servers




class Server:
    __buf_size = 1024

    __SUCCESS = "200 OK"
    __BAD_REQ = "400 Bad Request"
    __NOT_FOUND = "404 Not Found"
    __INT_ERROR = "500 Internal Server Error"

    __timeout = 30 # seconds before not accepting any more child server registrations


    def __init__(self, server_id, zookeeper_handler, server_address=('', 0), buf_size=None):
        self.data_dict = {}
        self.server_id = server_id
        self.zookeeper_handler = zookeeper_handler
        self.buf_size = buf_size if buf_size else self.__buf_size

        self.socket = create_socket(server_address)

        self.server_address = (self.IP, self.port) = socket.gethostbyname(
                                                                        socket.gethostname()
                                                                        ), \
                                                            self.socket.getsockname()[1]
        self.name = self.IP + ":" + str(self.port)

        self.zookeeper_handler.set_node('/cluster/servers/' + ':'.join(list(map(str, self.server_address))) )
        
        time.sleep(np.random.randint(low=0, high=10)) # seconds

        is_master = False
        with self.zookeeper_handler.get_lock('/cluster/', simultaneous=1, wait=0):
            master_address = self.zookeeper_handler.get('/cluster/')['data']['master']

            if master_address == '':
                self.zookeeper_handler.set_node('/cluster/', data=self.zookeeper_handler.get('/cluster/')['data'].update(
                        {   'master' : ':'.join(list(map(str, self.server_address))), 
                            'status' :  'initializing' 
                        })) # better practice to update the existing dict rather than reinitialize it with a possible loss of data

                is_master = True
                self.name = '[MASTER] ' + self.name
                print("{server}: Now assigned role of master".format(server=self.name)) 

            else:
                self.name = '[SLAVE] ' + self.name
                print("{server}: Now assigned role of slave".format(server=self.name)) 

        if is_master:    
            self.slave_list = []
            self.__invoke_master_routine()

        else: 
            self.__invoke_slave_routine(master_address)

        data = self.zookeeper_handler.get('/cluster/mapping/' + str(self.primary_key))['data']
        self.zookeeper_handler.set_node('/cluster/mapping/' + str(self.primary_key), data=data.update({'primary' : ':'.join(list(map(str, self.server_address)))} ))
        data = self.zookeeper_handler.get('/cluster/mapping/' + str(self.secondary_key))['data']
        self.zookeeper_handler.set_node('/cluster/mapping/' + str(self.secondary_key), data=data.update({'secondary' : ':'.join(list(map(str, self.server_address)))} ))


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
        self.socket.listen(10)

        start = time.time()
        while time.time() <= start + self.__timeout:
            (client_socket, client_address) = self.socket.accept()

            print("{server}: Registering {client}...".format(server=self.name, client=client_address))

            data = client_socket.recv(self.buf_size)
            try:
                data = json.loads(data.decode('utf-8'))
            except:
                response = {'status' : self.__BAD_REQ }
                client_socket.sendall(json.dumps(response).encode('utf-8'))
                continue

            response = {'status' : self.__SUCCESS }
            client_socket.sendall(json.dumps(response).encode('utf-8')) 
            client_socket.close()

            self.slave_list.append(tuple(data['address']))

        self.zookeeper_handler.set_data('/cluster/', data=self.nd.get('/cluster/')['data'].update({'status' :  'ready'
                            }))

        self.socket = create_socket(sock=self.socket) # will shutdown the existing socket and recreate it with the same address

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


        time.sleep(1)

        for i, slave_address in enumerate(self.slave_list):
            print("{server}: Sending config info to {client}...".format(server=self.name, client=slave_address))

            data = {'primary' : key_list[i + 1][0], 'secondary' : key_list[i + 1][1] }

            self.socket.connect(slave_address)
            self.socket.sendall(json.dumps(data).encode('utf-8'))
            _ = self.socket.recv(self.buf_size) # don't continue until the slave acknowledges the request and closes the connection


    def __invoke_slave_routine(self, master_address):
        # get the master address, and send a signal to the master that the other server is ready
        # wait for the ready signal, get server mappings from master and note it down
        data = {'address' : [self.IP, self.port] }

        self.socket.connect(master_address)
        self.socket.sendall(json.dumps(data).encode('utf-8'))
        _ = self.socket.recv(self.buf_size)

        self.socket = create_socket(sock=self.socket)
        self.socket.listen(1)

        (client_socket, client_address) = self.socket.accept()
        data = client_socket.recv(self.buf_size)
        try:
            data = json.loads(data.decode('utf-8'))
            response = {'status' : self.__SUCCESS }
            client_socket.sendall(json.dumps(response).encode('utf-8')) 
        except:
            response = {'status' : self.__BAD_REQ }
            client_socket.sendall(json.dumps(response).encode('utf-8'))
        finally:
            client_socket.close()      

        self.primary_key = data['primary']
        self.secondary_key = data['secondary']


    def __serve_forever(self, silence):
        shutdown_signal = False

        print('{server}: Serving on port {port}...'.format(server=self.name, port=self.port))

        while self.__keep_alive:
            (client_socket, client_address) = self.socket.accept()
            if not silence:
                print("{server}: Connected to {client}...".format(server=self.name, client=client_address))

            while True:
                data = client_socket.recv(self.buf_size)
                try:
                    data = json.loads(data.decode('utf-8'))
                except:
                    response = {'status' : self.__BAD_REQ }
                    client_socket.sendall(json.dumps(response).encode('utf-8'))
                    continue


                if data['op'].lower() not in ["get", "put", "close"]:
                    response = {'status' : self.__BAD_REQ }
                    client_socket.sendall(json.dumps(response).encode('utf-8')) 
                    continue

                op_code = data['op'].lower()

                if op_code == "put":
                    if not silence:
                        print("{server}: Received 'PUT' request".format(server=self.name))
                    
                    try:
                        self.data_dict.update(data['data'])
                        response = {'status' : self.__SUCCESS }
                        client_socket.sendall(json.dumps(response).encode('utf-8')) 
                    except:
                        response = {'status' : self.__INT_ERROR }
                        client_socket.sendall(json.dumps(response).encode('utf-8'))
                
                elif op_code == "get":
                    if not silence:
                        print("{server}: Received 'GET' request".format(server=self.name))
                    
                    try:
                        if data['data'] in self.data_dict.keys():
                            response = {    'status'    :   self.__SUCCESS, 
                                            'data'      :   self.data_dict[data['data']] 
                                        }    
                        else:
                            response = {    'status'    :   self.__NOT_FOUND } 
                        
                        client_socket.sendall(json.dumps(response).encode("utf-8"))
                    
                    except Exception as e:
                        response = {'status' : self.__INT_ERROR }
                        client_socket.sendall(json.dumps(response).encode('utf-8'))
                
                elif op_code == "close":
                    if not silence:
                        print("{server}: Received 'CLOSE' request".format(server=self.name))
                    
                    response = {'status' : self.__SUCCESS }
                    client_socket.sendall(json.dumps(response).encode('utf-8'))
                    client_socket.close()
                    break


    def start(self, silence=False):
        self.__keep_alive = True
        self.__service_task = threading.Thread(target=self.__serve_forever, args=(silence, ))
        self.__service_task.start()
     

    def stop(self):
        self.__keep_alive = False
        self.__service_task.join()

# -------------------------------------------------------------------------------------------

if __name__ == "__main__":
    SERVER_ADDRESS = (IP, PORT) = '', 8888

    server = Server(SERVER_ADDRESS)
    server.start(silence=False)
