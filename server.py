""" TODO: 

Automatic port allocation needed - don't hardcode ports
Integate with zookeeper - zookeeper coordination as part of the cluster or as part of the client
Designation of masters - how should this be done?
Distributing the range of keys across servers
Designation of backup servers for appropriate range
Hadoop's hashing function for keys... should make things more sane
data integrity
simulate server ready ... variable sleep?
simulate server death


Cluster class -- is this s good way to do it? yes.. controller class

    has a list of servers
    

"""

import socket
import pprint
import json
import threading

SERVER_ADDRESS = (IP, PORT) = '', 8888

# -------------------------------------------------------------------------------------------

class Server:
    __buf_size = 1024

    __SUCCESS = "200 OK"
    __BAD_REQ = "400 Bad Request"
    __NOT_FOUND = "404 Not Found"
    __INT_ERROR = "500 Internal Server Error"

    def __init__(self, server_address, buf_size=None):
        self.data_dict = {}
        self.server_address = (self.IP, self.port) = server_address

        self.buf_size = buf_size if buf_size else self.__buf_size

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(server_address)
        self.socket.listen(10)

        self.name = socket.getfqdn(self.IP)

        print('{server}: Serving HTTP on port {port}...'.format(server=self.name, port=self.port))


    def __del__(self):
        print("{server}: Closing socket, thanks!".format(server=self.name))
        self.socket.close()


    def __serve_forever(self, silence):
        shutdown_signal = False

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
        self.__task = threading.Thread(target=self.__serve_forever, args=(silence, ))
        self.__task.start()
     

    def stop(self):
        self.__keep_alive = False
        self.__task.join()

# -------------------------------------------------------------------------------------------

if __name__ == "__main__":
    server = Server(SERVER_ADDRESS)
    server.start(silence=False)
