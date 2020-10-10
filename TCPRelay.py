
import DTCProtocol_pb2 as DTC
import socket
import json
from threading import Lock, Thread
import time
from datetime import datetime
from queue import Queue
import logging
import sys
import signal
from termcolor import colored
import colorama


SC_PORT = 11099
# SC Address has to be 127.0.0.1, otherwise, connection will not be able to stream data
SC_ADDR = "127.0.0.1"

RELAY_PORT = 8888
RELAY_ADDR = "0.0.0.0"

logging.basicConfig(level=logging.INFO)
colorama.init()

print_lock = Lock()

def Print(*msgs):
    print_lock.acquire()
    print(*msgs)
    print_lock.release()

class Client:
    def __init__(self):
        self.client_to_sc_thread = None
        self.sc_to_client_thread = None
        self.sc_sock = None
        self.cl_sock = None
        self.index = 0

def ClientToSCThread(client):
    assert(client.sc_sock != None)
    assert(client.cl_sock != None)
    while True:
        try:
            msg = client.cl_sock.recv(2048)
            if len(msg) == 0:
                break
            Print(colored("MSG from CL(%d)" % client.index, 'green'))
            client.sc_sock.sendall(msg);
        except:
            Print(colored("Error on (%d), closing" % client.index, 'red'))
            client.cl_sock.close()
            client.sc_sock.close()
            break;

def SCToClientThread(client):
    assert(client.sc_sock != None)
    assert(client.cl_sock != None)
    while True:
        try:
            msg = client.sc_sock.recv(2048)
            Print(colored("MSG from SC(%d)" % client.index, 'green'))
            if len(msg) == 0:
                break
            client.cl_sock.sendall(msg);
        except:
            Print(colored("Error on (%d), closing" % client.index, 'red'))
            client.cl_sock.close()
            client.sc_sock.close()
            break;

def Main():

    # all connected clients
    clients = []
    client_index = 0

    # relay socket
    relay_server = socket.create_server((RELAY_ADDR, RELAY_PORT))
    relay_server.listen()
    Print(colored("Created relay server and listen on %s:%d" % (RELAY_ADDR, RELAY_PORT), "green"))

    while True:
        # check for dead clients
        dead_clients = []
        for client in clients:
            if (not client.client_to_sc_thread.is_alive()) and (not client.sc_to_client_thread.is_alive()):
                dead_clients.append(client)

        # cleanup resource and remove dead client from client list
        for client in dead_clients:
            client.client_to_sc_thread.join(timeout=0)
            client.sc_to_client_thread.join(timeout=0)
            client.cl_sock.close()
            client.sc_sock.close()
            clients.remove(client)
            Print(colored("Removed client %d from client list" % (client.index), "red"))

        # waiting on new client connections
        cl_sock, addr = relay_server.accept()

        # create new client
        client = Client()
        client.index = client_index
        client_index += 1
        client.cl_sock = cl_sock

        # create sc socket and connect to sc
        client.sc_sock = socket.create_connection((SC_ADDR, SC_PORT))

        # create client serving thread
        client.client_to_sc_thread = Thread(target=ClientToSCThread, args=(client, ))
        client.sc_to_client_thread = Thread(target=SCToClientThread, args=(client, ))
        client.client_to_sc_thread.daemon = True
        client.sc_to_client_thread.daemon = True

        # finally add client to client list
        clients.append(client)
        Print(colored("Created client %d" % (client.index), "red"))

        # start client thread
        client.client_to_sc_thread.start()
        client.sc_to_client_thread.start()

if __name__ == "__main__":
    Main()
