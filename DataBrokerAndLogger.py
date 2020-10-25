#!python3

# This implements DataBroker and DataLogger.
# This script will pull realtime market data from Sierria Chart using the DTC protocol
# and stream the data to multiple clients connecting to it. And at the same time also
# logs all the data to a file


import DTCProtocol_pb2 as DTC
from DTCClient import DTCClient
import socket
import json
import threading
from datetime import datetime
from termcolor import colored
import argparse
import colorama
from queue import Queue

class StreamClient:

    def __init__(self, conn, index):
        self.conn = conn
        self.handler = threading.Thread(target=self.client_thread, daemon=True)
        self.index = index
        self.messages = Queue()

        self.handler.start()
        print(colored("Client %d is connected." % self.index, 'green'))

    def push(self, msg):
        self.messages.put_nowait(msg)

    def client_thread(self):
        try:
            while True:
                msg = self.messages.get()
                self.conn.sendall(msg.encode('ascii') + b"\00")
        except Exception as e:
            self.conn.close()
            print(colored("Client %d exits with exception %s" % (self.index, e), 'red'))

    def is_alive(self):
        return self.handler.is_alive()

class DataBroker:

    def __init__(self, dtc, server_addr, server_port, logFile, isAppend=True):
        self.dtc = dtc

        print(colored('Creating server and listen on %s:%d' % (server_addr, server_port), 'green'))
        self.sock = socket.create_server((server_addr, server_port))
        self.thread = threading.Thread(target=self.server_thread, daemon=True)

        self.clients = []
        self.client_index = 0
        self.lock = threading.Lock()

        mode = 'a+' if isAppend else 'w+'
        self.logFD = open(logFile, mode)

        self.thread.start()

    def server_thread(self):

        self.sock.listen()

        print(colored('Server is ready for accepting connections', 'green'))
        while True:
            conn, addr = self.sock.accept()

            client = StreamClient(conn, self.client_index)
            self.client_index += 1

            self.lock.acquire()
            self.clients.append(client)
            self.lock.release()

    def message_handler(self, msg_json):

        msg_dump = json.dumps(msg_json)

        dead_clients = []
        self.lock.acquire()

        for client in self.clients:
            if not client.is_alive():
                dead_clients.append(client)
            else:
                client.push(msg_dump)

        for client in dead_clients:
            self.clients.remove(client)

        self.lock.release()

        self.logFD.write(msg_dump + '\n')
        self.logFD.flush()

def Main():

    colorama.init()

    parser = argparse.ArgumentParser()
    parser.add_argument('--userpass', "-i", default="userpass", help="Username and Password file.")
    parser.add_argument('--address', "-a", default="192.168.130.103", help="IP Address of Sierra Chart instance")
    parser.add_argument('--port', "-p", type=int, default=11199, help="Port number of Sierra Chart instance")
    parser.add_argument('--symbol', "-s", required=True, help="Symbol Name")
    parser.add_argument('--exchange', "-e", default="CME", help="Exchange Name")
    parser.add_argument('--logFile', "-f", default=None, help="Output file name")
    parser.add_argument('--append', default=True, help="Do we append to output file")
    parser.add_argument('--serverPort', type=int, default=1234, help="Server listening port")
    parser.add_argument('--serverAddress', default="0.0.0.0", help="Server IP Address")

    args = parser.parse_args()

    ADDR = args.address
    PORT = args.port
    SYMBOL = args.symbol
    EXCHANGE = args.exchange
    LOGFILE = "%s.log" % SYMBOL if args.logFile == None else args.logFile


    with open('userpass') as f:
        username = f.readline().strip('\n')
        password = f.readline().strip('\n')

    dtc = DTCClient()
    dtc.connect(ADDR, PORT)
    dtc.logon(username, password)

    # historical data request for symbol
    dtc.send_json_request({
        "Type": DTC.MARKET_DATA_REQUEST,
        "RequestAction": DTC.SUBSCRIBE,
        "SymbolID": 1,
        "Symbol": SYMBOL,
        "Exchange": EXCHANGE
    })

    broker = DataBroker(dtc, args.serverAddress, args.serverPort, LOGFILE, args.append)
    dtc.run(broker.message_handler)

if __name__ == "__main__":
    Main()


