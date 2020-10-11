
import DTCProtocol_pb2 as DTC
import socket
import struct
import json
from threading import Thread, Timer, Lock
from queue import Queue
import time
from datetime import datetime
from termcolor import colored
import colorama
colorama.init()

class DTCClient:

    HEARTBEAT_INTERNAL = 10

    def __init__(self):
        self.ip_addr = None
        self.port = None
        self.lock = Lock()
        self.msg_q = Queue(4096)
        self.json_q = Queue(4096)
        self.receiver_thread = None
        self.message_thread = None
        self.heartbeat_timer = None

    def send_json_request(self, json_obj):
        req = json.dumps(json_obj).encode("ascii");
        self.lock.acquire()
        self.sock.sendall(req + b"\x00")
        self.lock.release()

    def receiver(self):
        try:
            while True:
                msg = self.sock.recv(4096)
                if len(msg) == 0:
                    print(colored("Receiver handler done", 'green'));
                    break
                self.msg_q.put(msg)
        except Exception as err:
            print(colored("Receiver handler failed - %s" % repr(err), 'red'));

        # final signal for message handler to exit
        self.msg_q.put(b'')

    def message_to_json(self):

        msg = b''

        while True:

            msg += self.msg_q.get()

            if msg == b'':
                break

            while True:
                index = msg.find(b'\x00')
                if index != -1:
                    obj = json.loads(msg[0 : index].decode(encoding = 'ascii'))
                    self.json_q.put(obj);
                    msg = msg[index + 1:]
                else:
                    break

        print(colored("Message handler done", 'green'));

    def recv_json_response(self):
        msg = '';
        while True:
            c = self.sock.recv(1);
            if c == b'\x00':
                break;
            msg += c.decode(encoding="ascii");
        return json.loads(msg);

    def _heartbeat(self):
        try:
            while True:
                time.sleep(10)
                self.send_json_request({ "Type": DTC.HEARTBEAT });
        except Exception as err:
            print(colored("Heartbeat failed - %s" % repr(err), 'red'));

    def connect(self, ip_addr, port):

        self.ip_addr = ip_addr
        self.port = port
        self.sock = socket.create_connection((ip_addr, port))


    def logon(self, username, password, name = "hello"):
        req = {
            "Type": DTC.LOGON_REQUEST,
            "ProtocolVersion": DTC.CURRENT_VERSION,
            "Username": username,
            "Password": password,
            "HeartbeatIntervalInSeconds": 5,
            "ClientName": name
        }

        self.send_json_request(req);

        # start heartbeat after logon has been sent
        self.heartbeat_timer = Thread(target=self._heartbeat, daemon = True)
        self.heartbeat_timer.start()

        self.receiver_thread = Thread(target=self.receiver, daemon = True)
        self.receiver_thread.start()

        self.message_thread = Thread(target=self.message_to_json, daemon = True)
        self.message_thread.start()

    def close(self):

        if self.sock:
            self.sock.close()

    def run(self, handler):

        while True:
            res = self.json_q.get()
            handler(res)
