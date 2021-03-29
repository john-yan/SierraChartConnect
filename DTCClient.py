
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
import  asyncio as aio
from aiofile import async_open
import argparse
colorama.init()

class DTCClient:

    HEARTBEAT_INTERNAL = 10

    def __init__(self, ignore_heartbeat = True):
        self.ip_addr = None
        self.port = None
        self.lock = Lock()
        self.msg_q = Queue(4096)
        self.json_q = Queue(4096)
        self.receiver_thread = None
        self.message_thread = None
        self.heartbeat_timer = None
        self.ignore_heartbeat = ignore_heartbeat

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
                    if self.ignore_heartbeat and obj['Type'] == 3:
                        pass
                    else:
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


class DTCClientAsync:

    HEARTBEAT_INTERNAL = 10

    def __init__(self, decode_message=True, ignore_heartbeat=True):
        self.ip_addr = None
        self.port = None
        self.queue = aio.Queue()
        self.sock_reader = None
        self.sock_writter = None
        self.heartbeat_task = None
        self.ignore_heartbeat = ignore_heartbeat if decode_message else False
        self.decode_message = decode_message

    async def send_json_request(self, json_obj):
        req = json.dumps(json_obj).encode("ascii");
        self.sock_writter.write(req + b"\x00")
        await self.sock_writter.drain()

    async def receiver(self):

        try:
            while True:
                msg = await self.sock_reader.readuntil(b'\x00')
                if len(msg) == 0:
                    print(colored("Receiver handler done", 'green'));
                    break
                assert(msg[-1] == 0)
                if self.decode_message:
                    obj = json.loads(msg[:-1].decode(encoding='ascii'))
                    if self.ignore_heartbeat and obj['Type'] == 3:
                        continue
                    await self.queue.put(obj)
                else:
                    await self.queue.put(msg)
        except Exception as err:
            print(colored("Receiver handler failed - %s" % repr(err), 'red'));

        await self.queue.put(b'')
        print(colored("Receiver exiting", 'red'));

    async def _heartbeat(self):
        try:
            while True:
                await aio.sleep(self.HEARTBEAT_INTERNAL)
                await self.send_json_request({ "Type": DTC.HEARTBEAT });
        except Exception as err:
            print(colored("Heartbeat failed - %s" % repr(err), 'red'));

    async def connect(self, ip_addr, port):

        self.ip_addr = ip_addr
        self.port = port
        self.sock_reader, self.sock_writter = await aio.open_connection(ip_addr, port)


    async def logon(self, username, password, name = "hello"):
        req = {
            "Type": DTC.LOGON_REQUEST,
            "ProtocolVersion": DTC.CURRENT_VERSION,
            "Username": username,
            "Password": password,
            "HeartbeatIntervalInSeconds": 5,
            "ClientName": name
        }

        await self.send_json_request(req);

        # start heartbeat after logon has been sent
        loop = aio.get_event_loop()
        self.heartbeat_task = loop.create_task(self._heartbeat())
        self.receiver_task = loop.create_task(self.receiver())

    async def close(self):
        if self.sock_writter:
            self.sock_writter.close()
            await self.sock_writter.wait_closed()


    async def messages(self):

        while True:
            res = await self.queue.get()
            if res == b'':
                return
            yield res


async def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--userpass', "-i", default="userpass", help="Username and Password file.")
    parser.add_argument('--address', "-a", default="192.168.122.142", help="IP Address of Sierra Chart instance")
    parser.add_argument('--port', "-p", type=int, default=11199, help="Port number of Sierra Chart instance")
    parser.add_argument('--symbol', "-s", required=True, help="Symbol Name")
    parser.add_argument('--exchange', "-e", default="CME", help="Exchange Name")
    parser.add_argument('--logFile', "-f", default='async-client.log', help="Output file name")
    parser.add_argument('--append', default=False, action='store_true', help="Do we append to output file?")

    args = parser.parse_args()

    ADDR = args.address
    PORT = args.port
    SYMBOL = args.symbol
    EXCHANGE = args.exchange

    async with async_open(args.userpass, 'r') as f:
        username = (await f.readline()).strip('\n')
        password = (await f.readline()).strip('\n')

    dtc = DTCClientAsync(False, False)
    await dtc.connect(ADDR, PORT)
    await dtc.logon(username, password)

    await dtc.send_json_request({
        "Type": DTC.MARKET_DATA_REQUEST,
        "RequestAction": DTC.SUBSCRIBE,
        "SymbolID": 1,
        "Symbol": SYMBOL,
        "Exchange": EXCHANGE
    })

    await dtc.send_json_request({
        "Type": DTC.MARKET_DEPTH_REQUEST,
        "RequestAction": DTC.SUBSCRIBE,
        "SymbolID": 1,
        "Symbol": SYMBOL,
        "Exchange": EXCHANGE,
        "NumLevels": 100
    })

    mode = 'a' if args.append else 'w'
    async with async_open(args.logFile, mode) as log:
        async for message in dtc.messages():
            assert(message[-1] == 0)
            message = message[:-1].decode('ascii')
            message += '\n'
            await log.write(message)

if __name__ == '__main__':
    loop = aio.get_event_loop()
    loop.run_until_complete(main())
