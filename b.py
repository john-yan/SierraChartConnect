# This is a simple python client to streaming market data from Sierra Chart
# using the DTC protocol

import DTCProtocol_pb2 as DTC
import socket
import struct
import json
import threading
import time
from datetime import datetime
from termcolor import colored
import colorama
colorama.init()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 11099))

symbol='ESZ20'
exchange='CME'

def send(sock, json_obj):
    req = json.dumps(json_obj).encode("ascii");
    sock.sendall(req + b"\x00")

def recv(sock):
    msg = '';
    while True:
        c = sock.recv(1);
        if c == bytes(1):
            break;
        msg += c.decode(encoding="ascii");
    return json.loads(msg);

def heartbeat(sock):
    while True:
        time.sleep(5);
        send(sock, { "Type": DTC.HEARTBEAT });

with open('userpass') as f:
    username = f.readline().strip('\n')
    password = f.readline().strip('\n')

req = {
    "Type": DTC.LOGON_REQUEST,
    "ProtocolVersion": DTC.CURRENT_VERSION,
    "Username": username,
    "Password": password,
    "HeartbeatIntervalInSeconds": 5,
    "ClientName": "hello"
}

send(s, req);
print(json.dumps(recv(s), indent=4));

t = threading.Thread(target=heartbeat, args=(s,))
t.daemon = True
t.start()

mktdatareq = {
    "Type": DTC.MARKET_DATA_REQUEST,
    "RequestAction": DTC.SUBSCRIBE,
    "SymbolID": 1,
    "Symbol": symbol,
    "Exchange": exchange
}

send(s, mktdatareq);

bidask = open("{symbol}-bidask.csv".format(symbol=symbol), "w", buffering=128)
trade = open("{symbol}-trade.csv".format(symbol=symbol), "w", buffering=128)

while True:
    res = recv(s)
    if res['Type'] == 117:
        dt = str(datetime.fromtimestamp(res['DateTime']))
        bidask.write("{symbol},{datetime},{bid},{bvol},{ask},{avol}\n".format(
            symbol=symbol,
            datetime=dt,
            bid=res['BidPrice'],
            bvol=res['BidQuantity'],
            ask=res['AskPrice'],
            avol=res['AskQuantity']))
    elif res['Type'] == 112:
        dt = str(datetime.fromtimestamp(res['DateTime']))
        csv_str = "{symbol},{datetime},{atbidorask},{price},{vol}".format(
            symbol=symbol,
            datetime=dt,
            atbidorask=colored("AtBid", 'red') if res["AtBidOrAsk"] == 1 else colored("AtAsk", 'green'),
            price=res['Price'],
            vol=res['Volume'])
        print(csv_str);
        trade.write("{symbol},{datetime},{atbidorask},{price},{vol}\n".format(
            symbol=symbol,
            datetime=dt,
            atbidorask="AtBid" if res["AtBidOrAsk"] == 1 else "AtAsk",
            price=res['Price'],
            vol=res['Volume']));
    else:
        print(json.dumps(res))

