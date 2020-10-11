# simple historical data downloader
# this script will download tick-by-tick data from SC and save it as SYMBOL.csv file

import DTCProtocol_pb2 as DTC
from DTCClient import DTCClient
import socket
import json
import threading
from datetime import datetime
from termcolor import colored
import argparse
import colorama
colorama.init()

parser = argparse.ArgumentParser()
parser.add_argument('--userpass', "-i", default="userpass", help="Username and Password file.")
parser.add_argument('--address', "-a", default="192.168.130.103", help="IP Address of Sierra Chart instance")
parser.add_argument('--port', "-p", type=int, default=8888, help="Port number of Sierra Chart instance")
parser.add_argument('--symbol', "-s", required=True, help="Symbol Name")
parser.add_argument('--exchange', "-e", default="CME", help="Exchange Name")
parser.add_argument('--output', "-o", default=None, help="Output file name")

args = parser.parse_args()

ADDR = args.address
PORT = args.port
SYMBOL = args.symbol
EXCHANGE = args.exchange
OUTPUT = "%s.csv" % SYMBOL if args.output == None else args.output

print(SYMBOL)
'''
json format for incoming traffic:
{
    "Type": 803,
    "RequestID": 10,
    "StartDateTime": "2019-10-03 10:58:23",
    "OpenPrice": 0,
    "HighPrice": 2865,
    "LowPrice": 2859.5,
    "LastPrice": 2865,
    "Volume": 1,
    "NumTrades": 1,
    "BidVolume": 0,
    "AskVolume": 1,
    "IsFinalRecord": 0
}
'''

csv_format = "{StartDateTime},{OpenPrice},{HighPrice},{LowPrice},{LastPrice},{Volume},{NumTrades},{BidVolume},{AskVolume}\n"

class Downloader:

    def __init__(self, client):
        self.client = client
        self.max_json = 100
        self.fd = open(OUTPUT, 'w')
        self.done_msgs = 0

        # write csv header row
        self.fd.write("StartDateTime,OpenPrice,HighPrice,LowPrice,LastPrice,Volume,NumTrades,BidVolume,AskVolume\n")

    def json_handler(self, msg):

        if ("IsFinalRecord" in msg.keys() and msg['IsFinalRecord'] == 1):
            self.client.close()
            raise Exception("Done")
            return

        if msg['Type'] == 3:
            # heartbeat messages
            return

        if msg['Type'] != 803:
            print(colored("Unprocess MSG: " + json.dumps(msg), 'red'))
            return

        self.fd.write(csv_format.format(
            StartDateTime = msg["StartDateTime"],
            OpenPrice = msg["OpenPrice"],
            HighPrice = msg["HighPrice"],
            LowPrice = msg["LowPrice"],
            LastPrice = msg["LastPrice"],
            Volume = msg["Volume"],
            NumTrades = msg["NumTrades"],
            BidVolume = msg["BidVolume"],
            AskVolume = msg["AskVolume"]
        ));

        self.done_msgs += 1

        if self.done_msgs % 1000000 == 0:
            print("Has processed %d messages to up %s" % (self.done_msgs, str(datetime.fromtimestamp(msg['StartDateTime'])) if 'StartDateTime' in msg.keys() else "unknown-datetime"))

def Main():

    with open('userpass') as f:
        username = f.readline().strip('\n')
        password = f.readline().strip('\n')

    client = DTCClient()
    client.connect(ADDR, PORT)
    client.logon(username, password)

    # historical data request for symbol
    client.send_json_request({
        'Type': DTC.HISTORICAL_PRICE_DATA_REQUEST,
        'RequestID': 10,
        'Symbol': SYMBOL,
        'Exchange': EXCHANGE,
        'RecordInterval': DTC.INTERVAL_TICK,
        'StartDateTime': 0,
        'EndDateTime': 0,
        'MaxDaysToReturn': 0,
        'UseZLibCompression': 0
    })

    d = Downloader(client)
    client.run(d.json_handler)

if __name__ == "__main__":
    Main()


