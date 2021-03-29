# simple historical data downloader
# this script will download tick-by-tick data from SC and save it as SYMBOL.csv file

import DTCProtocol_pb2 as DTC
from DTCClient import DTCClient, DTCClientAsync
import socket
import json
import threading
from datetime import datetime
from termcolor import colored
import argparse
import colorama
from aiofile import async_open
import numpy as np
import asyncio as aio
import pandas as pd
'''
raw json format for incoming traffic:
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

class Downloader:

    def __init__(self, client, outfile, output_raw):
        self.client = client
        self.max_json = 100
        self.fd = open(outfile, 'w')
        self.done_msgs = 0
        self.output_raw = output_raw

        # write csv header row
        if output_raw:
            self.fd.write("StartDateTime,OpenPrice,HighPrice,LowPrice,LastPrice,Volume,NumTrades,BidVolume,AskVolume\n")
        else:
            self.fd.write("DateTime,Price,Volume,AtBidOrAsk\n")

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

        if self.output_raw:
            csv_format = "{StartDateTime},{OpenPrice},{HighPrice},{LowPrice},{LastPrice},{Volume},{NumTrades},{BidVolume},{AskVolume}\n"
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
        else:
            csv_format = "{DateTime},{Price},{Volume},{AtBidOrAsk}\n"
            #print(msg["BidVolume"], msg["AskVolume"])
            if not (int(msg["BidVolume"]) == 0 or int(msg["AskVolume"]) == 0):
                print(msg)
                assert(False)
            self.fd.write(csv_format.format(
                DateTime = msg["StartDateTime"],
                Price = msg["LastPrice"],
                Volume = msg["Volume"],
                AtBidOrAsk = '1' if int(msg["BidVolume"]) != 0 else '2'
            ))

        self.done_msgs += 1

        if self.done_msgs % 1000000 == 0:
            print("Has processed %d messages to up %s" % (
                self.done_msgs, str(datetime.fromtimestamp(msg['StartDateTime'])) if 'StartDateTime' in msg.keys() else "unknown-datetime"))

def Download(symbol, exchange='CME', userpass='userpass', address='192.168.122.107', port=11198, sDateTime=0, eDateTime=0, output=None, raw=False):

    # initialize color output
    colorama.init()

    OUTPUT = "%s.csv" % symbol if output == None else output

    with open(userpass) as f:
        username = f.readline().strip('\n')
        password = f.readline().strip('\n')
        f.close()

    client = DTCClient()
    client.connect(address, port)
    client.logon(username, password)

    # historical data request for symbol
    client.send_json_request({
        'Type': DTC.HISTORICAL_PRICE_DATA_REQUEST,
        'RequestID': 10,
        'Symbol': symbol,
        'Exchange': exchange,
        'RecordInterval': DTC.INTERVAL_TICK,
        'StartDateTime': sDateTime,
        'EndDateTime': eDateTime,
        'MaxDaysToReturn': 0,
        'UseZLibCompression': 0
    })

    d = Downloader(client, OUTPUT, raw)
    try:
        client.run(d.json_handler)
    except Exception as err:
        if err.args[0] == "Done":
            return
        else:
            raise err

async def DownloadAsync(symbol, exchange='CME', userpass='userpass', address='192.168.122.142', port=11198, sDateTime=0, eDateTime=0):

    async with async_open(userpass, 'r') as f:
        username = (await f.readline()).strip('\n')
        password = (await f.readline()).strip('\n')

    client = DTCClientAsync()
    await client.connect(address, port)
    await client.logon(username, password)

    # historical data request for symbol
    await client.send_json_request({
        'Type': DTC.HISTORICAL_PRICE_DATA_REQUEST,
        'RequestID': 10,
        'Symbol': symbol,
        'Exchange': exchange,
        'RecordInterval': DTC.INTERVAL_TICK,
        'StartDateTime': sDateTime,
        'EndDateTime': eDateTime,
        'MaxDaysToReturn': 0,
        'UseZLibCompression': 0
    })

    data = []
    done_msgs = 1
    async for message in client.messages():

        if message['Type'] != 803:
            print(colored("Unprocess MSG: " + json.dumps(message), 'red'))
            continue

        if (message['IsFinalRecord'] == 1):
            await client.close()
            break

        data.append(np.array(list(message.values())))
        if done_msgs % 1000000 == 0:
            print("Has processed %d messages to up %s" % (
                done_msgs,
                str(datetime.fromtimestamp(message['StartDateTime'])) if 'StartDateTime' in message.keys() else "unknown-datetime"))
        done_msgs += 1

    columns = [
        "Type",
        "RequestID",
        "StartDateTime",
        "OpenPrice",
        "HighPrice",
        "LowPrice",
        "LastPrice",
        "Volume",
        "NumTrades",
        "BidVolume",
        "AskVolume",
        "IsFinalRecord"
    ]

    return pd.DataFrame(np.array(data), columns=columns) if len(data) > 0 else pd.DataFrame()

async def Main():

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--userpass', "-i", default="userpass", help="Username and Password file.")
    parser.add_argument('--address', "-a", default="192.168.122.107", help="IP Address of Sierra Chart instance")
    parser.add_argument('--port', "-p", type=int, default=11198, help="Port number of Sierra Chart instance")
    parser.add_argument('--symbol', "-s", required=True, help="Symbol Name")
    parser.add_argument('--exchange', "-e", default="CME", help="Exchange Name")
    parser.add_argument('--sDateTime', default="0", help="Start DateTime")
    parser.add_argument('--eDateTime', default="0", help="End DateTime")
    parser.add_argument('--output', "-o", default=None, help="Output file name")

    args = parser.parse_args()

    ADDR = args.address
    PORT = args.port
    SYMBOL = args.symbol
    EXCHANGE = args.exchange
    sDateTime = int(args.sDateTime)
    eDateTime = int(args.eDateTime)
    OUTPUT = "%s.csv" % symbol if args.output == None else args.output

    data = await DownloadAsync(SYMBOL, EXCHANGE, args.userpass, ADDR, PORT, sDateTime, eDateTime)

    print("Download Finished. Saving to %s" % OUTPUT)

    data.drop(['Type', 'RequestID', 'IsFinalRecord'], axis=1).to_csv(OUTPUT, index=False)



if __name__ == "__main__":
    loop = aio.get_event_loop()
    loop.run_until_complete(Main())


