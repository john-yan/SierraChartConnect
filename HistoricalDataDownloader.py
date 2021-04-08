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
import numpy as np
import asyncio as aio
import pandas as pd
from Raw2TickData import ConvertRaw2Tick
import aiofiles

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

async def DownloadAsync(symbol,
                        exchange='CME',
                        userpass='userpass',
                        address='192.168.122.142',
                        port=11198,
                        sDateTime=0,
                        eDateTime=0,
                        recordInterval=DTC.INTERVAL_TICK):

    async with aiofiles.open(userpass, 'r') as f:
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
        'RecordInterval': recordInterval,
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
    parser.add_argument('--raw', default=False, action='store_true', help="output raw file")
    parser.add_argument('--record_interval', default='INTERVAL_TICK', help="Record interval")

    args = parser.parse_args()

    ADDR = args.address
    PORT = args.port
    SYMBOL = args.symbol
    EXCHANGE = args.exchange
    sDateTime = int(args.sDateTime)
    eDateTime = int(args.eDateTime)
    OUTPUT = "%s.csv" % symbol if args.output == None else args.output

    try:
        interval = eval('DTC.%s' % args.record_interval)
    except:
        print("Unknown interval: %s" % args.record_interval)
        return

    data = await DownloadAsync(SYMBOL, EXCHANGE, args.userpass, ADDR, PORT, sDateTime, eDateTime, interval)

    if args.raw:
        print("Download Finished. Saving to %s" % OUTPUT)
        data.drop(['Type', 'RequestID', 'IsFinalRecord'], axis=1).to_csv(OUTPUT, index=False)
    else:
        print("Download Finished. Running convertion...")
        data = ConvertRaw2Tick(data)
        print("Convertion Finished. Saving to %s ..." % OUTPUT)
        data.to_csv(OUTPUT, index=False)

if __name__ == "__main__":
    loop = aio.get_event_loop()
    loop.run_until_complete(Main())


