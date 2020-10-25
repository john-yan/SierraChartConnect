#!python3

import argparse
from select import select
import re, json
from functools import reduce

"""
Computes OHLC from realtime market data
"""

def ComputeOHLC(data, datetime, price, volume):

    if datetime not in data:
        data[datetime] = (price, price, price, price, 0)

    o, h, l, c, v = data[datetime]

    new_c = price
    new_h = max(h, price)
    new_l = min(l, price)
    new_v = v + volume

    data[datetime] = (o, new_h, new_l, new_c, new_v)

def ComputeImbalanceFactorForEntry(table, time, price, computeAbove=True, computeBelow=True):

    if time not in table or price not in table[time]:
        return

    bid, ask, _, imb, ima, _ = table[time][price]

    # imbalance bid
    if price + 0.25 in table[time]:
        ask_above = max(1, table[time][price + 0.25][1])
        imb = bid / ask_above

    # imbalance ask
    if price - 0.25 in table[time]:
        bid_below = max(1, table[time][price - 0.25][0])
        ima = ask / bid_below

    # update bid imbalance
    table[time][price][3] = imb
    # update ask imbalance
    table[time][price][4] = ima

    if computeAbove:
        ComputeImbalanceFactorForEntry(table, time, price + 0.25, False, False)
    if computeBelow:
        ComputeImbalanceFactorForEntry(table, time, price - 0.25, False, False)

def ComputeVolumeDistribution(table, time):

    if time not in table:
        return

    totalVol = reduce(lambda total, price: total + table[time][price][2], table[time].keys(), 0)

    for price in table[time].keys():
        bid, ask, tot, imb, ima, dist = table[time][price]
        dist = tot / totalVol
        table[time][price][5] = dist

def ComputeImbalanceTable(table, time, price, volume, isBid):

    # Create or update table entries
    if time not in table:
        table[time] = {
            price: [0, 0, 0, 0.0, 0.0, 0.0]
        }

    if price not in table[time]:
        table[time][price] = [0, 0, 0, 0.0, 0.0, 0.0]

    bid, ask, tot, imb, ima, dist = table[time][price]

    if isBid == 0:
        table[time][price][0] += volume
    else:
        table[time][price][1] += volume

    table[time][price][2] = bid + ask + volume

    ComputeImbalanceFactorForEntry(table, time, price, True, True)
    ComputeVolumeDistribution(table, time)

def follow(thefile):
    while True:
        line = thefile.readline()
        if not line:
            rlist, _, _ = select([thefile], [], [])
            if len(rlist) != 1 or thefile.closed:
                return None
            continue
        yield line

def MatchPeriod(Type):

    match = re.match(r'(\d+)s', Type)
    if match:
        return int(match.group(1))

    match = re.match(r'(\d+)min', Type)
    if match:
        return int(match.group(1)) * 60

    match = re.match(r'(\d+)hr', Type)
    if match:
        return int(match.group(1)) * 60 * 60

    return None

# ohlc_output_format: datetime, open, high, low, close, volume
ohlc_output_heads = 'DateTime, Open, High, Low, Close, volume\n'
ohlc_output_format = '%d,%.2f,%.2f,%.2f,%.2f,%d\n'

# imbalance_output_format: datetime, price, volatbid, volatask, imb, ima
imbalance_output_heads = 'DateTime, Price, VolumeAtBid, VolumeAtAsk, TotalVolume, BidImbalance, AskImbalance, VolumeDistribution\n'
imbalance_output_format = '%d,%.2f,%d,%d,%d,%.2f,%.2f,%.2f\n'

def WriteData(compute_type, datetime, data, thefile):

    assert(compute_type == 'ohlc' or compute_type == 'imbalance')

    if compute_type == 'ohlc':
        thefile.write(ohlc_output_format % (datetime, *data[datetime]))

    elif compute_type == 'imbalance':
        for p in data[datetime].keys():
            thefile.write(imbalance_output_format % (datetime, p, *data[datetime][p]))

    thefile.flush()

def process(compute_type, period_in_seconds, infile, hfile, rfile, follow_mode):

    assert(compute_type == 'ohlc' or compute_type == 'imbalance')

    data = {}
    last = 0

    read_from = infile
    if follow_mode:
        print("use follow")
        print(follow_mode)
        read_from = follow(infile)

    for line in read_from:

        obj = json.loads(line.rstrip())

        if 'Type' not in obj:
            continue
        if obj['Type'] != 112:
            continue

        datetime = obj['DateTime']
        price = obj['Price']
        volume = obj['Volume']
        isBid = obj['AtBidOrAsk'] == 1

        datetime -= datetime % period_in_seconds

        if compute_type == 'ohlc':
            ComputeOHLC(data, datetime, price, volume)
        elif compute_type == 'imbalance':
            ComputeImbalanceTable(data, datetime, price, volume, isBid)

        if last != datetime:
            # output to historical file if time has pass to new candle
            if last in data:
                WriteData(compute_type, last, data, hfile)
            last = datetime

        WriteData(compute_type, datetime, data, rfile)

def Main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', required=True, help="input file")
    parser.add_argument('--historicalFile', '-H', required=True, help="output historical data file")
    parser.add_argument('--realtimeFile', '-R', required=True, help="output realtime data file")
    parser.add_argument('--period', '-p', default='1min',  help="""Period could be 10s, 20, 30s,
                                                                                 1min, 5min, 10min,
                                                                                 1hr, 2hr, etc""")
    parser.add_argument('--type', '-t', default='ohlc', help="output type: ohlc or imbalance")
    parser.add_argument('--follow', '-f', default=False, action='store_true', help="Do we follow the input file?")

    args = parser.parse_args()

    period_in_seconds = MatchPeriod(args.period)

    if period_in_seconds == None:
        print('Unknown period')
        exit(0)

    infile = open(args.input, 'r')
    hfile = open(args.historicalFile, 'a+')
    rfile = open(args.realtimeFile, 'w')

    if not infile:
        print('Unable to open input file: ', args.input)
        exit(-1)

    if not hfile:
        print('Unable to open historical data file: ', args.historicalFile)
        exit(-1)

    if not rfile:
        print('Unable to open realtime data file: ', args.realtimeFile)
        exit(-1)

    process(args.type, period_in_seconds, infile, hfile, rfile, args.follow)


if __name__ == '__main__':
    Main()
