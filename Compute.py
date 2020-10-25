#!python3

import argparse
from select import select
import re, json

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

def ComputeImbalance(dist, time, price, computeAbove=True, computeBelow=True):

    key = (time, price)
    if time not in dist or price not in dist[time]:
        return

    bid, ask, imb, ima = dist[time][price]

    # imbalance bid
    if price + 0.25 in dist[time]:
        ask_above = max(1, dist[time][price + 0.25][1])
        imb = bid / ask_above

    # imbalance ask
    if price - 0.25 in dist[time]:
        bid_below = max(1, dist[time][price - 0.25][0])
        ima = ask / bid_below

    dist[time][price] = (bid, ask, imb, ima)

    if computeAbove:
        ComputeImbalance(dist, time, price + 0.25, False, False)
    if computeBelow:
        ComputeImbalance(dist, time, price - 0.25, False, False)

def ComputeVolumeDistribution(dist, time, price, volume, bidorask, compute_imbalance=False):

    # Create a new entry if not exist
    if time not in dist:
        dist[time] = {
            price: (0, 0, 0.0, 0.0)
        }

    if price not in dist[time]:
        dist[time][price] = (0, 0, 0.0, 0.0)

    bid, ask, imb, ima = dist[time][price]

    if bidorask == 0:
        bid += volume
    else:
        ask += volume

    # Update table entry
    dist[time][price] = (bid, ask, imb, ima)

    # compute imbalance if required
    if compute_imbalance:
        ComputeImbalance(dist, time, price, True, True)

def follow(thefile):
    while True:
        line = thefile.readline()
        if not line:
            return
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
ohlc_output_format = '%d,%.2f,%.2f,%.2f,%.2f,%d\n'

# imbalance_output_format: datetime, price, volatbid, volatask, imb, ima
imbalance_output_format = '%d,%.2f,%d,%d,%.2f,%.2f\n'

def WriteRealtimeData(compute_type, datetime, price, data, rfile):

    assert(compute_type == 'ohlc' or compute_type == 'imbalance')

    if compute_type == 'ohlc':
        rfile.write(ohlc_output_format % (datetime, *data[datetime]))

    elif compute_type == 'imbalance':
        rfile.write(imbalance_output_format % (datetime, price, *data[datetime][price]))

        if price + 0.25 in data[datetime]:
            rfile.write(imbalance_output_format % (datetime, price + 0.25, *data[datetime][price + 0.25]))
        if price - 0.25 in data[datetime]:
            rfile.write(imbalance_output_format % (datetime, price - 0.25, *data[datetime][price - 0.25]))


    rfile.flush()

def WriteHistoricalData(compute_type, last, data, hfile):

    assert(compute_type == 'ohlc' or compute_type == 'imbalance')

    if compute_type == 'ohlc':
        hfile.write(ohlc_output_format % (last, *data[last]))

    elif compute_type == 'imbalance':
        for p in data[last].keys():
            hfile.write(imbalance_output_format % (last, p, *data[last][p]))

    hfile.flush()

def process(compute_type, period_in_seconds, infile, hfile, rfile):

    assert(compute_type == 'ohlc' or compute_type == 'imbalance')

    data = {}
    last = 0

    for line in follow(infile):

        obj = json.loads(line.rstrip())

        if 'Type' not in obj:
            continue
        if obj['Type'] != 112:
            continue

        datetime = obj['DateTime']
        price = obj['Price']
        volume = obj['Volume']
        boa = obj['AtBidOrAsk']

        datetime -= datetime % period_in_seconds

        if compute_type == 'ohlc':
            ComputeOHLC(data, datetime, price, volume)
        elif compute_type == 'imbalance':
            ComputeVolumeDistribution(data, datetime, price, volume, boa, True)

        if last != datetime:
            # output to historical file if time has pass to new candle
            if last in data:
                WriteHistoricalData(compute_type, last, data, hfile)
            last = datetime

        WriteRealtimeData(compute_type, datetime, price, data, rfile)

def Main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', required=True, help="input file")
    parser.add_argument('--historicalFile', '-H', required=True, help="output historical data file")
    parser.add_argument('--realtimeFile', '-R', required=True, help="output realtime data file")
    parser.add_argument('--period', '-p', default='1min',  help="""Period could be 10s, 20, 30s,
                                                                                 1min, 5min, 10min,
                                                                                 1hr, 2hr, etc""")
    parser.add_argument('--type', '-t', default='ohlc', help="output type: ohlc or imbalance")

    args = parser.parse_args()

    period_in_seconds = MatchPeriod(args.period)

    if period_in_seconds == None:
        print('Unknown period')
        exit(0)

    infile = open(args.input, 'r')
    hfile = open(args.historicalFile, 'w')
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

    process(args.type, period_in_seconds, infile, hfile, rfile)


if __name__ == '__main__':
    Main()
