#!python3

import argparse
from select import select
import re, json
from functools import reduce
from time import sleep
from utilities import follow
import math

trade_header = "DateTime,Price,Volume,AtBidOrAsk\n"
trade_format = '%d,%.2f,%d,%d\n'

def RemoveLastSecondTrades(data):
    if len(data) == 0:
        return data

    # remove the last second
    last_second = data[-1][0]
    while True:
        if data[-1][0] == last_second:
            del data[-1]
        else:
            break
    return data

def ReadFile(f):
    header = f.readline()
    assert(header == trade_header)

    data = []
    for line in f:
        values = line.rstrip().split(',')
        dt = int(values[0])
        price = float(values[1])
        volume = int(values[2])
        at_bid_or_ask = int(values[3])
        data.append((dt, price, volume, at_bid_or_ask))

    return data

def AppendTrades(data, new_trades):

    if len(data) == 0:
        return new_trades

    last_second = data[-1][0]
    index = 0
    for index in range(0, len(new_trades)):
        if new_trades[index][0] > last_second:
            break

    data.append(new_trades[index:])
    return data

def ProcessFiles(infiles, outfile, follow_mode):

    trades = []
    for f in infiles[:-1]:

        new_trades = ReadFile(f)
        new_trades = RemoveLastSecondTrades(new_trades)

        trades = AppendTrades(trades, new_trades)

    # output all data up to now
    outfile.write(trade_header)
    for trade in trades:
        outfile.write(trade_format % trade)

    last_second = trades[-1][0]
    del trades

    header = infiles[-1].readline()
    assert(header == trade_header)

    read_from = infiles[-1]
    if follow_mode:
        read_from = follow(infiles[-1], 10)

    for line in read_from:
        values = line.rstrip().split(',')
        if int(values[0]) > last_second:
            outfile.write(line)
            break

    for line in read_from:
        outfile.write(line)
        outfile.flush()


def Main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--inputs', '-i', required=True, help="input csv files")
    parser.add_argument('--output', '-o', required=True, help="output csv file")
    parser.add_argument('--follow', '-f', default=False, action='store_true', help="Do we follow the input file?")

    args = parser.parse_args()

    filenames = args.inputs.split(',')
    files = []
    for fn in filenames:
        files.append(open(fn, 'r'))
        assert(files[-1])

    outfile = open(args.output, 'w')
    if not outfile:
        print('Unable to open output file: ', outfile)
        exit(-1)

    ProcessFiles(files, outfile, args.follow)


if __name__ == '__main__':
    Main()
