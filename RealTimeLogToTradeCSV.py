import argparse
import json
from utilities import follow

def process(input_file, output_file, follow_mode):

    read_from = input_file

    if follow_mode:
        read_from = follow(input_file, 10)

    read_cache = ''

    output_file.write('DateTime,Price,Volume,AtBidOrAsk\n')

    for line in read_from:

        read_cache += line

        if read_cache[-1] != '\n':
            continue

        obj = json.loads(read_cache.rstrip())
        read_cache = ''

        if 'Type' not in obj:
            continue
        if obj['Type'] != 112:
            continue

        datetime = obj['DateTime']
        price = obj['Price']
        volume = obj['Volume']
        at_bid_or_ask = obj['AtBidOrAsk']

        csv_format = "{DateTime},{Price},{Volume},{AtBidOrAsk}\n"
        output_file.write(csv_format.format(
            DateTime = datetime,
            Price = price,
            Volume = volume,
            AtBidOrAsk = at_bid_or_ask
        ))
        output_file.flush()

def Main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', required=True, help="input file")
    parser.add_argument('--output', '-o', default=None, help="output file")
    parser.add_argument('--follow', '-f', default=False, action='store_true', help="Do we follow the input file?")

    args = parser.parse_args()

    infile = open(args.input, 'r')
    outfile_name = args.output if args.output else args.input + '.trades'
    outfile = open(outfile_name, 'w')

    if not infile:
        print('Unable to open input file: ', args.input)
        exit(-1)

    if not outfile:
        print('Unable to open output file: ', args.output)
        exit(-1)

    process(infile, outfile, args.follow)


if __name__ == '__main__':
    Main()
