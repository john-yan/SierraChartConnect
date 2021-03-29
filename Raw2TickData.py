
import pandas as pd
import argparse
import numpy as np
import random

def ConvertRaw2Tick(input_file, output_file):
    raw = pd.read_csv(input_file)
    raw['AtBidOrAsk'] = (raw.Volume == raw.AskVolume).astype(np.int32) + 1
    check = (raw.Volume != raw.AskVolume) & (raw.Volume != raw.BidVolume)
    if check.any() == True:
        print('Correcting these errors')
        print(raw[check])
        for i in raw[check].index:
            if raw.iloc[i].HighPrice != raw.iloc[i].LowPrice:
                at_ask = abs(raw.iloc[i].LastPrice - raw.iloc[i].HighPrice) > abs(raw.iloc[i].LastPrice - raw.iloc[i].LowPrice)
                raw.at[i, 'AtBidOrAsk'] = int(at_ask) + 1
            else:
                raw.at[i, 'AtBidOrAsk'] = random.choice([1, 2])

        print('After corrections')
        print(raw[check])

    output_columns = ['StartDateTime', 'LastPrice', 'Volume', 'AtBidOrAsk']
    output_headers = ['DateTime', 'Price', 'Volume', 'AtBidOrAsk']
    raw.to_csv(output_file, columns=output_columns, header=output_headers, index=False)

    #assert((raw.OpenPrice == 0).all() == True)

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', "-i", default=None, help="Input file name")
    parser.add_argument('--output', "-o", default=None, help="Output file name")

    args = parser.parse_args()

    ConvertRaw2Tick(args.input, args.output)
