
import pandas as pd
import argparse
import numpy as np
import random
import sys

def ConvertRaw2Tick(raw_df):
    raw_df['AtBidOrAsk'] = (raw_df.Volume == raw_df.AskVolume).astype(np.int32) + 1
    check = (raw_df.Volume != raw_df.AskVolume) & (raw_df.Volume != raw_df.BidVolume)
    if check.any() == True:
        print('Correcting these errors')
        print(raw_df[check])
        for i in raw_df[check].index:
            if raw_df.iloc[i].HighPrice != raw_df.iloc[i].LowPrice:
                at_ask = abs(raw_df.iloc[i].LastPrice - raw_df.iloc[i].HighPrice) > abs(raw_df.iloc[i].LastPrice - raw_df.iloc[i].LowPrice)
                raw_df.at[i, 'AtBidOrAsk'] = int(at_ask) + 1
            else:
                raw_df.at[i, 'AtBidOrAsk'] = random.choice([1, 2])

        print('After corrections')
        print(raw_df[check])

    return pd.DataFrame({
        'DateTime': raw_df.StartDateTime,
        'Price': raw_df.LastPrice,
        'Volume': raw_df.Volume,
        'AtBidOrAsk': raw_df.AtBidOrAsk
    })

    #assert((raw_df.OpenPrice == 0).all() == True)

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', "-i", default='-', help="Input file name")
    parser.add_argument('--output', "-o", default='-', help="Output file name")

    args = parser.parse_args()

    INPUT = args.input if args.input != '-' else sys.stdin
    OUTPUT = args.output if args.output != '-' else sys.stdout

    raw_df = pd.read_csv(INPUT)
    df = ConvertRaw2Tick(raw_df)

    df.to_csv(OUTPUT, index=False)

