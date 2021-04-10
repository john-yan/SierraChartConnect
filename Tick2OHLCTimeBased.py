import pandas as pd
import argparse
import numpy as np
import random

def ConvertTick2OHLCTimeBased(ticks, period):
    ticks = pd.DataFrame(ticks.reset_index(drop=True))
    ticks['period'] = (ticks.DateTime / period).astype(np.int32)
    ticks['last_period'] = np.roll(ticks.period, 1)
    ticks.at[0, 'last_period'] = 0
    ticks['last_period'] = ticks.last_period.astype(np.int32)
    start_index = np.array(ticks[ticks.period != ticks.last_period].index)
    end_index = np.roll(start_index, -1)
    end_index[-1] = len(ticks)
    new_index = np.arange(0, len(start_index), 1)
    new_index = np.repeat(new_index, end_index - start_index)
    ticks['new_index'] = new_index
    group = ticks.groupby(['new_index'])

    result = pd.DataFrame({
        'StartDateTime': group.DateTime.first(),
        'EndDateTime': group.DateTime.last(),
        'Open': group.Price.first(),
        'High': group.Price.max(),
        'Low': group.Price.min(),
        'Close': group.Price.last(),
        'Volume': group.Volume.sum()
    }).reset_index(drop=True)

    return result

if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', "-i", default=None, help="Input file name")
    parser.add_argument('--output', "-o", default=None, help="Output file name")
    parser.add_argument('--period', "-p", default=None, help="OHLC time period")

    args = parser.parse_args()

    ticks = pd.read_csv(args.input)
    ohlc = ConvertTick2OHLCTimeBased(ticks, int(args.period))
    ohlc.to_csv(args.output, index=False)

