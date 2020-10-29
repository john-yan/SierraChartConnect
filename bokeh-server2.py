
import json
from sys import stdin
import colorama
from colored import fg, bg, attr
from datetime import datetime
from functools import reduce
import socket
from more_itertools import partition
from itertools import tee
import argparse
import threading
from queue import Queue, Empty
from select import select
import time
import pandas as pd
import numpy as np
from bokeh.plotting import figure, curdoc
from tornado import gen
from functools import partial
from bokeh.models import ColumnDataSource, HoverTool, FixedTicker
from time import sleep
import os
import pytz

TimeShift = pytz.timezone('US/Eastern').utcoffset(datetime.now()).total_seconds()

doc = curdoc()
time_factor = 1000
tick = 0.25

imba_columns = "DateTime,Price,VolumeAtBid,VolumeAtAsk,TotalVolume,BidImbalance,AskImbalance,VolumeDistribution".split(',')
ohlc_columns = 'DateTime,Open,High,Low,Close,Volume'.split(',')

# chart_columns = [
#     'CellTop',
#     'CellBottom',
#     'CellLeft',
#     'CellRight',
#     'CellMiddle',
#     'VolAtBidText',
#     'Separator',
#     'VolAtAskText',
#     'VolAtBidColor',
#     'VolAtAskColor',
#     'TotalVolume',
#     'VolumeEnd'
# ]

def ReadOneLine(thefile):

    line = thefile.readline()

    if not line:
        return line

    while line[-1] != '\n':
        sleep(0.01)
        line += thefile.readline()

    return line

def LineReader(thefile):
    while True:
        line = ReadOneLine(thefile)
        if not line:
            return None
        yield line

def SessionReader(thefile):

    reader = LineReader(thefile)
    isFirstLine = True

    for line in reader:
        if not line:
            if isFirstLine:
                return None
            sleep(0.5)
            continue

        if isFirstLine:
            if line == 'SESSION START\n':
                isFirstLine = False
            continue

        if line == 'SESSION END\n':
            return None

        yield line


def ComputeOHLCChartParameter(table, width):

    if table == []:
        return pd.DataFrame({
            "CellTop": [],
            "CellBottom": [],
            "CellLeft": [],
            "CellRight": [],
            "Color": []
        })

    raw_data = pd.DataFrame(table, columns=ohlc_columns)

    DateTime = (raw_data.DateTime.astype(np.int64) + TimeShift) * time_factor

    CellTop = raw_data[['Open', 'Close']].astype(np.float32).max(axis=1)
    CellBottom = raw_data[['Open', 'Close']].astype(np.float32).min(axis=1)
    CellLeft = DateTime  - width / 2
    CellRight = DateTime + width / 2
    Color = raw_data[['Open', 'Close']].astype(np.float32).apply(
        lambda x: '#FF0000' if x.Open > x.Close else '#00FF00', axis=1).astype('string')

    return pd.DataFrame({
        "CellTop": CellTop,
        "CellBottom": CellBottom,
        "CellLeft": CellLeft,
        "CellRight": CellRight,
        "Color": Color
    })

def ComputeImbalanceChartParameter(table, width, imbalance_highlight_factor):
    if table == []:
        return pd.DataFrame({
                'CellTop': [],
                'CellBottom': [],
                'CellLeft': [],
                'CellRight': [],
                'CellMiddle': [],
                'VolAtBidText': [],
                'Separator': [],
                'VolAtAskText': [],
                'VolAtBidColor': [],
                'VolAtAskColor': [],
                'TotalVolume': [],
                'VolumeEnd': [],
                'VolumeColor': []
        })

    raw_data = pd.DataFrame(table, columns=imba_columns)

    DateTime = (raw_data.DateTime.astype(np.int64) + TimeShift) * time_factor
    CellTop = raw_data.Price.astype(np.float32) + tick
    CellBottom = raw_data.Price.astype(np.float32)
    CellLeft = DateTime  - width / 2
    CellRight = DateTime + width / 2
    CellMiddle = DateTime
    VolAtBidText = raw_data.VolumeAtBid.astype('string')
    VolAtAskText = raw_data.VolumeAtAsk.astype('string')
    VolAtBidColor = raw_data.BidImbalance.astype(np.float32).apply(
            lambda x: '#000000' if x < imbalance_highlight_factor else '#8F0000')
    VolAtAskColor = raw_data.AskImbalance.astype(np.float32).apply(
            lambda x: '#000000' if x < imbalance_highlight_factor else '#008F00')

    TotalVolume = raw_data.TotalVolume.astype(np.int32)
    VolumeDist = raw_data.VolumeDistribution.astype(np.float32)
    VolumeEnd = CellLeft + VolumeDist * width
    VolumeColor = raw_data[['VolumeAtBid', 'VolumeAtAsk']].astype(np.float32).apply(
        lambda x: '#FFC0C0' if x.VolumeAtBid > x.VolumeAtAsk else '#C0FFC0', axis=1).astype('string')


    # TODO: make volume bar colorful
    chart_parameter = pd.DataFrame({
            'CellTop': CellTop,
            'CellBottom': CellBottom,
            'CellLeft': CellLeft.astype(np.int64),
            'CellRight': CellRight.astype(np.int64),
            'CellMiddle': CellMiddle.astype(np.int64),
            'VolAtBidText': VolAtBidText,
            'Separator': str('x'),
            'VolAtAskText': VolAtAskText,
            'VolAtBidColor': VolAtBidColor.astype('string'),
            'VolAtAskColor': VolAtAskColor.astype('string'),
            'TotalVolume': TotalVolume,
            'VolumeEnd': VolumeEnd.astype(np.int64),
            'VolumeColor': VolumeColor
    })

    return chart_parameter

def PlotOHLCChart(fig, source):
    fig.quad(top='CellTop', bottom='CellBottom', left='CellLeft',
                        right='CellRight', source=source, fill_alpha=0.0, line_color='Color', line_width=2)

def PlotImbalanceChart(fig, source, colorProfile):
    # plot base
    fig.quad(top='CellTop', bottom='CellBottom', left='CellLeft',
                        right='CellRight', color='#F0F0F0', source=source, name='hoverable')

    # plot volume profile
    fig.quad(top='CellTop', bottom='CellBottom', left='CellLeft',
                        right='VolumeEnd', color='VolumeColor' if colorProfile else '#C0C0C0', source=source)

    # plot bid
    fig.text(x='CellMiddle', y='CellBottom', text='VolAtBidText',
            text_color='VolAtBidColor', text_align='right', text_font_size='12px',
            source=source, x_offset=-5)

    # plot x
    fig.text(x='CellMiddle', y='CellBottom', text='Separator',
            text_color='#000000', text_align='center', text_font_size='12px',
            source=source)

    # plot ask
    fig.text(x='CellMiddle', y='CellBottom', text='VolAtAskText',
            text_color='VolAtAskColor', text_align='left', text_font_size='12px',
            source=source, x_offset=5)

def UpdateHoverTool(fig):

    # remove the old one
    for i in range(len(fig.tools)):
        if isinstance(fig.tools[i], HoverTool):
            del fig.tools[i]

    TOOLTIPS = [ ("Price", "@CellBottom{0.2f}") ]
    hoverTool = HoverTool(tooltips=TOOLTIPS, names = ['hoverable'])

    # Add the new one
    fig.add_tools(hoverTool)

class Server:

    def __init__(self, imba_rfile, imba_hfile, ohlc_rfile, ohlc_hfile, colorProfile):

        self.colorProfile = colorProfile
        self.imba_hfile = open(imba_hfile)
        self.imba_rfile = open(imba_rfile)
        self.ohlc_hfile = open(ohlc_hfile)
        self.ohlc_rfile = open(ohlc_rfile)

        assert(self.imba_hfile)
        assert(self.imba_rfile)
        assert(self.ohlc_hfile)
        assert(self.ohlc_rfile)

        self.imba_rfile.seek(0, 2)
        self.ohlc_rfile.seek(0, 2)

        self.period_in_seconds = int(self.imba_hfile.readline().rstrip())
        assert(self.period_in_seconds == int(self.ohlc_hfile.readline().rstrip()))

        self.width = int(self.period_in_seconds * time_factor * 0.85)
        self.highlight_factor = 3

        TOOLS = "pan,xwheel_zoom,ywheel_zoom,wheel_zoom,box_zoom,reset,save,crosshair"

        self.plot = figure(tools=TOOLS, x_axis_type = 'datetime')
        self.plot.sizing_mode = 'stretch_both'
        self.plot.yaxis.formatter.use_scientific = False
        self.plot.yaxis.ticker = FixedTicker(ticks=np.arange(start=2000, stop=4000, step=0.25))

        imba_table = [line.rstrip().split(',') for line in LineReader(self.imba_hfile)]
        ohlc_table = [line.rstrip().split(',') for line in LineReader(self.ohlc_hfile)]

        imba_source = ColumnDataSource(ComputeImbalanceChartParameter(imba_table, self.width, self.highlight_factor))
        ohlc_source = ColumnDataSource(ComputeOHLCChartParameter(ohlc_table, self.width))

        self.imba_rsource = ColumnDataSource(ComputeImbalanceChartParameter([], self.width, self.highlight_factor))
        self.ohlc_rsource = ColumnDataSource(ComputeOHLCChartParameter([], self.width))

        PlotImbalanceChart(self.plot, imba_source, self.colorProfile)
        PlotImbalanceChart(self.plot, self.imba_rsource, self.colorProfile)

        PlotOHLCChart(self.plot, ohlc_source)
        PlotOHLCChart(self.plot, self.ohlc_rsource)

        UpdateHoverTool(self.plot)

        doc.add_root(self.plot)
        doc.on_session_destroyed(self.close)

        self.queue = Queue(maxsize=1)
        self.thread = threading.Thread(target=self.update, daemon=True)
        self.thread.start()

    def close(self, session_context):
        self.imba_rfile.close()
        self.imba_hfile.close()
        self.ohlc_rfile.close()
        self.ohlc_hfile.close()

    @gen.coroutine
    def update_doc(self):

        hData, rData = self.queue.get()

        if not hData['imba'].empty:
            PlotImbalanceChart(self.plot, hData['imba'], self.colorProfile)

        if not hData['ohlc'].empty:
            PlotOHLCChart(self.plot, hData['ohlc'])

        if not rData['imba'].empty:
            self.imba_rsource.data = rData['imba']

        if not rData['ohlc'].empty:
            self.ohlc_rsource.data = rData['ohlc']

        UpdateHoverTool(self.plot)

    def update(self):

        try:
            while True:
                if self.imba_rfile.closed or self.imba_hfile.closed or self.ohlc_rfile.closed or self.ohlc_hfile.closed:
                    print('imba_rfile or imba_hfile has been closed.')
                    return

                hData = { 'imba': pd.DataFrame(), 'ohlc': pd.DataFrame() }
                rData = { 'imba': pd.DataFrame(), 'ohlc': pd.DataFrame() }
                update_ready = False

                imba_table = [ line.rstrip().split(',') for line in LineReader(self.imba_hfile) ]

                if len(imba_table) > 0:
                    hData['imba'] = ComputeImbalanceChartParameter(imba_table, self.width, self.highlight_factor)
                    update_ready = True

                ohlc_table = [ line.rstrip().split(',') for line in LineReader(self.ohlc_hfile) ]

                if len(ohlc_table) > 0:
                    hData['ohlc'] = ComputeOHLCChartParameter(ohlc_table, self.width)
                    update_ready = True

                imba_rtable = []
                while True:
                    table = [line.rstrip().split(',') for line in SessionReader(self.imba_rfile)]
                    if len(table) > 0:
                        imba_rtable = table
                    else:
                        break

                if len(imba_rtable) > 0:
                    imba = ComputeImbalanceChartParameter(imba_rtable, self.width, self.highlight_factor)
                    rData['imba'] = imba
                    update_ready = True

                ohlc_rtable = []
                while True:
                    table = [ line.rstrip().split(',') for line in SessionReader(self.ohlc_rfile) ]
                    if len(table) > 0:
                        ohlc_rtable = table
                    else:
                        break

                if len(ohlc_rtable) > 0:
                    ohlc = ComputeOHLCChartParameter(ohlc_rtable, self.width)
                    rData['ohlc'] = ohlc
                    update_ready = True


                if update_ready :
                    self.queue.put((hData, rData))

                    # update the document from callback
                    doc.add_next_tick_callback(self.update_doc)

                sleep(0.5)

        except Exception as err:
            print('updater exits due to error ', err)

def Main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--imbaRfile', default='ESZ0-CME-imbalance-5min.rfile', help="Realtime file")
    parser.add_argument('--imbaHfile', default='ESZ0-CME-imbalance-5min.hfile', help="Historical file")
    parser.add_argument('--ohlcRfile', default='ESZ0-CME-ohlc-5min.rfile', help="Realtime file")
    parser.add_argument('--ohlcHfile', default='ESZ0-CME-ohlc-5min.hfile', help="Historical file")
    parser.add_argument('--colorProfile', default=False, action='store_true', help="Historical file")

    args = parser.parse_args()
    server = Server(args.imbaRfile, args.imbaHfile, args.ohlcRfile, args.ohlcHfile, args.colorProfile)

Main()

