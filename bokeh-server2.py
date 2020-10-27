
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

columns = "DateTime,Price,VolumeAtBid,VolumeAtAsk,TotalVolume,BidImbalance,AskImbalance,VolumeDistribution".split(',')
chart_columns = [
    'CellTop',
    'CellBottom',
    'CellLeft',
    'CellRight',
    'CellMiddle',
    'VolAtBidText',
    'Separator',
    'VolAtAskText',
    'VolAtBidColor',
    'VolAtAskColor',
    'TotalVolume',
    'VolumeEnd'
]

def WaitUntilFileReady(filelist):
    rlist, _, _ = select(filelist, [], [])

def ReadOneLine(thefile):

    line = thefile.readline()

    if not line:
        return line

    while line[-1] != '\n':
        sleep(0.01)
        line += thefile.readline()

    return line

def FileReader(thefile):
    while True:
        line = ReadOneLine(thefile)
        if not line:
            return None
        yield line

def SessionReader(thefile):

    reader = FileReader(thefile)
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


def ComputeChartParameter(table, width, imbalance_highlight_factor):

    raw_data = pd.DataFrame(table, columns=columns)

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
            'VolumeEnd': VolumeEnd.astype(np.int64)
    })

    return chart_parameter

class Server:

    def plot_source(self, source):
        # plot base
        self.plot.quad(top='CellTop', bottom='CellBottom', left='CellLeft',
                            right='CellRight', color='#F0F0F0', source=source, name='hoverable')

        # plot volume profile
        self.plot.quad(top='CellTop', bottom='CellBottom', left='CellLeft',
                            right='VolumeEnd', color='#B0B0B0', source=source)

        # plot bid
        self.plot.text(x='CellMiddle', y='CellBottom', text='VolAtBidText',
                text_color='VolAtBidColor', text_align='right', text_font_size='12px',
                source=source, x_offset=-5)

        # plot x
        self.plot.text(x='CellMiddle', y='CellBottom', text='Separator',
                text_color='#000000', text_align='center', text_font_size='12px',
                source=source)

        # plot ask
        self.plot.text(x='CellMiddle', y='CellBottom', text='VolAtAskText',
                text_color='VolAtAskColor', text_align='left', text_font_size='12px',
                source=source, x_offset=5)

        for i in range(len(self.plot.tools)):
            if isinstance(self.plot.tools[i], HoverTool):
                del self.plot.tools[i]

        TOOLTIPS = [
                ("Price", "@CellBottom{0.2f}"),
        ]

        hoverTool = HoverTool(
            tooltips=TOOLTIPS,
            names = ['hoverable']
        )

        self.plot.add_tools(hoverTool)


    def __init__(self, rfile, hfile):

        self.hfile = open(hfile)
        self.rfile = open(rfile)
        self.rfile.seek(0, 2)

        self.period_in_seconds = int(self.hfile.readline().rstrip())
        self.width = int(self.period_in_seconds * time_factor * 0.85)
        self.highlight_factor = 3

        TOOLS = "pan,xwheel_zoom,ywheel_zoom,wheel_zoom,box_zoom,reset,save,crosshair"
        self.plot = figure(tools=TOOLS, x_axis_type = 'datetime')
        self.plot.sizing_mode = 'stretch_both'
        self.plot.yaxis.formatter.use_scientific = False
        self.plot.yaxis.ticker = FixedTicker(ticks=np.arange(start=2000, stop=4000, step=0.25))

        table = [line.rstrip().split(',') for line in FileReader(self.hfile)]
        source = ColumnDataSource(ComputeChartParameter(table, self.width, self.highlight_factor))
        self.plot_source(source)

        self.rsource = ColumnDataSource(ComputeChartParameter([], self.width, self.highlight_factor))
        self.plot_source(self.rsource)

        doc.add_root(self.plot)
        doc.on_session_destroyed(self.close)

        self.queue = Queue(maxsize=1)
        self.thread = threading.Thread(target=self.update, daemon=True)
        self.thread.start()

    def close(self, session_context):
        self.rfile.close()
        self.hfile.close()
        pass

    @gen.coroutine
    def update_doc(self):

        hData, rData = self.queue.get()

        if hData['update']:
            self.plot_source(hData['data'])

        if rData['update']:
            self.rsource.data = rData['data']

    def update(self):

        try:
            while True:
                if self.rfile.closed or self.hfile.closed:
                    print('rfile or hfile has been closed.')
                    return

                hData = { 'update': False, 'data': None }
                rData = { 'update': False, 'data': None }

                table = [line.rstrip().split(',') for line in FileReader(self.hfile)]
                if len(table) > 0:
                    hData['data'] = ComputeChartParameter(table, self.width, self.highlight_factor)
                    hData['update'] = True

                latest_table = []
                while True:
                    table = [line.rstrip().split(',') for line in SessionReader(self.rfile)]
                    if len(table) > 0:
                        latest_table = table
                    else:
                        break

                if len(latest_table) > 0:
                    data = ComputeChartParameter(latest_table, self.width, self.highlight_factor)
                    rData['data'] = data
                    rData['update'] = True

                if len(table) > 0 or len(latest_table) > 0:
                    self.queue.put((hData, rData))

                    # update the document from callback
                    doc.add_next_tick_callback(self.update_doc)

                sleep(0.5)

        except Exception as err:
            print('updater exits due to error ', err)

def Main():
    server = Server('ESZ0-CME-imbalance-5min.rfile', 'ESZ0-CME-imbalance-5min.hfile')

Main()

