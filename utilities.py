
import argparse
from time import sleep
import threading

def ReadOneLine(thefile):

    line = thefile.readline()

    if not line:
        return line

    while line[-1] != '\n':
        sleep(0.1)
        line += thefile.readline()

    return line

def follow(thefile, freq = 2, wait_time = 60 * 16):

    wait = wait_time
    sleep_time = 1 / freq

    while True:
        line = ReadOneLine(thefile)
        if not line:
            if thefile.closed or wait <= 0:
                return None
            wait -= sleep_time
            sleep(sleep_time)
            continue
        wait = wait_time
        yield line

class FileFollower:
    def __init__(self, file_name, freq = 10):
        self.thefile = open(file_name, 'r')
        assert(self.thefile)
        self.listeners = []
        self.freq = 10

    def AddListener(self, listener):
        self.listeners.append(listener)

    def RemoveListener(self, listener):
        if listener in self.listeners:
            self.listeners.remove(listener)

    def start(self):
        threading.Thread(target=self._thread, daemon=True).start()

    # follower thread
    def _thread(self):

        f = follow(self.thefile, self.freq)
        index = 0
        for line in f:
            for l in self.listeners:
                l(line, index)
            index += 1

