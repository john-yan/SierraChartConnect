#!/bin/bash

# activate python virtualenv
source env/bin/activate

PYTHON=$(which python3)

ES_SYMBOL=ESH21
NQ_SYMBOL=NQH21

ES_LOG=$ES_SYMBOL-$DATE.log
NQ_LOG=$NQ_SYMBOL-$DATE.log

FROM=$(date --date='yesterday 17:59:59' +"%s")
TO=$(date --date='today 17:00:00' +"%s")
echo "downloading trades from $FROM - $TO"
$PYTHON HistoricalDataDownloader.py -s $ES_SYMBOL -a 192.168.122.169 --sDateTime=$FROM --eDateTime=$TO -o today.trades

